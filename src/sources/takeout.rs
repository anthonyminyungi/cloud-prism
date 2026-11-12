use crate::router::{SourceType, TargetRouter};
use crate::sources::{
    sanitize_filename, DownloadedSourceFile, ListPage, RemoteFile, SourceProvider, TempFileGuard,
    DEFAULT_FALLBACK_TIMESTAMP,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use zip::ZipArchive;

/// Strips the outer `Takeout/` wrapper and service root folder (`드라이브`, `Drive`, `Google 포토`, etc.)
/// so Google Drive folder structures inside a Takeout ZIP map cleanly under `/GoogleDrive/...` in Nextcloud.
pub fn normalize_takeout_parent_path(raw_parent: &str) -> String {
    let raw_parent = raw_parent
        .split_once("::")
        .map(|(_, e)| e)
        .unwrap_or(raw_parent);
    let mut segments: Vec<&str> = raw_parent
        .split(['/', '\\'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();

    if segments
        .first()
        .map(|s| s.eq_ignore_ascii_case("takeout"))
        .unwrap_or(false)
    {
        segments.remove(0);
    }

    if let Some(&first) = segments.first() {
        let lower = first.to_lowercase();
        if matches!(
            lower.as_str(),
            "드라이브"
                | "drive"
                | "내 드라이브"
                | "my drive"
                | "google 드라이브"
                | "google drive"
                | "google 포토"
                | "google photos"
        ) {
            segments.remove(0);
        }
    }

    if segments.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", segments.join("/"))
    }
}

/// Returns `true` if a Takeout ZIP entry path (`resource_id`) originated from the Google Drive
/// service folder inside the archive (e.g. `Takeout/드라이브/...`, `Takeout/Drive/...`),
/// excluding `Google 포토` / `Google Photos`.
pub fn is_takeout_gdrive_entry(entry_name: &str) -> bool {
    let entry_name = entry_name
        .split_once("::")
        .map(|(_, e)| e)
        .unwrap_or(entry_name);
    let mut segments: Vec<&str> = entry_name
        .split(['/', '\\'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();

    if segments
        .first()
        .map(|s| s.eq_ignore_ascii_case("takeout"))
        .unwrap_or(false)
    {
        segments.remove(0);
    }

    let Some(&service_root) = segments.first() else {
        return false;
    };

    matches!(
        service_root.to_lowercase().as_str(),
        "드라이브"
            | "drive"
            | "내 드라이브"
            | "my drive"
            | "google 드라이브"
            | "google drive"
    )
}

fn is_takeout_system_entry(entry_name: &str, filename: &str) -> bool {
    let lower_entry = entry_name.to_ascii_lowercase();
    if lower_entry.ends_with(".json") {
        return true;
    }
    let lower_name = filename.to_lowercase();
    matches!(
        lower_name.as_str(),
        "archive_browser.html" | "아카이브_브라우저.html"
    )
}

fn zip_datetime_to_rfc3339(dt: Option<zip::DateTime>) -> String {
    let Some(dt) = dt else {
        return DEFAULT_FALLBACK_TIMESTAMP.to_string();
    };
    // Unset MS-DOS ZIP timestamps default to 1980-01-01 00:00:00
    if dt.year() <= 1980
        && dt.month() == 1
        && dt.day() == 1
        && dt.hour() == 0
        && dt.minute() == 0
        && dt.second() == 0
    {
        return DEFAULT_FALLBACK_TIMESTAMP.to_string();
    }
    NaiveDate::from_ymd_opt(dt.year() as i32, dt.month() as u32, dt.day() as u32)
        .and_then(|d| d.and_hms_opt(dt.hour() as u32, dt.minute() as u32, dt.second() as u32))
        .map(|naive| Utc.from_utc_datetime(&naive).to_rfc3339())
        .unwrap_or_else(|| DEFAULT_FALLBACK_TIMESTAMP.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TakeoutArchiveInfo {
    pub name: String,
    pub size_bytes: u64,
}

pub fn list_takeout_archives(dir: impl AsRef<Path>) -> Vec<TakeoutArchiveInfo> {
    let path = dir.as_ref();
    let Ok(entries) = std::fs::read_dir(path) else {
        return Vec::new();
    };
    let mut list: Vec<TakeoutArchiveInfo> = entries
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let p = e.path();
            let is_zip = p
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("zip"))
                .unwrap_or(false);
            if !is_zip {
                return None;
            }
            let name = p.file_name()?.to_str()?.to_string();
            let size_bytes = e.metadata().map(|m| m.len()).unwrap_or(0);
            Some(TakeoutArchiveInfo { name, size_bytes })
        })
        .collect();
    list.sort_by(|a, b| a.name.cmp(&b.name));
    list
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TakeoutSidecarMetadata {
    photo_taken_time: Option<TakeoutTimestamp>,
    creation_time: Option<TakeoutTimestamp>,
}

#[derive(Debug, Default, Deserialize)]
struct TakeoutTimestamp {
    timestamp: Option<String>,
}

#[derive(Debug, Clone)]
struct TakeoutArchiveIndex {
    zip_signatures: Vec<(PathBuf, u64, Option<SystemTime>)>,
    entry_to_zip: HashMap<String, PathBuf>,
    sidecar_to_zip: HashMap<String, PathBuf>,
    non_media_count: usize,
}

pub struct GoogleTakeoutProvider {
    takeout_dir: PathBuf,
    index_cache: Arc<Mutex<Option<TakeoutArchiveIndex>>>,
}

impl GoogleTakeoutProvider {
    pub fn new(takeout_dir: impl Into<PathBuf>) -> Self {
        Self {
            takeout_dir: takeout_dir.into(),
            index_cache: Arc::new(Mutex::new(None)),
        }
    }

    fn list_zip_paths(&self) -> Result<Vec<PathBuf>> {
        if !self.takeout_dir.exists() {
            return Ok(Vec::new());
        }

        let mut paths: Vec<PathBuf> = std::fs::read_dir(&self.takeout_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("zip"))
                    .unwrap_or(false)
            })
            .collect();
        paths.sort();
        Ok(paths)
    }

    fn compute_zip_signatures(&self) -> Result<Vec<(PathBuf, u64, Option<SystemTime>)>> {
        let paths = self.list_zip_paths()?;
        let mut sigs = Vec::with_capacity(paths.len());
        for p in paths {
            if let Ok(meta) = std::fs::metadata(&p) {
                sigs.push((p, meta.len(), meta.modified().ok()));
            }
        }
        Ok(sigs)
    }

    fn get_or_build_index(&self) -> Result<TakeoutArchiveIndex> {
        let sigs = self.compute_zip_signatures()?;
        if let Ok(guard) = self.index_cache.lock() {
            if let Some(ref cached) = *guard {
                if cached.zip_signatures == sigs {
                    return Ok(cached.clone());
                }
            }
        }

        let mut entry_to_zip = HashMap::new();
        let mut sidecar_to_zip = HashMap::new();
        let mut non_media_count = 0usize;

        for (zip_path, _, _) in &sigs {
            let Ok(file) = File::open(zip_path) else {
                continue;
            };
            let Ok(mut archive) = ZipArchive::new(file) else {
                continue;
            };

            for i in 0..archive.len() {
                let Ok(zf) = archive.by_index_raw(i) else {
                    continue;
                };
                if zf.is_dir() {
                    continue;
                }
                let entry_name = zf.name().to_string();
                if entry_name.to_ascii_lowercase().ends_with(".json") {
                    sidecar_to_zip
                        .entry(entry_name)
                        .or_insert_with(|| zip_path.clone());
                    continue;
                }

                let filename = Path::new(&entry_name)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&entry_name)
                    .to_string();
                if is_takeout_system_entry(&entry_name, &filename) {
                    continue;
                }

                if let std::collections::hash_map::Entry::Vacant(vacant) =
                    entry_to_zip.entry(entry_name)
                {
                    vacant.insert(zip_path.clone());
                    if TargetRouter::classify(&filename, false).is_none() {
                        non_media_count += 1;
                    }
                }
            }
        }

        let built = TakeoutArchiveIndex {
            zip_signatures: sigs,
            entry_to_zip,
            sidecar_to_zip,
            non_media_count,
        };

        if let Ok(mut guard) = self.index_cache.lock() {
            *guard = Some(built.clone());
        }

        Ok(built)
    }

    /// Returns the number of non-media user files inside the Takeout ZIP archives
    /// that would be skipped when `enable_nextcloud` is false.
    pub fn count_unclassified_files(&self, enable_nextcloud: bool) -> Result<usize> {
        if enable_nextcloud {
            return Ok(0);
        }
        let idx = self.get_or_build_index()?;
        Ok(idx.non_media_count)
    }

    fn parse_sidecar_timestamp(json_bytes: &[u8]) -> Option<String> {
        let meta: TakeoutSidecarMetadata = serde_json::from_slice(json_bytes).ok()?;
        let ts_str = meta
            .photo_taken_time
            .and_then(|t| t.timestamp)
            .or_else(|| meta.creation_time.and_then(|t| t.timestamp))?;
        let secs = ts_str.parse::<i64>().ok()?;
        Utc.timestamp_opt(secs, 0)
            .single()
            .map(|dt| dt.to_rfc3339())
    }

    fn sidecar_candidates(entry_name: &str) -> Vec<String> {
        let mut candidates = vec![
            format!("{}.json", entry_name),
            format!("{}.supplemental-metadata.json", entry_name),
        ];
        if let Some((stem, _ext)) = entry_name.rsplit_once('.') {
            candidates.push(format!("{}.json", stem));
            candidates.push(format!("{}.supplemental-metadata.json", stem));
        }
        candidates
    }

    fn read_sidecar_from_archive(
        archive: &mut ZipArchive<File>,
        candidates: &[String],
    ) -> Option<String> {
        candidates.iter().find_map(|cand| {
            let mut sidecar = archive.by_name(cand).ok()?;
            let mut json_buf = Vec::new();
            sidecar.read_to_end(&mut json_buf).ok()?;
            Self::parse_sidecar_timestamp(&json_buf)
        })
    }

    fn find_sidecar_timestamp_indexed(
        primary_zip_path: &Path,
        primary_archive: &mut ZipArchive<File>,
        entry_name: &str,
        sidecar_to_zip: &HashMap<String, PathBuf>,
    ) -> Option<String> {
        let candidates = Self::sidecar_candidates(entry_name);

        // 1. Fast path: look inside the same ZIP archive first
        if let Some(ts) = Self::read_sidecar_from_archive(primary_archive, &candidates) {
            return Some(ts);
        }

        // 2. Indexed split-ZIP lookup: only open the specific sibling archive containing the sidecar
        for cand in &candidates {
            let Some(sibling_zip_path) = sidecar_to_zip.get(cand) else {
                continue;
            };
            if sibling_zip_path == primary_zip_path {
                continue;
            }
            let Ok(file) = File::open(sibling_zip_path) else {
                continue;
            };
            let Ok(mut sibling_archive) = ZipArchive::new(file) else {
                continue;
            };
            if let Some(ts) =
                Self::read_sidecar_from_archive(&mut sibling_archive, std::slice::from_ref(cand))
            {
                return Some(ts);
            }
        }

        None
    }

    fn resolve_entry_zip_path(
        &self,
        resource_id: &str,
        index: &TakeoutArchiveIndex,
    ) -> Result<(PathBuf, String)> {
        if let Some((zip_name, entry_name)) = resource_id.split_once("::") {
            let candidate_path = self.takeout_dir.join(zip_name);
            if candidate_path.exists() {
                return Ok((candidate_path, entry_name.to_string()));
            }
        }

        let entry_name = resource_id
            .split_once("::")
            .map(|(_, e)| e)
            .unwrap_or(resource_id);

        if let Some(zip_path) = index.entry_to_zip.get(entry_name) {
            return Ok((zip_path.clone(), entry_name.to_string()));
        }

        for zip_path in self.list_zip_paths()? {
            let Ok(file) = File::open(&zip_path) else {
                continue;
            };
            let Ok(mut archive) = ZipArchive::new(file) else {
                continue;
            };
            if archive.by_name(entry_name).is_ok() {
                return Ok((zip_path, entry_name.to_string()));
            }
        }

        Err(anyhow!(
            "Takeout ZIP 아카이브에서 항목을 찾을 수 없습니다: {}",
            resource_id
        ))
    }
}

#[async_trait]
impl SourceProvider for GoogleTakeoutProvider {
    fn source_type(&self) -> SourceType {
        SourceType::Takeout
    }

    async fn test_connection(&self) -> Result<String> {
        if !self.takeout_dir.exists() {
            return Err(anyhow!(
                "Google Takeout 경로가 존재하지 않습니다: {}",
                self.takeout_dir.display()
            ));
        }

        let zip_count = self.list_zip_paths()?.len();

        Ok(format!(
            "Google Takeout 경로 확인 성공 (ZIP 아카이브 {}개 발견)",
            zip_count
        ))
    }

    async fn list_page(
        &self,
        folder_id: &str,
        _folder_path: &str,
        _cursor: Option<&str>,
    ) -> Result<ListPage> {
        let mut page = ListPage::default();
        if folder_id != "root" || !self.takeout_dir.exists() {
            return Ok(page);
        }

        let mut seen_entries = HashSet::new();

        for zip_path in self.list_zip_paths()? {
            let file = File::open(&zip_path)?;
            let mut archive = ZipArchive::new(file)?;

            for i in 0..archive.len() {
                let zf = archive.by_index_raw(i)?;
                if zf.is_dir() {
                    continue;
                }
                let entry_name = zf.name().to_string();
                let path_obj = Path::new(&entry_name);
                let filename = path_obj
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&entry_name)
                    .to_string();

                if is_takeout_system_entry(&entry_name, &filename) {
                    continue;
                }
                if !seen_entries.insert(entry_name.clone()) {
                    continue;
                }

                let parent = path_obj
                    .parent()
                    .and_then(|p| p.to_str())
                    .map(normalize_takeout_parent_path)
                    .unwrap_or_else(|| "/".to_string());
                let entry_timestamp = zip_datetime_to_rfc3339(zf.last_modified());

                page.files.push(RemoteFile {
                    resource_id: entry_name,
                    name: filename,
                    parent_path: parent,
                    size: zf.size() as i64,
                    created_time: entry_timestamp.clone(),
                    modified_time: entry_timestamp,
                    sha1_hash: None,
                });
            }
        }

        page.files.sort_by(|a, b| {
            a.created_time
                .cmp(&b.created_time)
                .then_with(|| a.modified_time.cmp(&b.modified_time))
        });

        Ok(page)
    }

    async fn download_to_temp(
        &self,
        resource_id: &str,
        filename: &str,
        tmp_dir: &Path,
    ) -> Result<DownloadedSourceFile> {
        let index = self.get_or_build_index()?;
        let (zip_path, entry_name) = self.resolve_entry_zip_path(resource_id, &index)?;

        tokio::fs::create_dir_all(tmp_dir).await?;
        let id_hash = hex::encode(Sha1::digest(resource_id.as_bytes()));
        let id_hash12 = &id_hash[..12];
        let temp_path = tmp_dir.join(format!(
            "takeout_{}_{}",
            id_hash12,
            sanitize_filename(filename)
        ));
        let temp_guard = TempFileGuard::new(temp_path.clone());
        let is_media = TargetRouter::classify(filename, false).is_some();
        let sidecar_to_zip = index.sidecar_to_zip;

        let (sha1_hash, total_bytes, created_override) =
            tokio::task::spawn_blocking(move || -> Result<(String, u64, Option<String>)> {
                let file = File::open(&zip_path)?;
                let mut archive = ZipArchive::new(file)?;
                let mut hasher = Sha1::new();
                let mut total_bytes: u64 = 0;
                {
                    let mut zf = archive.by_name(&entry_name)?;
                    let mut out = File::create(&temp_path)?;
                    let mut buf = [0u8; 64 * 1024];
                    loop {
                        let n = zf.read(&mut buf)?;
                        if n == 0 {
                            break;
                        }
                        hasher.update(&buf[..n]);
                        out.write_all(&buf[..n])?;
                        total_bytes += n as u64;
                    }
                    out.flush()?;
                }

                let created_override = if is_media {
                    Self::find_sidecar_timestamp_indexed(
                        &zip_path,
                        &mut archive,
                        &entry_name,
                        &sidecar_to_zip,
                    )
                } else {
                    None
                };

                Ok((
                    hex::encode(hasher.finalize()),
                    total_bytes,
                    created_override,
                ))
            })
            .await
            .map_err(|e| anyhow!("Takeout 압축 해제 작업 스레드 오류: {}", e))??;

        Ok(DownloadedSourceFile {
            temp_guard,
            sha1_hash,
            size: total_bytes,
            created_time_override: created_override.clone(),
            modified_time_override: created_override,
        })
    }

    async fn delete_resource(&self, _resource_id: &str) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use zip::write::SimpleFileOptions;
    use zip::ZipWriter;

    #[tokio::test]
    async fn test_takeout_zip_and_sidecar_extraction() {
        let dir = TempDir::new().unwrap();
        let zip_path = dir.path().join("takeout-001.zip");
        {
            let file = File::create(&zip_path).unwrap();
            let mut zip = ZipWriter::new(file);
            let opts = SimpleFileOptions::default();

            zip.start_file("Takeout/Google Photos/2026/beach.jpg", opts)
                .unwrap();
            zip.write_all(b"fake_jpeg_bytes_12345").unwrap();

            zip.start_file("Takeout/Google Photos/2026/beach.jpg.json", opts)
                .unwrap();
            zip.write_all(br#"{"photoTakenTime": {"timestamp": "1750000000"}}"#)
                .unwrap();

            zip.finish().unwrap();
        }

        let provider = GoogleTakeoutProvider::new(dir.path());
        let conn_msg = provider.test_connection().await.unwrap();
        assert!(conn_msg.contains("1개 발견"));

        let page = provider.list_page("root", "/", None).await.unwrap();
        assert_eq!(page.files.len(), 1);
        assert_eq!(page.files[0].name, "beach.jpg");
        assert_eq!(
            page.files[0].resource_id,
            "Takeout/Google Photos/2026/beach.jpg"
        );

        let tmp_dir = TempDir::new().unwrap();
        let downloaded = provider
            .download_to_temp(&page.files[0].resource_id, "beach.jpg", tmp_dir.path())
            .await
            .unwrap();

        assert_eq!(downloaded.size, 21);
        assert!(downloaded.created_time_override.is_some());
        let temp_file_path = downloaded.temp_guard.path().to_path_buf();
        assert!(temp_file_path.exists());
        drop(downloaded);
        assert!(!temp_file_path.exists());
    }

    #[tokio::test]
    async fn test_takeout_cross_zip_sidecar_and_canonical_id_dedup() {
        let dir = TempDir::new().unwrap();
        let opts = SimpleFileOptions::default();

        // 001.zip contains the photo itself
        {
            let file = File::create(dir.path().join("takeout-202605-001.zip")).unwrap();
            let mut zip = ZipWriter::new(file);
            zip.start_file("Takeout/Google Photos/Photos from 2025/sunset.jpg", opts)
                .unwrap();
            zip.write_all(b"sunset_image_payload").unwrap();
            zip.finish().unwrap();
        }

        // 002.zip contains the photo's .supplemental-metadata.json sidecar (split across archives!)
        // AND a duplicate copy of sunset.jpg (e.g. from a later export dropped in the same folder)
        {
            let file = File::create(dir.path().join("takeout-202606-002.zip")).unwrap();
            let mut zip = ZipWriter::new(file);
            zip.start_file(
                "Takeout/Google Photos/Photos from 2025/sunset.jpg.supplemental-metadata.json",
                opts,
            )
            .unwrap();
            zip.write_all(br#"{"photoTakenTime": {"timestamp": "1740000000"}}"#)
                .unwrap();

            zip.start_file("Takeout/Google Photos/Photos from 2025/sunset.jpg", opts)
                .unwrap();
            zip.write_all(b"sunset_image_payload").unwrap();
            zip.finish().unwrap();
        }

        let provider = GoogleTakeoutProvider::new(dir.path());
        let page = provider.list_page("root", "/", None).await.unwrap();

        // Must deduplicate across ZIP filenames using the canonical internal entry path
        assert_eq!(page.files.len(), 1);
        assert_eq!(
            page.files[0].resource_id,
            "Takeout/Google Photos/Photos from 2025/sunset.jpg"
        );

        let tmp_dir = TempDir::new().unwrap();
        let downloaded = provider
            .download_to_temp(&page.files[0].resource_id, "sunset.jpg", tmp_dir.path())
            .await
            .unwrap();

        assert_eq!(downloaded.size, 20);
        // Must have resolved the timestamp from sibling archive 002.zip!
        assert!(downloaded.created_time_override.is_some());
    }

    #[tokio::test]
    async fn test_takeout_drive_path_normalization_and_zip_header_timestamp() {
        let dir = TempDir::new().unwrap();
        let zip_path = dir.path().join("takeout-drive-photos.zip");
        {
            let file = File::create(&zip_path).unwrap();
            let mut zip = ZipWriter::new(file);
            let dt = zip::DateTime::from_date_and_time(2025, 8, 15, 14, 30, 0).unwrap();
            let opts = SimpleFileOptions::default().last_modified_time(dt);

            // Boilerplate Takeout index file must be ignored
            zip.start_file("Takeout/archive_browser.html", opts)
                .unwrap();
            zip.write_all(b"<html>Takeout Index</html>").unwrap();

            // Drive root document -> parent_path "/"
            zip.start_file("Takeout/드라이브/이력서.pdf", opts).unwrap();
            zip.write_all(b"pdf_resume").unwrap();

            // Drive subfolder document -> parent_path "/업무/2026"
            zip.start_file("Takeout/드라이브/업무/2026/보고서.docx", opts)
                .unwrap();
            zip.write_all(b"docx_report").unwrap();

            // English Drive subfolder photo -> parent_path "/Trips"
            zip.start_file("Takeout/Drive/Trips/jeju.jpg", opts)
                .unwrap();
            zip.write_all(b"jpg_jeju").unwrap();

            zip.finish().unwrap();
        }

        let provider = GoogleTakeoutProvider::new(dir.path());
        let page = provider.list_page("root", "/", None).await.unwrap();

        assert_eq!(page.files.len(), 3);
        assert_eq!(page.files[0].name, "이력서.pdf");
        assert_eq!(page.files[0].parent_path, "/");
        assert!(page.files[0]
            .modified_time
            .starts_with("2025-08-15T14:30:00"));

        assert_eq!(page.files[1].name, "보고서.docx");
        assert_eq!(page.files[1].parent_path, "/업무/2026");

        assert_eq!(page.files[2].name, "jeju.jpg");
        assert_eq!(page.files[2].parent_path, "/Trips");
    }

    #[tokio::test]
    async fn test_takeout_concurrent_identical_filenames_and_unclassified_count() {
        let dir = TempDir::new().unwrap();
        let zip_path = dir.path().join("takeout-concurrent.zip");
        {
            let file = File::create(&zip_path).unwrap();
            let mut zip = ZipWriter::new(file);
            let opts = SimpleFileOptions::default();

            // Two identically named photos in different folders
            zip.start_file("Takeout/Google Photos/2023/IMG_0001.JPG", opts)
                .unwrap();
            zip.write_all(b"photo_2023_payload_aaaaaa").unwrap();

            zip.start_file("Takeout/Google Photos/2024/IMG_0001.JPG", opts)
                .unwrap();
            zip.write_all(b"photo_2024_payload_bbbbbbbbbb").unwrap();

            // One non-media document inside Drive
            zip.start_file("Takeout/Drive/계약서.pdf", opts).unwrap();
            zip.write_all(b"pdf_document_bytes").unwrap();

            zip.finish().unwrap();
        }

        let provider = std::sync::Arc::new(GoogleTakeoutProvider::new(dir.path()));

        // When Nextcloud is disabled, 1 unclassified file (계약서.pdf) must be detected
        assert_eq!(provider.count_unclassified_files(false).unwrap(), 1);
        assert_eq!(provider.count_unclassified_files(true).unwrap(), 0);

        // Concurrent download_to_temp of identically-named files must not collide on temp file path
        let tmp_dir = TempDir::new().unwrap();
        let p1 = provider.clone();
        let p2 = provider.clone();
        let t1 = tmp_dir.path().to_path_buf();
        let t2 = tmp_dir.path().to_path_buf();

        let (res1, res2) = tokio::join!(
            p1.download_to_temp(
                "Takeout/Google Photos/2023/IMG_0001.JPG",
                "IMG_0001.JPG",
                &t1
            ),
            p2.download_to_temp(
                "Takeout/Google Photos/2024/IMG_0001.JPG",
                "IMG_0001.JPG",
                &t2
            )
        );
        let d1 = res1.unwrap();
        let d2 = res2.unwrap();

        assert_ne!(
            d1.temp_guard.path(),
            d2.temp_guard.path(),
            "Identical filenames in different folders must have distinct temp paths"
        );
        assert_eq!(d1.size, 25);
        assert_eq!(d2.size, 29);
    }

    #[test]
    fn test_is_takeout_gdrive_entry_identifies_only_drive_paths() {
        assert!(is_takeout_gdrive_entry("Takeout/드라이브/업무/시안.jpg"));
        assert!(is_takeout_gdrive_entry("Takeout/Drive/Trips/jeju.jpg"));
        assert!(is_takeout_gdrive_entry("Takeout/내 드라이브/문서/스캔.png"));
        assert!(is_takeout_gdrive_entry("Takeout/My Drive/Work/chart.webp"));
        assert!(is_takeout_gdrive_entry("Takeout/Google 드라이브/a.jpg"));
        assert!(is_takeout_gdrive_entry("Takeout/Google Drive/b.jpg"));
        assert!(is_takeout_gdrive_entry(
            "takeout-001.zip::Takeout/드라이브/업무/시안.jpg"
        ));

        assert!(!is_takeout_gdrive_entry(
            "Takeout/Google 포토/Photos from 2025/sunset.jpg"
        ));
        assert!(!is_takeout_gdrive_entry(
            "takeout-001.zip::Takeout/Google 포토/Photos from 2025/sunset.jpg"
        ));
        assert!(!is_takeout_gdrive_entry(
            "Takeout/Google Photos/2026/beach.jpg"
        ));
        assert!(!is_takeout_gdrive_entry("Takeout/archive_browser.html"));
        assert!(!is_takeout_gdrive_entry(""));
    }
}
