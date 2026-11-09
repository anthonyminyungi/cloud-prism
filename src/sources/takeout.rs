use crate::router::SourceType;
use crate::sources::{
    sanitize_filename, DownloadedSourceFile, ListPage, RemoteFile, SourceProvider, TempFileGuard,
    DEFAULT_FALLBACK_TIMESTAMP,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use zip::ZipArchive;

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

pub struct GoogleTakeoutProvider {
    takeout_dir: PathBuf,
}

impl GoogleTakeoutProvider {
    pub fn new(takeout_dir: impl Into<PathBuf>) -> Self {
        Self {
            takeout_dir: takeout_dir.into(),
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

    fn find_sidecar_timestamp(
        &self,
        primary_zip_path: &Path,
        primary_archive: &mut ZipArchive<File>,
        entry_name: &str,
    ) -> Option<String> {
        let candidates = Self::sidecar_candidates(entry_name);

        // 1. Fast path: look inside the same ZIP archive first
        if let Some(ts) = Self::read_sidecar_from_archive(primary_archive, &candidates) {
            return Some(ts);
        }

        // 2. Split-ZIP fallback: when Google Takeout places the media file in 001.zip
        //    and its .json sidecar in 002.zip, search sibling ZIP archives.
        let zip_paths = self.list_zip_paths().ok()?;
        for zip_path in zip_paths {
            if zip_path == primary_zip_path {
                continue;
            }
            let Ok(file) = File::open(&zip_path) else {
                continue;
            };
            let Ok(mut sibling_archive) = ZipArchive::new(file) else {
                continue;
            };
            if let Some(ts) = Self::read_sidecar_from_archive(&mut sibling_archive, &candidates) {
                return Some(ts);
            }
        }

        None
    }

    fn locate_entry_archive(
        &self,
        resource_id: &str,
    ) -> Result<(PathBuf, ZipArchive<File>, String)> {
        // Support legacy "zip_name::entry_name" format if present, otherwise treat resource_id as canonical entry_name
        if let Some((zip_name, entry_name)) = resource_id.split_once("::") {
            let candidate_path = self.takeout_dir.join(zip_name);
            if candidate_path.exists() {
                let file = File::open(&candidate_path)?;
                let mut archive = ZipArchive::new(file)?;
                if archive.by_name(entry_name).is_ok() {
                    return Ok((candidate_path, archive, entry_name.to_string()));
                }
            }
        }

        let entry_name = resource_id
            .split_once("::")
            .map(|(_, e)| e)
            .unwrap_or(resource_id);

        for zip_path in self.list_zip_paths()? {
            let Ok(file) = File::open(&zip_path) else {
                continue;
            };
            let Ok(mut archive) = ZipArchive::new(file) else {
                continue;
            };
            if archive.by_name(entry_name).is_ok() {
                return Ok((zip_path, archive, entry_name.to_string()));
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
                let zf = archive.by_index(i)?;
                if zf.is_dir() {
                    continue;
                }
                let entry_name = zf.name().to_string();
                if entry_name.to_ascii_lowercase().ends_with(".json") {
                    continue;
                }
                if !seen_entries.insert(entry_name.clone()) {
                    continue;
                }

                let path_obj = Path::new(&entry_name);
                let filename = path_obj
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&entry_name)
                    .to_string();
                let parent = path_obj
                    .parent()
                    .and_then(|p| p.to_str())
                    .map(|p| format!("/{}", p.trim_matches('/')))
                    .unwrap_or_else(|| "/".to_string());

                page.files.push(RemoteFile {
                    resource_id: entry_name,
                    name: filename,
                    parent_path: parent,
                    size: zf.size() as i64,
                    created_time: DEFAULT_FALLBACK_TIMESTAMP.to_string(),
                    modified_time: DEFAULT_FALLBACK_TIMESTAMP.to_string(),
                    sha1_hash: None,
                });
            }
        }

        Ok(page)
    }

    async fn download_to_temp(
        &self,
        resource_id: &str,
        filename: &str,
        tmp_dir: &Path,
    ) -> Result<DownloadedSourceFile> {
        let (zip_path, mut archive, entry_name) = self.locate_entry_archive(resource_id)?;

        tokio::fs::create_dir_all(tmp_dir).await?;
        let temp_path = tmp_dir.join(format!("takeout_{}", sanitize_filename(filename)));
        let temp_guard = TempFileGuard::new(temp_path);

        let mut hasher = Sha1::new();
        let mut total_bytes: u64 = 0;
        {
            let mut zf = archive.by_name(&entry_name)?;
            let mut out = File::create(temp_guard.path())?;
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

        let created_override = self.find_sidecar_timestamp(&zip_path, &mut archive, &entry_name);

        Ok(DownloadedSourceFile {
            temp_guard,
            sha1_hash: hex::encode(hasher.finalize()),
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
}
