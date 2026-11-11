pub mod gdrive;
pub mod mybox;
pub mod takeout;

use crate::router::{
    extract_timestamp_from_filename, inspect_magic_bytes, pick_older_timestamp,
    replace_or_append_extension, DetectedFileKind, SourceType, TargetRouter,
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{NaiveDate, TimeZone, Utc};
use futures::StreamExt;
use reqwest::Response;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use zip::ZipArchive;

pub const DEFAULT_FALLBACK_TIMESTAMP: &str = "1970-01-01T00:00:00Z";
pub const LARGE_FILE_DISK_FALLBACK_THRESHOLD: u64 = 256 * 1024 * 1024; // 256 MB
const HTTP_CONNECT_TIMEOUT_SECS: u64 = 15;
const HTTP_POOL_IDLE_TIMEOUT_SECS: u64 = 90;
const HTTP_TCP_KEEPALIVE_SECS: u64 = 30;
const STREAM_CHUNK_TIMEOUT_SECS: u64 = 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteFolder {
    pub folder_id: String,
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteFile {
    pub resource_id: String,
    pub name: String,
    pub parent_path: String,
    pub size: i64,
    pub created_time: String,
    pub modified_time: String,
    pub sha1_hash: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListPage {
    pub folders: Vec<RemoteFolder>,
    pub files: Vec<RemoteFile>,
    pub next_cursor: Option<String>,
}

/// RAII Guard that automatically removes the temporary file in `/tmp/mybox_sync`
/// when dropped, preventing ZFS / tmpfs space leaks even on error or panic.
#[derive(Debug)]
pub struct TempFileGuard {
    path: PathBuf,
}

impl TempFileGuard {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug)]
pub struct DownloadedSourceFile {
    pub temp_guard: TempFileGuard,
    pub sha1_hash: String,
    pub size: u64,
    pub created_time_override: Option<String>,
    pub modified_time_override: Option<String>,
}

#[derive(Debug)]
pub struct ExtractedLivePhoto {
    pub image_guard: TempFileGuard,
    pub image_filename: String,
    pub image_sha1: String,
    pub image_size: u64,
    pub video_guard: Option<TempFileGuard>,
    pub video_filename: Option<String>,
    pub timestamp_override: Option<String>,
}

pub fn zip_datetime_to_rfc3339_opt(dt: Option<zip::DateTime>) -> Option<String> {
    let dt = dt?;
    if dt.year() <= 1980
        && dt.month() == 1
        && dt.day() == 1
        && dt.hour() == 0
        && dt.minute() == 0
        && dt.second() == 0
    {
        return None;
    }
    NaiveDate::from_ymd_opt(dt.year() as i32, dt.month() as u32, dt.day() as u32)
        .and_then(|d| d.and_hms_opt(dt.hour() as u32, dt.minute() as u32, dt.second() as u32))
        .map(|naive| Utc.from_utc_datetime(&naive).to_rfc3339())
}

pub fn try_extract_livephoto_zip(
    zip_path: &Path,
    original_zip_name: &str,
    tmp_dir: &Path,
) -> Result<Option<ExtractedLivePhoto>> {
    let Ok(file) = std::fs::File::open(zip_path) else {
        return Ok(None);
    };
    let Ok(mut archive) = ZipArchive::new(file) else {
        return Ok(None);
    };

    let is_named_livephoto = TargetRouter::is_livephoto_archive_name(original_zip_name);
    let mut image_entry: Option<(usize, String, Option<String>)> = None;
    let mut video_entry: Option<(usize, String, Option<String>)> = None;
    let mut has_other_files = false;

    for idx in 0..archive.len() {
        let Ok(mut zf) = archive.by_index(idx) else {
            continue;
        };
        if zf.is_dir() {
            continue;
        }
        let raw_entry_name = zf.name().to_string();
        if raw_entry_name.starts_with("__MACOSX/") {
            continue;
        }
        let fname = Path::new(&raw_entry_name)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&raw_entry_name)
            .to_string();
        if fname.starts_with("._") || fname.eq_ignore_ascii_case(".DS_Store") {
            continue;
        }

        let mut header = [0u8; 64];
        let n = zf.read(&mut header).unwrap_or(0);
        let detected = inspect_magic_bytes(&header[..n]);
        let entry_ts = zip_datetime_to_rfc3339_opt(zf.last_modified())
            .or_else(|| extract_timestamp_from_filename(&fname));

        let ext = Path::new(&fname)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();

        match detected {
            Some(DetectedFileKind::Media {
                ext: det_ext,
                is_video: false,
                ..
            }) => {
                let normalized = if TargetRouter::has_null_extension(&fname)
                    || !TargetRouter::is_media_extension(&ext)
                {
                    replace_or_append_extension(&fname, det_ext)
                } else {
                    fname
                };
                if image_entry.is_none() {
                    image_entry = Some((idx, normalized, entry_ts));
                }
            }
            Some(DetectedFileKind::Media {
                ext: det_ext,
                is_video: true,
                ..
            }) => {
                let normalized = if TargetRouter::has_null_extension(&fname)
                    || !TargetRouter::is_video_extension(&ext)
                {
                    replace_or_append_extension(&fname, det_ext)
                } else {
                    fname
                };
                if video_entry.is_none() {
                    video_entry = Some((idx, normalized, entry_ts));
                }
            }
            _ => {
                if TargetRouter::is_video_extension(&ext) {
                    if video_entry.is_none() {
                        video_entry = Some((idx, fname, entry_ts));
                    }
                } else if TargetRouter::is_media_extension(&ext) {
                    if image_entry.is_none() {
                        image_entry = Some((idx, fname, entry_ts));
                    }
                } else {
                    has_other_files = true;
                }
            }
        }
    }

    let qualifies = if is_named_livephoto {
        image_entry.is_some()
    } else {
        image_entry.is_some() && video_entry.is_some() && !has_other_files
    };

    if !qualifies {
        return Ok(None);
    }

    let (img_idx, img_name, img_ts) = image_entry.expect("checked Some above");
    std::fs::create_dir_all(tmp_dir)?;
    let prefix_hash = hex::encode(Sha1::digest(
        format!("{}:{}", zip_path.display(), original_zip_name).as_bytes(),
    ));
    let prefix10 = &prefix_hash[..10];

    let img_temp_path = tmp_dir.join(format!("lp_img_{}_{}", prefix10, sanitize_filename(&img_name)));
    let img_guard = TempFileGuard::new(img_temp_path.clone());
    let mut img_hasher = Sha1::new();
    let mut img_size: u64 = 0;
    {
        let mut zf = archive.by_index(img_idx)?;
        let mut out = std::fs::File::create(&img_temp_path)?;
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = zf.read(&mut buf)?;
            if n == 0 {
                break;
            }
            img_hasher.update(&buf[..n]);
            out.write_all(&buf[..n])?;
            img_size += n as u64;
        }
        out.flush()?;
    }

    let (video_guard, video_filename, vid_ts) = if let Some((vid_idx, vid_name, v_ts)) = video_entry
    {
        let vid_temp_path =
            tmp_dir.join(format!("lp_vid_{}_{}", prefix10, sanitize_filename(&vid_name)));
        let v_guard = TempFileGuard::new(vid_temp_path.clone());
        let mut zf = archive.by_index(vid_idx)?;
        let mut out = std::fs::File::create(&vid_temp_path)?;
        std::io::copy(&mut zf, &mut out)?;
        out.flush()?;
        (Some(v_guard), Some(vid_name), v_ts)
    } else {
        (None, None, None)
    };

    let outer_ts = extract_timestamp_from_filename(original_zip_name);
    let best_ts = [img_ts, vid_ts, outer_ts]
        .into_iter()
        .flatten()
        .reduce(|acc, cand| pick_older_timestamp(&acc, Some(&cand)));

    Ok(Some(ExtractedLivePhoto {
        image_guard: img_guard,
        image_filename: img_name,
        image_sha1: hex::encode(img_hasher.finalize()),
        image_size: img_size,
        video_guard,
        video_filename,
        timestamp_override: best_ts,
    }))
}

pub fn join_remote_path(parent: &str, name: &str) -> String {
    let trimmed_parent = parent.trim_end_matches('/');
    let trimmed_name = name.trim_matches('/');
    if trimmed_parent.is_empty() {
        format!("/{}", trimmed_name)
    } else {
        format!("{}/{}", trimmed_parent, trimmed_name)
    }
}

pub fn sanitize_filename(filename: &str) -> String {
    filename.replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_")
}

pub fn resolve_temp_dir(
    preferred_tmp: &Path,
    fallback_disk_tmp: Option<&Path>,
    size_hint: Option<u64>,
) -> PathBuf {
    if let (Some(disk_dir), Some(sz)) = (fallback_disk_tmp, size_hint) {
        if sz >= LARGE_FILE_DISK_FALLBACK_THRESHOLD {
            return disk_dir.to_path_buf();
        }
    }
    preferred_tmp.to_path_buf()
}

pub fn cleanup_temp_dir(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        let p = entry.path();
        if p.is_file() {
            let _ = std::fs::remove_file(p);
        }
    }
}

pub fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(std::time::Duration::from_secs(HTTP_CONNECT_TIMEOUT_SECS))
        .pool_idle_timeout(std::time::Duration::from_secs(HTTP_POOL_IDLE_TIMEOUT_SECS))
        .tcp_keepalive(std::time::Duration::from_secs(HTTP_TCP_KEEPALIVE_SECS))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

/// Builds an HTTP client for self-hosted LAN/NAS targets (Immich, Nextcloud),
/// accepting self-signed or private CA TLS certificates on internal IPs/domains.
pub fn build_target_http_client(follow_redirects: bool) -> reqwest::Client {
    let redirect_policy = if follow_redirects {
        reqwest::redirect::Policy::default()
    } else {
        reqwest::redirect::Policy::none()
    };
    reqwest::Client::builder()
        .no_proxy()
        .danger_accept_invalid_certs(true)
        .redirect(redirect_policy)
        .connect_timeout(std::time::Duration::from_secs(HTTP_CONNECT_TIMEOUT_SECS))
        .pool_idle_timeout(std::time::Duration::from_secs(HTTP_POOL_IDLE_TIMEOUT_SECS))
        .tcp_keepalive(std::time::Duration::from_secs(HTTP_TCP_KEEPALIVE_SECS))
        .build()
        .unwrap_or_else(|_| build_http_client())
}

/// Streams an HTTP response body to a temporary file guarded by `TempFileGuard`
/// while computing its SHA-1 checksum in a single pass.
pub async fn stream_response_to_temp(
    resp: Response,
    source_prefix: &str,
    resource_id: &str,
    filename: &str,
    tmp_dir: &Path,
) -> Result<DownloadedSourceFile> {
    tokio::fs::create_dir_all(tmp_dir).await?;
    let safe_id = sanitize_filename(resource_id);
    let safe_name = sanitize_filename(filename);
    let temp_path = tmp_dir.join(format!("{}_{}_{}", source_prefix, safe_id, safe_name));
    let temp_guard = TempFileGuard::new(temp_path);

    let mut file = tokio::fs::File::create(temp_guard.path()).await?;
    let mut hasher = Sha1::new();
    let mut total_bytes: u64 = 0;
    let mut stream = resp.bytes_stream();
    let chunk_timeout = std::time::Duration::from_secs(STREAM_CHUNK_TIMEOUT_SECS);

    loop {
        let next_item = tokio::time::timeout(chunk_timeout, stream.next())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "다운로드 스트림 읽기 타임아웃 ({}초 초과)",
                    STREAM_CHUNK_TIMEOUT_SECS
                )
            })?;
        let Some(chunk_res) = next_item else {
            break;
        };
        let chunk = chunk_res?;
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
        total_bytes += chunk.len() as u64;
    }
    file.flush().await?;
    drop(file);

    Ok(DownloadedSourceFile {
        temp_guard,
        sha1_hash: hex::encode(hasher.finalize()),
        size: total_bytes,
        created_time_override: None,
        modified_time_override: None,
    })
}

#[async_trait]
pub trait SourceProvider: Send + Sync {
    fn source_type(&self) -> SourceType;
    async fn test_connection(&self) -> Result<String>;
    async fn list_page(
        &self,
        folder_id: &str,
        folder_path: &str,
        cursor: Option<&str>,
    ) -> Result<ListPage>;
    async fn download_to_temp(
        &self,
        resource_id: &str,
        filename: &str,
        tmp_dir: &Path,
    ) -> Result<DownloadedSourceFile>;
    async fn delete_resource(&self, resource_id: &str) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_temp_file_guard_raii_cleanup() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("temp_test.bin");
        std::fs::write(&file_path, b"hello tmpfs").unwrap();
        assert!(file_path.exists());

        {
            let _guard = TempFileGuard::new(file_path.clone());
            assert!(file_path.exists());
        }

        // Guard dropped: file must be removed automatically
        assert!(!file_path.exists());
    }

    #[test]
    fn test_livephoto_zip_extraction_and_normal_zip_ignore() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        use zip::ZipWriter;

        let dir = TempDir::new().unwrap();
        let tmp_dir = TempDir::new().unwrap();

        // 1. Live Photo ZIP with JPG + MOV and ZIP entry timestamp
        let lp_zip_path = dir.path().join("IMG_1234.livephoto.zip");
        {
            let f = std::fs::File::create(&lp_zip_path).unwrap();
            let mut zw = ZipWriter::new(f);
            let dt = zip::DateTime::from_date_and_time(2023, 7, 15, 10, 20, 30).unwrap();
            let opts = SimpleFileOptions::default().last_modified_time(dt);

            zw.start_file("__MACOSX/._IMG_1234.JPG", opts).unwrap();
            zw.write_all(b"macos_resource_fork").unwrap();

            zw.start_file("IMG_1234.JPG", opts).unwrap();
            zw.write_all(b"\xFF\xD8\xFF\xE0fake_jpeg_data_1234")
                .unwrap();

            zw.start_file("IMG_1234.MOV", opts).unwrap();
            zw.write_all(b"\x00\x00\x00\x14ftypqt  \x20\x05\x03\x00fake_mov_data")
                .unwrap();

            zw.finish().unwrap();
        }

        let extracted =
            try_extract_livephoto_zip(&lp_zip_path, "IMG_1234.livephoto.zip", tmp_dir.path())
                .unwrap()
                .expect("must extract Live Photo pair");
        assert_eq!(extracted.image_filename, "IMG_1234.JPG");
        assert_eq!(
            extracted.video_filename.as_deref(),
            Some("IMG_1234.MOV")
        );
        assert!(extracted.image_guard.path().exists());
        assert!(extracted.video_guard.as_ref().unwrap().path().exists());
        assert!(extracted
            .timestamp_override
            .as_deref()
            .unwrap()
            .starts_with("2023-07-15T10:20:30"));

        // 2. Normal document ZIP must return None
        let doc_zip_path = dir.path().join("project_docs.zip");
        {
            let f = std::fs::File::create(&doc_zip_path).unwrap();
            let mut zw = ZipWriter::new(f);
            let opts = SimpleFileOptions::default();
            zw.start_file("report.pdf", opts).unwrap();
            zw.write_all(b"%PDF-1.7 fake pdf").unwrap();
            zw.finish().unwrap();
        }
        assert!(
            try_extract_livephoto_zip(&doc_zip_path, "project_docs.zip", tmp_dir.path())
                .unwrap()
                .is_none()
        );
    }
}
