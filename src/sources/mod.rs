pub mod gdrive;
pub mod mybox;
pub mod takeout;

use crate::router::SourceType;
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use reqwest::Response;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

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
}
