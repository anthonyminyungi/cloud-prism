pub mod gdrive;
pub mod mybox;
pub mod takeout;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

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

#[async_trait]
pub trait SourceProvider: Send + Sync {
    fn source_type(&self) -> &'static str;
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
