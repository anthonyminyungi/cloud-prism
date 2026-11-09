pub mod immich;
pub mod nextcloud;

use crate::db::FileRecord;
use crate::router::{SourceType, SyncResultKind, TargetType};
use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;

pub struct UploadContext<'a> {
    pub source_type: SourceType,
    pub file_path: &'a Path,
    pub resource_id: &'a str,
    pub filename: &'a str,
    pub parent_path: &'a str,
    pub sha1_hash: &'a str,
    pub created_at: &'a str,
    pub modified_at: &'a str,
}

#[derive(Debug, Clone)]
pub struct UploadOutcome {
    pub remote_target_id: String,
    pub sync_result: SyncResultKind,
}

#[async_trait]
pub trait TargetProvider: Send + Sync {
    fn target_type(&self) -> TargetType;
    async fn test_connection(&self) -> Result<String>;

    /// Optional pre-download duplicate check when the source provides a server-side SHA-1 hash.
    async fn precheck_duplicate(
        &self,
        _resource_id: &str,
        _sha1_hash: &str,
    ) -> Result<Option<String>> {
        Ok(None)
    }

    async fn upload(&self, ctx: UploadContext<'_>) -> Result<UploadOutcome>;
    async fn verify_exists(&self, file: &FileRecord) -> Result<bool>;
}
