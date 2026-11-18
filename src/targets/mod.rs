pub mod immich;
pub mod nextcloud;

use crate::db::FileRecord;
use crate::router::{SourceType, SyncResultKind, TargetType};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyStatus {
    Present {
        healed_target_id: Option<String>,
        healed_sha1: Option<String>,
    },
    TrashedOnTarget,
    MissingOnTarget,
    SizeMismatch {
        expected: i64,
        actual: i64,
    },
    TargetError(String),
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

    async fn inspect_exists_batch(&self, files: &[FileRecord]) -> HashMap<String, VerifyStatus> {
        let mut out = HashMap::with_capacity(files.len());
        for f in files {
            let st = match self.verify_exists(f).await {
                Ok(true) => VerifyStatus::Present {
                    healed_target_id: None,
                    healed_sha1: None,
                },
                Ok(false) => VerifyStatus::MissingOnTarget,
                Err(e) => VerifyStatus::TargetError(e.to_string()),
            };
            out.insert(f.resource_id.clone(), st);
        }
        out
    }

    async fn verify_exists_batch(&self, files: &[FileRecord]) -> Result<HashMap<String, bool>> {
        let inspected = self.inspect_exists_batch(files).await;
        Ok(inspected
            .into_iter()
            .map(|(k, v)| (k, matches!(v, VerifyStatus::Present { .. })))
            .collect())
    }
}
