use crate::config::normalize_base_url;
use crate::db::FileRecord;
use crate::limiter::RateLimiter;
use crate::router::{SyncResultKind, TargetType};
use crate::sources::build_target_http_client;
use crate::targets::{TargetProvider, UploadContext, UploadOutcome};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

const ENDPOINT_PING: &str = "/api/server/ping";
const ENDPOINT_BULK_CHECK: &str = "/api/assets/bulk-upload-check";
const ENDPOINT_ASSETS: &str = "/api/assets";
const HEADER_API_KEY: &str = "x-api-key";
const DEVICE_ID: &str = "mybox-immich-sync";

#[derive(Debug, Serialize)]
struct BulkUploadCheckRequest<'a> {
    assets: Vec<BulkUploadCheckItem<'a>>,
}

#[derive(Debug, Serialize)]
struct BulkUploadCheckItem<'a> {
    id: &'a str,
    checksum: &'a str,
}

#[derive(Debug, Default, Deserialize)]
struct BulkUploadCheckResponse {
    #[serde(default)]
    results: Vec<BulkUploadCheckResultDto>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkUploadCheckResultDto {
    #[serde(default)]
    id: String,
    #[serde(default)]
    action: String,
    #[serde(default)]
    asset_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct AssetUploadResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

pub struct ImmichTarget {
    base_url: String,
    api_key: String,
    client: reqwest::Client,
    limiter: RateLimiter,
}

impl ImmichTarget {
    pub fn new(base_url: String, api_key: String, limiter: RateLimiter) -> Self {
        Self {
            base_url: normalize_base_url(&base_url),
            api_key,
            client: build_target_http_client(true),
            limiter,
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Calls `POST /api/assets/bulk-upload-check` with `(id, sha1_checksum)`.
    /// Returns a map from `id` to `Some(existing_asset_id)` if already present (`action == "reject"`),
    /// or `None` if it needs to be uploaded (`action == "accept"`).
    pub async fn check_bulk_hashes(
        &self,
        items: &[(String, String)],
    ) -> Result<HashMap<String, Option<String>>> {
        let mut out = HashMap::new();
        if items.is_empty() {
            return Ok(out);
        }

        let url = self.endpoint(ENDPOINT_BULK_CHECK);
        let payload = BulkUploadCheckRequest {
            assets: items
                .iter()
                .map(|(id, checksum)| BulkUploadCheckItem { id, checksum })
                .collect(),
        };

        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .post(&url)
                    .header(HEADER_API_KEY, &self.api_key)
                    .json(&payload)
            })
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Immich bulk-upload-check 실패: HTTP {}",
                resp.status()
            ));
        }

        let dto: BulkUploadCheckResponse = resp.json().await?;
        for r in dto.results {
            if r.action == "reject" {
                let asset_id = r
                    .asset_id
                    .unwrap_or_else(|| "existing_immich_asset".to_string());
                out.insert(r.id, Some(asset_id));
            } else {
                out.insert(r.id, None);
            }
        }

        Ok(out)
    }

    pub async fn verify_hash_exists(&self, resource_id: &str, sha1_hash: &str) -> Result<bool> {
        Ok(self
            .precheck_duplicate(resource_id, sha1_hash)
            .await?
            .is_some())
    }

    pub async fn upload_asset(
        &self,
        file_path: &Path,
        filename: &str,
        device_asset_id: &str,
        created_at: &str,
        modified_at: &str,
    ) -> Result<(String, SyncResultKind)> {
        let url = self.endpoint(ENDPOINT_ASSETS);
        let file_len = tokio::fs::metadata(file_path).await?.len();

        let resp = self
            .limiter
            .send_with_retry(|| {
                let std_file =
                    std::fs::File::open(file_path).expect("temp file must exist during upload");
                let async_file = tokio::fs::File::from_std(std_file);
                let reader_stream = tokio_util::io::ReaderStream::new(async_file);
                let body = reqwest::Body::wrap_stream(reader_stream);
                let part = Part::stream_with_length(body, file_len).file_name(filename.to_string());
                let form = Form::new()
                    .text("deviceAssetId", device_asset_id.to_string())
                    .text("deviceId", DEVICE_ID)
                    .text("fileCreatedAt", created_at.to_string())
                    .text("fileModifiedAt", modified_at.to_string())
                    .part("assetData", part);
                self.client
                    .post(&url)
                    .header(HEADER_API_KEY, &self.api_key)
                    .multipart(form)
            })
            .await
            .map_err(|e| anyhow!("Immich 업로드 전송 오류: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Immich 자산 업로드 실패 (HTTP {}): {}",
                status,
                text
            ));
        }

        let dto: AssetUploadResponse = resp.json().await.unwrap_or_default();
        let id = dto.id.unwrap_or_else(|| "immich_uploaded_id".to_string());
        let kind = match dto.status.as_deref() {
            Some("duplicate") => SyncResultKind::Duplicate,
            _ => SyncResultKind::Created,
        };

        Ok((id, kind))
    }
}

#[async_trait]
impl TargetProvider for ImmichTarget {
    fn target_type(&self) -> TargetType {
        TargetType::Immich
    }

    async fn test_connection(&self) -> Result<String> {
        if self.base_url.trim().is_empty() || self.api_key.trim().is_empty() {
            return Err(anyhow!(
                "Immich 서버 URL 또는 API Key가 설정되지 않았습니다."
            ));
        }

        let url = self.endpoint(ENDPOINT_PING);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).header(HEADER_API_KEY, &self.api_key))
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!("Immich 서버 연결 실패 (HTTP {})", resp.status()));
        }

        Ok(format!("Immich 연결 성공 ({})", self.base_url))
    }

    async fn precheck_duplicate(
        &self,
        resource_id: &str,
        sha1_hash: &str,
    ) -> Result<Option<String>> {
        let items = vec![(resource_id.to_string(), sha1_hash.to_string())];
        let mut map = self.check_bulk_hashes(&items).await?;
        Ok(map.remove(resource_id).flatten())
    }

    async fn upload(&self, ctx: UploadContext<'_>) -> Result<UploadOutcome> {
        if let Some(existing_asset_id) = self
            .precheck_duplicate(ctx.resource_id, ctx.sha1_hash)
            .await?
        {
            return Ok(UploadOutcome {
                remote_target_id: existing_asset_id,
                sync_result: SyncResultKind::Duplicate,
            });
        }

        let (remote_target_id, sync_result) = self
            .upload_asset(
                ctx.file_path,
                ctx.filename,
                ctx.resource_id,
                ctx.created_at,
                ctx.modified_at,
            )
            .await?;

        Ok(UploadOutcome {
            remote_target_id,
            sync_result,
        })
    }

    async fn verify_exists(&self, file: &FileRecord) -> Result<bool> {
        let Some(ref sha1) = file.sha1_hash else {
            return Ok(false);
        };
        self.verify_hash_exists(&file.resource_id, sha1).await
    }

    async fn verify_exists_batch(&self, files: &[FileRecord]) -> Result<HashMap<String, bool>> {
        let mut out = HashMap::with_capacity(files.len());
        let mut check_items = Vec::new();
        for f in files {
            if let Some(ref sha1) = f.sha1_hash {
                check_items.push((f.resource_id.clone(), sha1.clone()));
            } else {
                out.insert(f.resource_id.clone(), false);
            }
        }
        for chunk in check_items.chunks(500) {
            let chunk_res = self.check_bulk_hashes(chunk).await?;
            for (res_id, _) in chunk {
                let exists = chunk_res.get(res_id).and_then(|opt| opt.as_ref()).is_some();
                out.insert(res_id.clone(), exists);
            }
        }
        Ok(out)
    }
}
