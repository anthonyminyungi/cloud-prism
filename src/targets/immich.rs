use crate::limiter::RateLimiter;
use anyhow::{anyhow, Result};
use reqwest::multipart::{Form, Part};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;

pub struct ImmichTarget {
    base_url: String,
    api_key: String,
    client: reqwest::Client,
    limiter: RateLimiter,
}

impl ImmichTarget {
    pub fn new(base_url: String, api_key: String, limiter: RateLimiter) -> Self {
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            client,
            limiter,
        }
    }

    pub async fn test_connection(&self) -> Result<String> {
        if self.base_url.trim().is_empty() || self.api_key.trim().is_empty() {
            return Err(anyhow!(
                "Immich 서버 URL 또는 API Key가 설정되지 않았습니다."
            ));
        }

        let url = format!("{}/api/server/ping", self.base_url);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).header("x-api-key", &self.api_key))
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Immich 서버 연결 실패 (HTTP {})",
                resp.status()
            ));
        }

        Ok(format!("Immich 연결 성공 ({})", self.base_url))
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

        let url = format!("{}/api/assets/bulk-upload-check", self.base_url);
        let assets: Vec<Value> = items
            .iter()
            .map(|(id, checksum)| json!({ "id": id, "checksum": checksum }))
            .collect();
        let payload = json!({ "assets": assets });

        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .post(&url)
                    .header("x-api-key", &self.api_key)
                    .json(&payload)
            })
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Immich bulk-upload-check 실패: HTTP {}",
                resp.status()
            ));
        }

        let body: Value = resp.json().await?;
        if let Some(results) = body.get("results").and_then(|v| v.as_array()) {
            for r in results {
                let id = r
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let action = r
                    .get("action")
                    .and_then(|v| v.as_str())
                    .unwrap_or("accept");
                let asset_id = r
                    .get("assetId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "existing_immich_asset".to_string());

                if action == "reject" {
                    out.insert(id, Some(asset_id));
                } else {
                    out.insert(id, None);
                }
            }
        }

        Ok(out)
    }

    pub async fn verify_hash_exists(&self, resource_id: &str, sha1_hash: &str) -> Result<bool> {
        let items = vec![(resource_id.to_string(), sha1_hash.to_string())];
        let map = self.check_bulk_hashes(&items).await?;
        Ok(map.get(resource_id).and_then(|v| v.as_ref()).is_some())
    }

    pub async fn upload_asset(
        &self,
        file_path: &Path,
        filename: &str,
        device_asset_id: &str,
        created_at: &str,
        modified_at: &str,
    ) -> Result<(String, String)> {
        let url = format!("{}/api/assets", self.base_url);
        let bytes = tokio::fs::read(file_path).await?;
        let filename_owned = filename.to_string();
        let device_asset_owned = device_asset_id.to_string();
        let created_owned = created_at.to_string();
        let modified_owned = modified_at.to_string();

        self.limiter.acquire().await;
        let part = Part::bytes(bytes).file_name(filename_owned);
        let form = Form::new()
            .text("deviceAssetId", device_asset_owned)
            .text("deviceId", "mybox-immich-sync")
            .text("fileCreatedAt", created_owned)
            .text("fileModifiedAt", modified_owned)
            .part("assetData", part);

        let resp = self
            .client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .multipart(form)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Immich 자산 업로드 실패 (HTTP {}): {}",
                status,
                text
            ));
        }

        let body: Value = resp.json().await.unwrap_or_else(|_| json!({}));
        let id = body
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("immich_uploaded_id")
            .to_string();
        let status = body
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("created")
            .to_string();

        Ok((id, status))
    }
}
