use crate::limiter::RateLimiter;
use crate::sources::{
    DownloadedSourceFile, ListPage, RemoteFile, RemoteFolder, SourceProvider, TempFileGuard,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{json, Value};
use sha1::{Digest, Sha1};
use std::path::Path;
use tokio::io::AsyncWriteExt;

pub struct GoogleDriveProvider {
    api_base: String,
    access_token: String,
    client: reqwest::Client,
    limiter: RateLimiter,
}

impl GoogleDriveProvider {
    pub fn new(api_base: String, access_token: String, limiter: RateLimiter) -> Self {
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            api_base: api_base.trim_end_matches('/').to_string(),
            access_token,
            client,
            limiter,
        }
    }
}

#[async_trait]
impl SourceProvider for GoogleDriveProvider {
    fn source_type(&self) -> &'static str {
        "gdrive"
    }

    async fn test_connection(&self) -> Result<String> {
        if self.access_token.trim().is_empty() {
            return Err(anyhow!("Google Drive 액세스 토큰이 설정되지 않았습니다."));
        }

        let url = format!("{}/about?fields=user,storageQuota", self.api_base);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).bearer_auth(&self.access_token))
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Google Drive 인증 실패 (HTTP {})",
                resp.status()
            ));
        }

        let body: Value = resp.json().await.unwrap_or_else(|_| json!({}));
        let email = body
            .pointer("/user/emailAddress")
            .and_then(|v| v.as_str())
            .unwrap_or("Authenticated User");
        Ok(format!("Google Drive 연결 성공 ({})", email))
    }

    async fn list_page(
        &self,
        folder_id: &str,
        folder_path: &str,
        cursor: Option<&str>,
    ) -> Result<ListPage> {
        let mut url = format!("{}/files?folder_id={}", self.api_base, folder_id);
        if let Some(c) = cursor {
            if !c.is_empty() {
                url.push_str(&format!("&pageToken={}", c));
            }
        }

        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).bearer_auth(&self.access_token))
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Google Drive list_page 실패 (folder_id={}): HTTP {}",
                folder_id,
                resp.status()
            ));
        }

        let body: Value = resp.json().await?;
        let mut page = ListPage::default();

        if let Some(next) = body.get("nextPageToken").and_then(|v| v.as_str()) {
            if !next.is_empty() {
                page.next_cursor = Some(next.to_string());
            }
        }

        if let Some(files) = body.get("files").and_then(|v| v.as_array()) {
            for item in files {
                let id = item
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let name = item
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let mime = item
                    .get("mimeType")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();

                if mime == "application/vnd.google-apps.folder" {
                    let child_path = if folder_path == "/" {
                        format!("/{}", name)
                    } else {
                        format!("{}/{}", folder_path.trim_end_matches('/'), name)
                    };
                    page.folders.push(RemoteFolder {
                        folder_id: id,
                        name,
                        path: child_path,
                    });
                } else {
                    let size = item
                        .get("size")
                        .and_then(|v| {
                            v.as_i64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                        })
                        .unwrap_or(0);
                    let created_time = item
                        .get("createdTime")
                        .and_then(|v| v.as_str())
                        .unwrap_or("2026-01-01T00:00:00Z")
                        .to_string();
                    let modified_time = item
                        .get("modifiedTime")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&created_time)
                        .to_string();
                    let sha1_hash = item
                        .get("sha1Checksum")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());

                    page.files.push(RemoteFile {
                        resource_id: id,
                        name,
                        parent_path: folder_path.to_string(),
                        size,
                        created_time,
                        modified_time,
                        sha1_hash,
                    });
                }
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
        tokio::fs::create_dir_all(tmp_dir).await?;
        let safe_name = filename.replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_");
        let temp_path = tmp_dir.join(format!("gdrive_{}_{}", resource_id, safe_name));
        let temp_guard = TempFileGuard::new(temp_path);

        let url = format!("{}/files/{}?alt=media", self.api_base, resource_id);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).bearer_auth(&self.access_token))
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Google Drive 다운로드 실패 ({}): HTTP {}",
                resource_id,
                resp.status()
            ));
        }

        let mut file = tokio::fs::File::create(temp_guard.path()).await?;
        let mut hasher = Sha1::new();
        let mut total_bytes: u64 = 0;
        let mut stream = resp.bytes_stream();

        while let Some(chunk_res) = stream.next().await {
            let chunk = chunk_res?;
            hasher.update(&chunk);
            file.write_all(&chunk).await?;
            total_bytes += chunk.len() as u64;
        }
        file.flush().await?;
        drop(file);

        let sha1_hash = hex::encode(hasher.finalize());
        Ok(DownloadedSourceFile {
            temp_guard,
            sha1_hash,
            size: total_bytes,
            created_time_override: None,
            modified_time_override: None,
        })
    }

    async fn delete_resource(&self, resource_id: &str) -> Result<()> {
        let url = format!("{}/files/{}", self.api_base, resource_id);
        let payload = json!({ "trashed": true });
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .patch(&url)
                    .bearer_auth(&self.access_token)
                    .json(&payload)
            })
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Google Drive 휴지통 이동 실패 ({}): HTTP {}",
                resource_id,
                resp.status()
            ));
        }
        Ok(())
    }
}
