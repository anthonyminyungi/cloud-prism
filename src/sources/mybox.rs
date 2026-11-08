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

pub struct MyboxProvider {
    api_base: String,
    pat: String,
    client: reqwest::Client,
    limiter: RateLimiter,
}

impl MyboxProvider {
    pub fn new(api_base: String, pat: String, limiter: RateLimiter) -> Self {
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            api_base: api_base.trim_end_matches('/').to_string(),
            pat,
            client,
            limiter,
        }
    }
}

#[async_trait]
impl SourceProvider for MyboxProvider {
    fn source_type(&self) -> &'static str {
        "mybox"
    }

    async fn test_connection(&self) -> Result<String> {
        if self.pat.trim().is_empty() {
            return Err(anyhow!("MYBOX 개인 액세스 토큰(PAT)이 설정되지 않았습니다."));
        }

        let url = format!("{}/v1/quota", self.api_base);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).bearer_auth(&self.pat))
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("MYBOX 인증 실패 (HTTP {}): {}", status, text));
        }

        let body: Value = resp.json().await.unwrap_or_else(|_| json!({}));
        let used = body.get("used").and_then(|v| v.as_i64()).unwrap_or(0);
        let quota = body.get("quota").and_then(|v| v.as_i64()).unwrap_or(0);
        if quota > 0 {
            let used_gb = used as f64 / (1024.0 * 1024.0 * 1024.0);
            let quota_gb = quota as f64 / (1024.0 * 1024.0 * 1024.0);
            Ok(format!(
                "네이버 MYBOX 연결 성공 (사용량: {:.2} GB / {:.2} GB)",
                used_gb, quota_gb
            ))
        } else {
            Ok("네이버 MYBOX 연결 성공".to_string())
        }
    }

    async fn list_page(
        &self,
        folder_id: &str,
        folder_path: &str,
        cursor: Option<&str>,
    ) -> Result<ListPage> {
        let mut url = format!("{}/v1/files?folder_id={}", self.api_base, folder_id);
        if let Some(c) = cursor {
            if !c.is_empty() {
                url.push_str(&format!("&cursor={}", c));
            }
        }

        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).bearer_auth(&self.pat))
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "MYBOX list_page 실패 (folder_id={}): HTTP {}",
                folder_id,
                resp.status()
            ));
        }

        let body: Value = resp.json().await?;
        let mut page = ListPage::default();

        if let Some(next) = body
            .get("next_cursor")
            .or_else(|| body.get("nextCursor"))
            .and_then(|v| v.as_str())
        {
            if !next.is_empty() {
                page.next_cursor = Some(next.to_string());
            }
        }

        // Support both unified `items` array and separate `folders`/`files` arrays
        if let Some(items) = body.get("items").and_then(|v| v.as_array()) {
            for item in items {
                let is_folder = item
                    .get("is_folder")
                    .and_then(|v| v.as_bool())
                    .or_else(|| {
                        item.get("type")
                            .and_then(|v| v.as_str())
                            .map(|t| t.eq_ignore_ascii_case("folder"))
                    })
                    .unwrap_or(false);

                let id = item
                    .get("resource_id")
                    .or_else(|| item.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let name = item
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();

                if is_folder {
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
                    let size = item.get("size").and_then(|v| v.as_i64()).unwrap_or(0);
                    let created_time = item
                        .get("created_time")
                        .or_else(|| item.get("createdAt"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("2026-01-01T00:00:00Z")
                        .to_string();
                    let modified_time = item
                        .get("modified_time")
                        .or_else(|| item.get("modifiedAt"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(&created_time)
                        .to_string();

                    page.files.push(RemoteFile {
                        resource_id: id,
                        name,
                        parent_path: folder_path.to_string(),
                        size,
                        created_time,
                        modified_time,
                        sha1_hash: None,
                    });
                }
            }
        }

        if let Some(folders) = body.get("folders").and_then(|v| v.as_array()) {
            for f in folders {
                let id = f
                    .get("folder_id")
                    .or_else(|| f.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let name = f
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
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
            }
        }

        if let Some(files) = body.get("files").and_then(|v| v.as_array()) {
            for f in files {
                let id = f
                    .get("resource_id")
                    .or_else(|| f.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let name = f
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let size = f.get("size").and_then(|v| v.as_i64()).unwrap_or(0);
                let created_time = f
                    .get("created_time")
                    .and_then(|v| v.as_str())
                    .unwrap_or("2026-01-01T00:00:00Z")
                    .to_string();
                let modified_time = f
                    .get("modified_time")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&created_time)
                    .to_string();

                page.files.push(RemoteFile {
                    resource_id: id,
                    name,
                    parent_path: folder_path.to_string(),
                    size,
                    created_time,
                    modified_time,
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
        tokio::fs::create_dir_all(tmp_dir).await?;
        let safe_name = filename.replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_");
        let temp_path = tmp_dir.join(format!("mybox_{}_{}", resource_id, safe_name));
        let temp_guard = TempFileGuard::new(temp_path);

        let url = format!(
            "{}/v1/files/download?resource_id={}",
            self.api_base, resource_id
        );
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).bearer_auth(&self.pat))
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "MYBOX 파일 다운로드 실패 ({}): HTTP {}",
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
        let url = format!("{}/v1/files/trash", self.api_base);
        let payload = json!({ "resource_ids": [resource_id] });
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .post(&url)
                    .bearer_auth(&self.pat)
                    .json(&payload)
            })
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "MYBOX 원격 휴지통 이동 실패 ({}): HTTP {}",
                resource_id,
                resp.status()
            ));
        }
        Ok(())
    }
}
