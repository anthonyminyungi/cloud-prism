use crate::limiter::RateLimiter;
use crate::router::SourceType;
use crate::sources::{
    build_http_client, join_remote_path, stream_response_to_temp, DownloadedSourceFile, ListPage,
    RemoteFile, RemoteFolder, SourceProvider, DEFAULT_FALLBACK_TIMESTAMP,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::Path;

const ENDPOINT_QUOTA: &str = "/v1/quota";
const ENDPOINT_FILES: &str = "/v1/files";
const ENDPOINT_DOWNLOAD: &str = "/v1/files/download";
const ENDPOINT_TRASH: &str = "/v1/files/trash";
const BYTES_PER_GB: f64 = 1024.0 * 1024.0 * 1024.0;

#[derive(Debug, Default, Deserialize)]
struct MyboxQuotaResponse {
    #[serde(default)]
    used: i64,
    #[serde(default)]
    quota: i64,
}

#[derive(Debug, Default, Deserialize)]
struct MyboxListResponse {
    #[serde(default, alias = "nextCursor")]
    next_cursor: Option<String>,
    #[serde(default)]
    items: Vec<MyboxEntryDto>,
    #[serde(default)]
    folders: Vec<MyboxEntryDto>,
    #[serde(default)]
    files: Vec<MyboxEntryDto>,
}

#[derive(Debug, Default, Deserialize)]
struct MyboxEntryDto {
    #[serde(default, alias = "folder_id", alias = "id")]
    resource_id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    is_folder: Option<bool>,
    #[serde(default, rename = "type")]
    entry_type: Option<String>,
    #[serde(default)]
    size: i64,
    #[serde(default, alias = "createdAt")]
    created_time: Option<String>,
    #[serde(default, alias = "modifiedAt")]
    modified_time: Option<String>,
}

impl MyboxEntryDto {
    fn is_directory(&self) -> bool {
        self.is_folder.unwrap_or_else(|| {
            self.entry_type
                .as_deref()
                .map(|t| t.eq_ignore_ascii_case("folder"))
                .unwrap_or(false)
        })
    }

    fn into_folder(self, parent_path: &str) -> RemoteFolder {
        let path = join_remote_path(parent_path, &self.name);
        RemoteFolder {
            folder_id: self.resource_id,
            name: self.name,
            path,
        }
    }

    fn into_file(self, parent_path: &str) -> RemoteFile {
        let created = self
            .created_time
            .unwrap_or_else(|| DEFAULT_FALLBACK_TIMESTAMP.to_string());
        let modified = self.modified_time.unwrap_or_else(|| created.clone());
        RemoteFile {
            resource_id: self.resource_id,
            name: self.name,
            parent_path: parent_path.to_string(),
            size: self.size,
            created_time: created,
            modified_time: modified,
            sha1_hash: None,
        }
    }
}

#[derive(Debug, Serialize)]
struct MyboxTrashRequest<'a> {
    resource_ids: [&'a str; 1],
}

pub struct MyboxProvider {
    api_base: String,
    pat: String,
    client: reqwest::Client,
    limiter: RateLimiter,
}

impl MyboxProvider {
    pub fn new(api_base: String, pat: String, limiter: RateLimiter) -> Self {
        Self {
            api_base: api_base.trim_end_matches('/').to_string(),
            pat,
            client: build_http_client(),
            limiter,
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.api_base, path)
    }
}

#[async_trait]
impl SourceProvider for MyboxProvider {
    fn source_type(&self) -> SourceType {
        SourceType::Mybox
    }

    async fn test_connection(&self) -> Result<String> {
        if self.pat.trim().is_empty() {
            return Err(anyhow!(
                "MYBOX 개인 액세스 토큰(PAT)이 설정되지 않았습니다."
            ));
        }

        let url = self.endpoint(ENDPOINT_QUOTA);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).bearer_auth(&self.pat))
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("MYBOX 인증 실패 (HTTP {}): {}", status, text));
        }

        let quota_info: MyboxQuotaResponse = resp.json().await.unwrap_or_default();
        if quota_info.quota > 0 {
            Ok(format!(
                "네이버 MYBOX 연결 성공 (사용량: {:.2} GB / {:.2} GB)",
                quota_info.used as f64 / BYTES_PER_GB,
                quota_info.quota as f64 / BYTES_PER_GB
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
        let url = self.endpoint(ENDPOINT_FILES);
        let mut query_params = vec![("folder_id", folder_id)];
        if let Some(c) = cursor.filter(|c| !c.is_empty()) {
            query_params.push(("cursor", c));
        }

        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .get(&url)
                    .bearer_auth(&self.pat)
                    .query(&query_params)
            })
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "MYBOX list_page 실패 (folder_id={}): HTTP {}",
                folder_id,
                resp.status()
            ));
        }

        let dto: MyboxListResponse = resp.json().await?;
        let mut page = ListPage {
            next_cursor: dto.next_cursor.filter(|s| !s.is_empty()),
            ..Default::default()
        };

        for item in dto.items {
            if item.is_directory() {
                page.folders.push(item.into_folder(folder_path));
            } else {
                page.files.push(item.into_file(folder_path));
            }
        }
        for folder in dto.folders {
            page.folders.push(folder.into_folder(folder_path));
        }
        for file in dto.files {
            page.files.push(file.into_file(folder_path));
        }

        Ok(page)
    }

    async fn download_to_temp(
        &self,
        resource_id: &str,
        filename: &str,
        tmp_dir: &Path,
    ) -> Result<DownloadedSourceFile> {
        let url = self.endpoint(ENDPOINT_DOWNLOAD);
        let query = [("resource_id", resource_id)];
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).bearer_auth(&self.pat).query(&query))
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "MYBOX 파일 다운로드 실패 ({}): HTTP {}",
                resource_id,
                resp.status()
            ));
        }

        stream_response_to_temp(
            resp,
            self.source_type().as_str(),
            resource_id,
            filename,
            tmp_dir,
        )
        .await
    }

    async fn delete_resource(&self, resource_id: &str) -> Result<()> {
        let url = self.endpoint(ENDPOINT_TRASH);
        let payload = MyboxTrashRequest {
            resource_ids: [resource_id],
        };
        let resp = self
            .limiter
            .send_with_retry(|| self.client.post(&url).bearer_auth(&self.pat).json(&payload))
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
