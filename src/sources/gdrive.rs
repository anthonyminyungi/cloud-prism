use crate::limiter::RateLimiter;
use crate::router::SourceType;
use crate::sources::{
    build_http_client, join_remote_path, stream_response_to_temp, DownloadedSourceFile, ListPage,
    RemoteFile, RemoteFolder, SourceProvider, DEFAULT_FALLBACK_TIMESTAMP,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::path::Path;

const ENDPOINT_ABOUT: &str = "/about";
const ABOUT_FIELDS: &str = "user,storageQuota";
const ENDPOINT_FILES: &str = "/files";
const FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";

#[derive(Debug, Default, Deserialize)]
struct GDriveAboutResponse {
    #[serde(default)]
    user: Option<GDriveUserDto>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GDriveUserDto {
    #[serde(default)]
    email_address: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GDriveListResponse {
    #[serde(default)]
    next_page_token: Option<String>,
    #[serde(default)]
    files: Vec<GDriveFileDto>,
}

fn deserialize_flexible_i64<'de, D>(deserializer: D) -> std::result::Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let val = Option::<Value>::deserialize(deserializer)?;
    let parsed = match val {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(Value::String(s)) => s.parse::<i64>().unwrap_or(0),
        _ => 0,
    };
    Ok(parsed)
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GDriveFileDto {
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    mime_type: String,
    #[serde(default, deserialize_with = "deserialize_flexible_i64")]
    size: i64,
    #[serde(default)]
    created_time: Option<String>,
    #[serde(default)]
    modified_time: Option<String>,
    #[serde(default)]
    sha1_checksum: Option<String>,
}

#[derive(Debug, Serialize)]
struct GDriveTrashPatch {
    trashed: bool,
}

pub struct GoogleDriveProvider {
    api_base: String,
    access_token: String,
    client: reqwest::Client,
    limiter: RateLimiter,
}

impl GoogleDriveProvider {
    pub fn new(api_base: String, access_token: String, limiter: RateLimiter) -> Self {
        Self {
            api_base: api_base.trim_end_matches('/').to_string(),
            access_token,
            client: build_http_client(),
            limiter,
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.api_base, path)
    }
}

#[async_trait]
impl SourceProvider for GoogleDriveProvider {
    fn source_type(&self) -> SourceType {
        SourceType::Gdrive
    }

    async fn test_connection(&self) -> Result<String> {
        if self.access_token.trim().is_empty() {
            return Err(anyhow!("Google Drive 액세스 토큰이 설정되지 않았습니다."));
        }

        let url = self.endpoint(ENDPOINT_ABOUT);
        let query = [("fields", ABOUT_FIELDS)];
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .get(&url)
                    .bearer_auth(&self.access_token)
                    .query(&query)
            })
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!("Google Drive 인증 실패 (HTTP {})", resp.status()));
        }

        let about: GDriveAboutResponse = resp.json().await.unwrap_or_default();
        let email = about
            .user
            .and_then(|u| u.email_address)
            .unwrap_or_else(|| "Authenticated User".to_string());
        Ok(format!("Google Drive 연결 성공 ({})", email))
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
            query_params.push(("pageToken", c));
        }

        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .get(&url)
                    .bearer_auth(&self.access_token)
                    .query(&query_params)
            })
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Google Drive list_page 실패 (folder_id={}): HTTP {}",
                folder_id,
                resp.status()
            ));
        }

        let dto: GDriveListResponse = resp.json().await?;
        let mut page = ListPage {
            next_cursor: dto.next_page_token.filter(|s| !s.is_empty()),
            ..Default::default()
        };

        for item in dto.files {
            if item.mime_type == FOLDER_MIME_TYPE {
                let child_path = join_remote_path(folder_path, &item.name);
                page.folders.push(RemoteFolder {
                    folder_id: item.id,
                    name: item.name,
                    path: child_path,
                });
            } else {
                let created = item
                    .created_time
                    .unwrap_or_else(|| DEFAULT_FALLBACK_TIMESTAMP.to_string());
                let modified = item.modified_time.unwrap_or_else(|| created.clone());
                page.files.push(RemoteFile {
                    resource_id: item.id,
                    name: item.name,
                    parent_path: folder_path.to_string(),
                    size: item.size,
                    created_time: created,
                    modified_time: modified,
                    sha1_hash: item.sha1_checksum,
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
        let url = format!("{}/{}", self.endpoint(ENDPOINT_FILES), resource_id);
        let query = [("alt", "media")];
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .get(&url)
                    .bearer_auth(&self.access_token)
                    .query(&query)
            })
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Google Drive 다운로드 실패 ({}): HTTP {}",
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
        let url = format!("{}/{}", self.endpoint(ENDPOINT_FILES), resource_id);
        let payload = GDriveTrashPatch { trashed: true };
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
