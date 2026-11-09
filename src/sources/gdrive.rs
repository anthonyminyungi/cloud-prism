use crate::limiter::RateLimiter;
use crate::router::SourceType;
use crate::sources::{
    build_http_client, join_remote_path, sanitize_filename, stream_response_to_temp,
    DownloadedSourceFile, ListPage, RemoteFile, RemoteFolder, SourceProvider,
    DEFAULT_FALLBACK_TIMESTAMP,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use reqwest::StatusCode;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::Mutex;

const ENDPOINT_ABOUT: &str = "/about";
const ABOUT_FIELDS: &str = "user,storageQuota";
const ENDPOINT_FILES: &str = "/files";
const LIST_FIELDS: &str = "nextPageToken,files(id,name,mimeType,size,quotaBytesUsed,ownedByMe,createdTime,modifiedTime,sha1Checksum,exportLinks)";
const META_FIELDS: &str = "mimeType,exportLinks";
const PAGE_SIZE: &str = "200";
const FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";
const SHORTCUT_MIME_TYPE: &str = "application/vnd.google-apps.shortcut";
const GOOGLE_APPS_MIME_PREFIX: &str = "application/vnd.google-apps.";
const MAX_ERR_BODY_CHARS: usize = 240;

#[derive(Debug, Clone)]
struct ExportSpec {
    export_mime: &'static str,
    direct_link: Option<String>,
}

fn workspace_export_target(mime_type: &str) -> Option<(&'static str, &'static str)> {
    match mime_type {
        "application/vnd.google-apps.document" => Some((
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".docx",
        )),
        "application/vnd.google-apps.spreadsheet" => Some((
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xlsx",
        )),
        "application/vnd.google-apps.presentation" => Some((
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            ".pptx",
        )),
        "application/vnd.google-apps.drawing" => Some(("application/pdf", ".pdf")),
        _ => None,
    }
}

fn ensure_export_extension(sanitized_name: &str, ext: &str) -> String {
    if sanitized_name
        .to_ascii_lowercase()
        .ends_with(&ext.to_ascii_lowercase())
    {
        sanitized_name.to_string()
    } else {
        format!("{}{}", sanitized_name, ext)
    }
}

fn truncate_body(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let snippet: String = trimmed.chars().take(MAX_ERR_BODY_CHARS).collect();
    format!(" - {}", snippet.replace('\n', " "))
}

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
    #[serde(default, deserialize_with = "deserialize_flexible_i64")]
    quota_bytes_used: i64,
    #[serde(default)]
    owned_by_me: Option<bool>,
    #[serde(default)]
    created_time: Option<String>,
    #[serde(default)]
    modified_time: Option<String>,
    #[serde(default)]
    sha1_checksum: Option<String>,
    #[serde(default)]
    export_links: Option<HashMap<String, String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GDriveFileMetaDto {
    #[serde(default)]
    mime_type: String,
    #[serde(default)]
    export_links: Option<HashMap<String, String>>,
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
    export_specs: Mutex<HashMap<String, ExportSpec>>,
}

impl GoogleDriveProvider {
    pub fn new(api_base: String, access_token: String, limiter: RateLimiter) -> Self {
        Self {
            api_base: api_base.trim_end_matches('/').to_string(),
            access_token,
            client: build_http_client(),
            limiter,
            export_specs: Mutex::new(HashMap::new()),
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.api_base, path)
    }

    async fn resolve_export_spec_from_remote(&self, resource_id: &str) -> Option<ExportSpec> {
        let url = format!("{}/{}", self.endpoint(ENDPOINT_FILES), resource_id);
        let query = [("fields", META_FIELDS)];
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .get(&url)
                    .bearer_auth(&self.access_token)
                    .query(&query)
            })
            .await
            .ok()?;

        if !resp.status().is_success() {
            return None;
        }

        let mut meta: GDriveFileMetaDto = resp.json().await.ok()?;
        let (export_mime, _ext) = workspace_export_target(&meta.mime_type)?;
        let direct_link = meta
            .export_links
            .as_mut()
            .and_then(|m| m.remove(export_mime));
        let spec = ExportSpec {
            export_mime,
            direct_link,
        };
        self.export_specs
            .lock()
            .await
            .insert(resource_id.to_string(), spec.clone());
        Some(spec)
    }

    async fn export_to_temp(
        &self,
        resource_id: &str,
        filename: &str,
        tmp_dir: &Path,
        spec: &ExportSpec,
    ) -> Result<DownloadedSourceFile> {
        let url = format!("{}/{}/export", self.endpoint(ENDPOINT_FILES), resource_id);
        let query = [("mimeType", spec.export_mime)];
        let mut resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .get(&url)
                    .bearer_auth(&self.access_token)
                    .query(&query)
            })
            .await?;

        // Fallback to exportLinks URL if >10MB export limit is hit on standard REST export endpoint
        if resp.status() == StatusCode::FORBIDDEN {
            if let Some(ref direct_url) = spec.direct_link {
                let fallback_resp = self
                    .limiter
                    .send_with_retry(|| self.client.get(direct_url).bearer_auth(&self.access_token))
                    .await?;
                if fallback_resp.status().is_success() {
                    resp = fallback_resp;
                }
            }
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Google Drive 문서 변환 다운로드 실패 ({}): HTTP {}{}",
                resource_id,
                status,
                truncate_body(&body)
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
        let escaped_parent = folder_id.replace('\\', "\\\\").replace('\'', "\\'");
        let q = format!(
            "'{}' in parents and 'me' in owners and trashed = false and mimeType != '{}'",
            escaped_parent, SHORTCUT_MIME_TYPE
        );
        let mut query_params = vec![
            ("q", q.as_str()),
            ("fields", LIST_FIELDS),
            ("pageSize", PAGE_SIZE),
        ];
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
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Google Drive list_page 실패 (folder_id={}): HTTP {}{}",
                folder_id,
                status,
                truncate_body(&body)
            ));
        }

        let dto: GDriveListResponse = resp.json().await?;
        let mut page = ListPage {
            next_cursor: dto.next_page_token.filter(|s| !s.is_empty()),
            ..Default::default()
        };

        for mut item in dto.files {
            // Strictly accept only files/folders owned by the authenticated user
            if !item.owned_by_me.unwrap_or(false) {
                continue;
            }

            let sanitized = sanitize_filename(item.name.trim());
            let clean_name = if sanitized.is_empty() {
                item.id.clone()
            } else {
                sanitized
            };

            if item.mime_type == FOLDER_MIME_TYPE {
                let child_path = join_remote_path(folder_path, &clean_name);
                page.folders.push(RemoteFolder {
                    folder_id: item.id,
                    name: clean_name,
                    path: child_path,
                });
                continue;
            }

            let created = item
                .created_time
                .unwrap_or_else(|| DEFAULT_FALLBACK_TIMESTAMP.to_string());
            let modified = item.modified_time.unwrap_or_else(|| created.clone());

            if let Some((export_mime, ext)) = workspace_export_target(&item.mime_type) {
                let direct_link = item
                    .export_links
                    .as_mut()
                    .and_then(|m| m.remove(export_mime));
                self.export_specs.lock().await.insert(
                    item.id.clone(),
                    ExportSpec {
                        export_mime,
                        direct_link,
                    },
                );
                let exported_name = ensure_export_extension(&clean_name, ext);
                page.files.push(RemoteFile {
                    resource_id: item.id,
                    name: exported_name,
                    parent_path: folder_path.to_string(),
                    size: item.size,
                    created_time: created,
                    modified_time: modified,
                    sha1_hash: None,
                });
                continue;
            }

            // Skip non-exportable Google Workspace objects (Forms, Shortcuts, Maps, Sites, etc.)
            if item.mime_type.starts_with(GOOGLE_APPS_MIME_PREFIX) {
                continue;
            }

            // For binary files, strictly require actual storage quota consumption (> 0 bytes)
            if item.quota_bytes_used <= 0 {
                continue;
            }

            let effective_size = if item.size > 0 {
                item.size
            } else {
                item.quota_bytes_used
            };

            page.files.push(RemoteFile {
                resource_id: item.id,
                name: clean_name,
                parent_path: folder_path.to_string(),
                size: effective_size,
                created_time: created,
                modified_time: modified,
                sha1_hash: item.sha1_checksum,
            });
        }

        Ok(page)
    }

    async fn download_to_temp(
        &self,
        resource_id: &str,
        filename: &str,
        tmp_dir: &Path,
    ) -> Result<DownloadedSourceFile> {
        let cached_spec = self.export_specs.lock().await.get(resource_id).cloned();
        if let Some(ref spec) = cached_spec {
            return self
                .export_to_temp(resource_id, filename, tmp_dir, spec)
                .await;
        }

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

        if resp.status() == StatusCode::FORBIDDEN {
            if let Some(spec) = self.resolve_export_spec_from_remote(resource_id).await {
                return self
                    .export_to_temp(resource_id, filename, tmp_dir, &spec)
                    .await;
            }
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Google Drive 다운로드 실패 ({}): HTTP {}{}",
                resource_id,
                status,
                truncate_body(&body)
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
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Google Drive 휴지통 이동 실패 ({}): HTTP {}{}",
                resource_id,
                status,
                truncate_body(&body)
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workspace_export_mapping_and_extension_dedup() {
        assert_eq!(
            workspace_export_target("application/vnd.google-apps.document"),
            Some((
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ".docx"
            ))
        );
        assert_eq!(
            workspace_export_target("application/vnd.google-apps.spreadsheet"),
            Some((
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ".xlsx"
            ))
        );
        assert_eq!(
            workspace_export_target("application/vnd.google-apps.presentation"),
            Some((
                "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                ".pptx"
            ))
        );
        assert_eq!(
            workspace_export_target("application/vnd.google-apps.drawing"),
            Some(("application/pdf", ".pdf"))
        );
        assert_eq!(
            workspace_export_target("application/vnd.google-apps.form"),
            None
        );

        assert_eq!(ensure_export_extension("회의록", ".docx"), "회의록.docx");
        assert_eq!(
            ensure_export_extension("회의록.DOCX", ".docx"),
            "회의록.DOCX"
        );
    }
}
