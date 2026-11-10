use crate::config::normalize_mybox_api_base;
use crate::limiter::RateLimiter;
use crate::router::SourceType;
use crate::sources::{
    build_http_client, join_remote_path, stream_response_to_temp, DownloadedSourceFile, ListPage,
    RemoteFile, RemoteFolder, SourceProvider, DEFAULT_FALLBACK_TIMESTAMP,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde::Deserialize;
use std::path::Path;

const ENDPOINT_STORAGE: &str = "/v1/drive/storage";
const ENDPOINT_RESOURCES: &str = "/v1/drive/resources";
const ENDPOINT_FOLDERS_PREFIX: &str = "/v1/drive/folders";
const ENDPOINT_FILES_PREFIX: &str = "/v1/drive/files";
const DEFAULT_SORT_OLDEST: &str = "createdAt,asc";
const BYTES_PER_GB: f64 = 1024.0 * 1024.0 * 1024.0;
const MAX_ERR_BODY_CHARS: usize = 240;

const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'/')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}');

fn encode_path_segment(seg: &str) -> String {
    utf8_percent_encode(seg, PATH_SEGMENT_ENCODE_SET).to_string()
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
struct MyboxQuotaResponse {
    #[serde(default, alias = "usedBytes")]
    used: Option<i64>,
    #[serde(default, alias = "quotaBytes")]
    quota: Option<i64>,
    #[serde(default, alias = "errorMessage", alias = "message")]
    error_message: Option<String>,
    #[serde(default, alias = "errorCode", alias = "code")]
    error_code: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct MyboxResponseMeta {
    #[serde(default, alias = "nextCursor")]
    next_cursor: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct MyboxListResponse {
    #[serde(default, alias = "nextCursor")]
    next_cursor: Option<String>,
    #[serde(default, alias = "responseMetaData")]
    response_meta_data: Option<MyboxResponseMeta>,
    #[serde(default)]
    resources: Vec<MyboxEntryDto>,
    #[serde(default)]
    items: Vec<MyboxEntryDto>,
    #[serde(default)]
    folders: Vec<MyboxEntryDto>,
    #[serde(default)]
    files: Vec<MyboxEntryDto>,
    #[serde(default, alias = "errorMessage", alias = "message")]
    error_message: Option<String>,
    #[serde(default, alias = "errorCode", alias = "code")]
    error_code: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct MyboxDownloadUrlResponse {
    #[serde(default, alias = "downloadUrl", alias = "url")]
    download_url: String,
    #[serde(default, alias = "errorMessage", alias = "message")]
    error_message: Option<String>,
    #[serde(default, alias = "errorCode", alias = "code")]
    error_code: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct MyboxEntryDto {
    #[serde(
        default,
        alias = "resourceId",
        alias = "folderId",
        alias = "fileId",
        alias = "folder_id",
        alias = "id"
    )]
    resource_id: String,
    #[serde(default)]
    name: String,
    #[serde(default, alias = "isFolder")]
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

fn check_api_error_fields(
    error_code: Option<&str>,
    error_message: Option<&str>,
    context: &str,
) -> Result<()> {
    if error_code.is_some() || error_message.is_some() {
        let code = error_code.unwrap_or("UNKNOWN");
        let msg = error_message.unwrap_or("API 오류 응답");
        return Err(anyhow!("{} (코드 {}): {}", context, code, msg));
    }
    Ok(())
}

fn parse_quota_response(raw_json: &str) -> Result<String> {
    let quota_info: MyboxQuotaResponse = serde_json::from_str(raw_json).map_err(|e| {
        anyhow!(
            "MYBOX 용량 응답 JSON 파싱 실패 ({}){}",
            e,
            truncate_body(raw_json)
        )
    })?;

    check_api_error_fields(
        quota_info.error_code.as_deref(),
        quota_info.error_message.as_deref(),
        "MYBOX 인증/용량 조회 실패",
    )?;

    let Some(quota) = quota_info.quota else {
        return Err(anyhow!(
            "MYBOX 응답에 용량 필드(quotaBytes)가 없습니다. API Base URL(https://open-api.mybox.naver.com)과 PAT 토큰을 확인해 주세요{}",
            truncate_body(raw_json)
        ));
    };
    let used = quota_info.used.unwrap_or(0);

    if quota > 0 {
        Ok(format!(
            "네이버 MYBOX 연결 성공 (사용량: {:.2} GB / {:.2} GB)",
            used as f64 / BYTES_PER_GB,
            quota as f64 / BYTES_PER_GB
        ))
    } else {
        Ok("네이버 MYBOX 연결 성공".to_string())
    }
}

fn parse_list_response(raw_json: &str, folder_path: &str) -> Result<ListPage> {
    let dto: MyboxListResponse = serde_json::from_str(raw_json).map_err(|e| {
        anyhow!(
            "MYBOX 목록 응답 JSON 파싱 실패 (path={} / {}){}",
            folder_path,
            e,
            truncate_body(raw_json)
        )
    })?;

    check_api_error_fields(
        dto.error_code.as_deref(),
        dto.error_message.as_deref(),
        "MYBOX 폴더 목록 조회 실패",
    )?;

    let next_cursor = dto
        .response_meta_data
        .and_then(|m| m.next_cursor)
        .or(dto.next_cursor)
        .filter(|s| !s.is_empty());

    let mut page = ListPage {
        next_cursor,
        ..Default::default()
    };

    for item in dto.resources.into_iter().chain(dto.items) {
        if item.resource_id.trim().is_empty() || item.name.trim().is_empty() {
            continue;
        }
        if item.is_directory() {
            page.folders.push(item.into_folder(folder_path));
        } else {
            page.files.push(item.into_file(folder_path));
        }
    }
    for folder in dto.folders {
        if !folder.resource_id.trim().is_empty() && !folder.name.trim().is_empty() {
            page.folders.push(folder.into_folder(folder_path));
        }
    }
    for file in dto.files {
        if !file.resource_id.trim().is_empty() && !file.name.trim().is_empty() {
            page.files.push(file.into_file(folder_path));
        }
    }

    page.files.sort_by(|a, b| {
        a.created_time
            .cmp(&b.created_time)
            .then_with(|| a.modified_time.cmp(&b.modified_time))
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.resource_id.cmp(&b.resource_id))
    });

    Ok(page)
}

fn list_resources_path(folder_id: &str) -> String {
    let trimmed = folder_id.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("root") || trimmed == "/" {
        ENDPOINT_RESOURCES.to_string()
    } else {
        format!(
            "{}/{}/resources",
            ENDPOINT_FOLDERS_PREFIX,
            encode_path_segment(trimmed)
        )
    }
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
            api_base: normalize_mybox_api_base(&api_base),
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

        let url = self.endpoint(ENDPOINT_STORAGE);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).bearer_auth(&self.pat))
            .await?;

        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!(
                "MYBOX 인증 실패 (HTTP {}){}",
                status,
                truncate_body(&text)
            ));
        }

        parse_quota_response(&text)
    }

    async fn list_page(
        &self,
        folder_id: &str,
        folder_path: &str,
        cursor: Option<&str>,
    ) -> Result<ListPage> {
        let url = self.endpoint(&list_resources_path(folder_id));
        let mut query_params: Vec<(&str, &str)> = vec![("sort", DEFAULT_SORT_OLDEST)];
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

        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!(
                "MYBOX 폴더 목록 조회 실패 (folder_id={}, HTTP {}){}",
                folder_id,
                status,
                truncate_body(&text)
            ));
        }

        parse_list_response(&text, folder_path)
    }

    async fn download_to_temp(
        &self,
        resource_id: &str,
        filename: &str,
        tmp_dir: &Path,
    ) -> Result<DownloadedSourceFile> {
        let meta_path = format!(
            "{}/{}/download",
            ENDPOINT_FILES_PREFIX,
            encode_path_segment(resource_id)
        );
        let meta_url = self.endpoint(&meta_path);

        let meta_resp = self
            .limiter
            .send_with_retry(|| self.client.get(&meta_url).bearer_auth(&self.pat))
            .await?;

        let status = meta_resp.status();
        let body_text = meta_resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!(
                "MYBOX 다운로드 URL 발급 실패 ({}, HTTP {}){}",
                resource_id,
                status,
                truncate_body(&body_text)
            ));
        }

        let dl_info: MyboxDownloadUrlResponse = serde_json::from_str(&body_text).map_err(|e| {
            anyhow!(
                "MYBOX 다운로드 URL 응답 파싱 실패 ({}, {}){}",
                resource_id,
                e,
                truncate_body(&body_text)
            )
        })?;

        check_api_error_fields(
            dl_info.error_code.as_deref(),
            dl_info.error_message.as_deref(),
            "MYBOX 다운로드 URL 발급 실패",
        )?;

        let download_url = dl_info.download_url.trim();
        if download_url.is_empty() {
            return Err(anyhow!(
                "MYBOX 응답에 downloadUrl이 비어 있습니다 ({}){}",
                resource_id,
                truncate_body(&body_text)
            ));
        }

        let file_resp = self
            .limiter
            .send_with_retry(|| self.client.get(download_url))
            .await?;

        if !file_resp.status().is_success() {
            let dl_status = file_resp.status();
            let err_text = file_resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "MYBOX 파일 스트림 다운로드 실패 ({}, HTTP {}){}",
                resource_id,
                dl_status,
                truncate_body(&err_text)
            ));
        }

        stream_response_to_temp(
            file_resp,
            self.source_type().as_str(),
            resource_id,
            filename,
            tmp_dir,
        )
        .await
    }

    async fn delete_resource(&self, resource_id: &str) -> Result<()> {
        let path = format!(
            "{}/{}",
            ENDPOINT_RESOURCES,
            encode_path_segment(resource_id)
        );
        let url = self.endpoint(&path);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.delete(&url).bearer_auth(&self.pat))
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let err_text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "MYBOX 원격 휴지통 이동 실패 ({}, HTTP {}){}",
                resource_id,
                status,
                truncate_body(&err_text)
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mybox_openapi_storage_and_fake_200_error_rejection() {
        let valid_json = r#"{
            "quotaBytes": 32212254720,
            "usedBytes": 10737418240,
            "maxFileBytes": 10737418240
        }"#;
        let msg = parse_quota_response(valid_json).expect("valid MYBOX storage JSON must succeed");
        assert!(msg.contains("10.00 GB / 30.00 GB"));

        // Legacy apis.naver.com gateway returns HTTP 200 with errorCode "024" -> must fail!
        let gateway_err_json = r#"{
            "errorMessage": "Authentication failed (인증에 실패하였습니다.)",
            "errorCode": "024"
        }"#;
        assert!(parse_quota_response(gateway_err_json).is_err());

        // Empty JSON object without quotaBytes must also fail instead of pretending 0/0 success
        assert!(parse_quota_response("{}").is_err());
    }

    #[test]
    fn test_mybox_openapi_resources_list_and_endpoint_paths() {
        assert_eq!(list_resources_path("root"), "/v1/drive/resources");
        assert_eq!(list_resources_path("/"), "/v1/drive/resources");
        assert_eq!(
            list_resources_path("hV3sQ9pLzR2mT7kXwB5nDcF8gJ4yA6uE0o"),
            "/v1/drive/folders/hV3sQ9pLzR2mT7kXwB5nDcF8gJ4yA6uE0o/resources"
        );

        let raw_list_json = r#"{
            "fileCount": 1,
            "subFolderCount": 1,
            "resources": [
                {
                    "resourceId": "folder_res_1",
                    "parentId": "root_id",
                    "name": "2026년 사진첩",
                    "type": "folder",
                    "category": null,
                    "size": 0,
                    "createdAt": "2026-06-26T15:04:05+09:00",
                    "modifiedAt": "2026-06-26T15:04:05+09:00"
                },
                {
                    "resourceId": "file_res_1",
                    "parentId": "root_id",
                    "name": "IMG_0001.JPG",
                    "type": "file",
                    "category": "image",
                    "size": 1048576,
                    "createdAt": "2026-06-26T16:00:00+09:00",
                    "modifiedAt": "2026-06-26T16:00:00+09:00"
                }
            ],
            "responseMetaData": {
                "nextCursor": "Mjk5MTIzNDU2Nzg5MDE"
            }
        }"#;

        let page = parse_list_response(raw_list_json, "/").unwrap();
        assert_eq!(page.folders.len(), 1);
        assert_eq!(page.folders[0].folder_id, "folder_res_1");
        assert_eq!(page.folders[0].path, "/2026년 사진첩");

        assert_eq!(page.files.len(), 1);
        assert_eq!(page.files[0].resource_id, "file_res_1");
        assert_eq!(page.files[0].name, "IMG_0001.JPG");
        assert_eq!(page.files[0].size, 1048576);
        assert_eq!(page.files[0].created_time, "2026-06-26T16:00:00+09:00");
        assert_eq!(page.next_cursor.as_deref(), Some("Mjk5MTIzNDU2Nzg5MDE"));

        // Error JSON with HTTP 200 must fail in list_page too
        let gateway_err_json = r#"{"errorMessage":"Authentication failed","errorCode":"024"}"#;
        assert!(parse_list_response(gateway_err_json, "/").is_err());
    }
}
