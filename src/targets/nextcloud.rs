use crate::config::normalize_nextcloud_url;
use crate::db::FileRecord;
use crate::limiter::RateLimiter;
use crate::router::{SourceType, SyncResultKind, TargetType};
use crate::sources::{
    build_target_http_client, join_remote_path, sanitize_filename, stream_response_to_temp,
    DownloadedSourceFile,
};
use crate::targets::{TargetProvider, UploadContext, UploadOutcome};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::{Method, StatusCode};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

const WEBDAV_PREFIX: &str = "/remote.php/dav/files";
const HEADER_MTIME: &str = "X-OC-MTime";
const HEADER_DEPTH: &str = "Depth";
const MAX_ERR_BODY_CHARS: usize = 200;

fn truncate_dav_err(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let snippet: String = trimmed.chars().take(MAX_ERR_BODY_CHARS).collect();
    format!(" - {}", snippet.replace('\n', " "))
}

const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'?')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'{')
    .add(b'}');

fn encode_dav_path(raw_path: &str) -> String {
    let mut encoded = String::new();
    for seg in raw_path.split('/').filter(|s| !s.is_empty()) {
        encoded.push('/');
        encoded.push_str(&utf8_percent_encode(seg, PATH_SEGMENT_ENCODE_SET).to_string());
    }
    if encoded.is_empty() {
        "/".to_string()
    } else {
        encoded
    }
}

pub struct NextcloudTarget {
    base_url: String,
    user: String,
    app_password: String,
    base_dir: String,
    client: reqwest::Client,
    limiter: RateLimiter,
    created_dirs: Arc<Mutex<HashSet<String>>>,
    claimed_paths: Arc<Mutex<HashMap<String, String>>>,
}

impl NextcloudTarget {
    pub fn new(
        base_url: String,
        user: String,
        app_password: String,
        base_dir: String,
        limiter: RateLimiter,
    ) -> Self {
        let normalized_dir = join_remote_path("/", &base_dir);
        Self {
            base_url: normalize_nextcloud_url(&base_url),
            user: user.trim().to_string(),
            app_password,
            base_dir: normalized_dir,
            client: build_target_http_client(false),
            limiter,
            created_dirs: Arc::new(Mutex::new(HashSet::new())),
            claimed_paths: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn dav_root(&self) -> String {
        let encoded_user = utf8_percent_encode(&self.user, PATH_SEGMENT_ENCODE_SET);
        format!("{}{}/{}", self.base_url, WEBDAV_PREFIX, encoded_user)
    }

    fn dav_url(&self, remote_path: &str) -> String {
        format!("{}{}", self.dav_root(), encode_dav_path(remote_path))
    }

    pub async fn ensure_folder_tree(
        &self,
        source_type: SourceType,
        parent_path: &str,
    ) -> Result<String> {
        let mut combined = join_remote_path(&self.base_dir, source_type.nextcloud_folder_name());
        for seg in parent_path.split('/').filter(|s| !s.is_empty()) {
            let safe_seg = sanitize_filename(seg.trim());
            if !safe_seg.is_empty() {
                combined = join_remote_path(&combined, &safe_seg);
            }
        }

        let mkcol = Method::from_bytes(b"MKCOL").unwrap();
        let mut current = String::new();

        // Hold directory creation lock so concurrent workers never race MKCOL on the same folder
        let mut created = self.created_dirs.lock().await;

        for seg in combined.split('/').filter(|s| !s.is_empty()) {
            current.push('/');
            current.push_str(seg);

            if created.contains(&current) {
                continue;
            }

            let url = self.dav_url(&current);
            let resp = self
                .limiter
                .send_with_retry(|| {
                    self.client
                        .request(mkcol.clone(), &url)
                        .basic_auth(&self.user, Some(&self.app_password))
                })
                .await?;

            // 201 Created or 405 Method Not Allowed (already exists) are both valid
            if resp.status().is_success() || resp.status() == StatusCode::METHOD_NOT_ALLOWED {
                created.insert(current.clone());
            } else {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "Nextcloud MKCOL 실패 ({}): HTTP {}{}",
                    current,
                    status,
                    truncate_dav_err(&body)
                ));
            }
        }

        Ok(combined)
    }

    async fn claim_unique_remote_path(
        &self,
        dir_path: &str,
        filename: &str,
        resource_id: Option<&str>,
    ) -> String {
        let safe_name = sanitize_filename(filename.trim());
        let safe_name = if safe_name.is_empty() {
            "unnamed_file".to_string()
        } else {
            safe_name
        };
        let base_remote = join_remote_path(dir_path, &safe_name);

        let Some(res_id) = resource_id.filter(|id| !id.is_empty()) else {
            return base_remote;
        };

        let mut claimed = self.claimed_paths.lock().await;
        match claimed.get(&base_remote) {
            None => {
                claimed.insert(base_remote.clone(), res_id.to_string());
                base_remote
            }
            Some(existing_owner) if existing_owner == res_id => base_remote,
            Some(_) => {
                let safe_id = sanitize_filename(res_id);
                let disambiguated_name = match safe_name.rsplit_once('.') {
                    Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() => {
                        format!("{} ({}).{}", stem, safe_id, ext)
                    }
                    _ => format!("{} ({})", safe_name, safe_id),
                };
                let alt_remote = join_remote_path(dir_path, &disambiguated_name);
                claimed.insert(alt_remote.clone(), res_id.to_string());
                alt_remote
            }
        }
    }

    pub async fn upload_file(
        &self,
        source_type: SourceType,
        file_path: &Path,
        parent_path: &str,
        filename: &str,
        modified_time_iso: &str,
    ) -> Result<String> {
        self.upload_file_with_resource(
            source_type,
            file_path,
            parent_path,
            filename,
            modified_time_iso,
            None,
        )
        .await
    }

    async fn upload_file_with_resource(
        &self,
        source_type: SourceType,
        file_path: &Path,
        parent_path: &str,
        filename: &str,
        modified_time_iso: &str,
        resource_id: Option<&str>,
    ) -> Result<String> {
        let dir_path = self.ensure_folder_tree(source_type, parent_path).await?;
        let remote_path = self
            .claim_unique_remote_path(&dir_path, filename, resource_id)
            .await;
        let url = self.dav_url(&remote_path);

        let file_len = tokio::fs::metadata(file_path).await?.len();
        let mtime_epoch = DateTime::parse_from_rfc3339(modified_time_iso)
            .ok()
            .map(|dt| dt.timestamp())
            .filter(|&ts| ts > 0)
            .unwrap_or_else(|| Utc::now().timestamp())
            .to_string();

        let resp = self
            .limiter
            .send_with_retry(|| {
                let std_file =
                    std::fs::File::open(file_path).expect("temp file must exist during upload");
                let async_file = tokio::fs::File::from_std(std_file);
                let stream = tokio_util::io::ReaderStream::new(async_file);
                self.client
                    .put(&url)
                    .basic_auth(&self.user, Some(&self.app_password))
                    .header(HEADER_MTIME, &mtime_epoch)
                    .header(reqwest::header::CONTENT_LENGTH, file_len)
                    .body(reqwest::Body::wrap_stream(stream))
            })
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Nextcloud 파일 업로드 실패 ({}): HTTP {}{}",
                remote_path,
                status,
                truncate_dav_err(&body)
            ));
        }

        Ok(remote_path)
    }

    pub async fn check_file_exists(
        &self,
        remote_target_path: &str,
        expected_size: i64,
    ) -> Result<bool> {
        let url = self.dav_url(remote_target_path);

        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .head(&url)
                    .basic_auth(&self.user, Some(&self.app_password))
            })
            .await?;

        if !resp.status().is_success() {
            return Ok(false);
        }

        if let Some(remote_size) = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok())
        {
            return Ok(remote_size == expected_size);
        }

        Ok(true)
    }

    pub async fn download_file_to_temp(
        &self,
        remote_target_path: &str,
        filename: &str,
        tmp_dir: &Path,
    ) -> Result<DownloadedSourceFile> {
        let url = self.dav_url(remote_target_path);
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .get(&url)
                    .basic_auth(&self.user, Some(&self.app_password))
            })
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Nextcloud 파일 다운로드 실패 ({}): HTTP {}{}",
                remote_target_path,
                status,
                truncate_dav_err(&body)
            ));
        }

        stream_response_to_temp(resp, "nextcloud", remote_target_path, filename, tmp_dir).await
    }

    pub async fn delete_file(&self, remote_target_path: &str) -> Result<()> {
        let url = self.dav_url(remote_target_path);
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .delete(&url)
                    .basic_auth(&self.user, Some(&self.app_password))
            })
            .await?;

        if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        Err(anyhow!(
            "Nextcloud 파일 삭제 실패 ({}): HTTP {}{}",
            remote_target_path,
            status,
            truncate_dav_err(&body)
        ))
    }
}

#[async_trait]
impl TargetProvider for NextcloudTarget {
    fn target_type(&self) -> TargetType {
        TargetType::Nextcloud
    }

    async fn test_connection(&self) -> Result<String> {
        if self.base_url.trim().is_empty()
            || self.user.trim().is_empty()
            || self.app_password.trim().is_empty()
        {
            return Err(anyhow!(
                "Nextcloud URL, 사용자 ID 또는 앱 비밀번호가 설정되지 않았습니다."
            ));
        }

        let url = format!("{}/", self.dav_root());
        let method = Method::from_bytes(b"PROPFIND").unwrap_or(Method::HEAD);
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .request(method.clone(), &url)
                    .basic_auth(&self.user, Some(&self.app_password))
                    .header(HEADER_DEPTH, "0")
            })
            .await?;

        if resp.status().is_redirection() {
            let loc = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("알 수 없음");
            return Err(anyhow!(
                "Nextcloud 리다이렉트 감지 (HTTP {} -> {}). https:// 스킴 또는 포트 설정을 확인하세요.",
                resp.status(),
                loc
            ));
        }

        if !(resp.status().is_success() || resp.status().as_u16() == 207) {
            return Err(anyhow!(
                "Nextcloud WebDAV 인증 실패 (HTTP {})",
                resp.status()
            ));
        }

        Ok(format!(
            "Nextcloud WebDAV 연결 성공 ({} / 사용자: {})",
            self.base_url, self.user
        ))
    }

    async fn upload(&self, ctx: UploadContext<'_>) -> Result<UploadOutcome> {
        let remote_target_id = self
            .upload_file_with_resource(
                ctx.source_type,
                ctx.file_path,
                ctx.parent_path,
                ctx.filename,
                ctx.modified_at,
                Some(ctx.resource_id),
            )
            .await?;
        Ok(UploadOutcome {
            remote_target_id,
            sync_result: SyncResultKind::Created,
        })
    }

    async fn verify_exists(&self, file: &FileRecord) -> Result<bool> {
        let Some(ref remote_path) = file.remote_target_id else {
            return Ok(false);
        };
        self.check_file_exists(remote_path, file.size).await
    }
}
