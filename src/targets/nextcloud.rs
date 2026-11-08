use crate::db::FileRecord;
use crate::limiter::RateLimiter;
use crate::router::{SyncResultKind, TargetType};
use crate::sources::{build_http_client, join_remote_path};
use crate::targets::{TargetProvider, UploadContext, UploadOutcome};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::{Method, StatusCode};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

const WEBDAV_PREFIX: &str = "/remote.php/dav/files";
const HEADER_MTIME: &str = "X-OC-MTime";
const HEADER_DEPTH: &str = "Depth";

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
            base_url: base_url.trim_end_matches('/').to_string(),
            user,
            app_password,
            base_dir: normalized_dir,
            client: build_http_client(),
            limiter,
            created_dirs: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn dav_root(&self) -> String {
        format!("{}{}/{}", self.base_url, WEBDAV_PREFIX, self.user)
    }

    fn dav_url(&self, remote_path: &str) -> String {
        format!("{}{}", self.dav_root(), encode_dav_path(remote_path))
    }

    pub async fn ensure_folder_tree(&self, parent_path: &str) -> Result<String> {
        let combined = if parent_path == "/" || parent_path.is_empty() {
            self.base_dir.clone()
        } else {
            join_remote_path(&self.base_dir, parent_path)
        };

        let mkcol = Method::from_bytes(b"MKCOL").unwrap();
        let mut current = String::new();

        for seg in combined.split('/').filter(|s| !s.is_empty()) {
            current.push('/');
            current.push_str(seg);

            if self.created_dirs.lock().await.contains(&current) {
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
                self.created_dirs.lock().await.insert(current.clone());
            } else {
                return Err(anyhow!(
                    "Nextcloud MKCOL 실패 ({}): HTTP {}",
                    current,
                    resp.status()
                ));
            }
        }

        Ok(combined)
    }

    pub async fn upload_file(
        &self,
        file_path: &Path,
        parent_path: &str,
        filename: &str,
        modified_time_iso: &str,
    ) -> Result<String> {
        let dir_path = self.ensure_folder_tree(parent_path).await?;
        let remote_path = join_remote_path(&dir_path, filename);
        let url = self.dav_url(&remote_path);

        let bytes = tokio::fs::read(file_path).await?;
        let mtime_epoch = DateTime::parse_from_rfc3339(modified_time_iso)
            .map(|dt| dt.timestamp())
            .unwrap_or_else(|_| Utc::now().timestamp())
            .to_string();

        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .put(&url)
                    .basic_auth(&self.user, Some(&self.app_password))
                    .header(HEADER_MTIME, &mtime_epoch)
                    .body(bytes.clone())
            })
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Nextcloud 파일 업로드 실패 ({}): HTTP {}",
                remote_path,
                resp.status()
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

        let url = self.dav_root();
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
            .upload_file(
                ctx.file_path,
                ctx.parent_path,
                ctx.filename,
                ctx.modified_at,
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
