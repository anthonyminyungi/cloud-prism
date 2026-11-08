use crate::limiter::RateLimiter;
use anyhow::{anyhow, Result};
use chrono::DateTime;
use reqwest::{Method, StatusCode};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

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
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let normalized_dir = if base_dir.starts_with('/') {
            base_dir.trim_end_matches('/').to_string()
        } else {
            format!("/{}", base_dir.trim_end_matches('/'))
        };
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            user,
            app_password,
            base_dir: normalized_dir,
            client,
            limiter,
            created_dirs: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn dav_root(&self) -> String {
        format!("{}/remote.php/dav/files/{}", self.base_url, self.user)
    }

    pub async fn test_connection(&self) -> Result<String> {
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
                    .header("Depth", "0")
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

    pub async fn ensure_folder_tree(&self, parent_path: &str) -> Result<String> {
        let combined = if parent_path == "/" || parent_path.is_empty() {
            self.base_dir.clone()
        } else {
            format!(
                "{}/{}",
                self.base_dir.trim_end_matches('/'),
                parent_path.trim_matches('/')
            )
        };

        let mut current = String::new();
        for seg in combined.split('/').filter(|s| !s.is_empty()) {
            current.push('/');
            current.push_str(seg);

            {
                let cache = self.created_dirs.lock().await;
                if cache.contains(&current) {
                    continue;
                }
            }

            let url = format!("{}{}", self.dav_root(), current);
            let mkcol = Method::from_bytes(b"MKCOL").unwrap();
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
                let mut cache = self.created_dirs.lock().await;
                cache.insert(current.clone());
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
        let remote_path = format!("{}/{}", dir_path.trim_end_matches('/'), filename);
        let url = format!("{}{}", self.dav_root(), remote_path);

        let bytes = tokio::fs::read(file_path).await?;
        let mtime_epoch = DateTime::parse_from_rfc3339(modified_time_iso)
            .map(|dt| dt.timestamp().to_string())
            .unwrap_or_else(|_| "1767225600".to_string());

        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .put(&url)
                    .basic_auth(&self.user, Some(&self.app_password))
                    .header("X-OC-MTime", &mtime_epoch)
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
        let normalized = if remote_target_path.starts_with('/') {
            remote_target_path.to_string()
        } else {
            format!("/{}", remote_target_path)
        };
        let url = format!("{}{}", self.dav_root(), normalized);

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

        if let Some(len_val) = resp.headers().get(reqwest::header::CONTENT_LENGTH) {
            if let Ok(len_str) = len_val.to_str() {
                if let Ok(remote_size) = len_str.parse::<i64>() {
                    return Ok(remote_size == expected_size);
                }
            }
        }

        Ok(true)
    }
}
