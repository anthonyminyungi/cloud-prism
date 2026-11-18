use crate::config::normalize_nextcloud_url;
use crate::db::FileRecord;
use crate::limiter::RateLimiter;
use crate::router::{SourceType, SyncResultKind, TargetType};
use crate::sources::{
    build_target_http_client, join_remote_path, sanitize_filename, stream_response_to_temp,
    DownloadedSourceFile, IO_STREAM_BUFFER_SIZE,
};
use crate::targets::{TargetProvider, UploadContext, UploadOutcome, VerifyStatus};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

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

fn unescape_xml(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(pos) = rest.find('&') {
        out.push_str(&rest[..pos]);
        let after = &rest[pos..];
        if let Some(semi) = after.find(';') {
            let entity = &after[1..semi];
            let decoded = match entity {
                "amp" => Some("&"),
                "lt" => Some("<"),
                "gt" => Some(">"),
                "quot" => Some("\""),
                "apos" => Some("'"),
                _ => None,
            };
            if let Some(ch) = decoded {
                out.push_str(ch);
                rest = &after[semi + 1..];
                continue;
            }
        }
        out.push('&');
        rest = &rest[pos + 1..];
    }
    out.push_str(rest);
    out
}

pub fn normalize_href_path(raw_href: &str) -> String {
    let unescaped = unescape_xml(raw_href.trim());
    let path_only = if let Some(scheme_idx) = unescaped.find("://") {
        let after_scheme = &unescaped[scheme_idx + 3..];
        if let Some(slash_idx) = after_scheme.find('/') {
            &after_scheme[slash_idx..]
        } else {
            "/"
        }
    } else {
        unescaped.as_str()
    };
    let path_no_query = path_only.split(['?', '#']).next().unwrap_or(path_only);
    let decoded = percent_decode_str(path_no_query).decode_utf8_lossy();
    let segments: Vec<&str> = decoded.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", segments.join("/"))
    }
}

fn find_open_tag(lower: &str, from: usize, local_name: &str) -> Option<(usize, usize, bool)> {
    let bytes = lower.as_bytes();
    let mut i = from;
    while let Some(rel_lt) = lower[i..].find('<') {
        let lt = i + rel_lt;
        let after_lt = lt + 1;
        if after_lt >= bytes.len() {
            break;
        }
        if matches!(bytes[after_lt], b'/' | b'?' | b'!') {
            i = after_lt;
            continue;
        }
        let Some(rel_gt) = lower[after_lt..].find('>') else {
            break;
        };
        let gt = after_lt + rel_gt;
        let tag_inner = &lower[after_lt..gt];
        let self_closing = tag_inner.trim_end().ends_with('/');
        let tag_name_full = tag_inner
            .split(|c: char| c.is_ascii_whitespace() || c == '/')
            .next()
            .unwrap_or("");
        let tag_local = tag_name_full
            .rsplit_once(':')
            .map(|(_, local)| local)
            .unwrap_or(tag_name_full);
        if tag_local == local_name {
            return Some((lt, gt + 1, self_closing));
        }
        i = gt + 1;
    }
    None
}

fn find_close_tag(lower: &str, from: usize, local_name: &str) -> Option<(usize, usize)> {
    let mut i = from;
    while let Some(rel_lt) = lower[i..].find("</") {
        let lt = i + rel_lt;
        let after_slash = lt + 2;
        let Some(rel_gt) = lower[after_slash..].find('>') else {
            break;
        };
        let gt = after_slash + rel_gt;
        let tag_name_full = lower[after_slash..gt].trim();
        let tag_local = tag_name_full
            .rsplit_once(':')
            .map(|(_, local)| local)
            .unwrap_or(tag_name_full);
        if tag_local == local_name {
            return Some((lt, gt + 1));
        }
        i = gt + 1;
    }
    None
}

fn extract_xml_blocks<'a>(xml: &'a str, local_name: &str) -> Vec<&'a str> {
    let lower = xml.to_ascii_lowercase();
    let mut blocks = Vec::new();
    let mut pos = 0;
    while let Some((_, content_start, self_closing)) = find_open_tag(&lower, pos, local_name) {
        if self_closing {
            pos = content_start;
            continue;
        }
        let Some((close_start, close_end)) = find_close_tag(&lower, content_start, local_name)
        else {
            break;
        };
        blocks.push(&xml[content_start..close_start]);
        pos = close_end;
    }
    blocks
}

fn extract_first_tag_text(xml: &str, local_name: &str) -> Option<String> {
    let lower = xml.to_ascii_lowercase();
    let mut pos = 0;
    while let Some((_, content_start, self_closing)) = find_open_tag(&lower, pos, local_name) {
        if self_closing {
            pos = content_start;
            continue;
        }
        let (close_start, _) = find_close_tag(&lower, content_start, local_name)?;
        return Some(xml[content_start..close_start].trim().to_string());
    }
    None
}

fn has_xml_tag(xml: &str, local_name: &str) -> bool {
    let lower = xml.to_ascii_lowercase();
    find_open_tag(&lower, 0, local_name).is_some()
}

pub fn parse_webdav_propfind_entries(xml: &str) -> HashMap<String, (bool, Option<i64>)> {
    let mut entries = HashMap::new();
    for resp_block in extract_xml_blocks(xml, "response") {
        let Some(raw_href) = extract_first_tag_text(resp_block, "href") else {
            continue;
        };
        let norm_href = normalize_href_path(&raw_href);
        let propstats = extract_xml_blocks(resp_block, "propstat");
        let slices: Vec<&str> = if propstats.is_empty() {
            vec![resp_block]
        } else {
            propstats
        };

        let mut is_collection = false;
        let mut content_length: Option<i64> = None;
        let mut had_ok_propstat = false;

        for ps in slices {
            if let Some(status_text) = extract_first_tag_text(ps, "status") {
                if !status_text.contains(" 200") {
                    continue;
                }
            }
            had_ok_propstat = true;
            if has_xml_tag(ps, "collection") {
                is_collection = true;
            }
            if let Some(len_str) = extract_first_tag_text(ps, "getcontentlength") {
                if let Ok(parsed) = len_str.trim().parse::<i64>() {
                    content_length = Some(parsed);
                }
            }
        }

        if had_ok_propstat {
            entries.insert(norm_href, (is_collection, content_length));
        }
    }
    entries
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
    cookies: Arc<RwLock<HashMap<String, String>>>,
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
            cookies: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn dav_root(&self) -> String {
        let encoded_user = utf8_percent_encode(&self.user, PATH_SEGMENT_ENCODE_SET);
        format!("{}{}/{}", self.base_url, WEBDAV_PREFIX, encoded_user)
    }

    fn dav_url(&self, remote_path: &str) -> String {
        format!("{}{}", self.dav_root(), encode_dav_path(remote_path))
    }

    async fn cookie_header(&self) -> Option<String> {
        let guard = self.cookies.read().await;
        if guard.is_empty() {
            return None;
        }
        Some(
            guard
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("; "),
        )
    }

    async fn capture_cookies(&self, headers: &HeaderMap) {
        if !headers.contains_key(reqwest::header::SET_COOKIE) {
            return;
        }
        let mut guard = self.cookies.write().await;
        for raw_val in headers.get_all(reqwest::header::SET_COOKIE) {
            let Ok(s) = raw_val.to_str() else {
                continue;
            };
            let (kv, attrs) = s.split_once(';').unwrap_or((s, ""));
            let Some((k, v)) = kv.split_once('=') else {
                continue;
            };
            let key = k.trim();
            let val = v.trim();
            if key.is_empty() {
                continue;
            }
            if val.eq_ignore_ascii_case("deleted")
                || attrs.to_ascii_lowercase().contains("max-age=0")
            {
                guard.remove(key);
            } else {
                guard.insert(key.to_string(), val.to_string());
            }
        }
    }

    fn authed_request(
        &self,
        method: Method,
        url: &str,
        cookie: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let mut rb = self
            .client
            .request(method, url)
            .basic_auth(&self.user, Some(&self.app_password));
        if let Some(c) = cookie {
            rb = rb.header(reqwest::header::COOKIE, c);
        }
        rb
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
            let cookie = self.cookie_header().await;
            let resp = self
                .limiter
                .send_with_retry(|| self.authed_request(mkcol.clone(), &url, cookie.as_deref()))
                .await?;
            self.capture_cookies(resp.headers()).await;

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

        let cookie = self.cookie_header().await;
        let resp = self
            .limiter
            .send_with_retry(|| {
                let std_file =
                    std::fs::File::open(file_path).expect("temp file must exist during upload");
                let async_file = tokio::fs::File::from_std(std_file);
                let stream =
                    tokio_util::io::ReaderStream::with_capacity(async_file, IO_STREAM_BUFFER_SIZE);
                self.authed_request(Method::PUT, &url, cookie.as_deref())
                    .header(HEADER_MTIME, &mtime_epoch)
                    .header(reqwest::header::CONTENT_LENGTH, file_len)
                    .body(reqwest::Body::wrap_stream(stream))
            })
            .await?;
        self.capture_cookies(resp.headers()).await;

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

    async fn inspect_path_head(&self, remote_target_path: &str, expected_size: i64) -> VerifyStatus {
        let url = self.dav_url(remote_target_path);
        let cookie = self.cookie_header().await;

        let resp = match self
            .limiter
            .send_with_retry(|| self.authed_request(Method::HEAD, &url, cookie.as_deref()))
            .await
        {
            Ok(r) => r,
            Err(err) => return VerifyStatus::TargetError(err.to_string()),
        };
        self.capture_cookies(resp.headers()).await;

        let status = resp.status();
        if status.is_success() {
            let has_compression = resp
                .headers()
                .get(reqwest::header::CONTENT_ENCODING)
                .and_then(|v| v.to_str().ok())
                .map(|s| !s.trim().is_empty() && !s.trim().eq_ignore_ascii_case("identity"))
                .unwrap_or(false);

            if !has_compression && expected_size > 0 {
                if let Some(remote_size) = resp
                    .headers()
                    .get(reqwest::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    if remote_size != expected_size {
                        return VerifyStatus::SizeMismatch {
                            expected: expected_size,
                            actual: remote_size,
                        };
                    }
                }
            }

            return VerifyStatus::Present {
                healed_target_id: None,
                healed_sha1: None,
            };
        }

        if status == StatusCode::NOT_FOUND || status == StatusCode::GONE {
            VerifyStatus::MissingOnTarget
        } else {
            VerifyStatus::TargetError(format!("Nextcloud HEAD HTTP {}", status))
        }
    }

    async fn inspect_single_file_head(&self, file: &FileRecord) -> VerifyStatus {
        let Some(remote_path) = file
            .remote_target_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            return VerifyStatus::TargetError("Nextcloud 원격 경로 정보가 없습니다.".to_string());
        };
        self.inspect_path_head(remote_path, file.size).await
    }

    pub async fn check_file_exists(
        &self,
        remote_target_path: &str,
        expected_size: i64,
    ) -> Result<bool> {
        match self
            .inspect_path_head(remote_target_path, expected_size)
            .await
        {
            VerifyStatus::Present { .. } => Ok(true),
            VerifyStatus::MissingOnTarget
            | VerifyStatus::SizeMismatch { .. }
            | VerifyStatus::TrashedOnTarget => Ok(false),
            VerifyStatus::TargetError(msg) => Err(anyhow!(msg)),
        }
    }

    pub async fn download_file_to_temp(
        &self,
        remote_target_path: &str,
        filename: &str,
        tmp_dir: &Path,
    ) -> Result<DownloadedSourceFile> {
        let url = self.dav_url(remote_target_path);
        let cookie = self.cookie_header().await;
        let resp = self
            .limiter
            .send_with_retry(|| self.authed_request(Method::GET, &url, cookie.as_deref()))
            .await?;
        self.capture_cookies(resp.headers()).await;

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
        let cookie = self.cookie_header().await;
        let resp = self
            .limiter
            .send_with_retry(|| self.authed_request(Method::DELETE, &url, cookie.as_deref()))
            .await?;
        self.capture_cookies(resp.headers()).await;

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

fn strip_dav_user_prefix(norm_href: &str) -> Option<&str> {
    let idx = norm_href.find("/remote.php/dav/files/")?;
    let after = &norm_href[idx + "/remote.php/dav/files/".len()..];
    let (_, rel_path) = after.split_once('/')?;
    Some(rel_path)
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
        let cookie = self.cookie_header().await;
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.authed_request(method.clone(), &url, cookie.as_deref())
                    .header(HEADER_DEPTH, "0")
            })
            .await?;
        self.capture_cookies(resp.headers()).await;

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

    async fn inspect_exists_batch(&self, files: &[FileRecord]) -> HashMap<String, VerifyStatus> {
        let mut out = HashMap::with_capacity(files.len());
        if files.is_empty() {
            return out;
        }
        if self.base_url.trim().is_empty()
            || self.user.trim().is_empty()
            || self.app_password.trim().is_empty()
        {
            for f in files {
                out.insert(
                    f.resource_id.clone(),
                    VerifyStatus::TargetError(
                        "Nextcloud URL, 사용자 ID 또는 앱 비밀번호가 설정되지 않았습니다."
                            .to_string(),
                    ),
                );
            }
            return out;
        }

        let mut by_parent: HashMap<String, Vec<FileRecord>> = HashMap::new();
        for f in files {
            let Some(rp) = f
                .remote_target_id
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                out.insert(
                    f.resource_id.clone(),
                    VerifyStatus::TargetError("Nextcloud 원격 경로 정보가 없습니다.".to_string()),
                );
                continue;
            };
            let parent_dir = match rp.rsplit_once('/') {
                Some((p, _)) if !p.is_empty() => p.to_string(),
                _ => "/".to_string(),
            };
            by_parent.entry(parent_dir).or_default().push(f.clone());
        }

        let propfind = Method::from_bytes(b"PROPFIND").unwrap_or(Method::HEAD);
        let group_results: Vec<Vec<(String, VerifyStatus)>> = stream::iter(by_parent)
            .map(|(parent_dir, group)| {
                let propfind = propfind.clone();
                async move {
                    let dir_url = format!("{}/", self.dav_url(&parent_dir).trim_end_matches('/'));
                    let cookie = self.cookie_header().await;
                    let resp = match self
                        .limiter
                        .send_with_retry(|| {
                            self.authed_request(propfind.clone(), &dir_url, cookie.as_deref())
                                .header(HEADER_DEPTH, "1")
                        })
                        .await
                    {
                        Ok(r) => r,
                        Err(err) => {
                            let msg = err.to_string();
                            return group
                                .into_iter()
                                .map(|f| (f.resource_id, VerifyStatus::TargetError(msg.clone())))
                                .collect();
                        }
                    };
                    self.capture_cookies(resp.headers()).await;
                    let status = resp.status();

                    if status.as_u16() == 207 {
                        let xml = resp.text().await.unwrap_or_default();
                        let entries = parse_webdav_propfind_entries(&xml);
                        if !entries.is_empty() {
                            let mut by_rel_path: HashMap<&str, (bool, Option<i64>)> =
                                HashMap::with_capacity(entries.len());
                            for (href, meta) in &entries {
                                if let Some(rel) = strip_dav_user_prefix(href) {
                                    by_rel_path.insert(rel, *meta);
                                }
                            }
                            return group
                                .into_iter()
                                .map(|f| {
                                    let rp = f.remote_target_id.as_deref().unwrap_or("").trim();
                                    let expected_href = normalize_href_path(&self.dav_url(rp));
                                    let matched = entries.get(&expected_href).copied().or_else(|| {
                                        let rel_rp = rp.trim_start_matches('/');
                                        by_rel_path.get(rel_rp).copied()
                                    });
                                    let st = match matched {
                                        Some((false, Some(actual_len))) => {
                                            if f.size > 0 && actual_len != f.size {
                                                VerifyStatus::SizeMismatch {
                                                    expected: f.size,
                                                    actual: actual_len,
                                                }
                                            } else {
                                                VerifyStatus::Present {
                                                    healed_target_id: None,
                                                    healed_sha1: None,
                                                }
                                            }
                                        }
                                        Some((false, None)) => VerifyStatus::Present {
                                            healed_target_id: None,
                                            healed_sha1: None,
                                        },
                                        Some((true, _)) | None => VerifyStatus::MissingOnTarget,
                                    };
                                    (f.resource_id, st)
                                })
                                .collect();
                        }
                    }

                    // Fallback probe (also covers wiremock tests that only mock HEAD, or blocked PROPFIND)
                    let mut out_group = Vec::with_capacity(group.len());
                    let Some(first) = group.first() else {
                        return out_group;
                    };
                    let first_st = self.inspect_single_file_head(first).await;
                    if status == StatusCode::NOT_FOUND
                        && matches!(first_st, VerifyStatus::MissingOnTarget)
                    {
                        for f in group {
                            out_group.push((f.resource_id, VerifyStatus::MissingOnTarget));
                        }
                        return out_group;
                    }
                    if matches!(first_st, VerifyStatus::TargetError(_))
                        && (status == StatusCode::UNAUTHORIZED
                            || status == StatusCode::FORBIDDEN
                            || status.is_server_error())
                    {
                        for f in group {
                            out_group.push((f.resource_id, first_st.clone()));
                        }
                        return out_group;
                    }

                    out_group.push((first.resource_id.clone(), first_st));
                    let rest: Vec<(String, VerifyStatus)> =
                        stream::iter(group.into_iter().skip(1))
                            .map(|f| async move {
                                let st = self.inspect_single_file_head(&f).await;
                                (f.resource_id, st)
                            })
                            .buffer_unordered(4)
                            .collect()
                            .await;
                    out_group.extend(rest);
                    out_group
                }
            })
            .buffer_unordered(4)
            .collect()
            .await;

        for items in group_results {
            for (res_id, st) in items {
                out.insert(res_id, st);
            }
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_webdav_propfind_entries_handles_korean_entities_collections_and_404_propstats() {
        let xml = r#"<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:response>
    <d:href>/remote.php/dav/files/ncuser/MyBox/2023%20%EC%97%AC%ED%96%89/</d:href>
    <d:propstat>
      <d:prop>
        <d:resourcetype><d:collection/></d:resourcetype>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>https://nas.local/remote.php/dav/files/ncuser/MyBox/2023%20%EC%97%AC%ED%96%89/%EC%9D%BC%EC%A0%95%20&amp;%20%EC%98%88%EC%82%B0.pdf</d:href>
    <d:propstat>
      <d:prop>
        <d:resourcetype/>
        <d:getcontentlength>4096</d:getcontentlength>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
    <d:propstat>
      <d:prop>
        <oc:missing-prop/>
      </d:prop>
      <d:status>HTTP/1.1 404 Not Found</d:status>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>/remote.php/dav/files/ncuser/MyBox/2023%20%EC%97%AC%ED%96%89/broken.txt</d:href>
    <d:propstat>
      <d:prop>
        <d:getcontentlength>999</d:getcontentlength>
      </d:prop>
      <d:status>HTTP/1.1 404 Not Found</d:status>
    </d:propstat>
  </d:response>
</d:multistatus>"#;

        let parsed = parse_webdav_propfind_entries(xml);
        assert_eq!(
            parsed.get("/remote.php/dav/files/ncuser/MyBox/2023 여행"),
            Some(&(true, None))
        );
        assert_eq!(
            parsed.get("/remote.php/dav/files/ncuser/MyBox/2023 여행/일정 & 예산.pdf"),
            Some(&(false, Some(4096)))
        );
        assert_eq!(
            parsed.get("/remote.php/dav/files/ncuser/MyBox/2023 여행/broken.txt"),
            None
        );
    }
}
