use crate::config::normalize_base_url;
use crate::db::FileRecord;
use crate::limiter::RateLimiter;
use crate::router::{
    inspect_file_path, replace_or_append_extension, DetectedFileKind, SyncResultKind,
    TargetRouter, TargetType,
};
use crate::sources::{
    build_target_http_client, stream_response_to_temp, DownloadedSourceFile, ExtractedLivePhoto,
};
use crate::targets::{TargetProvider, UploadContext, UploadOutcome};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;

const ENDPOINT_PING: &str = "/api/server/ping";
const ENDPOINT_BULK_CHECK: &str = "/api/assets/bulk-upload-check";
const ENDPOINT_ASSETS: &str = "/api/assets";
const ENDPOINT_ALBUMS: &str = "/api/albums";
const ENDPOINT_SEARCH_METADATA: &str = "/api/search/metadata";
const ENDPOINT_TIMELINE_BUCKETS: &str = "/api/timeline/buckets";
const ENDPOINT_TIMELINE_BUCKET: &str = "/api/timeline/bucket";
const HEADER_API_KEY: &str = "x-api-key";
const DEVICE_ID: &str = "mybox-immich-sync";

fn deserialize_null_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImmichAlbumSummaryDto {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub id: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub album_name: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub asset_count: usize,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImmichAlbumDetailDto {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub id: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub album_name: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub asset_count: usize,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub assets: Vec<ImmichAlbumAsset>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImmichAlbumAsset {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub id: String,
    #[serde(default)]
    pub device_asset_id: Option<String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub original_file_name: String,
    #[serde(default)]
    pub file_created_at: Option<String>,
    #[serde(default)]
    pub file_modified_at: Option<String>,
    #[serde(default)]
    pub checksum: Option<String>,
    #[serde(default)]
    pub live_photo_video_id: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ImmichSearchMetadataResponse {
    #[serde(default)]
    assets: ImmichSearchAssetPage,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ImmichSearchAssetPage {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    items: Vec<ImmichAlbumAsset>,
    #[serde(default)]
    next_page: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ImmichTimeBucketSummary {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    time_bucket: String,
}

pub fn resolve_upload_filename_and_mime(
    file_path: &Path,
    filename: &str,
) -> (String, Option<&'static str>) {
    let Ok(Some(DetectedFileKind::Media { ext, mime, .. })) = inspect_file_path(file_path) else {
        return (filename.to_string(), None);
    };

    if TargetRouter::has_null_extension(filename)
        || TargetRouter::is_livephoto_archive_name(filename)
    {
        return (replace_or_append_extension(filename, ext), Some(mime));
    }

    let current_ext = Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
        .unwrap_or_default();

    let is_equivalent_ext = current_ext == ext
        || (ext == "jpg" && current_ext == "jpeg")
        || (ext == "tif" && current_ext == "tiff")
        || (ext == "heic" && current_ext == "heif");

    let final_name = if is_equivalent_ext {
        filename.to_string()
    } else {
        replace_or_append_extension(filename, ext)
    };

    (final_name, Some(mime))
}

#[derive(Debug, Serialize)]
struct BulkUploadCheckRequest<'a> {
    assets: Vec<BulkUploadCheckItem<'a>>,
}

#[derive(Debug, Serialize)]
struct BulkUploadCheckItem<'a> {
    id: &'a str,
    checksum: &'a str,
}

#[derive(Debug, Default, Deserialize)]
struct BulkUploadCheckResponse {
    #[serde(default)]
    results: Vec<BulkUploadCheckResultDto>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkUploadCheckResultDto {
    #[serde(default)]
    id: String,
    #[serde(default)]
    action: String,
    #[serde(default)]
    asset_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct AssetUploadResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

pub struct ImmichTarget {
    base_url: String,
    api_key: String,
    client: reqwest::Client,
    limiter: RateLimiter,
}

impl ImmichTarget {
    pub fn new(base_url: String, api_key: String, limiter: RateLimiter) -> Self {
        Self {
            base_url: normalize_base_url(&base_url),
            api_key,
            client: build_target_http_client(true),
            limiter,
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Calls `POST /api/assets/bulk-upload-check` with `(id, sha1_checksum)`.
    /// Returns a map from `id` to `Some(existing_asset_id)` if already present (`action == "reject"`),
    /// or `None` if it needs to be uploaded (`action == "accept"`).
    pub async fn check_bulk_hashes(
        &self,
        items: &[(String, String)],
    ) -> Result<HashMap<String, Option<String>>> {
        let mut out = HashMap::new();
        if items.is_empty() {
            return Ok(out);
        }

        let url = self.endpoint(ENDPOINT_BULK_CHECK);
        let payload = BulkUploadCheckRequest {
            assets: items
                .iter()
                .map(|(id, checksum)| BulkUploadCheckItem { id, checksum })
                .collect(),
        };

        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .post(&url)
                    .header(HEADER_API_KEY, &self.api_key)
                    .json(&payload)
            })
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Immich bulk-upload-check 실패: HTTP {}",
                resp.status()
            ));
        }

        let dto: BulkUploadCheckResponse = resp.json().await?;
        for r in dto.results {
            if r.action == "reject" {
                let asset_id = r
                    .asset_id
                    .unwrap_or_else(|| "existing_immich_asset".to_string());
                out.insert(r.id, Some(asset_id));
            } else {
                out.insert(r.id, None);
            }
        }

        Ok(out)
    }

    pub async fn verify_hash_exists(&self, resource_id: &str, sha1_hash: &str) -> Result<bool> {
        Ok(self
            .precheck_duplicate(resource_id, sha1_hash)
            .await?
            .is_some())
    }

    pub async fn upload_asset(
        &self,
        file_path: &Path,
        filename: &str,
        device_asset_id: &str,
        created_at: &str,
        modified_at: &str,
    ) -> Result<(String, SyncResultKind)> {
        self.upload_asset_with_options(
            file_path,
            filename,
            device_asset_id,
            created_at,
            modified_at,
            None,
        )
        .await
    }

    pub async fn upload_asset_with_options(
        &self,
        file_path: &Path,
        filename: &str,
        device_asset_id: &str,
        created_at: &str,
        modified_at: &str,
        live_photo_video_id: Option<&str>,
    ) -> Result<(String, SyncResultKind)> {
        let url = self.endpoint(ENDPOINT_ASSETS);
        let file_len = tokio::fs::metadata(file_path).await?.len();
        let (effective_filename, detected_mime) =
            resolve_upload_filename_and_mime(file_path, filename);
        let live_vid_opt = live_photo_video_id.map(str::to_string);

        let resp = self
            .limiter
            .send_with_retry(|| {
                let std_file =
                    std::fs::File::open(file_path).expect("temp file must exist during upload");
                let async_file = tokio::fs::File::from_std(std_file);
                let reader_stream = tokio_util::io::ReaderStream::new(async_file);
                let body = reqwest::Body::wrap_stream(reader_stream);
                let mut part = Part::stream_with_length(body, file_len)
                    .file_name(effective_filename.clone());
                if let Some(mime_str) = detected_mime {
                    part = part
                        .mime_str(mime_str)
                        .expect("DetectedFileKind mime must be a valid MIME string");
                }
                let mut form = Form::new()
                    .text("deviceAssetId", device_asset_id.to_string())
                    .text("deviceId", DEVICE_ID)
                    .text("fileCreatedAt", created_at.to_string())
                    .text("fileModifiedAt", modified_at.to_string());
                if let Some(ref vid_id) = live_vid_opt {
                    form = form.text("livePhotoVideoId", vid_id.clone());
                }
                form = form.part("assetData", part);
                self.client
                    .post(&url)
                    .header(HEADER_API_KEY, &self.api_key)
                    .multipart(form)
            })
            .await
            .map_err(|e| anyhow!("Immich 업로드 전송 오류: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Immich 자산 업로드 실패 (HTTP {}): {}",
                status,
                text
            ));
        }

        let dto: AssetUploadResponse = resp.json().await.unwrap_or_default();
        let id = dto.id.unwrap_or_else(|| "immich_uploaded_id".to_string());
        let kind = match dto.status.as_deref() {
            Some("duplicate") => SyncResultKind::Duplicate,
            _ => SyncResultKind::Created,
        };

        Ok((id, kind))
    }

    pub async fn link_live_photo_video(
        &self,
        photo_asset_id: &str,
        video_asset_id: &str,
    ) -> Result<()> {
        let url = format!("{}/{}", self.endpoint(ENDPOINT_ASSETS), photo_asset_id);
        let payload = json!({ "livePhotoVideoId": video_asset_id });
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .put(&url)
                    .header(HEADER_API_KEY, &self.api_key)
                    .json(&payload)
            })
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Immich 라이브포토 비디오 연결 실패 (HTTP {}): {}",
                status,
                text
            ));
        }
        Ok(())
    }

    pub async fn upload_live_photo(
        &self,
        extracted: &ExtractedLivePhoto,
        resource_id: &str,
        created_at: &str,
        modified_at: &str,
    ) -> Result<UploadOutcome> {
        let mut video_asset_id: Option<String> = None;
        if let Some(ref v_guard) = extracted.video_guard {
            let vid_name = extracted
                .video_filename
                .as_deref()
                .unwrap_or("livephoto.mov");
            let vid_device_id = format!("{}#livevideo", resource_id);
            let (vid_id, _) = self
                .upload_asset_with_options(
                    v_guard.path(),
                    vid_name,
                    &vid_device_id,
                    created_at,
                    modified_at,
                    None,
                )
                .await?;
            video_asset_id = Some(vid_id);
        }

        let (photo_id, sync_result) = self
            .upload_asset_with_options(
                extracted.image_guard.path(),
                &extracted.image_filename,
                resource_id,
                created_at,
                modified_at,
                video_asset_id.as_deref(),
            )
            .await?;

        if sync_result == SyncResultKind::Duplicate {
            if let Some(ref vid_id) = video_asset_id {
                let _ = self.link_live_photo_video(&photo_id, vid_id).await;
            }
        }

        Ok(UploadOutcome {
            remote_target_id: photo_asset_id_or_default(&photo_id),
            sync_result,
        })
    }

    pub async fn get_album_assets_by_name(
        &self,
        album_name: &str,
    ) -> Result<Option<(String, Vec<ImmichAlbumAsset>)>> {
        let list_url = self.endpoint(ENDPOINT_ALBUMS);
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .get(&list_url)
                    .header(HEADER_API_KEY, &self.api_key)
            })
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Immich 앨범 목록 조회 실패 (HTTP {}): {}",
                status,
                text
            ));
        }

        let mut albums: Vec<ImmichAlbumSummaryDto> = resp.json().await?;
        let target_lower = album_name.trim().to_lowercase();

        let mut matched_albums: Vec<ImmichAlbumSummaryDto> = albums
            .iter()
            .filter(|a| a.album_name.trim().to_lowercase() == target_lower)
            .cloned()
            .collect();

        if matched_albums.is_empty() {
            let shared_url = format!("{}?shared=true", self.endpoint(ENDPOINT_ALBUMS));
            if let Ok(shared_resp) = self
                .limiter
                .send_with_retry(|| {
                    self.client
                        .get(&shared_url)
                        .header(HEADER_API_KEY, &self.api_key)
                })
                .await
            {
                if shared_resp.status().is_success() {
                    if let Ok(shared_list) = shared_resp.json::<Vec<ImmichAlbumSummaryDto>>().await
                    {
                        for sa in shared_list {
                            if sa.album_name.trim().to_lowercase() == target_lower {
                                matched_albums.push(sa.clone());
                            }
                            albums.push(sa);
                        }
                    }
                }
            }
        }

        if matched_albums.is_empty() {
            return Ok(None);
        }

        // Prioritize albums with the highest reported asset_count
        matched_albums.sort_by(|a, b| b.asset_count.cmp(&a.asset_count));
        let primary_id = matched_albums[0].id.clone();
        let mut collected: Vec<ImmichAlbumAsset> = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();

        for matched in matched_albums {
            let detail_url = format!("{}/{}", self.endpoint(ENDPOINT_ALBUMS), matched.id);
            let detail_resp = self
                .limiter
                .send_with_retry(|| {
                    self.client
                        .get(&detail_url)
                        .header(HEADER_API_KEY, &self.api_key)
                })
                .await?;

            if !detail_resp.status().is_success() {
                let status = detail_resp.status();
                let text = detail_resp.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "Immich 앨범 상세 조회 실패 ({} / HTTP {}): {}",
                    matched.id,
                    status,
                    text
                ));
            }

            let detail: ImmichAlbumDetailDto = detail_resp.json().await?;
            let mut album_assets = detail.assets;

            if album_assets.is_empty() {
                if let Ok(search_assets) = self.fetch_album_assets_via_search(&matched.id).await {
                    if !search_assets.is_empty() {
                        album_assets = search_assets;
                    }
                }
            }

            if album_assets.is_empty() {
                if let Ok(tl_assets) = self.fetch_album_assets_via_timeline(&matched.id).await {
                    if !tl_assets.is_empty() {
                        album_assets = tl_assets;
                    }
                }
            }

            for asset in album_assets {
                if !asset.id.is_empty() && seen_ids.insert(asset.id.clone()) {
                    collected.push(asset);
                }
            }
        }

        Ok(Some((primary_id, collected)))
    }

    async fn fetch_album_assets_via_search(
        &self,
        album_id: &str,
    ) -> Result<Vec<ImmichAlbumAsset>> {
        let url = self.endpoint(ENDPOINT_SEARCH_METADATA);
        let mut all_items = Vec::new();
        let mut page: u32 = 1;

        loop {
            let payload = json!({
                "albumIds": [album_id],
                "size": 500,
                "page": page
            });
            let resp = self
                .limiter
                .send_with_retry(|| {
                    self.client
                        .post(&url)
                        .header(HEADER_API_KEY, &self.api_key)
                        .json(&payload)
                })
                .await?;

            if !resp.status().is_success() {
                break;
            }

            let dto: ImmichSearchMetadataResponse = resp.json().await?;
            let count = dto.assets.items.len();
            all_items.extend(dto.assets.items);

            let next_page = dto.assets.next_page.and_then(|v| match v {
                serde_json::Value::String(s) => s.parse::<u32>().ok(),
                serde_json::Value::Number(n) => n.as_u64().map(|x| x as u32),
                _ => None,
            });
            match next_page {
                Some(np) if np > page && count > 0 => page = np,
                _ => break,
            }
        }

        Ok(all_items)
    }

    async fn fetch_album_assets_via_timeline(
        &self,
        album_id: &str,
    ) -> Result<Vec<ImmichAlbumAsset>> {
        let buckets_url = format!(
            "{}?albumId={}",
            self.endpoint(ENDPOINT_TIMELINE_BUCKETS),
            album_id
        );
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .get(&buckets_url)
                    .header(HEADER_API_KEY, &self.api_key)
            })
            .await?;

        if !resp.status().is_success() {
            return Ok(Vec::new());
        }

        let buckets: Vec<ImmichTimeBucketSummary> = resp.json().await.unwrap_or_default();
        let mut all_assets = Vec::new();

        for b in buckets {
            if b.time_bucket.is_empty() {
                continue;
            }
            let bucket_url = format!(
                "{}?albumId={}&timeBucket={}",
                self.endpoint(ENDPOINT_TIMELINE_BUCKET),
                album_id,
                b.time_bucket
            );
            let b_resp = self
                .limiter
                .send_with_retry(|| {
                    self.client
                        .get(&bucket_url)
                        .header(HEADER_API_KEY, &self.api_key)
                })
                .await?;
            if !b_resp.status().is_success() {
                continue;
            }
            let val: serde_json::Value = b_resp.json().await.unwrap_or_default();
            if let Ok(list) = serde_json::from_value::<Vec<ImmichAlbumAsset>>(val.clone()) {
                all_assets.extend(list);
            } else if let Some(ids) = val.get("id").and_then(|v| v.as_array()) {
                // Columnar timeline format in newer Immich versions
                let names = val.get("originalFileName").and_then(|v| v.as_array());
                let live_ids = val.get("livePhotoVideoId").and_then(|v| v.as_array());
                for (i, id_val) in ids.iter().enumerate() {
                    if let Some(id_str) = id_val.as_str() {
                        let orig_name = names
                            .and_then(|arr| arr.get(i))
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string();
                        let live_id = live_ids
                            .and_then(|arr| arr.get(i))
                            .and_then(|v| v.as_str())
                            .map(str::to_string);
                        all_assets.push(ImmichAlbumAsset {
                            id: id_str.to_string(),
                            original_file_name: orig_name,
                            live_photo_video_id: live_id,
                            ..Default::default()
                        });
                    }
                }
            }
        }

        Ok(all_assets)
    }

    pub async fn download_asset_to_temp(
        &self,
        asset_id: &str,
        filename: &str,
        tmp_dir: &Path,
    ) -> Result<DownloadedSourceFile> {
        let url = format!("{}/{}/original", self.endpoint(ENDPOINT_ASSETS), asset_id);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).header(HEADER_API_KEY, &self.api_key))
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!(
                "Immich 원본 자산 다운로드 실패 ({}): HTTP {}",
                asset_id,
                resp.status()
            ));
        }

        stream_response_to_temp(resp, "immich", asset_id, filename, tmp_dir).await
    }

    pub async fn delete_assets(&self, asset_ids: &[String]) -> Result<()> {
        if asset_ids.is_empty() {
            return Ok(());
        }

        let url = self.endpoint(ENDPOINT_ASSETS);
        let payload = json!({
            "ids": asset_ids,
            "force": true
        });
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .delete(&url)
                    .header(HEADER_API_KEY, &self.api_key)
                    .json(&payload)
            })
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Immich 자산 삭제 실패 (HTTP {}): {}",
                status,
                text
            ));
        }
        Ok(())
    }
}

fn photo_asset_id_or_default(id: &str) -> String {
    if id.trim().is_empty() {
        "immich_uploaded_id".to_string()
    } else {
        id.to_string()
    }
}

#[async_trait]
impl TargetProvider for ImmichTarget {
    fn target_type(&self) -> TargetType {
        TargetType::Immich
    }

    async fn test_connection(&self) -> Result<String> {
        if self.base_url.trim().is_empty() || self.api_key.trim().is_empty() {
            return Err(anyhow!(
                "Immich 서버 URL 또는 API Key가 설정되지 않았습니다."
            ));
        }

        let url = self.endpoint(ENDPOINT_PING);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).header(HEADER_API_KEY, &self.api_key))
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!("Immich 서버 연결 실패 (HTTP {})", resp.status()));
        }

        Ok(format!("Immich 연결 성공 ({})", self.base_url))
    }

    async fn precheck_duplicate(
        &self,
        resource_id: &str,
        sha1_hash: &str,
    ) -> Result<Option<String>> {
        let items = vec![(resource_id.to_string(), sha1_hash.to_string())];
        let mut map = self.check_bulk_hashes(&items).await?;
        Ok(map.remove(resource_id).flatten())
    }

    async fn upload(&self, ctx: UploadContext<'_>) -> Result<UploadOutcome> {
        if let Some(existing_asset_id) = self
            .precheck_duplicate(ctx.resource_id, ctx.sha1_hash)
            .await?
        {
            return Ok(UploadOutcome {
                remote_target_id: existing_asset_id,
                sync_result: SyncResultKind::Duplicate,
            });
        }

        let (remote_target_id, sync_result) = self
            .upload_asset(
                ctx.file_path,
                ctx.filename,
                ctx.resource_id,
                ctx.created_at,
                ctx.modified_at,
            )
            .await?;

        Ok(UploadOutcome {
            remote_target_id,
            sync_result,
        })
    }

    async fn verify_exists(&self, file: &FileRecord) -> Result<bool> {
        let Some(ref sha1) = file.sha1_hash else {
            return Ok(false);
        };
        self.verify_hash_exists(&file.resource_id, sha1).await
    }

    async fn verify_exists_batch(&self, files: &[FileRecord]) -> Result<HashMap<String, bool>> {
        let mut out = HashMap::with_capacity(files.len());
        let mut check_items = Vec::new();
        for f in files {
            if let Some(ref sha1) = f.sha1_hash {
                check_items.push((f.resource_id.clone(), sha1.clone()));
            } else {
                out.insert(f.resource_id.clone(), false);
            }
        }
        for chunk in check_items.chunks(500) {
            let chunk_res = self.check_bulk_hashes(chunk).await?;
            for (res_id, _) in chunk {
                let exists = chunk_res.get(res_id).and_then(|opt| opt.as_ref()).is_some();
                out.insert(res_id.clone(), exists);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_resolve_upload_filename_and_mime_for_null_and_mismatched_ext() {
        let dir = TempDir::new().unwrap();

        let null_file = dir.path().join("IMG_9999.(null)");
        std::fs::write(&null_file, b"\xFF\xD8\xFF\xE0fake_jpeg").unwrap();
        let (name1, mime1) = resolve_upload_filename_and_mime(&null_file, "IMG_9999.(null)");
        assert_eq!(name1, "IMG_9999.jpg");
        assert_eq!(mime1, Some("image/jpeg"));

        let disguised_heic = dir.path().join("IMG_0001.jpg");
        std::fs::write(&disguised_heic, b"\x00\x00\x00\x18ftypheic\x00\x00\x00\x00").unwrap();
        let (name2, mime2) = resolve_upload_filename_and_mime(&disguised_heic, "IMG_0001.jpg");
        assert_eq!(name2, "IMG_0001.heic");
        assert_eq!(mime2, Some("image/heic"));
    }

    #[test]
    fn test_immich_album_dto_deserialization() {
        let raw_detail = r#"{
            "id": "album-uuid-1",
            "albumName": "to-nextcloud",
            "assets": [
                {
                    "id": "asset-uuid-100",
                    "deviceAssetId": "mybox_res_55",
                    "originalFileName": "app_icon.png",
                    "fileCreatedAt": "2025-05-01T10:00:00Z",
                    "fileModifiedAt": "2025-05-01T10:00:00Z",
                    "livePhotoVideoId": null
                }
            ]
        }"#;
        let parsed: ImmichAlbumDetailDto = serde_json::from_str(raw_detail).unwrap();
        assert_eq!(parsed.album_name, "to-nextcloud");
        assert_eq!(parsed.assets.len(), 1);
        assert_eq!(parsed.assets[0].id, "asset-uuid-100");
        assert_eq!(
            parsed.assets[0].device_asset_id.as_deref(),
            Some("mybox_res_55")
        );
        assert_eq!(parsed.assets[0].original_file_name, "app_icon.png");
    }
}
