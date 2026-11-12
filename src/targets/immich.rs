use crate::config::normalize_base_url;
use crate::db::FileRecord;
use crate::limiter::RateLimiter;
use crate::router::{
    extract_timestamp_from_filename, inspect_file_path, replace_or_append_extension,
    DetectedFileKind, SyncResultKind, TargetRouter, TargetType,
};
use crate::sources::{
    build_target_http_client, stream_response_to_temp, DownloadedSourceFile, ExtractedLivePhoto,
};
use crate::targets::{TargetProvider, UploadContext, UploadOutcome};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use base64::Engine as _;
use chrono::{DateTime, FixedOffset};
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::path::Path;

const ENDPOINT_PING: &str = "/api/server/ping";
const ENDPOINT_BULK_CHECK: &str = "/api/assets/bulk-upload-check";
const ENDPOINT_ASSETS: &str = "/api/assets";
const ENDPOINT_ALBUMS: &str = "/api/albums";
const ENDPOINT_DUPLICATES: &str = "/api/duplicates";
const ENDPOINT_TAGS: &str = "/api/tags";
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

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ImmichTagDto {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub id: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub name: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImmichExifInfoDto {
    #[serde(default)]
    pub file_size_in_byte: Option<i64>,
    #[serde(default)]
    pub exif_image_width: Option<i64>,
    #[serde(default)]
    pub exif_image_height: Option<i64>,
    #[serde(default)]
    pub make: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub lens_model: Option<String>,
    #[serde(default)]
    pub date_time_original: Option<String>,
    #[serde(default)]
    pub modify_date: Option<String>,
    #[serde(default)]
    pub time_zone: Option<String>,
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub rating: Option<i32>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImmichDuplicateAssetDto {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub id: String,
    #[serde(default)]
    pub device_asset_id: Option<String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub original_file_name: String,
    #[serde(default)]
    pub original_path: Option<String>,
    #[serde(default)]
    pub file_created_at: Option<String>,
    #[serde(default)]
    pub file_modified_at: Option<String>,
    #[serde(default)]
    pub is_favorite: bool,
    #[serde(default)]
    pub is_archived: bool,
    #[serde(default)]
    pub is_trashed: bool,
    #[serde(default)]
    pub visibility: Option<String>,
    #[serde(default)]
    pub checksum: Option<String>,
    #[serde(default)]
    pub live_photo_video_id: Option<String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub tags: Vec<ImmichTagDto>,
    #[serde(default)]
    pub exif_info: Option<ImmichExifInfoDto>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImmichDuplicateGroupDto {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub duplicate_id: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub assets: Vec<ImmichDuplicateAssetDto>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AssetMetadataUpdateRequest {
    pub ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_time_original: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_favorite: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latitude: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub longitude: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rating: Option<i32>,
}

impl AssetMetadataUpdateRequest {
    pub fn has_updates(&self) -> bool {
        self.date_time_original.is_some()
            || self.time_zone.is_some()
            || self.is_favorite.is_some()
            || self.visibility.is_some()
            || (self.latitude.is_some() && self.longitude.is_some())
            || self.description.is_some()
            || self.rating.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct DuplicateMergePlan {
    pub duplicate_id: String,
    pub winner: ImmichDuplicateAssetDto,
    pub losers: Vec<ImmichDuplicateAssetDto>,
    pub loser_ids: Vec<String>,
    pub loser_ids_to_unlink_live_video: Vec<String>,
    pub metadata_update: AssetMetadataUpdateRequest,
    pub merged_live_photo_video_id: Option<String>,
    pub tag_ids_to_add: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum DuplicateGroupEvaluation {
    Merge(Box<DuplicateMergePlan>),
    SkippedBurst {
        duplicate_id: String,
        reason: String,
    },
    SkippedInsufficientAssets,
}

pub fn is_utc_or_empty_tz(tz: Option<&str>) -> bool {
    let Some(s) = tz.map(str::trim).filter(|s| !s.is_empty()) else {
        return true;
    };
    matches!(
        s.to_ascii_lowercase().as_str(),
        "utc"
            | "utc+0"
            | "utc-0"
            | "utc+00:00"
            | "utc-00:00"
            | "gmt"
            | "etc/utc"
            | "+00:00"
            | "-00:00"
            | "z"
    )
}

pub fn decode_immich_checksum_to_hex(checksum: Option<&str>) -> Option<String> {
    let raw = checksum?.trim();
    if raw.is_empty() {
        return None;
    }
    if raw.len() == 40 && raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Some(raw.to_ascii_lowercase());
    }
    if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(raw) {
        if decoded.len() == 20 {
            return Some(hex::encode(decoded));
        }
    }
    None
}

pub fn format_quality_tier(filename: &str) -> u8 {
    let ext = Path::new(filename.trim())
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "dng" | "cr2" | "cr3" | "nef" | "arw" | "orf" | "rw2" | "pef" | "raf" => 4,
        "heic" | "heif" | "avif" => 3,
        "png" | "tif" | "tiff" | "bmp" => 2,
        "jpg" | "jpeg" | "webp" | "gif" => 1,
        _ => 0,
    }
}

pub fn normalize_photo_stem(filename: &str) -> String {
    let trimmed = filename.trim();
    let stem = match trimmed.rsplit_once('.') {
        Some((s, _)) if !s.is_empty() => s,
        _ => trimmed,
    };
    let mut s = stem.trim().to_ascii_lowercase();

    loop {
        let prev = s.clone();
        s = s.trim().to_string();

        for suffix in ["-edited", "_edited", "-복사본", "_복사본", " copy"] {
            if let Some(stripped) = s.strip_suffix(suffix) {
                if !stripped.trim().is_empty() {
                    s = stripped.trim().to_string();
                }
            }
        }

        // Strip trailing `+<digits>` (e.g. `img_9112+1` -> `img_9112`)
        if let Some((base, num)) = s.rsplit_once('+') {
            if !base.is_empty() && !num.is_empty() && num.bytes().all(|b| b.is_ascii_digit()) {
                s = base.to_string();
            }
        }

        // Strip trailing `(<digits_or_id>)` (e.g. `img_9112 (1)` -> `img_9112`)
        if s.ends_with(')') {
            if let Some(open_idx) = s.rfind('(') {
                let base = s[..open_idx].trim();
                let inside = &s[open_idx + 1..s.len() - 1];
                if !base.is_empty() && !inside.is_empty() {
                    s = base.to_string();
                }
            }
        }

        if s == prev {
            break;
        }
    }

    s
}

fn are_stems_copy_variants(stem_a: &str, stem_b: &str) -> bool {
    if stem_a.is_empty() || stem_b.is_empty() {
        return false;
    }
    if stem_a == stem_b {
        return true;
    }
    // Allow unsuffixed base vs `_1` or `-1` copy suffix (e.g. `img_9112` vs `img_9112_1`),
    // while keeping `_01` vs `_02` or `_1` vs `_2` distinct as Samsung/Android burst shots.
    for (short, long) in [(stem_a, stem_b), (stem_b, stem_a)] {
        let trailing_digits = short
            .bytes()
            .rev()
            .take_while(|b| b.is_ascii_digit())
            .count();
        if trailing_digits >= 3
            && (long == format!("{}_1", short) || long == format!("{}-1", short))
        {
            return true;
        }
    }
    false
}

fn split_camera_sequence_stem(stem: &str) -> Option<(&str, u64)> {
    let digit_len = stem
        .bytes()
        .rev()
        .take_while(|b| b.is_ascii_digit())
        .count();
    if !(1..=9).contains(&digit_len) {
        return None;
    }
    let split_idx = stem.len() - digit_len;
    let prefix = &stem[..split_idx];
    if prefix.is_empty() {
        return None;
    }
    let num_str = &stem[split_idx..];
    let num = num_str.parse::<u64>().ok()?;
    Some((prefix, num))
}

fn is_exact_hour_tz_offset(diff_secs: i64) -> bool {
    (3540..=14 * 3600 + 60).contains(&diff_secs) && ((diff_secs + 60) % 3600) <= 120
}

fn is_plausible_capture_dt(dt: &DateTime<FixedOffset>) -> bool {
    let ts = dt.timestamp();
    // Reject <= 1980-01-02
    if ts <= 315_619_200 {
        return false;
    }
    // Reject bogus 1999-11-30 and 2000-01-01T00:00:00Z default placeholders
    if (943_920_000..=944_006_400).contains(&ts) || ts == 946_684_800 {
        return false;
    }
    true
}

fn parse_plausible_dt(raw: Option<&str>) -> Option<(String, DateTime<FixedOffset>)> {
    let s = raw?.trim();
    if s.is_empty() {
        return None;
    }
    let dt = DateTime::parse_from_rfc3339(s).ok()?;
    if !is_plausible_capture_dt(&dt) {
        return None;
    }
    Some((s.to_string(), dt))
}

fn asset_exif_capture_dt(
    asset: &ImmichDuplicateAssetDto,
) -> Option<(String, DateTime<FixedOffset>)> {
    asset
        .exif_info
        .as_ref()
        .and_then(|e| parse_plausible_dt(e.date_time_original.as_deref()))
}

fn asset_primary_capture_dt(
    asset: &ImmichDuplicateAssetDto,
) -> Option<(String, DateTime<FixedOffset>)> {
    if let Some(parsed) = asset_exif_capture_dt(asset) {
        return Some(parsed);
    }
    if let Some(fn_iso) = extract_timestamp_from_filename(&asset.original_file_name) {
        if let Some(parsed) = parse_plausible_dt(Some(&fn_iso)) {
            return Some(parsed);
        }
    }
    if let Some(parsed) = parse_plausible_dt(asset.file_created_at.as_deref()) {
        return Some(parsed);
    }
    parse_plausible_dt(asset.file_modified_at.as_deref())
}

fn has_camera_exif(asset: &ImmichDuplicateAssetDto) -> bool {
    let Some(exif) = asset.exif_info.as_ref() else {
        return false;
    };
    exif.make
        .as_deref()
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
        || exif
            .model
            .as_deref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
        || exif
            .lens_model
            .as_deref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
}

fn check_same_photo_copy(assets: &[ImmichDuplicateAssetDto]) -> Result<(), String> {
    let stems: Vec<String> = assets
        .iter()
        .map(|a| normalize_photo_stem(&a.original_file_name))
        .collect();

    for i in 0..assets.len() {
        for j in (i + 1)..assets.len() {
            let a = &assets[i];
            let b = &assets[j];
            let stem_a = &stems[i];
            let stem_b = &stems[j];
            let same_stem = are_stems_copy_variants(stem_a, stem_b);

            // Even if stems match, if both have genuine EXIF dateTimeOriginal separated by 1..=120s, treat as burst
            if let (Some((_, exif_dt_a)), Some((_, exif_dt_b))) =
                (asset_exif_capture_dt(a), asset_exif_capture_dt(b))
            {
                let exif_diff = (exif_dt_a - exif_dt_b).num_seconds().abs();
                if (1..=120).contains(&exif_diff) {
                    return Err(format!(
                        "EXIF 촬영 시각이 {}초 차이 나는 연속 촬영 추정 ({} vs {})",
                        exif_diff, a.original_file_name, b.original_file_name
                    ));
                }
            }

            if same_stem {
                continue;
            }

            // Different stems: check sequential/different camera numbers under same prefix (e.g. IMG_9112 vs IMG_9113, _01 vs _02, PXL 9-digit)
            if let (Some((pref_a, num_a)), Some((pref_b, num_b))) = (
                split_camera_sequence_stem(stem_a),
                split_camera_sequence_stem(stem_b),
            ) {
                if pref_a == pref_b && num_a != num_b {
                    return Err(format!(
                        "파일 번호가 다른 연속/별개 촬영 추정 ({} vs {})",
                        a.original_file_name, b.original_file_name
                    ));
                }
            }

            // Different stems: if both filenames contain embedded timestamps that differ (not an exact timezone offset), reject
            if let (Some(fn_iso_a), Some(fn_iso_b)) = (
                extract_timestamp_from_filename(&a.original_file_name),
                extract_timestamp_from_filename(&b.original_file_name),
            ) {
                if let (Some((_, fn_dt_a)), Some((_, fn_dt_b))) = (
                    parse_plausible_dt(Some(&fn_iso_a)),
                    parse_plausible_dt(Some(&fn_iso_b)),
                ) {
                    let fn_diff = (fn_dt_a - fn_dt_b).num_seconds().abs();
                    if fn_diff > 0 && !is_exact_hour_tz_offset(fn_diff) {
                        return Err(format!(
                            "파일명 내장 촬영 시각이 서로 다른 별개 사진 추정 ({} vs {})",
                            a.original_file_name, b.original_file_name
                        ));
                    }
                }
            }

            if let (Some((_, dt_a)), Some((_, dt_b))) =
                (asset_primary_capture_dt(a), asset_primary_capture_dt(b))
            {
                let diff_secs = (dt_a - dt_b).num_seconds().abs();
                if (1..=120).contains(&diff_secs) {
                    return Err(format!(
                        "촬영 시각이 {}초 차이 나는 연속 촬영 추정 ({} vs {})",
                        diff_secs, a.original_file_name, b.original_file_name
                    ));
                }
                if diff_secs > 120
                    && has_camera_exif(a)
                    && has_camera_exif(b)
                    && !is_exact_hour_tz_offset(diff_secs)
                {
                    return Err(format!(
                        "파일명과 카메라 EXIF 촬영 시각이 모두 다른 별개 사진 추정 ({} vs {})",
                        a.original_file_name, b.original_file_name
                    ));
                }
            }
        }
    }

    Ok(())
}

pub fn evaluate_duplicate_group(group: &ImmichDuplicateGroupDto) -> DuplicateGroupEvaluation {
    let active_assets: Vec<ImmichDuplicateAssetDto> = group
        .assets
        .iter()
        .filter(|a| !a.is_trashed && !a.id.trim().is_empty())
        .cloned()
        .collect();

    if active_assets.len() < 2 {
        return DuplicateGroupEvaluation::SkippedInsufficientAssets;
    }

    if let Err(reason) = check_same_photo_copy(&active_assets) {
        return DuplicateGroupEvaluation::SkippedBurst {
            duplicate_id: group.duplicate_id.clone(),
            reason,
        };
    }

    let mut sorted = active_assets;
    sorted.sort_by(|a, b| {
        let res_a = a
            .exif_info
            .as_ref()
            .map(|e| e.exif_image_width.unwrap_or(0) * e.exif_image_height.unwrap_or(0))
            .unwrap_or(0);
        let res_b = b
            .exif_info
            .as_ref()
            .map(|e| e.exif_image_width.unwrap_or(0) * e.exif_image_height.unwrap_or(0))
            .unwrap_or(0);

        let tier_a = format_quality_tier(&a.original_file_name);
        let tier_b = format_quality_tier(&b.original_file_name);

        let cam_a = has_camera_exif(a);
        let cam_b = has_camera_exif(b);

        let size_a = a
            .exif_info
            .as_ref()
            .and_then(|e| e.file_size_in_byte)
            .unwrap_or(0);
        let size_b = b
            .exif_info
            .as_ref()
            .and_then(|e| e.file_size_in_byte)
            .unwrap_or(0);

        let live_a = a
            .live_photo_video_id
            .as_deref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        let live_b = b
            .live_photo_video_id
            .as_deref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);

        res_b
            .cmp(&res_a)
            .then_with(|| tier_b.cmp(&tier_a))
            .then_with(|| cam_b.cmp(&cam_a))
            .then_with(|| size_b.cmp(&size_a))
            .then_with(|| live_b.cmp(&live_a))
            .then_with(|| b.is_favorite.cmp(&a.is_favorite))
            .then_with(|| a.id.cmp(&b.id))
    });

    let winner = sorted.remove(0);
    let losers = sorted;

    // 1. Collect candidate timestamps across 3 tiers:
    // Tier 1: EXIF dateTimeOriginal (paired with asset's non-UTC timeZone)
    // Tier 2: Filename embedded timestamp (always tz = None since filename parser outputs UTC Z)
    // Tier 3: fileCreatedAt / fileModifiedAt
    let mut tier1_exif: Vec<(String, DateTime<FixedOffset>, Option<String>)> = Vec::new();
    let mut tier2_filename: Vec<(String, DateTime<FixedOffset>, Option<String>)> = Vec::new();
    let mut tier3_file: Vec<(String, DateTime<FixedOffset>, Option<String>)> = Vec::new();

    for asset in std::iter::once(&winner).chain(losers.iter()) {
        let non_utc_tz = asset
            .exif_info
            .as_ref()
            .and_then(|e| e.time_zone.as_deref())
            .map(str::trim)
            .filter(|s| !is_utc_or_empty_tz(Some(s)))
            .map(str::to_string);

        if let Some(exif) = asset.exif_info.as_ref() {
            if let Some((iso, dt)) = parse_plausible_dt(exif.date_time_original.as_deref()) {
                tier1_exif.push((iso, dt, non_utc_tz.clone()));
            }
        }
        if let Some(fn_iso) = extract_timestamp_from_filename(&asset.original_file_name) {
            if let Some((iso, dt)) = parse_plausible_dt(Some(&fn_iso)) {
                tier2_filename.push((iso, dt, None));
            }
        }
        if let Some((iso, dt)) = parse_plausible_dt(asset.file_created_at.as_deref()) {
            tier3_file.push((iso, dt, non_utc_tz.clone()));
        }
        if let Some((iso, dt)) = parse_plausible_dt(asset.file_modified_at.as_deref()) {
            tier3_file.push((iso, dt, non_utc_tz));
        }
    }

    let min_t1 = tier1_exif.iter().min_by_key(|(_, dt, _)| dt.timestamp()).cloned();
    let min_t2 = tier2_filename
        .iter()
        .min_by_key(|(_, dt, _)| dt.timestamp())
        .cloned();
    let min_t3 = tier3_file.iter().min_by_key(|(_, dt, _)| dt.timestamp()).cloned();

    // Prefer Tier 1 EXIF timestamp unless Tier 2/Tier 3 is > 24 hours older (rescuing a backup-date-corrupted EXIF)
    let mut best_date_candidate = min_t1.clone().or_else(|| min_t2.clone()).or_else(|| min_t3.clone());
    for fallback in [min_t2, min_t3].into_iter().flatten() {
        if let Some((_, ref current_best_dt, _)) = best_date_candidate {
            if current_best_dt.timestamp() - fallback.1.timestamp() > 86_400 {
                best_date_candidate = Some(fallback);
            }
        }
    }

    let winner_current_dt = asset_primary_capture_dt(&winner);
    let winner_tz = winner
        .exif_info
        .as_ref()
        .and_then(|e| e.time_zone.as_deref())
        .map(str::trim)
        .filter(|s| !is_utc_or_empty_tz(Some(s)))
        .map(str::to_string);

    let mut merged_date_time_original: Option<String> = None;
    let mut merged_time_zone: Option<String> = None;

    if let Some((best_iso, best_dt, best_tz)) = best_date_candidate {
        let should_update = match winner_current_dt {
            None => true,
            Some((_, ref w_dt)) => {
                let diff_secs = (w_dt.timestamp() - best_dt.timestamp()).abs();
                let is_older = w_dt.timestamp() - best_dt.timestamp() > 1;
                // 9-hour (or 1..=14h exact hour) UTC timezone shift guard
                let best_tz_missing_or_utc = is_utc_or_empty_tz(best_tz.as_deref());
                let is_tz_offset_artifact = has_camera_exif(&winner)
                    && winner_tz.is_some()
                    && best_tz_missing_or_utc
                    && is_exact_hour_tz_offset(diff_secs);

                is_older && !is_tz_offset_artifact
            }
        };

        if should_update {
            merged_date_time_original = Some(best_iso);
            if let Some(tz) = best_tz.filter(|s| !is_utc_or_empty_tz(Some(s))) {
                merged_time_zone = Some(tz);
            }
        }
    }

    if merged_time_zone.is_none() && winner_tz.is_none() {
        merged_time_zone = losers.iter().find_map(|l| {
            l.exif_info
                .as_ref()
                .and_then(|e| e.time_zone.as_deref())
                .map(str::trim)
                .filter(|s| !is_utc_or_empty_tz(Some(s)))
                .map(str::to_string)
        });
    }

    let merged_is_favorite = if !winner.is_favorite && losers.iter().any(|l| l.is_favorite) {
        Some(true)
    } else {
        None
    };

    let winner_is_archived = winner.is_archived
        || winner
            .visibility
            .as_deref()
            .map(|v| v.eq_ignore_ascii_case("archive"))
            .unwrap_or(false);
    let loser_is_archived = losers.iter().any(|l| {
        l.is_archived
            || l.visibility
                .as_deref()
                .map(|v| v.eq_ignore_ascii_case("archive"))
                .unwrap_or(false)
    });
    let merged_visibility = if !winner_is_archived && loser_is_archived {
        Some("archive".to_string())
    } else {
        None
    };

    let has_valid_gps = |asset: &ImmichDuplicateAssetDto| -> Option<(f64, f64)> {
        let exif = asset.exif_info.as_ref()?;
        let lat = exif.latitude?;
        let lon = exif.longitude?;
        if lat.abs() < 1e-6 && lon.abs() < 1e-6 {
            return None;
        }
        Some((lat, lon))
    };

    let (merged_lat, merged_lon) = if has_valid_gps(&winner).is_none() {
        losers
            .iter()
            .find_map(has_valid_gps)
            .map(|(lat, lon)| (Some(lat), Some(lon)))
            .unwrap_or((None, None))
    } else {
        (None, None)
    };

    let winner_desc = winner
        .exif_info
        .as_ref()
        .and_then(|e| e.description.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let merged_description = if winner_desc.is_none() {
        losers.iter().find_map(|l| {
            l.exif_info
                .as_ref()
                .and_then(|e| e.description.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
    } else {
        None
    };

    let winner_rating = winner
        .exif_info
        .as_ref()
        .and_then(|e| e.rating)
        .unwrap_or(0);
    let max_loser_rating = losers
        .iter()
        .filter_map(|l| l.exif_info.as_ref().and_then(|e| e.rating))
        .max()
        .unwrap_or(0);
    let merged_rating = if max_loser_rating > winner_rating && max_loser_rating > 0 {
        Some(max_loser_rating)
    } else {
        None
    };

    let winner_live_id = winner
        .live_photo_video_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let merged_live_photo_video_id = if winner_live_id.is_none() {
        losers.iter().find_map(|l| {
            l.live_photo_video_id
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
    } else {
        None
    };

    let protected_live_video_id = winner_live_id
        .as_deref()
        .or(merged_live_photo_video_id.as_deref());

    let mut loser_ids: Vec<String> = Vec::new();
    let mut loser_ids_to_unlink_live_video: Vec<String> = Vec::new();
    let mut seen_trash_ids: HashSet<String> = HashSet::new();
    seen_trash_ids.insert(winner.id.clone());
    if let Some(vid) = protected_live_video_id {
        seen_trash_ids.insert(vid.to_string());
    }

    for loser in &losers {
        if seen_trash_ids.insert(loser.id.clone()) {
            loser_ids.push(loser.id.clone());
        }
        if let Some(l_vid) = loser
            .live_photo_video_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            if Some(l_vid) == merged_live_photo_video_id.as_deref() {
                loser_ids_to_unlink_live_video.push(loser.id.clone());
            } else if seen_trash_ids.insert(l_vid.to_string()) {
                loser_ids.push(l_vid.to_string());
            }
        }
    }

    let existing_winner_tags: HashSet<String> = winner
        .tags
        .iter()
        .map(|t| t.id.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let mut tag_ids_to_add = Vec::new();
    let mut seen_tags = existing_winner_tags;
    for loser in &losers {
        for tag in &loser.tags {
            let tid = tag.id.trim();
            if !tid.is_empty() && seen_tags.insert(tid.to_string()) {
                tag_ids_to_add.push(tid.to_string());
            }
        }
    }

    let metadata_update = AssetMetadataUpdateRequest {
        ids: vec![winner.id.clone()],
        date_time_original: merged_date_time_original,
        time_zone: merged_time_zone,
        is_favorite: merged_is_favorite,
        visibility: merged_visibility,
        latitude: merged_lat,
        longitude: merged_lon,
        description: merged_description,
        rating: merged_rating,
    };

    DuplicateGroupEvaluation::Merge(Box::new(DuplicateMergePlan {
        duplicate_id: group.duplicate_id.clone(),
        winner,
        losers,
        loser_ids,
        loser_ids_to_unlink_live_video,
        metadata_update,
        merged_live_photo_video_id,
        tag_ids_to_add,
    }))
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

    pub async fn trash_assets(&self, asset_ids: &[String]) -> Result<()> {
        if asset_ids.is_empty() {
            return Ok(());
        }

        let url = self.endpoint(ENDPOINT_ASSETS);
        let payload = json!({
            "ids": asset_ids,
            "force": false
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
                "Immich 자산 휴지통 이동 실패 (HTTP {}): {}",
                status,
                text
            ));
        }
        Ok(())
    }

    pub async fn get_duplicates(&self) -> Result<Vec<ImmichDuplicateGroupDto>> {
        let url = self.endpoint(ENDPOINT_DUPLICATES);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).header(HEADER_API_KEY, &self.api_key))
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Immich 중복 목록 조회 실패 (HTTP {}): {}",
                status,
                text
            ));
        }

        let groups: Vec<ImmichDuplicateGroupDto> = resp.json().await?;
        Ok(groups)
    }

    pub async fn get_albums_for_asset(&self, asset_id: &str) -> Result<Vec<ImmichAlbumSummaryDto>> {
        let url = format!("{}?assetId={}", self.endpoint(ENDPOINT_ALBUMS), asset_id);
        let resp = self
            .limiter
            .send_with_retry(|| self.client.get(&url).header(HEADER_API_KEY, &self.api_key))
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Immich 자산 소속 앨범 조회 실패 ({} / HTTP {}): {}",
                asset_id,
                status,
                text
            ));
        }

        let albums: Vec<ImmichAlbumSummaryDto> = resp.json().await?;
        Ok(albums)
    }

    pub async fn list_all_albums(&self) -> Result<Vec<ImmichAlbumSummaryDto>> {
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
        let mut seen_ids: std::collections::HashSet<String> =
            albums.iter().map(|a| a.id.clone()).collect();

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
                if let Ok(shared_list) = shared_resp.json::<Vec<ImmichAlbumSummaryDto>>().await {
                    for sa in shared_list {
                        if seen_ids.insert(sa.id.clone()) {
                            albums.push(sa);
                        }
                    }
                }
            }
        }

        Ok(albums)
    }

    pub async fn unlink_live_photo_video(&self, photo_asset_id: &str) -> Result<()> {
        let url = format!("{}/{}", self.endpoint(ENDPOINT_ASSETS), photo_asset_id);
        let payload = json!({ "livePhotoVideoId": serde_json::Value::Null });
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
                "Immich 라이브포토 비디오 연결 해제 실패 ({} / HTTP {}): {}",
                photo_asset_id,
                status,
                text
            ));
        }
        Ok(())
    }

    pub async fn add_assets_to_album(&self, album_id: &str, asset_ids: &[String]) -> Result<()> {
        if asset_ids.is_empty() {
            return Ok(());
        }
        let url = format!("{}/{}/assets", self.endpoint(ENDPOINT_ALBUMS), album_id);
        let payload = json!({ "ids": asset_ids });
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
                "Immich 앨범 자산 추가 실패 ({} / HTTP {}): {}",
                album_id,
                status,
                text
            ));
        }
        Ok(())
    }

    pub async fn add_assets_to_tag(&self, tag_id: &str, asset_ids: &[String]) -> Result<()> {
        if asset_ids.is_empty() {
            return Ok(());
        }
        let url = format!("{}/{}/assets", self.endpoint(ENDPOINT_TAGS), tag_id);
        let payload = json!({ "ids": asset_ids });
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
                "Immich 태그 자산 추가 실패 ({} / HTTP {}): {}",
                tag_id,
                status,
                text
            ));
        }
        Ok(())
    }

    pub async fn update_asset_metadata(&self, req: &AssetMetadataUpdateRequest) -> Result<()> {
        if req.ids.is_empty() || !req.has_updates() {
            return Ok(());
        }
        let url = self.endpoint(ENDPOINT_ASSETS);
        let resp = self
            .limiter
            .send_with_retry(|| {
                self.client
                    .put(&url)
                    .header(HEADER_API_KEY, &self.api_key)
                    .json(req)
            })
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Immich 자산 메타데이터 업데이트 실패 (HTTP {}): {}",
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

    #[test]
    fn test_evaluate_duplicate_group_prefers_heic_over_bloated_jpg_and_merges_all_metadata() {
        let raw_group = r#"{
            "duplicateId": "dup-heic-vs-jpg",
            "assets": [
                {
                    "id": "asset-icloud-heic",
                    "originalFileName": "IMG_9112.heic",
                    "fileCreatedAt": "2026-07-02T09:00:00.000Z",
                    "fileModifiedAt": "2026-07-02T09:00:00.000Z",
                    "isFavorite": false,
                    "isArchived": false,
                    "isTrashed": false,
                    "exifInfo": {
                        "fileSizeInByte": 1910000,
                        "exifImageWidth": 4032,
                        "exifImageHeight": 3024,
                        "make": "Apple",
                        "model": "iPhone 14 Pro",
                        "dateTimeOriginal": "2026-07-02T09:00:00.000Z"
                    }
                },
                {
                    "id": "asset-cloud-bloated-jpg",
                    "originalFileName": "IMG_9112+1.jpg",
                    "fileCreatedAt": "2023-10-25T05:12:30.000Z",
                    "fileModifiedAt": "2023-10-25T05:12:30.000Z",
                    "isFavorite": true,
                    "isArchived": true,
                    "isTrashed": false,
                    "livePhotoVideoId": "video-uuid-99",
                    "tags": [{ "id": "tag-1", "name": "일본여행" }],
                    "exifInfo": {
                        "fileSizeInByte": 2850000,
                        "exifImageWidth": 4032,
                        "exifImageHeight": 3024,
                        "dateTimeOriginal": "2023-10-25T05:12:30.000Z",
                        "timeZone": "Asia/Tokyo",
                        "latitude": 35.6895,
                        "longitude": 139.6917,
                        "description": "도쿄 가챠샵",
                        "rating": 5
                    }
                }
            ]
        }"#;

        let group: ImmichDuplicateGroupDto = serde_json::from_str(raw_group).unwrap();
        let DuplicateGroupEvaluation::Merge(plan) = evaluate_duplicate_group(&group) else {
            panic!("Expected Merge plan");
        };

        assert_eq!(plan.winner.id, "asset-icloud-heic");
        assert_eq!(plan.loser_ids, vec!["asset-cloud-bloated-jpg".to_string()]);
        assert_eq!(
            plan.metadata_update.date_time_original.as_deref(),
            Some("2023-10-25T05:12:30.000Z")
        );
        assert_eq!(
            plan.metadata_update.time_zone.as_deref(),
            Some("Asia/Tokyo")
        );
        assert_eq!(plan.metadata_update.is_favorite, Some(true));
        assert_eq!(
            plan.metadata_update.visibility.as_deref(),
            Some("archive")
        );
        assert_eq!(plan.metadata_update.latitude, Some(35.6895));
        assert_eq!(plan.metadata_update.longitude, Some(139.6917));
        assert_eq!(
            plan.metadata_update.description.as_deref(),
            Some("도쿄 가챠샵")
        );
        assert_eq!(plan.metadata_update.rating, Some(5));
        assert_eq!(
            plan.merged_live_photo_video_id.as_deref(),
            Some("video-uuid-99")
        );
        assert_eq!(plan.tag_ids_to_add, vec!["tag-1".to_string()]);
    }

    #[test]
    fn test_evaluate_duplicate_group_skips_burst_shots_with_different_numbers() {
        let raw_burst = r#"{
            "duplicateId": "dup-burst-1",
            "assets": [
                {
                    "id": "asset-burst-1",
                    "originalFileName": "IMG_9112.HEIC",
                    "fileCreatedAt": "2023-10-25T05:12:30.000Z",
                    "exifInfo": {
                        "fileSizeInByte": 1900000,
                        "exifImageWidth": 4032,
                        "exifImageHeight": 3024,
                        "dateTimeOriginal": "2023-10-25T05:12:30.000Z"
                    }
                },
                {
                    "id": "asset-burst-2",
                    "originalFileName": "IMG_9113.HEIC",
                    "fileCreatedAt": "2023-10-25T05:12:31.000Z",
                    "exifInfo": {
                        "fileSizeInByte": 1920000,
                        "exifImageWidth": 4032,
                        "exifImageHeight": 3024,
                        "dateTimeOriginal": "2023-10-25T05:12:31.000Z"
                    }
                }
            ]
        }"#;

        let group: ImmichDuplicateGroupDto = serde_json::from_str(raw_burst).unwrap();
        let eval = evaluate_duplicate_group(&group);
        assert!(matches!(eval, DuplicateGroupEvaluation::SkippedBurst { .. }));
    }

    #[test]
    fn test_evaluate_duplicate_group_protects_against_9h_timezone_shift() {
        let raw_tz = r#"{
            "duplicateId": "dup-tz-shift",
            "assets": [
                {
                    "id": "asset-with-tz",
                    "originalFileName": "IMG_5000.heic",
                    "fileCreatedAt": "2023-10-25T14:00:00.000Z",
                    "exifInfo": {
                        "fileSizeInByte": 2000000,
                        "exifImageWidth": 4032,
                        "exifImageHeight": 3024,
                        "make": "Apple",
                        "model": "iPhone 14",
                        "dateTimeOriginal": "2023-10-25T14:00:00.000Z",
                        "timeZone": "Asia/Seoul"
                    }
                },
                {
                    "id": "asset-utc-broken",
                    "originalFileName": "IMG_5000.jpg",
                    "fileCreatedAt": "2023-10-25T05:00:00.000Z",
                    "exifInfo": {
                        "fileSizeInByte": 1500000,
                        "exifImageWidth": 4032,
                        "exifImageHeight": 3024,
                        "dateTimeOriginal": "2023-10-25T05:00:00.000Z",
                        "timeZone": null
                    }
                }
            ]
        }"#;

        let group: ImmichDuplicateGroupDto = serde_json::from_str(raw_tz).unwrap();
        let DuplicateGroupEvaluation::Merge(plan) = evaluate_duplicate_group(&group) else {
            panic!("Expected Merge plan");
        };
        assert_eq!(plan.winner.id, "asset-with-tz");
        assert_eq!(plan.metadata_update.date_time_original, None);
    }

    #[test]
    fn test_evaluate_duplicate_group_skips_samsung_pixel_and_different_day_clip_false_positives() {
        // 1. Samsung _01 vs _02 burst
        let raw_samsung = r#"{
            "duplicateId": "dup-samsung-burst",
            "assets": [
                {
                    "id": "sam-1",
                    "originalFileName": "20231025_141230_01.jpg",
                    "fileCreatedAt": "2023-10-25T14:12:30.000Z"
                },
                {
                    "id": "sam-2",
                    "originalFileName": "20231025_141230_02.jpg",
                    "fileCreatedAt": "2023-10-25T14:12:30.000Z"
                }
            ]
        }"#;
        let group_sam: ImmichDuplicateGroupDto = serde_json::from_str(raw_samsung).unwrap();
        assert!(matches!(
            evaluate_duplicate_group(&group_sam),
            DuplicateGroupEvaluation::SkippedBurst { .. }
        ));

        // 2. Pixel 9-digit millisecond burst
        let raw_pixel = r#"{
            "duplicateId": "dup-pixel-burst",
            "assets": [
                {
                    "id": "pxl-1",
                    "originalFileName": "PXL_20231025_141230123.jpg",
                    "fileCreatedAt": "2023-10-25T14:12:30.123Z"
                },
                {
                    "id": "pxl-2",
                    "originalFileName": "PXL_20231025_141230456.jpg",
                    "fileCreatedAt": "2023-10-25T14:12:30.456Z"
                }
            ]
        }"#;
        let group_pxl: ImmichDuplicateGroupDto = serde_json::from_str(raw_pixel).unwrap();
        assert!(matches!(
            evaluate_duplicate_group(&group_pxl),
            DuplicateGroupEvaluation::SkippedBurst { .. }
        ));

        // 3. CLIP visually similar photos taken on different days
        let raw_clip_diff_day = r#"{
            "duplicateId": "dup-clip-diff-day",
            "assets": [
                {
                    "id": "day-1",
                    "originalFileName": "20231025_140000.jpg",
                    "fileCreatedAt": "2023-10-25T14:00:00.000Z",
                    "exifInfo": { "dateTimeOriginal": "2023-10-25T14:00:00.000Z" }
                },
                {
                    "id": "day-2",
                    "originalFileName": "20231026_150000.jpg",
                    "fileCreatedAt": "2023-10-26T15:00:00.000Z",
                    "exifInfo": { "dateTimeOriginal": "2023-10-26T15:00:00.000Z" }
                }
            ]
        }"#;
        let group_clip: ImmichDuplicateGroupDto = serde_json::from_str(raw_clip_diff_day).unwrap();
        assert!(matches!(
            evaluate_duplicate_group(&group_clip),
            DuplicateGroupEvaluation::SkippedBurst { .. }
        ));
    }
}
