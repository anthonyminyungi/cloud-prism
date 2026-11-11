use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Mybox,
    Gdrive,
    Takeout,
}

impl SourceType {
    pub const ALL: &'static [SourceType] =
        &[SourceType::Mybox, SourceType::Gdrive, SourceType::Takeout];

    pub fn as_str(&self) -> &'static str {
        match self {
            SourceType::Mybox => "mybox",
            SourceType::Gdrive => "gdrive",
            SourceType::Takeout => "takeout",
        }
    }

    pub fn nextcloud_folder_name(&self) -> &'static str {
        match self {
            SourceType::Mybox => "MyBox",
            SourceType::Gdrive | SourceType::Takeout => "GoogleDrive",
        }
    }
}

impl fmt::Display for SourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SourceType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "mybox" => Ok(SourceType::Mybox),
            "gdrive" => Ok(SourceType::Gdrive),
            "takeout" => Ok(SourceType::Takeout),
            other => Err(anyhow!("지원하지 않는 소스 타입입니다: {}", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TargetType {
    Immich,
    Nextcloud,
}

impl TargetType {
    pub const ALL: &'static [TargetType] = &[TargetType::Immich, TargetType::Nextcloud];

    pub fn as_str(&self) -> &'static str {
        match self {
            TargetType::Immich => "immich",
            TargetType::Nextcloud => "nextcloud",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl fmt::Display for TargetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TargetType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "immich" => Ok(TargetType::Immich),
            "nextcloud" => Ok(TargetType::Nextcloud),
            other => Err(anyhow!("지원하지 않는 타겟 타입입니다: {}", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileStatus {
    #[serde(rename = "PENDING")]
    Pending,
    #[serde(rename = "UPLOADED")]
    Uploaded,
    #[serde(rename = "DELETED")]
    Deleted,
    #[serde(rename = "FAILED")]
    Failed,
}

impl FileStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            FileStatus::Pending => "PENDING",
            FileStatus::Uploaded => "UPLOADED",
            FileStatus::Deleted => "DELETED",
            FileStatus::Failed => "FAILED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncResultKind {
    Created,
    Duplicate,
}

impl SyncResultKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncResultKind::Created => "created",
            SyncResultKind::Duplicate => "duplicate",
        }
    }
}

const MEDIA_EXTENSIONS: &[&str] = &[
    // Photos & RAW
    "jpg", "jpeg", "png", "heic", "heif", "webp", "gif", "bmp", "tiff", "tif", "avif", "dng", "cr2",
    "cr3", "nef", "arw", "orf", "rw2", "pef", "raf", // Videos
    "mp4", "mov", "avi", "mkv", "webm", "3gp", "m4v", "mts", "m2ts", "mpg", "mpeg", "wmv", "flv",
];

const VIDEO_EXTENSIONS: &[&str] = &[
    "mp4", "mov", "avi", "mkv", "webm", "3gp", "m4v", "mts", "m2ts", "mpg", "mpeg", "wmv", "flv",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetectedFileKind {
    Media {
        ext: &'static str,
        mime: &'static str,
        is_video: bool,
    },
    ZipArchive,
    Document {
        ext: &'static str,
    },
}

pub fn inspect_magic_bytes(header: &[u8]) -> Option<DetectedFileKind> {
    if header.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some(DetectedFileKind::Media {
            ext: "jpg",
            mime: "image/jpeg",
            is_video: false,
        });
    }
    if header.starts_with(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]) {
        return Some(DetectedFileKind::Media {
            ext: "png",
            mime: "image/png",
            is_video: false,
        });
    }
    if header.starts_with(b"GIF87a") || header.starts_with(b"GIF89a") {
        return Some(DetectedFileKind::Media {
            ext: "gif",
            mime: "image/gif",
            is_video: false,
        });
    }
    if header.len() >= 12 && &header[0..4] == b"RIFF" && &header[8..12] == b"WEBP" {
        return Some(DetectedFileKind::Media {
            ext: "webp",
            mime: "image/webp",
            is_video: false,
        });
    }
    if header.starts_with(&[0x49, 0x49, 0x2A, 0x00])
        || header.starts_with(&[0x4D, 0x4D, 0x00, 0x2A])
    {
        return Some(DetectedFileKind::Media {
            ext: "tif",
            mime: "image/tiff",
            is_video: false,
        });
    }
    if header.len() >= 14 && header.starts_with(b"BM") {
        return Some(DetectedFileKind::Media {
            ext: "bmp",
            mime: "image/bmp",
            is_video: false,
        });
    }
    if header.len() >= 12 && &header[4..8] == b"ftyp" {
        let brand = &header[8..12];
        if matches!(
            brand,
            b"heic" | b"heix" | b"hevc" | b"hevx" | b"mif1" | b"msf1"
        ) {
            return Some(DetectedFileKind::Media {
                ext: "heic",
                mime: "image/heic",
                is_video: false,
            });
        }
        if matches!(brand, b"avif" | b"avis") {
            return Some(DetectedFileKind::Media {
                ext: "avif",
                mime: "image/avif",
                is_video: false,
            });
        }
        if brand == b"qt  " {
            return Some(DetectedFileKind::Media {
                ext: "mov",
                mime: "video/quicktime",
                is_video: true,
            });
        }
        if matches!(
            brand,
            b"isom"
                | b"iso2"
                | b"mp41"
                | b"mp42"
                | b"M4V "
                | b"m4v "
                | b"avc1"
                | b"3gp4"
                | b"3gp5"
        ) {
            return Some(DetectedFileKind::Media {
                ext: "mp4",
                mime: "video/mp4",
                is_video: true,
            });
        }
    }
    if header.starts_with(b"PK\x03\x04") || header.starts_with(b"PK\x05\x06") {
        return Some(DetectedFileKind::ZipArchive);
    }
    if header.starts_with(b"%PDF-") {
        return Some(DetectedFileKind::Document { ext: "pdf" });
    }
    None
}

pub fn inspect_file_path(path: &Path) -> Result<Option<DetectedFileKind>> {
    let mut f = std::fs::File::open(path)?;
    let mut buf = [0u8; 64];
    let n = f.read(&mut buf)?;
    Ok(inspect_magic_bytes(&buf[..n]))
}

pub fn replace_or_append_extension(filename: &str, new_ext: &str) -> String {
    let clean_ext = new_ext.trim_start_matches('.');
    let trimmed = filename.trim();
    let lower = trimmed.to_ascii_lowercase();

    let stem = if lower.ends_with(".livephoto.zip") {
        &trimmed[..trimmed.len() - ".livephoto.zip".len()]
    } else if lower.ends_with(".livephoto.,zip") {
        &trimmed[..trimmed.len() - ".livephoto.,zip".len()]
    } else if lower.ends_with(".,zip") {
        &trimmed[..trimmed.len() - ".,zip".len()]
    } else if lower.ends_with(".(null)") {
        &trimmed[..trimmed.len() - ".(null)".len()]
    } else if lower.ends_with("(null)") {
        trimmed[..trimmed.len() - "(null)".len()].trim_end_matches('.')
    } else if lower.ends_with(".null") {
        &trimmed[..trimmed.len() - ".null".len()]
    } else if let Some((s, _)) = trimmed.rsplit_once('.') {
        if !s.is_empty() {
            s
        } else {
            trimmed
        }
    } else {
        trimmed
    };

    let stem = stem.trim_end_matches(['.', ',']);
    let final_stem = if stem.is_empty() { "recovered" } else { stem };
    format!("{}.{}", final_stem, clean_ext)
}

pub fn extract_timestamp_from_filename(filename: &str) -> Option<String> {
    let bytes = filename.as_bytes();
    if bytes.len() < 8 {
        return None;
    }

    for i in 0..=bytes.len().saturating_sub(8) {
        if i > 0 && bytes[i - 1].is_ascii_digit() {
            continue;
        }
        // Check YYYYMMDD_HHMMSS or YYYYMMDD-HHMMSS
        if bytes[i..i + 8].iter().all(|b| b.is_ascii_digit()) {
            let year: i32 = std::str::from_utf8(&bytes[i..i + 4]).ok()?.parse().ok()?;
            let month: u32 = std::str::from_utf8(&bytes[i + 4..i + 6])
                .ok()?
                .parse()
                .ok()?;
            let day: u32 = std::str::from_utf8(&bytes[i + 6..i + 8])
                .ok()?
                .parse()
                .ok()?;

            if !(1995..=2035).contains(&year) {
                continue;
            }
            let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
                continue;
            };

            let (mut hour, mut min, mut sec) = (12u32, 0u32, 0u32);
            if i + 15 <= bytes.len()
                && matches!(bytes[i + 8], b'_' | b'-' | b' ')
                && bytes[i + 9..i + 15].iter().all(|b| b.is_ascii_digit())
            {
                if let (Ok(h), Ok(m), Ok(s)) = (
                    std::str::from_utf8(&bytes[i + 9..i + 11])
                        .unwrap_or("")
                        .parse::<u32>(),
                    std::str::from_utf8(&bytes[i + 11..i + 13])
                        .unwrap_or("")
                        .parse::<u32>(),
                    std::str::from_utf8(&bytes[i + 13..i + 15])
                        .unwrap_or("")
                        .parse::<u32>(),
                ) {
                    if h < 24 && m < 60 && s < 60 {
                        hour = h;
                        min = m;
                        sec = s;
                    }
                }
            }

            if let Some(naive) = date.and_hms_opt(hour, min, sec) {
                return Some(Utc.from_utc_datetime(&naive).to_rfc3339());
            }
        }
    }
    None
}

pub fn pick_older_timestamp(primary_iso: &str, candidate_iso: Option<&str>) -> String {
    let Some(cand_str) = candidate_iso.filter(|s| !s.trim().is_empty()) else {
        return primary_iso.to_string();
    };
    let Ok(cand_dt) = DateTime::parse_from_rfc3339(cand_str) else {
        return primary_iso.to_string();
    };
    // Ignore epoch / unset 1970-1980 placeholder timestamps
    if cand_dt.timestamp() <= 315_619_200 {
        return primary_iso.to_string();
    }
    let Ok(prim_dt) = DateTime::parse_from_rfc3339(primary_iso) else {
        return cand_str.to_string();
    };
    if prim_dt.timestamp() <= 315_619_200 || cand_dt < prim_dt {
        cand_str.to_string()
    } else {
        primary_iso.to_string()
    }
}

pub struct TargetRouter;

impl TargetRouter {
    pub fn is_livephoto_archive_name(filename: &str) -> bool {
        let lower = filename.trim().to_ascii_lowercase();
        (lower.ends_with(".zip") || lower.ends_with(",zip")) && lower.contains("livephoto")
    }

    pub fn has_null_extension(filename: &str) -> bool {
        let lower = filename.trim().to_ascii_lowercase();
        lower.ends_with(".(null)") || lower.ends_with("(null)") || lower.ends_with(".null")
    }

    /// Classifies a filename into `TargetType::Immich` for photos/videos/LivePhoto archives,
    /// or `TargetType::Nextcloud` for documents/general files when `enable_nextcloud` is true.
    pub fn classify(filename: &str, enable_nextcloud: bool) -> Option<TargetType> {
        if Self::is_livephoto_archive_name(filename) || Self::has_null_extension(filename) {
            return Some(TargetType::Immich);
        }

        let ext = Path::new(filename)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase())
            .unwrap_or_default();

        if Self::is_media_extension(&ext) {
            Some(TargetType::Immich)
        } else if enable_nextcloud {
            Some(TargetType::Nextcloud)
        } else {
            None
        }
    }

    pub fn is_media_extension(ext: &str) -> bool {
        let lower = ext.trim_start_matches('.').to_ascii_lowercase();
        MEDIA_EXTENSIONS.contains(&lower.as_str())
    }

    pub fn is_video_extension(ext: &str) -> bool {
        let lower = ext.trim_start_matches('.').to_ascii_lowercase();
        VIDEO_EXTENSIONS.contains(&lower.as_str())
    }
}
