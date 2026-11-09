use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
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
            SourceType::Gdrive => "GoogleDrive",
            SourceType::Takeout => "GoogleTakeout",
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

pub struct TargetRouter;

impl TargetRouter {
    /// Classifies a filename into `TargetType::Immich` for photos/videos,
    /// or `TargetType::Nextcloud` for documents/general files when `enable_nextcloud` is true.
    pub fn classify(filename: &str, enable_nextcloud: bool) -> Option<TargetType> {
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
}
