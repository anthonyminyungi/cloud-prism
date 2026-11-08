use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TargetType {
    Immich,
    Nextcloud,
}

impl TargetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TargetType::Immich => "immich",
            TargetType::Nextcloud => "nextcloud",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "immich" => Some(TargetType::Immich),
            "nextcloud" => Some(TargetType::Nextcloud),
            _ => None,
        }
    }
}

const MEDIA_EXTENSIONS: &[&str] = &[
    // Photos & RAW
    "jpg", "jpeg", "png", "heic", "heif", "webp", "gif", "bmp", "tiff", "tif", "avif", "dng",
    "cr2", "cr3", "nef", "arw", "orf", "rw2", "pef", "raf",
    // Videos
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
