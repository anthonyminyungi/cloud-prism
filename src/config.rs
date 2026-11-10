use crate::router::SourceType;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};

pub const DEFAULT_PORT: u16 = 3080;
pub const DEFAULT_DB_PATH: &str = "/data/sync_state.db";
pub const DEFAULT_TMP_DIR: &str = "/tmp/mybox_sync";
pub const DEFAULT_MYBOX_API_BASE: &str = "https://open-api.mybox.naver.com";
pub const DEFAULT_GDRIVE_API_BASE: &str = "https://www.googleapis.com/drive/v3";
pub const DEFAULT_TAKEOUT_DIR: &str = "/data/takeout";
pub const DEFAULT_NEXTCLOUD_BASE_DIR: &str = "/";
pub const DEFAULT_CONCURRENCY: usize = 3;
pub const DEFAULT_RATE_LIMIT_RPS: f64 = 5.0;
pub const DEFAULT_MAX_RETRIES: u32 = 3;
pub const DEFAULT_SYNC_INTERVAL_MINUTES: u64 = 0;

const URL_KEYS: &[&str] = &[
    "mybox_api_base",
    "gdrive_api_base",
    "immich_url",
    "nextcloud_url",
];

pub fn normalize_base_url(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{}", trimmed)
    }
}

pub fn normalize_mybox_api_base(raw: &str) -> String {
    let mut url = normalize_base_url(raw);
    if url.is_empty()
        || url.eq_ignore_ascii_case("https://apis.naver.com/mybox")
        || url.eq_ignore_ascii_case("http://apis.naver.com/mybox")
    {
        return DEFAULT_MYBOX_API_BASE.to_string();
    }
    for marker in ["/v1/drive", "/v1"] {
        if let Some((prefix, _)) = url.split_once(marker) {
            url = prefix.trim_end_matches('/').to_string();
            break;
        }
    }
    url
}

pub fn normalize_nextcloud_url(raw: &str) -> String {
    let mut url = normalize_base_url(raw);
    for marker in ["/remote.php", "/index.php", "/apps/", "/login"] {
        if let Some((prefix, _)) = url.split_once(marker) {
            url = prefix.trim_end_matches('/').to_string();
        }
    }
    url
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppConfig {
    pub port: u16,
    pub db_path: String,
    pub tmp_dir: String,
    pub mybox_pat: String,
    pub mybox_api_base: String,
    pub gdrive_access_token: String,
    pub gdrive_api_base: String,
    pub takeout_dir: String,
    pub immich_url: String,
    pub immich_api_key: String,
    pub enable_nextcloud: bool,
    pub nextcloud_url: String,
    pub nextcloud_user: String,
    pub nextcloud_app_password: String,
    pub nextcloud_base_dir: String,
    pub concurrency: usize,
    pub rate_limit_rps: f64,
    pub max_retries: u32,
    #[serde(default)]
    pub sync_interval_minutes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeConfigSummary {
    pub mybox_configured: bool,
    pub mybox_api_base: String,
    pub gdrive_configured: bool,
    pub gdrive_api_base: String,
    pub takeout_dir: String,
    pub immich_configured: bool,
    pub immich_url: String,
    pub enable_nextcloud: bool,
    pub nextcloud_configured: bool,
    pub nextcloud_url: String,
    pub nextcloud_user: String,
    pub nextcloud_base_dir: String,
    pub concurrency: usize,
    pub rate_limit_rps: f64,
    pub max_retries: u32,
    pub sync_interval_minutes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SettingsUpdateRequest {
    pub mybox_pat: Option<String>,
    pub mybox_api_base: Option<String>,
    pub gdrive_access_token: Option<String>,
    pub gdrive_api_base: Option<String>,
    pub takeout_dir: Option<String>,
    pub immich_url: Option<String>,
    pub immich_api_key: Option<String>,
    pub enable_nextcloud: Option<bool>,
    pub nextcloud_url: Option<String>,
    pub nextcloud_user: Option<String>,
    pub nextcloud_app_password: Option<String>,
    pub nextcloud_base_dir: Option<String>,
    pub concurrency: Option<usize>,
    pub rate_limit_rps: Option<f64>,
    pub max_retries: Option<u32>,
    pub sync_interval_minutes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_service: Option<String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            db_path: DEFAULT_DB_PATH.to_string(),
            tmp_dir: DEFAULT_TMP_DIR.to_string(),
            mybox_pat: String::new(),
            mybox_api_base: DEFAULT_MYBOX_API_BASE.to_string(),
            gdrive_access_token: String::new(),
            gdrive_api_base: DEFAULT_GDRIVE_API_BASE.to_string(),
            takeout_dir: DEFAULT_TAKEOUT_DIR.to_string(),
            immich_url: String::new(),
            immich_api_key: String::new(),
            enable_nextcloud: false,
            nextcloud_url: String::new(),
            nextcloud_user: String::new(),
            nextcloud_app_password: String::new(),
            nextcloud_base_dir: DEFAULT_NEXTCLOUD_BASE_DIR.to_string(),
            concurrency: DEFAULT_CONCURRENCY,
            rate_limit_rps: DEFAULT_RATE_LIMIT_RPS,
            max_retries: DEFAULT_MAX_RETRIES,
            sync_interval_minutes: DEFAULT_SYNC_INTERVAL_MINUTES,
        }
    }
}

impl AppConfig {
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        let mut cfg = Self::default();
        let env_map: HashMap<String, String> = env::vars()
            .map(|(k, v)| (k.to_ascii_lowercase(), v))
            .collect();
        cfg.apply_env_map(&env_map);
        cfg
    }

    /// Declaratively applies non-empty environment variable values onto `AppConfig`.
    pub fn apply_env_map(&mut self, map: &HashMap<String, String>) {
        let Ok(Value::Object(mut obj)) = serde_json::to_value(&*self) else {
            return;
        };

        for (key, current_val) in &mut obj {
            let Some(raw) = map
                .get(key.as_str())
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            else {
                continue;
            };

            match current_val {
                Value::String(_) => {
                    let normalized = if key == "nextcloud_url" {
                        normalize_nextcloud_url(raw)
                    } else if key == "mybox_api_base" {
                        normalize_mybox_api_base(raw)
                    } else if key == "immich_url" {
                        normalize_base_url(raw)
                    } else if URL_KEYS.contains(&key.as_str()) {
                        raw.trim_end_matches('/').to_string()
                    } else {
                        raw.to_string()
                    };
                    *current_val = Value::String(normalized);
                }
                Value::Bool(_) => {
                    if let Ok(b) = raw.parse::<bool>() {
                        *current_val = Value::Bool(b);
                    }
                }
                Value::Number(num) => {
                    if num.is_f64() {
                        if let Ok(f) = raw.parse::<f64>() {
                            if f > 0.0 {
                                if let Some(n) = serde_json::Number::from_f64(f) {
                                    *current_val = Value::Number(n);
                                }
                            }
                        }
                    } else if let Ok(u) = raw.parse::<u64>() {
                        if key == "concurrency" && u == 0 {
                            continue;
                        }
                        *current_val = Value::Number(u.into());
                    }
                }
                _ => {}
            }
        }

        if let Ok(updated) = serde_json::from_value::<AppConfig>(Value::Object(obj)) {
            *self = updated;
        }
    }

    pub fn summary(&self) -> RuntimeConfigSummary {
        RuntimeConfigSummary {
            mybox_configured: !self.mybox_pat.is_empty(),
            mybox_api_base: self.mybox_api_base.clone(),
            gdrive_configured: !self.gdrive_access_token.is_empty(),
            gdrive_api_base: self.gdrive_api_base.clone(),
            takeout_dir: self.takeout_dir.clone(),
            immich_configured: !self.immich_url.is_empty() && !self.immich_api_key.is_empty(),
            immich_url: self.immich_url.clone(),
            enable_nextcloud: self.enable_nextcloud,
            nextcloud_configured: self.enable_nextcloud
                && !self.nextcloud_url.is_empty()
                && !self.nextcloud_user.is_empty()
                && !self.nextcloud_app_password.is_empty(),
            nextcloud_url: self.nextcloud_url.clone(),
            nextcloud_user: self.nextcloud_user.clone(),
            nextcloud_base_dir: self.nextcloud_base_dir.clone(),
            concurrency: self.concurrency,
            rate_limit_rps: self.rate_limit_rps,
            max_retries: self.max_retries,
            sync_interval_minutes: self.sync_interval_minutes,
        }
    }

    pub fn settings_file_path(&self) -> PathBuf {
        Path::new(&self.db_path)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("settings.json")
    }

    pub fn disk_fallback_tmp_dir(&self) -> PathBuf {
        Path::new(&self.db_path)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(".tmp_sync")
    }

    pub fn load_persisted_if_exists(&mut self) {
        let path = self.settings_file_path();
        let Ok(bytes) = std::fs::read(&path) else {
            return;
        };
        let Ok(mut req) = serde_json::from_slice::<SettingsUpdateRequest>(&bytes) else {
            return;
        };
        // Google Drive migration now uses Google Takeout ZIP archives instead of short-lived OAuth tokens
        req.gdrive_access_token = None;
        self.apply_settings_update(&req);
    }

    pub fn save_persisted(&self) -> Result<()> {
        let path = self.settings_file_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let persisted = SettingsUpdateRequest {
            mybox_pat: Some(self.mybox_pat.clone()),
            mybox_api_base: Some(self.mybox_api_base.clone()),
            gdrive_access_token: Some(self.gdrive_access_token.clone()),
            gdrive_api_base: Some(self.gdrive_api_base.clone()),
            takeout_dir: Some(self.takeout_dir.clone()),
            immich_url: Some(self.immich_url.clone()),
            immich_api_key: Some(self.immich_api_key.clone()),
            enable_nextcloud: Some(self.enable_nextcloud),
            nextcloud_url: Some(self.nextcloud_url.clone()),
            nextcloud_user: Some(self.nextcloud_user.clone()),
            nextcloud_app_password: Some(self.nextcloud_app_password.clone()),
            nextcloud_base_dir: Some(self.nextcloud_base_dir.clone()),
            concurrency: Some(self.concurrency),
            rate_limit_rps: Some(self.rate_limit_rps),
            max_retries: Some(self.max_retries),
            sync_interval_minutes: Some(self.sync_interval_minutes),
            verify_service: None,
        };
        let json_bytes = serde_json::to_vec_pretty(&persisted)?;
        std::fs::write(&path, json_bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }

    pub fn apply_settings_update(&mut self, req: &SettingsUpdateRequest) {
        if let Some(ref v) = req.mybox_pat {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                self.mybox_pat = trimmed.to_string();
            }
        }
        if let Some(ref v) = req.mybox_api_base {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                self.mybox_api_base = normalize_mybox_api_base(trimmed);
            }
        }
        if let Some(ref v) = req.gdrive_access_token {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                self.gdrive_access_token = trimmed.to_string();
            }
        }
        if let Some(ref v) = req.gdrive_api_base {
            let trimmed = v.trim().trim_end_matches('/');
            if !trimmed.is_empty() {
                self.gdrive_api_base = trimmed.to_string();
            }
        }
        if let Some(ref v) = req.takeout_dir {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                self.takeout_dir = trimmed.to_string();
            }
        }
        if let Some(ref v) = req.immich_url {
            self.immich_url = normalize_base_url(v);
        }
        if let Some(ref v) = req.immich_api_key {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                self.immich_api_key = trimmed.to_string();
            }
        }
        if let Some(enable) = req.enable_nextcloud {
            self.enable_nextcloud = enable;
        }
        if let Some(ref v) = req.nextcloud_url {
            self.nextcloud_url = normalize_nextcloud_url(v);
        }
        if let Some(ref v) = req.nextcloud_user {
            self.nextcloud_user = v.trim().to_string();
        }
        if let Some(ref v) = req.nextcloud_app_password {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                self.nextcloud_app_password = trimmed.to_string();
            }
        }
        if let Some(ref v) = req.nextcloud_base_dir {
            let trimmed = v.trim().trim_end_matches('/');
            if trimmed.is_empty() {
                self.nextcloud_base_dir = "/".to_string();
            } else if trimmed.starts_with('/') {
                self.nextcloud_base_dir = trimmed.to_string();
            } else {
                self.nextcloud_base_dir = format!("/{}", trimmed);
            }
        }
        if let Some(c) = req.concurrency {
            if c > 0 {
                self.concurrency = c;
            }
        }
        if let Some(rps) = req.rate_limit_rps {
            if rps > 0.0 {
                self.rate_limit_rps = rps;
            }
        }
        if let Some(r) = req.max_retries {
            self.max_retries = r;
        }
        if let Some(mins) = req.sync_interval_minutes {
            self.sync_interval_minutes = mins;
        }
    }

    pub fn configured_sources(&self) -> Vec<SourceType> {
        let mut sources = Vec::new();
        if !self.mybox_pat.trim().is_empty() {
            sources.push(SourceType::Mybox);
        }
        if !self.gdrive_access_token.trim().is_empty() {
            sources.push(SourceType::Gdrive);
        }
        let takeout_path = Path::new(&self.takeout_dir);
        if takeout_path.exists() {
            let has_zip = std::fs::read_dir(takeout_path)
                .ok()
                .map(|entries| {
                    entries.filter_map(|e| e.ok()).any(|e| {
                        e.path()
                            .extension()
                            .and_then(|ext| ext.to_str())
                            .map(|ext| ext.eq_ignore_ascii_case("zip"))
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false);
            if has_zip {
                sources.push(SourceType::Takeout);
            }
        }
        sources
    }
}
