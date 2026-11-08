use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::env;

pub const DEFAULT_PORT: u16 = 3080;
pub const DEFAULT_DB_PATH: &str = "/data/sync_state.db";
pub const DEFAULT_TMP_DIR: &str = "/tmp/mybox_sync";
pub const DEFAULT_MYBOX_API_BASE: &str = "https://apis.naver.com/mybox";
pub const DEFAULT_GDRIVE_API_BASE: &str = "https://www.googleapis.com/drive/v3";
pub const DEFAULT_TAKEOUT_DIR: &str = "/data/takeout";
pub const DEFAULT_NEXTCLOUD_BASE_DIR: &str = "/Cloud-Backup";
pub const DEFAULT_CONCURRENCY: usize = 3;
pub const DEFAULT_RATE_LIMIT_RPS: f64 = 5.0;
pub const DEFAULT_MAX_RETRIES: u32 = 3;

const URL_KEYS: &[&str] = &[
    "mybox_api_base",
    "gdrive_api_base",
    "immich_url",
    "nextcloud_url",
];

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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeConfigSummary {
    pub mybox_configured: bool,
    pub gdrive_configured: bool,
    pub takeout_dir: String,
    pub immich_configured: bool,
    pub enable_nextcloud: bool,
    pub nextcloud_configured: bool,
    pub nextcloud_base_dir: String,
    pub concurrency: usize,
    pub rate_limit_rps: f64,
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
            enable_nextcloud: true,
            nextcloud_url: String::new(),
            nextcloud_user: String::new(),
            nextcloud_app_password: String::new(),
            nextcloud_base_dir: DEFAULT_NEXTCLOUD_BASE_DIR.to_string(),
            concurrency: DEFAULT_CONCURRENCY,
            rate_limit_rps: DEFAULT_RATE_LIMIT_RPS,
            max_retries: DEFAULT_MAX_RETRIES,
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
                    let normalized = if URL_KEYS.contains(&key.as_str()) {
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
            gdrive_configured: !self.gdrive_access_token.is_empty(),
            takeout_dir: self.takeout_dir.clone(),
            immich_configured: !self.immich_url.is_empty() && !self.immich_api_key.is_empty(),
            enable_nextcloud: self.enable_nextcloud,
            nextcloud_configured: self.enable_nextcloud
                && !self.nextcloud_url.is_empty()
                && !self.nextcloud_user.is_empty()
                && !self.nextcloud_app_password.is_empty(),
            nextcloud_base_dir: self.nextcloud_base_dir.clone(),
            concurrency: self.concurrency,
            rate_limit_rps: self.rate_limit_rps,
        }
    }
}
