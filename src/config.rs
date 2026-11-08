use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;

pub const SECRET_MASK: &str = "********";

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

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            port: 3080,
            db_path: "/data/sync_state.db".to_string(),
            tmp_dir: "/tmp/mybox_sync".to_string(),
            mybox_pat: String::new(),
            mybox_api_base: "https://apis.naver.com/mybox".to_string(),
            gdrive_access_token: String::new(),
            gdrive_api_base: "https://www.googleapis.com/drive/v3".to_string(),
            takeout_dir: "/data/takeout".to_string(),
            immich_url: String::new(),
            immich_api_key: String::new(),
            enable_nextcloud: true,
            nextcloud_url: String::new(),
            nextcloud_user: String::new(),
            nextcloud_app_password: String::new(),
            nextcloud_base_dir: "/Cloud-Backup".to_string(),
            concurrency: 3,
            rate_limit_rps: 5.0,
            max_retries: 3,
        }
    }
}

impl AppConfig {
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        let mut cfg = Self::default();

        if let Ok(v) = env::var("PORT") {
            if let Ok(p) = v.trim().parse::<u16>() {
                cfg.port = p;
            }
        }
        if let Ok(v) = env::var("DB_PATH") {
            if !v.trim().is_empty() {
                cfg.db_path = v.trim().to_string();
            }
        }
        if let Ok(v) = env::var("TMP_DIR") {
            if !v.trim().is_empty() {
                cfg.tmp_dir = v.trim().to_string();
            }
        }
        if let Ok(v) = env::var("MYBOX_PAT") {
            if !v.trim().is_empty() {
                cfg.mybox_pat = v.trim().to_string();
            }
        }
        if let Ok(v) = env::var("MYBOX_API_BASE") {
            if !v.trim().is_empty() {
                cfg.mybox_api_base = v.trim().trim_end_matches('/').to_string();
            }
        }
        if let Ok(v) = env::var("GDRIVE_ACCESS_TOKEN") {
            if !v.trim().is_empty() {
                cfg.gdrive_access_token = v.trim().to_string();
            }
        }
        if let Ok(v) = env::var("GDRIVE_API_BASE") {
            if !v.trim().is_empty() {
                cfg.gdrive_api_base = v.trim().trim_end_matches('/').to_string();
            }
        }
        if let Ok(v) = env::var("TAKEOUT_DIR") {
            if !v.trim().is_empty() {
                cfg.takeout_dir = v.trim().to_string();
            }
        }
        if let Ok(v) = env::var("IMMICH_URL") {
            if !v.trim().is_empty() {
                cfg.immich_url = v.trim().trim_end_matches('/').to_string();
            }
        }
        if let Ok(v) = env::var("IMMICH_API_KEY") {
            if !v.trim().is_empty() {
                cfg.immich_api_key = v.trim().to_string();
            }
        }
        if let Ok(v) = env::var("ENABLE_NEXTCLOUD") {
            if let Ok(b) = v.trim().parse::<bool>() {
                cfg.enable_nextcloud = b;
            }
        }
        if let Ok(v) = env::var("NEXTCLOUD_URL") {
            if !v.trim().is_empty() {
                cfg.nextcloud_url = v.trim().trim_end_matches('/').to_string();
            }
        }
        if let Ok(v) = env::var("NEXTCLOUD_USER") {
            if !v.trim().is_empty() {
                cfg.nextcloud_user = v.trim().to_string();
            }
        }
        if let Ok(v) = env::var("NEXTCLOUD_APP_PASSWORD") {
            if !v.trim().is_empty() {
                cfg.nextcloud_app_password = v.trim().to_string();
            }
        }
        if let Ok(v) = env::var("NEXTCLOUD_BASE_DIR") {
            if !v.trim().is_empty() {
                cfg.nextcloud_base_dir = v.trim().to_string();
            }
        }
        if let Ok(v) = env::var("CONCURRENCY") {
            if let Ok(c) = v.trim().parse::<usize>() {
                if c > 0 {
                    cfg.concurrency = c;
                }
            }
        }
        if let Ok(v) = env::var("RATE_LIMIT_RPS") {
            if let Ok(r) = v.trim().parse::<f64>() {
                if r > 0.0 {
                    cfg.rate_limit_rps = r;
                }
            }
        }
        if let Ok(v) = env::var("MAX_RETRIES") {
            if let Ok(m) = v.trim().parse::<u32>() {
                cfg.max_retries = m;
            }
        }

        cfg
    }

    /// Applies non-empty overrides from the SQLite `settings` table.
    /// Empty strings or `SECRET_MASK` values do not overwrite existing values.
    pub fn apply_db_overrides(&mut self, map: &HashMap<String, String>) {
        let get_non_empty = |key: &str| -> Option<&str> {
            map.get(key)
                .map(|s| s.trim())
                .filter(|s| !s.is_empty() && *s != SECRET_MASK)
        };

        if let Some(v) = get_non_empty("mybox_pat") {
            self.mybox_pat = v.to_string();
        }
        if let Some(v) = get_non_empty("mybox_api_base") {
            self.mybox_api_base = v.trim_end_matches('/').to_string();
        }
        if let Some(v) = get_non_empty("gdrive_access_token") {
            self.gdrive_access_token = v.to_string();
        }
        if let Some(v) = get_non_empty("gdrive_api_base") {
            self.gdrive_api_base = v.trim_end_matches('/').to_string();
        }
        if let Some(v) = get_non_empty("takeout_dir") {
            self.takeout_dir = v.to_string();
        }
        if let Some(v) = get_non_empty("immich_url") {
            self.immich_url = v.trim_end_matches('/').to_string();
        }
        if let Some(v) = get_non_empty("immich_api_key") {
            self.immich_api_key = v.to_string();
        }
        if let Some(v) = get_non_empty("enable_nextcloud") {
            if let Ok(b) = v.parse::<bool>() {
                self.enable_nextcloud = b;
            }
        }
        if let Some(v) = get_non_empty("nextcloud_url") {
            self.nextcloud_url = v.trim_end_matches('/').to_string();
        }
        if let Some(v) = get_non_empty("nextcloud_user") {
            self.nextcloud_user = v.to_string();
        }
        if let Some(v) = get_non_empty("nextcloud_app_password") {
            self.nextcloud_app_password = v.to_string();
        }
        if let Some(v) = get_non_empty("nextcloud_base_dir") {
            self.nextcloud_base_dir = v.to_string();
        }
        if let Some(v) = get_non_empty("concurrency") {
            if let Ok(c) = v.parse::<usize>() {
                if c > 0 {
                    self.concurrency = c;
                }
            }
        }
        if let Some(v) = get_non_empty("rate_limit_rps") {
            if let Ok(r) = v.parse::<f64>() {
                if r > 0.0 {
                    self.rate_limit_rps = r;
                }
            }
        }
        if let Some(v) = get_non_empty("max_retries") {
            if let Ok(m) = v.parse::<u32>() {
                self.max_retries = m;
            }
        }
        if let Some(v) = get_non_empty("tmp_dir") {
            self.tmp_dir = v.to_string();
        }
    }

    pub fn to_settings_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("mybox_pat".to_string(), self.mybox_pat.clone());
        map.insert("mybox_api_base".to_string(), self.mybox_api_base.clone());
        map.insert(
            "gdrive_access_token".to_string(),
            self.gdrive_access_token.clone(),
        );
        map.insert("gdrive_api_base".to_string(), self.gdrive_api_base.clone());
        map.insert("takeout_dir".to_string(), self.takeout_dir.clone());
        map.insert("immich_url".to_string(), self.immich_url.clone());
        map.insert("immich_api_key".to_string(), self.immich_api_key.clone());
        map.insert(
            "enable_nextcloud".to_string(),
            self.enable_nextcloud.to_string(),
        );
        map.insert("nextcloud_url".to_string(), self.nextcloud_url.clone());
        map.insert("nextcloud_user".to_string(), self.nextcloud_user.clone());
        map.insert(
            "nextcloud_app_password".to_string(),
            self.nextcloud_app_password.clone(),
        );
        map.insert(
            "nextcloud_base_dir".to_string(),
            self.nextcloud_base_dir.clone(),
        );
        map.insert("concurrency".to_string(), self.concurrency.to_string());
        map.insert(
            "rate_limit_rps".to_string(),
            self.rate_limit_rps.to_string(),
        );
        map.insert("max_retries".to_string(), self.max_retries.to_string());
        map
    }

    pub fn masked(&self) -> Self {
        let mut m = self.clone();
        if !m.mybox_pat.is_empty() {
            m.mybox_pat = SECRET_MASK.to_string();
        }
        if !m.gdrive_access_token.is_empty() {
            m.gdrive_access_token = SECRET_MASK.to_string();
        }
        if !m.immich_api_key.is_empty() {
            m.immich_api_key = SECRET_MASK.to_string();
        }
        if !m.nextcloud_app_password.is_empty() {
            m.nextcloud_app_password = SECRET_MASK.to_string();
        }
        m
    }
}
