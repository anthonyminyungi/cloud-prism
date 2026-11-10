use crate::config::{AppConfig, SettingsUpdateRequest};
use crate::db::CheckpointDB;
use crate::sources::takeout::list_takeout_archives;
use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub const MAX_RECENT_LOGS: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Idle,
    Scanning,
    Syncing,
    Deleting,
    Stopping,
}

impl JobState {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobState::Idle => "idle",
            JobState::Scanning => "scanning",
            JobState::Syncing => "syncing",
            JobState::Deleting => "deleting",
            JobState::Stopping => "stopping",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceConnectionStatus {
    pub verified: bool,
    pub ok: bool,
    pub message: String,
    pub checked_at: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReadinessSummary {
    pub immich_verified: bool,
    pub nextcloud_verified: bool,
    pub targets_ready: bool,
    pub sync_tab_enabled: bool,
    pub delete_tab_enabled: bool,
    pub verified_sources: Vec<String>,
    pub verified_cloud_sources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressEvent {
    pub timestamp: String,
    pub source: String,
    pub state: JobState,
    pub current_file: Option<String>,
    pub processed_files: u64,
    pub total_files: u64,
    pub speed_fps: f64,
    pub message: String,
}

pub struct AppState {
    pub db: CheckpointDB,
    pub config: AppConfig,
    pub runtime_config: RwLock<AppConfig>,
    pub connection_status: RwLock<HashMap<String, ServiceConnectionStatus>>,
    pub job_state: RwLock<JobState>,
    pub active_source: RwLock<Option<String>>,
    pub current_file: RwLock<Option<String>>,
    pub processed_files: AtomicU64,
    pub total_files: AtomicU64,
    pub job_started_at: RwLock<Option<Instant>>,
    pub stop_flag: Arc<AtomicBool>,
    pub batch_active: AtomicBool,
    pub recent_logs: RwLock<VecDeque<ProgressEvent>>,
    pub event_tx: broadcast::Sender<ProgressEvent>,
    pub job_handle: Mutex<Option<JoinHandle<()>>>,
}

impl AppState {
    pub fn new(db: CheckpointDB, config: AppConfig) -> Arc<Self> {
        let mut initial_runtime = config.clone();
        initial_runtime.load_persisted_if_exists();
        let (event_tx, _) = broadcast::channel(256);
        Arc::new(Self {
            db,
            config,
            runtime_config: RwLock::new(initial_runtime),
            connection_status: RwLock::new(HashMap::new()),
            job_state: RwLock::new(JobState::Idle),
            active_source: RwLock::new(None),
            current_file: RwLock::new(None),
            processed_files: AtomicU64::new(0),
            total_files: AtomicU64::new(0),
            job_started_at: RwLock::new(None),
            stop_flag: Arc::new(AtomicBool::new(false)),
            batch_active: AtomicBool::new(false),
            recent_logs: RwLock::new(VecDeque::with_capacity(MAX_RECENT_LOGS)),
            event_tx,
            job_handle: Mutex::new(None),
        })
    }

    pub async fn effective_config(&self) -> AppConfig {
        self.runtime_config.read().await.clone()
    }

    pub async fn set_takeout_dir(&self, dir: String) {
        {
            let mut cfg = self.runtime_config.write().await;
            if cfg.takeout_dir != dir {
                self.connection_status.write().await.remove("takeout");
            }
            cfg.takeout_dir = dir;
            let _ = cfg.save_persisted();
        }
    }

    pub async fn update_settings(&self, req: &SettingsUpdateRequest) -> Result<AppConfig> {
        let mut cfg = self.runtime_config.write().await;
        let before = cfg.clone();
        cfg.apply_settings_update(req);
        cfg.save_persisted()?;

        let mut conn = self.connection_status.write().await;
        if before.mybox_pat != cfg.mybox_pat || before.mybox_api_base != cfg.mybox_api_base {
            conn.remove("mybox");
        }
        if before.gdrive_access_token != cfg.gdrive_access_token
            || before.gdrive_api_base != cfg.gdrive_api_base
        {
            conn.remove("gdrive");
        }
        if before.takeout_dir != cfg.takeout_dir {
            conn.remove("takeout");
        }
        if before.immich_url != cfg.immich_url || before.immich_api_key != cfg.immich_api_key {
            conn.remove("immich");
        }
        if before.enable_nextcloud != cfg.enable_nextcloud
            || before.nextcloud_url != cfg.nextcloud_url
            || before.nextcloud_user != cfg.nextcloud_user
            || before.nextcloud_app_password != cfg.nextcloud_app_password
            || before.nextcloud_base_dir != cfg.nextcloud_base_dir
        {
            conn.remove("nextcloud");
        }

        Ok(cfg.clone())
    }

    pub async fn record_connection_result(
        &self,
        service: &str,
        ok: bool,
        message: impl Into<String>,
    ) -> ServiceConnectionStatus {
        let status = ServiceConnectionStatus {
            verified: true,
            ok,
            message: message.into(),
            checked_at: Some(Utc::now().to_rfc3339()),
        };
        self.connection_status
            .write()
            .await
            .insert(service.to_string(), status.clone());
        status
    }

    pub async fn readiness_summary(&self) -> ReadinessSummary {
        let cfg = self.runtime_config.read().await.clone();
        let summary = cfg.summary();
        let conn = self.connection_status.read().await;

        let is_ok = |key: &str| conn.get(key).map(|s| s.verified && s.ok).unwrap_or(false);

        let immich_verified = summary.immich_configured && is_ok("immich");
        let nextcloud_verified = if !summary.enable_nextcloud {
            true
        } else {
            summary.nextcloud_configured && is_ok("nextcloud")
        };
        let targets_ready = immich_verified && nextcloud_verified;

        let mut verified_sources = Vec::new();
        if summary.mybox_configured && is_ok("mybox") {
            verified_sources.push("mybox".to_string());
        }
        let has_takeout_zips = !list_takeout_archives(&cfg.takeout_dir).is_empty();
        if has_takeout_zips && is_ok("takeout") {
            verified_sources.push("takeout".to_string());
        }

        let verified_cloud_sources: Vec<String> = verified_sources
            .iter()
            .filter(|s| s.as_str() == "mybox")
            .cloned()
            .collect();

        let sync_tab_enabled = targets_ready && !verified_sources.is_empty();
        let delete_tab_enabled = targets_ready && !verified_cloud_sources.is_empty();

        ReadinessSummary {
            immich_verified,
            nextcloud_verified,
            targets_ready,
            sync_tab_enabled,
            delete_tab_enabled,
            verified_sources,
            verified_cloud_sources,
        }
    }

    pub fn is_stopping(&self) -> bool {
        self.stop_flag.load(Ordering::SeqCst)
    }

    pub async fn begin_phase(&self, source: &str, phase: JobState, total: u64) {
        self.stop_flag.store(false, Ordering::SeqCst);
        *self.job_state.write().await = phase;
        *self.active_source.write().await = Some(source.to_string());
        *self.current_file.write().await = None;
        self.processed_files.store(0, Ordering::SeqCst);
        self.total_files.store(total, Ordering::SeqCst);
        *self.job_started_at.write().await = Some(Instant::now());
    }

    pub async fn finish_job(&self, source: &str, message: impl Into<String>) {
        if !self.batch_active.load(Ordering::SeqCst) {
            *self.job_state.write().await = JobState::Idle;
        }
        *self.current_file.write().await = None;
        let current = *self.job_state.read().await;
        self.emit_event(source, current, None, message).await;
    }

    pub async fn request_stop(&self) -> bool {
        let current = *self.job_state.read().await;
        if current == JobState::Idle {
            return false;
        }
        self.stop_flag.store(true, Ordering::SeqCst);
        *self.job_state.write().await = JobState::Stopping;
        let source = self
            .active_source
            .read()
            .await
            .clone()
            .unwrap_or_else(|| "mybox".to_string());
        self.emit_event(
            &source,
            JobState::Stopping,
            None,
            "사용자 안전 정지 요청 수신 — 현재 처리 중인 파일까지만 마치고 대기 상태로 전환합니다.",
        )
        .await;
        true
    }

    pub async fn speed_fps(&self) -> f64 {
        let processed = self.processed_files.load(Ordering::SeqCst);
        if processed == 0 {
            return 0.0;
        }
        if let Some(started) = *self.job_started_at.read().await {
            let secs = started.elapsed().as_secs_f64();
            if secs > 0.05 {
                return processed as f64 / secs;
            }
        }
        0.0
    }

    pub async fn emit_event(
        &self,
        source: &str,
        state: JobState,
        current_file: Option<String>,
        message: impl Into<String>,
    ) {
        if let Some(ref f) = current_file {
            *self.current_file.write().await = Some(f.clone());
        }
        let ev = ProgressEvent {
            timestamp: Utc::now().to_rfc3339(),
            source: source.to_string(),
            state,
            current_file,
            processed_files: self.processed_files.load(Ordering::SeqCst),
            total_files: self.total_files.load(Ordering::SeqCst),
            speed_fps: self.speed_fps().await,
            message: message.into(),
        };

        {
            let mut logs = self.recent_logs.write().await;
            if logs.len() >= MAX_RECENT_LOGS {
                logs.pop_front();
            }
            logs.push_back(ev.clone());
        }

        let _ = self.event_tx.send(ev);
    }
}
