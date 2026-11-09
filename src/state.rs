use crate::config::{AppConfig, SettingsUpdateRequest};
use crate::db::CheckpointDB;
use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
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
    pub job_state: RwLock<JobState>,
    pub active_source: RwLock<Option<String>>,
    pub current_file: RwLock<Option<String>>,
    pub processed_files: AtomicU64,
    pub total_files: AtomicU64,
    pub job_started_at: RwLock<Option<Instant>>,
    pub stop_flag: Arc<AtomicBool>,
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
            job_state: RwLock::new(JobState::Idle),
            active_source: RwLock::new(None),
            current_file: RwLock::new(None),
            processed_files: AtomicU64::new(0),
            total_files: AtomicU64::new(0),
            job_started_at: RwLock::new(None),
            stop_flag: Arc::new(AtomicBool::new(false)),
            recent_logs: RwLock::new(VecDeque::with_capacity(MAX_RECENT_LOGS)),
            event_tx,
            job_handle: Mutex::new(None),
        })
    }

    pub async fn effective_config(&self) -> AppConfig {
        self.runtime_config.read().await.clone()
    }

    pub async fn set_takeout_dir(&self, dir: String) {
        let mut cfg = self.runtime_config.write().await;
        cfg.takeout_dir = dir;
        let _ = cfg.save_persisted();
    }

    pub async fn update_settings(&self, req: &SettingsUpdateRequest) -> Result<AppConfig> {
        let mut cfg = self.runtime_config.write().await;
        cfg.apply_settings_update(req);
        cfg.save_persisted()?;
        Ok(cfg.clone())
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
        *self.job_state.write().await = JobState::Idle;
        *self.current_file.write().await = None;
        self.stop_flag.store(false, Ordering::SeqCst);
        self.emit_event(source, JobState::Idle, None, message).await;
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
