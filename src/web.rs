use crate::limiter::RateLimiter;
use crate::pipeline::{build_source_provider, build_target_providers, SyncPipeline};
use crate::router::{SourceType, TargetType};
use crate::sources::sanitize_filename;
use crate::sources::takeout::list_takeout_archives;
use crate::state::{AppState, JobState};
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::convert::Infallible;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

const UI_HTML: &str = include_str!("ui.html");
const RECENT_FAILURES_LIMIT: i64 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobMode {
    Auto,
    Scan,
    Sync,
    Delete,
}

impl JobMode {
    pub fn initial_state(&self) -> JobState {
        match self {
            JobMode::Auto | JobMode::Scan => JobState::Scanning,
            JobMode::Sync => JobState::Syncing,
            JobMode::Delete => JobState::Deleting,
        }
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(serve_ui))
        .route("/api/connections/test", post(test_connections))
        .route("/api/status", get(get_status))
        .route("/api/jobs/start", post(start_job))
        .route("/api/jobs/stop", post(stop_job))
        .route("/api/jobs/retry-failed", post(retry_failed))
        .route("/api/takeout/path", post(set_takeout_path))
        .route(
            "/api/takeout/upload",
            post(upload_takeout_zip).layer(DefaultBodyLimit::disable()),
        )
        .route("/api/events", get(sse_events))
        .with_state(state)
}

async fn serve_ui() -> Html<&'static str> {
    Html(UI_HTML)
}

#[derive(Debug, Serialize)]
struct ServiceTestResult {
    ok: bool,
    message: String,
}

impl ServiceTestResult {
    fn from_result(res: anyhow::Result<String>) -> Self {
        match res {
            Ok(message) => Self { ok: true, message },
            Err(err) => Self {
                ok: false,
                message: err.to_string(),
            },
        }
    }
}

async fn test_connections(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cfg = state.effective_config().await;
    let limiter = RateLimiter::new(cfg.rate_limit_rps, 1);
    let mut results: HashMap<String, ServiceTestResult> = HashMap::new();

    let configured_sources: Vec<(SourceType, bool)> = vec![
        (SourceType::Mybox, !cfg.mybox_pat.is_empty()),
        (SourceType::Gdrive, !cfg.gdrive_access_token.is_empty()),
        (
            SourceType::Takeout,
            std::path::Path::new(&cfg.takeout_dir).exists(),
        ),
    ];

    for (src_type, should_test) in configured_sources {
        if should_test {
            let provider = build_source_provider(&cfg, src_type, limiter.clone());
            results.insert(
                src_type.as_str().to_string(),
                ServiceTestResult::from_result(provider.test_connection().await),
            );
        }
    }

    let targets = build_target_providers(&cfg, limiter);
    let configured_targets: Vec<(TargetType, bool)> = vec![
        (TargetType::Immich, !cfg.immich_url.is_empty()),
        (
            TargetType::Nextcloud,
            cfg.enable_nextcloud && !cfg.nextcloud_url.is_empty(),
        ),
    ];

    for (target_type, should_test) in configured_targets {
        if should_test {
            if let Some(target) = targets.get(&target_type) {
                results.insert(
                    target_type.as_str().to_string(),
                    ServiceTestResult::from_result(target.test_connection().await),
                );
            }
        }
    }

    (StatusCode::OK, Json(json!({ "results": results }))).into_response()
}

#[derive(Debug, Deserialize)]
struct StatusQuery {
    source: Option<String>,
}

async fn get_status(
    State(state): State<Arc<AppState>>,
    Query(q): Query<StatusQuery>,
) -> impl IntoResponse {
    let active_source = state.active_source.read().await.clone();
    let source = q
        .source
        .or_else(|| active_source.clone())
        .unwrap_or_else(|| SourceType::Mybox.as_str().to_string());

    let cfg = state.effective_config().await;
    let job_state = *state.job_state.read().await;
    let current_file = state.current_file.read().await.clone();
    let processed_files = state.processed_files.load(Ordering::SeqCst);
    let total_files = state.total_files.load(Ordering::SeqCst);
    let speed_fps = state.speed_fps().await;
    let stats = state.db.get_stats(&source).await.unwrap_or_default();
    let recent_failures = state
        .db
        .get_recent_failures(&source, RECENT_FAILURES_LIMIT)
        .await
        .unwrap_or_default();
    let recent_logs: Vec<_> = state.recent_logs.read().await.iter().cloned().collect();

    let takeout_archives = list_takeout_archives(&cfg.takeout_dir);
    let takeout_dir_exists = Path::new(&cfg.takeout_dir).exists();

    (
        StatusCode::OK,
        Json(json!({
            "state": job_state,
            "active_source": active_source,
            "source": source,
            "current_file": current_file,
            "processed_files": processed_files,
            "total_files": total_files,
            "speed_fps": speed_fps,
            "config_summary": cfg.summary(),
            "takeout_info": {
                "dir": cfg.takeout_dir,
                "exists": takeout_dir_exists,
                "archives": takeout_archives
            },
            "stats": stats,
            "recent_failures": recent_failures,
            "recent_logs": recent_logs
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct SetTakeoutPathRequest {
    takeout_dir: String,
}

async fn set_takeout_path(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SetTakeoutPathRequest>,
) -> impl IntoResponse {
    let trimmed = req.takeout_dir.trim();
    if trimmed.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "Takeout 경로가 비어 있습니다.");
    }

    if let Err(err) = tokio::fs::create_dir_all(trimmed).await {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!("Takeout 디렉터리를 열거나 생성할 수 없습니다 ({}): {}", trimmed, err),
        );
    }

    state.set_takeout_dir(trimmed.to_string()).await;
    let archives = list_takeout_archives(trimmed);
    let current_state = *state.job_state.read().await;
    state
        .emit_event(
            "takeout",
            current_state,
            None,
            format!(
                "[takeout] 아카이브 경로 변경: {} (ZIP {}개 인식됨)",
                trimmed,
                archives.len()
            ),
        )
        .await;

    (
        StatusCode::OK,
        Json(json!({
            "takeout_dir": trimmed,
            "exists": true,
            "archives": archives
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct UploadTakeoutQuery {
    filename: Option<String>,
}

async fn upload_takeout_zip(
    State(state): State<Arc<AppState>>,
    Query(q): Query<UploadTakeoutQuery>,
    body: Body,
) -> impl IntoResponse {
    let raw_name = q
        .filename
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("takeout-upload.zip");

    let safe_name = sanitize_filename(raw_name);
    if !safe_name.to_ascii_lowercase().ends_with(".zip") {
        return error_response(
            StatusCode::BAD_REQUEST,
            "Google Takeout 아카이브는 .zip 확장자 파일만 업로드할 수 있습니다.",
        );
    }

    let cfg = state.effective_config().await;
    if let Err(err) = tokio::fs::create_dir_all(&cfg.takeout_dir).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Takeout 폴더 생성 실패 ({}): {}", cfg.takeout_dir, err),
        );
    }

    let dest_path = Path::new(&cfg.takeout_dir).join(&safe_name);
    let mut file = match tokio::fs::File::create(&dest_path).await {
        Ok(f) => f,
        Err(err) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("ZIP 파일 생성 실패: {}", err),
            )
        }
    };

    let mut stream = body.into_data_stream();
    let mut written_bytes: u64 = 0;

    while let Some(chunk_res) = stream.next().await {
        match chunk_res {
            Ok(chunk) => {
                if let Err(err) = file.write_all(&chunk).await {
                    let _ = tokio::fs::remove_file(&dest_path).await;
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("ZIP 파일 쓰기 실패: {}", err),
                    );
                }
                written_bytes += chunk.len() as u64;
            }
            Err(err) => {
                let _ = tokio::fs::remove_file(&dest_path).await;
                return error_response(
                    StatusCode::BAD_REQUEST,
                    format!("업로드 스트림 수신 중단: {}", err),
                );
            }
        }
    }

    let _ = file.flush().await;
    let archives = list_takeout_archives(&cfg.takeout_dir);
    let current_state = *state.job_state.read().await;
    state
        .emit_event(
            "takeout",
            current_state,
            None,
            format!(
                "[takeout] ZIP 업로드 완료: {} ({} bytes)",
                safe_name, written_bytes
            ),
        )
        .await;

    (
        StatusCode::OK,
        Json(json!({
            "uploaded": safe_name,
            "size_bytes": written_bytes,
            "takeout_dir": cfg.takeout_dir,
            "archives": archives
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct StartJobRequest {
    source: String,
    mode: JobMode,
    #[serde(default)]
    dry_run: bool,
}

async fn start_job(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StartJobRequest>,
) -> impl IntoResponse {
    {
        let mut js = state.job_state.write().await;
        if *js != JobState::Idle {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "Job is already running",
                    "state": *js
                })),
            )
                .into_response();
        }
        *js = req.mode.initial_state();
    }

    let cfg = state.effective_config().await;
    let pipeline = match SyncPipeline::from_config(state.clone(), cfg, &req.source) {
        Ok(p) => p,
        Err(e) => {
            *state.job_state.write().await = JobState::Idle;
            return error_response(StatusCode::BAD_REQUEST, e);
        }
    };

    let state_clone = state.clone();
    let source = req.source.clone();
    let mode = req.mode;
    let dry_run = req.dry_run;

    let handle = tokio::spawn(async move {
        let res = match mode {
            JobMode::Auto => pipeline.run_auto().await,
            JobMode::Scan => pipeline.run_scan().await,
            JobMode::Sync => pipeline.run_sync().await,
            JobMode::Delete => pipeline.run_delete(dry_run).await,
        };

        if let Err(err) = res {
            state_clone
                .finish_job(&source, format!("작업 중 오류 발생: {}", err))
                .await;
        }
    });

    *state.job_handle.lock().await = Some(handle);

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "started": true,
            "source": req.source,
            "mode": req.mode,
            "dry_run": req.dry_run
        })),
    )
        .into_response()
}

async fn stop_job(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let stopping = state.request_stop().await;
    (StatusCode::OK, Json(json!({ "stopping": stopping }))).into_response()
}

#[derive(Debug, Deserialize)]
struct RetryFailedRequest {
    source: String,
}

async fn retry_failed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RetryFailedRequest>,
) -> impl IntoResponse {
    match state.db.reset_failed_files(&req.source).await {
        Ok(count) => (StatusCode::OK, Json(json!({ "reset_count": count }))).into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn sse_events(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.event_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|msg| match msg {
        Ok(ev) => {
            let data = serde_json::to_string(&ev).unwrap_or_else(|_| "{}".to_string());
            Some(Ok(Event::default().data(data)))
        }
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn error_response(status: StatusCode, err: impl std::fmt::Display) -> axum::response::Response {
    (status, Json(json!({ "error": err.to_string() }))).into_response()
}
