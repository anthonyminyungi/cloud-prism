use crate::config::SECRET_MASK;
use crate::limiter::RateLimiter;
use crate::pipeline::SyncPipeline;
use crate::sources::gdrive::GoogleDriveProvider;
use crate::sources::mybox::MyboxProvider;
use crate::sources::takeout::GoogleTakeoutProvider;
use crate::sources::SourceProvider;
use crate::state::{AppState, JobState};
use crate::targets::immich::ImmichTarget;
use crate::targets::nextcloud::NextcloudTarget;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

const UI_HTML: &str = include_str!("ui.html");

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(serve_ui))
        .route("/api/settings", get(get_settings).put(put_settings))
        .route("/api/connections/test", post(test_connections))
        .route("/api/status", get(get_status))
        .route("/api/jobs/start", post(start_job))
        .route("/api/jobs/stop", post(stop_job))
        .route("/api/jobs/retry-failed", post(retry_failed))
        .route("/api/events", get(sse_events))
        .with_state(state)
}

async fn serve_ui() -> Html<&'static str> {
    Html(UI_HTML)
}

async fn get_settings(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.effective_config().await {
        Ok(cfg) => (StatusCode::OK, Json(json!(cfg.masked()))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn put_settings(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<HashMap<String, Value>>,
) -> impl IntoResponse {
    let mut to_save = HashMap::new();
    for (k, v) in payload {
        let s = match v {
            Value::String(s) => s,
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => continue,
        };
        let trimmed = s.trim();
        if !trimmed.is_empty() && trimmed != SECRET_MASK {
            to_save.insert(k, trimmed.to_string());
        }
    }

    if let Err(e) = state.db.save_settings(&to_save).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response();
    }

    match state.effective_config().await {
        Ok(cfg) => (StatusCode::OK, Json(json!(cfg.masked()))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Debug, Serialize)]
struct ServiceTestResult {
    ok: bool,
    message: String,
}

async fn test_connections(
    State(state): State<Arc<AppState>>,
    Json(overrides): Json<HashMap<String, String>>,
) -> impl IntoResponse {
    let mut cfg = match state.effective_config().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    };
    cfg.apply_db_overrides(&overrides);

    let limiter = RateLimiter::new(cfg.rate_limit_rps, 1);
    let mut results: HashMap<String, ServiceTestResult> = HashMap::new();

    // 1. MYBOX
    if !cfg.mybox_pat.is_empty() {
        let mybox = MyboxProvider::new(
            cfg.mybox_api_base.clone(),
            cfg.mybox_pat.clone(),
            limiter.clone(),
        );
        match mybox.test_connection().await {
            Ok(msg) => {
                results.insert(
                    "mybox".to_string(),
                    ServiceTestResult {
                        ok: true,
                        message: msg,
                    },
                );
            }
            Err(err) => {
                results.insert(
                    "mybox".to_string(),
                    ServiceTestResult {
                        ok: false,
                        message: err.to_string(),
                    },
                );
            }
        }
    }

    // 2. Google Drive
    if !cfg.gdrive_access_token.is_empty() {
        let gdrive = GoogleDriveProvider::new(
            cfg.gdrive_api_base.clone(),
            cfg.gdrive_access_token.clone(),
            limiter.clone(),
        );
        match gdrive.test_connection().await {
            Ok(msg) => {
                results.insert(
                    "gdrive".to_string(),
                    ServiceTestResult {
                        ok: true,
                        message: msg,
                    },
                );
            }
            Err(err) => {
                results.insert(
                    "gdrive".to_string(),
                    ServiceTestResult {
                        ok: false,
                        message: err.to_string(),
                    },
                );
            }
        }
    }

    // 3. Google Takeout (if directory exists)
    let takeout = GoogleTakeoutProvider::new(&cfg.takeout_dir);
    if let Ok(msg) = takeout.test_connection().await {
        results.insert(
            "takeout".to_string(),
            ServiceTestResult {
                ok: true,
                message: msg,
            },
        );
    }

    // 4. Immich
    if !cfg.immich_url.is_empty() {
        let immich = ImmichTarget::new(
            cfg.immich_url.clone(),
            cfg.immich_api_key.clone(),
            limiter.clone(),
        );
        match immich.test_connection().await {
            Ok(msg) => {
                results.insert(
                    "immich".to_string(),
                    ServiceTestResult {
                        ok: true,
                        message: msg,
                    },
                );
            }
            Err(err) => {
                results.insert(
                    "immich".to_string(),
                    ServiceTestResult {
                        ok: false,
                        message: err.to_string(),
                    },
                );
            }
        }
    }

    // 5. Nextcloud
    if cfg.enable_nextcloud && !cfg.nextcloud_url.is_empty() {
        let nc = NextcloudTarget::new(
            cfg.nextcloud_url.clone(),
            cfg.nextcloud_user.clone(),
            cfg.nextcloud_app_password.clone(),
            cfg.nextcloud_base_dir.clone(),
            limiter,
        );
        match nc.test_connection().await {
            Ok(msg) => {
                results.insert(
                    "nextcloud".to_string(),
                    ServiceTestResult {
                        ok: true,
                        message: msg,
                    },
                );
            }
            Err(err) => {
                results.insert(
                    "nextcloud".to_string(),
                    ServiceTestResult {
                        ok: false,
                        message: err.to_string(),
                    },
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
        .unwrap_or_else(|| "mybox".to_string());

    let job_state = *state.job_state.read().await;
    let current_file = state.current_file.read().await.clone();
    let processed_files = state.processed_files.load(Ordering::SeqCst);
    let total_files = state.total_files.load(Ordering::SeqCst);
    let speed_fps = state.speed_fps().await;
    let stats = state.db.get_stats(&source).await.unwrap_or_default();
    let recent_failures = state
        .db
        .get_recent_failures(&source, 20)
        .await
        .unwrap_or_default();
    let recent_logs: Vec<_> = state.recent_logs.read().await.iter().cloned().collect();

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
            "stats": stats,
            "recent_failures": recent_failures,
            "recent_logs": recent_logs
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct StartJobRequest {
    source: String,
    mode: String,
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
        *js = match req.mode.as_str() {
            "scan" | "auto" => JobState::Scanning,
            "sync" => JobState::Syncing,
            "delete" => JobState::Deleting,
            _ => JobState::Scanning,
        };
    }

    let cfg = match state.effective_config().await {
        Ok(c) => c,
        Err(e) => {
            *state.job_state.write().await = JobState::Idle;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    let pipeline = match SyncPipeline::from_config(state.clone(), cfg, &req.source) {
        Ok(p) => p,
        Err(e) => {
            *state.job_state.write().await = JobState::Idle;
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    let state_clone = state.clone();
    let source = req.source.clone();
    let mode = req.mode.clone();
    let dry_run = req.dry_run;

    let handle = tokio::spawn(async move {
        let res = match mode.as_str() {
            "auto" => pipeline.run_auto().await,
            "scan" => pipeline.run_scan().await,
            "sync" => pipeline.run_sync().await,
            "delete" => pipeline.run_delete(dry_run).await,
            _ => pipeline.run_auto().await,
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
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
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
