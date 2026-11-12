use crate::config::SettingsUpdateRequest;
use crate::limiter::{format_error_chain, RateLimiter};
use crate::pipeline::{
    build_source_provider, build_target_providers, run_merge_immich_duplicates_with_limit,
    run_move_immich_album_to_nextcloud_with_limit, run_repair_nextcloud_anomalies_with_limit,
    SyncPipeline,
};
use crate::router::{SourceType, TargetType};
use crate::sources::sanitize_filename;
use crate::sources::takeout::list_takeout_archives;
use crate::state::{AppState, JobState, ServiceConnectionStatus};
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
        .route("/api/settings", post(update_settings))
        .route("/api/status", get(get_status))
        .route("/api/jobs/start", post(start_job))
        .route("/api/jobs/repair", post(start_repair_job))
        .route("/api/jobs/stop", post(stop_job))
        .route("/api/jobs/retry-failed", post(retry_failed))
        .route("/api/takeout/path", post(set_takeout_path))
        .route("/api/takeout/fetch-url", post(fetch_takeout_url))
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

pub async fn verify_single_service(
    state: &Arc<AppState>,
    service: &str,
) -> Option<ServiceConnectionStatus> {
    let cfg = state.effective_config().await;
    let limiter = RateLimiter::new(cfg.rate_limit_rps, 1);

    let (ok, msg) = match service {
        "mybox" => {
            let provider = build_source_provider(&cfg, SourceType::Mybox, limiter);
            match provider.test_connection().await {
                Ok(m) => (true, m),
                Err(e) => (false, e.to_string()),
            }
        }
        "gdrive" => {
            let provider = build_source_provider(&cfg, SourceType::Gdrive, limiter);
            match provider.test_connection().await {
                Ok(m) => (true, m),
                Err(e) => (false, e.to_string()),
            }
        }
        "takeout" => {
            let dir_path = Path::new(&cfg.takeout_dir);
            if !dir_path.exists() {
                (
                    false,
                    format!("경로가 존재하지 않습니다: {}", cfg.takeout_dir),
                )
            } else {
                let archives = list_takeout_archives(&cfg.takeout_dir);
                if archives.is_empty() {
                    (
                        false,
                        format!(
                            "폴더({})는 확인되었으나 .zip 파일이 없습니다.",
                            cfg.takeout_dir
                        ),
                    )
                } else {
                    (
                        true,
                        format!(
                            "Google Takeout 준비 완료 (ZIP 아카이브 {}개 확인됨)",
                            archives.len()
                        ),
                    )
                }
            }
        }
        "immich" => {
            let targets = build_target_providers(&cfg, limiter);
            match targets.get(&TargetType::Immich) {
                Some(target) => match target.test_connection().await {
                    Ok(m) => (true, m),
                    Err(e) => (false, e.to_string()),
                },
                None => (false, "Immich 타겟 초기화 실패".to_string()),
            }
        }
        "nextcloud" => {
            if !cfg.enable_nextcloud {
                (true, "Nextcloud 사용 안 함 (비활성화 상태)".to_string())
            } else {
                let targets = build_target_providers(&cfg, limiter);
                match targets.get(&TargetType::Nextcloud) {
                    Some(target) => match target.test_connection().await {
                        Ok(m) => (true, m),
                        Err(e) => {
                            if cfg.nextcloud_url.starts_with("http://") {
                                let https_url =
                                    cfg.nextcloud_url.replacen("http://", "https://", 1);
                                let mut https_cfg = cfg.clone();
                                https_cfg.nextcloud_url = https_url.clone();
                                let quick_limiter = RateLimiter::new(cfg.rate_limit_rps, 0);
                                let https_targets =
                                    build_target_providers(&https_cfg, quick_limiter);
                                if let Some(https_target) =
                                    https_targets.get(&TargetType::Nextcloud)
                                {
                                    if let Ok(m) = https_target.test_connection().await {
                                        let _ = state
                                            .update_settings(&SettingsUpdateRequest {
                                                nextcloud_url: Some(https_url),
                                                ..Default::default()
                                            })
                                            .await;
                                        (true, m)
                                    } else {
                                        (false, e.to_string())
                                    }
                                } else {
                                    (false, e.to_string())
                                }
                            } else {
                                (false, e.to_string())
                            }
                        }
                    },
                    None => (false, "Nextcloud 타겟 초기화 실패".to_string()),
                }
            }
        }
        _ => return None,
    };

    Some(state.record_connection_result(service, ok, msg).await)
}

pub async fn verify_all_configured(
    state: &Arc<AppState>,
) -> HashMap<String, ServiceConnectionStatus> {
    let cfg = state.effective_config().await;
    let mut results = HashMap::new();

    let candidates = [
        ("mybox", !cfg.mybox_pat.is_empty()),
        ("takeout", Path::new(&cfg.takeout_dir).exists()),
        ("immich", !cfg.immich_url.is_empty()),
        (
            "nextcloud",
            cfg.enable_nextcloud && !cfg.nextcloud_url.is_empty(),
        ),
    ];

    for (svc, should_test) in candidates {
        if should_test {
            if let Some(res) = verify_single_service(state, svc).await {
                results.insert(svc.to_string(), res);
            }
        }
    }

    results
}

#[derive(Debug, Deserialize)]
struct TestConnectionsQuery {
    service: Option<String>,
}

async fn test_connections(
    State(state): State<Arc<AppState>>,
    Query(q): Query<TestConnectionsQuery>,
) -> impl IntoResponse {
    let mut results = HashMap::new();

    if let Some(svc) = q
        .service
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        if let Some(res) = verify_single_service(&state, svc).await {
            results.insert(svc.to_string(), res);
        } else {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("알 수 없는 서비스: {}", svc),
            );
        }
    } else {
        results = verify_all_configured(&state).await;
    }

    let connections = state.connection_status.read().await.clone();
    let readiness = state.readiness_summary().await;

    (
        StatusCode::OK,
        Json(json!({
            "results": results,
            "connections": connections,
            "readiness": readiness
        })),
    )
        .into_response()
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

    let mybox_stats = state.db.get_stats("mybox").await.unwrap_or_default();
    let gdrive_stats = state.db.get_stats("gdrive").await.unwrap_or_default();
    let takeout_stats = state.db.get_stats("takeout").await.unwrap_or_default();

    let stats = match source.as_str() {
        "gdrive" => gdrive_stats.clone(),
        "takeout" => takeout_stats.clone(),
        _ => mybox_stats.clone(),
    };

    let recent_failures = state
        .db
        .get_recent_failures(&source, RECENT_FAILURES_LIMIT)
        .await
        .unwrap_or_default();

    let mut all_failures = Vec::new();
    for src_key in ["mybox", "gdrive", "takeout"] {
        if let Ok(list) = state.db.get_recent_failures(src_key, 10).await {
            for item in list {
                all_failures.push(json!({
                    "source": src_key,
                    "resource_id": item.resource_id,
                    "name": item.name,
                    "parent_path": item.parent_path,
                    "target_type": item.target_type,
                    "retry_count": item.retry_count,
                    "error_msg": item.error_msg
                }));
            }
        }
    }

    let recent_logs: Vec<_> = state.recent_logs.read().await.iter().cloned().collect();
    let takeout_archives = list_takeout_archives(&cfg.takeout_dir);
    let takeout_dir_exists = Path::new(&cfg.takeout_dir).exists();
    let connections = state.connection_status.read().await.clone();
    let readiness = state.readiness_summary().await;
    let nextcloud_anomaly_count = state
        .db
        .count_nextcloud_repair_candidates()
        .await
        .unwrap_or(0);

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
            "connections": connections,
            "readiness": readiness,
            "takeout_info": {
                "dir": cfg.takeout_dir,
                "exists": takeout_dir_exists,
                "archives": takeout_archives
            },
            "repair_info": {
                "nextcloud_anomaly_count": nextcloud_anomaly_count,
                "default_album_name": "to-nextcloud"
            },
            "stats": stats,
            "all_stats": {
                "mybox": mybox_stats,
                "gdrive": gdrive_stats,
                "takeout": takeout_stats
            },
            "recent_failures": recent_failures,
            "all_failures": all_failures,
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
            format!(
                "Takeout 디렉터리를 열거나 생성할 수 없습니다 ({}): {}",
                trimmed, err
            ),
        );
    }

    state.set_takeout_dir(trimmed.to_string()).await;
    let verification = verify_single_service(&state, "takeout").await;
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

    let connections = state.connection_status.read().await.clone();
    let readiness = state.readiness_summary().await;

    (
        StatusCode::OK,
        Json(json!({
            "takeout_dir": trimmed,
            "exists": true,
            "archives": archives,
            "verification": verification,
            "connections": connections,
            "readiness": readiness
        })),
    )
        .into_response()
}

async fn update_settings(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SettingsUpdateRequest>,
) -> impl IntoResponse {
    let verify_target = req.verify_service.clone();
    match state.update_settings(&req).await {
        Ok(_) => {
            let verification = if let Some(ref svc) = verify_target {
                verify_single_service(&state, svc).await
            } else {
                None
            };
            let latest_cfg = state.effective_config().await;

            let current_state = *state.job_state.read().await;
            let log_msg = match (&verify_target, &verification) {
                (Some(svc), Some(v)) => format!(
                    "[{}] 설정 저장 및 연결 확인: {} ({})",
                    svc,
                    if v.ok { "정상" } else { "실패" },
                    v.message
                ),
                _ => "설정이 저장되어 반영되었습니다.".to_string(),
            };
            state
                .emit_event("system", current_state, None, log_msg)
                .await;

            let connections = state.connection_status.read().await.clone();
            let readiness = state.readiness_summary().await;

            (
                StatusCode::OK,
                Json(json!({
                    "saved": true,
                    "config_summary": latest_cfg.summary(),
                    "verification": verification,
                    "connections": connections,
                    "readiness": readiness
                })),
            )
                .into_response()
        }
        Err(err) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("설정 저장 실패: {}", err),
        ),
    }
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
    let part_path = Path::new(&cfg.takeout_dir).join(format!("{}.part", safe_name));
    let mut file = match tokio::fs::File::create(&part_path).await {
        Ok(f) => f,
        Err(err) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("ZIP 임시 파일 생성 실패: {}", err),
            )
        }
    };

    let mut stream = body.into_data_stream();
    let mut written_bytes: u64 = 0;

    while let Some(chunk_res) = stream.next().await {
        match chunk_res {
            Ok(chunk) => {
                if let Err(err) = file.write_all(&chunk).await {
                    drop(file);
                    let _ = tokio::fs::remove_file(&part_path).await;
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("ZIP 파일 쓰기 실패: {}", err),
                    );
                }
                written_bytes += chunk.len() as u64;
            }
            Err(err) => {
                drop(file);
                let _ = tokio::fs::remove_file(&part_path).await;
                return error_response(
                    StatusCode::BAD_REQUEST,
                    format!("업로드 스트림 수신 중단: {}", err),
                );
            }
        }
    }

    if let Err(err) = file.flush().await {
        drop(file);
        let _ = tokio::fs::remove_file(&part_path).await;
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("ZIP 파일 플러시 실패: {}", err),
        );
    }
    drop(file);

    if dest_path.exists() {
        let _ = tokio::fs::remove_file(&dest_path).await;
    }
    if let Err(err) = tokio::fs::rename(&part_path, &dest_path).await {
        let _ = tokio::fs::remove_file(&part_path).await;
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("ZIP 파일 원자적 이름 변경 실패: {}", err),
        );
    }
    let verification = verify_single_service(&state, "takeout").await;
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

    let connections = state.connection_status.read().await.clone();
    let readiness = state.readiness_summary().await;

    (
        StatusCode::OK,
        Json(json!({
            "uploaded": safe_name,
            "size_bytes": written_bytes,
            "takeout_dir": cfg.takeout_dir,
            "archives": archives,
            "verification": verification,
            "connections": connections,
            "readiness": readiness
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct FetchTakeoutUrlRequest {
    url: String,
    #[serde(default)]
    takeout_dir: Option<String>,
    #[serde(default)]
    filename: Option<String>,
}

fn derive_takeout_zip_filename(
    url: &str,
    content_disposition: Option<&str>,
    explicit_name: Option<&str>,
) -> String {
    let ensure_zip = |raw: &str| -> String {
        let clean = sanitize_filename(raw.trim());
        if clean.to_ascii_lowercase().ends_with(".zip") {
            clean
        } else {
            format!("{}.zip", clean)
        }
    };

    if let Some(name) = explicit_name.map(str::trim).filter(|s| !s.is_empty()) {
        return ensure_zip(name);
    }

    if let Some(cd) = content_disposition {
        for part in cd.split(';') {
            let p = part.trim();
            if let Some(val) = p.strip_prefix("filename=") {
                let unquoted = val.trim_matches('"').trim_matches('\'').trim();
                if !unquoted.is_empty() {
                    return ensure_zip(unquoted);
                }
            }
        }
    }

    let without_query = url.split('?').next().unwrap_or(url);
    if let Some(seg) = without_query.rsplit('/').next() {
        let decoded = percent_encoding::percent_decode_str(seg).decode_utf8_lossy();
        if decoded.to_ascii_lowercase().ends_with(".zip") && decoded.len() > 4 {
            return ensure_zip(&decoded);
        }
    }

    format!("takeout-{}.zip", chrono::Utc::now().format("%Y%m%d-%H%M%S"))
}

async fn fetch_takeout_url(
    State(state): State<Arc<AppState>>,
    Json(req): Json<FetchTakeoutUrlRequest>,
) -> impl IntoResponse {
    let url = req.url.trim().to_string();
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "유효한 다운로드 URL(http:// 또는 https://)을 입력해 주세요.",
        );
    }

    if let Some(dir) = req
        .takeout_dir
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        state.set_takeout_dir(dir.to_string()).await;
    }

    {
        let mut js = state.job_state.write().await;
        if *js != JobState::Idle {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "다른 작업이 실행 중입니다. 완료 후 다시 시도해 주세요.",
                    "state": *js
                })),
            )
                .into_response();
        }
        *js = JobState::Scanning;
    }

    let cfg = state.effective_config().await;
    if let Err(err) = tokio::fs::create_dir_all(&cfg.takeout_dir).await {
        *state.job_state.write().await = JobState::Idle;
        return error_response(
            StatusCode::BAD_REQUEST,
            format!(
                "Takeout 저장 디렉터리를 생성할 수 없습니다 ({}): {}",
                cfg.takeout_dir, err
            ),
        );
    }

    let client = reqwest::Client::builder()
        .no_proxy()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(err) => {
            *state.job_state.write().await = JobState::Idle;
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("다운로드 URL 연결 실패: {}", format_error_chain(&err)),
            );
        }
    };

    if !resp.status().is_success() {
        let status = resp.status();
        *state.job_state.write().await = JobState::Idle;
        return error_response(
            StatusCode::BAD_REQUEST,
            format!(
                "다운로드 URL 응답 오류 (HTTP {}). 링크가 만료되었거나 인증이 필요합니다.",
                status
            ),
        );
    }

    let total_bytes = resp.content_length().unwrap_or(0);
    let content_disp = resp
        .headers()
        .get(reqwest::header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let safe_name =
        derive_takeout_zip_filename(&url, content_disp.as_deref(), req.filename.as_deref());

    let mut stream = resp.bytes_stream();
    let mut initial_buf = Vec::new();
    while initial_buf.len() < 4 {
        match stream.next().await {
            Some(Ok(chunk)) => {
                if !chunk.is_empty() {
                    initial_buf.extend_from_slice(&chunk);
                }
            }
            Some(Err(err)) => {
                *state.job_state.write().await = JobState::Idle;
                return error_response(
                    StatusCode::BAD_REQUEST,
                    format!("다운로드 스트림 읽기 실패: {}", format_error_chain(&err)),
                );
            }
            None => break,
        }
    }

    if initial_buf.len() < 4 || &initial_buf[..2] != b"PK" {
        *state.job_state.write().await = JobState::Idle;
        return error_response(
            StatusCode::BAD_REQUEST,
            "ZIP 파일 스트림이 아닌 웹페이지(Google 로그인 화면 등)가 반환되었습니다. 이메일 내 버튼 주소 대신, 브라우저에서 다운로드를 시작(즉시 일시정지 가능)한 뒤 다운로드 목록(Ctrl+J)에서 복사한 실제 다운로드 링크(googleusercontent.com)를 입력해 주세요.",
        );
    }

    let dest_path = Path::new(&cfg.takeout_dir).join(&safe_name);
    let part_path = Path::new(&cfg.takeout_dir).join(format!("{}.part", safe_name));
    let mut file = match tokio::fs::File::create(&part_path).await {
        Ok(f) => f,
        Err(err) => {
            *state.job_state.write().await = JobState::Idle;
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("임시 ZIP 파일 생성 실패: {}", err),
            );
        }
    };

    if let Err(err) = file.write_all(&initial_buf).await {
        let _ = tokio::fs::remove_file(&part_path).await;
        *state.job_state.write().await = JobState::Idle;
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("임시 ZIP 파일 쓰기 실패: {}", err),
        );
    }

    state.stop_flag.store(false, Ordering::SeqCst);
    state
        .begin_phase("takeout", JobState::Scanning, total_bytes)
        .await;
    state
        .processed_files
        .store(initial_buf.len() as u64, Ordering::SeqCst);
    state
        .emit_event(
            "takeout",
            JobState::Scanning,
            Some(safe_name.clone()),
            format!(
                "[takeout] URL에서 NAS로 ZIP 직접 다운로드 시작: {}",
                safe_name
            ),
        )
        .await;

    let state_clone = state.clone();
    let name_for_task = safe_name.clone();
    let initial_written = initial_buf.len() as u64;

    let handle = tokio::spawn(async move {
        let mut written = initial_written;
        let mut last_logged_bytes = written;
        const LOG_STEP_BYTES: u64 = 10 * 1024 * 1024; // 10 MB

        while let Some(chunk_res) = stream.next().await {
            if state_clone.is_stopping() {
                drop(file);
                let _ = tokio::fs::remove_file(&part_path).await;
                state_clone
                    .finish_job(
                        "takeout",
                        format!(
                            "[takeout] 사용자 요청으로 ZIP 다운로드 중단 및 임시파일 삭제: {}",
                            name_for_task
                        ),
                    )
                    .await;
                state_clone.stop_flag.store(false, Ordering::SeqCst);
                return;
            }

            match chunk_res {
                Ok(chunk) => {
                    if let Err(err) = file.write_all(&chunk).await {
                        drop(file);
                        let _ = tokio::fs::remove_file(&part_path).await;
                        state_clone
                            .finish_job(
                                "takeout",
                                format!("[takeout] 디스크 쓰기 실패 ({}): {}", name_for_task, err),
                            )
                            .await;
                        return;
                    }
                    written += chunk.len() as u64;
                    state_clone.processed_files.store(written, Ordering::SeqCst);

                    if written.saturating_sub(last_logged_bytes) >= LOG_STEP_BYTES {
                        last_logged_bytes = written;
                        let mb = written as f64 / (1024.0 * 1024.0);
                        let msg = if total_bytes > 0 {
                            let total_mb = total_bytes as f64 / (1024.0 * 1024.0);
                            format!(
                                "[takeout] ZIP 수신 중: {} ({:.1} MB / {:.1} MB)",
                                name_for_task, mb, total_mb
                            )
                        } else {
                            format!("[takeout] ZIP 수신 중: {} ({:.1} MB)", name_for_task, mb)
                        };
                        state_clone
                            .emit_event(
                                "takeout",
                                JobState::Scanning,
                                Some(name_for_task.clone()),
                                msg,
                            )
                            .await;
                    }
                }
                Err(err) => {
                    drop(file);
                    let _ = tokio::fs::remove_file(&part_path).await;
                    state_clone
                        .finish_job(
                            "takeout",
                            format!(
                                "[takeout] 다운로드 스트림 중단 ({}): {}",
                                name_for_task,
                                format_error_chain(&err)
                            ),
                        )
                        .await;
                    return;
                }
            }
        }

        let _ = file.flush().await;
        drop(file);
        if dest_path.exists() {
            let _ = tokio::fs::remove_file(&dest_path).await;
        }
        if let Err(err) = tokio::fs::rename(&part_path, &dest_path).await {
            let _ = tokio::fs::remove_file(&part_path).await;
            state_clone
                .finish_job(
                    "takeout",
                    format!("[takeout] 다운로드 파일 저장 실패: {}", err),
                )
                .await;
            return;
        }

        let _ = verify_single_service(&state_clone, "takeout").await;
        let mb = written as f64 / (1024.0 * 1024.0);
        state_clone
            .finish_job(
                "takeout",
                format!(
                    "[takeout] URL 다운로드 및 검증 완료: {} ({:.2} MB) — 이제 [동기화] 탭에서 실행할 수 있습니다.",
                    name_for_task, mb
                ),
            )
            .await;
    });

    *state.job_handle.lock().await = Some(handle);

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "started": true,
            "filename": safe_name,
            "total_bytes": total_bytes,
            "takeout_dir": cfg.takeout_dir
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct StartJobRequest {
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    sources: Vec<String>,
    mode: JobMode,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    limit: Option<u64>,
}

async fn start_job(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StartJobRequest>,
) -> impl IntoResponse {
    let mut target_sources: Vec<String> = req
        .sources
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if target_sources.is_empty() {
        if let Some(single) = req
            .source
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            target_sources.push(single.to_string());
        }
    }

    if target_sources.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "실행할 데이터 소스가 지정되지 않았습니다.",
        );
    }

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
    let mut pipelines = Vec::with_capacity(target_sources.len());
    for src_name in &target_sources {
        match SyncPipeline::from_config(state.clone(), cfg.clone(), src_name) {
            Ok(p) => pipelines.push((src_name.clone(), p.with_limit(req.limit))),
            Err(e) => {
                *state.job_state.write().await = JobState::Idle;
                return error_response(StatusCode::BAD_REQUEST, e);
            }
        }
    }

    state.stop_flag.store(false, Ordering::SeqCst);
    let state_clone = state.clone();
    let mode = req.mode;
    let dry_run = req.dry_run;
    let total_sources = pipelines.len();

    let handle = tokio::spawn(async move {
        for (idx, (source, pipeline)) in pipelines.into_iter().enumerate() {
            let is_last = idx + 1 == total_sources;
            state_clone.batch_active.store(!is_last, Ordering::SeqCst);

            if state_clone.is_stopping() {
                state_clone.batch_active.store(false, Ordering::SeqCst);
                state_clone
                    .finish_job(&source, "일괄 실행 도중 안전 일시정지되었습니다.")
                    .await;
                state_clone.stop_flag.store(false, Ordering::SeqCst);
                return;
            }

            if total_sources > 1 {
                state_clone
                    .emit_event(
                        &source,
                        mode.initial_state(),
                        None,
                        format!(
                            "[{}/{}] '{}' 작업을 시작합니다.",
                            idx + 1,
                            total_sources,
                            source
                        ),
                    )
                    .await;
            }

            let res = match mode {
                JobMode::Auto => pipeline.run_auto().await,
                JobMode::Scan => pipeline.run_scan().await,
                JobMode::Sync => pipeline.run_sync().await,
                JobMode::Delete => pipeline.run_delete(dry_run).await,
            };

            if let Err(err) = res {
                state_clone.batch_active.store(false, Ordering::SeqCst);
                state_clone
                    .finish_job(&source, format!("작업 중 오류 발생: {}", err))
                    .await;
                state_clone.stop_flag.store(false, Ordering::SeqCst);
                return;
            }

            if !is_last && state_clone.is_stopping() {
                state_clone.batch_active.store(false, Ordering::SeqCst);
                state_clone
                    .finish_job(&source, "일괄 실행 도중 안전 일시정지되었습니다.")
                    .await;
                state_clone.stop_flag.store(false, Ordering::SeqCst);
                return;
            }
        }

        state_clone.batch_active.store(false, Ordering::SeqCst);
        state_clone.stop_flag.store(false, Ordering::SeqCst);
    });

    *state.job_handle.lock().await = Some(handle);

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "started": true,
            "sources": target_sources,
            "mode": req.mode,
            "dry_run": req.dry_run,
            "limit": req.limit
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairMode {
    Anomalies,
    AlbumToNextcloud,
    ImmichDuplicates,
    All,
}

#[derive(Debug, Deserialize)]
struct StartRepairRequest {
    mode: RepairMode,
    #[serde(default)]
    album_name: Option<String>,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    limit: Option<u64>,
}

async fn start_repair_job(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StartRepairRequest>,
) -> impl IntoResponse {
    let cfg = state.effective_config().await;
    let album_name = req
        .album_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("to-nextcloud")
        .to_string();
    let dry_run = req.dry_run;
    let limit = req.limit.filter(|&n| n > 0);

    if matches!(req.mode, RepairMode::AlbumToNextcloud | RepairMode::All) && !cfg.enable_nextcloud {
        return error_response(
            StatusCode::BAD_REQUEST,
            "Nextcloud 연동이 활성화되어 있지 않습니다. 설정 탭에서 Nextcloud를 먼저 활성화해 주세요.",
        );
    }

    if matches!(req.mode, RepairMode::ImmichDuplicates)
        && (cfg.immich_url.trim().is_empty() || cfg.immich_api_key.trim().is_empty())
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            "Immich 서버 URL 또는 API Key가 설정되어 있지 않습니다.",
        );
    }

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
        *js = JobState::Syncing;
    }

    state.stop_flag.store(false, Ordering::SeqCst);
    let state_clone = state.clone();
    let mode = req.mode;
    let album_for_task = album_name.clone();

    let handle = tokio::spawn(async move {
        let res = match mode {
            RepairMode::Anomalies => {
                run_repair_nextcloud_anomalies_with_limit(state_clone.clone(), cfg, limit).await
            }
            RepairMode::AlbumToNextcloud => {
                run_move_immich_album_to_nextcloud_with_limit(
                    state_clone.clone(),
                    cfg,
                    &album_for_task,
                    limit,
                )
                .await
            }
            RepairMode::ImmichDuplicates => {
                run_merge_immich_duplicates_with_limit(state_clone.clone(), cfg, dry_run, limit)
                    .await
            }
            RepairMode::All => {
                state_clone.batch_active.store(true, Ordering::SeqCst);
                let first = run_repair_nextcloud_anomalies_with_limit(
                    state_clone.clone(),
                    cfg.clone(),
                    limit,
                )
                .await;
                state_clone.batch_active.store(false, Ordering::SeqCst);
                match first {
                    Ok(c1) => {
                        if state_clone.is_stopping() {
                            state_clone
                                .finish_job("repair", "사후 보정 작업이 일시정지되었습니다.")
                                .await;
                            Ok(c1)
                        } else {
                            run_move_immich_album_to_nextcloud_with_limit(
                                state_clone.clone(),
                                cfg,
                                &album_for_task,
                                limit,
                            )
                            .await
                            .map(|c2| c1 + c2)
                        }
                    }
                    Err(e) => Err(e),
                }
            }
        };

        if let Err(err) = res {
            state_clone.batch_active.store(false, Ordering::SeqCst);
            state_clone
                .finish_job("repair", format!("사후 보정 중 오류 발생: {}", err))
                .await;
        }
        state_clone.stop_flag.store(false, Ordering::SeqCst);
    });

    *state.job_handle.lock().await = Some(handle);

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "started": true,
            "mode": req.mode,
            "album_name": album_name,
            "dry_run": dry_run,
            "limit": limit
        })),
    )
        .into_response()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::db::{CheckpointDB, FileUpsert};
    use axum::body::to_bytes;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_status_includes_repair_info_and_repair_endpoint_validation() {
        let db = CheckpointDB::connect(":memory:").await.unwrap();
        db.upsert_files(&[FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "res_lp_1".to_string(),
            name: "livephoto.,zip".to_string(),
            parent_path: "/2023".to_string(),
            size: 100,
            created_time: "2023-05-01T00:00:00Z".to_string(),
            modified_time: "2023-05-01T00:00:00Z".to_string(),
            target_type: "nextcloud".to_string(),
            sha1_hash: None,
        }])
        .await
        .unwrap();
        db.mark_file_uploaded(
            "mybox",
            "res_lp_1",
            "sha1",
            "/Cloud-Backup/MyBox/2023/livephoto.,zip",
            "UPLOADED",
        )
        .await
        .unwrap();

        let tmp = tempfile::TempDir::new().unwrap();
        let cfg = AppConfig {
            db_path: tmp.path().join("test.db").to_str().unwrap().to_string(),
            enable_nextcloud: false,
            ..Default::default()
        };
        let state = AppState::new(db, cfg);
        let app = build_router(state);

        // 1. Check GET /api/status includes repair_info
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(val["repair_info"]["nextcloud_anomaly_count"], 1);
        assert_eq!(val["repair_info"]["default_album_name"], "to-nextcloud");

        // 2. Check POST /api/jobs/repair rejects album_to_nextcloud when Nextcloud is disabled
        let resp2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/jobs/repair")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "mode": "album_to_nextcloud",
                            "album_name": "to-nextcloud"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::BAD_REQUEST);

        // 3. Check POST /api/jobs/repair rejects immich_duplicates when Immich is unconfigured
        let resp3 = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/jobs/repair")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "mode": "immich_duplicates",
                            "dry_run": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp3.status(), StatusCode::BAD_REQUEST);
    }
}
