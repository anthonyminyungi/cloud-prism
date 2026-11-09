use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mybox_immich_sync::config::AppConfig;
use mybox_immich_sync::db::CheckpointDB;
use mybox_immich_sync::state::{AppState, JobState};
use mybox_immich_sync::web::build_router;
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn test_web_dashboard_connection_and_job_lifecycle() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("web_test.db");

    // Mock MYBOX quota endpoint for connection test
    Mock::given(method("GET"))
        .and(path("/v1/quota"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "used": 10737418240_i64,
            "quota": 32212254720_i64
        })))
        .mount(&mock_server)
        .await;

    // Mock Immich ping endpoint for connection test
    Mock::given(method("GET"))
        .and(path("/api/server/ping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "res": "pong" })))
        .mount(&mock_server)
        .await;

    // Mock MYBOX list_page for scan job
    Mock::given(method("GET"))
        .and(path("/v1/files"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {
                    "resource_id": "web_photo_1",
                    "name": "sunset.jpg",
                    "is_folder": false,
                    "size": 12345,
                    "created_time": "2026-10-01T00:00:00Z",
                    "modified_time": "2026-10-01T00:00:00Z"
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    let cfg = AppConfig {
        db_path: db_path.to_str().unwrap().to_string(),
        tmp_dir: temp_dir.path().join("tmp").to_str().unwrap().to_string(),
        mybox_pat: "my_secret_pat_token".to_string(),
        mybox_api_base: mock_server.uri(),
        immich_url: mock_server.uri(),
        immich_api_key: "immich_secret_key".to_string(),
        enable_nextcloud: false,
        concurrency: 4,
        rate_limit_rps: 100.0,
        ..AppConfig::default()
    };

    let db = CheckpointDB::connect(&cfg.db_path).await.unwrap();
    let state = AppState::new(db, cfg);
    let app = build_router(state.clone());

    // 1. GET / serves embedded Korean Web UI
    let resp = app
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let html_bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let html = String::from_utf8_lossy(&html_bytes);
    assert!(html.contains("네이버 MYBOX"));
    assert!(html.contains("Immich"));
    assert!(html.contains("id=\"workspaceView\""));
    assert!(html.contains("id=\"takeoutArchivePanel\""));
    assert!(html.contains("id=\"settingsView\""));
    assert!(html.contains("id=\"metricStrip\""));
    assert!(html.contains("toggleTheme"));
    assert!(!html.contains("id=\"envDrawer\""));
    assert!(!html.contains("id=\"actionDropdown\""));
    assert!(!html.contains("📦"));
    assert!(!html.contains("🚀"));

    // 2. POST /api/connections/test verifies MYBOX and Immich connectivity from environment config
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/connections/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let conn_json: Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(conn_json["results"]["mybox"]["ok"], true);
    assert_eq!(conn_json["results"]["immich"]["ok"], true);

    // 3. Conflict check: if job_state is not Idle, POST /api/jobs/start returns 409 Conflict
    *state.job_state.write().await = JobState::Scanning;
    let conflict_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/jobs/start")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"source": "mybox", "mode": "scan", "dry_run": false}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(conflict_resp.status(), StatusCode::CONFLICT);
    *state.job_state.write().await = JobState::Idle;

    // 4. Start scan job via POST /api/jobs/start and verify status + config_summary via GET /api/status
    let start_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/jobs/start")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"source": "mybox", "mode": "scan", "dry_run": false}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(start_resp.status().is_success());

    // Wait briefly for the spawned scan task to complete
    for _ in 0..30 {
        if *state.job_state.read().await == JobState::Idle {
            let st = state.db.get_stats("mybox").await.unwrap();
            if st.pending_files == 1 {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let status_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/status?source=mybox")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status_resp.status(), StatusCode::OK);
    let status_json: Value =
        serde_json::from_slice(&to_bytes(status_resp.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(status_json["stats"]["pending_files"], 1);
    assert_eq!(status_json["config_summary"]["mybox_configured"], true);
    assert_eq!(status_json["config_summary"]["immich_configured"], true);
    assert_eq!(status_json["config_summary"]["concurrency"], 4);

    // 5. Set Takeout directory path and upload a .zip archive via Web API
    let custom_takeout_dir = temp_dir.path().join("custom_takeout");
    let custom_takeout_str = custom_takeout_dir.to_str().unwrap().to_string();

    let path_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/takeout/path")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "takeout_dir": custom_takeout_str }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(path_resp.status(), StatusCode::OK);

    let upload_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/takeout/upload?filename=takeout-2026-001.zip")
                .body(Body::from("PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload_resp.status(), StatusCode::OK);
    let upload_json: Value =
        serde_json::from_slice(&to_bytes(upload_resp.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(upload_json["uploaded"], "takeout-2026-001.zip");
    assert_eq!(upload_json["archives"].as_array().unwrap().len(), 1);
}
