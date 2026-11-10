use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mybox_immich_sync::config::AppConfig;
use mybox_immich_sync::db::CheckpointDB;
use mybox_immich_sync::state::{AppState, JobState};
use mybox_immich_sync::web::build_router;
use serde_json::{json, Value};
use std::io::{Cursor, Write};
use tempfile::TempDir;
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use zip::write::SimpleFileOptions;
use zip::ZipWriter;

fn build_sample_takeout_zip_bytes() -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut zip = ZipWriter::new(&mut cursor);
        let opts = SimpleFileOptions::default();
        zip.start_file("Takeout/Google Photos/2026/beach.jpg", opts)
            .unwrap();
        zip.write_all(b"fake_jpeg_bytes_12345").unwrap();
        zip.finish().unwrap();
    }
    cursor.into_inner()
}

#[tokio::test]
async fn test_web_dashboard_connection_and_job_lifecycle() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("web_test.db");

    // Mock MYBOX storage endpoint for connection test (official Open API)
    Mock::given(method("GET"))
        .and(path("/v1/drive/storage"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "usedBytes": 10737418240_i64,
            "quotaBytes": 32212254720_i64,
            "maxFileBytes": 10737418240_i64
        })))
        .mount(&mock_server)
        .await;

    // Mock Immich ping endpoint for connection test
    Mock::given(method("GET"))
        .and(path("/api/server/ping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "res": "pong" })))
        .mount(&mock_server)
        .await;

    // Mock MYBOX list_page for scan job (returns 2 items to verify sample limit=1)
    Mock::given(method("GET"))
        .and(path("/v1/drive/resources"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "fileCount": 2,
            "subFolderCount": 0,
            "resources": [
                {
                    "resourceId": "web_photo_1",
                    "name": "sunset.jpg",
                    "type": "file",
                    "size": 12345,
                    "createdAt": "2026-10-01T00:00:00Z",
                    "modifiedAt": "2026-10-01T00:00:00Z"
                },
                {
                    "resourceId": "web_photo_2",
                    "name": "ocean.jpg",
                    "type": "file",
                    "size": 23456,
                    "createdAt": "2026-10-02T00:00:00Z",
                    "modifiedAt": "2026-10-02T00:00:00Z"
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

    // 1. GET / serves embedded 3-tab Korean Web UI without emojis or gradients
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
    assert!(html.contains("id=\"navTabSync\""));
    assert!(html.contains("id=\"navTabCleanup\""));
    assert!(html.contains("id=\"navTabSettings\""));
    assert!(html.contains("id=\"syncView\""));
    assert!(html.contains("id=\"cleanupView\""));
    assert!(html.contains("id=\"settingsView\""));
    assert!(html.contains("id=\"zipDropzone\""));
    assert!(html.contains("id=\"cfgTakeoutDir\""));
    assert!(html.contains("Google 포토 · 드라이브 (Google Takeout)"));
    assert!(!html.contains("id=\"cfgGdriveToken\""));
    assert!(html.contains("id=\"sampleLimitSelect\""));
    assert!(html.contains("toggleTheme"));
    assert!(html.contains("escapeHtml"));
    assert!(!html.contains("linear-gradient("));
    assert!(!html.contains("📦"));
    assert!(!html.contains("🚀"));

    // 2. Before connection verification, Sync and Cleanup tabs are locked (disabled)
    let init_status_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(init_status_resp.status(), StatusCode::OK);
    let init_status: Value = serde_json::from_slice(
        &to_bytes(init_status_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(init_status["readiness"]["targets_ready"], false);
    assert_eq!(init_status["readiness"]["sync_tab_enabled"], false);
    assert_eq!(init_status["readiness"]["delete_tab_enabled"], false);

    // 3. Scoped verification: verify ONLY Immich first -> targets_ready=true, tabs still locked without verified source
    let immich_conn_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/connections/test?service=immich")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(immich_conn_resp.status(), StatusCode::OK);
    let immich_conn: Value = serde_json::from_slice(
        &to_bytes(immich_conn_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(immich_conn["results"]["immich"]["ok"], true);
    assert!(immich_conn["results"]["mybox"].is_null());
    assert_eq!(immich_conn["readiness"]["immich_verified"], true);
    assert_eq!(immich_conn["readiness"]["targets_ready"], true);
    assert_eq!(immich_conn["readiness"]["sync_tab_enabled"], false);

    // 4. Scoped verification: verify MYBOX -> unlocks both Sync and Cleanup tabs
    let mybox_conn_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/connections/test?service=mybox")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(mybox_conn_resp.status(), StatusCode::OK);
    let mybox_conn: Value = serde_json::from_slice(
        &to_bytes(mybox_conn_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(mybox_conn["results"]["mybox"]["ok"], true);
    assert_eq!(mybox_conn["readiness"]["sync_tab_enabled"], true);
    assert_eq!(mybox_conn["readiness"]["delete_tab_enabled"], true);
    assert_eq!(
        mybox_conn["readiness"]["verified_sources"],
        json!(["mybox"])
    );
    assert_eq!(
        mybox_conn["readiness"]["verified_cloud_sources"],
        json!(["mybox"])
    );

    // 5. Set Takeout directory (initially empty -> not verified) then upload a valid .zip -> auto-verified!
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
    let path_json: Value =
        serde_json::from_slice(&to_bytes(path_resp.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(path_json["verification"]["ok"], false);

    let zip_bytes = build_sample_takeout_zip_bytes();

    // 5a-0. Direct browser ZIP upload via POST /api/takeout/upload writes atomically (.part -> .zip)
    let upload_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/takeout/upload?filename=takeout-2026-001.zip")
                .body(Body::from(zip_bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload_resp.status(), StatusCode::OK);
    assert!(custom_takeout_dir.join("takeout-2026-001.zip").exists());
    assert!(!custom_takeout_dir
        .join("takeout-2026-001.zip.part")
        .exists());

    // 5a. Reject HTML login page URLs (when user pastes webpage link instead of direct stream URL)
    Mock::given(method("GET"))
        .and(path("/takeout/login-page"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string("<!DOCTYPE html><html>Sign in</html>"),
        )
        .mount(&mock_server)
        .await;

    let bad_url_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/takeout/fetch-url")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "url": format!("{}/takeout/login-page", mock_server.uri()) })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad_url_resp.status(), StatusCode::BAD_REQUEST);

    // 5b. Download Takeout .zip directly into NAS directory via signed stream URL
    Mock::given(method("GET"))
        .and(path("/download/takeout-2026-001.zip"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(zip_bytes))
        .mount(&mock_server)
        .await;

    let fetch_url_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/takeout/fetch-url")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "url": format!("{}/download/takeout-2026-001.zip?qk=signed_token", mock_server.uri()),
                        "takeout_dir": custom_takeout_dir.to_str().unwrap()
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fetch_url_resp.status(), StatusCode::ACCEPTED);

    for _ in 0..50 {
        if *state.job_state.read().await == JobState::Idle {
            let r = state.readiness_summary().await;
            if r.verified_sources.contains(&"takeout".to_string()) {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let r_after_url = state.readiness_summary().await;
    assert_eq!(r_after_url.verified_sources, vec!["mybox", "takeout"]);
    // Local Takeout archives must NEVER appear in remote cloud cleanup targets
    assert_eq!(r_after_url.verified_cloud_sources, vec!["mybox"]);

    // 6. Conflict check: if job_state is not Idle, POST /api/jobs/start returns 409 Conflict
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

    // 7. Sequential multi-source batch job with limit=1 across ["mybox", "takeout"]
    let start_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/jobs/start")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "sources": ["mybox", "takeout"],
                        "mode": "scan",
                        "dry_run": false,
                        "limit": 1
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(start_resp.status(), StatusCode::ACCEPTED);

    // Wait briefly for sequential scan across both sources to complete
    for _ in 0..50 {
        if *state.job_state.read().await == JobState::Idle {
            let mybox_st = state.db.get_stats("mybox").await.unwrap();
            let takeout_st = state.db.get_stats("takeout").await.unwrap();
            if mybox_st.pending_files == 1 && takeout_st.pending_files == 1 {
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
    assert_eq!(status_json["all_stats"]["mybox"]["pending_files"], 1);
    assert_eq!(status_json["all_stats"]["takeout"]["pending_files"], 1);
    assert_eq!(status_json["config_summary"]["mybox_configured"], true);
    assert_eq!(status_json["config_summary"]["immich_configured"], true);
    assert_eq!(status_json["config_summary"]["concurrency"], 4);

    // 8. Save runtime settings via POST /api/settings and verify persistence + selective cache preservation
    let settings_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/settings")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "gdrive_access_token": "new_gdrive_oauth_token",
                        "concurrency": 6,
                        "rate_limit_rps": 12.5,
                        "sync_interval_minutes": 30
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(settings_resp.status(), StatusCode::OK);
    let settings_json: Value = serde_json::from_slice(
        &to_bytes(settings_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(settings_json["saved"], true);
    assert_eq!(settings_json["config_summary"]["gdrive_configured"], true);
    assert_eq!(settings_json["config_summary"]["mybox_configured"], true); // Blank secret preserves existing MYBOX token
    assert_eq!(settings_json["config_summary"]["concurrency"], 6);
    assert_eq!(settings_json["config_summary"]["sync_interval_minutes"], 30);
    // Unmodified MYBOX and Takeout verifications remain intact
    assert_eq!(
        settings_json["readiness"]["verified_sources"],
        json!(["mybox", "takeout"])
    );
    assert!(temp_dir.path().join("settings.json").exists());
}
