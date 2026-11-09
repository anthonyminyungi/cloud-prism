use mybox_immich_sync::config::AppConfig;
use mybox_immich_sync::db::CheckpointDB;
use mybox_immich_sync::pipeline::SyncPipeline;
use mybox_immich_sync::state::{AppState, JobState};
use serde_json::json;
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn test_full_pipeline_scan_sync_and_verified_delete() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_pipeline.db");
    let tmp_download_dir = temp_dir.path().join("tmp_mybox_sync");

    // 1. Mock MYBOX list_page (root has 1 photo and 1 PDF)
    Mock::given(method("GET"))
        .and(path("/v1/files"))
        .and(query_param("folder_id", "root"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {
                    "resource_id": "photo_1",
                    "name": "vacation.jpg",
                    "is_folder": false,
                    "size": 14,
                    "created_time": "2026-05-01T12:00:00Z",
                    "modified_time": "2026-05-01T12:00:00Z"
                },
                {
                    "resource_id": "doc_1",
                    "name": "manual.pdf",
                    "is_folder": false,
                    "size": 11,
                    "created_time": "2026-05-02T12:00:00Z",
                    "modified_time": "2026-05-02T12:00:00Z"
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    // 2. Mock MYBOX file downloads
    Mock::given(method("GET"))
        .and(path("/v1/files/download"))
        .and(query_param("resource_id", "photo_1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"fake_jpg_bytes".as_slice()))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/files/download"))
        .and(query_param("resource_id", "doc_1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello_world".as_slice()))
        .mount(&mock_server)
        .await;

    // 3. Mock Immich bulk-upload-check:
    // First call (during sync) returns "accept"; second call (during delete verification) returns "reject" (exists!)
    Mock::given(method("POST"))
        .and(path("/api/assets/bulk-upload-check"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "photo_1", "action": "accept" }
            ]
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/assets/bulk-upload-check"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "photo_1", "action": "reject", "assetId": "immich_asset_100" }
            ]
        })))
        .mount(&mock_server)
        .await;

    // 4. Mock Immich multipart upload
    Mock::given(method("POST"))
        .and(path("/api/assets"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": "immich_asset_100",
            "status": "created"
        })))
        .mount(&mock_server)
        .await;

    // 5. Mock Nextcloud WebDAV MKCOL, PUT, and HEAD verification
    Mock::given(method("MKCOL"))
        .and(path("/remote.php/dav/files/ncuser/Cloud-Backup"))
        .respond_with(ResponseTemplate::new(201))
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/remote.php/dav/files/ncuser/Cloud-Backup/manual.pdf"))
        .respond_with(ResponseTemplate::new(201))
        .mount(&mock_server)
        .await;

    Mock::given(method("HEAD"))
        .and(path("/remote.php/dav/files/ncuser/Cloud-Backup/manual.pdf"))
        .respond_with(ResponseTemplate::new(200).insert_header("Content-Length", "11"))
        .mount(&mock_server)
        .await;

    // 6. Mock MYBOX remote trash endpoint
    Mock::given(method("POST"))
        .and(path("/v1/files/trash"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": "trashed"})))
        .expect(2)
        .mount(&mock_server)
        .await;

    let cfg = AppConfig {
        db_path: db_path.to_str().unwrap().to_string(),
        tmp_dir: tmp_download_dir.to_str().unwrap().to_string(),
        mybox_pat: "test_pat".to_string(),
        mybox_api_base: mock_server.uri(),
        immich_url: mock_server.uri(),
        immich_api_key: "test_immich_key".to_string(),
        enable_nextcloud: true,
        nextcloud_url: mock_server.uri(),
        nextcloud_user: "ncuser".to_string(),
        nextcloud_app_password: "ncpassword".to_string(),
        nextcloud_base_dir: "/Cloud-Backup".to_string(),
        rate_limit_rps: 100.0,
        concurrency: 2,
        ..AppConfig::default()
    };

    let db = CheckpointDB::connect(&cfg.db_path).await.unwrap();
    let state = AppState::new(db, cfg.clone());
    let mut rx = state.event_tx.subscribe();

    let pipeline = SyncPipeline::from_config(state.clone(), cfg, "mybox").unwrap();

    // Step A: Scan
    let stats_after_scan = pipeline.run_scan().await.unwrap();
    assert_eq!(stats_after_scan.pending_files, 2);
    assert_eq!(stats_after_scan.scanned_folders, 1);

    // Step B: Sync (must route photo_1 -> Immich and doc_1 -> Nextcloud, never deleting source)
    let stats_after_sync = pipeline.run_sync().await.unwrap();
    assert_eq!(stats_after_sync.uploaded_immich_created, 1);
    assert_eq!(stats_after_sync.uploaded_nextcloud, 1);
    assert_eq!(stats_after_sync.deleted_files, 0);

    // Verify TempFileGuard cleaned up all temporary downloaded files
    if tmp_download_dir.exists() {
        let leftover: Vec<_> = std::fs::read_dir(&tmp_download_dir).unwrap().collect();
        assert!(
            leftover.is_empty(),
            "Temp directory must be empty after sync"
        );
    }

    // Step C: Dry-run delete (verifies targets exist, but does NOT delete or mark DELETED)
    let stats_dry_del = pipeline.run_delete(true).await.unwrap();
    assert_eq!(stats_dry_del.deleted_files, 0);

    // Step D: Actual verified delete
    let stats_real_del = pipeline.run_delete(false).await.unwrap();
    assert_eq!(stats_real_del.deleted_files, 2);
    assert_eq!(*state.job_state.read().await, JobState::Idle);

    // Ensure progress events were emitted over the broadcast channel
    let first_event = rx
        .try_recv()
        .expect("should have emitted SSE progress events");
    assert_eq!(first_event.source, "mybox");
}

#[tokio::test]
async fn test_incremental_rescan_and_remote_direct_deletion_excludes_from_sync() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_incremental.db");
    let tmp_download_dir = temp_dir.path().join("tmp_inc_sync");

    // Cycle 1: MYBOX root has photo_1 (will be synced) and doc_1 (user will delete on MYBOX after sync)
    Mock::given(method("GET"))
        .and(path("/v1/files"))
        .and(query_param("folder_id", "root"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {
                    "resource_id": "photo_1",
                    "name": "vacation.jpg",
                    "is_folder": false,
                    "size": 14,
                    "created_time": "2026-05-01T12:00:00Z",
                    "modified_time": "2026-05-01T12:00:00Z"
                },
                {
                    "resource_id": "doc_1",
                    "name": "manual.pdf",
                    "is_folder": false,
                    "size": 11,
                    "created_time": "2026-05-02T12:00:00Z",
                    "modified_time": "2026-05-02T12:00:00Z"
                }
            ]
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Cycle 2: User deleted doc_1 directly on MYBOX, kept photo_1 unchanged, and added photo_2
    Mock::given(method("GET"))
        .and(path("/v1/files"))
        .and(query_param("folder_id", "root"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {
                    "resource_id": "photo_1",
                    "name": "vacation.jpg",
                    "is_folder": false,
                    "size": 14,
                    "created_time": "2026-05-01T12:00:00Z",
                    "modified_time": "2026-05-01T12:00:00Z"
                },
                {
                    "resource_id": "photo_2",
                    "name": "newly_added.jpg",
                    "is_folder": false,
                    "size": 14,
                    "created_time": "2026-05-03T12:00:00Z",
                    "modified_time": "2026-05-03T12:00:00Z"
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    // Downloads: photo_1 must only be downloaded ONCE (in Cycle 1), NOT re-downloaded in Cycle 2!
    Mock::given(method("GET"))
        .and(path("/v1/files/download"))
        .and(query_param("resource_id", "photo_1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"fake_jpg_bytes".as_slice()))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/files/download"))
        .and(query_param("resource_id", "doc_1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello_world".as_slice()))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/files/download"))
        .and(query_param("resource_id", "photo_2"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"new_jpg_bytes!".as_slice()))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/assets/bulk-upload-check"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "photo_1", "action": "accept" },
                { "id": "photo_2", "action": "accept" }
            ]
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/assets"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": "immich_asset_new",
            "status": "created"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("MKCOL"))
        .and(path("/remote.php/dav/files/ncuser/Cloud-Backup"))
        .respond_with(ResponseTemplate::new(201))
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/remote.php/dav/files/ncuser/Cloud-Backup/manual.pdf"))
        .respond_with(ResponseTemplate::new(201))
        .mount(&mock_server)
        .await;

    let cfg = AppConfig {
        db_path: db_path.to_str().unwrap().to_string(),
        tmp_dir: tmp_download_dir.to_str().unwrap().to_string(),
        mybox_pat: "test_pat".to_string(),
        mybox_api_base: mock_server.uri(),
        immich_url: mock_server.uri(),
        immich_api_key: "test_immich_key".to_string(),
        enable_nextcloud: true,
        nextcloud_url: mock_server.uri(),
        nextcloud_user: "ncuser".to_string(),
        nextcloud_app_password: "ncpassword".to_string(),
        nextcloud_base_dir: "/Cloud-Backup".to_string(),
        rate_limit_rps: 100.0,
        concurrency: 2,
        ..AppConfig::default()
    };

    let db = CheckpointDB::connect(&cfg.db_path).await.unwrap();
    let state = AppState::new(db, cfg.clone());
    let pipeline = SyncPipeline::from_config(state.clone(), cfg, "mybox").unwrap();

    // Cycle 1: Auto (Scan + Sync)
    let stats_cycle1 = pipeline.run_auto().await.unwrap();
    assert_eq!(stats_cycle1.uploaded_immich_created, 1);
    assert_eq!(stats_cycle1.uploaded_nextcloud, 1);
    assert_eq!(stats_cycle1.pending_files, 0);

    tokio::time::sleep(std::time::Duration::from_millis(15)).await;

    // Cycle 2: Incremental Scan (discovers photo_2, skips photo_1, excludes remotely-deleted doc_1)
    let stats_scan2 = pipeline.run_scan().await.unwrap();
    assert_eq!(stats_scan2.pending_files, 1); // Only photo_2!
    assert_eq!(stats_scan2.deleted_files, 1); // doc_1 marked DELETED (excluded from sync/delete queues)
    assert_eq!(stats_scan2.uploaded_nextcloud, 1); // Nextcloud backup stat preserved!

    let stats_sync2 = pipeline.run_sync().await.unwrap();
    assert_eq!(stats_sync2.uploaded_immich_created, 2); // photo_1 + photo_2
    assert_eq!(stats_sync2.uploaded_nextcloud, 1);
    assert_eq!(stats_sync2.pending_files, 0);
}
