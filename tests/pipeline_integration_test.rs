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

    // 1. Mock MYBOX list_page (root has 1 subfolder, 1 photo, and 1 PDF; subfolder is empty)
    Mock::given(method("GET"))
        .and(path("/v1/drive/resources"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "fileCount": 2,
            "subFolderCount": 1,
            "resources": [
                {
                    "resourceId": "folder_sub_1",
                    "name": "Album2026",
                    "type": "folder",
                    "size": 0,
                    "createdAt": "2026-05-01T10:00:00Z",
                    "modifiedAt": "2026-05-01T10:00:00Z"
                },
                {
                    "resourceId": "photo_1",
                    "name": "vacation.jpg",
                    "type": "file",
                    "size": 14,
                    "createdAt": "2026-05-01T12:00:00Z",
                    "modifiedAt": "2026-05-01T12:00:00Z"
                },
                {
                    "resourceId": "doc_1",
                    "name": "manual.pdf",
                    "type": "file",
                    "size": 11,
                    "createdAt": "2026-05-02T12:00:00Z",
                    "modifiedAt": "2026-05-02T12:00:00Z"
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/drive/folders/folder_sub_1/resources"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "fileCount": 0,
            "subFolderCount": 0,
            "resources": []
        })))
        .mount(&mock_server)
        .await;

    // 2. Mock MYBOX 2-step file downloads (GET /v1/drive/files/{fileId}/download -> downloadUrl -> stream)
    Mock::given(method("GET"))
        .and(path("/v1/drive/files/photo_1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "downloadUrl": format!("{}/v1/storage/download/photo_1", mock_server.uri()),
            "expiresIn": 600
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/storage/download/photo_1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"fake_jpg_bytes".as_slice()))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/drive/files/doc_1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "downloadUrl": format!("{}/v1/storage/download/doc_1", mock_server.uri()),
            "expiresIn": 600
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/storage/download/doc_1"))
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

    // 5. Mock Nextcloud WebDAV MKCOL, PUT, and HEAD verification (with dynamic /MyBox subfolder)
    Mock::given(method("MKCOL"))
        .and(path("/remote.php/dav/files/ncuser/Cloud-Backup"))
        .respond_with(ResponseTemplate::new(201))
        .mount(&mock_server)
        .await;

    Mock::given(method("MKCOL"))
        .and(path("/remote.php/dav/files/ncuser/Cloud-Backup/MyBox"))
        .respond_with(ResponseTemplate::new(201))
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path(
            "/remote.php/dav/files/ncuser/Cloud-Backup/MyBox/manual.pdf",
        ))
        .respond_with(ResponseTemplate::new(201))
        .mount(&mock_server)
        .await;

    Mock::given(method("HEAD"))
        .and(path(
            "/remote.php/dav/files/ncuser/Cloud-Backup/MyBox/manual.pdf",
        ))
        .respond_with(ResponseTemplate::new(200).insert_header("Content-Length", "11"))
        .mount(&mock_server)
        .await;

    // 6. Mock MYBOX remote trash endpoints (DELETE /v1/drive/resources/{resourceId})
    Mock::given(method("DELETE"))
        .and(path("/v1/drive/resources/photo_1"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/v1/drive/resources/doc_1"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
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

    // Step A: Scan (root + folder_sub_1 = 2 scanned folders)
    let stats_after_scan = pipeline.run_scan().await.unwrap();
    assert_eq!(stats_after_scan.pending_files, 2);
    assert_eq!(stats_after_scan.scanned_folders, 2);

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
        .and(path("/v1/drive/resources"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "fileCount": 2,
            "subFolderCount": 0,
            "resources": [
                {
                    "resourceId": "photo_1",
                    "name": "vacation.jpg",
                    "type": "file",
                    "size": 14,
                    "createdAt": "2026-05-01T12:00:00Z",
                    "modifiedAt": "2026-05-01T12:00:00Z"
                },
                {
                    "resourceId": "doc_1",
                    "name": "manual.pdf",
                    "type": "file",
                    "size": 11,
                    "createdAt": "2026-05-02T12:00:00Z",
                    "modifiedAt": "2026-05-02T12:00:00Z"
                }
            ]
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Cycle 2: User deleted doc_1 directly on MYBOX, kept photo_1 unchanged, and added photo_2
    Mock::given(method("GET"))
        .and(path("/v1/drive/resources"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "fileCount": 2,
            "subFolderCount": 0,
            "resources": [
                {
                    "resourceId": "photo_1",
                    "name": "vacation.jpg",
                    "type": "file",
                    "size": 14,
                    "createdAt": "2026-05-01T12:00:00Z",
                    "modifiedAt": "2026-05-01T12:00:00Z"
                },
                {
                    "resourceId": "photo_2",
                    "name": "newly_added.jpg",
                    "type": "file",
                    "size": 14,
                    "createdAt": "2026-05-03T12:00:00Z",
                    "modifiedAt": "2026-05-03T12:00:00Z"
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    // Downloads: photo_1 must only be downloaded ONCE (in Cycle 1), NOT re-downloaded in Cycle 2!
    Mock::given(method("GET"))
        .and(path("/v1/drive/files/photo_1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "downloadUrl": format!("{}/v1/storage/download/photo_1", mock_server.uri()),
            "expiresIn": 600
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/storage/download/photo_1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"fake_jpg_bytes".as_slice()))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/drive/files/doc_1/download"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "downloadUrl": format!("{}/v1/storage/download/doc_1", mock_server.uri()),
            "expiresIn": 600
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/storage/download/doc_1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello_world".as_slice()))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/drive/files/photo_2/download"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "downloadUrl": format!("{}/v1/storage/download/photo_2", mock_server.uri()),
            "expiresIn": 600
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/storage/download/photo_2"))
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

    Mock::given(method("MKCOL"))
        .and(path("/remote.php/dav/files/ncuser/Cloud-Backup/MyBox"))
        .respond_with(ResponseTemplate::new(201))
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path(
            "/remote.php/dav/files/ncuser/Cloud-Backup/MyBox/manual.pdf",
        ))
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

#[tokio::test]
async fn test_takeout_sync_preserves_zip_on_sample_limit_and_auto_deletes_after_full_verified_sync()
{
    use std::fs::File;
    use std::io::Write;
    use zip::write::SimpleFileOptions;
    use zip::ZipWriter;

    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_takeout_cleanup.db");
    let tmp_download_dir = temp_dir.path().join("tmp_takeout_sync");
    let takeout_dir = temp_dir.path().join("takeout_staging");
    std::fs::create_dir_all(&takeout_dir).unwrap();

    let zip_path = takeout_dir.join("takeout-2026-001.zip");
    {
        let file = File::create(&zip_path).unwrap();
        let mut zip = ZipWriter::new(file);
        let opts = SimpleFileOptions::default();
        zip.start_file("Takeout/Photos/a.jpg", opts).unwrap();
        zip.write_all(b"jpeg_content_alpha").unwrap();
        zip.start_file("Takeout/Photos/b.jpg", opts).unwrap();
        zip.write_all(b"jpeg_content_bravo").unwrap();
        zip.finish().unwrap();
    }

    // First 2 bulk-upload-check calls happen during upload of a.jpg and b.jpg -> "accept"
    Mock::given(method("POST"))
        .and(path("/api/assets/bulk-upload-check"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "Takeout/Photos/a.jpg", "action": "accept" },
                { "id": "Takeout/Photos/b.jpg", "action": "accept" }
            ]
        })))
        .up_to_n_times(2)
        .mount(&mock_server)
        .await;

    // Subsequent bulk-upload-check calls happen during post-sync target verification -> "reject" (exists!)
    Mock::given(method("POST"))
        .and(path("/api/assets/bulk-upload-check"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "Takeout/Photos/a.jpg", "action": "reject", "assetId": "immich_a" },
                { "id": "Takeout/Photos/b.jpg", "action": "reject", "assetId": "immich_b" }
            ]
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/assets"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": "immich_takeout_asset",
            "status": "created"
        })))
        .expect(2)
        .mount(&mock_server)
        .await;

    let cfg = AppConfig {
        db_path: db_path.to_str().unwrap().to_string(),
        tmp_dir: tmp_download_dir.to_str().unwrap().to_string(),
        takeout_dir: takeout_dir.to_str().unwrap().to_string(),
        immich_url: mock_server.uri(),
        immich_api_key: "test_immich_key".to_string(),
        enable_nextcloud: false,
        rate_limit_rps: 100.0,
        concurrency: 1,
        ..AppConfig::default()
    };

    let db = CheckpointDB::connect(&cfg.db_path).await.unwrap();
    let state = AppState::new(db, cfg.clone());

    // 1. Sample-limited test run (limit = 1): syncs 1 photo and MUST preserve takeout-2026-001.zip
    let limited_pipeline = SyncPipeline::from_config(state.clone(), cfg.clone(), "takeout")
        .unwrap()
        .with_limit(Some(1));
    let stats_limited = limited_pipeline.run_auto().await.unwrap();
    assert_eq!(stats_limited.uploaded_immich_created, 1);
    assert!(
        zip_path.exists(),
        "Sample-limited test run must NOT delete local Takeout ZIP"
    );

    // 2. Running sync-only while scan was incomplete (pending_folders > 0) MUST also preserve ZIP
    let full_pipeline = SyncPipeline::from_config(state.clone(), cfg, "takeout").unwrap();
    let _ = full_pipeline.run_sync().await.unwrap();
    assert!(
        zip_path.exists(),
        "Sync before full archive scan completes must NOT delete local Takeout ZIP"
    );

    // 3. Full auto run (limit = None): scans remaining b.jpg, uploads it, verifies BOTH on Immich, and deletes ZIP
    let stats_full = full_pipeline.run_auto().await.unwrap();
    assert_eq!(stats_full.uploaded_immich_created, 2);
    assert_eq!(stats_full.pending_files, 0);
    assert_eq!(stats_full.failed_files, 0);
    assert_eq!(stats_full.deleted_files, 2);
    assert!(
        !zip_path.exists(),
        "Local Takeout ZIP must be automatically deleted after full sync and Immich verification"
    );
    assert!(!state.readiness_summary().await.sync_tab_enabled);
}

#[tokio::test]
async fn test_gdrive_owned_quota_filtering_workspace_export_and_concurrent_nextcloud_sync() {
    use wiremock::matchers::body_string_contains;

    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_gdrive_pipeline.db");
    let tmp_download_dir = temp_dir.path().join("tmp_gdrive_sync");

    let expected_root_q = "'root' in parents and 'me' in owners and trashed = false and mimeType != 'application/vnd.google-apps.shortcut'";
    let expected_sub_q = "'folder_sub_1' in parents and 'me' in owners and trashed = false and mimeType != 'application/vnd.google-apps.shortcut'";

    // 1. Root folder list_page: mixes owned photo, shared file, zero-quota file, Google Form,
    //    two identically-named Google Docs, and a subfolder containing `/` in its Drive title.
    Mock::given(method("GET"))
        .and(path("/files"))
        .and(query_param("q", expected_root_q))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "files": [
                {
                    "id": "photo_owned_1",
                    "name": "family_trip.jpg",
                    "mimeType": "image/jpeg",
                    "size": "18",
                    "quotaBytesUsed": "18",
                    "ownedByMe": true,
                    "createdTime": "2025-06-15T09:30:00Z",
                    "modifiedTime": "2025-06-15T09:30:00Z"
                },
                {
                    "id": "shared_pdf_ignore",
                    "name": "shared_lecture.pdf",
                    "mimeType": "application/pdf",
                    "size": "99999",
                    "quotaBytesUsed": "0",
                    "ownedByMe": false,
                    "createdTime": "2025-06-10T00:00:00Z",
                    "modifiedTime": "2025-06-10T00:00:00Z"
                },
                {
                    "id": "zero_quota_bin_ignore",
                    "name": "free_legacy.jpg",
                    "mimeType": "image/jpeg",
                    "size": "5000",
                    "quotaBytesUsed": "0",
                    "ownedByMe": true,
                    "createdTime": "2019-01-01T00:00:00Z",
                    "modifiedTime": "2019-01-01T00:00:00Z"
                },
                {
                    "id": "gform_ignore",
                    "name": "설문조사",
                    "mimeType": "application/vnd.google-apps.form",
                    "size": "0",
                    "quotaBytesUsed": "0",
                    "ownedByMe": true,
                    "createdTime": "2025-07-01T00:00:00Z",
                    "modifiedTime": "2025-07-01T00:00:00Z"
                },
                {
                    "id": "gdoc_1",
                    "name": "제목 없는 문서",
                    "mimeType": "application/vnd.google-apps.document",
                    "size": "0",
                    "quotaBytesUsed": "0",
                    "ownedByMe": true,
                    "createdTime": "2025-08-01T12:00:00Z",
                    "modifiedTime": "2025-08-01T12:00:00Z"
                },
                {
                    "id": "gdoc_2",
                    "name": "제목 없는 문서",
                    "mimeType": "application/vnd.google-apps.document",
                    "size": "0",
                    "quotaBytesUsed": "0",
                    "ownedByMe": true,
                    "createdTime": "2025-08-02T12:00:00Z",
                    "modifiedTime": "2025-08-02T12:00:00Z"
                },
                {
                    "id": "folder_sub_1",
                    "name": "2025/09 정산",
                    "mimeType": "application/vnd.google-apps.folder",
                    "size": "0",
                    "quotaBytesUsed": "0",
                    "ownedByMe": true
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    // 2. Subfolder list_page: contains 1 Google Sheet with `/` in filename
    Mock::given(method("GET"))
        .and(path("/files"))
        .and(query_param("q", expected_sub_q))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "files": [
                {
                    "id": "gsheet_1",
                    "name": "Q3/Q4 가계부",
                    "mimeType": "application/vnd.google-apps.spreadsheet",
                    "size": "0",
                    "quotaBytesUsed": "0",
                    "ownedByMe": true,
                    "createdTime": "2025-09-10T08:00:00Z",
                    "modifiedTime": "2025-09-10T08:00:00Z"
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    // 3. Binary photo download (?alt=media)
    Mock::given(method("GET"))
        .and(path("/files/photo_owned_1"))
        .and(query_param("alt", "media"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"owned_jpeg_payload".as_slice()))
        .expect(1)
        .mount(&mock_server)
        .await;

    // 4. Google Workspace exports (/files/{id}/export?mimeType=...)
    Mock::given(method("GET"))
        .and(path("/files/gdoc_1/export"))
        .and(query_param(
            "mimeType",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"docx_bytes_alpha_19".as_slice()))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/files/gdoc_2/export"))
        .and(query_param(
            "mimeType",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"docx_bytes_bravo_19".as_slice()))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/files/gsheet_1/export"))
        .and(query_param(
            "mimeType",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"xlsx_bytes_13".as_slice()))
        .expect(1)
        .mount(&mock_server)
        .await;

    // 5. Immich check & upload: MUST receive real createdTime ("2025-06-15T09:30:00Z"), NOT 1970!
    Mock::given(method("POST"))
        .and(path("/api/assets/bulk-upload-check"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "photo_owned_1", "action": "accept" }
            ]
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/assets/bulk-upload-check"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "photo_owned_1", "action": "reject", "assetId": "immich_gdrive_1" }
            ]
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/assets"))
        .and(body_string_contains("2025-06-15T09:30:00Z"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": "immich_gdrive_1",
            "status": "created"
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    // 6. Nextcloud MKCOL: even with concurrency = 4, /GoogleDrive MUST only be MKCOL'd ONCE!
    let enc_sub_dir =
        percent_encoding::utf8_percent_encode("2025_09 정산", percent_encoding::NON_ALPHANUMERIC);
    // Match exact WebDAV percent-encoding used on HTTP wire (spaces & UTF-8 bytes encoded)
    let dav_encode_seg = |s: &str| -> String {
        use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
        const SET: &AsciiSet = &CONTROLS
            .add(b' ')
            .add(b'"')
            .add(b'#')
            .add(b'%')
            .add(b'?')
            .add(b'<')
            .add(b'>')
            .add(b'`')
            .add(b'{')
            .add(b'}');
        utf8_percent_encode(s, SET).to_string()
    };
    let _ = enc_sub_dir;

    let sub_folder_wire = format!(
        "/remote.php/dav/files/ncuser/GoogleDrive/{}",
        dav_encode_seg("2025_09 정산")
    );
    let doc1_wire = format!(
        "/remote.php/dav/files/ncuser/GoogleDrive/{}",
        dav_encode_seg("제목 없는 문서.docx")
    );
    let doc2_wire = format!(
        "/remote.php/dav/files/ncuser/GoogleDrive/{}",
        dav_encode_seg("제목 없는 문서 (gdoc_2).docx")
    );
    let sheet_wire = format!(
        "{}/{}",
        sub_folder_wire,
        dav_encode_seg("Q3_Q4 가계부.xlsx")
    );

    Mock::given(method("MKCOL"))
        .and(path("/remote.php/dav/files/ncuser/GoogleDrive"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("MKCOL"))
        .and(path(sub_folder_wire.as_str()))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Nextcloud PUTs: two identically-named docs must NOT collide on the same path!
    Mock::given(method("PUT"))
        .and(path(doc1_wire.as_str()))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path(doc2_wire.as_str()))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path(sheet_wire.as_str()))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Nextcloud HEAD verification with actual exported sizes (19 bytes for docx, 13 bytes for xlsx)
    Mock::given(method("HEAD"))
        .and(path(doc1_wire.as_str()))
        .respond_with(ResponseTemplate::new(200).insert_header("Content-Length", "19"))
        .mount(&mock_server)
        .await;

    Mock::given(method("HEAD"))
        .and(path(doc2_wire.as_str()))
        .respond_with(ResponseTemplate::new(200).insert_header("Content-Length", "19"))
        .mount(&mock_server)
        .await;

    Mock::given(method("HEAD"))
        .and(path(sheet_wire.as_str()))
        .respond_with(ResponseTemplate::new(200).insert_header("Content-Length", "13"))
        .mount(&mock_server)
        .await;

    // 7. Google Drive Trash PATCH for the 4 synced owned items
    for id in ["photo_owned_1", "gdoc_1", "gdoc_2", "gsheet_1"] {
        Mock::given(method("PATCH"))
            .and(path(format!("/files/{}", id)))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"id": id, "trashed": true})),
            )
            .expect(1)
            .mount(&mock_server)
            .await;
    }

    let cfg = AppConfig {
        db_path: db_path.to_str().unwrap().to_string(),
        tmp_dir: tmp_download_dir.to_str().unwrap().to_string(),
        gdrive_access_token: "test_gdrive_oauth_token".to_string(),
        gdrive_api_base: mock_server.uri(),
        immich_url: mock_server.uri(),
        immich_api_key: "test_immich_key".to_string(),
        enable_nextcloud: true,
        nextcloud_url: mock_server.uri(),
        nextcloud_user: "ncuser".to_string(),
        nextcloud_app_password: "ncpassword".to_string(),
        nextcloud_base_dir: "/".to_string(),
        rate_limit_rps: 200.0,
        concurrency: 4,
        ..AppConfig::default()
    };

    let db = CheckpointDB::connect(&cfg.db_path).await.unwrap();
    let state = AppState::new(db, cfg.clone());
    let pipeline = SyncPipeline::from_config(state, cfg, "gdrive").unwrap();

    // Scan + Sync
    let sync_stats = pipeline.run_auto().await.unwrap();
    assert_eq!(sync_stats.scanned_folders, 2); // root + folder_sub_1
    assert_eq!(sync_stats.total_files, 4); // photo_owned_1 + gdoc_1 + gdoc_2 + gsheet_1 (shared, 0-quota, form excluded!)
    assert_eq!(sync_stats.uploaded_immich_created, 1);
    assert_eq!(sync_stats.uploaded_nextcloud, 3);
    assert_eq!(sync_stats.failed_files, 0);

    // Verified remote delete (confirms exported sizes match Nextcloud HEAD Content-Length)
    let del_stats = pipeline.run_delete(false).await.unwrap();
    assert_eq!(del_stats.deleted_files, 4);
}

#[tokio::test]
async fn test_takeout_combined_photos_and_drive_routes_photos_to_immich_and_docs_to_nextcloud_google_drive(
) {
    use std::fs::File;
    use std::io::Write;
    use zip::write::SimpleFileOptions;
    use zip::ZipWriter;

    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_takeout_combined.db");
    let tmp_download_dir = temp_dir.path().join("tmp_takeout_combined");
    let takeout_dir = temp_dir.path().join("takeout_combined_staging");
    std::fs::create_dir_all(&takeout_dir).unwrap();

    let zip_path = takeout_dir.join("takeout-combined-001.zip");
    {
        let file = File::create(&zip_path).unwrap();
        let mut zip = ZipWriter::new(file);
        let dt = zip::DateTime::from_date_and_time(2025, 10, 20, 11, 0, 0).unwrap();
        let opts = SimpleFileOptions::default().last_modified_time(dt);

        // 0. Takeout index HTML -> must be skipped
        zip.start_file("Takeout/archive_browser.html", opts)
            .unwrap();
        zip.write_all(b"<html>Takeout</html>").unwrap();

        // 1. Google Photos image + JSON sidecar -> Immich
        zip.start_file("Takeout/Google 포토/Photos from 2025/family.jpg", opts)
            .unwrap();
        zip.write_all(b"family_photo_bytes").unwrap();
        zip.start_file("Takeout/Google 포토/Photos from 2025/family.jpg.json", opts)
            .unwrap();
        zip.write_all(br#"{"photoTakenTime": {"timestamp": "1750000000"}}"#)
            .unwrap();

        // 2. Google Drive image inside folder -> MUST ALSO go to Immich
        zip.start_file("Takeout/드라이브/여행사진/jeju_trip.png", opts)
            .unwrap();
        zip.write_all(b"jeju_png_bytes").unwrap();

        // 3. Google Drive root document -> Nextcloud /GoogleDrive/contract.pdf
        zip.start_file("Takeout/드라이브/contract.pdf", opts)
            .unwrap();
        zip.write_all(b"pdf_bytes_12").unwrap();

        // 4. Google Drive nested folder document -> Nextcloud /GoogleDrive/Work/plan.docx
        zip.start_file("Takeout/드라이브/Work/plan.docx", opts)
            .unwrap();
        zip.write_all(b"docx_bytes_13").unwrap();

        zip.finish().unwrap();
    }

    // Immich bulk-upload-check: first 2 calls during sync -> accept, next 2 calls during post-sync verify -> reject
    Mock::given(method("POST"))
        .and(path("/api/assets/bulk-upload-check"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "Takeout/Google 포토/Photos from 2025/family.jpg", "action": "accept" },
                { "id": "Takeout/드라이브/여행사진/jeju_trip.png", "action": "accept" }
            ]
        })))
        .up_to_n_times(2)
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/assets/bulk-upload-check"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "Takeout/Google 포토/Photos from 2025/family.jpg", "action": "reject", "assetId": "imm_1" },
                { "id": "Takeout/드라이브/여행사진/jeju_trip.png", "action": "reject", "assetId": "imm_2" }
            ]
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/assets"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": "immich_combined_asset",
            "status": "created"
        })))
        .expect(2)
        .mount(&mock_server)
        .await;

    // Nextcloud WebDAV MKCOL, PUT, and HEAD under /GoogleDrive (with Takeout/드라이브 stripped!)
    Mock::given(method("MKCOL"))
        .and(path("/remote.php/dav/files/ncuser/GoogleDrive"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("MKCOL"))
        .and(path("/remote.php/dav/files/ncuser/GoogleDrive/Work"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path(
            "/remote.php/dav/files/ncuser/GoogleDrive/contract.pdf",
        ))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path(
            "/remote.php/dav/files/ncuser/GoogleDrive/Work/plan.docx",
        ))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("HEAD"))
        .and(path(
            "/remote.php/dav/files/ncuser/GoogleDrive/contract.pdf",
        ))
        .respond_with(ResponseTemplate::new(200).insert_header("Content-Length", "12"))
        .mount(&mock_server)
        .await;

    Mock::given(method("HEAD"))
        .and(path(
            "/remote.php/dav/files/ncuser/GoogleDrive/Work/plan.docx",
        ))
        .respond_with(ResponseTemplate::new(200).insert_header("Content-Length", "13"))
        .mount(&mock_server)
        .await;

    let cfg = AppConfig {
        db_path: db_path.to_str().unwrap().to_string(),
        tmp_dir: tmp_download_dir.to_str().unwrap().to_string(),
        takeout_dir: takeout_dir.to_str().unwrap().to_string(),
        immich_url: mock_server.uri(),
        immich_api_key: "test_immich_key".to_string(),
        enable_nextcloud: true,
        nextcloud_url: mock_server.uri(),
        nextcloud_user: "ncuser".to_string(),
        nextcloud_app_password: "ncpassword".to_string(),
        nextcloud_base_dir: "/".to_string(),
        rate_limit_rps: 200.0,
        concurrency: 2,
        ..AppConfig::default()
    };

    let db = CheckpointDB::connect(&cfg.db_path).await.unwrap();
    let state = AppState::new(db, cfg.clone());
    let pipeline = SyncPipeline::from_config(state, cfg, "takeout").unwrap();

    let stats = pipeline.run_auto().await.unwrap();
    assert_eq!(stats.total_files, 4); // 2 photos + 2 docs (archive_browser.html & .json skipped)
    assert_eq!(stats.uploaded_immich_created, 2); // Both Photos photo AND Drive photo went to Immich
    assert_eq!(stats.uploaded_nextcloud, 2); // Both Drive docs went to Nextcloud /GoogleDrive/...
    assert_eq!(stats.failed_files, 0);
    assert_eq!(stats.deleted_files, 4);
    assert!(
        !zip_path.exists(),
        "Combined Takeout ZIP must be deleted after both Immich and Nextcloud verify all 4 files"
    );
}
