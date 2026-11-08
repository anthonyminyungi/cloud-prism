use std::collections::HashMap;
use mybox_immich_sync::db::{CheckpointDB, FileUpsert};
use tempfile::TempDir;

#[tokio::test]
async fn test_sqlite_wal_settings_and_file_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("sync_state.db");
    let db = CheckpointDB::connect(db_path.to_str().unwrap())
        .await
        .expect("should connect and initialize SQLite WAL database");

    // 1. Verify WAL journal mode
    let mode = db.get_journal_mode().await.unwrap();
    assert_eq!(mode.to_lowercase(), "wal");

    // 2. Verify settings persistence
    let mut settings = HashMap::new();
    settings.insert("mybox_pat".to_string(), "pat_secret_123".to_string());
    settings.insert("immich_url".to_string(), "http://192.168.0.10:2283".to_string());
    db.save_settings(&settings).await.unwrap();

    let loaded = db.get_all_settings().await.unwrap();
    assert_eq!(loaded.get("mybox_pat").map(|s| s.as_str()), Some("pat_secret_123"));
    assert_eq!(
        loaded.get("immich_url").map(|s| s.as_str()),
        Some("http://192.168.0.10:2283")
    );

    // 3. Verify folder checkpoint lifecycle with composite key (source_type, folder_id)
    db.upsert_folder("mybox", "root", "/").await.unwrap();
    db.upsert_folder("gdrive", "root", "/").await.unwrap();
    db.update_folder_cursor("mybox", "root", Some("cursor_page_2"))
        .await
        .unwrap();

    let pending_mybox_folders = db.get_pending_folders("mybox").await.unwrap();
    assert_eq!(pending_mybox_folders.len(), 1);
    assert_eq!(
        pending_mybox_folders[0].last_cursor.as_deref(),
        Some("cursor_page_2")
    );

    db.mark_folder_scanned("mybox", "root").await.unwrap();
    assert!(db.get_pending_folders("mybox").await.unwrap().is_empty());
    // gdrive 'root' folder should still be pending (composite key isolation)
    assert_eq!(db.get_pending_folders("gdrive").await.unwrap().len(), 1);

    // 4. Verify file checkpoint lifecycle: PENDING -> UPLOADED -> DELETED and FAILED -> reset
    let files = vec![
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "res_photo_1".to_string(),
            name: "trip.jpg".to_string(),
            parent_path: "/2026/Summer".to_string(),
            size: 1024 * 500,
            created_time: "2026-07-01T10:00:00Z".to_string(),
            modified_time: "2026-07-01T10:00:00Z".to_string(),
            target_type: "immich".to_string(),
            sha1_hash: None,
        },
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "res_doc_1".to_string(),
            name: "tax.pdf".to_string(),
            parent_path: "/Docs".to_string(),
            size: 2048,
            created_time: "2026-08-01T10:00:00Z".to_string(),
            modified_time: "2026-08-01T10:00:00Z".to_string(),
            target_type: "nextcloud".to_string(),
            sha1_hash: None,
        },
    ];
    db.upsert_files(&files).await.unwrap();

    let pending_files = db.get_pending_files("mybox", 3, 100).await.unwrap();
    assert_eq!(pending_files.len(), 2);

    // Mark photo uploaded (created in Immich)
    db.mark_file_uploaded(
        "mybox",
        "res_photo_1",
        "abcdef1234567890abcdef1234567890abcdef12",
        "immich_asset_99",
        "created",
    )
    .await
    .unwrap();

    // Re-upserting already UPLOADED file during a re-scan must NOT revert its status to PENDING
    db.upsert_files(&files).await.unwrap();
    let pending_after_rescan = db.get_pending_files("mybox", 3, 100).await.unwrap();
    assert_eq!(pending_after_rescan.len(), 1);
    assert_eq!(pending_after_rescan[0].resource_id, "res_doc_1");

    // Mark doc failed 3 times so it reaches max_retries and transitions to FAILED
    db.mark_file_failed("mybox", "res_doc_1", "WebDAV 503", 3).await.unwrap();
    db.mark_file_failed("mybox", "res_doc_1", "WebDAV 503", 3).await.unwrap();
    db.mark_file_failed("mybox", "res_doc_1", "WebDAV 503", 3).await.unwrap();

    let stats = db.get_stats("mybox").await.unwrap();
    assert_eq!(stats.uploaded_immich_created, 1);
    assert_eq!(stats.failed_files, 1);

    let failures = db.get_recent_failures("mybox", 10).await.unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].resource_id, "res_doc_1");

    // Reset failed files back to PENDING
    let reset_count = db.reset_failed_files("mybox").await.unwrap();
    assert_eq!(reset_count, 1);

    // Upload doc to Nextcloud and then delete photo
    db.mark_file_uploaded(
        "mybox",
        "res_doc_1",
        "1111111111111111111111111111111111111111",
        "/Cloud-Backup/Docs/tax.pdf",
        "created",
    )
    .await
    .unwrap();

    let deletable = db.get_uploaded_files_for_delete("mybox", 100).await.unwrap();
    assert_eq!(deletable.len(), 2);

    db.mark_file_deleted("mybox", "res_photo_1").await.unwrap();
    let stats_after_del = db.get_stats("mybox").await.unwrap();
    assert_eq!(stats_after_del.deleted_files, 1);
    assert_eq!(stats_after_del.uploaded_nextcloud, 1);
}
