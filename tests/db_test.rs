use mybox_immich_sync::db::{CheckpointDB, FileUpsert};
use tempfile::TempDir;

#[tokio::test]
async fn test_sqlite_wal_checkpoint_and_file_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("sync_state.db");
    let db = CheckpointDB::connect(db_path.to_str().unwrap())
        .await
        .expect("should connect and initialize SQLite WAL database");

    // 1. Verify WAL journal mode
    let mode = db.get_journal_mode().await.unwrap();
    assert_eq!(mode.to_lowercase(), "wal");

    // 2. Verify folder checkpoint lifecycle with composite key (source_type, folder_id)
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
    db.mark_file_failed("mybox", "res_doc_1", "WebDAV 503", 3)
        .await
        .unwrap();
    db.mark_file_failed("mybox", "res_doc_1", "WebDAV 503", 3)
        .await
        .unwrap();
    db.mark_file_failed("mybox", "res_doc_1", "WebDAV 503", 3)
        .await
        .unwrap();

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

    let deletable = db
        .get_uploaded_files_for_delete("mybox", 100)
        .await
        .unwrap();
    assert_eq!(deletable.len(), 2);

    db.mark_file_deleted("mybox", "res_photo_1").await.unwrap();
    let stats_after_del = db.get_stats("mybox").await.unwrap();
    assert_eq!(stats_after_del.deleted_files, 1);
    assert_eq!(stats_after_del.uploaded_nextcloud, 1);
}

#[tokio::test]
async fn test_incremental_scan_modification_and_remote_direct_delete_reconciliation() {
    let db = CheckpointDB::connect(":memory:").await.unwrap();

    // Cycle 1: Initial scan
    let (is_resuming, _) = db.prepare_scan("mybox").await.unwrap();
    assert!(!is_resuming);
    db.mark_folder_scanned("mybox", "root").await.unwrap();

    let initial_files = vec![
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "f_unchanged".to_string(),
            name: "keep.jpg".to_string(),
            parent_path: "/".to_string(),
            size: 1000,
            created_time: "2026-01-01T00:00:00Z".to_string(),
            modified_time: "2026-01-01T00:00:00Z".to_string(),
            target_type: "immich".to_string(),
            sha1_hash: None,
        },
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "f_modified".to_string(),
            name: "edit.jpg".to_string(),
            parent_path: "/".to_string(),
            size: 2000,
            created_time: "2026-01-01T00:00:00Z".to_string(),
            modified_time: "2026-01-01T00:00:00Z".to_string(),
            target_type: "immich".to_string(),
            sha1_hash: None,
        },
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "f_remote_del_uploaded".to_string(),
            name: "backed_up_then_deleted_on_cloud.pdf".to_string(),
            parent_path: "/".to_string(),
            size: 3000,
            created_time: "2026-01-01T00:00:00Z".to_string(),
            modified_time: "2026-01-01T00:00:00Z".to_string(),
            target_type: "nextcloud".to_string(),
            sha1_hash: None,
        },
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "f_remote_del_pending".to_string(),
            name: "deleted_before_sync.jpg".to_string(),
            parent_path: "/".to_string(),
            size: 4000,
            created_time: "2026-01-01T00:00:00Z".to_string(),
            modified_time: "2026-01-01T00:00:00Z".to_string(),
            target_type: "immich".to_string(),
            sha1_hash: None,
        },
    ];
    db.upsert_files(&initial_files).await.unwrap();

    // First 3 files are uploaded to NAS; 4th remains PENDING
    db.mark_file_uploaded("mybox", "f_unchanged", "h1", "id1", "created")
        .await
        .unwrap();
    db.mark_file_uploaded("mybox", "f_modified", "h2", "id2", "created")
        .await
        .unwrap();
    db.mark_file_uploaded("mybox", "f_remote_del_uploaded", "h3", "id3", "created")
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(15)).await;

    // Cycle 2: All folders were SCANNED, so prepare_scan starts a fresh incremental cycle
    let (is_resuming_2, scan_started_at) = db.prepare_scan("mybox").await.unwrap();
    assert!(!is_resuming_2);
    assert_eq!(db.get_pending_folders("mybox").await.unwrap().len(), 1);

    // On remote cloud:
    // - f_unchanged is untouched
    // - f_modified has new size & modified_time
    // - f_remote_del_uploaded & f_remote_del_pending were deleted directly by user on remote cloud
    // - f_new was newly added
    let cycle2_files = vec![
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "f_unchanged".to_string(),
            name: "keep.jpg".to_string(),
            parent_path: "/".to_string(),
            size: 1000,
            created_time: "2026-01-01T00:00:00Z".to_string(),
            modified_time: "2026-01-01T00:00:00Z".to_string(),
            target_type: "immich".to_string(),
            sha1_hash: None,
        },
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "f_modified".to_string(),
            name: "edit.jpg".to_string(),
            parent_path: "/".to_string(),
            size: 2500,
            created_time: "2026-01-01T00:00:00Z".to_string(),
            modified_time: "2026-02-01T00:00:00Z".to_string(),
            target_type: "immich".to_string(),
            sha1_hash: None,
        },
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "f_new".to_string(),
            name: "brand_new.jpg".to_string(),
            parent_path: "/".to_string(),
            size: 5000,
            created_time: "2026-02-01T00:00:00Z".to_string(),
            modified_time: "2026-02-01T00:00:00Z".to_string(),
            target_type: "immich".to_string(),
            sha1_hash: None,
        },
    ];
    db.upsert_files(&cycle2_files).await.unwrap();
    db.mark_folder_scanned("mybox", "root").await.unwrap();

    let (removed_unbacked, excluded_uploaded) = db
        .reconcile_missing_remote_items("mybox", &scan_started_at)
        .await
        .unwrap();
    assert_eq!(removed_unbacked, 1); // f_remote_del_pending removed
    assert_eq!(excluded_uploaded, 1); // f_remote_del_uploaded marked DELETED (excluded from sync/delete)

    // Only f_modified (2026-01-01) and f_new (2026-02-01) should be in the pending sync queue,
    // ordered oldest created_time first!
    let pending_queue = db.get_pending_files("mybox", 3, 100).await.unwrap();
    let pending_ids: Vec<&str> = pending_queue
        .iter()
        .map(|f| f.resource_id.as_str())
        .collect();
    assert_eq!(pending_ids, vec!["f_modified", "f_new"]);

    // Only f_unchanged should be in the remote-trash delete queue (f_remote_del_uploaded is already deleted remotely)
    let delete_queue = db
        .get_uploaded_files_for_delete("mybox", 100)
        .await
        .unwrap();
    assert_eq!(delete_queue.len(), 1);
    assert_eq!(delete_queue[0].resource_id, "f_unchanged");

    // NAS backup stats for f_remote_del_uploaded are still preserved!
    let stats = db.get_stats("mybox").await.unwrap();
    assert_eq!(stats.uploaded_nextcloud, 1);
}

#[tokio::test]
async fn test_upsert_files_heals_fallback_created_time_and_updates_exported_size() {
    let db = CheckpointDB::connect(":memory:").await.unwrap();

    // Simulate legacy gdrive row scanned without `fields` (1970 fallback timestamps & size 0)
    db.upsert_files(&[FileUpsert {
        source_type: "gdrive".to_string(),
        resource_id: "gdoc_legacy".to_string(),
        name: "회의록".to_string(),
        parent_path: "/".to_string(),
        size: 0,
        created_time: "1970-01-01T00:00:00Z".to_string(),
        modified_time: "1970-01-01T00:00:00Z".to_string(),
        target_type: "nextcloud".to_string(),
        sha1_hash: None,
    }])
    .await
    .unwrap();

    // Re-scan with proper Google Drive metadata & exported extension
    db.upsert_files(&[FileUpsert {
        source_type: "gdrive".to_string(),
        resource_id: "gdoc_legacy".to_string(),
        name: "회의록.docx".to_string(),
        parent_path: "/".to_string(),
        size: 0,
        created_time: "2025-06-15T09:30:00Z".to_string(),
        modified_time: "2025-06-16T10:00:00Z".to_string(),
        target_type: "nextcloud".to_string(),
        sha1_hash: None,
    }])
    .await
    .unwrap();

    let pending = db.get_pending_files("gdrive", 3, 10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].name, "회의록.docx");
    assert_eq!(pending[0].created_time, "2025-06-15T09:30:00Z");
    assert_eq!(pending[0].modified_time, "2025-06-16T10:00:00Z");

    // Mark uploaded with actual exported byte size -> must persist size for Nextcloud HEAD verification
    db.mark_file_uploaded_with_size(
        "gdrive",
        "gdoc_legacy",
        "abcd",
        "/GoogleDrive/회의록.docx",
        "created",
        Some(12345),
    )
    .await
    .unwrap();

    let deletable = db
        .get_uploaded_files_for_delete("gdrive", 10)
        .await
        .unwrap();
    assert_eq!(deletable.len(), 1);
    assert_eq!(deletable[0].size, 12345);
}

#[tokio::test]
async fn test_rescan_zero_size_uploaded_preserved_and_filtered_pending_and_batch_delete() {
    let db = CheckpointDB::connect(":memory:").await.unwrap();

    // 1. Insert a Google Doc (size = 0 at scan time) and a photo (size = 500)
    db.upsert_files(&[
        FileUpsert {
            source_type: "gdrive".to_string(),
            resource_id: "gdoc_1".to_string(),
            name: "기획서.docx".to_string(),
            parent_path: "/".to_string(),
            size: 0,
            created_time: "2026-01-01T00:00:00Z".to_string(),
            modified_time: "2026-01-01T00:00:00Z".to_string(),
            target_type: "nextcloud".to_string(),
            sha1_hash: None,
        },
        FileUpsert {
            source_type: "gdrive".to_string(),
            resource_id: "photo_1".to_string(),
            name: "pic.jpg".to_string(),
            parent_path: "/".to_string(),
            size: 500,
            created_time: "2026-01-02T00:00:00Z".to_string(),
            modified_time: "2026-01-02T00:00:00Z".to_string(),
            target_type: "immich".to_string(),
            sha1_hash: None,
        },
    ])
    .await
    .unwrap();

    // When enable_nextcloud = false, only immich pending files should be returned
    let immich_only = db
        .get_pending_files_for_config("gdrive", 3, 100, false)
        .await
        .unwrap();
    assert_eq!(immich_only.len(), 1);
    assert_eq!(immich_only[0].resource_id, "photo_1");

    // Mark gdoc_1 uploaded with actual exported size (9876 bytes)
    db.mark_file_uploaded_with_size(
        "gdrive",
        "gdoc_1",
        "sha1_gdoc",
        "/GoogleDrive/기획서.docx",
        "created",
        Some(9876),
    )
    .await
    .unwrap();
    db.mark_file_uploaded("gdrive", "photo_1", "sha1_pic", "imm_1", "created")
        .await
        .unwrap();

    // 2. Re-scan where Google Drive API again reports size = 0 for gdoc_1 with identical timestamps:
    // MUST NOT reset status to PENDING and MUST preserve size = 9876!
    db.upsert_files(&[FileUpsert {
        source_type: "gdrive".to_string(),
        resource_id: "gdoc_1".to_string(),
        name: "기획서.docx".to_string(),
        parent_path: "/".to_string(),
        size: 0,
        created_time: "2026-01-01T00:00:00Z".to_string(),
        modified_time: "2026-01-01T00:00:00Z".to_string(),
        target_type: "nextcloud".to_string(),
        sha1_hash: None,
    }])
    .await
    .unwrap();

    let pending_after = db.get_pending_files("gdrive", 3, 100).await.unwrap();
    assert!(
        pending_after.is_empty(),
        "Uploaded zero-metadata-size file must not reset to PENDING on rescan"
    );

    let deletable = db
        .get_uploaded_files_for_delete("gdrive", 100)
        .await
        .unwrap();
    let gdoc_row = deletable
        .iter()
        .find(|r| r.resource_id == "gdoc_1")
        .unwrap();
    assert_eq!(gdoc_row.size, 9876);

    // 3. Batch mark deleted
    let deleted_cnt = db
        .mark_files_deleted_batch("gdrive", &["gdoc_1".to_string(), "photo_1".to_string()])
        .await
        .unwrap();
    assert_eq!(deleted_cnt, 2);
    assert_eq!(db.get_stats("gdrive").await.unwrap().deleted_files, 2);
}

#[tokio::test]
async fn test_repair_candidates_immich_asset_lookup_and_reclassification() {
    use mybox_immich_sync::router::TargetType;

    let db = CheckpointDB::connect(":memory:").await.unwrap();

    db.upsert_files(&[
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "res_lp_1".to_string(),
            name: "livephoto.,zip".to_string(),
            parent_path: "/2023/아이폰".to_string(),
            size: 4096,
            created_time: "2023-05-01T10:00:00Z".to_string(),
            modified_time: "2023-05-01T10:00:00Z".to_string(),
            target_type: "nextcloud".to_string(),
            sha1_hash: None,
        },
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "res_null_1".to_string(),
            name: "IMG_7777.(null)".to_string(),
            parent_path: "/2023/아이폰".to_string(),
            size: 2048,
            created_time: "2023-05-02T10:00:00Z".to_string(),
            modified_time: "2023-05-02T10:00:00Z".to_string(),
            target_type: "nextcloud".to_string(),
            sha1_hash: None,
        },
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "res_normal_zip".to_string(),
            name: "work_backup.zip".to_string(),
            parent_path: "/업무".to_string(),
            size: 8192,
            created_time: "2023-05-03T10:00:00Z".to_string(),
            modified_time: "2023-05-03T10:00:00Z".to_string(),
            target_type: "nextcloud".to_string(),
            sha1_hash: None,
        },
        FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "res_icon_1".to_string(),
            name: "favicon.png".to_string(),
            parent_path: "/웹프로젝트/assets".to_string(),
            size: 512,
            created_time: "2024-01-01T00:00:00Z".to_string(),
            modified_time: "2024-01-01T00:00:00Z".to_string(),
            target_type: "immich".to_string(),
            sha1_hash: None,
        },
    ])
    .await
    .unwrap();

    db.mark_file_uploaded(
        "mybox",
        "res_lp_1",
        "sha_lp",
        "/MyBox/2023/아이폰/livephoto.,zip",
        "created",
    )
    .await
    .unwrap();
    db.mark_file_uploaded(
        "mybox",
        "res_null_1",
        "sha_null",
        "/MyBox/2023/아이폰/IMG_7777.(null)",
        "created",
    )
    .await
    .unwrap();
    db.mark_file_uploaded(
        "mybox",
        "res_normal_zip",
        "sha_zip",
        "/MyBox/업무/work_backup.zip",
        "created",
    )
    .await
    .unwrap();
    db.mark_file_uploaded(
        "mybox",
        "res_icon_1",
        "sha_icon",
        "immich-asset-uuid-icon-1",
        "created",
    )
    .await
    .unwrap();

    // 1. Only livephoto.,zip and IMG_7777.(null) must be returned as Nextcloud repair candidates
    assert_eq!(db.count_nextcloud_repair_candidates().await.unwrap(), 2);
    let candidates = db.get_nextcloud_repair_candidates().await.unwrap();
    assert_eq!(candidates.len(), 2);

    // 2. Reclassify res_lp_1 from Nextcloud to Immich
    db.update_file_reclassified(
        "mybox",
        "res_lp_1",
        "IMG_0001.JPG",
        TargetType::Immich,
        "immich-live-photo-uuid-1",
        "sha_img_0001",
        3500,
        "created",
    )
    .await
    .unwrap();

    assert_eq!(db.count_nextcloud_repair_candidates().await.unwrap(), 1);

    // 3. Lookup Immich asset by remote_target_id or deviceAssetId and reclassify to Nextcloud
    let matched = db
        .find_file_for_immich_asset("immich-asset-uuid-icon-1", Some("res_icon_1"), "favicon.png")
        .await
        .unwrap()
        .expect("should find icon record in DB");
    assert_eq!(matched.parent_path, "/웹프로젝트/assets");

    db.update_file_reclassified(
        &matched.source_type,
        &matched.resource_id,
        &matched.name,
        TargetType::Nextcloud,
        "/MyBox/웹프로젝트/assets/favicon.png",
        "sha_icon",
        512,
        "created",
    )
    .await
    .unwrap();

    let stats = db.get_stats("mybox").await.unwrap();
    // Immich now has res_lp_1 (1), Nextcloud has res_null_1 + res_normal_zip + res_icon_1 (3)
    assert_eq!(stats.uploaded_immich_created, 1);
    assert_eq!(stats.uploaded_nextcloud, 3);
}
