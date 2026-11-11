use mybox_immich_sync::config::{
    normalize_base_url, normalize_mybox_api_base, normalize_nextcloud_url, AppConfig,
};
use mybox_immich_sync::limiter::RateLimiter;
use mybox_immich_sync::router::{
    extract_timestamp_from_filename, inspect_magic_bytes, pick_older_timestamp,
    replace_or_append_extension, DetectedFileKind, SourceType, TargetRouter, TargetType,
};
use mybox_immich_sync::targets::nextcloud::NextcloudTarget;
use mybox_immich_sync::targets::TargetProvider;
use std::collections::HashMap;
use std::time::Instant;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn test_target_router_classification() {
    // Media extensions route to Immich regardless of enable_nextcloud
    assert_eq!(
        TargetRouter::classify("IMG_20261106.HEIC", true),
        Some(TargetType::Immich)
    );
    assert_eq!(
        TargetRouter::classify("family_video.MP4", false),
        Some(TargetType::Immich)
    );
    assert_eq!(
        TargetRouter::classify("raw_photo.DNG", false),
        Some(TargetType::Immich)
    );

    // LivePhoto ZIP and (null) extension route to Immich during scan so they are inspected/unpacked
    assert!(TargetRouter::is_livephoto_archive_name(
        "IMG_1234.livephoto.zip"
    ));
    assert!(TargetRouter::is_livephoto_archive_name("livephoto.,zip"));
    assert!(TargetRouter::is_livephoto_archive_name(
        "20230501_LIVEPHOTO.ZIP"
    ));
    assert!(!TargetRouter::is_livephoto_archive_name("project_backup.zip"));

    assert!(TargetRouter::has_null_extension("IMG_9999.(null)"));
    assert!(TargetRouter::has_null_extension("20230501_112233(null)"));
    assert!(TargetRouter::has_null_extension("photo.null"));
    assert!(!TargetRouter::has_null_extension("photo.jpg"));

    assert_eq!(
        TargetRouter::classify("IMG_1234.livephoto.zip", false),
        Some(TargetType::Immich)
    );
    assert_eq!(
        TargetRouter::classify("livephoto.,zip", true),
        Some(TargetType::Immich)
    );
    assert_eq!(
        TargetRouter::classify("IMG_9999.(null)", false),
        Some(TargetType::Immich)
    );

    // Documents/archives route to Nextcloud when enabled, None when disabled
    assert_eq!(
        TargetRouter::classify("contract_2026.pdf", true),
        Some(TargetType::Nextcloud)
    );
    assert_eq!(
        TargetRouter::classify("report.hwpx", true),
        Some(TargetType::Nextcloud)
    );
    assert_eq!(TargetRouter::classify("contract_2026.pdf", false), None);
}

#[test]
fn test_magic_bytes_extension_recovery_and_filename_dates() {
    assert_eq!(
        inspect_magic_bytes(&[0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10]),
        Some(DetectedFileKind::Media {
            ext: "jpg",
            mime: "image/jpeg",
            is_video: false,
        })
    );
    assert_eq!(
        inspect_magic_bytes(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]),
        Some(DetectedFileKind::Media {
            ext: "png",
            mime: "image/png",
            is_video: false,
        })
    );
    assert_eq!(
        inspect_magic_bytes(b"RIFF\x24\x00\x00\x00WEBPVP8 "),
        Some(DetectedFileKind::Media {
            ext: "webp",
            mime: "image/webp",
            is_video: false,
        })
    );
    assert_eq!(
        inspect_magic_bytes(b"\x00\x00\x00\x18ftypheic\x00\x00\x00\x00"),
        Some(DetectedFileKind::Media {
            ext: "heic",
            mime: "image/heic",
            is_video: false,
        })
    );
    assert_eq!(
        inspect_magic_bytes(b"\x00\x00\x00\x14ftypqt  \x20\x05\x03\x00"),
        Some(DetectedFileKind::Media {
            ext: "mov",
            mime: "video/quicktime",
            is_video: true,
        })
    );
    assert_eq!(
        inspect_magic_bytes(b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00"),
        Some(DetectedFileKind::Media {
            ext: "mp4",
            mime: "video/mp4",
            is_video: true,
        })
    );
    assert_eq!(
        inspect_magic_bytes(b"PK\x03\x04\x14\x00\x00\x00"),
        Some(DetectedFileKind::ZipArchive)
    );
    assert_eq!(
        inspect_magic_bytes(b"%PDF-1.7\n"),
        Some(DetectedFileKind::Document { ext: "pdf" })
    );

    assert_eq!(
        replace_or_append_extension("IMG_9999.(null)", "jpg"),
        "IMG_9999.jpg"
    );
    assert_eq!(
        replace_or_append_extension("20230501_112233(null)", "heic"),
        "20230501_112233.heic"
    );
    assert_eq!(
        replace_or_append_extension("IMG_1234.livephoto.zip", "heic"),
        "IMG_1234.heic"
    );
    assert_eq!(
        replace_or_append_extension("livephoto.,zip", "jpg"),
        "livephoto.jpg"
    );

    let kakao_ts = extract_timestamp_from_filename("KakaoTalk_20220514_153022123.jpg")
        .expect("should parse KakaoTalk date");
    assert!(kakao_ts.starts_with("2022-05-14T15:30:22"));

    let chosen = pick_older_timestamp("2026-11-11T02:00:00Z", Some(&kakao_ts));
    assert_eq!(chosen, kakao_ts);
}

#[test]
fn test_config_env_map_parsing_and_url_normalization() {
    let mut cfg = AppConfig::default();

    let mut env_map = HashMap::new();
    env_map.insert("mybox_pat".to_string(), "env_pat_token".to_string());
    // Trailing slash on URL should be stripped automatically
    env_map.insert(
        "immich_url".to_string(),
        "http://env-immich:2283/".to_string(),
    );
    // Empty value in env map should NOT overwrite default values
    env_map.insert("takeout_dir".to_string(), "".to_string());
    env_map.insert("concurrency".to_string(), "8".to_string());
    env_map.insert("sync_interval_minutes".to_string(), "180".to_string());

    cfg.apply_env_map(&env_map);

    assert_eq!(cfg.mybox_pat, "env_pat_token");
    assert_eq!(cfg.immich_url, "http://env-immich:2283");
    assert_eq!(cfg.takeout_dir, "/data/takeout");
    assert_eq!(cfg.concurrency, 8);
    assert_eq!(cfg.sync_interval_minutes, 180);
    assert!(cfg.summary().mybox_configured);
    assert_eq!(cfg.summary().sync_interval_minutes, 180);
    cfg.takeout_dir = "/nonexistent_takeout_dir_for_unit_test".to_string();
    assert_eq!(cfg.configured_sources().len(), 1);

    // Verify scheme auto-prefix and Nextcloud WebDAV suffix stripping
    assert_eq!(
        normalize_base_url("192.168.0.10:2283/"),
        "http://192.168.0.10:2283"
    );
    assert_eq!(
        normalize_nextcloud_url("https://192.168.0.10:9001/remote.php/dav/files/admin/"),
        "https://192.168.0.10:9001"
    );
    assert_eq!(
        normalize_nextcloud_url("https://nas.local/nextcloud/remote.php/dav/files/user"),
        "https://nas.local/nextcloud"
    );
    assert_eq!(
        normalize_mybox_api_base("https://apis.naver.com/mybox"),
        "https://open-api.mybox.naver.com"
    );
    assert_eq!(
        normalize_mybox_api_base("https://open-api.mybox.naver.com/v1/drive/"),
        "https://open-api.mybox.naver.com"
    );
}

#[tokio::test]
async fn test_rate_limiter_pacing_and_429_retry() {
    let mock_server = MockServer::start().await;

    // First two calls return 429 Too Many Requests, third returns 200 OK
    Mock::given(method("GET"))
        .and(path("/test-retry"))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(2)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/test-retry"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .mount(&mock_server)
        .await;

    let limiter = RateLimiter::with_base_delay_ms(50.0, 3, 10);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let url = format!("{}/test-retry", mock_server.uri());

    let resp = limiter
        .send_with_retry(|| client.get(&url))
        .await
        .expect("should succeed on 3rd attempt");

    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");

    // Verify token-bucket pacing (10 RPS = ~100ms interval after initial burst of 1)
    let paced_limiter = RateLimiter::new(10.0, 2);
    let start = Instant::now();
    paced_limiter.acquire().await;
    paced_limiter.acquire().await;
    paced_limiter.acquire().await;
    let elapsed = start.elapsed();
    assert!(
        elapsed.as_millis() >= 150,
        "Expected >= 150ms for 3 acquisitions at 10 RPS, got {:?}",
        elapsed
    );

    // Verify Nextcloud WebDAV test_connection strips /remote.php suffix and sends PROPFIND with trailing slash
    Mock::given(method("PROPFIND"))
        .and(path("/remote.php/dav/files/user@nas.local/"))
        .respond_with(ResponseTemplate::new(207))
        .mount(&mock_server)
        .await;

    let nc = NextcloudTarget::new(
        format!("{}/remote.php/dav/files/user@nas.local", mock_server.uri()),
        "user@nas.local".to_string(),
        "app_pass_123".to_string(),
        "/".to_string(),
        RateLimiter::new(50.0, 1),
    );
    let nc_msg = nc
        .test_connection()
        .await
        .expect("PROPFIND 207 should succeed");
    assert!(nc_msg.contains("Nextcloud WebDAV 연결 성공"));

    // Verify dynamic top-level folder mapping per SourceType (/MyBox, /GoogleDrive) when base_dir is "/"
    Mock::given(method("MKCOL"))
        .and(path("/remote.php/dav/files/user@nas.local/MyBox"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("MKCOL"))
        .and(path("/remote.php/dav/files/user@nas.local/MyBox/docs"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("MKCOL"))
        .and(path("/remote.php/dav/files/user@nas.local/GoogleDrive"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    let mybox_dir = nc
        .ensure_folder_tree(SourceType::Mybox, "/docs")
        .await
        .unwrap();
    assert_eq!(mybox_dir, "/MyBox/docs");

    let gdrive_dir = nc
        .ensure_folder_tree(SourceType::Gdrive, "/")
        .await
        .unwrap();
    assert_eq!(gdrive_dir, "/GoogleDrive");

    let takeout_dir = nc
        .ensure_folder_tree(SourceType::Takeout, "/")
        .await
        .unwrap();
    assert_eq!(takeout_dir, "/GoogleDrive");
}

#[test]
fn test_hybrid_temp_dir_selection_and_cleanup() {
    use mybox_immich_sync::sources::{
        cleanup_temp_dir, resolve_temp_dir, LARGE_FILE_DISK_FALLBACK_THRESHOLD,
    };
    use tempfile::TempDir;

    let ram_tmp = TempDir::new().unwrap();
    let disk_tmp = TempDir::new().unwrap();

    // Small file (< 256 MB) uses preferred tmpfs directory
    let chosen_small = resolve_temp_dir(
        ram_tmp.path(),
        Some(disk_tmp.path()),
        Some(10 * 1024 * 1024),
    );
    assert_eq!(chosen_small, ram_tmp.path());

    // Large file (>= 256 MB) automatically falls back to disk temp directory
    let chosen_large = resolve_temp_dir(
        ram_tmp.path(),
        Some(disk_tmp.path()),
        Some(LARGE_FILE_DISK_FALLBACK_THRESHOLD + 1),
    );
    assert_eq!(chosen_large, disk_tmp.path());

    // Stale file cleanup removes leftover files without deleting the directory itself
    let stale_file = ram_tmp.path().join("mybox_stale_123.tmp");
    std::fs::write(&stale_file, b"leftover").unwrap();
    assert!(stale_file.exists());
    cleanup_temp_dir(ram_tmp.path());
    assert!(!stale_file.exists());
    assert!(ram_tmp.path().exists());
}
