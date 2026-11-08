use std::collections::HashMap;
use std::time::Instant;
use mybox_immich_sync::config::AppConfig;
use mybox_immich_sync::limiter::RateLimiter;
use mybox_immich_sync::router::{TargetRouter, TargetType};
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

    // Documents/archives route to Nextcloud when enabled, None when disabled
    assert_eq!(
        TargetRouter::classify("contract_2026.pdf", true),
        Some(TargetType::Nextcloud)
    );
    assert_eq!(
        TargetRouter::classify("report.hwpx", true),
        Some(TargetType::Nextcloud)
    );
    assert_eq!(
        TargetRouter::classify("contract_2026.pdf", false),
        None
    );
}

#[test]
fn test_config_db_overrides_precedence() {
    let mut cfg = AppConfig::default();
    cfg.mybox_pat = "env_pat_token".to_string();
    cfg.immich_url = "http://env-immich:2283".to_string();
    cfg.concurrency = 3;

    let mut overrides = HashMap::new();
    overrides.insert("mybox_pat".to_string(), "db_override_pat".to_string());
    // Empty value in DB should NOT overwrite existing non-empty env config
    overrides.insert("immich_url".to_string(), "".to_string());
    overrides.insert("concurrency".to_string(), "8".to_string());

    cfg.apply_db_overrides(&overrides);

    assert_eq!(cfg.mybox_pat, "db_override_pat");
    assert_eq!(cfg.immich_url, "http://env-immich:2283");
    assert_eq!(cfg.concurrency, 8);
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
}
