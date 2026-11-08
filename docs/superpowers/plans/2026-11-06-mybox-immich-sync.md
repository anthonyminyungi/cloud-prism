# Multi-Cloud (MYBOX, Google Drive, Google Takeout) to NAS (Immich & Nextcloud) Rust Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a blazing-fast, memory-safe Rust CLI and Docker Compose engine (`mybox-sync`) optimized for TrueNAS SCALE that migrates photos/videos to self-hosted Immich and general files/documents to self-hosted Nextcloud from Naver MYBOX, Google Drive, and Google Takeout archives, backed by a SQLite (`WAL`) checkpoint database and hash/size-verified deferred remote deletion.

**Architecture:**
- **Source Trait (`SourceProvider`)**: Implemented by `MyboxProvider` (Naver MYBOX Open API), `GoogleDriveProvider` (Google Drive API v3 with server-side `sha1Checksum` zero-download deduplication), and `GoogleTakeoutProvider` (ZIP archive + JSON sidecar metadata restoration).
- **Router (`TargetRouter`)**: Classifies media extensions (`TargetType::Immich`) vs general files/documents (`TargetType::Nextcloud`).
- **Target Clients (`ImmichTarget`, `NextcloudTarget`)**: Async `reqwest` clients supporting Immich SHA-1 `bulk-upload-check` + streaming multipart upload, and Nextcloud WebDAV `MKCOL` recursive folder creation + `PUT` with `X-OC-MTime` original timestamp preservation + `HEAD` verification.
- **Checkpoint Engine (`CheckpointDB` + `SyncPipeline`)**: `tokio` multi-worker pipeline with token-bucket rate limiting (`limiter.rs`), `sqlx` SQLite `WAL` mode, RAII temp file cleanup on `tmpfs`, and `exit(0)` container auto-stop on completion.

**Tech Stack:** Rust (Edition 2021), `tokio`, `reqwest`, `sqlx` (SQLite), `sha1`, `hex`, `zip`, `serde`, `serde_json`, `clap`, `indicatif`, `comfy-table`, `anyhow`, `dotenvy`, `wiremock` (for async HTTP tests), Docker Compose.

## Global Constraints

- Rust Edition 2021 (`rustc >= 1.80`)
- SQLite must enable `PRAGMA journal_mode=WAL;`, `PRAGMA synchronous=NORMAL;`, and composite primary keys `(source_type, folder_id)` and `(source_type, resource_id)`
- Temporary downloads must stream into `/tmp/mybox_sync` (`tmpfs` on TrueNAS SCALE) and guarantee cleanup via an RAII guard (`TempFileGuard` implementing `Drop`) even on panic or early return
- `sync` mode must NEVER delete source files; `delete` mode must re-verify target existence (Immich `bulk-upload-check` for media, Nextcloud WebDAV `HEAD` size check for files) before invoking `source.delete_resource()`
- Container exits with code `0` upon finishing `auto`, `scan`, `sync`, or `delete`

---

### Task 1: Cargo Project Setup, Config, TargetRouter, and Async Rate Limiter

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `src/config.rs`
- Create: `src/router.rs`
- Create: `src/limiter.rs`
- Test: `tests/router_limiter_test.rs`

**Interfaces:**
- Produces:
  - `Config::from_env(require_credentials: bool) -> anyhow::Result<Config>`
  - `TargetType` enum (`Immich`, `Nextcloud`) and `TargetRouter::classify(filename: &str, enable_nextcloud: bool) -> Option<TargetType>`
  - `RateLimiter::new(rps: f64, max_retries: u32, base_delay: Duration)` with `pub async fn send_with_retry<F, Fut>(&self, factory: F) -> anyhow::Result<reqwest::Response>`

- [ ] **Step 1: Write `Cargo.toml` and failing test `tests/router_limiter_test.rs`**

```toml
# Cargo.toml
[package]
name = "mybox-immich-sync"
version = "0.1.0"
edition = "2021"
description = "Multi-Cloud (MYBOX, Google Drive, Takeout) to NAS (Immich & Nextcloud) async sync and deferred delete tool"

[[bin]]
name = "mybox-sync"
path = "src/main.rs"

[dependencies]
anyhow = "1.0"
async-trait = "0.1"
chrono = { version = "0.4", features = ["serde"] }
clap = { version = "4.5", features = ["derive", "env"] }
comfy-table = "7.1"
dotenvy = "0.15"
futures = "0.3"
hex = "0.4"
indicatif = "0.17"
 percent-encoding = "2.3"
reqwest = { version = "0.12", default-features = false, features = ["json", "multipart", "stream", "rustls-tls"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
sha1 = "0.10"
sqlx = { version = "0.8", features = ["runtime-tokio", "sqlite"] }
tokio = { version = "1.40", features = ["full"] }
tokio-util = { version = "0.7", features = ["io"] }
zip = "2.2"

[dev-dependencies]
tempfile = "3.12"
wiremock = "0.6"
```

```rust
// tests/router_limiter_test.rs
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use mybox_immich_sync::limiter::RateLimiter;
use mybox_immich_sync::router::{TargetRouter, TargetType};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

#[test]
fn test_target_router_classification() {
    assert_eq!(
        TargetRouter::classify("IMG_001.HEIC", true),
        Some(TargetType::Immich)
    );
    assert_eq!(
        TargetRouter::classify("vacation.mp4", false),
        Some(TargetType::Immich)
    );
    assert_eq!(
        TargetRouter::classify("tax_2025.pdf", true),
        Some(TargetType::Nextcloud)
    );
    assert_eq!(TargetRouter::classify("tax_2025.pdf", false), None);
}

struct FlakyResponder {
    calls: Arc<AtomicUsize>,
}

impl Respond for FlakyResponder {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        let prev = self.calls.fetch_add(1, Ordering::SeqCst);
        if prev < 2 {
            ResponseTemplate::new(429)
        } else {
            ResponseTemplate::new(200).set_body_string("ok")
        }
    }
}

#[tokio::test]
async fn test_rate_limiter_retries_on_429() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));

    Mock::given(method("GET"))
        .and(path("/test"))
        .respond_with(FlakyResponder {
            calls: calls.clone(),
        })
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let limiter = RateLimiter::new(50.0, 4, Duration::from_millis(10));
    let url = format!("{}/test", server.uri());

    let resp = limiter
        .send_with_retry(|| client.get(&url).send())
        .await
        .expect("should succeed after retries");

    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test router_limiter_test`
Expected: FAIL (missing modules `config`, `router`, `limiter`)

- [ ] **Step 3: Implement `src/config.rs`, `src/router.rs`, `src/limiter.rs`, and `src/lib.rs`**

```rust
// src/config.rs
use std::env;
use std::path::PathBuf;
use anyhow::{bail, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub mybox_pat: String,
    pub mybox_base_url: String,
    pub gdrive_token: String,
    pub gdrive_base_url: String,
    pub takeout_dir: PathBuf,
    pub immich_url: String,
    pub immich_api_key: String,
    pub nextcloud_url: String,
    pub nextcloud_user: String,
    pub nextcloud_app_password: String,
    pub nextcloud_base_dir: String,
    pub concurrency: usize,
    pub rate_limit_rps: f64,
    pub max_retries: u32,
    pub db_path: PathBuf,
    pub temp_dir: PathBuf,
}

impl Config {
    pub fn from_env(require_immich: bool) -> Result<Self> {
        let _ = dotenvy::dotenv();
        let immich_url = env::var("IMMICH_URL").unwrap_or_default().trim().trim_end_matches('/').to_string();
        let immich_api_key = env::var("IMMICH_API_KEY").unwrap_or_default().trim().to_string();

        if require_immich && (immich_url.is_empty() || immich_api_key.is_empty()) {
            bail!("Missing required environment variables: IMMICH_URL and IMMICH_API_KEY");
        }

        Ok(Self {
            mybox_pat: env::var("MYBOX_PAT").unwrap_or_default().trim().to_string(),
            mybox_base_url: env::var("MYBOX_BASE_URL")
                .unwrap_or_else(|_| "https://open-api.mybox.naver.com/v1".to_string())
                .trim_end_matches('/')
                .to_string(),
            gdrive_token: env::var("GDRIVE_ACCESS_TOKEN").unwrap_or_default().trim().to_string(),
            gdrive_base_url: env::var("GDRIVE_BASE_URL")
                .unwrap_or_else(|_| "https://www.googleapis.com/drive/v3".to_string())
                .trim_end_matches('/')
                .to_string(),
            takeout_dir: PathBuf::from(env::var("TAKEOUT_DIR").unwrap_or_else(|_| "./takeout".to_string())),
            immich_url,
            immich_api_key,
            nextcloud_url: env::var("NEXTCLOUD_URL").unwrap_or_default().trim().trim_end_matches('/').to_string(),
            nextcloud_user: env::var("NEXTCLOUD_USER").unwrap_or_default().trim().to_string(),
            nextcloud_app_password: env::var("NEXTCLOUD_APP_PASSWORD").unwrap_or_default().trim().to_string(),
            nextcloud_base_dir: env::var("NEXTCLOUD_BASE_DIR")
                .unwrap_or_else(|_| "/MYBOX_Backup".to_string()),
            concurrency: env::var("CONCURRENCY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3)
                .max(1),
            rate_limit_rps: env::var("RATE_LIMIT_RPS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3.0)
                .max(0.2),
            max_retries: env::var("MAX_RETRIES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5)
                .max(1),
            db_path: PathBuf::from(env::var("DB_PATH").unwrap_or_else(|_| "./data/sync_state.db".to_string())),
            temp_dir: PathBuf::from(env::var("TEMP_DIR").unwrap_or_else(|_| "./tmp/mybox_sync".to_string())),
        })
    }

    pub fn nextcloud_enabled(&self) -> bool {
        !self.nextcloud_url.is_empty()
            && !self.nextcloud_user.is_empty()
            && !self.nextcloud_app_password.is_empty()
    }
}
```

```rust
// src/router.rs
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetType {
    Immich,
    Nextcloud,
}

impl TargetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TargetType::Immich => "immich",
            TargetType::Nextcloud => "nextcloud",
        }
    }
}

const MEDIA_EXTENSIONS: &[&str] = &[
    "jpg", "jpeg", "png", "heic", "heif", "webp", "gif", "bmp", "tiff", "dng", "cr2", "nef", "arw",
    "mp4", "mov", "avi", "mkv", "webm", "3gp", "m4v",
];

pub struct TargetRouter;

impl TargetRouter {
    pub fn classify(filename: &str, enable_nextcloud: bool) -> Option<TargetType> {
        let ext = Path::new(filename)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase())
            .unwrap_or_default();

        if MEDIA_EXTENSIONS.contains(&ext.as_str()) {
            Some(TargetType::Immich)
        } else if enable_nextcloud && !filename.is_empty() {
            Some(TargetType::Nextcloud)
        } else {
            None
        }
    }
}
```

```rust
// src/limiter.rs
use std::future::Future;
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use reqwest::Response;
use tokio::sync::Mutex;

pub struct RateLimiter {
    min_interval: Duration,
    max_retries: u32,
    base_delay: Duration,
    last_call: Mutex<Option<Instant>>,
}

impl RateLimiter {
    pub fn new(rps: f64, max_retries: u32, base_delay: Duration) -> Self {
        let interval_secs = 1.0 / rps.max(0.1);
        Self {
            min_interval: Duration::from_secs_f64(interval_secs),
            max_retries: max_retries.max(1),
            base_delay,
            last_call: Mutex::new(None),
        }
    }

    pub async fn acquire(&self) {
        let mut guard = self.last_call.lock().await;
        if let Some(last) = *guard {
            let elapsed = last.elapsed();
            if elapsed < self.min_interval {
                tokio::time::sleep(self.min_interval - elapsed).await;
            }
        }
        *guard = Some(Instant::now());
    }

    pub async fn send_with_retry<F, Fut>(&self, mut factory: F) -> Result<Response>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<Response, reqwest::Error>>,
    {
        for attempt in 1..=self.max_retries {
            self.acquire().await;
            match factory().await {
                Ok(resp) => {
                    let status = resp.status();
                    if (status.as_u16() == 429 || status.is_server_error()) && attempt < self.max_retries {
                        let retry_delay = resp
                            .headers()
                            .get("Retry-After")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|s| s.parse::<u64>().ok())
                            .map(Duration::from_secs)
                            .unwrap_or_else(|| self.base_delay * 2u32.pow(attempt - 1));
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }
                    let checked = resp.error_for_status()?;
                    return Ok(checked);
                }
                Err(err) => {
                    if attempt < self.max_retries {
                        tokio::time::sleep(self.base_delay * 2u32.pow(attempt - 1)).await;
                    } else {
                        return Err(anyhow!(err));
                    }
                }
            }
        }
        Err(anyhow!("RateLimiter retries exhausted"))
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test router_limiter_test`
Expected: PASS (2 tests passed)

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml src/ tests/router_limiter_test.rs
git commit -m "feat(core): add Rust config, target router, and async rate limiter"
```

---

### Task 2: SQLite WAL Checkpoint Database Manager (`src/db.rs`)

**Files:**
- Create: `src/db.rs`
- Modify: `src/lib.rs`
- Test: `tests/db_test.rs`

**Interfaces:**
- Produces: `CheckpointDB` wrapping `sqlx::SqlitePool` (with WAL mode enabled) and methods:
  - `connect(db_path: &Path) -> Result<Self>`
  - `upsert_folder(...)`, `update_folder_cursor(...)`, `mark_folder_scanned(...)`, `get_pending_folders(source_type: &str)`
  - `upsert_files(source_type: &str, files: &[NewFileRecord]) -> Result<usize>`
  - `get_pending_files(source_type: &str, max_retries: i64) -> Result<Vec<PendingFileRow>>`
  - `mark_file_uploaded(source_type: &str, resource_id: &str, sha1_hash: &str, remote_target_id: &str, sync_result: &str) -> Result<()>`
  - `mark_file_failed(source_type: &str, resource_id: &str, error_msg: &str) -> Result<()>`
  - `get_uploaded_files_for_delete(source_type: &str) -> Result<Vec<UploadedFileRow>>`
  - `mark_file_deleted(source_type: &str, resource_id: &str) -> Result<()>`
  - `get_stats(source_type: Option<&str>) -> Result<DbStats>`

- [ ] **Step 1: Write failing test `tests/db_test.rs`**
- [ ] **Step 2: Run `cargo test --test db_test` to verify it fails**
- [ ] **Step 3: Implement `src/db.rs` using `sqlx::sqlite::SqliteConnectOptions`**
- [ ] **Step 4: Run `cargo test --test db_test` to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 3: Source Providers (`MyboxProvider`, `GoogleDriveProvider`, `GoogleTakeoutProvider`) & Target Providers (`ImmichTarget`, `NextcloudTarget`)

**Files:**
- Create: `src/sources/mod.rs`
- Create: `src/sources/mybox.rs`
- Create: `src/sources/gdrive.rs`
- Create: `src/sources/takeout.rs`
- Create: `src/targets/mod.rs`
- Create: `src/targets/immich.rs`
- Create: `src/targets/nextcloud.rs`

**Interfaces:**
- Produces:
  - `SourceProvider` trait & implementations (`MyboxProvider`, `GoogleDriveProvider`, `GoogleTakeoutProvider`)
  - `TempFileGuard` RAII struct in `src/sources/mod.rs` that removes `.part` files automatically on `Drop`
  - `ImmichTarget` (`check_bulk_hashes`, `upload_asset`)
  - `NextcloudTarget` (`ensure_folder_tree`, `check_file_exists`, `upload_file` with `X-OC-MTime` header)

- [ ] **Step 1: Implement `src/sources/` and `src/targets/` with streaming SHA-1 and RAII temp file guard**
- [ ] **Step 2: Verify compilation and unit tests**
- [ ] **Step 3: Commit**

---

### Task 4: Async Multi-Worker Pipeline (`src/pipeline.rs`), CLI (`src/main.rs`), and End-to-End Integration Tests

**Files:**
- Create: `src/pipeline.rs`
- Create: `src/main.rs`
- Test: `tests/pipeline_integration_test.rs`

**Interfaces:**
- Produces:
  - `SyncPipeline` orchestrating `run_scan()`, `run_sync()` (including zero-download deduplication when `sha1_hash` is already provided by Google Drive!), and `run_delete(dry_run: bool)` (with Immich hash re-verification and Nextcloud `HEAD` size re-verification).
  - `mybox-sync` binary CLI (`--source {mybox,gdrive,takeout}` + `auto`, `scan`, `sync`, `delete [--dry-run]`, `status`).

- [ ] **Step 1: Write end-to-end integration test `tests/pipeline_integration_test.rs` using `wiremock` for MYBOX, Google Drive, Immich, and Nextcloud WebDAV**
- [ ] **Step 2: Implement `src/pipeline.rs` and `src/main.rs`**
- [ ] **Step 3: Run `cargo test` to verify all tests pass**
- [ ] **Step 4: Commit**

---

### Task 5: TrueNAS SCALE Multi-Stage `Dockerfile`, `docker-compose.yml`, and Documentation

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `README.md`

- [ ] **Step 1: Write `Dockerfile`, `docker-compose.yml`, `.env.example`, and `README.md`**
- [ ] **Step 2: Run `cargo test --all` and `cargo clippy` check**
- [ ] **Step 3: Commit**
