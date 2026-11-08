# TrueNAS SCALE Web App: Multi-Cloud (MYBOX, Google) to NAS (Immich & Nextcloud) Rust Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a user-friendly TrueNAS SCALE Web App container in Rust (`axum` + `tokio` + `reqwest` + `sqlx`) with an embedded real-time Web UI dashboard (`:3080`) that lets users configure API tokens (via TrueNAS env vars or Web UI), test connections, explicitly trigger/pause background syncs from Naver MYBOX / Google Drive / Google Takeout, route photos/videos to Immich and general files to Nextcloud, monitor live progress via SSE, and trigger hash/size-verified batch remote deletions.

**Architecture:**
- **Embedded Web Server & Job Controller (`src/web.rs`, `src/state.rs`, `src/ui.html`)**: `axum` serves the single-page dashboard and REST/SSE APIs (`/api/settings`, `/api/connections/test`, `/api/status`, `/api/jobs/start`, `/api/jobs/stop`, `/api/events`). Container starts in `Idle` state (0% CPU, ~10MB RAM) until the user starts a job in the UI.
- **Persistent Settings & Checkpoint DB (`src/db.rs`)**: SQLite (`WAL` mode) stores both runtime settings (`settings` table, falling back to environment variables) and per-file checkpoints (`folders` and `files` tables).
- **Plugin Pipeline (`src/sources/`, `src/targets/`, `src/router.rs`, `src/pipeline.rs`)**: `SourceProvider` trait (`MyboxProvider`, `GoogleDriveProvider`, `GoogleTakeoutProvider`) streams files to `tmpfs` with an RAII guard while computing SHA-1, routes media to `ImmichTarget` and documents/files to `NextcloudTarget`, and re-verifies targets before remote trash deletion.

**Tech Stack:** Rust (Edition 2021), `axum`, `tokio`, `tokio-stream`, `reqwest`, `sqlx` (SQLite WAL), `sha1`, `hex`, `zip`, `serde`, `serde_json`, `clap`, `anyhow`, `dotenvy`, `wiremock` (testing), Docker Compose.

## Global Constraints

- Rust Edition 2021 (`rustc >= 1.80`)
- Default execution (`mybox-sync` or `mybox-sync serve`) starts the Web UI server on `0.0.0.0:3080` (configurable via `PORT` env var); CLI batch subcommands (`auto`, `scan`, `sync`, `delete`, `status`) remain supported
- Settings resolution order: SQLite `settings` table value (if non-empty) overrides environment variable (`MYBOX_PAT`, `IMMICH_URL`, etc.)
- SQLite must enable `PRAGMA journal_mode=WAL;`, `PRAGMA synchronous=NORMAL;`, and composite primary keys `(source_type, folder_id)` and `(source_type, resource_id)`
- Temporary downloads must stream into `/tmp/mybox_sync` (`tmpfs` in Docker) and guarantee cleanup via `TempFileGuard` (`Drop` trait)
- `sync` mode must NEVER delete source files; `delete` mode must re-verify existence in Immich (`bulk-upload-check`) or Nextcloud (WebDAV `HEAD` size check) before calling `source.delete_resource()`

---

### Task 1: Cargo Setup, Dynamic Config, TargetRouter, and Rate Limiter

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `src/config.rs`
- Create: `src/router.rs`
- Create: `src/limiter.rs`
- Test: `tests/router_limiter_test.rs`

**Interfaces:**
- Produces:
  - `AppConfig` struct (`Serialize`, `Deserialize`, `Clone`) in `src/config.rs` with `from_env() -> Self` and `apply_db_overrides(&mut self, map: &HashMap<String, String>)`
  - `TargetType` enum (`Immich`, `Nextcloud`) and `TargetRouter::classify(filename: &str, enable_nextcloud: bool) -> Option<TargetType>`
  - `RateLimiter` in `src/limiter.rs` with token-bucket pacing and exponential backoff on HTTP `429`/`5xx`

- [x] **Step 1: Create `Cargo.toml` (including `axum`, `tokio-stream`, `tower-http`) and failing test `tests/router_limiter_test.rs`**

```toml
# Cargo.toml
[package]
name = "mybox-immich-sync"
version = "0.1.0"
edition = "2021"
description = "TrueNAS SCALE Web App for Multi-Cloud (MYBOX, Google) to NAS (Immich & Nextcloud) migration and deferred delete"

[[bin]]
name = "mybox-sync"
path = "src/main.rs"

[dependencies]
anyhow = "1.0"
async-trait = "0.1"
axum = "0.7"
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
tokio-stream = { version = "0.1", features = ["sync"] }
tokio-util = { version = "0.7", features = ["io"] }
zip = "2.2"

[dev-dependencies]
tempfile = "3.12"
tower = { version = "0.5", features = ["util"] }
wiremock = "0.6"
```

- [x] **Step 2: Run `cargo test --test router_limiter_test` to verify it fails**
- [x] **Step 3: Implement `src/config.rs`, `src/router.rs`, `src/limiter.rs`, and `src/lib.rs`**
- [x] **Step 4: Run `cargo test --test router_limiter_test` to verify it passes**
- [x] **Step 5: Commit**

---

### Task 2: SQLite WAL Database for Settings & File Checkpoints (`src/db.rs`)

**Files:**
- Create: `src/db.rs`
- Test: `tests/db_test.rs`

**Interfaces:**
- Produces: `CheckpointDB` with:
  - `get_all_settings() -> Result<HashMap<String, String>>`
  - `save_settings(entries: &HashMap<String, String>) -> Result<()>`
  - Folder checkpoint methods (`upsert_folder`, `update_folder_cursor`, `mark_folder_scanned`, `get_pending_folders`)
  - File checkpoint methods (`upsert_files`, `get_pending_files`, `mark_file_uploaded`, `mark_file_failed`, `reset_failed_files`, `get_uploaded_files_for_delete`, `mark_file_deleted`, `get_stats`, `get_recent_failures`)

- [x] **Step 1: Write failing test `tests/db_test.rs` testing both `settings` persistence and `(source_type, resource_id)` file lifecycle**
- [x] **Step 2: Run `cargo test --test db_test` to verify it fails**
- [x] **Step 3: Implement `src/db.rs`**
- [x] **Step 4: Run `cargo test --test db_test` to verify it passes**
- [x] **Step 5: Commit**

---

### Task 3: Source Providers (`MyboxProvider`, `GoogleDriveProvider`, `GoogleTakeoutProvider`) & Target Providers (`ImmichTarget`, `NextcloudTarget`) with Connection Testing

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
  - `SourceProvider` trait including `async fn test_connection(&self) -> Result<String>` (e.g., MYBOX returns quota usage string; Google Drive returns user/quota info; Takeout returns count of `.zip` archives found)
  - `ImmichTarget` including `async fn test_connection(&self) -> Result<String>` and `check_bulk_hashes` / `upload_asset`
  - `NextcloudTarget` including `async fn test_connection(&self) -> Result<String>`, `ensure_folder_tree`, `check_file_exists`, and `upload_file` (with `X-OC-MTime`)

- [x] **Step 1: Implement `src/sources/` and `src/targets/` with connection test methods and RAII `TempFileGuard`**
- [x] **Step 2: Verify compilation and unit tests**
- [x] **Step 3: Commit**

---

### Task 4: Background Job Manager, Event Broadcaster, and Pipeline (`src/state.rs`, `src/pipeline.rs`)

**Files:**
- Create: `src/state.rs`
- Create: `src/pipeline.rs`
- Test: `tests/pipeline_integration_test.rs`

**Interfaces:**
- Produces:
  - `JobState` enum (`Idle`, `Scanning`, `Syncing`, `Deleting`, `Stopping`)
  - `ProgressEvent` struct broadcast over `tokio::sync::broadcast::Sender<ProgressEvent>` and stored in a ring buffer of recent logs
  - `AppState` (`Arc<AppState>`) managing the active background `JoinHandle`, stop flag (`AtomicBool`), `CheckpointDB`, and SSE broadcast channel
  - `SyncPipeline` emitting real-time `ProgressEvent` updates during `run_scan()`, `run_sync()`, and `run_delete(dry_run)`

- [x] **Step 1: Write `tests/pipeline_integration_test.rs` verifying MYBOX/GDrive scan -> route to Immich/Nextcloud -> deferred delete with event emission**
- [x] **Step 2: Implement `src/state.rs` and `src/pipeline.rs`**
- [x] **Step 3: Run `cargo test --test pipeline_integration_test` to verify it passes**
- [x] **Step 4: Commit**

---

### Task 5: Axum Web Server, REST/SSE APIs, and Embedded Dashboard UI (`src/web.rs`, `src/ui.html`, `src/main.rs`)

**Files:**
- Create: `src/web.rs`
- Create: `src/ui.html`
- Create: `src/main.rs`
- Test: `tests/web_api_test.rs`

**Interfaces:**
- Produces:
  - `build_router(state: Arc<AppState>) -> axum::Router` exposing:
    - `GET /` (serves `src/ui.html`)
    - `GET /api/settings` & `PUT /api/settings`
    - `POST /api/connections/test`
    - `GET /api/status`
    - `POST /api/jobs/start`, `POST /api/jobs/stop`, `POST /api/jobs/retry-failed`
    - `GET /api/events` (SSE stream)
  - Responsive Korean Web Dashboard (`src/ui.html`) with live progress bars, Immich/Nextcloud cards, connection tester, settings drawer, delete confirmation modal (with Dry-Run option), and live SSE console.

- [x] **Step 1: Write `tests/web_api_test.rs` testing `/api/settings`, `/api/connections/test`, `/api/status`, and `/api/jobs/start`**
- [x] **Step 2: Implement `src/ui.html`, `src/web.rs`, and `src/main.rs`**
- [x] **Step 3: Run `cargo test --test web_api_test` to verify all endpoints pass**
- [x] **Step 4: Commit**

---

### Task 6: TrueNAS SCALE Multi-Stage `Dockerfile`, `docker-compose.yml`, and Documentation

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `README.md`

- [x] **Step 1: Create `Dockerfile` (exposing port `3080`, default command `serve`), TrueNAS SCALE `docker-compose.yml`, `.env.example`, and `README.md`**
- [x] **Step 2: Run full `cargo test` suite**
- [x] **Step 3: Commit**
