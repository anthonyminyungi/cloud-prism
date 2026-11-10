# NAS Production Readiness Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden Cloud Prism for 24/7 TrueNAS SCALE deployment by eliminating full-file RAM buffering during uploads, adding hybrid RAM/disk temporary storage fallback, fixing Takeout concurrent temp-file collisions and premature ZIP deletion when Nextcloud is disabled, preventing incremental rescan status resets on zero-size metadata items, enabling dynamic runtime auto-sync scheduling, separating source vs. LAN target rate limiters, and escaping HTML in the dashboard UI.

**Architecture:** Preserve the existing single-binary Rust architecture (`config`, `db`, `limiter`, `sources`, `targets`, `pipeline`, `state`, `web`, `ui.html`) and all existing REST/SSE contracts. Apply targeted, surgical upgrades across 5 self-contained tasks: (1) DB checkpoint & batch operations, (2) Streaming uploads & hybrid temp storage + timeouts, (3) Takeout concurrency, indexing, and non-media ZIP preservation guard, (4) Dynamic scheduler, split rate limiters, and bulk target verification in `pipeline`, and (5) Web `.part` upload atomicity, Unix `0600` settings permissions, and UI `escapeHtml` hardening.

**Tech Stack:** Rust 2021 (`tokio`, `axum`, `reqwest` with `stream`, `tokio-util` `ReaderStream`, `sea-orm` / `sqlx` SQLite WAL, `zip`, `sha1`), Vanilla JS / HTML5.

## Global Constraints

- Maintain 100% backward compatibility with all existing tests in `tests/db_test.rs`, `tests/pipeline_integration_test.rs`, `tests/router_limiter_test.rs`, and `tests/web_api_test.rs`.
- Zero emojis and zero CSS gradients in `src/ui.html`.
- Never load full file contents into a `Vec<u8>` during Immich or Nextcloud uploads; always stream via `tokio_util::io::ReaderStream` and `reqwest::Body::wrap_stream`.
- Preserve NAS backup files at all times: never delete files on Immich or Nextcloud, and never auto-delete local Takeout `.zip` archives if unclassified non-media files remain inside when `enable_nextcloud == false`.

---

### Task 1: Fix Incremental Rescan Reset on Zero-Size Metadata, Target-Filtered Pending Queue, and Batch Delete in `src/db.rs`

**Files:**
- Modify: `src/db.rs:325-411`
- Modify: `src/db.rs:461-481`
- Modify: `src/db.rs:612-626`
- Test: `tests/db_test.rs`

**Interfaces:**
- Consumes: `FileStatus`, `TargetType` from `crate::router`
- Produces:
  - Fixed `CheckpointDB::upsert_files(&self, files: &[FileUpsert]) -> Result<usize>` (preserves `UPLOADED` status and `old.size` when `f.size == 0` on rescan)
  - `CheckpointDB::get_pending_files_for_config(&self, source_type: &str, max_retries: i64, limit: i64, enable_nextcloud: bool) -> Result<Vec<FileRecord>>`
  - `CheckpointDB::mark_files_deleted_batch(&self, source_type: &str, resource_ids: &[String]) -> Result<u64>`

- [ ] **Step 1: Write the failing test in `tests/db_test.rs`**

Append the following test to `tests/db_test.rs`:

```rust
#[tokio::test]
async fn test_rescan_zero_size_uploaded_preserved_and_filtered_pending_and_batch_delete() {
    let db = CheckpointDB::connect(":memory:").await.unwrap();

    // 1. Insert a Google Doc (size = 0 at scan time) and a PDF (size = 500)
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

    let deletable = db.get_uploaded_files_for_delete("gdrive", 100).await.unwrap();
    let gdoc_row = deletable.iter().find(|r| r.resource_id == "gdoc_1").unwrap();
    assert_eq!(gdoc_row.size, 9876);

    // 3. Batch mark deleted
    let deleted_cnt = db
        .mark_files_deleted_batch(
            "gdrive",
            &["gdoc_1".to_string(), "photo_1".to_string()],
        )
        .await
        .unwrap();
    assert_eq!(deleted_cnt, 2);
    assert_eq!(db.get_stats("gdrive").await.unwrap().deleted_files, 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test db_test`
Expected: FAIL with missing methods `get_pending_files_for_config` and `mark_files_deleted_batch`.

- [ ] **Step 3: Implement the DB fixes and new methods in `src/db.rs`**

In `src/db.rs`, update `upsert_files` (lines 374–406) so `size_changed` only triggers when `f.size > 0 && old.size > 0 && old.size != f.size`, and preserve `old.size` when `f.size == 0 && old.size > 0`:

```rust
                Some(old) => {
                    let hash_changed = match (&f.sha1_hash, &old.sha1_hash) {
                        (Some(new_h), Some(old_h)) => new_h != old_h,
                        _ => false,
                    };
                    let size_changed = f.size > 0 && old.size > 0 && old.size != f.size;
                    let content_changed = size_changed
                        || old.created_time != f.created_time
                        || old.modified_time != f.modified_time
                        || hash_changed;

                    let preserved_size = if f.size == 0 && old.size > 0 && !content_changed {
                        old.size
                    } else {
                        f.size
                    };

                    let mut active: file_entity::ActiveModel = old.into();
                    active.name = Set(effective_name);
                    active.parent_path = Set(f.parent_path.clone());
                    active.size = Set(preserved_size);
                    active.created_time = Set(f.created_time.clone());
                    active.modified_time = Set(f.modified_time.clone());
                    active.target_type = Set(f.target_type.clone());
                    active.updated_at = Set(now.clone());

                    if content_changed {
                        active.sha1_hash = Set(f.sha1_hash.clone());
                        active.remote_target_id = Set(None);
                        active.sync_result = Set(None);
                        active.status = Set(FileStatus::Pending.as_str().to_string());
                        active.retry_count = Set(0);
                        active.error_msg = Set(None);
                    } else if f.sha1_hash.is_some() {
                        active.sha1_hash = Set(f.sha1_hash.clone());
                    }

                    file_entity::Entity::update(active).exec(&txn).await?;
                }
```

Also in `src/db.rs`, add `get_pending_files_for_config` and `mark_files_deleted_batch`:

```rust
    pub async fn get_pending_files(
        &self,
        source_type: &str,
        max_retries: i64,
        limit: i64,
    ) -> Result<Vec<FileRecord>> {
        self.get_pending_files_for_config(source_type, max_retries, limit, true)
            .await
    }

    pub async fn get_pending_files_for_config(
        &self,
        source_type: &str,
        max_retries: i64,
        limit: i64,
        enable_nextcloud: bool,
    ) -> Result<Vec<FileRecord>> {
        let mut query = file_entity::Entity::find()
            .filter(file_entity::Column::SourceType.eq(source_type))
            .filter(file_entity::Column::Status.eq(FileStatus::Pending.as_str()))
            .filter(file_entity::Column::RetryCount.lt(max_retries));

        if !enable_nextcloud {
            query = query.filter(file_entity::Column::TargetType.eq(TargetType::Immich.as_str()));
        }

        let rows = query
            .order_by_asc(file_entity::Column::CreatedTime)
            .order_by_asc(file_entity::Column::ModifiedTime)
            .order_by_asc(file_entity::Column::ParentPath)
            .order_by_asc(file_entity::Column::Name)
            .order_by_asc(file_entity::Column::ResourceId)
            .limit(Some(limit.max(0) as u64))
            .all(&self.conn)
            .await?;
        Ok(rows)
    }
```

And below `mark_file_deleted`:

```rust
    pub async fn mark_files_deleted_batch(
        &self,
        source_type: &str,
        resource_ids: &[String],
    ) -> Result<u64> {
        if resource_ids.is_empty() {
            return Ok(0);
        }
        let now = Utc::now().to_rfc3339();
        let txn = self.conn.begin().await?;
        let mut total = 0u64;

        for chunk in resource_ids.chunks(500) {
            let res = file_entity::Entity::update_many()
                .col_expr(
                    file_entity::Column::Status,
                    Expr::value(FileStatus::Deleted.as_str()),
                )
                .col_expr(file_entity::Column::UpdatedAt, Expr::value(now.clone()))
                .filter(file_entity::Column::SourceType.eq(source_type))
                .filter(file_entity::Column::ResourceId.is_in(chunk.iter().cloned()))
                .exec(&txn)
                .await?;
            total += res.rows_affected;
        }

        txn.commit().await?;
        Ok(total)
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test db_test`
Expected: PASS (4 tests passed).

- [ ] **Step 5: Commit**

```bash
git add src/db.rs tests/db_test.rs
git commit -m "fix(db): preserve uploaded zero-size metadata items on rescan and add batch delete"
```

---

### Task 2: Zero-Buffering Streaming Uploads, Hybrid Temp Storage Fallback, and HTTP Timeouts

**Files:**
- Modify: `src/sources/mod.rs:1-155`
- Modify: `src/targets/immich.rs:136-184`
- Modify: `src/targets/nextcloud.rs:211-258`
- Modify: `src/config.rs:254-265`
- Modify: `docker-compose.yml:18-21`
- Test: `tests/router_limiter_test.rs`

**Interfaces:**
- Consumes: `tokio_util::io::ReaderStream`, `reqwest::Body::wrap_stream`, `reqwest::multipart::Part::stream_with_length`
- Produces:
  - `LARGE_FILE_DISK_FALLBACK_THRESHOLD: u64 = 256 * 1024 * 1024`
  - `resolve_temp_dir(preferred_tmp: &Path, fallback_disk_tmp: Option<&Path>, size_hint: Option<u64>) -> PathBuf`
  - `cleanup_temp_dir(dir: &Path)`
  - `AppConfig::disk_fallback_tmp_dir(&self) -> PathBuf`
  - Streaming `ImmichTarget::upload_asset` with retry support
  - Streaming `NextcloudTarget::upload_file_with_resource` without `Vec<u8>` buffering

- [ ] **Step 1: Write the failing test in `tests/router_limiter_test.rs`**

Append the following test to `tests/router_limiter_test.rs`:

```rust
#[test]
fn test_hybrid_temp_dir_selection_and_cleanup() {
    use mybox_immich_sync::sources::{
        cleanup_temp_dir, resolve_temp_dir, LARGE_FILE_DISK_FALLBACK_THRESHOLD,
    };
    use tempfile::TempDir;

    let ram_tmp = TempDir::new().unwrap();
    let disk_tmp = TempDir::new().unwrap();

    // Small file (< 256 MB) uses preferred tmpfs directory
    let chosen_small = resolve_temp_dir(ram_tmp.path(), Some(disk_tmp.path()), Some(10 * 1024 * 1024));
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test router_limiter_test`
Expected: FAIL with unresolved imports `cleanup_temp_dir`, `resolve_temp_dir`, `LARGE_FILE_DISK_FALLBACK_THRESHOLD`.

- [ ] **Step 3: Implement hybrid temp helpers, HTTP/stream timeouts, and streaming uploads**

1. In `src/config.rs`, add `disk_fallback_tmp_dir(&self) -> PathBuf` below `settings_file_path`:

```rust
    pub fn disk_fallback_tmp_dir(&self) -> PathBuf {
        Path::new(&self.db_path)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(".tmp_sync")
    }
```

2. In `src/sources/mod.rs`, add `LARGE_FILE_DISK_FALLBACK_THRESHOLD`, `STREAM_CHUNK_TIMEOUT`, `resolve_temp_dir`, `cleanup_temp_dir`, and update `build_http_client`, `build_target_http_client`, and `stream_response_to_temp`:

```rust
pub const DEFAULT_FALLBACK_TIMESTAMP: &str = "1970-01-01T00:00:00Z";
pub const LARGE_FILE_DISK_FALLBACK_THRESHOLD: u64 = 256 * 1024 * 1024; // 256 MB
const HTTP_CONNECT_TIMEOUT_SECS: u64 = 15;
const HTTP_POOL_IDLE_TIMEOUT_SECS: u64 = 90;
const HTTP_TCP_KEEPALIVE_SECS: u64 = 30;
const STREAM_CHUNK_TIMEOUT_SECS: u64 = 60;

pub fn resolve_temp_dir(
    preferred_tmp: &Path,
    fallback_disk_tmp: Option<&Path>,
    size_hint: Option<u64>,
) -> PathBuf {
    if let (Some(disk_dir), Some(sz)) = (fallback_disk_tmp, size_hint) {
        if sz >= LARGE_FILE_DISK_FALLBACK_THRESHOLD {
            return disk_dir.to_path_buf();
        }
    }
    preferred_tmp.to_path_buf()
}

pub fn cleanup_temp_dir(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        let p = entry.path();
        if p.is_file() {
            let _ = std::fs::remove_file(p);
        }
    }
}

pub fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(std::time::Duration::from_secs(HTTP_CONNECT_TIMEOUT_SECS))
        .pool_idle_timeout(std::time::Duration::from_secs(HTTP_POOL_IDLE_TIMEOUT_SECS))
        .tcp_keepalive(std::time::Duration::from_secs(HTTP_TCP_KEEPALIVE_SECS))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

pub fn build_target_http_client(follow_redirects: bool) -> reqwest::Client {
    let redirect_policy = if follow_redirects {
        reqwest::redirect::Policy::default()
    } else {
        reqwest::redirect::Policy::none()
    };
    reqwest::Client::builder()
        .no_proxy()
        .danger_accept_invalid_certs(true)
        .redirect(redirect_policy)
        .connect_timeout(std::time::Duration::from_secs(HTTP_CONNECT_TIMEOUT_SECS))
        .pool_idle_timeout(std::time::Duration::from_secs(HTTP_POOL_IDLE_TIMEOUT_SECS))
        .tcp_keepalive(std::time::Duration::from_secs(HTTP_TCP_KEEPALIVE_SECS))
        .build()
        .unwrap_or_else(|_| build_http_client())
}
```

And in `stream_response_to_temp` (`src/sources/mod.rs`), wrap `stream.next()` with `tokio::time::timeout`:

```rust
    let chunk_timeout = std::time::Duration::from_secs(STREAM_CHUNK_TIMEOUT_SECS);
    loop {
        let next_item = tokio::time::timeout(chunk_timeout, stream.next())
            .await
            .map_err(|_| anyhow::anyhow!("다운로드 스트림 읽기 타임아웃 ({}초 초과)", STREAM_CHUNK_TIMEOUT_SECS))?;
        let Some(chunk_res) = next_item else {
            break;
        };
        let chunk = chunk_res?;
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
        total_bytes += chunk.len() as u64;
    }
```

3. In `src/targets/immich.rs`, update `upload_asset` to stream from `tokio::fs::File` using `tokio_util::io::ReaderStream` without loading `Vec<u8>` into memory:

```rust
    pub async fn upload_asset(
        &self,
        file_path: &Path,
        filename: &str,
        device_asset_id: &str,
        created_at: &str,
        modified_at: &str,
    ) -> Result<(String, SyncResultKind)> {
        let url = self.endpoint(ENDPOINT_ASSETS);
        let file_len = tokio::fs::metadata(file_path).await?.len();

        self.limiter.acquire().await;
        let file = tokio::fs::File::open(file_path).await?;
        let reader_stream = tokio_util::io::ReaderStream::new(file);
        let body = reqwest::Body::wrap_stream(reader_stream);
        let part = Part::stream_with_length(body, file_len).file_name(filename.to_string());
        let form = Form::new()
            .text("deviceAssetId", device_asset_id.to_string())
            .text("deviceId", DEVICE_ID)
            .text("fileCreatedAt", created_at.to_string())
            .text("fileModifiedAt", modified_at.to_string())
            .part("assetData", part);

        let resp = self
            .client
            .post(&url)
            .header(HEADER_API_KEY, &self.api_key)
            .multipart(form)
            .send()
            .await
            .map_err(|e| anyhow!("Immich 업로드 전송 오류: {}", format_error_chain(&e)))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Immich 자산 업로드 실패 (HTTP {}): {}",
                status,
                text
            ));
        }

        let dto: AssetUploadResponse = resp.json().await.unwrap_or_default();
        let id = dto.id.unwrap_or_else(|| "immich_uploaded_id".to_string());
        let kind = match dto.status.as_deref() {
            Some("duplicate") => SyncResultKind::Duplicate,
            _ => SyncResultKind::Created,
        };

        Ok((id, kind))
    }
```

4. In `src/targets/nextcloud.rs`, update `upload_file_with_resource` to stream directly from `tokio::fs::File` (via `std::fs::File` -> `tokio::fs::File::from_std` inside the retry closure so retries work cleanly without buffering into RAM):

```rust
        let file_len = tokio::fs::metadata(file_path).await?.len();
        let mtime_epoch = DateTime::parse_from_rfc3339(modified_time_iso)
            .ok()
            .map(|dt| dt.timestamp())
            .filter(|&ts| ts > 0)
            .unwrap_or_else(|| Utc::now().timestamp())
            .to_string();

        let resp = self
            .limiter
            .send_with_retry(|| {
                let std_file = std::fs::File::open(file_path).expect("temp file must exist during upload");
                let async_file = tokio::fs::File::from_std(std_file);
                let stream = tokio_util::io::ReaderStream::new(async_file);
                self.client
                    .put(&url)
                    .basic_auth(&self.user, Some(&self.app_password))
                    .header(HEADER_MTIME, &mtime_epoch)
                    .header(reqwest::header::CONTENT_LENGTH, file_len)
                    .body(reqwest::Body::wrap_stream(stream))
            })
            .await?;
```

5. In `docker-compose.yml`, increase `tmpfs` size from `1G` to `2G`:

```yaml
    tmpfs:
      # ZFS 디스크 쓰기(CoW) 및 스냅샷 부풀림 방지를 위해 임시 다운로드는 RAM(tmpfs)에서 처리 후 즉시 삭제 (대용량 256MB+ 파일은 /data/.tmp_sync로 자동 우회)
      - /tmp/mybox_sync:size=2G,mode=1777
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test router_limiter_test --test pipeline_integration_test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/config.rs src/sources/mod.rs src/targets/immich.rs src/targets/nextcloud.rs docker-compose.yml tests/router_limiter_test.rs
git commit -m "perf(io): stream target uploads without RAM buffering and add hybrid temp fallback"
```

---

### Task 3: Harden Google Takeout Concurrency, Archive Index Caching, and Non-Media ZIP Deletion Guard

**Files:**
- Modify: `src/sources/takeout.rs:1-406`
- Test: `src/sources/takeout.rs` (unit tests)

**Interfaces:**
- Consumes: `TargetRouter::is_media_extension` from `crate::router`
- Produces:
  - Collision-free temp filename in `GoogleTakeoutProvider::download_to_temp`: `takeout_{id_hash12}_{safe_filename}`
  - Cached `TakeoutArchiveIndex` inside `GoogleTakeoutProvider` avoiding repeated ZIP Central Directory reads
  - `tokio::task::spawn_blocking` extraction in `GoogleTakeoutProvider::download_to_temp`
  - `GoogleTakeoutProvider::count_unclassified_files(&self, enable_nextcloud: bool) -> Result<usize>`

- [ ] **Step 1: Write the failing tests in `src/sources/takeout.rs`**

Add the following unit tests inside `mod tests` in `src/sources/takeout.rs`:

```rust
    #[tokio::test]
    async fn test_takeout_concurrent_identical_filenames_and_unclassified_count() {
        let dir = TempDir::new().unwrap();
        let zip_path = dir.path().join("takeout-concurrent.zip");
        {
            let file = File::create(&zip_path).unwrap();
            let mut zip = ZipWriter::new(file);
            let opts = SimpleFileOptions::default();

            // Two identically named photos in different folders
            zip.start_file("Takeout/Google Photos/2023/IMG_0001.JPG", opts)
                .unwrap();
            zip.write_all(b"photo_2023_payload_aaaaaa").unwrap();

            zip.start_file("Takeout/Google Photos/2024/IMG_0001.JPG", opts)
                .unwrap();
            zip.write_all(b"photo_2024_payload_bbbbbbbbbb").unwrap();

            // One non-media document inside Drive
            zip.start_file("Takeout/Drive/계약서.pdf", opts).unwrap();
            zip.write_all(b"pdf_document_bytes").unwrap();

            zip.finish().unwrap();
        }

        let provider = std::sync::Arc::new(GoogleTakeoutProvider::new(dir.path()));

        // When Nextcloud is disabled, 1 unclassified file (계약서.pdf) must be detected
        assert_eq!(provider.count_unclassified_files(false).unwrap(), 1);
        assert_eq!(provider.count_unclassified_files(true).unwrap(), 0);

        // Concurrent download_to_temp of identically-named files must not collide on temp file path
        let tmp_dir = TempDir::new().unwrap();
        let p1 = provider.clone();
        let p2 = provider.clone();
        let t1 = tmp_dir.path().to_path_buf();
        let t2 = tmp_dir.path().to_path_buf();

        let (res1, res2) = tokio::join!(
            p1.download_to_temp("Takeout/Google Photos/2023/IMG_0001.JPG", "IMG_0001.JPG", &t1),
            p2.download_to_temp("Takeout/Google Photos/2024/IMG_0001.JPG", "IMG_0001.JPG", &t2)
        );
        let d1 = res1.unwrap();
        let d2 = res2.unwrap();

        assert_ne!(
            d1.temp_guard.path(),
            d2.temp_guard.path(),
            "Identical filenames in different folders must have distinct temp paths"
        );
        assert_eq!(d1.size, 25);
        assert_eq!(d2.size, 29);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib sources::takeout::tests::test_takeout_concurrent_identical_filenames_and_unclassified_count`
Expected: FAIL with missing method `count_unclassified_files`.

- [ ] **Step 3: Implement `TakeoutArchiveIndex`, unique temp filenames, `spawn_blocking`, and `count_unclassified_files` in `src/sources/takeout.rs`**

Update `GoogleTakeoutProvider` in `src/sources/takeout.rs`:
1. Add `TakeoutArchiveIndex` caching `(zip_signatures, entry_to_zip, sidecar_to_zip, non_media_count)` behind `Arc<std::sync::Mutex<Option<TakeoutArchiveIndex>>>`.
2. Add `pub fn count_unclassified_files(&self, enable_nextcloud: bool) -> Result<usize>` which returns `0` if `enable_nextcloud` is `true`, or `index.non_media_count` if `false`.
3. In `download_to_temp`, compute a 12-char hex prefix from `Sha1::digest(resource_id.as_bytes())` so the temp path is `tmp_dir.join(format!("takeout_{}_{}", id_hash12, sanitize_filename(filename)))`.
4. Only look up JSON sidecars when `TargetRouter::is_media_extension` is true for `filename`, using `index.sidecar_to_zip` in $O(1)$ time instead of scanning every sibling `.zip`.
5. Run the synchronous ZIP read + SHA-1 computation inside `tokio::task::spawn_blocking`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib sources::takeout`
Expected: PASS (all takeout unit tests pass).

- [ ] **Step 5: Commit**

```bash
git add src/sources/takeout.rs
git commit -m "fix(takeout): prevent concurrent temp file collisions and cache ZIP entry index"
```

---

### Task 4: Dynamic Auto-Sync Scheduler, Split RateLimiters, Bulk Verification, and Takeout Cleanup Guard in `src/pipeline.rs` & `src/state.rs`

**Files:**
- Modify: `src/state.rs:69-160`
- Modify: `src/pipeline.rs:78-100`
- Modify: `src/pipeline.rs:320-338`
- Modify: `src/pipeline.rs:360-518`
- Modify: `src/pipeline.rs:610-630`
- Modify: `src/pipeline.rs:680-760`
- Modify: `src/pipeline.rs:771-832`
- Test: `tests/pipeline_integration_test.rs`

**Interfaces:**
- Consumes:
  - `GoogleTakeoutProvider::count_unclassified_files`
  - `CheckpointDB::get_pending_files_for_config`, `CheckpointDB::mark_files_deleted_batch`
  - `resolve_temp_dir`, `cleanup_temp_dir`
  - `ImmichTarget::check_bulk_hashes`
- Produces:
  - `AppState::scheduler_notify: Arc<tokio::sync::Notify>` triggered on `update_settings`
  - Dynamic `spawn_auto_sync_scheduler(state: Arc<AppState>)` that wakes on `scheduler_notify` and reads `state.effective_config().await.sync_interval_minutes`
  - Separate `source_limiter` (`config.rate_limit_rps`) vs. `target_limiter` (`(config.rate_limit_rps * 10.0).max(50.0)`)
  - Takeout auto-cleanup guard preserving `.zip` files when `enable_nextcloud == false` and non-media files exist in the archive
  - Bulk Immich verification (`verify_uploaded_batch`) in `cleanup_takeout_archives_after_sync` and `run_delete`

- [ ] **Step 1: Write the failing test in `tests/pipeline_integration_test.rs`**

Append the following test to `tests/pipeline_integration_test.rs`:

```rust
#[tokio::test]
async fn test_takeout_preserves_zip_when_nextcloud_disabled_and_archive_contains_documents() {
    use std::fs::File;
    use std::io::Write;
    use zip::write::SimpleFileOptions;
    use zip::ZipWriter;

    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_takeout_guard.db");
    let tmp_download_dir = temp_dir.path().join("tmp_guard_sync");
    let takeout_dir = temp_dir.path().join("takeout_guard_staging");
    std::fs::create_dir_all(&takeout_dir).unwrap();

    let zip_path = takeout_dir.join("takeout-mixed-001.zip");
    {
        let file = File::create(&zip_path).unwrap();
        let mut zip = ZipWriter::new(file);
        let opts = SimpleFileOptions::default();
        zip.start_file("Takeout/Google 포토/2026/photo.jpg", opts)
            .unwrap();
        zip.write_all(b"photo_bytes_123").unwrap();
        zip.start_file("Takeout/드라이브/중요문서.pdf", opts)
            .unwrap();
        zip.write_all(b"important_pdf_bytes").unwrap();
        zip.finish().unwrap();
    }

    Mock::given(method("POST"))
        .and(path("/api/assets/bulk-upload-check"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                { "id": "Takeout/Google 포토/2026/photo.jpg", "action": "accept" }
            ]
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/assets"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": "immich_photo_1",
            "status": "created"
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let cfg = AppConfig {
        db_path: db_path.to_str().unwrap().to_string(),
        tmp_dir: tmp_download_dir.to_str().unwrap().to_string(),
        takeout_dir: takeout_dir.to_str().unwrap().to_string(),
        immich_url: mock_server.uri(),
        immich_api_key: "test_key".to_string(),
        enable_nextcloud: false, // Nextcloud disabled!
        rate_limit_rps: 100.0,
        concurrency: 2,
        ..AppConfig::default()
    };

    let db = CheckpointDB::connect(&cfg.db_path).await.unwrap();
    let state = AppState::new(db, cfg.clone());
    let pipeline = SyncPipeline::from_config(state, cfg, "takeout").unwrap();

    let stats = pipeline.run_auto().await.unwrap();
    assert_eq!(stats.uploaded_immich_created, 1);
    assert!(
        zip_path.exists(),
        "Takeout ZIP must NOT be deleted when Nextcloud is disabled and archive contains non-media documents"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test pipeline_integration_test test_takeout_preserves_zip_when_nextcloud_disabled_and_archive_contains_documents`
Expected: FAIL because `zip_path.exists()` is `false` (the ZIP is currently deleted even though `중요문서.pdf` was skipped).

- [ ] **Step 3: Implement the state, pipeline, bulk verification, and dynamic scheduler changes**

1. In `src/state.rs`:
   - Add `pub scheduler_notify: Arc<tokio::sync::Notify>` to `AppState`.
   - In `AppState::new`, call `crate::sources::cleanup_temp_dir(std::path::Path::new(&initial_runtime.tmp_dir))` and `crate::sources::cleanup_temp_dir(&initial_runtime.disk_fallback_tmp_dir())`.
   - In `AppState::update_settings`, if `before.sync_interval_minutes != cfg.sync_interval_minutes`, call `self.scheduler_notify.notify_waiters()`.

2. In `src/pipeline.rs`:
   - In `SyncPipeline::from_config`, create `source_limiter = RateLimiter::new(config.rate_limit_rps, config.max_retries)` and `target_limiter = RateLimiter::new((config.rate_limit_rps * 10.0).max(50.0), config.max_retries)`.
   - In `run_sync`, call `self.state.db.get_pending_files_for_config(src, max_retries, batch_size, self.config.enable_nextcloud).await?`.
   - In `transfer_file`, choose the temp directory via:
     ```rust
     let fallback_dir = self.config.disk_fallback_tmp_dir();
     let chosen_tmp = crate::sources::resolve_temp_dir(
         Path::new(&self.config.tmp_dir),
         Some(&fallback_dir),
         if file.size > 0 { Some(file.size as u64) } else { None },
     );
     ```
   - In `cleanup_takeout_archives_after_sync`:
     - Before verifying/deleting, check:
       ```rust
       if !self.config.enable_nextcloud {
           let takeout_provider = GoogleTakeoutProvider::new(&self.config.takeout_dir);
           let skipped_docs = takeout_provider.count_unclassified_files(false).unwrap_or(0);
           if skipped_docs > 0 {
               self.state
                   .emit_event(
                       src,
                       JobState::Syncing,
                       None,
                       format!(
                           "[{}] ZIP 내에 Nextcloud 미사용으로 건너뛴 일반 문서 파일 {}건이 포함되어 있어 로컬 ZIP 아카이브를 보존합니다.",
                           src, skipped_docs
                       ),
                   )
                   .await;
               return Ok(());
           }
       }
       ```
     - Use `self.state.db.mark_files_deleted_batch(src, &deleted_ids).await?` instead of per-row `mark_file_deleted` loop.
   - Update `spawn_auto_sync_scheduler` to loop continuously using `tokio::select!` on `state.scheduler_notify.notified()` and `tokio::time::sleep(...)` using `state.effective_config().await.sync_interval_minutes`.

- [ ] **Step 4: Run test suite to verify it passes**

Run: `cargo test --test pipeline_integration_test`
Expected: PASS (all 5 integration tests pass).

- [ ] **Step 5: Commit**

```bash
git add src/state.rs src/pipeline.rs tests/pipeline_integration_test.rs
git commit -m "feat(pipeline): guard takeout ZIP cleanup, split LAN limiter, and make scheduler dynamic"
```

---

### Task 5: Atomic `.part` Upload for Takeout ZIP, Unix `0600` Settings Permissions, and UI `escapeHtml`

**Files:**
- Modify: `src/web.rs:480-535`
- Modify: `src/config.rs:274-302`
- Modify: `src/ui.html:1200-1210`
- Modify: `src/ui.html:1665-1735`
- Test: `tests/web_api_test.rs`

**Interfaces:**
- Consumes: `tokio::fs::rename` in `upload_takeout_zip`
- Produces:
  - Atomic `.part` -> `.zip` upload in `POST /api/takeout/upload`
  - `0o600` file permissions on `/data/settings.json` on Unix systems
  - `escapeHtml(raw)` applied to archive names and failure paths/messages in `src/ui.html`

- [ ] **Step 1: Write the failing test assertion in `tests/web_api_test.rs`**

In `tests/web_api_test.rs`, add an assertion in `test_web_dashboard_connection_and_job_lifecycle` (around line 121) checking that `escapeHtml` is present in `src/ui.html`, and test `POST /api/takeout/upload`:

```rust
    assert!(html.contains("escapeHtml"));
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test web_api_test`
Expected: FAIL at `assert!(html.contains("escapeHtml"))`.

- [ ] **Step 3: Implement `.part` upload atomicity, `0600` permissions, and `escapeHtml` in `src/ui.html`**

1. In `src/web.rs` (`upload_takeout_zip`), write to `part_path = Path::new(&cfg.takeout_dir).join(format!("{}.part", safe_name))`, flush and drop the file, remove existing `dest_path` if present, and `tokio::fs::rename(&part_path, &dest_path).await`. Clean up `part_path` on any error.
2. In `src/config.rs` (`save_persisted`), after `std::fs::write(&path, json_bytes)?;`, add:
```rust
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
```
3. In `src/ui.html`:
   - Add `escapeHtml`:
```javascript
    function escapeHtml(raw) {
      return String(raw ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }
```
   - Apply `escapeHtml(a.name)` in `renderSettingsCards` and `escapeHtml(f.source)`, `escapeHtml(f.target_type)`, `escapeHtml(f.parent_path)`, `escapeHtml(f.name)`, `escapeHtml(f.error_msg)` in `renderStatus`.

- [ ] **Step 4: Run full test suite to verify everything passes**

Run: `cargo test --lib --tests`
Expected: PASS across all unit and integration tests.

- [ ] **Step 5: Commit**

```bash
git add src/web.rs src/config.rs src/ui.html tests/web_api_test.rs
git commit -m "fix(web): use atomic .part file for ZIP uploads, 0600 settings perms, and escapeHtml in UI"
```
