use crate::config::AppConfig;
use crate::db::{FileRecord, FileUpsert, SyncStats};
use crate::limiter::RateLimiter;
use crate::router::{SourceType, SyncResultKind, TargetRouter, TargetType};
use crate::sources::gdrive::GoogleDriveProvider;
use crate::sources::mybox::MyboxProvider;
use crate::sources::takeout::{list_takeout_archives, GoogleTakeoutProvider};
use crate::sources::SourceProvider;
use crate::state::{AppState, JobState};
use crate::targets::immich::ImmichTarget;
use crate::targets::nextcloud::NextcloudTarget;
use crate::targets::{TargetProvider, UploadContext, UploadOutcome};
use anyhow::{anyhow, Result};
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

const SYNC_BATCH_SIZE: i64 = 100;
const DELETE_BATCH_LIMIT: i64 = 10_000;

pub fn build_source_provider(
    config: &AppConfig,
    source_type: SourceType,
    limiter: RateLimiter,
) -> Arc<dyn SourceProvider> {
    match source_type {
        SourceType::Mybox => Arc::new(MyboxProvider::new(
            config.mybox_api_base.clone(),
            config.mybox_pat.clone(),
            limiter,
        )),
        SourceType::Gdrive => Arc::new(GoogleDriveProvider::new(
            config.gdrive_api_base.clone(),
            config.gdrive_access_token.clone(),
            limiter,
        )),
        SourceType::Takeout => Arc::new(GoogleTakeoutProvider::new(&config.takeout_dir)),
    }
}

pub fn build_target_providers(
    config: &AppConfig,
    limiter: RateLimiter,
) -> HashMap<TargetType, Arc<dyn TargetProvider>> {
    let mut targets: HashMap<TargetType, Arc<dyn TargetProvider>> = HashMap::new();
    targets.insert(
        TargetType::Immich,
        Arc::new(ImmichTarget::new(
            config.immich_url.clone(),
            config.immich_api_key.clone(),
            limiter.clone(),
        )),
    );
    targets.insert(
        TargetType::Nextcloud,
        Arc::new(NextcloudTarget::new(
            config.nextcloud_url.clone(),
            config.nextcloud_user.clone(),
            config.nextcloud_app_password.clone(),
            config.nextcloud_base_dir.clone(),
            limiter,
        )),
    );
    targets
}

pub struct SyncPipeline {
    state: Arc<AppState>,
    config: AppConfig,
    source_type: SourceType,
    source: Arc<dyn SourceProvider>,
    targets: HashMap<TargetType, Arc<dyn TargetProvider>>,
    limit_files: Option<u64>,
}

impl SyncPipeline {
    pub fn from_config(state: Arc<AppState>, config: AppConfig, source_name: &str) -> Result<Self> {
        let source_type: SourceType = source_name.parse()?;
        let source_limiter = RateLimiter::new(config.rate_limit_rps, config.max_retries);
        let target_limiter =
            RateLimiter::new((config.rate_limit_rps * 10.0).max(50.0), config.max_retries);
        let source = build_source_provider(&config, source_type, source_limiter);
        let targets = build_target_providers(&config, target_limiter);

        Ok(Self {
            state,
            config,
            source_type,
            source,
            targets,
            limit_files: None,
        })
    }

    pub fn with_limit(mut self, limit: Option<u64>) -> Self {
        self.limit_files = limit.filter(|&n| n > 0);
        self
    }

    fn is_limit_reached(&self) -> bool {
        if let Some(limit) = self.limit_files {
            self.state.processed_files.load(Ordering::SeqCst) >= limit
        } else {
            false
        }
    }

    fn source_key(&self) -> &'static str {
        self.source_type.as_str()
    }

    pub async fn run_auto(&self) -> Result<SyncStats> {
        let src = self.source_key();
        self.run_scan().await?;
        if self.state.is_stopping() {
            let stats = self.state.db.get_stats(src).await?;
            self.state
                .finish_job(src, "스캔 단계에서 안전 일시정지되었습니다.")
                .await;
            return Ok(stats);
        }
        self.run_sync().await
    }

    pub async fn run_scan(&self) -> Result<SyncStats> {
        let src = self.source_key();
        self.state.begin_phase(src, JobState::Scanning, 0).await;
        let (is_resuming, scan_started_at) = self.state.db.prepare_scan(src).await?;

        self.state
            .emit_event(
                src,
                JobState::Scanning,
                None,
                if is_resuming {
                    format!(
                        "[{}] 이전 중단 지점부터 원격 폴더 스캔을 이어받습니다.",
                        src
                    )
                } else {
                    format!("[{}] 원격 폴더 트리 및 증분 변경분 스캔을 시작합니다.", src)
                },
            )
            .await;

        while !self.state.is_stopping() && !self.is_limit_reached() {
            let pending_folders = self.state.db.get_pending_folders(src).await?;
            if pending_folders.is_empty() {
                break;
            }

            for folder in pending_folders {
                if self.state.is_stopping() || self.is_limit_reached() {
                    break;
                }
                self.scan_single_folder(&folder.folder_id, &folder.path, folder.last_cursor)
                    .await?;
            }
        }

        if !is_resuming && !self.state.is_stopping() && self.limit_files.is_none() {
            let (removed_unbacked, excluded_uploaded) = self
                .state
                .db
                .reconcile_missing_remote_items(src, &scan_started_at)
                .await?;
            if removed_unbacked > 0 || excluded_uploaded > 0 {
                self.state
                    .emit_event(
                        src,
                        JobState::Scanning,
                        None,
                        format!(
                            "[{}] 원격에서 직접 삭제된 파일 정리 완료 (NAS 백업 보존 / 동기화 큐 제외: 미백업 {}건, 백업완료 {}건)",
                            src, removed_unbacked, excluded_uploaded
                        ),
                    )
                    .await;
            }
        }

        let stats = self.state.db.get_stats(src).await?;
        self.state
            .finish_job(
                src,
                format!(
                    "[{}] 스캔 완료 (대기 파일: {}건, 스캔된 폴더: {}개)",
                    src, stats.pending_files, stats.scanned_folders
                ),
            )
            .await;
        Ok(stats)
    }

    async fn scan_single_folder(
        &self,
        folder_id: &str,
        folder_path: &str,
        mut cursor: Option<String>,
    ) -> Result<()> {
        let src = self.source_key();

        while !self.state.is_stopping() && !self.is_limit_reached() {
            let page = self
                .source
                .list_page(folder_id, folder_path, cursor.as_deref())
                .await?;

            for sub in &page.folders {
                self.state
                    .db
                    .upsert_folder(src, &sub.folder_id, &sub.path)
                    .await?;
            }

            let mut upserts: Vec<FileUpsert> = page
                .files
                .into_iter()
                .filter_map(|rf| {
                    let target_type =
                        TargetRouter::classify(&rf.name, self.config.enable_nextcloud)?;
                    Some(FileUpsert {
                        source_type: src.to_string(),
                        resource_id: rf.resource_id,
                        name: rf.name,
                        parent_path: rf.parent_path,
                        size: rf.size,
                        created_time: rf.created_time,
                        modified_time: rf.modified_time,
                        target_type: target_type.as_str().to_string(),
                        sha1_hash: rf.sha1_hash,
                    })
                })
                .collect();

            let mut truncated_by_limit = false;
            if let Some(limit) = self.limit_files {
                let current = self.state.processed_files.load(Ordering::SeqCst);
                let remaining = limit.saturating_sub(current) as usize;
                if upserts.len() > remaining {
                    upserts.truncate(remaining);
                    truncated_by_limit = true;
                }
            }

            if !upserts.is_empty() {
                let added = self.state.db.upsert_files(&upserts).await?;
                self.state
                    .processed_files
                    .fetch_add(added as u64, Ordering::SeqCst);
                self.state
                    .emit_event(
                        src,
                        JobState::Scanning,
                        Some(folder_path.to_string()),
                        format!(
                            "폴더 스캔 중: {} (발견된 파일 {}건 체크포인트 등록)",
                            folder_path, added
                        ),
                    )
                    .await;
            }

            if truncated_by_limit || self.is_limit_reached() {
                break;
            }

            if let Some(next_c) = page.next_cursor {
                self.state
                    .db
                    .update_folder_cursor(src, folder_id, Some(&next_c))
                    .await?;
                cursor = Some(next_c);
            } else {
                self.state.db.mark_folder_scanned(src, folder_id).await?;
                break;
            }
        }

        Ok(())
    }

    pub async fn run_sync(&self) -> Result<SyncStats> {
        let src = self.source_key();
        let initial_stats = self.state.db.get_stats(src).await?;
        let planned_total = match self.limit_files {
            Some(limit) => (initial_stats.pending_files as u64).min(limit),
            None => initial_stats.pending_files as u64,
        };
        self.state
            .begin_phase(src, JobState::Syncing, planned_total)
            .await;
        self.state
            .emit_event(
                src,
                JobState::Syncing,
                None,
                format!(
                    "[{}] 동기화 시작 (대상 항목: {}건 / 전체 대기: {}건, 워커 수: {})",
                    src, planned_total, initial_stats.pending_files, self.config.concurrency
                ),
            )
            .await;

        let max_retries = self.config.max_retries as i64;
        let concurrency = self.config.concurrency.max(1);

        while !self.state.is_stopping() && !self.is_limit_reached() {
            let batch_size = match self.limit_files {
                Some(limit) => {
                    let done = self.state.processed_files.load(Ordering::SeqCst);
                    let rem = limit.saturating_sub(done);
                    if rem == 0 {
                        break;
                    }
                    rem.min(SYNC_BATCH_SIZE as u64) as i64
                }
                None => SYNC_BATCH_SIZE,
            };

            let batch = self
                .state
                .db
                .get_pending_files_for_config(
                    src,
                    max_retries,
                    batch_size,
                    self.config.enable_nextcloud,
                )
                .await?;
            if batch.is_empty() {
                break;
            }

            stream::iter(batch)
                .for_each_concurrent(concurrency, |file| async move {
                    if !self.state.is_stopping() && !self.is_limit_reached() {
                        self.sync_single_file(file).await;
                    }
                })
                .await;
        }

        if self.source_type == SourceType::Takeout {
            self.cleanup_takeout_archives_after_sync().await?;
        }

        let stats = self.state.db.get_stats(src).await?;
        self.state
            .finish_job(
                src,
                format!(
                    "[{}] 동기화 완료 (Immich 신규: {}, 중복 스킵: {}, Nextcloud: {}, 실패: {})",
                    src,
                    stats.uploaded_immich_created,
                    stats.uploaded_immich_duplicate,
                    stats.uploaded_nextcloud,
                    stats.failed_files
                ),
            )
            .await;
        Ok(stats)
    }

    async fn verify_uploaded_batch(&self, files: &[FileRecord]) -> Result<HashMap<String, bool>> {
        let mut by_target: HashMap<TargetType, Vec<FileRecord>> = HashMap::new();
        let mut results = HashMap::with_capacity(files.len());

        for f in files {
            match f.target_type.parse::<TargetType>() {
                Ok(t) => by_target.entry(t).or_default().push(f.clone()),
                Err(_) => {
                    results.insert(f.resource_id.clone(), false);
                }
            }
        }

        for (target_type, group) in by_target {
            if self.state.is_stopping() {
                break;
            }
            match self.targets.get(&target_type) {
                Some(target) => match target.verify_exists_batch(&group).await {
                    Ok(map) => results.extend(map),
                    Err(_) => {
                        for f in group {
                            results.insert(f.resource_id, false);
                        }
                    }
                },
                None => {
                    for f in group {
                        results.insert(f.resource_id, false);
                    }
                }
            }
        }

        Ok(results)
    }

    async fn cleanup_takeout_archives_after_sync(&self) -> Result<()> {
        let src = self.source_key();
        let archives = list_takeout_archives(&self.config.takeout_dir);
        if archives.is_empty() || self.state.is_stopping() {
            return Ok(());
        }

        if let Some(limit) = self.limit_files {
            self.state
                .emit_event(
                    src,
                    JobState::Syncing,
                    None,
                    format!(
                        "[{}] 테스트 건수 제한({}건) 실행이므로 로컬 ZIP 아카이브 자동 삭제를 건너뜁니다.",
                        src, limit
                    ),
                )
                .await;
            return Ok(());
        }

        if !self.config.enable_nextcloud {
            let takeout_provider = GoogleTakeoutProvider::new(&self.config.takeout_dir);
            let skipped_docs = takeout_provider
                .count_unclassified_files(false)
                .unwrap_or(0);
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

        let stats = self.state.db.get_stats(src).await?;
        if stats.scanned_folders == 0 || stats.pending_folders > 0 {
            self.state
                .emit_event(
                    src,
                    JobState::Syncing,
                    None,
                    format!(
                        "[{}] 전체 ZIP 스캔이 완료되지 않아 로컬 ZIP 아카이브를 보존합니다.",
                        src
                    ),
                )
                .await;
            return Ok(());
        }

        if stats.total_files == 0 || stats.pending_files > 0 || stats.failed_files > 0 {
            self.state
                .emit_event(
                    src,
                    JobState::Syncing,
                    None,
                    format!(
                        "[{}] 미완료 항목(전체 {}, 대기 {}, 실패 {})이 있어 로컬 ZIP 아카이브를 보존합니다.",
                        src, stats.total_files, stats.pending_files, stats.failed_files
                    ),
                )
                .await;
            return Ok(());
        }

        let fetch_limit = stats.total_files.max(DELETE_BATCH_LIMIT);
        let uploaded = self
            .state
            .db
            .get_uploaded_files_for_delete(src, fetch_limit)
            .await?;

        self.state
            .emit_event(
                src,
                JobState::Syncing,
                None,
                format!(
                    "[{}] 로컬 ZIP 자동 정리 전 타겟 존재 재검증 중 ({}건)...",
                    src,
                    uploaded.len()
                ),
            )
            .await;

        let verified_map = self.verify_uploaded_batch(&uploaded).await?;
        for file in &uploaded {
            if self.state.is_stopping() {
                return Ok(());
            }
            let verified = verified_map
                .get(&file.resource_id)
                .copied()
                .unwrap_or(false);
            if !verified {
                self.state
                    .emit_event(
                        src,
                        JobState::Syncing,
                        Some(file.name.clone()),
                        format!(
                            "[{}] 타겟 재검증 실패 항목이 있어 로컬 ZIP 삭제를 중단합니다: {} ({})",
                            src, file.name, file.target_type
                        ),
                    )
                    .await;
                return Ok(());
            }
        }

        let deleted_ids: Vec<String> = uploaded.iter().map(|f| f.resource_id.clone()).collect();
        self.state
            .db
            .mark_files_deleted_batch(src, &deleted_ids)
            .await?;

        let dir_path = Path::new(&self.config.takeout_dir);
        let mut removed_count = 0usize;
        let mut freed_bytes = 0u64;
        for arch in &archives {
            let zip_path = dir_path.join(&arch.name);
            match tokio::fs::remove_file(&zip_path).await {
                Ok(()) => {
                    removed_count += 1;
                    freed_bytes += arch.size_bytes;
                }
                Err(err) => {
                    self.state
                        .emit_event(
                            src,
                            JobState::Syncing,
                            Some(arch.name.clone()),
                            format!("[{}] 로컬 ZIP 삭제 실패 ({}): {}", src, arch.name, err),
                        )
                        .await;
                }
            }
        }

        if removed_count > 0 {
            let remaining = list_takeout_archives(&self.config.takeout_dir);
            if remaining.is_empty() {
                self.state
                    .record_connection_result(
                        "takeout",
                        false,
                        format!(
                            "동기화 및 검증 완료 후 ZIP {}개 자동 삭제됨 (새 ZIP 업로드 대기)",
                            removed_count
                        ),
                    )
                    .await;
            }
            self.state
                .emit_event(
                    src,
                    JobState::Syncing,
                    None,
                    format!(
                        "[{}] 타겟 재검증 통과 — 로컬 Takeout ZIP {}개 자동 삭제 완료 ({} bytes 회수)",
                        src, removed_count, freed_bytes
                    ),
                )
                .await;
        }

        Ok(())
    }

    async fn sync_single_file(&self, file: FileRecord) {
        let src = self.source_key();
        let max_retries = self.config.max_retries as i64;

        self.state
            .emit_event(
                src,
                JobState::Syncing,
                Some(file.name.clone()),
                format!(
                    "처리 중: {}/{}",
                    file.parent_path.trim_end_matches('/'),
                    file.name
                ),
            )
            .await;

        match self.transfer_file(&file).await {
            Ok((sha1, actual_size, outcome)) => {
                let result_str = outcome.sync_result.as_str();
                let _ = self
                    .state
                    .db
                    .mark_file_uploaded_with_size(
                        src,
                        &file.resource_id,
                        &sha1,
                        &outcome.remote_target_id,
                        result_str,
                        actual_size,
                    )
                    .await;
                self.state.processed_files.fetch_add(1, Ordering::SeqCst);
                self.state
                    .emit_event(
                        src,
                        JobState::Syncing,
                        Some(file.name.clone()),
                        format!(
                            "이주 완료 [{} / {}]: {}",
                            file.target_type, result_str, file.name
                        ),
                    )
                    .await;
            }
            Err(err) => {
                let err_str = err.to_string();
                let _ = self
                    .state
                    .db
                    .mark_file_failed(src, &file.resource_id, &err_str, max_retries)
                    .await;
                self.state
                    .emit_event(
                        src,
                        JobState::Syncing,
                        Some(file.name.clone()),
                        format!("오류 발생 ({}): {}", file.name, err_str),
                    )
                    .await;
            }
        }
    }

    async fn transfer_file(
        &self,
        file: &FileRecord,
    ) -> Result<(String, Option<i64>, UploadOutcome)> {
        let target_type: TargetType = file.target_type.parse()?;
        let target = self
            .targets
            .get(&target_type)
            .ok_or_else(|| anyhow!("등록되지 않은 타겟 플러그인: {}", target_type))?;

        // Zero-download fast path if source metadata already includes a SHA-1 checksum
        if let Some(ref existing_sha1) = file.sha1_hash {
            if let Ok(Some(existing_asset_id)) = target
                .precheck_duplicate(&file.resource_id, existing_sha1)
                .await
            {
                return Ok((
                    existing_sha1.clone(),
                    None,
                    UploadOutcome {
                        remote_target_id: existing_asset_id,
                        sync_result: SyncResultKind::Duplicate,
                    },
                ));
            }
        }

        let fallback_dir = self.config.disk_fallback_tmp_dir();
        let size_hint = if file.size > 0 {
            Some(file.size as u64)
        } else {
            None
        };
        let chosen_tmp = crate::sources::resolve_temp_dir(
            Path::new(&self.config.tmp_dir),
            Some(&fallback_dir),
            size_hint,
        );
        let downloaded = match self
            .source
            .download_to_temp(&file.resource_id, &file.name, &chosen_tmp)
            .await
        {
            Ok(d) => d,
            Err(_first_err) if chosen_tmp != fallback_dir => {
                self.source
                    .download_to_temp(&file.resource_id, &file.name, &fallback_dir)
                    .await?
            }
            Err(err) => return Err(err),
        };

        let created_at = downloaded
            .created_time_override
            .as_deref()
            .unwrap_or(&file.created_time);
        let modified_at = downloaded
            .modified_time_override
            .as_deref()
            .unwrap_or(&file.modified_time);

        let outcome = target
            .upload(UploadContext {
                source_type: self.source_type,
                file_path: downloaded.temp_guard.path(),
                resource_id: &file.resource_id,
                filename: &file.name,
                parent_path: &file.parent_path,
                sha1_hash: &downloaded.sha1_hash,
                created_at,
                modified_at,
            })
            .await?;

        Ok((
            downloaded.sha1_hash.clone(),
            Some(downloaded.size as i64),
            outcome,
        ))
    }

    pub async fn run_delete(&self, dry_run: bool) -> Result<SyncStats> {
        let src = self.source_key();
        let initial_stats = self.state.db.get_stats(src).await?;
        let fetch_limit = initial_stats.total_files.max(DELETE_BATCH_LIMIT);
        let uploaded = self
            .state
            .db
            .get_uploaded_files_for_delete(src, fetch_limit)
            .await?;
        self.state
            .begin_phase(src, JobState::Deleting, uploaded.len() as u64)
            .await;

        self.state
            .emit_event(
                src,
                JobState::Deleting,
                None,
                if dry_run {
                    format!(
                        "[{}] 원격 삭제 시뮬레이션(Dry-Run) 시작 (대상: {}건)",
                        src,
                        uploaded.len()
                    )
                } else {
                    format!(
                        "[{}] 재검증 후 원격 휴지통 삭제 시작 (대상: {}건)",
                        src,
                        uploaded.len()
                    )
                },
            )
            .await;

        let verified_map = self.verify_uploaded_batch(&uploaded).await?;

        for file in uploaded {
            if self.state.is_stopping() {
                break;
            }

            let verified = verified_map
                .get(&file.resource_id)
                .copied()
                .unwrap_or(false);

            if !verified {
                self.state
                    .emit_event(
                        src,
                        JobState::Deleting,
                        Some(file.name.clone()),
                        format!(
                            "⚠️ 삭제 보류 (타겟 서버 재검증 실패): {} ({})",
                            file.name, file.target_type
                        ),
                    )
                    .await;
                continue;
            }

            if dry_run {
                self.state.processed_files.fetch_add(1, Ordering::SeqCst);
                self.state
                    .emit_event(
                        src,
                        JobState::Deleting,
                        Some(file.name.clone()),
                        format!("[Dry-Run] 타겟 존재 검증 통과 (삭제 가능): {}", file.name),
                    )
                    .await;
                continue;
            }

            match self.source.delete_resource(&file.resource_id).await {
                Ok(()) => {
                    self.state
                        .db
                        .mark_file_deleted(src, &file.resource_id)
                        .await?;
                    self.state.processed_files.fetch_add(1, Ordering::SeqCst);
                    self.state
                        .emit_event(
                            src,
                            JobState::Deleting,
                            Some(file.name.clone()),
                            format!("🗑️ 원격 휴지통 이동 완료: {}", file.name),
                        )
                        .await;
                }
                Err(err) => {
                    self.state
                        .emit_event(
                            src,
                            JobState::Deleting,
                            Some(file.name.clone()),
                            format!("원격 삭제 실패 ({}): {}", file.name, err),
                        )
                        .await;
                }
            }
        }

        let stats = self.state.db.get_stats(src).await?;
        self.state
            .finish_job(
                src,
                if dry_run {
                    format!("[{}] 원격 삭제 Dry-Run 시뮬레이션 완료", src)
                } else {
                    format!(
                        "[{}] 원격 휴지통 이동 완료 (누적 삭제: {}건)",
                        src, stats.deleted_files
                    )
                },
            )
            .await;
        Ok(stats)
    }
}

pub fn spawn_auto_sync_scheduler(state: Arc<AppState>) {
    tokio::spawn(async move {
        loop {
            let interval_mins = state.effective_config().await.sync_interval_minutes;
            if interval_mins == 0 {
                state.scheduler_notify.notified().await;
                continue;
            }

            tokio::select! {
                _ = state.scheduler_notify.notified() => {
                    continue;
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(interval_mins * 60)) => {}
            }

            let current_cfg = state.effective_config().await;
            if current_cfg.sync_interval_minutes == 0 || !current_cfg.summary().immich_configured {
                continue;
            }
            let readiness = state.readiness_summary().await;
            let sources: Vec<String> = if !readiness.verified_sources.is_empty() {
                readiness.verified_sources
            } else {
                current_cfg
                    .configured_sources()
                    .into_iter()
                    .map(|s| s.as_str().to_string())
                    .collect()
            };
            if sources.is_empty() {
                continue;
            }

            for src_key in &sources {
                let src_key = src_key.as_str();
                {
                    let mut js = state.job_state.write().await;
                    if *js != JobState::Idle {
                        break;
                    }
                    *js = JobState::Scanning;
                }

                state
                    .emit_event(
                        src_key,
                        JobState::Scanning,
                        None,
                        format!(
                            "[자동 스케줄러] {}분 주기 증분 스캔 & 동기화를 시작합니다. (원격 삭제 제외)",
                            current_cfg.sync_interval_minutes
                        ),
                    )
                    .await;

                match SyncPipeline::from_config(state.clone(), current_cfg.clone(), src_key) {
                    Ok(pipeline) => {
                        if let Err(err) = pipeline.run_auto().await {
                            state
                                .finish_job(
                                    src_key,
                                    format!("[자동 스케줄러] 작업 중 오류 발생: {}", err),
                                )
                                .await;
                        }
                    }
                    Err(err) => {
                        state
                            .finish_job(src_key, format!("[자동 스케줄러] 초기화 실패: {}", err))
                            .await;
                    }
                }
            }
        }
    });
}
