use crate::config::AppConfig;
use crate::db::{FileRecord, FileUpsert, SyncStats};
use crate::limiter::RateLimiter;
use crate::router::{
    extract_timestamp_from_filename, inspect_file_path, pick_older_timestamp,
    replace_or_append_extension, DetectedFileKind, SourceType, SyncResultKind, TargetRouter,
    TargetType,
};
use crate::sources::gdrive::GoogleDriveProvider;
use crate::sources::mybox::MyboxProvider;
use crate::sources::takeout::{list_takeout_archives, GoogleTakeoutProvider};
use crate::sources::{try_extract_livephoto_zip, SourceProvider};
use crate::state::{AppState, JobState};
use crate::targets::immich::{
    decode_immich_checksum_to_hex, evaluate_duplicate_group, DuplicateGroupEvaluation, ImmichTarget,
};
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
    immich_target: Arc<ImmichTarget>,
    nextcloud_target: Arc<NextcloudTarget>,
    limit_files: Option<u64>,
}

impl SyncPipeline {
    pub fn from_config(state: Arc<AppState>, config: AppConfig, source_name: &str) -> Result<Self> {
        let source_type: SourceType = source_name.parse()?;
        let source_limiter = RateLimiter::new(config.rate_limit_rps, config.max_retries);
        let target_limiter =
            RateLimiter::new((config.rate_limit_rps * 10.0).max(50.0), config.max_retries);
        let source = build_source_provider(&config, source_type, source_limiter);
        let immich_target = Arc::new(ImmichTarget::new(
            config.immich_url.clone(),
            config.immich_api_key.clone(),
            target_limiter.clone(),
        ));
        let nextcloud_target = Arc::new(NextcloudTarget::new(
            config.nextcloud_url.clone(),
            config.nextcloud_user.clone(),
            config.nextcloud_app_password.clone(),
            config.nextcloud_base_dir.clone(),
            target_limiter,
        ));
        let mut targets: HashMap<TargetType, Arc<dyn TargetProvider>> = HashMap::new();
        targets.insert(TargetType::Immich, immich_target.clone());
        targets.insert(TargetType::Nextcloud, nextcloud_target.clone());

        Ok(Self {
            state,
            config,
            source_type,
            source,
            targets,
            immich_target,
            nextcloud_target,
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
            Ok((sha1, actual_size, outcome, reclassified)) => {
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
                let (final_name, final_target) =
                    if let Some((new_name, new_target)) = reclassified {
                        let _ = self
                            .state
                            .db
                            .update_file_reclassified(
                                src,
                                &file.resource_id,
                                &new_name,
                                new_target,
                                &outcome.remote_target_id,
                                &sha1,
                                actual_size.unwrap_or(file.size),
                                result_str,
                            )
                            .await;
                        (new_name, new_target.as_str().to_string())
                    } else {
                        (file.name.clone(), file.target_type.clone())
                    };
                self.state.processed_files.fetch_add(1, Ordering::SeqCst);
                self.state
                    .emit_event(
                        src,
                        JobState::Syncing,
                        Some(final_name.clone()),
                        format!("이주 완료 [{} / {}]: {}", final_target, result_str, final_name),
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
    ) -> Result<(
        String,
        Option<i64>,
        UploadOutcome,
        Option<(String, TargetType)>,
    )> {
        let target_type: TargetType = file.target_type.parse()?;
        let target = self
            .targets
            .get(&target_type)
            .ok_or_else(|| anyhow!("등록되지 않은 타겟 플러그인: {}", target_type))?;

        let is_livephoto_zip = TargetRouter::is_livephoto_archive_name(&file.name);
        let is_null_ext = TargetRouter::has_null_extension(&file.name);

        // Zero-download fast path if source metadata already includes a SHA-1 checksum
        if !is_livephoto_zip && !is_null_ext {
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
                        None,
                    ));
                }
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

        let base_created = downloaded
            .created_time_override
            .as_deref()
            .unwrap_or(&file.created_time);
        let filename_ts = extract_timestamp_from_filename(&file.name);
        let resolved_created = pick_older_timestamp(base_created, filename_ts.as_deref());
        let modified_at = downloaded
            .modified_time_override
            .as_deref()
            .unwrap_or(&file.modified_time);

        let inspected = if is_null_ext {
            inspect_file_path(downloaded.temp_guard.path())
                .ok()
                .flatten()
        } else {
            None
        };

        if is_livephoto_zip || matches!(inspected, Some(DetectedFileKind::ZipArchive)) {
            if let Ok(Some(extracted)) = try_extract_livephoto_zip(
                downloaded.temp_guard.path(),
                &file.name,
                &chosen_tmp,
            ) {
                let created_with_zip = pick_older_timestamp(
                    &resolved_created,
                    extracted.timestamp_override.as_deref(),
                );
                let img_ts = extract_timestamp_from_filename(&extracted.image_filename);
                let final_created = pick_older_timestamp(&created_with_zip, img_ts.as_deref());

                let outcome = self
                    .immich_target
                    .upload_live_photo(
                        &extracted,
                        &file.resource_id,
                        &final_created,
                        modified_at,
                    )
                    .await?;

                return Ok((
                    extracted.image_sha1,
                    Some(extracted.image_size as i64),
                    outcome,
                    Some((extracted.image_filename, TargetType::Immich)),
                ));
            } else if is_livephoto_zip {
                // Not a valid Live Photo pair inside the archive; fall back to Nextcloud if enabled
                if !self.config.enable_nextcloud {
                    return Err(anyhow!(
                        "라이브포토 아카이브 내 유효한 사진+영상 쌍이 없으며 Nextcloud가 비활성화 상태입니다: {}",
                        file.name
                    ));
                }
                let outcome = self
                    .nextcloud_target
                    .upload(UploadContext {
                        source_type: self.source_type,
                        file_path: downloaded.temp_guard.path(),
                        resource_id: &file.resource_id,
                        filename: &file.name,
                        parent_path: &file.parent_path,
                        sha1_hash: &downloaded.sha1_hash,
                        created_at: &resolved_created,
                        modified_at,
                    })
                    .await?;
                return Ok((
                    downloaded.sha1_hash.clone(),
                    Some(downloaded.size as i64),
                    outcome,
                    Some((file.name.clone(), TargetType::Nextcloud)),
                ));
            }
        }

        if is_null_ext {
            match inspected {
                Some(DetectedFileKind::Media { ext, .. }) => {
                    let restored_name = replace_or_append_extension(&file.name, ext);
                    let outcome = self
                        .immich_target
                        .upload(UploadContext {
                            source_type: self.source_type,
                            file_path: downloaded.temp_guard.path(),
                            resource_id: &file.resource_id,
                            filename: &restored_name,
                            parent_path: &file.parent_path,
                            sha1_hash: &downloaded.sha1_hash,
                            created_at: &resolved_created,
                            modified_at,
                        })
                        .await?;
                    return Ok((
                        downloaded.sha1_hash.clone(),
                        Some(downloaded.size as i64),
                        outcome,
                        Some((restored_name, TargetType::Immich)),
                    ));
                }
                Some(DetectedFileKind::Document { ext, .. }) => {
                    let restored_name = replace_or_append_extension(&file.name, ext);
                    let outcome = self
                        .nextcloud_target
                        .upload(UploadContext {
                            source_type: self.source_type,
                            file_path: downloaded.temp_guard.path(),
                            resource_id: &file.resource_id,
                            filename: &restored_name,
                            parent_path: &file.parent_path,
                            sha1_hash: &downloaded.sha1_hash,
                            created_at: &resolved_created,
                            modified_at,
                        })
                        .await?;
                    return Ok((
                        downloaded.sha1_hash.clone(),
                        Some(downloaded.size as i64),
                        outcome,
                        Some((restored_name, TargetType::Nextcloud)),
                    ));
                }
                Some(DetectedFileKind::ZipArchive) => {
                    let restored_name = replace_or_append_extension(&file.name, "zip");
                    let outcome = self
                        .nextcloud_target
                        .upload(UploadContext {
                            source_type: self.source_type,
                            file_path: downloaded.temp_guard.path(),
                            resource_id: &file.resource_id,
                            filename: &restored_name,
                            parent_path: &file.parent_path,
                            sha1_hash: &downloaded.sha1_hash,
                            created_at: &resolved_created,
                            modified_at,
                        })
                        .await?;
                    return Ok((
                        downloaded.sha1_hash.clone(),
                        Some(downloaded.size as i64),
                        outcome,
                        Some((restored_name, TargetType::Nextcloud)),
                    ));
                }
                None => {}
            }
        }

        let outcome = target
            .upload(UploadContext {
                source_type: self.source_type,
                file_path: downloaded.temp_guard.path(),
                resource_id: &file.resource_id,
                filename: &file.name,
                parent_path: &file.parent_path,
                sha1_hash: &downloaded.sha1_hash,
                created_at: &resolved_created,
                modified_at,
            })
            .await?;

        Ok((
            downloaded.sha1_hash.clone(),
            Some(downloaded.size as i64),
            outcome,
            None,
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

pub async fn run_repair_nextcloud_anomalies(
    state: Arc<AppState>,
    config: AppConfig,
) -> Result<u64> {
    run_repair_nextcloud_anomalies_with_limit(state, config, None).await
}

pub async fn run_repair_nextcloud_anomalies_with_limit(
    state: Arc<AppState>,
    config: AppConfig,
    limit: Option<u64>,
) -> Result<u64> {
    let mut candidates = state.db.get_nextcloud_repair_candidates().await?;
    if let Some(n) = limit.filter(|&n| n > 0) {
        candidates.truncate(n as usize);
    }
    if candidates.is_empty() {
        state
            .finish_job(
                "repair",
                "[사후 보정] Nextcloud 내 복구 대상(livephoto.zip / (null)) 항목이 없습니다.",
            )
            .await;
        return Ok(0);
    }

    state
        .begin_phase("repair", JobState::Syncing, candidates.len() as u64)
        .await;
    state
        .emit_event(
            "repair",
            JobState::Syncing,
            None,
            format!(
                "[사후 보정] Nextcloud 내 livephoto.zip 및 (null) 파일 {}건 복구 및 재분류를 시작합니다.",
                candidates.len()
            ),
        )
        .await;

    let target_limiter =
        RateLimiter::new((config.rate_limit_rps * 10.0).max(50.0), config.max_retries);
    let immich_target = ImmichTarget::new(
        config.immich_url.clone(),
        config.immich_api_key.clone(),
        target_limiter.clone(),
    );
    let nextcloud_target = NextcloudTarget::new(
        config.nextcloud_url.clone(),
        config.nextcloud_user.clone(),
        config.nextcloud_app_password.clone(),
        config.nextcloud_base_dir.clone(),
        target_limiter,
    );
    let fallback_dir = config.disk_fallback_tmp_dir();
    let mut repaired_count = 0u64;

    for file in candidates {
        if state.is_stopping() {
            break;
        }
        let source_type: SourceType = file.source_type.parse().unwrap_or(SourceType::Mybox);
        let size_hint = (file.size > 0).then_some(file.size as u64);
        let chosen_tmp = crate::sources::resolve_temp_dir(
            Path::new(&config.tmp_dir),
            Some(&fallback_dir),
            size_hint,
        );

        let old_remote_path = file.remote_target_id.clone().unwrap_or_else(|| {
            format!(
                "{}/{}/{}/{}",
                config.nextcloud_base_dir.trim_end_matches('/'),
                source_type.nextcloud_folder_name(),
                file.parent_path.trim_matches('/'),
                file.name
            )
        });

        let downloaded = match nextcloud_target
            .download_file_to_temp(&old_remote_path, &file.name, &chosen_tmp)
            .await
        {
            Ok(d) => d,
            Err(_nc_err) => {
                let source_limiter = RateLimiter::new(config.rate_limit_rps, config.max_retries);
                let source_provider = build_source_provider(&config, source_type, source_limiter);
                match source_provider
                    .download_to_temp(&file.resource_id, &file.name, &chosen_tmp)
                    .await
                {
                    Ok(d) => d,
                    Err(err) => {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(file.name.clone()),
                                format!("[사후 보정] 원본 다운로드 실패 ({}): {}", file.name, err),
                            )
                            .await;
                        continue;
                    }
                }
            }
        };

        let is_livephoto_zip = TargetRouter::is_livephoto_archive_name(&file.name);
        let is_null_ext = TargetRouter::has_null_extension(&file.name);
        let inspected = if is_null_ext {
            inspect_file_path(downloaded.temp_guard.path())
                .ok()
                .flatten()
        } else {
            None
        };

        if is_livephoto_zip || matches!(inspected, Some(DetectedFileKind::ZipArchive)) {
            if let Ok(Some(extracted)) = try_extract_livephoto_zip(
                downloaded.temp_guard.path(),
                &file.name,
                &chosen_tmp,
            ) {
                let base_created = pick_older_timestamp(
                    &file.created_time,
                    extracted.timestamp_override.as_deref(),
                );
                let img_ts = extract_timestamp_from_filename(&extracted.image_filename);
                let created_at = pick_older_timestamp(&base_created, img_ts.as_deref());

                match immich_target
                    .upload_live_photo(
                        &extracted,
                        &file.resource_id,
                        &created_at,
                        &file.modified_time,
                    )
                    .await
                {
                    Ok(outcome) => {
                        let verified = !outcome.remote_target_id.is_empty();
                        if verified {
                            let _ = nextcloud_target.delete_file(&old_remote_path).await;
                        }
                        let vid_label = extracted
                            .video_filename
                            .as_deref()
                            .unwrap_or("livephoto.mov");
                        state
                            .db
                            .update_file_reclassified(
                                &file.source_type,
                                &file.resource_id,
                                &extracted.image_filename,
                                TargetType::Immich,
                                &outcome.remote_target_id,
                                &extracted.image_sha1,
                                extracted.image_size as i64,
                                outcome.sync_result.as_str(),
                            )
                            .await?;
                        repaired_count += 1;
                        state.processed_files.fetch_add(1, Ordering::SeqCst);
                        let date_label = created_at.get(..10).unwrap_or(&created_at);
                        let immich_link = format!(
                            "{}/photos/{}",
                            config.immich_url.trim_end_matches('/'),
                            outcome.remote_target_id
                        );
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(extracted.image_filename.clone()),
                                format!(
                                    "[사후 보정] 라이브포토 복구 완료 [{} / 촬영일: {} / 확인: {}]: {} → {} + {}",
                                    outcome.sync_result.as_str(),
                                    date_label,
                                    immich_link,
                                    file.name,
                                    extracted.image_filename,
                                    vid_label
                                ),
                            )
                            .await;
                        continue;
                    }
                    Err(err) => {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(file.name.clone()),
                                format!("[사후 보정] 라이브포토 업로드 실패 ({}): {}", file.name, err),
                            )
                            .await;
                        continue;
                    }
                }
            }
        }

        if is_null_ext {
            match inspected {
                Some(DetectedFileKind::Media { ext, .. }) => {
                    let restored_name = replace_or_append_extension(&file.name, ext);
                    let fn_ts = extract_timestamp_from_filename(&restored_name);
                    let created_at = pick_older_timestamp(&file.created_time, fn_ts.as_deref());
                    match immich_target
                        .upload(UploadContext {
                            source_type,
                            file_path: downloaded.temp_guard.path(),
                            resource_id: &file.resource_id,
                            filename: &restored_name,
                            parent_path: &file.parent_path,
                            sha1_hash: &downloaded.sha1_hash,
                            created_at: &created_at,
                            modified_at: &file.modified_time,
                        })
                        .await
                    {
                        Ok(outcome) => {
                            if !outcome.remote_target_id.is_empty() {
                                let _ = nextcloud_target.delete_file(&old_remote_path).await;
                            }
                            state
                                .db
                                .update_file_reclassified(
                                    &file.source_type,
                                    &file.resource_id,
                                    &restored_name,
                                    TargetType::Immich,
                                    &outcome.remote_target_id,
                                    &downloaded.sha1_hash,
                                    downloaded.size as i64,
                                    outcome.sync_result.as_str(),
                                )
                                .await?;
                            repaired_count += 1;
                            state.processed_files.fetch_add(1, Ordering::SeqCst);
                            let date_label = created_at.get(..10).unwrap_or(&created_at);
                            let immich_link = format!(
                                "{}/photos/{}",
                                config.immich_url.trim_end_matches('/'),
                                outcome.remote_target_id
                            );
                            let status_note = if outcome.sync_result == SyncResultKind::Duplicate {
                                "DUPLICATE(동일 해시 사진 이미 존재)"
                            } else {
                                "CREATED(신규 업로드)"
                            };
                            state
                                .emit_event(
                                    "repair",
                                    JobState::Syncing,
                                    Some(restored_name.clone()),
                                    format!(
                                        "[사후 보정] (null) 미디어 복구 완료 [{} / 촬영일: {} / 확인: {}]: {} → {}",
                                        status_note,
                                        date_label,
                                        immich_link,
                                        file.name,
                                        restored_name
                                    ),
                                )
                                .await;
                        }
                        Err(err) => {
                            state
                                .emit_event(
                                    "repair",
                                    JobState::Syncing,
                                    Some(file.name.clone()),
                                    format!("[사후 보정] 미디어 업로드 실패 ({}): {}", file.name, err),
                                )
                                .await;
                        }
                    }
                }
                Some(DetectedFileKind::Document { ext, .. }) => {
                    let restored_name = replace_or_append_extension(&file.name, ext);
                    match nextcloud_target
                        .upload(UploadContext {
                            source_type,
                            file_path: downloaded.temp_guard.path(),
                            resource_id: &file.resource_id,
                            filename: &restored_name,
                            parent_path: &file.parent_path,
                            sha1_hash: &downloaded.sha1_hash,
                            created_at: &file.created_time,
                            modified_at: &file.modified_time,
                        })
                        .await
                    {
                        Ok(outcome) => {
                            if outcome.remote_target_id != old_remote_path {
                                let _ = nextcloud_target.delete_file(&old_remote_path).await;
                            }
                            state
                                .db
                                .update_file_reclassified(
                                    &file.source_type,
                                    &file.resource_id,
                                    &restored_name,
                                    TargetType::Nextcloud,
                                    &outcome.remote_target_id,
                                    &downloaded.sha1_hash,
                                    downloaded.size as i64,
                                    outcome.sync_result.as_str(),
                                )
                                .await?;
                            repaired_count += 1;
                            state.processed_files.fetch_add(1, Ordering::SeqCst);
                            state
                                .emit_event(
                                    "repair",
                                    JobState::Syncing,
                                    Some(restored_name.clone()),
                                    format!(
                                        "[사후 보정] (null) 문서 확장자 복원 완료 (Nextcloud): {} → {}",
                                        file.name, restored_name
                                    ),
                                )
                                .await;
                        }
                        Err(err) => {
                            state
                                .emit_event(
                                    "repair",
                                    JobState::Syncing,
                                    Some(file.name.clone()),
                                    format!("[사후 보정] 문서 재업로드 실패 ({}): {}", file.name, err),
                                )
                                .await;
                        }
                    }
                }
                Some(DetectedFileKind::ZipArchive) => {
                    let restored_name = replace_or_append_extension(&file.name, "zip");
                    match nextcloud_target
                        .upload(UploadContext {
                            source_type,
                            file_path: downloaded.temp_guard.path(),
                            resource_id: &file.resource_id,
                            filename: &restored_name,
                            parent_path: &file.parent_path,
                            sha1_hash: &downloaded.sha1_hash,
                            created_at: &file.created_time,
                            modified_at: &file.modified_time,
                        })
                        .await
                    {
                        Ok(outcome) => {
                            if outcome.remote_target_id != old_remote_path {
                                let _ = nextcloud_target.delete_file(&old_remote_path).await;
                            }
                            state
                                .db
                                .update_file_reclassified(
                                    &file.source_type,
                                    &file.resource_id,
                                    &restored_name,
                                    TargetType::Nextcloud,
                                    &outcome.remote_target_id,
                                    &downloaded.sha1_hash,
                                    downloaded.size as i64,
                                    outcome.sync_result.as_str(),
                                )
                                .await?;
                            repaired_count += 1;
                            state.processed_files.fetch_add(1, Ordering::SeqCst);
                            state
                                .emit_event(
                                    "repair",
                                    JobState::Syncing,
                                    Some(restored_name.clone()),
                                    format!(
                                        "[사후 보정] (null) 압축파일 확장자 복원 완료 (Nextcloud): {} → {}",
                                        file.name, restored_name
                                    ),
                                )
                                .await;
                        }
                        Err(err) => {
                            state
                                .emit_event(
                                    "repair",
                                    JobState::Syncing,
                                    Some(file.name.clone()),
                                    format!("[사후 보정] 압축파일 재업로드 실패 ({}): {}", file.name, err),
                                )
                                .await;
                        }
                    }
                }
                None => {
                    state
                        .emit_event(
                            "repair",
                            JobState::Syncing,
                            Some(file.name.clone()),
                            format!(
                                "[사후 보정] Magic Bytes 미식별 — Nextcloud에 원본 유지: {}",
                                file.name
                            ),
                        )
                        .await;
                }
            }
        }
    }

    state
        .finish_job(
            "repair",
            format!(
                "[사후 보정] Nextcloud 내 livephoto.zip 및 (null) 복구 완료 ({}건 처리됨)",
                repaired_count
            ),
        )
        .await;
    Ok(repaired_count)
}

fn is_valid_immich_asset_id(raw: Option<&str>) -> Option<String> {
    let trimmed = raw?.trim();
    if trimmed.is_empty()
        || trimmed == "existing_immich_asset"
        || trimmed == "immich_uploaded_id"
    {
        return None;
    }
    Some(trimmed.to_string())
}

pub async fn run_stage_gdrive_to_immich_album(
    state: Arc<AppState>,
    config: AppConfig,
    album_name: &str,
) -> Result<u64> {
    run_stage_gdrive_to_immich_album_with_limit(state, config, album_name, None).await
}

pub async fn run_stage_gdrive_to_immich_album_with_limit(
    state: Arc<AppState>,
    config: AppConfig,
    album_name: &str,
    limit: Option<u64>,
) -> Result<u64> {
    if config.immich_url.trim().is_empty() || config.immich_api_key.trim().is_empty() {
        return Err(anyhow!(
            "Immich 서버 URL 또는 API Key가 설정되지 않았습니다."
        ));
    }

    let mut candidates = state.db.get_gdrive_immich_staging_candidates().await?;
    if let Some(n) = limit.filter(|&n| n > 0) {
        candidates.truncate(n as usize);
    }
    if candidates.is_empty() {
        state
            .finish_job(
                "repair",
                "[Google Drive 앨범 수집] DB에서 Google Drive 출신으로 식별된 Immich 항목이 0건입니다.",
            )
            .await;
        return Ok(0);
    }

    state
        .begin_phase("repair", JobState::Syncing, candidates.len() as u64)
        .await;
    state
        .emit_event(
            "repair",
            JobState::Syncing,
            None,
            format!(
                "[Google Drive 앨범 수집] Google Drive 출신 Immich 항목 {}건을 '{}' 앨범으로 모으는 중...",
                candidates.len(),
                album_name
            ),
        )
        .await;

    let target_limiter =
        RateLimiter::new((config.rate_limit_rps * 10.0).max(50.0), config.max_retries);
    let immich_target = ImmichTarget::new(
        config.immich_url.clone(),
        config.immich_api_key.clone(),
        target_limiter,
    );

    let album_id = immich_target.ensure_album_by_name(album_name).await?;

    // Resolve placeholder/missing remote_target_id via bulk hash check when sha1_hash is available
    let mut unresolved_hashes: Vec<(String, String)> = Vec::new();
    for file in &candidates {
        if is_valid_immich_asset_id(file.remote_target_id.as_deref()).is_none() {
            if let Some(ref sha1) = file.sha1_hash {
                if !sha1.trim().is_empty() {
                    unresolved_hashes.push((file.resource_id.clone(), sha1.trim().to_string()));
                }
            }
        }
    }

    let mut resolved_by_resource: HashMap<String, String> = HashMap::new();
    for chunk in unresolved_hashes.chunks(500) {
        if state.is_stopping() {
            break;
        }
        match immich_target.check_bulk_hashes(chunk).await {
            Ok(map) => {
                for (res_id, opt_asset_id) in map {
                    if let Some(valid_id) = is_valid_immich_asset_id(opt_asset_id.as_deref()) {
                        resolved_by_resource.insert(res_id, valid_id);
                    }
                }
            }
            Err(err) => {
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        None,
                        format!(
                            "[Google Drive 앨범 수집] Immich 해시 일괄 조회 실패: {}",
                            err
                        ),
                    )
                    .await;
            }
        }
    }

    if !resolved_by_resource.is_empty() {
        let healed_triples: Vec<(String, String, String)> = candidates
            .iter()
            .filter_map(|file| {
                resolved_by_resource
                    .get(&file.resource_id)
                    .map(|asset_id| {
                        (
                            file.source_type.clone(),
                            file.resource_id.clone(),
                            asset_id.clone(),
                        )
                    })
            })
            .collect();
        let _ = state.db.heal_immich_remote_target_ids(&healed_triples).await;
    }

    let mut unique_asset_ids: Vec<String> = Vec::new();
    let mut seen_asset_ids = std::collections::HashSet::new();
    let mut staged_records = 0u64;

    for file in &candidates {
        if state.is_stopping() {
            break;
        }
        let resolved_id = is_valid_immich_asset_id(file.remote_target_id.as_deref())
            .or_else(|| resolved_by_resource.get(&file.resource_id).cloned());

        match resolved_id {
            Some(asset_id) => {
                staged_records += 1;
                if seen_asset_ids.insert(asset_id.clone()) {
                    unique_asset_ids.push(asset_id);
                }
            }
            None => {
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(file.name.clone()),
                        format!(
                            "[Google Drive 앨범 수집] Immich 자산 ID를 확인할 수 없어 건너뜀: {}/{}",
                            file.parent_path.trim_end_matches('/'),
                            file.name
                        ),
                    )
                    .await;
            }
        }
        state.processed_files.fetch_add(1, Ordering::SeqCst);
    }

    for chunk in unique_asset_ids.chunks(500) {
        if state.is_stopping() {
            break;
        }
        immich_target.add_assets_to_album(&album_id, chunk).await?;
    }

    let album_url = format!(
        "{}/albums/{}",
        config.immich_url.trim_end_matches('/'),
        album_id
    );
    state
        .finish_job(
            "repair",
            format!(
                "[Google Drive 앨범 수집] '{}' 앨범에 Google Drive 출신 항목 {}건(고유 자산 {}개) 담기 완료 — 앨범에서 남길 사진을 뺀 뒤 2단계 이동을 실행하세요 (확인: {})",
                album_name,
                staged_records,
                unique_asset_ids.len(),
                album_url
            ),
        )
        .await;

    Ok(staged_records)
}

pub async fn run_move_immich_album_to_nextcloud(
    state: Arc<AppState>,
    config: AppConfig,
    album_name: &str,
) -> Result<u64> {
    run_move_immich_album_to_nextcloud_with_limit(state, config, album_name, None).await
}

pub async fn run_move_immich_album_to_nextcloud_with_limit(
    state: Arc<AppState>,
    config: AppConfig,
    album_name: &str,
    limit: Option<u64>,
) -> Result<u64> {
    if !config.enable_nextcloud {
        return Err(anyhow!(
            "Nextcloud 연동이 활성화되어 있지 않습니다. 설정에서 Nextcloud를 먼저 활성화해 주세요."
        ));
    }

    let target_limiter =
        RateLimiter::new((config.rate_limit_rps * 10.0).max(50.0), config.max_retries);
    let immich_target = ImmichTarget::new(
        config.immich_url.clone(),
        config.immich_api_key.clone(),
        target_limiter.clone(),
    );
    let nextcloud_target = NextcloudTarget::new(
        config.nextcloud_url.clone(),
        config.nextcloud_user.clone(),
        config.nextcloud_app_password.clone(),
        config.nextcloud_base_dir.clone(),
        target_limiter,
    );

    let Some((album_id, mut assets)) = immich_target.get_album_assets_by_name(album_name).await?
    else {
        state
            .finish_job(
                "repair",
                format!(
                    "[앨범 핀포인트 이동] Immich에 '{}' 앨범이 존재하지 않습니다.",
                    album_name
                ),
            )
            .await;
        return Ok(0);
    };
    if let Some(n) = limit.filter(|&n| n > 0) {
        assets.truncate(n as usize);
    }
    if assets.is_empty() {
        state
            .finish_job(
                "repair",
                format!(
                    "[앨범 핀포인트 이동] Immich '{}' 앨범(ID: {})에 조회된 사진이 0건입니다. (앨범에 사진이 있다면 Immich API 키에 asset.read / album.read 권한이 있는지 확인해 주세요)",
                    album_name, album_id
                ),
            )
            .await;
        return Ok(0);
    }

    state
        .begin_phase("repair", JobState::Syncing, assets.len() as u64)
        .await;
    state
        .emit_event(
            "repair",
            JobState::Syncing,
            None,
            format!(
                "[앨범 핀포인트 이동] Immich '{}' 앨범 내 {}건 항목을 Nextcloud 원래 폴더로 이관합니다.",
                album_name,
                assets.len()
            ),
        )
        .await;

    let fallback_dir = config.disk_fallback_tmp_dir();
    let mut moved_count = 0u64;

    for asset in assets {
        if state.is_stopping() {
            break;
        }

        let db_record = state
            .db
            .find_file_for_immich_asset(
                &asset.id,
                asset.device_asset_id.as_deref(),
                &asset.original_file_name,
            )
            .await?;

        let (source_type, resource_id, parent_path, filename, created_at, modified_at) =
            if let Some(ref rec) = db_record {
                (
                    rec.source_type.parse().unwrap_or(SourceType::Mybox),
                    rec.resource_id.clone(),
                    rec.parent_path.clone(),
                    rec.name.clone(),
                    rec.created_time.clone(),
                    rec.modified_time.clone(),
                )
            } else {
                let fallback_name = if asset.original_file_name.trim().is_empty() {
                    format!("{}.bin", asset.id)
                } else {
                    asset.original_file_name.clone()
                };
                let now = chrono::Utc::now().to_rfc3339();
                (
                    SourceType::Mybox,
                    asset
                        .device_asset_id
                        .clone()
                        .unwrap_or_else(|| asset.id.clone()),
                    "/Immich_Moved".to_string(),
                    fallback_name,
                    now.clone(),
                    now,
                )
            };

        let chosen_tmp = crate::sources::resolve_temp_dir(
            Path::new(&config.tmp_dir),
            Some(&fallback_dir),
            None,
        );

        let downloaded = match immich_target
            .download_asset_to_temp(&asset.id, &filename, &chosen_tmp)
            .await
        {
            Ok(d) => d,
            Err(err) => {
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(filename.clone()),
                        format!(
                            "[앨범 핀포인트 이동] Immich 원본 다운로드 실패 ({}): {}",
                            filename, err
                        ),
                    )
                    .await;
                continue;
            }
        };

        let outcome = match nextcloud_target
            .upload(UploadContext {
                source_type,
                file_path: downloaded.temp_guard.path(),
                resource_id: &resource_id,
                filename: &filename,
                parent_path: &parent_path,
                sha1_hash: &downloaded.sha1_hash,
                created_at: &created_at,
                modified_at: &modified_at,
            })
            .await
        {
            Ok(o) => o,
            Err(err) => {
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(filename.clone()),
                        format!(
                            "[앨범 핀포인트 이동] Nextcloud 업로드 실패 ({}): {}",
                            filename, err
                        ),
                    )
                    .await;
                continue;
            }
        };

        let verified = nextcloud_target
            .check_file_exists(&outcome.remote_target_id, downloaded.size as i64)
            .await
            .unwrap_or(false);
        if !verified {
            state
                .emit_event(
                    "repair",
                    JobState::Syncing,
                    Some(filename.clone()),
                    format!(
                        "[앨범 핀포인트 이동] Nextcloud 존재 검증 실패로 Immich 삭제 보류: {}",
                        filename
                    ),
                )
                .await;
            continue;
        }

        let mut ids_to_delete = vec![asset.id.clone()];
        if let Some(ref vid) = asset.live_photo_video_id {
            if !vid.trim().is_empty() {
                ids_to_delete.push(vid.clone());
            }
        }
        if let Err(err) = immich_target.delete_assets(&ids_to_delete).await {
            state
                .emit_event(
                    "repair",
                    JobState::Syncing,
                    Some(filename.clone()),
                    format!(
                        "[앨범 핀포인트 이동] Nextcloud 업로드 완료했으나 Immich 자산 삭제 실패 ({}): {}",
                        filename, err
                    ),
                )
                .await;
        }

        if let Some(rec) = db_record {
            state
                .db
                .update_file_reclassified(
                    &rec.source_type,
                    &rec.resource_id,
                    &filename,
                    TargetType::Nextcloud,
                    &outcome.remote_target_id,
                    &downloaded.sha1_hash,
                    downloaded.size as i64,
                    outcome.sync_result.as_str(),
                )
                .await?;
        }
        state
            .db
            .reclassify_sibling_immich_records(
                &asset.id,
                &downloaded.sha1_hash,
                TargetType::Nextcloud,
                &outcome.remote_target_id,
            )
            .await?;

        moved_count += 1;
        state.processed_files.fetch_add(1, Ordering::SeqCst);
        state
            .emit_event(
                "repair",
                JobState::Syncing,
                Some(filename.clone()),
                format!(
                    "[앨범 핀포인트 이동] Immich → Nextcloud 이관 완료: {}/{}",
                    parent_path.trim_end_matches('/'),
                    filename
                ),
            )
            .await;
    }

    state
        .finish_job(
            "repair",
            format!(
                "[앨범 핀포인트 이동] Immich '{}' 앨범 → Nextcloud 이관 완료 ({}건 처리됨)",
                album_name, moved_count
            ),
        )
        .await;
    Ok(moved_count)
}

pub async fn run_merge_immich_duplicates(
    state: Arc<AppState>,
    config: AppConfig,
    dry_run: bool,
) -> Result<u64> {
    run_merge_immich_duplicates_with_limit(state, config, dry_run, None).await
}

pub async fn run_merge_immich_duplicates_with_limit(
    state: Arc<AppState>,
    config: AppConfig,
    dry_run: bool,
    limit: Option<u64>,
) -> Result<u64> {
    if config.immich_url.trim().is_empty() || config.immich_api_key.trim().is_empty() {
        return Err(anyhow!(
            "Immich 서버 URL 또는 API Key가 설정되지 않았습니다."
        ));
    }

    let target_limiter =
        RateLimiter::new((config.rate_limit_rps * 10.0).max(50.0), config.max_retries);
    let immich_target = ImmichTarget::new(
        config.immich_url.clone(),
        config.immich_api_key.clone(),
        target_limiter,
    );

    let mut groups = immich_target.get_duplicates().await?;
    if groups.is_empty() {
        state
            .finish_job(
                "repair",
                "[중복 스마트 병합] Immich에서 감지된 중복 사진 그룹이 없습니다.",
            )
            .await;
        return Ok(0);
    }

    if let Some(n) = limit.filter(|&n| n > 0) {
        groups.truncate(n as usize);
    }

    let mode_tag = if dry_run {
        "중복 스마트 병합 Dry-Run"
    } else {
        "중복 스마트 병합"
    };

    state
        .begin_phase("repair", JobState::Syncing, groups.len() as u64)
        .await;
    state
        .emit_event(
            "repair",
            JobState::Syncing,
            None,
            format!(
                "[{}] Immich 중복 그룹 {}건 분석 및 선택적 메타데이터 병합을 시작합니다.",
                mode_tag,
                groups.len()
            ),
        )
        .await;

    let all_albums = immich_target.list_all_albums().await?;
    let has_non_empty_albums = !all_albums.is_empty();

    let mut merged_groups = 0u64;
    let mut skipped_burst_groups = 0u64;
    let mut trashed_assets = 0u64;

    for group in groups {
        if state.is_stopping() {
            break;
        }

        let eval = evaluate_duplicate_group(&group);
        let plan = match eval {
            DuplicateGroupEvaluation::SkippedInsufficientAssets => {
                state.processed_files.fetch_add(1, Ordering::SeqCst);
                continue;
            }
            DuplicateGroupEvaluation::SkippedBurst { reason, .. } => {
                skipped_burst_groups += 1;
                state.processed_files.fetch_add(1, Ordering::SeqCst);
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        None,
                        format!("[{}] 자동 보호 (Skip): {}", mode_tag, reason),
                    )
                    .await;
                continue;
            }
            DuplicateGroupEvaluation::Merge(p) => p,
        };

        let mut albums_to_join: Vec<(String, String)> = Vec::new();
        if has_non_empty_albums {
            let winner_albums = match immich_target.get_albums_for_asset(&plan.winner.id).await {
                Ok(list) => list,
                Err(err) => {
                    state
                        .emit_event(
                            "repair",
                            JobState::Syncing,
                            Some(plan.winner.original_file_name.clone()),
                            format!(
                                "[{}] Winner 소속 앨범 조회 실패로 삭제 보류 ({}): {}",
                                mode_tag, plan.winner.original_file_name, err
                            ),
                        )
                        .await;
                    state.processed_files.fetch_add(1, Ordering::SeqCst);
                    continue;
                }
            };

            let mut seen_album_ids: std::collections::HashSet<String> = winner_albums
                .into_iter()
                .map(|a| a.id.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            let mut album_query_failed = false;
            for loser in &plan.losers {
                match immich_target.get_albums_for_asset(&loser.id).await {
                    Ok(loser_albums) => {
                        for album in loser_albums {
                            let aid = album.id.trim().to_string();
                            if !aid.is_empty() && seen_album_ids.insert(aid.clone()) {
                                albums_to_join.push((aid, album.album_name));
                            }
                        }
                    }
                    Err(err) => {
                        album_query_failed = true;
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(loser.original_file_name.clone()),
                                format!(
                                    "[{}] 중복본 소속 앨범 조회 실패로 삭제 보류 ({}): {}",
                                    mode_tag, loser.original_file_name, err
                                ),
                            )
                            .await;
                        break;
                    }
                }
            }
            if album_query_failed {
                state.processed_files.fetch_add(1, Ordering::SeqCst);
                continue;
            }
        }

        let mut absorbed_parts: Vec<String> = Vec::new();
        if let Some(ref dt) = plan.metadata_update.date_time_original {
            absorbed_parts.push(format!("촬영일={}", &dt[..dt.len().min(19)]));
        }
        if let Some(ref tz) = plan.metadata_update.time_zone {
            absorbed_parts.push(format!("시간대={}", tz));
        }
        if !albums_to_join.is_empty() {
            let names: Vec<&str> = albums_to_join.iter().map(|(_, n)| n.as_str()).collect();
            absorbed_parts.push(format!("앨범 {}개({})", albums_to_join.len(), names.join(", ")));
        }
        if !plan.tag_ids_to_add.is_empty() {
            absorbed_parts.push(format!("태그 {}개", plan.tag_ids_to_add.len()));
        }
        if plan.metadata_update.latitude.is_some() && plan.metadata_update.longitude.is_some() {
            absorbed_parts.push("GPS 좌표".to_string());
        }
        if plan.metadata_update.is_favorite == Some(true) {
            absorbed_parts.push("즐겨찾기".to_string());
        }
        if plan.metadata_update.visibility.as_deref() == Some("archive") {
            absorbed_parts.push("보관함".to_string());
        }
        if plan.metadata_update.description.is_some() {
            absorbed_parts.push("설명".to_string());
        }
        if plan.metadata_update.rating.is_some() {
            absorbed_parts.push("별점".to_string());
        }
        if plan.merged_live_photo_video_id.is_some() {
            absorbed_parts.push("라이브포토 영상".to_string());
        }

        let absorbed_summary = if absorbed_parts.is_empty() {
            "Winner에 이미 모든 최신/최적 메타데이터 보유".to_string()
        } else {
            format!("흡수 병합: {}", absorbed_parts.join(", "))
        };

        let winner_mb = plan
            .winner
            .exif_info
            .as_ref()
            .and_then(|e| e.file_size_in_byte)
            .unwrap_or(0) as f64
            / (1024.0 * 1024.0);
        let loser_names: Vec<&str> = plan
            .losers
            .iter()
            .map(|l| l.original_file_name.as_str())
            .collect();

        if dry_run {
            merged_groups += 1;
            trashed_assets += plan.loser_ids.len() as u64;
            state.processed_files.fetch_add(1, Ordering::SeqCst);
            state
                .emit_event(
                    "repair",
                    JobState::Syncing,
                    Some(plan.winner.original_file_name.clone()),
                    format!(
                        "[{}] 유지: {} ({:.2} MB) | {} | 휴지통 이동 예정: {}건 ({})",
                        mode_tag,
                        plan.winner.original_file_name,
                        winner_mb,
                        absorbed_summary,
                        plan.loser_ids.len(),
                        loser_names.join(", ")
                    ),
                )
                .await;
            continue;
        }

        if plan.metadata_update.has_updates() {
            if let Err(err) = immich_target
                .update_asset_metadata(&plan.metadata_update)
                .await
            {
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(plan.winner.original_file_name.clone()),
                        format!(
                            "[{}] 메타데이터 병합 실패로 삭제 보류 ({}): {}",
                            mode_tag, plan.winner.original_file_name, err
                        ),
                    )
                    .await;
                state.processed_files.fetch_add(1, Ordering::SeqCst);
                continue;
            }
        }

        if let Some(ref vid_id) = plan.merged_live_photo_video_id {
            if let Err(err) = immich_target
                .link_live_photo_video(&plan.winner.id, vid_id)
                .await
            {
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(plan.winner.original_file_name.clone()),
                        format!(
                            "[{}] 라이브포토 영상 연결 실패로 삭제 보류 ({}): {}",
                            mode_tag, plan.winner.original_file_name, err
                        ),
                    )
                    .await;
                state.processed_files.fetch_add(1, Ordering::SeqCst);
                continue;
            }
        }

        let mut album_transfer_failed = false;
        for (album_id, album_name) in &albums_to_join {
            if let Err(err) = immich_target
                .add_assets_to_album(album_id, std::slice::from_ref(&plan.winner.id))
                .await
            {
                album_transfer_failed = true;
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(plan.winner.original_file_name.clone()),
                        format!(
                            "[{}] 앨범('{}') 편입 실패로 삭제 보류 ({}): {}",
                            mode_tag, album_name, plan.winner.original_file_name, err
                        ),
                    )
                    .await;
                break;
            }
        }
        if album_transfer_failed {
            state.processed_files.fetch_add(1, Ordering::SeqCst);
            continue;
        }

        let mut tag_transfer_failed = false;
        for tag_id in &plan.tag_ids_to_add {
            if let Err(err) = immich_target
                .add_assets_to_tag(tag_id, std::slice::from_ref(&plan.winner.id))
                .await
            {
                tag_transfer_failed = true;
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(plan.winner.original_file_name.clone()),
                        format!(
                            "[{}] 태그 편입 실패로 삭제 보류 ({}): {}",
                            mode_tag, plan.winner.original_file_name, err
                        ),
                    )
                    .await;
                break;
            }
        }
        if tag_transfer_failed {
            state.processed_files.fetch_add(1, Ordering::SeqCst);
            continue;
        }

        let mut unlink_failed = false;
        for loser_id in &plan.loser_ids_to_unlink_live_video {
            if let Err(err) = immich_target.unlink_live_photo_video(loser_id).await {
                unlink_failed = true;
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(plan.winner.original_file_name.clone()),
                        format!(
                            "[{}] 중복본 라이브포토 연결 해제 실패로 삭제 보류 ({}): {}",
                            mode_tag, plan.winner.original_file_name, err
                        ),
                    )
                    .await;
                break;
            }
        }
        if unlink_failed {
            state.processed_files.fetch_add(1, Ordering::SeqCst);
            continue;
        }

        if let Err(err) = immich_target.trash_assets(&plan.loser_ids).await {
            state
                .emit_event(
                    "repair",
                    JobState::Syncing,
                    Some(plan.winner.original_file_name.clone()),
                    format!(
                        "[{}] 중복본 휴지통 이동 실패 ({}): {}",
                        mode_tag, plan.winner.original_file_name, err
                    ),
                )
                .await;
            state.processed_files.fetch_add(1, Ordering::SeqCst);
            continue;
        }

        let loser_device_ids: Vec<String> = plan
            .losers
            .iter()
            .filter_map(|l| l.device_asset_id.clone())
            .collect();
        let winner_sha1_hex = decode_immich_checksum_to_hex(plan.winner.checksum.as_deref());
        if let Err(err) = state
            .db
            .remap_merged_immich_assets(
                &plan.loser_ids,
                &loser_device_ids,
                &plan.winner.id,
                winner_sha1_hex.as_deref(),
            )
            .await
        {
            state
                .emit_event(
                    "repair",
                    JobState::Syncing,
                    Some(plan.winner.original_file_name.clone()),
                    format!(
                        "[{}] 로컬 DB 매핑 갱신 경고 ({}): {}",
                        mode_tag, plan.winner.original_file_name, err
                    ),
                )
                .await;
        }

        merged_groups += 1;
        trashed_assets += plan.loser_ids.len() as u64;
        state.processed_files.fetch_add(1, Ordering::SeqCst);
        state
            .emit_event(
                "repair",
                JobState::Syncing,
                Some(plan.winner.original_file_name.clone()),
                format!(
                    "[{}] 병합 완료: {} ({:.2} MB) 유지 | {} | 중복 {}건 휴지통 이동",
                    mode_tag,
                    plan.winner.original_file_name,
                    winner_mb,
                    absorbed_summary,
                    plan.loser_ids.len()
                ),
            )
            .await;
    }

    state
        .finish_job(
            "repair",
            format!(
                "[{}] 완료 — 병합 {}그룹 (중복 자산 {}건 {}) · 연속 촬영 자동 보호 {}그룹",
                mode_tag,
                merged_groups,
                trashed_assets,
                if dry_run {
                    "휴지통 이동 예정"
                } else {
                    "휴지통 이동됨"
                },
                skipped_burst_groups
            ),
        )
        .await;

    Ok(merged_groups)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::CheckpointDB;

    #[tokio::test]
    async fn test_move_immich_album_requires_enabled_nextcloud() {
        let db = CheckpointDB::connect(":memory:").await.unwrap();
        let cfg = AppConfig {
            enable_nextcloud: false,
            ..Default::default()
        };
        let state = AppState::new(db, cfg.clone());

        let res = run_move_immich_album_to_nextcloud(state, cfg, "to-nextcloud").await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Nextcloud 연동이 활성화되어 있지 않습니다"));
    }

    #[tokio::test]
    async fn test_repair_nextcloud_anomalies_noop_when_empty() {
        let db = CheckpointDB::connect(":memory:").await.unwrap();
        let cfg = AppConfig::default();
        let state = AppState::new(db, cfg.clone());

        let count = run_repair_nextcloud_anomalies(state, cfg).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_merge_immich_duplicates_requires_configured_immich() {
        let db = CheckpointDB::connect(":memory:").await.unwrap();
        let cfg = AppConfig {
            immich_url: String::new(),
            immich_api_key: String::new(),
            ..Default::default()
        };
        let state = AppState::new(db, cfg.clone());

        let res = run_merge_immich_duplicates(state, cfg, true).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Immich 서버 URL 또는 API Key가 설정되지 않았습니다"));
    }
}
