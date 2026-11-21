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
    asset_capture_epoch_secs, decode_immich_checksum_to_hex, evaluate_duplicate_group,
    find_healthy_duplicate, is_broken_asset_candidate, is_valid_immich_asset_id_candidate,
    normalize_filename_stem, AssetMetadataUpdateRequest, DuplicateGroupEvaluation, HealthyAssetRef,
    ImmichDuplicateAssetDto, ImmichTarget, ThumbnailHealth,
};
use crate::targets::nextcloud::NextcloudTarget;
use crate::targets::{TargetProvider, UploadContext, UploadOutcome, VerifyStatus};
use anyhow::{anyhow, Result};
use futures::stream::{self, StreamExt};
use std::collections::{HashMap, HashSet};
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
                    .emit_progress_throttled(
                        src,
                        JobState::Scanning,
                        Some(folder_path.to_string()),
                        || {
                            format!(
                                "폴더 스캔 중: {} (발견된 파일 {}건 체크포인트 등록)",
                                folder_path, added
                            )
                        },
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

    async fn inspect_uploaded_batch(
        &self,
        files: &[FileRecord],
        phase_state: JobState,
    ) -> HashMap<String, VerifyStatus> {
        let src = self.source_key();
        let mut by_target: HashMap<TargetType, Vec<FileRecord>> = HashMap::new();
        let mut results = HashMap::with_capacity(files.len());
        let mut record_lookup: HashMap<&str, &FileRecord> = HashMap::with_capacity(files.len());

        for f in files {
            record_lookup.insert(f.resource_id.as_str(), f);
            match f.target_type.parse::<TargetType>() {
                Ok(t) => by_target.entry(t).or_default().push(f.clone()),
                Err(_) => {
                    results.insert(
                        f.resource_id.clone(),
                        VerifyStatus::TargetError(format!("알 수 없는 타겟 타입: {}", f.target_type)),
                    );
                }
            }
        }

        for (target_type, group) in by_target {
            if self.state.is_stopping() {
                break;
            }
            self.state
                .emit_event(
                    src,
                    phase_state,
                    None,
                    format!(
                        "[{}] 타겟 재검증 진행 중 ({}: {}건)...",
                        src,
                        target_type,
                        group.len()
                    ),
                )
                .await;

            match self.targets.get(&target_type) {
                Some(target) => {
                    let map = target.inspect_exists_batch(&group).await;
                    results.extend(map);
                }
                None => {
                    for f in group {
                        results.insert(
                            f.resource_id,
                            VerifyStatus::TargetError(format!(
                                "등록되지 않은 타겟 플러그인: {}",
                                target_type
                            )),
                        );
                    }
                }
            }
        }

        let mut heals: Vec<(String, String, Option<String>, Option<String>)> = Vec::new();
        for (res_id, status) in &results {
            if let VerifyStatus::Present {
                healed_target_id,
                healed_sha1,
            } = status
            {
                if healed_target_id.is_some() || healed_sha1.is_some() {
                    if let Some(rec) = record_lookup.get(res_id.as_str()) {
                        heals.push((
                            rec.source_type.clone(),
                            rec.resource_id.clone(),
                            healed_target_id.clone(),
                            healed_sha1.clone(),
                        ));
                    }
                }
            }
        }
        if !heals.is_empty() {
            let _ = self.state.db.heal_immich_hashes_and_targets(&heals).await;
        }

        results
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

        let verified_map = self
            .inspect_uploaded_batch(&uploaded, JobState::Syncing)
            .await;
        for file in &uploaded {
            if self.state.is_stopping() {
                return Ok(());
            }
            let st = verified_map.get(&file.resource_id);
            if !matches!(st, Some(VerifyStatus::Present { .. })) {
                let reason = match st {
                    Some(VerifyStatus::TrashedOnTarget) => "타겟 휴지통에 보관 중".to_string(),
                    Some(VerifyStatus::MissingOnTarget) | None => "타겟 미존재".to_string(),
                    Some(VerifyStatus::SizeMismatch { expected, actual }) => {
                        format!("크기 불일치 ({} != {})", expected, actual)
                    }
                    Some(VerifyStatus::TargetError(err)) => format!("통신 오류: {}", err),
                    Some(VerifyStatus::Present { .. }) => unreachable!(),
                };
                self.state
                    .emit_event(
                        src,
                        JobState::Syncing,
                        Some(file.name.clone()),
                        format!(
                            "[{}] 타겟 재검증 실패({}) 항목이 있어 로컬 ZIP 삭제를 중단합니다: {} ({})",
                            src, reason, file.name, file.target_type
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
            .emit_progress_throttled(src, JobState::Syncing, Some(file.name.clone()), || {
                format!(
                    "처리 중: {}/{}",
                    file.parent_path.trim_end_matches('/'),
                    file.name
                )
            })
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
                    .emit_progress_throttled(
                        src,
                        JobState::Syncing,
                        Some(final_name.clone()),
                        || format!("이주 완료 [{} / {}]: {}", final_target, result_str, final_name),
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

        let inspected = if is_null_ext || target_type == TargetType::Immich {
            inspect_file_path(downloaded.temp_guard.path())
                .ok()
                .flatten()
        } else {
            None
        };

        if is_livephoto_zip || matches!(inspected, Some(DetectedFileKind::ZipArchive)) {
            let zip_path_owned = downloaded.temp_guard.path().to_path_buf();
            let zip_name_owned = file.name.clone();
            let extract_tmp = chosen_tmp.clone();
            let extracted_opt = tokio::task::spawn_blocking(move || {
                try_extract_livephoto_zip(&zip_path_owned, &zip_name_owned, &extract_tmp)
            })
            .await
            .ok()
            .and_then(|r| r.ok())
            .flatten();

            if let Some(extracted) = extracted_opt {
                let created_with_zip = pick_older_timestamp(
                    &resolved_created,
                    extracted.timestamp_override.as_deref(),
                );
                let img_ts = extract_timestamp_from_filename(&extracted.image_filename);
                let final_created = pick_older_timestamp(&created_with_zip, img_ts.as_deref());

                let outcome = self
                    .immich_target
                    .upload_live_photo(&extracted, &file.resource_id, &final_created, modified_at)
                    .await?;

                let db_size = if !is_livephoto_zip
                    && !is_null_ext
                    && file.size > 0
                    && extracted.image_filename == file.name
                {
                    file.size
                } else {
                    extracted.image_size as i64
                };

                return Ok((
                    extracted.image_sha1,
                    Some(db_size),
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
            } else if !is_null_ext && target_type == TargetType::Immich {
                return Err(anyhow!(
                    "미디어 확장자({}) 파일이 ZIP 아카이브(PK)로 위장되어 있으나 유효한 사진이 없어 Immich 업로드를 중단합니다.",
                    file.name
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
        self.run_delete_with_options(dry_run, false).await
    }

    pub async fn run_delete_with_options(
        &self,
        dry_run: bool,
        include_target_deleted: bool,
    ) -> Result<SyncStats> {
        let src = self.source_key();
        let _ = self.state.db.heal_misclassified_nextcloud_targets().await;
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

        let mode_suffix = if include_target_deleted {
            " · 타겟 선삭제 포함"
        } else {
            ""
        };
        self.state
            .emit_event(
                src,
                JobState::Deleting,
                None,
                if dry_run {
                    format!(
                        "[{}] 원격 삭제 시뮬레이션(Dry-Run) 시작 (대상: {}건{})",
                        src,
                        uploaded.len(),
                        mode_suffix
                    )
                } else {
                    format!(
                        "[{}] 재검증 후 원격 휴지통 삭제 시작 (대상: {}건{})",
                        src,
                        uploaded.len(),
                        mode_suffix
                    )
                },
            )
            .await;

        if uploaded.is_empty() {
            let stats = self.state.db.get_stats(src).await?;
            self.state
                .finish_job(
                    src,
                    format!("[{}] 정리 대상(UPLOADED) 파일이 없습니다.", src),
                )
                .await;
            return Ok(stats);
        }

        let total_checked = uploaded.len();
        let verified_map = self
            .inspect_uploaded_batch(&uploaded, JobState::Deleting)
            .await;

        if self.state.is_stopping() {
            let stats = self.state.db.get_stats(src).await?;
            self.state
                .finish_job(src, format!("[{}] 삭제 작업이 안전 일시정지되었습니다.", src))
                .await;
            return Ok(stats);
        }

        // Per-target safety circuit breaker when `include_target_deleted` is enabled
        let mut tripped_targets: HashSet<TargetType> = HashSet::new();
        if include_target_deleted {
            let mut per_target: HashMap<TargetType, (usize, usize, usize)> = HashMap::new();
            for file in &uploaded {
                let Ok(tt) = file.target_type.parse::<TargetType>() else {
                    continue;
                };
                let entry = per_target.entry(tt).or_insert((0, 0, 0));
                entry.0 += 1;
                match verified_map.get(&file.resource_id) {
                    Some(VerifyStatus::Present { .. } | VerifyStatus::TrashedOnTarget) => {
                        entry.1 += 1;
                    }
                    Some(VerifyStatus::MissingOnTarget) => {
                        entry.2 += 1;
                    }
                    _ => {}
                }
            }
            for (tt, (target_total, alive_count, missing_count)) in per_target {
                if missing_count > 0 && alive_count == 0 && target_total >= 5 {
                    tripped_targets.insert(tt);
                    self.state
                        .emit_event(
                            src,
                            JobState::Deleting,
                            None,
                            format!(
                                "[안전 차단] {} 검증 대상 {}건 중 타겟에 존재하는 파일이 0건입니다. 스토리지 마운트 해제 또는 빈 서버 연결 사고 방지를 위해 선삭제 항목 자동 정리를 보류합니다.",
                                tt, target_total
                            ),
                        )
                        .await;
                }
            }
        }

        let mut eligible: Vec<FileRecord> = Vec::with_capacity(uploaded.len());
        let mut cnt_present = 0usize;
        let mut cnt_trashed = 0usize;
        let mut cnt_included_missing = 0usize;
        let mut cnt_held_missing = 0usize;
        let mut cnt_held_mismatch = 0usize;
        let mut cnt_held_error = 0usize;
        let mut held_sample_logs = 0u32;
        const MAX_HELD_SAMPLE_LOGS: u32 = 20;

        for file in uploaded {
            let status = verified_map
                .get(&file.resource_id)
                .cloned()
                .unwrap_or_else(|| {
                    VerifyStatus::TargetError("검증 결과 누락".to_string())
                });

            match status {
                VerifyStatus::Present { .. } => {
                    cnt_present += 1;
                    eligible.push(file);
                }
                VerifyStatus::TrashedOnTarget => {
                    cnt_trashed += 1;
                    eligible.push(file);
                }
                VerifyStatus::MissingOnTarget => {
                    let tripped = file
                        .target_type
                        .parse::<TargetType>()
                        .map(|tt| tripped_targets.contains(&tt))
                        .unwrap_or(false);
                    if include_target_deleted && !tripped {
                        cnt_included_missing += 1;
                        eligible.push(file);
                    } else {
                        cnt_held_missing += 1;
                        if held_sample_logs < MAX_HELD_SAMPLE_LOGS {
                            held_sample_logs += 1;
                            self.state
                                .emit_event(
                                    src,
                                    JobState::Deleting,
                                    Some(file.name.clone()),
                                    format!(
                                        "⚠️ 삭제 보류 (타겟 미존재): {} ({})",
                                        file.name, file.target_type
                                    ),
                                )
                                .await;
                        }
                    }
                }
                VerifyStatus::SizeMismatch { expected, actual } => {
                    cnt_held_mismatch += 1;
                    if held_sample_logs < MAX_HELD_SAMPLE_LOGS {
                        held_sample_logs += 1;
                        self.state
                            .emit_event(
                                src,
                                JobState::Deleting,
                                Some(file.name.clone()),
                                format!(
                                    "⚠️ 삭제 보류 (크기 불일치 {} != {}): {} ({})",
                                    expected, actual, file.name, file.target_type
                                ),
                            )
                            .await;
                    }
                }
                VerifyStatus::TargetError(err) => {
                    cnt_held_error += 1;
                    if held_sample_logs < MAX_HELD_SAMPLE_LOGS {
                        held_sample_logs += 1;
                        self.state
                            .emit_event(
                                src,
                                JobState::Deleting,
                                Some(file.name.clone()),
                                format!(
                                    "⚠️ 삭제 보류 (타겟 통신 오류: {}): {} ({})",
                                    err, file.name, file.target_type
                                ),
                            )
                            .await;
                    }
                }
            }
        }

        let held_total = cnt_held_missing + cnt_held_mismatch + cnt_held_error;
        let eligible_total = eligible.len();

        if dry_run {
            self.state
                .processed_files
                .store(total_checked as u64, Ordering::SeqCst);
            let stats = self.state.db.get_stats(src).await?;
            self.state
                .finish_job(
                    src,
                    format!(
                        "[{}] 삭제 Dry-Run 완료 — 정리 가능 {}건 (타겟 보관 {}, 타겟 휴지통 {}, 타겟 선삭제 포함 {}) · 보류 {}건 (타겟 미존재 {}, 크기 불일치 {}, 통신 오류 {})",
                        src,
                        eligible_total,
                        cnt_present,
                        cnt_trashed,
                        cnt_included_missing,
                        held_total,
                        cnt_held_missing,
                        cnt_held_mismatch,
                        cnt_held_error
                    ),
                )
                .await;
            return Ok(stats);
        }

        if held_total > 0 {
            self.state
                .processed_files
                .fetch_add(held_total as u64, Ordering::SeqCst);
        }

        let concurrency = self.config.concurrency.max(1);
        let mut deleted_total = 0u64;
        let mut delete_failed_total = 0u64;
        let mut fail_logs = 0u32;

        for chunk in eligible.chunks(100) {
            if self.state.is_stopping() {
                break;
            }
            let chunk_vec: Vec<FileRecord> = chunk.to_vec();
            let outcomes: Vec<Option<(FileRecord, Result<()>)>> = stream::iter(chunk_vec)
                .map(|file| async move {
                    if self.state.is_stopping() {
                        return None;
                    }
                    let res = self.source.delete_resource(&file.resource_id).await;
                    Some((file, res))
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;

            let mut deleted_ids: Vec<String> = Vec::new();
            let mut last_deleted_name: Option<String> = None;

            for item in outcomes.into_iter().flatten() {
                let (file, res) = item;
                self.state.processed_files.fetch_add(1, Ordering::SeqCst);
                match res {
                    Ok(()) => {
                        deleted_total += 1;
                        last_deleted_name = Some(file.name);
                        deleted_ids.push(file.resource_id);
                    }
                    Err(err) => {
                        delete_failed_total += 1;
                        if fail_logs < 30 {
                            fail_logs += 1;
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
            }

            if !deleted_ids.is_empty() {
                self.state
                    .db
                    .mark_files_deleted_batch(src, &deleted_ids)
                    .await?;
                self.state
                    .emit_progress_throttled(src, JobState::Deleting, last_deleted_name, || {
                        format!(
                            "[{}] 원격 휴지통 이동 진행 중... ({}건 완료 / 대상 {}건)",
                            src, deleted_total, eligible_total
                        )
                    })
                    .await;
            }
        }

        let stats = self.state.db.get_stats(src).await?;
        self.state
            .finish_job(
                src,
                format!(
                    "[{}] 원격 휴지통 이동 완료 — 이번 실행 {}건 삭제 (실패 {}건, 보류 {}건 / 누적 삭제: {}건)",
                    src, deleted_total, delete_failed_total, held_total, stats.deleted_files
                ),
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
            let zip_path_owned = downloaded.temp_guard.path().to_path_buf();
            let zip_name_owned = file.name.clone();
            let extract_tmp = chosen_tmp.clone();
            let extracted_opt = tokio::task::spawn_blocking(move || {
                try_extract_livephoto_zip(&zip_path_owned, &zip_name_owned, &extract_tmp)
            })
            .await
            .ok()
            .and_then(|r| r.ok())
            .flatten();

            if let Some(extracted) = extracted_opt {
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
                            .emit_progress_throttled(
                                "repair",
                                JobState::Syncing,
                                Some(extracted.image_filename.clone()),
                                || {
                                    format!(
                                        "[사후 보정] 라이브포토 복구 완료 [{} / 촬영일: {} / 확인: {}]: {} → {} + {}",
                                        outcome.sync_result.as_str(),
                                        date_label,
                                        immich_link,
                                        file.name,
                                        extracted.image_filename,
                                        vid_label
                                    )
                                },
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
                                .emit_progress_throttled(
                                    "repair",
                                    JobState::Syncing,
                                    Some(restored_name.clone()),
                                    || {
                                        format!(
                                            "[사후 보정] (null) 미디어 복구 완료 [{} / 촬영일: {} / 확인: {}]: {} → {}",
                                            status_note,
                                            date_label,
                                            immich_link,
                                            file.name,
                                            restored_name
                                        )
                                    },
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
                                .emit_progress_throttled(
                                    "repair",
                                    JobState::Syncing,
                                    Some(restored_name.clone()),
                                    || {
                                        format!(
                                            "[사후 보정] (null) 문서 확장자 복원 완료 (Nextcloud): {} → {}",
                                            file.name, restored_name
                                        )
                                    },
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
                                .emit_progress_throttled(
                                    "repair",
                                    JobState::Syncing,
                                    Some(restored_name.clone()),
                                    || {
                                        format!(
                                            "[사후 보정] (null) 압축파일 확장자 복원 완료 (Nextcloud): {} → {}",
                                            file.name, restored_name
                                        )
                                    },
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
    let immich_target = Arc::new(ImmichTarget::new(
        config.immich_url.clone(),
        config.immich_api_key.clone(),
        target_limiter,
    ));

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
            .emit_progress_throttled(
                "repair",
                JobState::Syncing,
                Some(filename.clone()),
                || {
                    format!(
                        "[앨범 핀포인트 이동] Immich → Nextcloud 이관 완료: {}/{}",
                        parent_path.trim_end_matches('/'),
                        filename
                    )
                },
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
    let immich_target = Arc::new(ImmichTarget::new(
        config.immich_url.clone(),
        config.immich_api_key.clone(),
        target_limiter,
    ));

    let groups = immich_target.get_duplicates().await?;
    let mut plans: Vec<Box<crate::targets::immich::DuplicateMergePlan>> = groups
        .into_iter()
        .filter_map(|g| match evaluate_duplicate_group(&g) {
            DuplicateGroupEvaluation::Merge(p) => Some(p),
            DuplicateGroupEvaluation::SkippedInsufficientAssets => None,
        })
        .collect();

    if plans.is_empty() {
        state
            .finish_job(
                "repair",
                "[중복 스마트 병합] Immich에서 감지된 중복 사진 그룹이 없습니다.",
            )
            .await;
        return Ok(0);
    }

    if let Some(n) = limit.filter(|&n| n > 0) {
        plans.truncate(n as usize);
    }

    let mode_tag = if dry_run {
        "중복 스마트 병합 Dry-Run"
    } else {
        "중복 스마트 병합"
    };

    state
        .begin_phase("repair", JobState::Syncing, plans.len() as u64)
        .await;
    state
        .emit_event(
            "repair",
            JobState::Syncing,
            None,
            format!(
                "[{}] Immich 중복 그룹 {}건 분석 및 선택적 메타데이터 병합을 시작합니다.",
                mode_tag,
                plans.len()
            ),
        )
        .await;

    let all_albums = immich_target.list_all_albums().await?;
    let has_non_empty_albums = !all_albums.is_empty();
    let mut album_map: HashMap<String, Vec<(String, String)>> = HashMap::new();
    let mut albums_preloaded = !has_non_empty_albums;

    if has_non_empty_albums {
        let populated: Vec<(String, String)> = all_albums
            .iter()
            .filter(|a| a.asset_count > 0 && !a.id.trim().is_empty())
            .map(|a| (a.id.clone(), a.album_name.clone()))
            .collect();
        let fetched: Vec<Result<(String, String, Vec<_>)>> = stream::iter(populated)
            .map(|(aid, aname)| {
                let target = Arc::clone(&immich_target);
                async move {
                    let assets = target.get_album_assets(&aid).await?;
                    Ok((aid, aname, assets))
                }
            })
            .buffer_unordered(4)
            .collect()
            .await;

        let mut all_ok = true;
        for item in fetched {
            match item {
                Ok((aid, aname, assets)) => {
                    for asset in assets {
                        if !asset.id.is_empty() {
                            album_map
                                .entry(asset.id)
                                .or_default()
                                .push((aid.clone(), aname.clone()));
                        }
                    }
                }
                Err(_) => {
                    all_ok = false;
                    break;
                }
            }
        }
        albums_preloaded = all_ok;
    }

    let mut merged_groups = 0u64;
    let mut trashed_assets = 0u64;

    for plan in plans {
        if state.is_stopping() {
            break;
        }

        let mut albums_to_join: Vec<(String, String)> = Vec::new();
        if has_non_empty_albums && albums_preloaded {
            let mut seen_album_ids: HashSet<String> = album_map
                .get(&plan.winner.id)
                .into_iter()
                .flatten()
                .map(|(aid, _)| aid.clone())
                .collect();
            for loser in &plan.losers {
                if let Some(list) = album_map.get(&loser.id) {
                    for (aid, aname) in list {
                        if seen_album_ids.insert(aid.clone()) {
                            albums_to_join.push((aid.clone(), aname.clone()));
                        }
                    }
                }
            }
        } else if has_non_empty_albums {
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
                .emit_progress_throttled(
                    "repair",
                    JobState::Syncing,
                    Some(plan.winner.original_file_name.clone()),
                    || {
                        format!(
                            "[{}] 유지: {} ({:.2} MB) | {} | 휴지통 이동 예정: {}건 ({})",
                            mode_tag,
                            plan.winner.original_file_name,
                            winner_mb,
                            absorbed_summary,
                            plan.loser_ids.len(),
                            loser_names.join(", ")
                        )
                    },
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
        let loser_sha1_hashes: Vec<String> = plan
            .losers
            .iter()
            .filter_map(|l| decode_immich_checksum_to_hex(l.checksum.as_deref()))
            .collect();
        let winner_sha1_hex = decode_immich_checksum_to_hex(plan.winner.checksum.as_deref());
        if let Err(err) = state
            .db
            .remap_merged_immich_assets_with_hashes(
                &plan.loser_ids,
                &loser_device_ids,
                &loser_sha1_hashes,
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
            .emit_progress_throttled(
                "repair",
                JobState::Syncing,
                Some(plan.winner.original_file_name.clone()),
                || {
                    format!(
                        "[{}] 병합 완료: {} ({:.2} MB) 유지 | {} | 중복 {}건 휴지통 이동",
                        mode_tag,
                        plan.winner.original_file_name,
                        winner_mb,
                        absorbed_summary,
                        plan.loser_ids.len()
                    )
                },
            )
            .await;
    }

    state
        .finish_job(
            "repair",
            format!(
                "[{}] 완료 — 병합 {}그룹 (중복 자산 {}건 {})",
                mode_tag,
                merged_groups,
                trashed_assets,
                if dry_run {
                    "휴지통 이동 예정"
                } else {
                    "휴지통 이동됨"
                }
            ),
        )
        .await;

    Ok(merged_groups)
}

async fn transfer_broken_asset_metadata_and_albums(
    immich_target: &ImmichTarget,
    broken_detail: &ImmichDuplicateAssetDto,
    target_asset_id: &str,
    target_detail_opt: Option<&ImmichDuplicateAssetDto>,
) -> Result<()> {
    let broken_albums = immich_target
        .get_albums_for_asset(&broken_detail.id)
        .await?;

    let merged_is_favorite = if broken_detail.is_favorite
        && !target_detail_opt.map(|t| t.is_favorite).unwrap_or(false)
    {
        Some(true)
    } else {
        None
    };

    let is_archived = |a: &ImmichDuplicateAssetDto| {
        a.is_archived
            || a.visibility
                .as_deref()
                .map(|v| v.eq_ignore_ascii_case("archive"))
                .unwrap_or(false)
    };
    let merged_visibility =
        if is_archived(broken_detail) && !target_detail_opt.map(is_archived).unwrap_or(false) {
            Some("archive".to_string())
        } else {
            None
        };

    let broken_desc = broken_detail
        .exif_info
        .as_ref()
        .and_then(|e| e.description.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let target_has_desc = target_detail_opt
        .and_then(|t| t.exif_info.as_ref())
        .and_then(|e| e.description.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .is_some();
    let merged_description = if !target_has_desc {
        broken_desc.map(str::to_string)
    } else {
        None
    };

    let broken_rating = broken_detail
        .exif_info
        .as_ref()
        .and_then(|e| e.rating)
        .unwrap_or(0);
    let target_rating = target_detail_opt
        .and_then(|t| t.exif_info.as_ref())
        .and_then(|e| e.rating)
        .unwrap_or(0);
    let merged_rating = if broken_rating > target_rating && broken_rating > 0 {
        Some(broken_rating)
    } else {
        None
    };

    let update_req = AssetMetadataUpdateRequest {
        ids: vec![target_asset_id.to_string()],
        is_favorite: merged_is_favorite,
        visibility: merged_visibility,
        description: merged_description,
        rating: merged_rating,
        ..Default::default()
    };
    if update_req.has_updates() {
        immich_target.update_asset_metadata(&update_req).await?;
    }

    let target_id_owned = target_asset_id.to_string();
    for album in broken_albums {
        let aid = album.id.trim();
        if !aid.is_empty() {
            immich_target
                .add_assets_to_album(aid, std::slice::from_ref(&target_id_owned))
                .await?;
        }
    }

    let existing_target_tags: HashSet<&str> = target_detail_opt
        .map(|t| t.tags.iter().map(|tag| tag.id.trim()).collect())
        .unwrap_or_default();
    for tag in &broken_detail.tags {
        let tid = tag.id.trim();
        if !tid.is_empty() && !existing_target_tags.contains(tid) {
            immich_target
                .add_assets_to_tag(tid, std::slice::from_ref(&target_id_owned))
                .await?;
        }
    }

    Ok(())
}

fn resolve_broken_zip_timestamps(
    asset: &ImmichDuplicateAssetDto,
    orig_filename: &str,
    extracted_image_filename: &str,
    extracted_ts_override: Option<&str>,
) -> (String, String) {
    let fallback_now = chrono::Utc::now().to_rfc3339();
    let base_created = asset
        .exif_info
        .as_ref()
        .and_then(|e| e.date_time_original.as_deref())
        .or(asset.file_created_at.as_deref())
        .unwrap_or(&fallback_now);
    let mut created_at = pick_older_timestamp(base_created, asset.file_created_at.as_deref());
    created_at = pick_older_timestamp(&created_at, extracted_ts_override);
    let ext_img_ts = extract_timestamp_from_filename(extracted_image_filename);
    created_at = pick_older_timestamp(&created_at, ext_img_ts.as_deref());
    let orig_fn_ts = extract_timestamp_from_filename(orig_filename);
    created_at = pick_older_timestamp(&created_at, orig_fn_ts.as_deref());
    let modified_at = asset
        .file_modified_at
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(&created_at)
        .to_string();
    (created_at, modified_at)
}

pub async fn run_repair_immich_broken_assets(
    state: Arc<AppState>,
    config: AppConfig,
    dry_run: bool,
) -> Result<u64> {
    run_repair_immich_broken_assets_with_limit(state, config, dry_run, None).await
}

pub async fn run_repair_immich_broken_assets_with_limit(
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

    let mode_tag = if dry_run {
        "오류 자산 진단 Dry-Run"
    } else {
        "오류 자산 복구·정리"
    };

    state.begin_phase("repair", JobState::Syncing, 0).await;
    state
        .emit_event(
            "repair",
            JobState::Syncing,
            None,
            format!(
                "[{}] Immich 전체 라이브러리 메타데이터 스캔을 시작합니다...",
                mode_tag
            ),
        )
        .await;

    let target_limiter =
        RateLimiter::new((config.rate_limit_rps * 10.0).max(50.0), config.max_retries);
    let immich_target = Arc::new(ImmichTarget::new(
        config.immich_url.clone(),
        config.immich_api_key.clone(),
        target_limiter,
    ));

    let mut page: u32 = 1;
    let mut scanned_total: u64 = 0;
    let mut suspects: Vec<ImmichDuplicateAssetDto> = Vec::new();
    let mut healthy_by_stem: HashMap<String, Vec<HealthyAssetRef>> = HashMap::new();

    loop {
        if state.is_stopping() {
            state
                .finish_job(
                    "repair",
                    format!(
                        "[{}] 메타데이터 스캔 중 안전 일시정지되었습니다 ({}건 스캔).",
                        mode_tag, scanned_total
                    ),
                )
                .await;
            return Ok(0);
        }

        let (items, next_page) = immich_target.search_metadata_page(page, 500).await?;
        for asset in items {
            scanned_total += 1;
            if asset.is_trashed || asset.id.trim().is_empty() {
                continue;
            }
            if !asset.is_offline
                && !asset
                    .thumbhash
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("")
                    .is_empty()
            {
                let stem = normalize_filename_stem(&asset.original_file_name);
                if stem.len() >= 4 {
                    healthy_by_stem
                        .entry(stem)
                        .or_default()
                        .push(HealthyAssetRef {
                            id: asset.id.clone(),
                            original_file_name: asset.original_file_name.clone(),
                            capture_ts: asset_capture_epoch_secs(&asset),
                        });
                }
            }
            if is_broken_asset_candidate(&asset) {
                suspects.push(asset);
            }
        }

        state
            .emit_progress_throttled("repair", JobState::Syncing, None, || {
                format!(
                    "[{}] Immich 스캔 중... ({}건 조회, 오류 후보 {}건)",
                    mode_tag,
                    scanned_total,
                    suspects.len()
                )
            })
            .await;

        match next_page {
            Some(np) => page = np,
            None => break,
        }
    }

    if suspects.is_empty() {
        state
            .finish_job(
                "repair",
                format!(
                    "[{}] Immich 전체 {}건 스캔 완료 — 로딩 오류 후보 자산이 없습니다.",
                    mode_tag, scanned_total
                ),
            )
            .await;
        return Ok(0);
    }

    state
        .emit_event(
            "repair",
            JobState::Syncing,
            None,
            format!(
                "[{}] 전체 {}건 중 오류 후보 {}건의 실제 썸네일 응답을 검증합니다...",
                mode_tag,
                scanned_total,
                suspects.len()
            ),
        )
        .await;

    let mut confirmed_broken: Vec<(ImmichDuplicateAssetDto, String)> = Vec::new();
    let mut transient_errors = 0u64;

    for chunk in suspects.chunks(32) {
        if state.is_stopping() {
            state
                .finish_job(
                    "repair",
                    format!("[{}] 썸네일 검증 중 안전 일시정지되었습니다.", mode_tag),
                )
                .await;
            return Ok(0);
        }

        let checks: Vec<(ImmichDuplicateAssetDto, ThumbnailHealth)> =
            stream::iter(chunk.iter().cloned())
                .map(|asset| {
                    let target = Arc::clone(&immich_target);
                    async move {
                        let health = target.check_thumbnail_health(&asset.id).await;
                        (asset, health)
                    }
                })
                .buffered(4)
                .collect()
                .await;

        for (asset, health) in checks {
            match health {
                ThumbnailHealth::Healthy => {
                    let stem = normalize_filename_stem(&asset.original_file_name);
                    if stem.len() >= 4 {
                        let capture_ts = asset_capture_epoch_secs(&asset);
                        let list = healthy_by_stem.entry(stem).or_default();
                        if !list.iter().any(|h| h.id == asset.id) {
                            list.push(HealthyAssetRef {
                                id: asset.id,
                                original_file_name: asset.original_file_name,
                                capture_ts,
                            });
                        }
                    }
                }
                ThumbnailHealth::Broken { status: _, reason } => {
                    confirmed_broken.push((asset, reason));
                }
                ThumbnailHealth::TransientError(_) => {
                    transient_errors += 1;
                }
            }
        }
    }

    if transient_errors > 0 {
        state
            .emit_event(
                "repair",
                JobState::Syncing,
                None,
                format!(
                    "[{}] 일시적 통신 오류로 썸네일 판정을 건너뛴 항목 {}건 (안전상 유지)",
                    mode_tag, transient_errors
                ),
            )
            .await;
    }

    if confirmed_broken.is_empty() {
        state
            .finish_job(
                "repair",
                format!(
                    "[{}] Immich 전체 {}건 중 후보 {}건 확인 완료 — 실제 썸네일 로딩 오류 자산이 없습니다.",
                    mode_tag,
                    scanned_total,
                    suspects.len()
                ),
            )
            .await;
        return Ok(0);
    }

    if let Some(n) = limit.filter(|&n| n > 0) {
        confirmed_broken.truncate(n as usize);
    }

    let total_broken = confirmed_broken.len() as u64;
    state.processed_files.store(0, Ordering::SeqCst);
    state.total_files.store(total_broken, Ordering::SeqCst);
    *state.job_started_at.write().await = Some(tokio::time::Instant::now());

    state
        .emit_event(
            "repair",
            JobState::Syncing,
            None,
            format!(
                "[{}] 썸네일 로딩 오류 확정 {}건에 대해 원본 검사를 시작합니다.",
                mode_tag, total_broken
            ),
        )
        .await;

    let fallback_dir = config.disk_fallback_tmp_dir();
    let immich_web_base = config.immich_url.trim_end_matches('/');

    let mut recovered_zip_created = 0u64;
    let mut recovered_zip_duplicate = 0u64;
    let mut trashed_non_zip_dup = 0u64;
    let mut staged_standalone_broken = 0u64;
    let mut failed_count = 0u64;
    let mut broken_album_id: Option<String> = None;

    for (asset, thumb_reason) in confirmed_broken {
        if state.is_stopping() {
            break;
        }

        let filename = if asset.original_file_name.trim().is_empty() {
            format!("{}.bin", asset.id)
        } else {
            asset.original_file_name.clone()
        };
        let size_hint = asset
            .exif_info
            .as_ref()
            .and_then(|e| e.file_size_in_byte)
            .filter(|&s| s > 0)
            .map(|s| s as u64);
        let chosen_tmp = crate::sources::resolve_temp_dir(
            Path::new(&config.tmp_dir),
            Some(&fallback_dir),
            size_hint,
        );
        let broken_url = format!("{}/photos/{}", immich_web_base, asset.id);

        let downloaded_opt = match immich_target
            .download_asset_optional_to_temp(&asset.id, &filename, &chosen_tmp)
            .await
        {
            Ok(opt) => opt,
            Err(err) => {
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(filename.clone()),
                        format!(
                            "[{}] 원본 다운로드 실패로 보류 ({}): {}",
                            mode_tag, filename, err
                        ),
                    )
                    .await;
                failed_count += 1;
                state.processed_files.fetch_add(1, Ordering::SeqCst);
                continue;
            }
        };

        let mut extracted_zip = None;
        let mut non_zip_reason: Option<String> = None;

        match downloaded_opt {
            None => {
                non_zip_reason = Some("원본 파일 미존재(HTTP 404)".to_string());
            }
            Some(downloaded) => {
                if downloaded.size == 0 {
                    non_zip_reason = Some("0바이트 빈 파일".to_string());
                } else {
                    let inspected = inspect_file_path(downloaded.temp_guard.path())
                        .ok()
                        .flatten();
                    match inspected {
                        Some(DetectedFileKind::ZipArchive) => {
                            let zip_path_owned = downloaded.temp_guard.path().to_path_buf();
                            let zip_name_owned = filename.clone();
                            let extract_tmp = chosen_tmp.clone();
                            let extracted_opt = tokio::task::spawn_blocking(move || {
                                try_extract_livephoto_zip(
                                    &zip_path_owned,
                                    &zip_name_owned,
                                    &extract_tmp,
                                )
                            })
                            .await
                            .ok()
                            .and_then(|r| r.ok())
                            .flatten();

                            if let Some(extracted) = extracted_opt {
                                extracted_zip = Some(extracted);
                            } else {
                                non_zip_reason =
                                    Some("ZIP 아카이브 내부에 추출 가능한 미디어 없음".to_string());
                            }
                        }
                        Some(DetectedFileKind::Media { ext, .. }) => {
                            non_zip_reason = Some(format!(
                                "원본 헤더({}) 확인되나 Immich 썸네일 디코딩 실패 ({})",
                                ext, thumb_reason
                            ));
                        }
                        Some(DetectedFileKind::Document { ext }) => {
                            non_zip_reason =
                                Some(format!("문서 헤더({}) 파일 ({})", ext, thumb_reason));
                        }
                        None => {
                            non_zip_reason =
                                Some(format!("비이미지/손상 데이터 ({})", thumb_reason));
                        }
                    }
                }
            }
        }

        if let Some(extracted) = extracted_zip {
            let (created_at, modified_at) = resolve_broken_zip_timestamps(
                &asset,
                &filename,
                &extracted.image_filename,
                extracted.timestamp_override.as_deref(),
            );
            let base_dev = asset
                .device_asset_id
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(&asset.id);
            let extracted_dev_id = format!("{}#extracted", base_dev);

            let dup_id_opt = match immich_target
                .precheck_duplicate(&extracted_dev_id, &extracted.image_sha1)
                .await
            {
                Ok(res) => res,
                Err(err) => {
                    state
                        .emit_event(
                            "repair",
                            JobState::Syncing,
                            Some(filename.clone()),
                            format!(
                                "[{}] 추출 사진 중복 확인 실패로 보류 ({}): {}",
                                mode_tag, filename, err
                            ),
                        )
                        .await;
                    failed_count += 1;
                    state.processed_files.fetch_add(1, Ordering::SeqCst);
                    continue;
                }
            };

            let mut existing_dup: Option<ImmichDuplicateAssetDto> = None;
            if let Some(valid_dup_id) = is_valid_immich_asset_id_candidate(dup_id_opt.as_deref()) {
                if valid_dup_id != asset.id {
                    match immich_target.get_asset_detail(valid_dup_id).await {
                        Ok(Some(detail)) if !detail.is_trashed => {
                            existing_dup = Some(detail);
                        }
                        Ok(_) => {}
                        Err(err) => {
                            state
                                .emit_event(
                                    "repair",
                                    JobState::Syncing,
                                    Some(filename.clone()),
                                    format!(
                                        "[{}] 중복 정상본 상세 조회 실패로 보류 ({}): {}",
                                        mode_tag, filename, err
                                    ),
                                )
                                .await;
                            failed_count += 1;
                            state.processed_files.fetch_add(1, Ordering::SeqCst);
                            continue;
                        }
                    }
                }
            }

            if let Some(existing) = existing_dup {
                let needs_live_link = existing
                    .live_photo_video_id
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("")
                    .is_empty()
                    && extracted.video_guard.is_some();
                let existing_url = format!("{}/photos/{}", immich_web_base, existing.id);

                if dry_run {
                    recovered_zip_duplicate += 1;
                    state
                        .emit_event(
                            "repair",
                            JobState::Syncing,
                            Some(filename.clone()),
                            format!(
                                "[{}] ZIP 위장 라이브포토(동일 정상본 존재): {} | 정상본: {} ({}) | {} | 깨진 ZIP 자산({}) 휴지통 이동 예정",
                                mode_tag,
                                filename,
                                existing.original_file_name,
                                existing_url,
                                if needs_live_link {
                                    "라이브포토 영상 연결 예정"
                                } else {
                                    "라이브포토 영상 이미 보유"
                                },
                                broken_url
                            ),
                        )
                        .await;
                } else {
                    let broken_detail = immich_target
                        .get_asset_detail(&asset.id)
                        .await
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| asset.clone());

                    if needs_live_link {
                        if let Some(ref v_guard) = extracted.video_guard {
                            let vid_name = extracted
                                .video_filename
                                .as_deref()
                                .unwrap_or("livephoto.mov");
                            let vid_dev_id = format!("{}#extracted-livevideo", base_dev);
                            let vid_id = match immich_target
                                .upload_asset_with_options(
                                    v_guard.path(),
                                    vid_name,
                                    &vid_dev_id,
                                    &created_at,
                                    &modified_at,
                                    None,
                                )
                                .await
                            {
                                Ok((vid_id, _)) => vid_id,
                                Err(err) => {
                                    state
                                        .emit_event(
                                            "repair",
                                            JobState::Syncing,
                                            Some(filename.clone()),
                                            format!(
                                                "[{}] 라이브포토 영상 업로드 실패로 휴지통 이동 보류 ({}): {}",
                                                mode_tag, filename, err
                                            ),
                                        )
                                        .await;
                                    failed_count += 1;
                                    state.processed_files.fetch_add(1, Ordering::SeqCst);
                                    continue;
                                }
                            };
                            if is_valid_immich_asset_id_candidate(Some(&vid_id)).is_none() {
                                state
                                    .emit_event(
                                        "repair",
                                        JobState::Syncing,
                                        Some(filename.clone()),
                                        format!(
                                            "[{}] 유효한 라이브포토 영상 ID를 받지 못해 휴지통 이동 보류 ({})",
                                            mode_tag, filename
                                        ),
                                    )
                                    .await;
                                failed_count += 1;
                                state.processed_files.fetch_add(1, Ordering::SeqCst);
                                continue;
                            }
                            if let Err(err) = immich_target
                                .link_live_photo_video(&existing.id, &vid_id)
                                .await
                            {
                                state
                                    .emit_event(
                                        "repair",
                                        JobState::Syncing,
                                        Some(filename.clone()),
                                        format!(
                                            "[{}] 라이브포토 영상 연결 실패로 휴지통 이동 보류 ({}): {}",
                                            mode_tag, filename, err
                                        ),
                                    )
                                    .await;
                                failed_count += 1;
                                state.processed_files.fetch_add(1, Ordering::SeqCst);
                                continue;
                            }
                        }
                    }

                    if let Err(err) = transfer_broken_asset_metadata_and_albums(
                        &immich_target,
                        &broken_detail,
                        &existing.id,
                        Some(&existing),
                    )
                    .await
                    {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(filename.clone()),
                                format!(
                                    "[{}] 메타데이터/앨범 승계 실패로 휴지통 이동 보류 ({}): {}",
                                    mode_tag, filename, err
                                ),
                            )
                            .await;
                        failed_count += 1;
                        state.processed_files.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }

                    if let Err(err) = immich_target
                        .trash_assets(std::slice::from_ref(&asset.id))
                        .await
                    {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(filename.clone()),
                                format!(
                                    "[{}] 깨진 ZIP 자산 휴지통 이동 실패 ({}): {}",
                                    mode_tag, filename, err
                                ),
                            )
                            .await;
                        failed_count += 1;
                        state.processed_files.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }

                    let broken_dev_ids: Vec<String> =
                        broken_detail.device_asset_id.clone().into_iter().collect();
                    let broken_hashes: Vec<String> =
                        decode_immich_checksum_to_hex(broken_detail.checksum.as_deref())
                            .into_iter()
                            .collect();
                    if let Err(err) = state
                        .db
                        .remap_merged_immich_assets_with_hashes(
                            std::slice::from_ref(&asset.id),
                            &broken_dev_ids,
                            &broken_hashes,
                            &existing.id,
                            Some(&extracted.image_sha1),
                        )
                        .await
                    {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(filename.clone()),
                                format!(
                                    "[{}] 로컬 DB 매핑 갱신 경고 ({}): {}",
                                    mode_tag, filename, err
                                ),
                            )
                            .await;
                    }

                    recovered_zip_duplicate += 1;
                    state
                        .emit_event(
                            "repair",
                            JobState::Syncing,
                            Some(filename.clone()),
                            format!(
                                "[{}] ZIP 위장 라이브포토 정리 완료: {} → 기존 정상본({}) 연결 후 깨진 자산 휴지통 이동",
                                mode_tag, filename, existing_url
                            ),
                        )
                        .await;
                }
            } else {
                let img_mb = extracted.image_size as f64 / (1024.0 * 1024.0);
                let vid_note = extracted
                    .video_filename
                    .as_deref()
                    .map(|v| format!(" + {}", v))
                    .unwrap_or_default();

                if dry_run {
                    recovered_zip_created += 1;
                    state
                        .emit_event(
                            "repair",
                            JobState::Syncing,
                            Some(filename.clone()),
                            format!(
                                "[{}] ZIP 위장 라이브포토(신규 복구): {} → {}{} ({:.2} MB) 재업로드 후 깨진 자산({}) 휴지통 이동 예정",
                                mode_tag,
                                filename,
                                extracted.image_filename,
                                vid_note,
                                img_mb,
                                broken_url
                            ),
                        )
                        .await;
                } else {
                    let broken_detail = immich_target
                        .get_asset_detail(&asset.id)
                        .await
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| asset.clone());

                    let outcome = match immich_target
                        .upload_live_photo(&extracted, &extracted_dev_id, &created_at, &modified_at)
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
                                        "[{}] ZIP 추출 자산 업로드 실패로 기존 자산 유지 ({}): {}",
                                        mode_tag, filename, err
                                    ),
                                )
                                .await;
                            failed_count += 1;
                            state.processed_files.fetch_add(1, Ordering::SeqCst);
                            continue;
                        }
                    };

                    let new_id = outcome.remote_target_id;
                    if is_valid_immich_asset_id_candidate(Some(&new_id)).is_none()
                        || new_id == asset.id
                    {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(filename.clone()),
                                format!(
                                    "[{}] 유효한 신규 Immich 자산 ID를 받지 못해 기존 자산 삭제 보류 ({}): {}",
                                    mode_tag, filename, new_id
                                ),
                            )
                            .await;
                        failed_count += 1;
                        state.processed_files.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }

                    if let Err(err) = transfer_broken_asset_metadata_and_albums(
                        &immich_target,
                        &broken_detail,
                        &new_id,
                        None,
                    )
                    .await
                    {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(filename.clone()),
                                format!(
                                    "[{}] 메타데이터/앨범 승계 실패로 기존 자산 휴지통 이동 보류 ({}): {}",
                                    mode_tag, filename, err
                                ),
                            )
                            .await;
                        failed_count += 1;
                        state.processed_files.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }

                    if let Err(err) = immich_target
                        .trash_assets(std::slice::from_ref(&asset.id))
                        .await
                    {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(filename.clone()),
                                format!(
                                    "[{}] 신규 복구 완료했으나 깨진 자산 휴지통 이동 실패 ({}): {}",
                                    mode_tag, filename, err
                                ),
                            )
                            .await;
                        failed_count += 1;
                        state.processed_files.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }

                    let broken_dev_ids: Vec<String> =
                        broken_detail.device_asset_id.clone().into_iter().collect();
                    let broken_hashes: Vec<String> =
                        decode_immich_checksum_to_hex(broken_detail.checksum.as_deref())
                            .into_iter()
                            .collect();
                    if let Err(err) = state
                        .db
                        .remap_merged_immich_assets_with_hashes(
                            std::slice::from_ref(&asset.id),
                            &broken_dev_ids,
                            &broken_hashes,
                            &new_id,
                            Some(&extracted.image_sha1),
                        )
                        .await
                    {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(filename.clone()),
                                format!(
                                    "[{}] 로컬 DB 매핑 갱신 경고 ({}): {}",
                                    mode_tag, filename, err
                                ),
                            )
                            .await;
                    }

                    recovered_zip_created += 1;
                    let new_url = format!("{}/photos/{}", immich_web_base, new_id);
                    state
                        .emit_event(
                            "repair",
                            JobState::Syncing,
                            Some(extracted.image_filename.clone()),
                            format!(
                                "[{}] ZIP 위장 라이브포토 복구 완료: {} → {}{} (확인: {})",
                                mode_tag, filename, extracted.image_filename, vid_note, new_url
                            ),
                        )
                        .await;
                }
            }

            state.processed_files.fetch_add(1, Ordering::SeqCst);
            continue;
        }

        let reason = non_zip_reason.unwrap_or_else(|| thumb_reason.clone());
        if let Some(healthy) = find_healthy_duplicate(&asset, &healthy_by_stem) {
            let healthy_url = format!("{}/photos/{}", immich_web_base, healthy.id);
            if dry_run {
                trashed_non_zip_dup += 1;
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(filename.clone()),
                        format!(
                            "[{}] 로딩 오류 중복본: {} ({}) | 정상본: {} ({}) | 깨진 자산({}) 휴지통 이동 예정",
                            mode_tag,
                            filename,
                            reason,
                            healthy.original_file_name,
                            healthy_url,
                            broken_url
                        ),
                    )
                    .await;
            } else {
                let broken_detail = immich_target
                    .get_asset_detail(&asset.id)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| asset.clone());
                let healthy_detail = match immich_target.get_asset_detail(&healthy.id).await {
                    Ok(Some(d)) if !d.is_trashed => d,
                    Ok(_) => {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(filename.clone()),
                                format!(
                                    "[{}] 정상 중복본이 휴지통/미존재 상태여서 삭제 보류 ({})",
                                    mode_tag, filename
                                ),
                            )
                            .await;
                        failed_count += 1;
                        state.processed_files.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                    Err(err) => {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(filename.clone()),
                                format!(
                                    "[{}] 정상 중복본 상세 조회 실패로 삭제 보류 ({}): {}",
                                    mode_tag, filename, err
                                ),
                            )
                            .await;
                        failed_count += 1;
                        state.processed_files.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                };

                if let Err(err) = transfer_broken_asset_metadata_and_albums(
                    &immich_target,
                    &broken_detail,
                    &healthy_detail.id,
                    Some(&healthy_detail),
                )
                .await
                {
                    state
                        .emit_event(
                            "repair",
                            JobState::Syncing,
                            Some(filename.clone()),
                            format!(
                                "[{}] 메타데이터/앨범 승계 실패로 휴지통 이동 보류 ({}): {}",
                                mode_tag, filename, err
                            ),
                        )
                        .await;
                    failed_count += 1;
                    state.processed_files.fetch_add(1, Ordering::SeqCst);
                    continue;
                }

                if let Err(err) = immich_target
                    .trash_assets(std::slice::from_ref(&asset.id))
                    .await
                {
                    state
                        .emit_event(
                            "repair",
                            JobState::Syncing,
                            Some(filename.clone()),
                            format!(
                                "[{}] 오류 중복본 휴지통 이동 실패 ({}): {}",
                                mode_tag, filename, err
                            ),
                        )
                        .await;
                    failed_count += 1;
                    state.processed_files.fetch_add(1, Ordering::SeqCst);
                    continue;
                }

                let broken_dev_ids: Vec<String> =
                    broken_detail.device_asset_id.clone().into_iter().collect();
                let broken_hashes: Vec<String> =
                    decode_immich_checksum_to_hex(broken_detail.checksum.as_deref())
                        .into_iter()
                        .collect();
                let healthy_sha1 =
                    decode_immich_checksum_to_hex(healthy_detail.checksum.as_deref());
                if let Err(err) = state
                    .db
                    .remap_merged_immich_assets_with_hashes(
                        std::slice::from_ref(&asset.id),
                        &broken_dev_ids,
                        &broken_hashes,
                        &healthy_detail.id,
                        healthy_sha1.as_deref(),
                    )
                    .await
                {
                    state
                        .emit_event(
                            "repair",
                            JobState::Syncing,
                            Some(filename.clone()),
                            format!(
                                "[{}] 로컬 DB 매핑 갱신 경고 ({}): {}",
                                mode_tag, filename, err
                            ),
                        )
                        .await;
                }

                trashed_non_zip_dup += 1;
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(filename.clone()),
                        format!(
                            "[{}] 로딩 오류 중복본 휴지통 이동 완료: {} → 정상본({}) 유지",
                            mode_tag, filename, healthy_url
                        ),
                    )
                    .await;
            }
        } else if dry_run {
            staged_standalone_broken += 1;
            state
                .emit_event(
                    "repair",
                    JobState::Syncing,
                    Some(filename.clone()),
                    format!(
                        "[{}] 단독 로딩 오류(삭제 안 함): {} ({}) | {} → 'broken-assets' 앨범 수집 예정",
                        mode_tag, filename, reason, broken_url
                    ),
                )
                .await;
        } else {
            let album_id = match broken_album_id {
                Some(ref aid) => aid.clone(),
                None => match immich_target.ensure_album_by_name("broken-assets").await {
                    Ok(aid) => {
                        broken_album_id = Some(aid.clone());
                        aid
                    }
                    Err(err) => {
                        state
                            .emit_event(
                                "repair",
                                JobState::Syncing,
                                Some(filename.clone()),
                                format!(
                                    "[{}] 'broken-assets' 앨범 생성/조회 실패 ({}): {}",
                                    mode_tag, filename, err
                                ),
                            )
                            .await;
                        failed_count += 1;
                        state.processed_files.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                },
            };

            if let Err(err) = immich_target
                .add_assets_to_album(&album_id, std::slice::from_ref(&asset.id))
                .await
            {
                state
                    .emit_event(
                        "repair",
                        JobState::Syncing,
                        Some(filename.clone()),
                        format!(
                            "[{}] 'broken-assets' 앨범 추가 실패 ({}): {}",
                            mode_tag, filename, err
                        ),
                    )
                    .await;
                failed_count += 1;
                state.processed_files.fetch_add(1, Ordering::SeqCst);
                continue;
            }

            staged_standalone_broken += 1;
            state
                .emit_event(
                    "repair",
                    JobState::Syncing,
                    Some(filename.clone()),
                    format!(
                        "[{}] 단독 로딩 오류 격리 완료('broken-assets' 앨범): {} ({}) | 확인: {}",
                        mode_tag, filename, reason, broken_url
                    ),
                )
                .await;
        }

        state.processed_files.fetch_add(1, Ordering::SeqCst);
    }

    let handled_total = recovered_zip_created
        + recovered_zip_duplicate
        + trashed_non_zip_dup
        + staged_standalone_broken;
    let action_label = if dry_run { "예정" } else { "완료" };
    state
        .finish_job(
            "repair",
            format!(
                "[{}] {} — 전체 {}건 스캔, 오류 확정 {}건 | ZIP 신규복구 {}건 · ZIP 기존연결 {}건 · 오류중복 휴지통 {}건 · 단독격리(broken-assets) {}건 · 실패/보류 {}건",
                mode_tag,
                action_label,
                scanned_total,
                total_broken,
                recovered_zip_created,
                recovered_zip_duplicate,
                trashed_non_zip_dup,
                staged_standalone_broken,
                failed_count
            ),
        )
        .await;

    Ok(handled_total)
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

    #[tokio::test]
    async fn test_repair_immich_broken_assets_requires_configured_immich() {
        let db = CheckpointDB::connect(":memory:").await.unwrap();
        let cfg = AppConfig {
            immich_url: String::new(),
            immich_api_key: String::new(),
            ..Default::default()
        };
        let state = AppState::new(db, cfg.clone());

        let res = run_repair_immich_broken_assets(state, cfg, true).await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Immich 서버 URL 또는 API Key가 설정되지 않았습니다"));
    }
}
