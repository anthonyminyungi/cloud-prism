use crate::config::AppConfig;
use crate::db::{FileRecord, FileUpsert, SyncStats};
use crate::limiter::RateLimiter;
use crate::router::TargetRouter;
use crate::sources::gdrive::GoogleDriveProvider;
use crate::sources::mybox::MyboxProvider;
use crate::sources::takeout::GoogleTakeoutProvider;
use crate::sources::SourceProvider;
use crate::state::{AppState, JobState};
use crate::targets::immich::ImmichTarget;
use crate::targets::nextcloud::NextcloudTarget;
use anyhow::{anyhow, Result};
use futures::stream::{self, StreamExt};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct SyncPipeline {
    state: Arc<AppState>,
    config: AppConfig,
    source_name: String,
    source: Arc<dyn SourceProvider>,
    immich: Arc<ImmichTarget>,
    nextcloud: Arc<NextcloudTarget>,
}

impl SyncPipeline {
    pub fn from_config(
        state: Arc<AppState>,
        config: AppConfig,
        source_name: &str,
    ) -> Result<Self> {
        let limiter = RateLimiter::new(config.rate_limit_rps, config.max_retries);
        let normalized_source = source_name.trim().to_ascii_lowercase();

        let source: Arc<dyn SourceProvider> = match normalized_source.as_str() {
            "mybox" => Arc::new(MyboxProvider::new(
                config.mybox_api_base.clone(),
                config.mybox_pat.clone(),
                limiter.clone(),
            )),
            "gdrive" => Arc::new(GoogleDriveProvider::new(
                config.gdrive_api_base.clone(),
                config.gdrive_access_token.clone(),
                limiter.clone(),
            )),
            "takeout" => Arc::new(GoogleTakeoutProvider::new(&config.takeout_dir)),
            other => return Err(anyhow!("지원하지 않는 소스 타입입니다: {}", other)),
        };

        let immich = Arc::new(ImmichTarget::new(
            config.immich_url.clone(),
            config.immich_api_key.clone(),
            limiter.clone(),
        ));

        let nextcloud = Arc::new(NextcloudTarget::new(
            config.nextcloud_url.clone(),
            config.nextcloud_user.clone(),
            config.nextcloud_app_password.clone(),
            config.nextcloud_base_dir.clone(),
            limiter,
        ));

        Ok(Self {
            state,
            config,
            source_name: normalized_source,
            source,
            immich,
            nextcloud,
        })
    }

    pub async fn run_auto(&self) -> Result<SyncStats> {
        self.run_scan().await?;
        if self.state.is_stopping() {
            let stats = self.state.db.get_stats(&self.source_name).await?;
            self.state
                .finish_job(&self.source_name, "스캔 단계에서 안전 일시정지되었습니다.")
                .await;
            return Ok(stats);
        }
        self.run_sync().await
    }

    pub async fn run_scan(&self) -> Result<SyncStats> {
        let src = &self.source_name;
        self.state.begin_phase(src, JobState::Scanning, 0).await;
        self.state
            .emit_event(
                src,
                JobState::Scanning,
                None,
                format!("[{}] 원격 폴더 트리 및 파일 목록 스캔을 시작합니다.", src),
            )
            .await;

        self.state.db.upsert_folder(src, "root", "/").await?;

        loop {
            if self.state.is_stopping() {
                break;
            }

            let pending_folders = self.state.db.get_pending_folders(src).await?;
            if pending_folders.is_empty() {
                break;
            }

            for folder in pending_folders {
                if self.state.is_stopping() {
                    break;
                }

                let mut cursor = folder.last_cursor.clone();
                loop {
                    if self.state.is_stopping() {
                        break;
                    }

                    let page = self
                        .source
                        .list_page(&folder.folder_id, &folder.path, cursor.as_deref())
                        .await?;

                    for sub in &page.folders {
                        self.state
                            .db
                            .upsert_folder(src, &sub.folder_id, &sub.path)
                            .await?;
                    }

                    let mut upserts = Vec::new();
                    for rf in &page.files {
                        if let Some(target_type) =
                            TargetRouter::classify(&rf.name, self.config.enable_nextcloud)
                        {
                            upserts.push(FileUpsert {
                                source_type: src.clone(),
                                resource_id: rf.resource_id.clone(),
                                name: rf.name.clone(),
                                parent_path: rf.parent_path.clone(),
                                size: rf.size,
                                created_time: rf.created_time.clone(),
                                modified_time: rf.modified_time.clone(),
                                target_type: target_type.as_str().to_string(),
                                sha1_hash: rf.sha1_hash.clone(),
                            });
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
                                Some(folder.path.clone()),
                                format!(
                                    "폴더 스캔 중: {} (발견된 파일 {}건 체크포인트 등록)",
                                    folder.path, added
                                ),
                            )
                            .await;
                    }

                    if let Some(next_c) = page.next_cursor {
                        self.state
                            .db
                            .update_folder_cursor(src, &folder.folder_id, Some(&next_c))
                            .await?;
                        cursor = Some(next_c);
                    } else {
                        self.state
                            .db
                            .mark_folder_scanned(src, &folder.folder_id)
                            .await?;
                        break;
                    }
                }
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

    pub async fn run_sync(&self) -> Result<SyncStats> {
        let src = &self.source_name;
        let initial_stats = self.state.db.get_stats(src).await?;
        self.state
            .begin_phase(src, JobState::Syncing, initial_stats.pending_files as u64)
            .await;
        self.state
            .emit_event(
                src,
                JobState::Syncing,
                None,
                format!(
                    "[{}] 동기화 시작 (대기 항목: {}건, 워커 수: {})",
                    src, initial_stats.pending_files, self.config.concurrency
                ),
            )
            .await;

        let max_retries = self.config.max_retries as i64;
        let concurrency = self.config.concurrency.max(1);

        loop {
            if self.state.is_stopping() {
                break;
            }

            let batch = self
                .state
                .db
                .get_pending_files(src, max_retries, 100)
                .await?;
            if batch.is_empty() {
                break;
            }

            stream::iter(batch)
                .for_each_concurrent(concurrency, |file| async move {
                    if self.state.is_stopping() {
                        return;
                    }
                    self.sync_single_file(file).await;
                })
                .await;
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

    async fn sync_single_file(&self, file: FileRecord) {
        let src = &self.source_name;
        let max_retries = self.config.max_retries as i64;

        self.state
            .emit_event(
                src,
                JobState::Syncing,
                Some(file.name.clone()),
                format!("처리 중: {}/{}", file.parent_path.trim_end_matches('/'), file.name),
            )
            .await;

        let res: Result<(String, String, String)> = async {
            // Fast path: if target is Immich and source already provided a SHA-1 hash (e.g. Google Drive),
            // check Immich bulk-upload-check before downloading the file!
            if file.target_type == "immich" {
                if let Some(ref existing_sha1) = file.sha1_hash {
                    let check_items = vec![(file.resource_id.clone(), existing_sha1.clone())];
                    if let Ok(map) = self.immich.check_bulk_hashes(&check_items).await {
                        if let Some(Some(existing_asset_id)) = map.get(&file.resource_id) {
                            return Ok((
                                existing_sha1.clone(),
                                existing_asset_id.clone(),
                                "duplicate".to_string(),
                            ));
                        }
                    }
                }
            }

            // Stream download to tmpfs (/tmp/mybox_sync) with RAII TempFileGuard and 1-pass SHA-1
            let downloaded = self
                .source
                .download_to_temp(
                    &file.resource_id,
                    &file.name,
                    Path::new(&self.config.tmp_dir),
                )
                .await?;

            let sha1 = downloaded.sha1_hash.clone();
            let created_time = downloaded
                .created_time_override
                .as_deref()
                .unwrap_or(&file.created_time);
            let modified_time = downloaded
                .modified_time_override
                .as_deref()
                .unwrap_or(&file.modified_time);

            if file.target_type == "immich" {
                let check_items = vec![(file.resource_id.clone(), sha1.clone())];
                let map = self.immich.check_bulk_hashes(&check_items).await?;
                if let Some(Some(existing_asset_id)) = map.get(&file.resource_id) {
                    return Ok((
                        sha1,
                        existing_asset_id.clone(),
                        "duplicate".to_string(),
                    ));
                }

                let (asset_id, status) = self
                    .immich
                    .upload_asset(
                        downloaded.temp_guard.path(),
                        &file.name,
                        &file.resource_id,
                        created_time,
                        modified_time,
                    )
                    .await?;
                Ok((sha1, asset_id, status))
            } else {
                let remote_path = self
                    .nextcloud
                    .upload_file(
                        downloaded.temp_guard.path(),
                        &file.parent_path,
                        &file.name,
                        modified_time,
                    )
                    .await?;
                Ok((sha1, remote_path, "created".to_string()))
            }
        }
        .await;

        match res {
            Ok((sha1, remote_id, sync_result)) => {
                let _ = self
                    .state
                    .db
                    .mark_file_uploaded(src, &file.resource_id, &sha1, &remote_id, &sync_result)
                    .await;
                self.state.processed_files.fetch_add(1, Ordering::SeqCst);
                self.state
                    .emit_event(
                        src,
                        JobState::Syncing,
                        Some(file.name.clone()),
                        format!(
                            "이주 완료 [{} / {}]: {}",
                            file.target_type, sync_result, file.name
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

    pub async fn run_delete(&self, dry_run: bool) -> Result<SyncStats> {
        let src = &self.source_name;
        let uploaded = self.state.db.get_uploaded_files_for_delete(src, 10_000).await?;
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

        for file in uploaded {
            if self.state.is_stopping() {
                break;
            }

            let verified = if file.target_type == "immich" {
                if let Some(ref sha1) = file.sha1_hash {
                    self.immich
                        .verify_hash_exists(&file.resource_id, sha1)
                        .await
                        .unwrap_or(false)
                } else {
                    false
                }
            } else {
                if let Some(ref remote_path) = file.remote_target_id {
                    self.nextcloud
                        .check_file_exists(remote_path, file.size)
                        .await
                        .unwrap_or(false)
                } else {
                    false
                }
            };

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
            } else {
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
