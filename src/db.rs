use crate::router::{FileStatus, SyncResultKind, TargetRouter, TargetType};
use anyhow::{anyhow, Result};
use chrono::Utc;
use sea_orm::entity::prelude::*;
use sea_orm::sea_query::{Expr, ExprTrait, Index, OnConflict};
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, FromQueryResult, QueryOrder, QuerySelect,
    Schema, Set, SqlxSqliteConnector, Statement, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::path::Path;
use std::str::FromStr;

pub const SCAN_STATUS_PENDING: &str = "PENDING";
pub const SCAN_STATUS_SCANNED: &str = "SCANNED";

fn disambiguate_filename(filename: &str, resource_id: &str) -> String {
    let safe_id: String = resource_id
        .chars()
        .map(|c| {
            if matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect();
    match filename.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() => {
            format!("{} ({}).{}", stem, safe_id, ext)
        }
        _ => format!("{} ({})", filename, safe_id),
    }
}

pub mod folder_entity {
    use sea_orm::entity::prelude::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "folders")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub source_type: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub folder_id: String,
        pub path: String,
        pub last_cursor: Option<String>,
        pub scan_status: String,
        pub updated_at: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod file_entity {
    use sea_orm::entity::prelude::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "files")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub source_type: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub resource_id: String,
        pub name: String,
        pub parent_path: String,
        pub size: i64,
        pub created_time: String,
        pub modified_time: String,
        pub target_type: String,
        pub sha1_hash: Option<String>,
        pub remote_target_id: Option<String>,
        pub sync_result: Option<String>,
        pub status: String,
        pub retry_count: i64,
        pub error_msg: Option<String>,
        pub updated_at: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub type FolderRecord = folder_entity::Model;
pub type FileRecord = file_entity::Model;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileUpsert {
    pub source_type: String,
    pub resource_id: String,
    pub name: String,
    pub parent_path: String,
    pub size: i64,
    pub created_time: String,
    pub modified_time: String,
    pub target_type: String,
    pub sha1_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureRecord {
    pub source_type: String,
    pub resource_id: String,
    pub name: String,
    pub parent_path: String,
    pub target_type: String,
    pub retry_count: i64,
    pub error_msg: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncStats {
    pub pending_folders: i64,
    pub scanned_folders: i64,
    pub pending_files: i64,
    pub pending_bytes: i64,
    pub uploaded_immich_created: i64,
    pub uploaded_immich_duplicate: i64,
    pub uploaded_nextcloud: i64,
    pub uploaded_bytes: i64,
    pub deleted_files: i64,
    pub deleted_bytes: i64,
    pub failed_files: i64,
    pub total_files: i64,
}

#[derive(Debug, FromQueryResult)]
struct FolderStatGroup {
    scan_status: String,
    cnt: i64,
}

#[derive(Debug, FromQueryResult)]
struct FileStatGroup {
    status: String,
    target_type: String,
    sync_result: Option<String>,
    cnt: i64,
    total_bytes: Option<i64>,
}

#[derive(Clone)]
pub struct CheckpointDB {
    conn: DatabaseConnection,
}

impl CheckpointDB {
    pub async fn connect(db_path: &str) -> Result<Self> {
        if db_path != ":memory:" {
            if let Some(parent) = Path::new(db_path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }
        }

        let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await?;

        let conn = SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);
        let db = Self { conn };
        db.init_schema().await?;
        Ok(db)
    }

    async fn init_schema(&self) -> Result<()> {
        let builder = self.conn.get_database_backend();
        let schema = Schema::new(builder);

        let create_folders = builder.build(
            schema
                .create_table_from_entity(folder_entity::Entity)
                .if_not_exists(),
        );
        self.conn.execute(create_folders).await?;

        let create_files = builder.build(
            schema
                .create_table_from_entity(file_entity::Entity)
                .if_not_exists(),
        );
        self.conn.execute(create_files).await?;

        let create_idx = builder.build(
            Index::create()
                .if_not_exists()
                .name("idx_files_source_status")
                .table(file_entity::Entity)
                .col(file_entity::Column::SourceType)
                .col(file_entity::Column::Status),
        );
        self.conn.execute(create_idx).await?;

        Ok(())
    }

    pub async fn get_journal_mode(&self) -> Result<String> {
        let row = self
            .conn
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA journal_mode;",
            ))
            .await?
            .ok_or_else(|| anyhow!("Failed to query SQLite journal_mode"))?;
        Ok(row.try_get_by_index::<String>(0)?)
    }

    pub async fn upsert_folder(
        &self,
        source_type: &str,
        folder_id: &str,
        path: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let active = folder_entity::ActiveModel {
            source_type: Set(source_type.to_string()),
            folder_id: Set(folder_id.to_string()),
            path: Set(path.to_string()),
            last_cursor: Set(None),
            scan_status: Set(SCAN_STATUS_PENDING.to_string()),
            updated_at: Set(now),
        };

        folder_entity::Entity::insert(active)
            .on_conflict(
                OnConflict::columns([
                    folder_entity::Column::SourceType,
                    folder_entity::Column::FolderId,
                ])
                .update_columns([
                    folder_entity::Column::Path,
                    folder_entity::Column::LastCursor,
                    folder_entity::Column::ScanStatus,
                    folder_entity::Column::UpdatedAt,
                ])
                .to_owned(),
            )
            .exec(&self.conn)
            .await?;

        Ok(())
    }

    pub async fn update_folder_cursor(
        &self,
        source_type: &str,
        folder_id: &str,
        cursor: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        folder_entity::Entity::update_many()
            .col_expr(
                folder_entity::Column::LastCursor,
                Expr::value(cursor.map(str::to_string)),
            )
            .col_expr(folder_entity::Column::UpdatedAt, Expr::value(now))
            .filter(folder_entity::Column::SourceType.eq(source_type))
            .filter(folder_entity::Column::FolderId.eq(folder_id))
            .exec(&self.conn)
            .await?;
        Ok(())
    }

    pub async fn mark_folder_scanned(&self, source_type: &str, folder_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        folder_entity::Entity::update_many()
            .col_expr(
                folder_entity::Column::ScanStatus,
                Expr::value(SCAN_STATUS_SCANNED),
            )
            .col_expr(
                folder_entity::Column::LastCursor,
                Expr::value(Option::<String>::None),
            )
            .col_expr(folder_entity::Column::UpdatedAt, Expr::value(now))
            .filter(folder_entity::Column::SourceType.eq(source_type))
            .filter(folder_entity::Column::FolderId.eq(folder_id))
            .exec(&self.conn)
            .await?;
        Ok(())
    }

    pub async fn get_pending_folders(&self, source_type: &str) -> Result<Vec<FolderRecord>> {
        let rows = folder_entity::Entity::find()
            .filter(folder_entity::Column::SourceType.eq(source_type))
            .filter(folder_entity::Column::ScanStatus.eq(SCAN_STATUS_PENDING))
            .order_by_asc(folder_entity::Column::Path)
            .all(&self.conn)
            .await?;
        Ok(rows)
    }

    /// Prepares a scan cycle for `source_type`.
    /// Returns `(is_resuming, scan_started_at)`:
    /// - If `PENDING` folders exist from a previously paused scan, resumes from them (`is_resuming = true`).
    /// - Otherwise, starts a fresh incremental scan from `"root"` (`is_resuming = false`).
    pub async fn prepare_scan(&self, source_type: &str) -> Result<(bool, String)> {
        let pending = self.get_pending_folders(source_type).await?;
        let scan_started_at = Utc::now().to_rfc3339();
        if !pending.is_empty() {
            return Ok((true, scan_started_at));
        }

        self.upsert_folder(source_type, "root", "/").await?;
        Ok((false, scan_started_at))
    }

    pub async fn upsert_files(&self, files: &[FileUpsert]) -> Result<usize> {
        if files.is_empty() {
            return Ok(0);
        }

        let now = Utc::now().to_rfc3339();
        let txn = self.conn.begin().await?;

        for f in files {
            let colliding = file_entity::Entity::find()
                .filter(file_entity::Column::SourceType.eq(&f.source_type))
                .filter(file_entity::Column::ParentPath.eq(&f.parent_path))
                .filter(file_entity::Column::Name.eq(&f.name))
                .filter(file_entity::Column::ResourceId.ne(&f.resource_id))
                .one(&txn)
                .await?;

            let effective_name = if colliding.is_some() {
                disambiguate_filename(&f.name, &f.resource_id)
            } else {
                f.name.clone()
            };

            let existing =
                file_entity::Entity::find_by_id((f.source_type.clone(), f.resource_id.clone()))
                    .one(&txn)
                    .await?;

            match existing {
                None => {
                    let active = file_entity::ActiveModel {
                        source_type: Set(f.source_type.clone()),
                        resource_id: Set(f.resource_id.clone()),
                        name: Set(effective_name),
                        parent_path: Set(f.parent_path.clone()),
                        size: Set(f.size),
                        created_time: Set(f.created_time.clone()),
                        modified_time: Set(f.modified_time.clone()),
                        target_type: Set(f.target_type.clone()),
                        sha1_hash: Set(f.sha1_hash.clone()),
                        remote_target_id: Set(None),
                        sync_result: Set(None),
                        status: Set(FileStatus::Pending.as_str().to_string()),
                        retry_count: Set(0),
                        error_msg: Set(None),
                        updated_at: Set(now.clone()),
                    };
                    file_entity::Entity::insert(active).exec(&txn).await?;
                }
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
            }
        }

        txn.commit().await?;
        Ok(files.len())
    }

    /// Reconciles items that were deleted directly by the user on the remote cloud
    /// (i.e. their `updated_at` is older than `scan_started_at` after a full scan completes).
    /// - NAS (Immich/Nextcloud) files are NEVER deleted.
    /// - Unbacked (`PENDING` / `FAILED`) files missing on remote are removed from the sync queue.
    /// - Already backed-up (`UPLOADED`) files missing on remote are marked `DELETED` so they are
    ///   excluded from future sync and remote-trash deletion while keeping NAS backup stats intact.
    pub async fn reconcile_missing_remote_items(
        &self,
        source_type: &str,
        scan_started_at: &str,
    ) -> Result<(u64, u64)> {
        let now = Utc::now().to_rfc3339();
        let txn = self.conn.begin().await?;

        let removed_unbacked = file_entity::Entity::delete_many()
            .filter(file_entity::Column::SourceType.eq(source_type))
            .filter(file_entity::Column::UpdatedAt.lt(scan_started_at))
            .filter(
                file_entity::Column::Status
                    .is_in([FileStatus::Pending.as_str(), FileStatus::Failed.as_str()]),
            )
            .exec(&txn)
            .await?
            .rows_affected;

        let excluded_uploaded = file_entity::Entity::update_many()
            .col_expr(
                file_entity::Column::Status,
                Expr::value(FileStatus::Deleted.as_str()),
            )
            .col_expr(file_entity::Column::UpdatedAt, Expr::value(now))
            .filter(file_entity::Column::SourceType.eq(source_type))
            .filter(file_entity::Column::UpdatedAt.lt(scan_started_at))
            .filter(file_entity::Column::Status.eq(FileStatus::Uploaded.as_str()))
            .exec(&txn)
            .await?
            .rows_affected;

        folder_entity::Entity::delete_many()
            .filter(folder_entity::Column::SourceType.eq(source_type))
            .filter(folder_entity::Column::UpdatedAt.lt(scan_started_at))
            .exec(&txn)
            .await?;

        txn.commit().await?;
        Ok((removed_unbacked, excluded_uploaded))
    }

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

    pub async fn mark_file_uploaded(
        &self,
        source_type: &str,
        resource_id: &str,
        sha1_hash: &str,
        remote_target_id: &str,
        sync_result: &str,
    ) -> Result<()> {
        self.mark_file_uploaded_with_size(
            source_type,
            resource_id,
            sha1_hash,
            remote_target_id,
            sync_result,
            None,
        )
        .await
    }

    pub async fn mark_file_uploaded_with_size(
        &self,
        source_type: &str,
        resource_id: &str,
        sha1_hash: &str,
        remote_target_id: &str,
        sync_result: &str,
        actual_size: Option<i64>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut update = file_entity::Entity::update_many()
            .col_expr(
                file_entity::Column::Status,
                Expr::value(FileStatus::Uploaded.as_str()),
            )
            .col_expr(
                file_entity::Column::Sha1Hash,
                Expr::value(Some(sha1_hash.to_string())),
            )
            .col_expr(
                file_entity::Column::RemoteTargetId,
                Expr::value(Some(remote_target_id.to_string())),
            )
            .col_expr(
                file_entity::Column::SyncResult,
                Expr::value(Some(sync_result.to_string())),
            )
            .col_expr(
                file_entity::Column::ErrorMsg,
                Expr::value(Option::<String>::None),
            )
            .col_expr(file_entity::Column::UpdatedAt, Expr::value(now));

        if let Some(sz) = actual_size.filter(|&s| s > 0) {
            update = update.col_expr(file_entity::Column::Size, Expr::value(sz));
        }

        update
            .filter(file_entity::Column::SourceType.eq(source_type))
            .filter(file_entity::Column::ResourceId.eq(resource_id))
            .exec(&self.conn)
            .await?;
        Ok(())
    }

    pub async fn mark_file_failed(
        &self,
        source_type: &str,
        resource_id: &str,
        error_msg: &str,
        max_retries: i64,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let next_retry = Expr::col(file_entity::Column::RetryCount).add(1);
        let next_status = Expr::case(
            next_retry.clone().gte(max_retries),
            FileStatus::Failed.as_str(),
        )
        .finally(FileStatus::Pending.as_str());

        file_entity::Entity::update_many()
            .col_expr(file_entity::Column::RetryCount, next_retry)
            .col_expr(file_entity::Column::Status, next_status.into())
            .col_expr(
                file_entity::Column::ErrorMsg,
                Expr::value(Some(error_msg.to_string())),
            )
            .col_expr(file_entity::Column::UpdatedAt, Expr::value(now))
            .filter(file_entity::Column::SourceType.eq(source_type))
            .filter(file_entity::Column::ResourceId.eq(resource_id))
            .exec(&self.conn)
            .await?;
        Ok(())
    }

    pub async fn reset_failed_files(&self, source_type: &str) -> Result<u64> {
        let now = Utc::now().to_rfc3339();
        let res = file_entity::Entity::update_many()
            .col_expr(
                file_entity::Column::Status,
                Expr::value(FileStatus::Pending.as_str()),
            )
            .col_expr(file_entity::Column::RetryCount, Expr::value(0_i64))
            .col_expr(
                file_entity::Column::ErrorMsg,
                Expr::value(Option::<String>::None),
            )
            .col_expr(file_entity::Column::UpdatedAt, Expr::value(now))
            .filter(file_entity::Column::SourceType.eq(source_type))
            .filter(file_entity::Column::Status.eq(FileStatus::Failed.as_str()))
            .exec(&self.conn)
            .await?;
        Ok(res.rows_affected)
    }

    pub async fn get_uploaded_files_for_delete(
        &self,
        source_type: &str,
        limit: i64,
    ) -> Result<Vec<FileRecord>> {
        let rows = file_entity::Entity::find()
            .filter(file_entity::Column::SourceType.eq(source_type))
            .filter(file_entity::Column::Status.eq(FileStatus::Uploaded.as_str()))
            .order_by_asc(file_entity::Column::UpdatedAt)
            .order_by_asc(file_entity::Column::ResourceId)
            .limit(Some(limit.max(0) as u64))
            .all(&self.conn)
            .await?;
        Ok(rows)
    }

    pub async fn mark_file_deleted(&self, source_type: &str, resource_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        file_entity::Entity::update_many()
            .col_expr(
                file_entity::Column::Status,
                Expr::value(FileStatus::Deleted.as_str()),
            )
            .col_expr(file_entity::Column::UpdatedAt, Expr::value(now))
            .filter(file_entity::Column::SourceType.eq(source_type))
            .filter(file_entity::Column::ResourceId.eq(resource_id))
            .exec(&self.conn)
            .await?;
        Ok(())
    }

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

    pub async fn get_stats(&self, source_type: &str) -> Result<SyncStats> {
        let mut stats = SyncStats::default();

        let folder_groups = folder_entity::Entity::find()
            .select_only()
            .column(folder_entity::Column::ScanStatus)
            .column_as(folder_entity::Column::FolderId.count(), "cnt")
            .filter(folder_entity::Column::SourceType.eq(source_type))
            .group_by(folder_entity::Column::ScanStatus)
            .into_model::<FolderStatGroup>()
            .all(&self.conn)
            .await?;

        for row in folder_groups {
            match row.scan_status.as_str() {
                SCAN_STATUS_PENDING => stats.pending_folders += row.cnt,
                SCAN_STATUS_SCANNED => stats.scanned_folders += row.cnt,
                _ => {}
            }
        }

        let file_groups = file_entity::Entity::find()
            .select_only()
            .column(file_entity::Column::Status)
            .column(file_entity::Column::TargetType)
            .column(file_entity::Column::SyncResult)
            .column_as(file_entity::Column::ResourceId.count(), "cnt")
            .column_as(file_entity::Column::Size.sum(), "total_bytes")
            .filter(file_entity::Column::SourceType.eq(source_type))
            .group_by(file_entity::Column::Status)
            .group_by(file_entity::Column::TargetType)
            .group_by(file_entity::Column::SyncResult)
            .into_model::<FileStatGroup>()
            .all(&self.conn)
            .await?;

        for row in file_groups {
            let cnt = row.cnt;
            let bytes = row.total_bytes.unwrap_or(0);
            let sync_res = row.sync_result.as_deref().unwrap_or("");
            let target = TargetType::from_str_opt(&row.target_type);

            stats.total_files += cnt;

            match row.status.as_str() {
                s if s == FileStatus::Pending.as_str() => {
                    stats.pending_files += cnt;
                    stats.pending_bytes += bytes;
                }
                s if s == FileStatus::Uploaded.as_str() => {
                    stats.uploaded_bytes += bytes;
                    Self::accumulate_target_counts(&mut stats, target, sync_res, cnt);
                }
                s if s == FileStatus::Deleted.as_str() => {
                    stats.deleted_files += cnt;
                    stats.deleted_bytes += bytes;
                    Self::accumulate_target_counts(&mut stats, target, sync_res, cnt);
                }
                s if s == FileStatus::Failed.as_str() => {
                    stats.failed_files += cnt;
                }
                _ => {}
            }
        }

        Ok(stats)
    }

    fn accumulate_target_counts(
        stats: &mut SyncStats,
        target: Option<TargetType>,
        sync_res: &str,
        cnt: i64,
    ) {
        match target {
            Some(TargetType::Immich) => {
                if sync_res == SyncResultKind::Duplicate.as_str() {
                    stats.uploaded_immich_duplicate += cnt;
                } else {
                    stats.uploaded_immich_created += cnt;
                }
            }
            Some(TargetType::Nextcloud) => {
                stats.uploaded_nextcloud += cnt;
            }
            None => {}
        }
    }

    pub async fn get_recent_failures(
        &self,
        source_type: &str,
        limit: i64,
    ) -> Result<Vec<FailureRecord>> {
        let rows = file_entity::Entity::find()
            .filter(file_entity::Column::SourceType.eq(source_type))
            .filter(file_entity::Column::Status.eq(FileStatus::Failed.as_str()))
            .order_by_desc(file_entity::Column::UpdatedAt)
            .limit(Some(limit.max(0) as u64))
            .all(&self.conn)
            .await?;

        Ok(rows
            .into_iter()
            .map(|r| FailureRecord {
                source_type: r.source_type,
                resource_id: r.resource_id,
                name: r.name,
                parent_path: r.parent_path,
                target_type: r.target_type,
                retry_count: r.retry_count,
                error_msg: r.error_msg.unwrap_or_else(|| "알 수 없는 오류".to_string()),
                updated_at: r.updated_at,
            })
            .collect())
    }

    pub async fn get_nextcloud_repair_candidates(&self) -> Result<Vec<FileRecord>> {
        let rows = file_entity::Entity::find()
            .filter(file_entity::Column::TargetType.eq(TargetType::Nextcloud.as_str()))
            .filter(
                file_entity::Column::Status
                    .is_in([FileStatus::Uploaded.as_str(), FileStatus::Deleted.as_str()]),
            )
            .order_by_asc(file_entity::Column::CreatedTime)
            .order_by_asc(file_entity::Column::ResourceId)
            .all(&self.conn)
            .await?;

        Ok(rows
            .into_iter()
            .filter(|r| {
                TargetRouter::is_livephoto_archive_name(&r.name)
                    || TargetRouter::has_null_extension(&r.name)
            })
            .collect())
    }

    pub async fn count_nextcloud_repair_candidates(&self) -> Result<u64> {
        Ok(self.get_nextcloud_repair_candidates().await?.len() as u64)
    }

    pub async fn find_file_for_immich_asset(
        &self,
        asset_id: &str,
        device_asset_id: Option<&str>,
        original_file_name: &str,
    ) -> Result<Option<FileRecord>> {
        let trimmed_asset = asset_id.trim();
        if !trimmed_asset.is_empty() {
            if let Some(found) = file_entity::Entity::find()
                .filter(file_entity::Column::RemoteTargetId.eq(trimmed_asset))
                .one(&self.conn)
                .await?
            {
                return Ok(Some(found));
            }
        }

        if let Some(dev_id) = device_asset_id.map(str::trim).filter(|s| !s.is_empty()) {
            let clean_dev_id = dev_id.split_once('#').map(|(base, _)| base).unwrap_or(dev_id);
            if let Some(found) = file_entity::Entity::find()
                .filter(file_entity::Column::ResourceId.eq(clean_dev_id))
                .one(&self.conn)
                .await?
            {
                return Ok(Some(found));
            }
        }

        let trimmed_name = original_file_name.trim();
        if !trimmed_name.is_empty() {
            if let Some(found) = file_entity::Entity::find()
                .filter(file_entity::Column::TargetType.eq(TargetType::Immich.as_str()))
                .filter(file_entity::Column::Name.eq(trimmed_name))
                .order_by_desc(file_entity::Column::UpdatedAt)
                .one(&self.conn)
                .await?
            {
                return Ok(Some(found));
            }
        }

        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_file_reclassified(
        &self,
        source_type: &str,
        resource_id: &str,
        new_name: &str,
        new_target_type: TargetType,
        new_remote_target_id: &str,
        new_sha1: &str,
        new_size: i64,
        sync_result: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut update = file_entity::Entity::update_many()
            .col_expr(file_entity::Column::Name, Expr::value(new_name.to_string()))
            .col_expr(
                file_entity::Column::TargetType,
                Expr::value(new_target_type.as_str().to_string()),
            )
            .col_expr(
                file_entity::Column::RemoteTargetId,
                Expr::value(Some(new_remote_target_id.to_string())),
            )
            .col_expr(
                file_entity::Column::Sha1Hash,
                Expr::value(Some(new_sha1.to_string())),
            )
            .col_expr(
                file_entity::Column::SyncResult,
                Expr::value(Some(sync_result.to_string())),
            )
            .col_expr(
                file_entity::Column::ErrorMsg,
                Expr::value(Option::<String>::None),
            )
            .col_expr(file_entity::Column::UpdatedAt, Expr::value(now));

        if new_size > 0 {
            update = update.col_expr(file_entity::Column::Size, Expr::value(new_size));
        }

        update
            .filter(file_entity::Column::SourceType.eq(source_type))
            .filter(file_entity::Column::ResourceId.eq(resource_id))
            .exec(&self.conn)
            .await?;
        Ok(())
    }

    pub async fn remap_merged_immich_assets(
        &self,
        loser_asset_ids: &[String],
        loser_device_asset_ids: &[String],
        winner_asset_id: &str,
        winner_sha1_hex: Option<&str>,
    ) -> Result<u64> {
        let clean_loser_ids: Vec<String> = loser_asset_ids
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let clean_dev_ids: Vec<String> = loser_device_asset_ids
            .iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.split_once('#').map(|(base, _)| base).unwrap_or(s).to_string())
            .collect();

        if clean_loser_ids.is_empty() && clean_dev_ids.is_empty() {
            return Ok(0);
        }

        let now = Utc::now().to_rfc3339();
        let mut cond = sea_orm::Condition::any();
        if !clean_loser_ids.is_empty() {
            cond = cond.add(file_entity::Column::RemoteTargetId.is_in(clean_loser_ids));
        }
        if !clean_dev_ids.is_empty() {
            cond = cond.add(file_entity::Column::ResourceId.is_in(clean_dev_ids));
        }

        let mut update = file_entity::Entity::update_many()
            .col_expr(
                file_entity::Column::RemoteTargetId,
                Expr::value(Some(winner_asset_id.to_string())),
            )
            .col_expr(
                file_entity::Column::SyncResult,
                Expr::value(Some(SyncResultKind::Duplicate.as_str().to_string())),
            )
            .col_expr(file_entity::Column::UpdatedAt, Expr::value(now));

        if let Some(sha1) = winner_sha1_hex.map(str::trim).filter(|s| !s.is_empty()) {
            update = update.col_expr(
                file_entity::Column::Sha1Hash,
                Expr::value(Some(sha1.to_string())),
            );
        }

        let res = update
            .filter(file_entity::Column::TargetType.eq(TargetType::Immich.as_str()))
            .filter(cond)
            .exec(&self.conn)
            .await?;

        Ok(res.rows_affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_remap_merged_immich_assets_updates_target_id_and_sha1() {
        let db = CheckpointDB::connect(":memory:").await.unwrap();
        db.upsert_files(&[FileUpsert {
            source_type: "mybox".to_string(),
            resource_id: "mybox_res_9112".to_string(),
            name: "IMG_9112.HEIC".to_string(),
            parent_path: "/2023/10".to_string(),
            size: 1851777,
            created_time: "2023-10-25T05:12:30Z".to_string(),
            modified_time: "2023-10-25T05:12:30Z".to_string(),
            target_type: "immich".to_string(),
            sha1_hash: Some("old_loser_sha1".to_string()),
        }])
        .await
        .unwrap();

        db.mark_file_uploaded(
            "mybox",
            "mybox_res_9112",
            "old_loser_sha1",
            "asset-mybox-loser",
            SyncResultKind::Created.as_str(),
        )
        .await
        .unwrap();

        let affected = db
            .remap_merged_immich_assets(
                &["asset-mybox-loser".to_string()],
                &["mybox_res_9112".to_string()],
                "asset-icloud-winner",
                Some("new_winner_sha1_hex"),
            )
            .await
            .unwrap();
        assert_eq!(affected, 1);

        let rows = db.get_uploaded_files_for_delete("mybox", 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].remote_target_id.as_deref(),
            Some("asset-icloud-winner")
        );
        assert_eq!(rows[0].sha1_hash.as_deref(), Some("new_winner_sha1_hex"));
        assert_eq!(rows[0].sync_result.as_deref(), Some("duplicate"));
    }
}
