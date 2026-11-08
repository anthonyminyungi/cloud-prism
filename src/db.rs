use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::Row;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderRecord {
    pub source_type: String,
    pub folder_id: String,
    pub path: String,
    pub last_cursor: Option<String>,
    pub scan_status: String,
    pub updated_at: String,
}

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
pub struct FileRecord {
    pub source_type: String,
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

#[derive(Clone)]
pub struct CheckpointDB {
    pool: SqlitePool,
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

        let db = Self { pool };
        db.init_schema().await?;
        Ok(db)
    }

    async fn init_schema(&self) -> Result<()> {
        sqlx::query("PRAGMA journal_mode=WAL;")
            .execute(&self.pool)
            .await?;
        sqlx::query("PRAGMA synchronous=NORMAL;")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS folders (
                source_type TEXT NOT NULL,
                folder_id TEXT NOT NULL,
                path TEXT NOT NULL,
                last_cursor TEXT,
                scan_status TEXT NOT NULL DEFAULT 'PENDING',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source_type, folder_id)
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS files (
                source_type TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                name TEXT NOT NULL,
                parent_path TEXT NOT NULL,
                size INTEGER NOT NULL DEFAULT 0,
                created_time TEXT NOT NULL,
                modified_time TEXT NOT NULL,
                target_type TEXT NOT NULL,
                sha1_hash TEXT,
                remote_target_id TEXT,
                sync_result TEXT,
                status TEXT NOT NULL DEFAULT 'PENDING',
                retry_count INTEGER NOT NULL DEFAULT 0,
                error_msg TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source_type, resource_id)
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_files_source_status ON files (source_type, status);",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_journal_mode(&self) -> Result<String> {
        let row = sqlx::query("PRAGMA journal_mode;")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<String, _>(0))
    }

    pub async fn get_all_settings(&self) -> Result<HashMap<String, String>> {
        let rows = sqlx::query("SELECT key, value FROM settings")
            .fetch_all(&self.pool)
            .await?;
        let mut map = HashMap::new();
        for row in rows {
            let k: String = row.get("key");
            let v: String = row.get("value");
            map.insert(k, v);
        }
        Ok(map)
    }

    pub async fn save_settings(&self, entries: &HashMap<String, String>) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        for (k, v) in entries {
            sqlx::query(
                r#"
                INSERT INTO settings (key, value, updated_at)
                VALUES (?1, ?2, ?3)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                "#,
            )
            .bind(k)
            .bind(v)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_folder(
        &self,
        source_type: &str,
        folder_id: &str,
        path: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO folders (source_type, folder_id, path, scan_status, updated_at)
            VALUES (?1, ?2, ?3, 'PENDING', ?4)
            ON CONFLICT(source_type, folder_id) DO UPDATE SET
                path = excluded.path,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(source_type)
        .bind(folder_id)
        .bind(path)
        .bind(now)
        .execute(&self.pool)
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
        sqlx::query(
            r#"
            UPDATE folders
            SET last_cursor = ?1, updated_at = ?2
            WHERE source_type = ?3 AND folder_id = ?4
            "#,
        )
        .bind(cursor)
        .bind(now)
        .bind(source_type)
        .bind(folder_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_folder_scanned(&self, source_type: &str, folder_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            UPDATE folders
            SET scan_status = 'SCANNED', last_cursor = NULL, updated_at = ?1
            WHERE source_type = ?2 AND folder_id = ?3
            "#,
        )
        .bind(now)
        .bind(source_type)
        .bind(folder_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_pending_folders(&self, source_type: &str) -> Result<Vec<FolderRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT source_type, folder_id, path, last_cursor, scan_status, updated_at
            FROM folders
            WHERE source_type = ?1 AND scan_status = 'PENDING'
            ORDER BY path ASC
            "#,
        )
        .bind(source_type)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| FolderRecord {
                source_type: r.get("source_type"),
                folder_id: r.get("folder_id"),
                path: r.get("path"),
                last_cursor: r.get("last_cursor"),
                scan_status: r.get("scan_status"),
                updated_at: r.get("updated_at"),
            })
            .collect())
    }

    pub async fn upsert_files(&self, files: &[FileUpsert]) -> Result<usize> {
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        let mut count = 0;

        for f in files {
            sqlx::query(
                r#"
                INSERT INTO files (
                    source_type, resource_id, name, parent_path, size,
                    created_time, modified_time, target_type, sha1_hash,
                    status, retry_count, updated_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'PENDING', 0, ?10)
                ON CONFLICT(source_type, resource_id) DO UPDATE SET
                    name = excluded.name,
                    parent_path = excluded.parent_path,
                    size = excluded.size,
                    modified_time = excluded.modified_time,
                    target_type = excluded.target_type,
                    sha1_hash = COALESCE(excluded.sha1_hash, files.sha1_hash),
                    updated_at = excluded.updated_at
                "#,
            )
            .bind(&f.source_type)
            .bind(&f.resource_id)
            .bind(&f.name)
            .bind(&f.parent_path)
            .bind(f.size)
            .bind(&f.created_time)
            .bind(&f.modified_time)
            .bind(&f.target_type)
            .bind(&f.sha1_hash)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
            count += 1;
        }

        tx.commit().await?;
        Ok(count)
    }

    pub async fn get_pending_files(
        &self,
        source_type: &str,
        max_retries: i64,
        limit: i64,
    ) -> Result<Vec<FileRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT source_type, resource_id, name, parent_path, size,
                   created_time, modified_time, target_type, sha1_hash,
                   remote_target_id, sync_result, status, retry_count, error_msg, updated_at
            FROM files
            WHERE source_type = ?1 AND status = 'PENDING' AND retry_count < ?2
            ORDER BY parent_path ASC, name ASC
            LIMIT ?3
            "#,
        )
        .bind(source_type)
        .bind(max_retries)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::row_to_file_record).collect())
    }

    pub async fn mark_file_uploaded(
        &self,
        source_type: &str,
        resource_id: &str,
        sha1_hash: &str,
        remote_target_id: &str,
        sync_result: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            UPDATE files
            SET status = 'UPLOADED',
                sha1_hash = ?1,
                remote_target_id = ?2,
                sync_result = ?3,
                error_msg = NULL,
                updated_at = ?4
            WHERE source_type = ?5 AND resource_id = ?6
            "#,
        )
        .bind(sha1_hash)
        .bind(remote_target_id)
        .bind(sync_result)
        .bind(now)
        .bind(source_type)
        .bind(resource_id)
        .execute(&self.pool)
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
        sqlx::query(
            r#"
            UPDATE files
            SET retry_count = retry_count + 1,
                status = CASE WHEN retry_count + 1 >= ?1 THEN 'FAILED' ELSE 'PENDING' END,
                error_msg = ?2,
                updated_at = ?3
            WHERE source_type = ?4 AND resource_id = ?5
            "#,
        )
        .bind(max_retries)
        .bind(error_msg)
        .bind(now)
        .bind(source_type)
        .bind(resource_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reset_failed_files(&self, source_type: &str) -> Result<u64> {
        let now = Utc::now().to_rfc3339();
        let res = sqlx::query(
            r#"
            UPDATE files
            SET status = 'PENDING', retry_count = 0, error_msg = NULL, updated_at = ?1
            WHERE source_type = ?2 AND status = 'FAILED'
            "#,
        )
        .bind(now)
        .bind(source_type)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    pub async fn get_uploaded_files_for_delete(
        &self,
        source_type: &str,
        limit: i64,
    ) -> Result<Vec<FileRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT source_type, resource_id, name, parent_path, size,
                   created_time, modified_time, target_type, sha1_hash,
                   remote_target_id, sync_result, status, retry_count, error_msg, updated_at
            FROM files
            WHERE source_type = ?1 AND status = 'UPLOADED'
            ORDER BY updated_at ASC
            LIMIT ?2
            "#,
        )
        .bind(source_type)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::row_to_file_record).collect())
    }

    pub async fn mark_file_deleted(&self, source_type: &str, resource_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            UPDATE files
            SET status = 'DELETED', updated_at = ?1
            WHERE source_type = ?2 AND resource_id = ?3
            "#,
        )
        .bind(now)
        .bind(source_type)
        .bind(resource_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_stats(&self, source_type: &str) -> Result<SyncStats> {
        let mut stats = SyncStats::default();

        let folder_rows = sqlx::query(
            r#"
            SELECT scan_status, COUNT(*) as cnt
            FROM folders
            WHERE source_type = ?1
            GROUP BY scan_status
            "#,
        )
        .bind(source_type)
        .fetch_all(&self.pool)
        .await?;

        for row in folder_rows {
            let status: String = row.get("scan_status");
            let cnt: i64 = row.get("cnt");
            match status.as_str() {
                "PENDING" => stats.pending_folders += cnt,
                "SCANNED" => stats.scanned_folders += cnt,
                _ => {}
            }
        }

        let file_rows = sqlx::query(
            r#"
            SELECT status, target_type, COALESCE(sync_result, '') as sync_res,
                   COUNT(*) as cnt, COALESCE(SUM(size), 0) as total_bytes
            FROM files
            WHERE source_type = ?1
            GROUP BY status, target_type, sync_res
            "#,
        )
        .bind(source_type)
        .fetch_all(&self.pool)
        .await?;

        for row in file_rows {
            let status: String = row.get("status");
            let target: String = row.get("target_type");
            let sync_res: String = row.get("sync_res");
            let cnt: i64 = row.get("cnt");
            let bytes: i64 = row.get("total_bytes");

            stats.total_files += cnt;

            match status.as_str() {
                "PENDING" => {
                    stats.pending_files += cnt;
                    stats.pending_bytes += bytes;
                }
                "UPLOADED" => {
                    stats.uploaded_bytes += bytes;
                    if target == "immich" {
                        if sync_res == "duplicate" {
                            stats.uploaded_immich_duplicate += cnt;
                        } else {
                            stats.uploaded_immich_created += cnt;
                        }
                    } else if target == "nextcloud" {
                        stats.uploaded_nextcloud += cnt;
                    }
                }
                "DELETED" => {
                    stats.deleted_files += cnt;
                    stats.deleted_bytes += bytes;
                    if target == "immich" {
                        if sync_res == "duplicate" {
                            stats.uploaded_immich_duplicate += cnt;
                        } else {
                            stats.uploaded_immich_created += cnt;
                        }
                    } else if target == "nextcloud" {
                        stats.uploaded_nextcloud += cnt;
                    }
                }
                "FAILED" => {
                    stats.failed_files += cnt;
                }
                _ => {}
            }
        }

        Ok(stats)
    }

    pub async fn get_recent_failures(
        &self,
        source_type: &str,
        limit: i64,
    ) -> Result<Vec<FailureRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT source_type, resource_id, name, parent_path, target_type,
                   retry_count, COALESCE(error_msg, 'Unknown error') as error_msg, updated_at
            FROM files
            WHERE source_type = ?1 AND status = 'FAILED'
            ORDER BY updated_at DESC
            LIMIT ?2
            "#,
        )
        .bind(source_type)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| FailureRecord {
                source_type: r.get("source_type"),
                resource_id: r.get("resource_id"),
                name: r.get("name"),
                parent_path: r.get("parent_path"),
                target_type: r.get("target_type"),
                retry_count: r.get("retry_count"),
                error_msg: r.get("error_msg"),
                updated_at: r.get("updated_at"),
            })
            .collect())
    }

    fn row_to_file_record(r: sqlx::sqlite::SqliteRow) -> FileRecord {
        FileRecord {
            source_type: r.get("source_type"),
            resource_id: r.get("resource_id"),
            name: r.get("name"),
            parent_path: r.get("parent_path"),
            size: r.get("size"),
            created_time: r.get("created_time"),
            modified_time: r.get("modified_time"),
            target_type: r.get("target_type"),
            sha1_hash: r.get("sha1_hash"),
            remote_target_id: r.get("remote_target_id"),
            sync_result: r.get("sync_result"),
            status: r.get("status"),
            retry_count: r.get("retry_count"),
            error_msg: r.get("error_msg"),
            updated_at: r.get("updated_at"),
        }
    }
}
