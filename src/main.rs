use anyhow::Result;
use clap::{Parser, Subcommand};
use comfy_table::Table;
use mybox_immich_sync::config::AppConfig;
use mybox_immich_sync::db::CheckpointDB;
use mybox_immich_sync::pipeline::{
    run_merge_immich_duplicates_with_limit, run_move_immich_album_to_nextcloud_with_limit,
    run_repair_nextcloud_anomalies_with_limit, spawn_auto_sync_scheduler, SyncPipeline,
};
use mybox_immich_sync::state::AppState;
use mybox_immich_sync::web::{build_router, verify_all_configured};
use std::net::SocketAddr;

#[derive(Parser, Debug)]
#[command(
    name = "mybox-sync",
    about = "TrueNAS SCALE Web App & CLI for Multi-Cloud (MYBOX, Google) to NAS (Immich & Nextcloud) migration"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the embedded Web UI dashboard and REST/SSE server (default)
    Serve {
        #[arg(long, env = "PORT", default_value_t = 3080)]
        port: u16,
    },
    /// Scan and sync in one pass from CLI
    Auto {
        #[arg(long, default_value = "mybox")]
        source: String,
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Scan remote folder tree and register checkpoints
    Scan {
        #[arg(long, default_value = "mybox")]
        source: String,
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Sync pending files to Immich and Nextcloud
    Sync {
        #[arg(long, default_value = "mybox")]
        source: String,
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Re-verify targets and move uploaded files to remote cloud trash
    Delete {
        #[arg(long, default_value = "mybox")]
        source: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    /// Print checkpoint status table
    Status {
        #[arg(long, default_value = "mybox")]
        source: String,
    },
    /// Post-migration repair: recover Nextcloud livephoto.zip & (null) files, move Immich album items, or smart-merge Immich duplicates
    Repair {
        #[arg(long, default_value = "to-nextcloud")]
        album: String,
        #[arg(long, default_value_t = false)]
        merge_duplicates: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long)]
        limit: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = AppConfig::from_env();
    let db = CheckpointDB::connect(&cfg.db_path).await?;
    let state = AppState::new(db, cfg.clone());

    match cli.command.unwrap_or(Commands::Serve { port: cfg.port }) {
        Commands::Serve { port } => {
            let boot_state = state.clone();
            tokio::spawn(async move {
                let _ = verify_all_configured(&boot_state).await;
            });
            spawn_auto_sync_scheduler(state.clone());
            let app = build_router(state);
            let addr = SocketAddr::from(([0, 0, 0, 0], port));
            println!("🚀 mybox-immich-sync Web UI listening on http://{}", addr);
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
        }
        Commands::Auto { source, limit } => {
            let pipeline = SyncPipeline::from_config(state, cfg, &source)?.with_limit(limit);
            let stats = pipeline.run_auto().await?;
            println!("Auto completed: {:?}", stats);
        }
        Commands::Scan { source, limit } => {
            let pipeline = SyncPipeline::from_config(state, cfg, &source)?.with_limit(limit);
            let stats = pipeline.run_scan().await?;
            println!("Scan completed: {:?}", stats);
        }
        Commands::Sync { source, limit } => {
            let pipeline = SyncPipeline::from_config(state, cfg, &source)?.with_limit(limit);
            let stats = pipeline.run_sync().await?;
            println!("Sync completed: {:?}", stats);
        }
        Commands::Delete { source, dry_run } => {
            let pipeline = SyncPipeline::from_config(state, cfg, &source)?;
            let stats = pipeline.run_delete(dry_run).await?;
            println!("Delete (dry_run={}) completed: {:?}", dry_run, stats);
        }
        Commands::Status { source } => {
            let stats = state.db.get_stats(&source).await?;
            let mut table = Table::new();
            table.set_header(vec!["항목 (Metric)", "수치 (Count)"]);
            table.add_row(vec!["소스 (Source)", &source]);
            table.add_row(vec![
                "스캔 완료 폴더 (SCANNED Folders)",
                &stats.scanned_folders.to_string(),
            ]);
            table.add_row(vec![
                "대기 파일 (PENDING Files)",
                &stats.pending_files.to_string(),
            ]);
            table.add_row(vec![
                "Immich 신규 업로드 (Created)",
                &stats.uploaded_immich_created.to_string(),
            ]);
            table.add_row(vec![
                "Immich 중복 스킵 (Duplicate)",
                &stats.uploaded_immich_duplicate.to_string(),
            ]);
            table.add_row(vec![
                "Nextcloud 업로드 (UPLOADED)",
                &stats.uploaded_nextcloud.to_string(),
            ]);
            table.add_row(vec![
                "원격 휴지통 삭제 완료 (DELETED)",
                &stats.deleted_files.to_string(),
            ]);
            table.add_row(vec!["실패 항목 (FAILED)", &stats.failed_files.to_string()]);
            println!("{table}");
        }
        Commands::Repair {
            album,
            merge_duplicates,
            dry_run,
            limit,
        } => {
            if merge_duplicates {
                let merged =
                    run_merge_immich_duplicates_with_limit(state, cfg, dry_run, limit).await?;
                println!(
                    "Duplicate smart-merge (dry_run={}) completed: groups_merged={}",
                    dry_run, merged
                );
            } else {
                let repaired =
                    run_repair_nextcloud_anomalies_with_limit(state.clone(), cfg.clone(), limit)
                        .await?;
                let moved = if cfg.enable_nextcloud {
                    run_move_immich_album_to_nextcloud_with_limit(state, cfg, &album, limit)
                        .await?
                } else {
                    0
                };
                println!(
                    "Repair completed: anomalies_repaired={}, album_moved={}",
                    repaired, moved
                );
            }
        }
    }

    Ok(())
}
