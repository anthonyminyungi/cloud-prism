use crate::sources::{
    DownloadedSourceFile, ListPage, RemoteFile, SourceProvider, TempFileGuard,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use serde_json::Value;
use sha1::{Digest, Sha1};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use zip::ZipArchive;

pub struct GoogleTakeoutProvider {
    takeout_dir: PathBuf,
}

impl GoogleTakeoutProvider {
    pub fn new(takeout_dir: impl Into<PathBuf>) -> Self {
        Self {
            takeout_dir: takeout_dir.into(),
        }
    }

    fn parse_sidecar_timestamp(json_bytes: &[u8]) -> Option<String> {
        let val: Value = serde_json::from_slice(json_bytes).ok()?;
        let ts_str = val
            .pointer("/photoTakenTime/timestamp")
            .or_else(|| val.pointer("/creationTime/timestamp"))
            .and_then(|v| v.as_str())?;
        let secs = ts_str.parse::<i64>().ok()?;
        Utc.timestamp_opt(secs, 0)
            .single()
            .map(|dt| dt.to_rfc3339())
    }
}

#[async_trait]
impl SourceProvider for GoogleTakeoutProvider {
    fn source_type(&self) -> &'static str {
        "takeout"
    }

    async fn test_connection(&self) -> Result<String> {
        if !self.takeout_dir.exists() {
            return Err(anyhow!(
                "Google Takeout 경로가 존재하지 않습니다: {}",
                self.takeout_dir.display()
            ));
        }

        let mut zip_count = 0usize;
        for entry in std::fs::read_dir(&self.takeout_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.eq_ignore_ascii_case("zip"))
                .unwrap_or(false)
            {
                zip_count += 1;
            }
        }

        Ok(format!(
            "Google Takeout 경로 확인 성공 (ZIP 아카이브 {}개 발견)",
            zip_count
        ))
    }

    async fn list_page(
        &self,
        folder_id: &str,
        _folder_path: &str,
        _cursor: Option<&str>,
    ) -> Result<ListPage> {
        let mut page = ListPage::default();
        if folder_id != "root" || !self.takeout_dir.exists() {
            return Ok(page);
        }

        for entry in std::fs::read_dir(&self.takeout_dir)? {
            let entry = entry?;
            let zip_path = entry.path();
            if !zip_path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.eq_ignore_ascii_case("zip"))
                .unwrap_or(false)
            {
                continue;
            }

            let zip_name = zip_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("takeout.zip")
                .to_string();

            let file = File::open(&zip_path)?;
            let mut archive = ZipArchive::new(file)?;

            for i in 0..archive.len() {
                let zf = archive.by_index(i)?;
                if zf.is_dir() {
                    continue;
                }
                let entry_name = zf.name().to_string();
                if entry_name.to_ascii_lowercase().ends_with(".json") {
                    continue;
                }

                let path_obj = Path::new(&entry_name);
                let filename = path_obj
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&entry_name)
                    .to_string();
                let parent = path_obj
                    .parent()
                    .and_then(|p| p.to_str())
                    .map(|p| format!("/{}", p.trim_matches('/')))
                    .unwrap_or_else(|| "/".to_string());

                let resource_id = format!("{}::{}", zip_name, entry_name);
                page.files.push(RemoteFile {
                    resource_id,
                    name: filename,
                    parent_path: parent,
                    size: zf.size() as i64,
                    created_time: "2026-01-01T00:00:00Z".to_string(),
                    modified_time: "2026-01-01T00:00:00Z".to_string(),
                    sha1_hash: None,
                });
            }
        }

        Ok(page)
    }

    async fn download_to_temp(
        &self,
        resource_id: &str,
        filename: &str,
        tmp_dir: &Path,
    ) -> Result<DownloadedSourceFile> {
        let (zip_name, entry_name) = resource_id
            .split_once("::")
            .ok_or_else(|| anyhow!("잘못된 Takeout resource_id 형식: {}", resource_id))?;

        let zip_path = self.takeout_dir.join(zip_name);
        tokio::fs::create_dir_all(tmp_dir).await?;
        let safe_name = filename.replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_");
        let temp_path = tmp_dir.join(format!("takeout_{}", safe_name));
        let temp_guard = TempFileGuard::new(temp_path);

        let file = File::open(&zip_path)?;
        let mut archive = ZipArchive::new(file)?;

        let mut hasher = Sha1::new();
        let mut total_bytes: u64 = 0;
        {
            let mut zf = archive.by_name(entry_name)?;
            let mut out = File::create(temp_guard.path())?;
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = zf.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                out.write_all(&buf[..n])?;
                total_bytes += n as u64;
            }
            out.flush()?;
        }

        // Check optional Google Takeout sidecar JSON (`<entry>.json` or `<entry>.supplemental-metadata.json`)
        let mut created_override = None;
        let sidecar_candidates = [
            format!("{}.json", entry_name),
            format!("{}.supplemental-metadata.json", entry_name),
        ];
        for cand in sidecar_candidates {
            if let Ok(mut sidecar) = archive.by_name(&cand) {
                let mut json_buf = Vec::new();
                if sidecar.read_to_end(&mut json_buf).is_ok() {
                    if let Some(ts) = Self::parse_sidecar_timestamp(&json_buf) {
                        created_override = Some(ts);
                        break;
                    }
                }
            }
        }

        let sha1_hash = hex::encode(hasher.finalize());
        Ok(DownloadedSourceFile {
            temp_guard,
            sha1_hash,
            size: total_bytes,
            created_time_override: created_override.clone(),
            modified_time_override: created_override,
        })
    }

    async fn delete_resource(&self, _resource_id: &str) -> Result<()> {
        // Local Takeout ZIP archives are not deleted remotely
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use zip::write::SimpleFileOptions;
    use zip::ZipWriter;

    #[tokio::test]
    async fn test_takeout_zip_and_sidecar_extraction() {
        let dir = TempDir::new().unwrap();
        let zip_path = dir.path().join("takeout-001.zip");
        {
            let file = File::create(&zip_path).unwrap();
            let mut zip = ZipWriter::new(file);
            let opts = SimpleFileOptions::default();

            zip.start_file("Takeout/Google Photos/2026/beach.jpg", opts)
                .unwrap();
            zip.write_all(b"fake_jpeg_bytes_12345").unwrap();

            zip.start_file("Takeout/Google Photos/2026/beach.jpg.json", opts)
                .unwrap();
            zip.write_all(br#"{"photoTakenTime": {"timestamp": "1750000000"}}"#)
                .unwrap();

            zip.finish().unwrap();
        }

        let provider = GoogleTakeoutProvider::new(dir.path());
        let conn_msg = provider.test_connection().await.unwrap();
        assert!(conn_msg.contains("1개 발견"));

        let page = provider.list_page("root", "/", None).await.unwrap();
        assert_eq!(page.files.len(), 1);
        assert_eq!(page.files[0].name, "beach.jpg");

        let tmp_dir = TempDir::new().unwrap();
        let downloaded = provider
            .download_to_temp(&page.files[0].resource_id, "beach.jpg", tmp_dir.path())
            .await
            .unwrap();

        assert_eq!(downloaded.size, 21);
        assert!(downloaded.created_time_override.is_some());
        let temp_file_path = downloaded.temp_guard.path().to_path_buf();
        assert!(temp_file_path.exists());
        drop(downloaded);
        assert!(!temp_file_path.exists());
    }
}

