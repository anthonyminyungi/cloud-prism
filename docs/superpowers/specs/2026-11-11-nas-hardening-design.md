# Cloud Prism NAS 실전 배포 전 하드닝(Hardening) 설계서

- **작성일**: 2026-11-11
- **대상 모듈**: `src/config.rs`, `src/db.rs`, `src/limiter.rs`, `src/sources/*`, `src/targets/*`, `src/pipeline.rs`, `src/state.rs`, `src/web.rs`, `src/ui.html`, `docker-compose.yml`
- **목표**: TrueNAS SCALE(Docker + ZFS + 제한된 RAM/CPU + 24/7 무인 구동) 환경에 투입하기 전, 대용량 영상 OOM 위험, Takeout 임시 파일 충돌 및 문서 유실 위험, 증분 스캔 시 완료 항목 초기화 버그, 동적 스케줄러 미작동, ZIP 반복 파싱 병목, 타임아웃 부재 등 진단된 12개 문제를 근본적으로 해결한다.

---

## 1. 스트리밍 업로드 전환 및 하이브리드 임시 저장소 (`sources`, `targets`, `docker-compose.yml`)

### 1.1 하이브리드 임시 디렉터리 선택 (`resolve_temp_dir`)
- **배경**: `docker-compose.yml`에서 ZFS CoW 부하를 줄이기 위해 `/tmp/mybox_sync`를 `tmpfs`(RAM 디스크)로 마운트하나, 수 GB 단위 4K 동영상이나 다중 워커 처리 시 `tmpfs` 용량 한계(`ENOSPC`)에 부딪힐 수 있다.
- **설계**:
  - `docker-compose.yml`의 기본 `tmpfs` 크기를 `1G`에서 `2G`로 상향한다 (`- /tmp/mybox_sync:size=2G,mode=1777`).
  - `src/sources/mod.rs`에 `LARGE_FILE_DISK_FALLBACK_THRESHOLD: u64 = 256 * 1024 * 1024` (256 MB) 상수를 정의하고, `resolve_temp_dir(preferred_tmp: &Path, fallback_disk_tmp: &Path, size_hint: Option<u64>) -> PathBuf` 헬퍼를 제공한다.
  - 파일 예상 크기(`size_hint`)가 256 MB 이상이면 RAM(`tmpfs`) 대신 영구 볼륨 하위의 디스크 임시 폴더(`/data/.tmp_sync`, 즉 `db_path` 상위 디렉터리의 `.tmp_sync`)를 사용한다.
  - `stream_response_to_temp` 또는 `GoogleTakeoutProvider::download_to_temp` 도중 `preferred_tmp`에서 쓰기 오류(`ErrorKind::StorageFull` 등)가 발생하면 자동으로 `fallback_disk_tmp`에 재시도한다.
  - `AppState::new` 초기화 시 `tmp_dir` 및 디스크 폴백 폴더(`/data/.tmp_sync`) 내의 이전 비정상 종료 잔여 임시 파일을 정리(`cleanup_stale_temp_files`)한다.

### 1.2 Immich 및 Nextcloud 제로-버퍼링(Zero-Buffering) 스트리밍 업로드
- **배경**: 기존 `ImmichTarget::upload_asset`과 `NextcloudTarget::upload_file_with_resource`는 `tokio::fs::read(file_path)`로 파일 전체를 `Vec<u8>` 힙 메모리에 적재하며, Nextcloud는 재시도 클로저 안에서 `bytes.clone()`까지 수행해 대용량 영상 업로드 시 심각한 메모리 피크를 유발한다.
- **설계**:
  - **`ImmichTarget::upload_asset`**:
    - `tokio::fs::read`를 제거하고, `tokio::fs::metadata(file_path)`로 파일 크기(`file_len`)를 구한다.
    - `tokio::fs::File::open(file_path)` → `tokio_util::io::ReaderStream::new(file)` → `reqwest::Body::wrap_stream(stream)` → `Part::stream_with_length(body, file_len).file_name(...)`로 멀티파트 폼을 구성한다.
    - 업로드 중 HTTP 429 또는 5xx/네트워크 오류 발생 시 `max_retries`까지 지수 백오프로 재시도할 수 있도록 루프 내부에서 매 시도마다 파일을 새로 열어 스트리밍 `Form`을 생성한다.
  - **`NextcloudTarget::upload_file_with_resource`**:
    - `tokio::fs::read`와 `bytes.clone()`을 제거한다.
    - 재시도 루프의 매 시도마다 `tokio::fs::File::open(file_path)` 및 `ReaderStream::new(file)`로 `reqwest::Body::wrap_stream`을 생성하고, `Content-Length: file_len` 헤더와 `X-OC-MTime` 헤더를 함께 전송한다.

---

## 2. Takeout 동시성 안전성 · 인덱싱 최적화 · 조건부 자동 삭제 (`sources/takeout.rs`, `pipeline.rs`, `web.rs`)

### 2.1 다중 워커 임시 파일명 충돌 방지
- **배경**: `GoogleTakeoutProvider::download_to_temp`가 `takeout_{filename}`만 사용해 다른 폴더의 동명 파일(예: `2023/IMG_0001.JPG`, `2024/IMG_0001.JPG`)을 동시 추출할 때 파일 덮어쓰기 및 조기 삭제 레이스 컨디션이 발생한다.
- **설계**:
  - `resource_id`의 SHA-1 해시 앞 12자리(hex)를 프리픽스로 결합하여 `takeout_{id_hash12}_{safe_filename}` 형식으로 임시 파일 경로를 생성한다. 이를 통해 경로 길이 제한(255바이트)을 초과하지 않으면서도 100% 고유한 임시 파일명을 보장한다.

### 2.2 Takeout ZIP 인덱스 캐싱 및 `spawn_blocking` 오프로딩
- **배경**: 분할된 Takeout ZIP 환경에서 매 파일 추출마다 모든 `.zip`의 Central Directory를 반복 파싱하고, 일반 문서에 대해서도 모든 형제 `.zip`을 열어 JSON 사이드카를 탐색하는 $O(N \times M)$ 디스크 병목이 존재한다.
- **설계**:
  - `GoogleTakeoutProvider` 내부에 디렉터리 스냅샷(`Vec<(PathBuf, u64, Option<SystemTime>)>`) 기반의 캐시 구조체 `TakeoutArchiveIndex` (`Arc< std::sync::Mutex<Option<TakeoutArchiveIndex>> >`)를 둔다:
    - `entry_to_zip: HashMap<String, PathBuf>`: 정규 엔트리 경로 → 소속 `.zip` 파일 경로
    - `sidecar_to_zip: HashMap<String, PathBuf>`: `.json` 사이드카 경로 → 소속 `.zip` 파일 경로
    - `unclassified_non_media_count: usize`: 시스템 엔트리(`.json`, `archive_browser.html`)가 아니면서 미디어 확장자도 아닌 일반 파일의 총 개수
  - 폴더 내 `.zip` 파일 목록이나 크기/수정시각이 변하지 않았다면 캐시된 인덱스를 즉시 재사용한다 ($O(1)$ 조회).
  - 미디어 확장자(`TargetRouter::is_media_extension`)가 아닌 일반 문서 파일은 JSON 사이드카 탐색(`find_sidecar_timestamp`)을 건너뛴다.
  - `download_to_temp`의 ZIP 엔트리 압축 해제 + SHA-1 해시 계산 + 사이드카 읽기를 `tokio::task::spawn_blocking` 내부에서 수행해 Tokio 비동기 워커 스레드 블로킹을 방지한다.

### 2.3 Nextcloud 비활성 시 Takeout ZIP 자동 삭제 보호
- **배경**: `enable_nextcloud = false`일 때 사진과 일반 문서가 함께 들어있는 Takeout ZIP을 동기화하면, 문서는 스캔에서 제외된 채 사진만 업로드된 후 원본 ZIP이 자동 삭제되어 문서가 유실될 수 있다.
- **설계**:
  - `GoogleTakeoutProvider::count_unclassified_files(&self, enable_nextcloud: bool) -> Result<usize>`를 제공한다.
  - `SyncPipeline::cleanup_takeout_archives_after_sync`에서 `!self.config.enable_nextcloud`일 때 미분류 일반 파일이 1개 이상(`unclassified_count > 0`) 존재하면:
    - 로컬 ZIP 자동 삭제를 중단(보존)하고,
    - `"[takeout] ZIP 내에 Nextcloud 미사용으로 건너뛴 일반 문서 파일 N건이 포함되어 있어 로컬 ZIP 아카이브를 보존합니다."` 로그 이벤트를 발행한다.

### 2.4 브라우저 Takeout ZIP 업로드 원자적(`.part`) 저장
- **배경**: `POST /api/takeout/upload`가 최종 `.zip` 파일명으로 직접 스트림을 기록하여 업로드 도중 스캔/상태 조회 시 손상된 ZIP으로 인식될 수 있다.
- **설계**:
  - `upload_takeout_zip`에서 스트림 수신 중에는 `{safe_name}.part` 파일에 기록하고, 스트림이 정상 종료되고 `flush()`가 완료된 직후 `{safe_name}`으로 `tokio::fs::rename`한다.
  - 중간에 스트림 오류나 쓰기 실패가 발생하면 `.part` 파일을 즉시 삭제한다.

---

## 3. DB 체크포인트 정합성 & 벌크 검증/삭제 (`db.rs`, `pipeline.rs`, `targets/immich.rs`)

### 3.1 증분 재스캔 시 `UPLOADED` 항목 초기화 버그 수정
- **배경**: 스캔 시 `f.size == 0`(예: Google Docs/Sheets)이었던 파일이 업로드 후 `mark_file_uploaded_with_size`에 의해 실제 크기(`actual_size > 0`)로 갱신되면, 다음 스캔에서 `old.size != f.size` (`19 != 0`)로 인해 `content_changed = true`로 판정되어 `PENDING`으로 초기화된다.
- **설계**:
  - `CheckpointDB::upsert_files`의 크기 변경 조건을 `let size_changed = f.size > 0 && old.size > 0 && old.size != f.size;`로 변경한다.
  - 또한 `old.status == FileStatus::Uploaded.as_str()` 또는 `FileStatus::Deleted.as_str()`인 항목이 스캔 시 `f.size == 0`을 반환하더라도 기존의 검증된 `old.size`를 `0`으로 덮어쓰지 않고 보존한다 (`if f.size > 0 { active.size = Set(f.size); }`).

### 3.2 Nextcloud 비활성 시 `PENDING` 큐 타겟 필터링
- **배경**: Nextcloud를 켰다가 끈 경우, 기존에 `target_type = "nextcloud"`로 스캔된 `PENDING` 파일들이 동기화 대상에 포함되어 실패를 유발한다.
- **설계**:
  - `CheckpointDB::get_pending_files_for_config(source_type, max_retries, limit, enable_nextcloud)`를 추가(기존 `get_pending_files`는 호환성 유지)하여, `enable_nextcloud == false`일 때는 `target_type == "immich"`인 항목만 조회 및 카운트하도록 한다.

### 3.3 Immich 벌크 검증 및 DB 일괄 삭제 마킹
- **설계**:
  - `SyncPipeline::verify_uploaded_batch(&self, files: &[FileRecord]) -> Result<HashMap<String, bool>>` 헬퍼를 도입한다.
  - `TargetType::Immich` 항목들은 500개 단위 청크로 묶어 `ImmichTarget::check_bulk_hashes`를 호출함으로써 수만 건의 검증을 단 몇 번의 HTTP 요청으로 처리한다.
  - `CheckpointDB::mark_files_deleted_batch(&self, source_type: &str, resource_ids: &[String]) -> Result<u64>`를 추가해 단일 SQLite 트랜잭션 안에서 500건 단위 `IN (...)` 조건으로 고속 업데이트한다.

---

## 4. 동적 자동 스케줄러 · 네트워크 타임아웃 · RateLimiter 분리 (`pipeline.rs`, `state.rs`, `sources/mod.rs`, `limiter.rs`)

### 4.1 동적 자동 스케줄러 (`spawn_auto_sync_scheduler`)
- **배경**: 기존 스케줄러는 부팅 시 `state.config.sync_interval_minutes`가 `0`이면 태스크를 종료해 버려, `settings.json`에 저장된 값이나 웹 UI에서 런타임에 변경한 주기가 전혀 동작하지 않는다.
- **설계**:
  - `AppState`에 `pub scheduler_notify: Arc< tokio::sync::Notify >` 필드를 추가하고, `update_settings`에서 `sync_interval_minutes`가 변경될 때 `self.scheduler_notify.notify_waiters()`를 호출한다.
  - `spawn_auto_sync_scheduler`는 항상 백그라운드 루프를 유지하며:
    1. `let interval_mins = state.effective_config().await.sync_interval_minutes;`를 확인한다.
    2. `interval_mins == 0`이면 `state.scheduler_notify.notified().await`로 대기한다.
    3. `interval_mins > 0`이면 `tokio::select!`로 `tokio::time::sleep(Duration::from_secs(interval_mins * 60))`과 `state.scheduler_notify.notified()`를 동시에 대기하여, 주기가 변경되면 즉시 새 타이머로 리셋하고, 주기가 도래하면 `readiness_summary().await`에서 검증 완료된(또는 설정 완료된) 소스들을 순차 실행한다.

### 4.2 HTTP 클라이언트 타임아웃 및 스트림 읽기 타임아웃
- **설계**:
  - `build_http_client` 및 `build_target_http_client`에 `.connect_timeout(Duration::from_secs(15))`, `.pool_idle_timeout(Duration::from_secs(90))`, `.tcp_keepalive(Duration::from_secs(30))`를 설정한다.
  - `stream_response_to_temp`의 `stream.next().await` 루프에 청크당 60초 읽기 타임아웃(`tokio::time::timeout(Duration::from_secs(60), stream.next())`)을 적용해 Half-open TCP 연결 시 영구 블로킹을 방지한다.

### 4.3 외부 클라우드 소스 vs 내부 LAN 타겟 RateLimiter 분리
- **설계**:
  - `SyncPipeline::from_config`에서:
    - 외부 클라우드 소스(`Mybox`, `Gdrive`)용 리미터: `RateLimiter::new(config.rate_limit_rps, config.max_retries)`
    - 로컬 LAN 타겟(`Immich`, `Nextcloud`)용 리미터: `RateLimiter::new((config.rate_limit_rps * 10.0).max(50.0), config.max_retries)`
  - 이를 통해 외부 클라우드 API 제한은 엄격히 준수하면서도, 로컬 NAS(Immich/Nextcloud) 통신과 Google Takeout 로컬 이주 속도는 병목 없이 최고 속도로 동작한다.

---

## 5. 웹 대시보드 XSS 방어 및 설정 파일 보안 (`src/ui.html`, `src/config.rs`)

1. **`escapeHtml` 적용 (`src/ui.html`)**:
   - `escapeHtml(raw)` 함수를 추가해 `&`, `<`, `>`, `"`, `'` 문자를 치환한다.
   - `renderSettingsCards`의 아카이브 이름(`a.name`)과 `renderStatus`의 실패 목록(`f.source`, `f.target_type`, `f.parent_path`, `f.name`, `f.error_msg`)에 `escapeHtml`을 적용해 HTML 응답 에러 메시지나 특수 파일명으로 인한 DOM 깨짐 및 XSS를 방지한다.
2. **`/data/settings.json` Unix 권한 `0600` 설정 (`src/config.rs`)**:
   - `AppConfig::save_persisted`에서 파일 저장 직후 `#[cfg(unix)]` 블록을 통해 `std::os::unix::fs::PermissionsExt::from_mode(0o600)` 권한을 설정한다.

---

## 6. 테스트 및 검증 계획
- **단위 및 통합 테스트 추가**:
  1. `tests/db_test.rs`: Google Docs(`size: 0` 스캔 → `actual_size: 12345` 업로드 → 재스캔 시 `size: 0` 반환) 시나리오에서 `UPLOADED` 상태와 `size: 12345`가 그대로 유지(`PENDING`으로 초기화되지 않음)되는지 검증. `mark_files_deleted_batch` 및 `enable_nextcloud = false` 필터링 검증.
  2. `tests/pipeline_integration_test.rs`:
     - 대용량 파일(또는 하이브리드 디스크 폴백) 및 스트리밍 업로드가 정상 동작하는지 검증.
     - `enable_nextcloud = false` 상태에서 사진+문서가 섞인 Takeout ZIP 동기화 시 사진은 Immich로 백업되고 **원본 ZIP은 삭제되지 않고 보존**되는지 검증.
     - 동명 파일이 서로 다른 폴더에 있는 Takeout ZIP을 `concurrency = 4`로 동시 동기화할 때 임시 파일 충돌 없이 모두 성공하는지 검증.
  3. `tests/web_api_test.rs`:
     - `POST /api/takeout/upload`가 `.part` 임시 파일을 거쳐 원자적으로 저장되는지 검증.
     - `src/ui.html`에 `escapeHtml`이 포함되어 있는지 검증.
