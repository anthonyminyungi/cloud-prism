# Cloud Prism Meta (Instagram · Facebook) 백업 아카이브 이주 설계서

- **작성일**: 2026-11-11
- **대상 모듈**: `src/router.rs`, `src/config.rs`, `src/sources/mod.rs`, `src/sources/meta.rs` (신규), `src/targets/mod.rs`, `src/targets/immich.rs`, `src/pipeline.rs`, `src/state.rs`, `src/web.rs`, `src/ui.html`, `docker-compose.yml`
- **목표**: Meta 계정 센터(`내 정보 내보내기`)에서 추출한 Instagram 및 Facebook 다중 분할 ZIP 백업 아카이브를 파싱하여, (1) 피드·스토리·릴스·사진첩 미디어는 촬영 시각(`creation_timestamp`)과 한글 복원된 본문 캡션(`description`) 및 플랫폼별 앨범(`Instagram` / `Facebook`)을 매핑해 **Immich**로 이주하고, (2) DM/메신저 첨부 미디어와 HTML/JSON 텍스트 기록(Latin-1 Mojibake 한글 깨짐 자동 복원 포함)은 **Nextcloud (`/MetaArchive/...`)**로 안전하게 분리 보관한다.

---

## 1. 아키텍처 개요 및 소스 분리 (`router.rs`, `config.rs`, `docker-compose.yml`)

### 1.1 독립 소스 `SourceType::Meta` 도입
- `SourceType` 열거형에 `Meta` (`"meta"`)를 추가한다 (`SourceType::ALL = &[SourceType::Mybox, SourceType::Gdrive, SourceType::Takeout, SourceType::Meta]`).
- `SourceType::Meta.nextcloud_folder_name()`은 `"MetaArchive"`를 반환하여 Nextcloud 내 저장 경로가 `/MetaArchive/Instagram/...` 및 `/MetaArchive/Facebook/...`으로 격리되도록 한다.
- `AppConfig` 및 `RuntimeConfigSummary`, `SettingsUpdateRequest`에 `meta_dir: String` (기본값 `DEFAULT_META_DIR = "/data/meta"`, 환경변수 `META_DIR`)을 추가한다.
- `docker-compose.yml` 환경변수에 `- META_DIR=${META_DIR:-/data/meta}` 기본값 주석을 추가한다.

### 1.2 제로-버퍼링 & `tmpfs` 아키텍처 유지
- 외부 프로그램(`exiftool`, `python`)이나 ZIP 전체 디스크 압축 해제 없이, Rust 단일 바이너리 내에서 분할 ZIP Central Directory 인덱싱 및 `tmpfs` (`/tmp/mybox_sync`, 256MB 이상은 `/data/.tmp_sync` 폴백) 기반 스트리밍 추출을 수행한다.

---

## 2. 경로 기반 정밀 타겟 라우팅 (`router.rs`, `sources/meta.rs`, `pipeline.rs`)

### 2.1 플랫폼 식별 및 상위 경로 정규화 (`normalize_meta_parent_path`)
Meta 백업 ZIP은 단독 추출 또는 통합 추출에 따라 루트 폴더명이 달라진다(예: `instagram-username-2026-11-11-hash/media/posts/...`, `facebook-username-2026-11-11-hash/your_facebook_activity/...`, 또는 `instagram/...`, `facebook/...`).
- **플랫폼 판별 (`MetaPlatform`)**:
  - 엔트리 경로에 `your_instagram_activity`, `instagram-`, `/instagram/`이 포함되거나 최상위 `media/posts/`, `media/stories/`, `media/reels/`, `media/archived_posts/` 구조이면 **`MetaPlatform::Instagram`** (`"Instagram"`).
  - 엔트리 경로에 `your_facebook_activity`, `facebook-`, `/facebook/`, `messages/e2ee_cutover` 등이 포함되면 **`MetaPlatform::Facebook`** (`"Facebook"`).
  - 그 외에는 기본값 `"Meta"` (또는 상위 폴더명 기반 추정).
- **Nextcloud 상대 경로 정규화**:
  - 최상위 아카이브 래퍼 폴더(`instagram-*`, `facebook-*`, `meta-*`, `instagram`, `facebook`)를 벗겨내고 `/Instagram/<하위경로>` 또는 `/Facebook/<하위경로>` 형태로 정규화한다.
  - 이를 통해 `NextcloudTarget::ensure_folder_tree`와 결합 시 최종 경로가 `/MetaArchive/Instagram/your_instagram_activity/messages/...` 형태로 생성된다.

### 2.2 미디어 vs 기록물/DM 정밀 분류 (`classify_meta_entry`)
기존 `TargetRouter::classify`는 파일 확장자(`.jpg`, `.mp4` 등)만 보고 무조건 `TargetType::Immich`로 분류하나, Meta 백업에서는 **DM 채팅방 내 타인 전송 짤방/스크린샷이나 프로필 스티커 등이 Immich 타임라인을 오염시키지 않도록 경로 기반 라우팅**을 적용한다.
- **`RemoteFile` 확장**:
  - `src/sources/mod.rs`의 `RemoteFile` 구조체에 `#[serde(default)] pub preferred_target: Option<TargetType>` 필드를 추가한다(기존 소스들은 `None` 유지).
  - `SyncPipeline::scan_single_folder`에서 `rf.preferred_target`이 `Some(TargetType::Immich)`이면 `Immich`로, `Some(TargetType::Nextcloud)`이면 `self.config.enable_nextcloud`가 `true`일 때만 `Nextcloud`로 분류(비활성 시 스캔 제외)하고, `None`이면 기존 `TargetRouter::classify(&rf.name, self.config.enable_nextcloud)`를 호출한다.
- **Meta 엔트리 분류 규칙**:
  1. **무시할 시스템/임시 파일**: `.DS_Store`, `Thumbs.db`, 빈 앨범 더미 파일 등.
  2. **Immich행 (`TargetType::Immich`)**:
     - 파일 확장자가 미디어(`TargetRouter::is_media_extension`)이면서, 경로에 `/messages/` 또는 `/e2ee_cutover/`가 포함되지 **않고**, 아래 게시물/스토리/릴스/사진첩 경로에 속하는 경우:
       - Instagram: `media/posts/`, `media/stories/`, `media/reels/`, `media/archived_posts/`
       - Facebook: `posts/media/`, `photos_and_videos/` (단, `your_facebook_activity/messages/` 제외)
  3. **Nextcloud행 (`TargetType::Nextcloud`)**:
     - 위 Immich행 조건에 해당하지 않는 **모든 유의미한 백업 파일**:
       - DM/메신저 대화방 내 사진·영상·오디오 파일 (`*/messages/inbox/**`, E2EE 보안 저장소 미디어 등)
       - 기타 프로필/배경 미디어 (`media/other/**` 등)
       - 모든 텍스트·열람용 문서 (`.html`, `.json`, `.txt`, `.csv` 등 — `start_here.html`, 게시글/댓글/메시지/팔로워 기록 전체)

---

## 3. 중앙 JSON 파싱 및 Latin-1 Mojibake 한글 복원 (`sources/meta.rs`)

### 3.1 Latin-1 → UTF-8 한글/이모지 복원 (`fix_meta_mojibake_str`, `fix_meta_json_value`)
Meta의 JSON 내보내기 엔진은 UTF-8 바이트 시퀀스를 Latin-1(`ISO-8859-1`) 코드포인트로 잘못 해석해 `\u00ec\u0095\u0088...` 형태로 직렬화한다. `serde_json`이 이를 파싱하면 각 UTF-8 바이트가 `U+0080..=U+00FF` 범위의 개별 `char`로 들어오게 된다.
- **복원 알고리즘 (`fix_meta_mojibake_str`)**:
  - 입력 문자열에 `'\u{0080}'..='\u{00FF}'` 범위의 문자가 하나도 없으면(순수 ASCII이거나 이미 정상 UTF-8 한글 `U+AC00..` 인 경우) 원본을 그대로 반환한다.
  - 문자열 내 연속된 `<= '\u{00FF}'` 문자 구간을 바이트 배열(`Vec<u8>`, `c as u8`)로 모은 뒤 ` std::str::from_utf8`이 성공하면 복원된 UTF-8 문자열로 치환하고, 실패하면 원본 문자를 유지한다.
- **JSON 전체 트리 복원 (`fix_meta_json_value`)**:
  - `serde_json::Value`를 재귀 순회하며 모든 `Value::String` 및 객체 키에 `fix_meta_mojibake_str`를 적용한다.

### 3.2 중앙 JSON 메타데이터 인덱싱 (`MetaMediaMetadata`)
Google Takeout과 달리 Meta는 `your_instagram_activity/content/posts_1.json`이나 `your_facebook_activity/posts/your_posts__check_ins__photos_and_videos_1.json` 등 중앙 JSON 파일 하나에 수백 개 미디어의 상대 경로(`"uri"`)와 타임스탬프·본문이 배열로 들어있다.
- **추출 구조체**:
  ```rust
  #[derive(Debug, Clone, Default)]
  pub struct MetaMediaMetadata {
      pub created_time_rfc3339: Option<String>,
      pub description: Option<String>,
      pub album_name: String,
  }
  ```
- **재귀 인덱싱 규칙 (`extract_media_metadata_from_json`)**:
  - ZIP 아카이브 인덱스(`MetaArchiveIndex`) 구축 시 `.json` 파일들을 순회하며 JSON 트리를 탐색한다.
  - 상위 게시물 객체에 존재하는 `timestamp` / `creation_timestamp`, 본문 텍스트(`title`, `description`, `data[].post`), 앨범명(`name`)을 컨텍스트로 유지하며 하위 배열(`media`, `attachments`, `data` 등)로 내려간다.
  - `"uri"` 문자열 필드를 가진 객체를 만나면:
    1. `uri`를 정규화(앞뒤 슬래시 정리 및 아카이브 루트 대비 상대 경로 매칭 키 생성)한다.
    2. **타임스탬프**: 미디어 객체의 `creation_timestamp` / `timestamp` → 상위 게시물 객체의 `creation_timestamp` / `timestamp` 순으로 Unix Epoch 초(`i64`)를 읽어 UTC RFC3339 문자열로 변환한다.
    3. **본문 설명(`description`)**: 미디어 객체의 `title` 또는 `description` (비어있지 않을 때) → 상위 게시물의 `post` / `title` / `description` 순으로 선택하고 `fix_meta_mojibake_str`로 한글을 복원한다.
    4. **플랫폼 앨범명(`album_name`)**: 엔트리 소속 플랫폼에 따라 `"Instagram"` 또는 `"Facebook"`을 부여한다.
- **폴백 타임스탬프**:
  - JSON에 매핑되지 않은 미디어 파일은 경로 내 `YYYYMM` (예: `media/posts/202305/...` → `2023-05-01T00:00:00Z`) 또는 ZIP 엔트리 수정 시각(`zip_datetime_to_rfc3339`) 순으로 폴백한다.

### 3.3 Nextcloud행 `.json` 파일의 한글 복원 저장
- `MetaArchiveProvider::download_to_temp`에서 추출 대상 파일이 `.json` 확장자인 경우:
  - ZIP에서 읽은 바이트를 `serde_json::from_slice::<serde_json::Value>`로 파싱 후 `fix_meta_json_value`를 수행하고 `serde_json::to_vec_pretty`로 직렬화하여 임시 파일에 기록한다.
  - 변환된 바이트 기준으로 `sha1_hash`와 `total_bytes`(`actual_size`)를 계산해 반환하므로, Nextcloud 업로드(`Content-Length`) 및 사후 삭제 검증(`HEAD Content-Length`) 시 크기 불일치가 발생하지 않는다.

---

## 4. Immich 캡션(`description`) 주입 및 앨범(`Instagram` / `Facebook`) 자동 묶기 (`targets/immich.rs`, `targets/mod.rs`, `sources/mod.rs`)

### 4.1 데이터 전달 파이프라인 확장
- `DownloadedSourceFile` (`src/sources/mod.rs`)에 다음 필드를 추가한다:
  - `pub description: Option<String>`
  - `pub album_name: Option<String>`
- `UploadContext<'a>` (`src/targets/mod.rs`)에 다음 필드를 추가한다:
  - `pub description: Option<&'a str>`
  - `pub album_name: Option<&'a str>`

### 4.2 Immich API 연동 (`ImmichTarget`)
- **자산 설명(`description`) 업데이트**:
  - `POST /api/assets`로 파일 업로드가 성공하여 `asset_id`를 획득한 직후, `ctx.description`이 `Some(desc)` (공백 제외 비어있지 않음)이면 `PUT /api/assets/{asset_id}`에 JSON 바디 `{"description": desc}`를 전송한다.
- **출처별 앨범(`Instagram` / `Facebook`) 자동 생성 및 편입**:
  - `ImmichTarget` 내부에 `album_cache: Arc<tokio::sync::Mutex<HashMap<String, String>>>` (앨범명 → `album_id`)를 둔다.
  - `ctx.album_name`이 `Some(album_name)`이면 `ensure_album_id(&self, album_name: &str) -> Result<String>`을 호출한다:
    1. `album_cache`에 캐시된 ID가 있으면 즉시 반환한다.
    2. 없으면 `GET /api/albums`를 호출해 동일 `albumName`이 있는지 찾고, 없으면 `POST /api/albums` (`{"albumName": album_name, "description": "Cloud-Prism Meta 백업 자동 생성 앨범"}`)로 생성하여 캐시에 저장한다.
  - 획득한 `album_id`에 대해 `PUT /api/albums/{album_id}/assets` (`{"ids": [asset_id]}`)를 호출해 자산을 앨범에 추가한다.
  - 부가 메타데이터 API(설명 주입 또는 앨범 편입) 호출이 구버전 Immich 서버 등에서 실패하더라도 사진 원본 업로드 자체는 성공으로 유지하되 경고(warn) 또는 세부 상태를 안전하게 처리한다.

---

## 5. 웹 API · 대시보드 UI · 자동 삭제 보호 가드 (`web.rs`, `ui.html`, `pipeline.rs`, `state.rs`)

### 5.1 웹 엔드포인트 및 상태 조회 확장 (`web.rs`, `state.rs`)
- 다음 엔드포인트를 추가한다:
  - `POST /api/meta/path`: Meta 백업 폴더 경로 변경 및 연결 확인 (`{"meta_dir": "/data/meta"}`)
  - `POST /api/meta/fetch-url`: 브라우저 다운로드 링크에서 Meta 백업 `.zip` 직접 다운로드
  - `POST /api/meta/upload`: 웹 브라우저 드래그앤드롭 `.zip` 원자적(`.part`) 업로드
- `GET /api/status` 응답에 `meta_info: { dir, exists, archives }` 및 `all_stats.meta`를 추가하고, `verify_single_service("meta")`, `readiness_summary()`의 `verified_sources`에 `"meta"`를 포함시킨다.

### 5.2 대시보드 UI 확장 (`ui.html`)
- `[1. 연결 및 설정]` 탭의 소스 목록에 **`Meta (Instagram · Facebook)`** 카드를 추가한다 (폴더 경로 설정, ZIP 드래그앤드롭 업로드, URL 직접 다운로드, 인식된 `.zip` 목록 표시).
- `[2. 데이터 동기화]` 탭의 소스 선택 드롭다운/탭에 `Meta (Instagram · Facebook)`을 추가하여 샘플 테스트(`1개`, `5개` 등) 및 전체 동기화를 지원한다.

### 5.3 로컬 Meta ZIP 자동 삭제 및 유실 방지 가드 (`pipeline.rs`)
- `SyncPipeline::run_sync` 종료 시 `SourceType::Meta`에 대해서도 아카이브 정리(`cleanup_meta_archives_after_sync`)를 수행한다:
  - 테스트 제한 실행(`limit_files.is_some()`)이거나 미완료/실패 항목이 있으면 ZIP을 보존한다.
  - **`enable_nextcloud == false` 가드**: Nextcloud가 비활성화된 상태에서 Meta ZIP 내에 Nextcloud행 파일(HTML/JSON 텍스트 기록, DM 첨부파일 등)이 1건 이상 존재하면(`count_unclassified_files(false) > 0`), **로컬 Meta ZIP 자동 삭제를 중단(보존)**하고 안내 로그를 발행한다.
  - 모든 항목이 업로드 완료되고 `verify_uploaded_batch` 100% 타겟 재검증을 통과한 경우에만 로컬 Meta `.zip` 파일들을 자동 삭제해 NAS 디스크 공간을 회수한다.
