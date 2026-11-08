# 멀티 클라우드(MYBOX · Google · Takeout) to NAS(Immich · Nextcloud) Rust 비동기 이주 및 사후 삭제 엔진 설계 명세서 (Design Spec)

- **최종 업데이트**: 2026-11-09
- **프로젝트 경로**: `C:\Users\dbstn\.gemini\antigravity\scratch\mybox-immich-sync`
- **언어 및 기술 스택**: **Rust (Edition 2021, `rustc 1.93+`)** — `tokio`, `reqwest`, `sqlx` (SQLite WAL), `sha1`, `zip`, `serde`, `clap`, `indicatif`, Docker Multi-Stage Build (TrueNAS SCALE 최적화)
- **목적**: 네이버 MYBOX, Google Drive, Google Photos(Takeout) 등 외부 클라우드에 남아있는 데이터를 `trait` 기반 플러그인으로 스캔하여 **사진·동영상은 셀프호스팅 Immich**로, **일반 문서·파일은 폴더 구조를 유지한 채 셀프호스팅 Nextcloud**로 고속 비동기 스트리밍 이주시키고, SQLite(`WAL` 모드) 체크포인트 및 해시/크기 이중 검증을 거쳐 안전하게 사후 일괄 원격 삭제(Offload & Purge)를 수행하는 초경량 단일 바이너리/컨테이너 오픈소스 도구 구축.

---

## 1. 핵심 요구사항 및 아키텍처 원칙

1. **Rust `trait` 기반 양방향 플러그인 추상화 (`SourceProvider` → `TargetRouter` → `TargetProvider`)**:
   - **입력부(`#[async_trait] pub trait SourceProvider: Send + Sync`)**:
     - `MyboxProvider`: 네이버 MYBOX Open API (`PAT` 기반 비동기 폴더 탐색·스트리밍 다운로드·휴지통 이동)
     - `GoogleDriveProvider`: Google Drive API v3 (서버 측 `sha1Checksum`을 활용한 무다운로드 중복 스킵, Google Docs/Sheets 자동 변환 다운로드, 휴지통 이동)
     - `GoogleTakeoutProvider`: Google Photos Takeout `.zip` 아카이브 스트리밍 + JSON Sidecar 메타데이터(원본 촬영일·설명) 결합 복원
   - **자동 분류 라우터(`TargetRouter`)**:
     - 파일 확장자 및 MIME 타입을 판별하여 사진/영상(`TargetType::Immich`)은 Immich로, 일반 파일/문서(`TargetType::Nextcloud`)는 Nextcloud로 자동 라우팅한다.
   - **출력부(`TargetProvider`)**:
     - `ImmichTarget`: SHA-1 해시 기반 사전 중복 검사(`POST /api/assets/bulk-upload-check`) 및 신규 에셋 멀티파트 스트리밍 업로드(`POST /api/assets`)
     - `NextcloudTarget`: 표준 WebDAV API(`MKCOL`, `PUT`, `HEAD`)를 통해 원본 폴더 구조를 재현하고, `X-OC-MTime` 헤더로 원본 수정 시각을 보존하며 스트리밍 업로드한다.
2. **Zero-Copy 스트리밍 SHA-1 연산 & 극강의 메모리 효율**:
   - `tokio::io::AsyncWrite`와 `sha1::Sha1` 해셔를 결합해 네트워크 청크를 `tmpfs`에 쓰면서 동시에 해시를 계산한다. 대용량 4K 동영상(수 GB) 처리 시에도 메모리 사용량이 15MB 미만으로 일정하게 유지된다.
3. **TrueNAS SCALE (ZFS) 최적화 및 완료 시 컨테이너 자동 종료**:
   - 스트리밍 임시 파일(`/tmp/mybox_sync`)은 호스트 ZFS 디스크가 아닌 컨테이너 RAM 디스크(`tmpfs`)에 마운트하여 ZFS Copy-on-Write(CoW) 디스크 마모 및 스냅샷 용량 증가를 원천 차단한다.
   - Rust Multi-Stage Docker 빌드를 통해 최종 런타임 이미지를 초경량(`debian:bookworm-slim` + 단일 바이너리)으로 구성하며, `restart: on-failure:3` 정책을 통해 완료(`exit 0`) 시 컨테이너가 자동으로 내려간다(Stopped).
4. **SQLite(`WAL` 모드) 체크포인트 및 사후 이중 검증 일괄 삭제**:
   - `sync` 단계에서는 원본 클라우드 파일을 절대 삭제하지 않으며, `(source_type, resource_id)` 복합키 기준으로 `target_type`, `sha1_hash`, `remote_target_id`, `status = 'UPLOADED'`만 기록한다.
   - 나중에 사용자가 `delete` 명령을 실행하면 타겟 서버(Immich 해시 재검증 / Nextcloud `HEAD` 크기 재검증) 확인을 통과한 파일만 원본 클라우드의 휴지통 이동 API를 호출한다.

---

## 2. 시스템 아키텍처 및 디렉터리 구조 (Rust Crate Layout)

```text
mybox-immich-sync/
├── Cargo.toml
├── Dockerfile                 # Multi-stage Rust build -> slim runtime image
├── docker-compose.yml         # TrueNAS SCALE (tmpfs + restart: on-failure:3)
├── .env.example
├── README.md
├── src/
│   ├── lib.rs                 # 라이브러리 루트 모듈 노출
│   ├── main.rs                # clap 기반 CLI 엔트리포인트 (auto, scan, sync, delete, status)
│   ├── config.rs              # 환경변수 로드 및 설정 구조체 (Config)
│   ├── db.rs                  # sqlx (SQLite WAL 모드) 비동기 체크포인트 DB 매니저
│   ├── limiter.rs             # Token-Bucket Rate Limiter & 429/5xx 지수 백오프 재시도
│   ├── router.rs              # 확장자 기반 TargetRouter (Immich vs Nextcloud)
│   ├── pipeline.rs            # tokio 멀티 워커 파이프라인 (scan / sync / delete)
│   ├── sources/
│   │   ├── mod.rs             # SourceProvider trait 및 공통 모델 (RemoteFile, SourceType)
│   │   ├── mybox.rs           # 네이버 MYBOX Open API 구현체
│   │   ├── gdrive.rs          # Google Drive API v3 구현체 (서버 SHA-1 사전 체크 포함)
│   │   └── takeout.rs         # Google Takeout ZIP + JSON 메타데이터 복원 구현체
│   └── targets/
│       ├── mod.rs             # TargetProvider trait 및 공통 모델 (TargetType, UploadOutcome)
│       ├── immich.rs          # 셀프호스팅 Immich REST API 구현체
│       └── nextcloud.rs       # 셀프호스팅 Nextcloud WebDAV 구현체
└── tests/
    ├── db_test.rs
    ├── router_limiter_test.rs
    └── pipeline_integration_test.rs
```

---

## 3. SQLite 데이터베이스 스키마 (`/data/sync_state.db`)

연결 풀 생성 시 `SqliteConnectOptions`에서 `journal_mode(SqliteJournalMode::Wal)`, `synchronous(SqliteSynchronous::Normal)`, `busy_timeout(Duration::from_secs(10))`을 강제한다.

### 3.1 `folders` 테이블 (소스별 폴더 트리 탐색 체크포인트)
| 컬럼명 | 타입 | 설명 |
|---|---|---|
| `source_type` | `TEXT NOT NULL` | `'mybox'` \| `'gdrive'` \| `'takeout'` |
| `folder_id` | `TEXT NOT NULL` | 클라우드 폴더 ID (루트는 `'ROOT'`) |
| `path` | `TEXT NOT NULL` | 전체 폴더 경로 (예: `'/문서/2025'`) |
| `last_cursor` | `TEXT` | 페이지네이션 중단 시 재개할 커서(`cursor` / `nextPageToken`) |
| `scan_status` | `TEXT NOT NULL` | `'PENDING'` \| `'SCANNED'` \| `'FAILED'` |
| `updated_at` | `TEXT NOT NULL` | ISO-8601 갱신 시각 |
| **PK** | `PRIMARY KEY (source_type, folder_id)` | 소스별 폴더 고유 식별 |

### 3.2 `files` 테이블 (파일별 라우팅, 동기화 및 삭제 체크포인트)
| 컬럼명 | 타입 | 설명 |
|---|---|---|
| `source_type` | `TEXT NOT NULL` | `'mybox'` \| `'gdrive'` \| `'takeout'` |
| `resource_id` | `TEXT NOT NULL` | 원본 클라우드 파일 고유 ID (원격 삭제 시 사용) |
| `name` | `TEXT NOT NULL` | 파일명 (예: `'IMG_1234.HEIC'`, `'계약서.pdf'`) |
| `parent_path` | `TEXT NOT NULL` | 원본 상위 폴더 경로 (Nextcloud 업로드 시 경로 보존에 사용) |
| `size` | `INTEGER NOT NULL` | 원본 파일 크기 (Bytes) |
| `created_time` | `TEXT` | 원본 생성/촬영 시각 |
| `modified_time` | `TEXT NOT NULL` | 원본 수정/촬영 시각 |
| `target_type` | `TEXT NOT NULL` | `'immich'` (사진·영상) \| `'nextcloud'` (일반 파일·문서) |
| `sha1_hash` | `TEXT` | 서버 제공 또는 스트리밍 다운로드 중 계산된 SHA-1 해시 (Hex) |
| `remote_target_id` | `TEXT` | Immich 에셋 ID 또는 Nextcloud 원격 경로 |
| `sync_result` | `TEXT` | `'created'` (신규 업로드) \| `'duplicate'` (기존 존재로 업로드 생략) |
| `status` | `TEXT NOT NULL` | `'PENDING'` → `'UPLOADED'` → `'DELETED'` (오류 시 `'FAILED'`) |
| `retry_count` | `INTEGER DEFAULT 0` | 실패 재시도 횟수 |
| `error_msg` | `TEXT` | 마지막 오류 메시지 |
| `updated_at` | `TEXT NOT NULL` | ISO-8601 갱신 시각 |
| **PK** | `PRIMARY KEY (source_type, resource_id)` | 소스별 파일 고유 식별 |

---

## 4. 핵심 Rust Trait 인터페이스 설계

```rust
#[async_trait::async_trait]
pub trait SourceProvider: Send + Sync {
    fn source_type(&self) -> &'static str;
    fn supports_remote_delete(&self) -> bool;

    /// 지정한 폴더의 하위 항목 1페이지를 조회하여 (폴더 목록, 파일 목록, 다음 커서) 반환
    async fn list_page(
        &self,
        folder_id: &str,
        folder_path: &str,
        cursor: Option<&str>,
    ) -> anyhow::Result<(Vec<RemoteFolder>, Vec<RemoteFile>, Option<String>)>;

    /// 파일을 dest_path(tmpfs)로 스트리밍 다운로드하면서 동시에 SHA-1 해시와 바이트 수를 반환
    async fn download_to_path_with_sha1(
        &self,
        resource_id: &str,
        dest_path: &std::path::Path,
    ) -> anyhow::Result<(String, u64)>;

    /// 검증 완료된 원본 파일을 클라우드 휴지통으로 이동
    async fn delete_resource(&self, resource_id: &str) -> anyhow::Result<()>;
}
```

---

## 5. TrueNAS SCALE Multi-Stage Docker 구성

```dockerfile
# Stage 1: Builder
FROM rust:1.85-slim-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

# Stage 2: Minimal Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data /tmp/mybox_sync
COPY --from=builder /app/target/release/mybox-sync /usr/local/bin/mybox-sync
ENV DB_PATH=/data/sync_state.db \
    TEMP_DIR=/tmp/mybox_sync
ENTRYPOINT ["mybox-sync"]
CMD ["auto"]
```
