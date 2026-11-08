# 멀티 클라우드(MYBOX · Google · iCloud) to NAS(Immich · Nextcloud) 비동기 이주 및 사후 삭제 엔진 설계 명세서 (Design Spec)

- **최종 업데이트**: 2026-11-09
- **프로젝트 경로**: `C:\Users\dbstn\.gemini\antigravity\scratch\mybox-immich-sync`
- **목적**: 네이버 MYBOX, Google Drive, Google Photos(Takeout), iCloud 등 외부 클라우드에 남아있는 데이터를 플러그인 방식으로 스캔하여 **사진·동영상은 셀프호스팅 Immich**로, **일반 문서·파일은 폴더 구조를 유지한 채 셀프호스팅 Nextcloud**로 비동기 스트리밍 이주시키고, SQLite(`WAL` 모드) 체크포인트 및 해시/크기 이중 검증을 거쳐 안전하게 사후 일괄 원격 삭제(Offload & Purge)를 수행하는 TrueNAS SCALE 최적화 오픈소스 도구 구축.

---

## 1. 핵심 요구사항 및 아키텍처 원칙

1. **양방향 플러그인 추상화 (`SourceProvider` → `TargetRouter` → `TargetProvider`)**:
   - **입력부(`SourceProvider`)**:
     - `MyboxProvider`: 네이버 MYBOX Open API (`PAT` 기반 스캔·다운로드·휴지통 이동)
     - `GoogleDriveProvider`: Google Drive API v3 (서버 측 `sha1Checksum`을 활용한 무다운로드 중복 스킵, Google Docs/Sheets 자동 변환 다운로드, 휴지통 이동)
     - `GoogleTakeoutProvider`: Google Photos Takeout `.zip` 아카이브 무압축해제 스트리밍 + JSON Sidecar 메타데이터(원본 촬영일·설명) 결합 복원
     - `ICloudProvider` (확장 인터페이스): `pyicloud` 기반 조회·다운로드·원격 삭제 연동 규격
   - **자동 분류 라우터(`TargetRouter`)**:
     - 파일 확장자 및 MIME 타입을 판별하여 사진/영상(`MEDIA_EXTENSIONS`)은 `immich` 타겟으로, 일반 파일/문서는 `nextcloud` 타겟으로 자동 라우팅한다.
   - **출력부(`TargetProvider`)**:
     - `ImmichTarget`: SHA-1 해시 기반 사전 중복 검사(`POST /api/assets/bulk-upload-check`) 및 신규 에셋 업로드(`POST /api/assets`)
     - `NextcloudTarget`: 표준 WebDAV API(`MKCOL`, `PUT`, `HEAD`)를 통해 MYBOX/Google Drive의 원본 폴더 구조를 재현하고, `X-OC-MTime` 헤더로 원본 수정 시각을 보존하며 업로드한다.
2. **TrueNAS SCALE (ZFS) 최적화 및 완료 시 컨테이너 자동 종료**:
   - 스트리밍 임시 파일(`/tmp/mybox_sync`)은 호스트 ZFS 디스크가 아닌 컨테이너 RAM 디스크(`tmpfs`)에 마운트하여 ZFS Copy-on-Write(CoW) 디스크 마모 및 스냅샷 용량 증가를 원천 차단한다.
   - `restart: on-failure:3` 정책을 통해 네트워크 오류(`exit 1`) 시에만 자동 재기동하여 체크포인트부터 이어하고, **동기화 또는 삭제 작업이 100% 완료(`exit 0`)되면 컨테이너가 자동으로 내려간다(Stopped).**
3. **SQLite(`WAL` 모드) 체크포인트 및 사후 이중 검증 일괄 삭제**:
   - `sync` 단계에서는 원본 클라우드 파일을 절대 삭제하지 않으며, `(source_type, resource_id)` 복합키 기준으로 `target_type`, `sha1_hash`, `remote_target_id`, `status = 'UPLOADED'`만 기록한다.
   - 나중에 사용자가 `delete` 명령을 실행하면:
     - `target_type == 'immich'` 항목은 Immich `bulk-upload-check`로 해당 `sha1_hash`가 실제 존재하는지 재검증
     - `target_type == 'nextcloud'` 항목은 Nextcloud WebDAV `HEAD` 요청으로 해당 경로에 파일이 동일한 바이트 크기(`Content-Length`)로 존재하는지 재검증
     - 재검증을 통과한 파일만 원본 클라우드(`MyboxProvider`, `GoogleDriveProvider` 등)의 휴지통 이동 API를 호출하고 `status = 'DELETED'`로 갱신한다.

---

## 2. 시스템 아키텍처 및 디렉터리 구조

```text
mybox-immich-sync/
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── pyproject.toml
├── README.md
├── src/
│   └── mybox_sync/
│       ├── __init__.py
│       ├── config.py                  # 멀티 소스(MYBOX/GDrive/Takeout) 및 멀티 타겟(Immich/Nextcloud) 설정
│       ├── db.py                      # aiosqlite WAL 모드 체크포인트 DB ((source_type, resource_id) 복합키)
│       ├── limiter.py                 # 비동기 Token Bucket Rate Limiter & 지수 백오프 재시도
│       ├── router.py                  # 파일 확장자/타입 기반 TargetRouter ('immich' vs 'nextcloud')
│       ├── sources/
│       │   ├── __init__.py
│       │   ├── base.py                # SourceProvider Protocol & RemoteItem 데이터 모델
│       │   ├── mybox.py               # 네이버 MYBOX Open API 플러그인
│       │   ├── gdrive.py              # Google Drive API v3 플러그인 (서버 SHA-1 사전 체크 포함)
│       │   └── takeout.py             # Google Takeout ZIP 스트리밍 + JSON 메타데이터 복원 플러그인
│       ├── targets/
│       │   ├── __init__.py
│       │   ├── base.py                # TargetProvider Protocol
│       │   ├── immich.py              # 셀프호스팅 Immich REST API 타겟 플러그인
│       │   └── nextcloud.py           # 셀프호스팅 Nextcloud WebDAV 타겟 플러그인
│       ├── pipeline.py                # scan / sync / delete 비동기 멀티 워커 파이프라인
│       └── cli.py                     # CLI 엔트리포인트 (--source 선택 및 auto, scan, sync, delete, status)
└── tests/
    ├── test_db.py
    ├── test_limiter.py
    ├── test_sources_and_targets.py
    └── test_pipeline.py
```

---

## 3. SQLite 데이터베이스 스키마 (`/data/sync_state.db`)

### 3.1 `folders` 테이블 (소스별 폴더 트리 탐색 체크포인트)
| 컬럼명 | 타입 | 설명 |
|---|---|---|
| `source_type` | `TEXT NOT NULL` | `'mybox'` \| `'gdrive'` \| `'takeout'` \| `'icloud'` |
| `folder_id` | `TEXT NOT NULL` | 클라우드 폴더 ID (루트는 `'ROOT'`) |
| `path` | `TEXT NOT NULL` | 전체 폴더 경로 (예: `'/문서/2025'`) |
| `last_cursor` | `TEXT` | 페이지네이션 중단 시 재개할 커서(`cursor` / `nextPageToken`) |
| `scan_status` | `TEXT NOT NULL` | `'PENDING'` \| `'SCANNED'` \| `'FAILED'` |
| `updated_at` | `TEXT NOT NULL` | ISO-8601 갱신 시각 |
| **PK** | `PRIMARY KEY (source_type, folder_id)` | 소스별 폴더 고유 식별 |

### 3.2 `files` 테이블 (파일별 라우팅, 동기화 및 삭제 체크포인트)
| 컬럼명 | 타입 | 설명 |
|---|---|---|
| `source_type` | `TEXT NOT NULL` | `'mybox'` \| `'gdrive'` \| `'takeout'` \| `'icloud'` |
| `resource_id` | `TEXT NOT NULL` | 원본 클라우드 파일 고유 ID (삭제 시 사용) |
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

## 4. 플러그인별 핵심 동작 규격

### 4.1 입력 플러그인 (`SourceProvider`)
1. **`MyboxProvider` (`source_type='mybox'`)**:
   - `GET /v1/drive/resources` 및 `/v1/drive/folders/{folderId}/resources`로 트리 탐색
   - `GET /v1/drive/files/{fileId}/download`로 `tmpfs`에 스트리밍 다운로드하며 `SHA-1` 계산
   - `DELETE /v1/drive/resources/{resourceId}`로 휴지통 이동
2. **`GoogleDriveProvider` (`source_type='gdrive'`)**:
   - `GET https://www.googleapis.com/drive/v3/files` (`fields="nextPageToken,files(id,name,mimeType,size,sha1Checksum,createdTime,modifiedTime,parents)"`, `q="trashed = false"`)
   - **사전 해시 기록**: 스캔 단계에서 이미 `sha1Checksum`을 받아 DB에 저장하므로, `sync` 단계에서 다운로드 전에 Immich `bulk-upload-check`를 먼저 호출하여 중복 사진은 다운로드조차 생략!
   - 다운로드: 일반 파일은 `?alt=media`, Google Docs/Sheets/Slides는 `/export` 엔드포인트로 `.docx`/`.xlsx`/`.pptx` 변환 다운로드
   - 삭제: `PATCH https://www.googleapis.com/drive/v3/files/{fileId}` (`{"trashed": true}`)로 안전하게 구글 드라이브 휴지통 이동
3. **`GoogleTakeoutProvider` (`source_type='takeout'`)**:
   - 지정된 디렉터리의 `Takeout-*.zip` 파일들을 압축 해제 없이 순회하며 내부 `.json` 메타데이터(`photoTakenTime.timestamp`)를 파싱해 원본 촬영 시각을 복원하고, 파일 스트림과 SHA-1 해시를 추출해 Immich/Nextcloud로 전달한다.

### 4.2 출력 플러그인 (`TargetProvider`)
1. **`ImmichTarget` (`target_type='immich'`)**:
   - `check_exists(items)`: `POST /api/assets/bulk-upload-check`로 SHA-1 해시 중복 여부 일괄 확인
   - `upload(...)`: `POST /api/assets`로 멀티파트 업로드 (`fileCreatedAt`, `fileModifiedAt` 보존)
2. **`NextcloudTarget` (`target_type='nextcloud'`)**:
   - 기본 경로: `{NEXTCLOUD_URL}/remote.php/dav/files/{NEXTCLOUD_USER}/{NEXTCLOUD_BASE_DIR}/{parent_path}/{name}`
   - `check_exists(item)`: `HEAD` 요청으로 파일 존재 여부 및 `Content-Length == size` 일치 여부 확인
   - `upload(...)`: 상위 폴더가 없으면 `MKCOL`로 재귀 생성(캐시 적용) 후, `PUT` 요청과 `X-OC-MTime` 헤더로 원본 수정일을 유지하여 스트리밍 업로드
