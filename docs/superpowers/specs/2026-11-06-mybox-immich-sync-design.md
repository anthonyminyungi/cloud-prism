# Naver MYBOX to Immich 비동기 동기화 및 사후 일괄 삭제 도구 설계 명세서 (Design Spec)

- **작성일**: 2026-11-06
- **프로젝트 경로**: `C:\Users\dbstn\.gemini\antigravity\scratch\mybox-immich-sync`
- **목적**: 네이버 MYBOX에 남아있는 대량의 사진/동영상을 Open API(`https://open-api.mybox.naver.com/v1`)를 통해 자작 NAS의 셀프호스팅 Immich 서버로 비동기 동기화(중복 제거 포함)하고, SQLite 체크포인트 및 SHA-1 해시 검증을 거쳐 안전하게 사후 일괄 원격 삭제(휴지통 이동)를 수행하는 Docker Compose 기반 백그라운드 자동화 도구 구축.

---

## 1. 핵심 요구사항 및 설계 원칙

1. **Docker Compose 기반 백그라운드 실행 및 완료 시 자동 종료**:
   - `docker compose up -d`로 실행 시 터미널을 블로킹하지 않고 백그라운드에서 `scan` → `sync` 파이프라인을 자동 수행한다.
   - `restart: on-failure:3` 정책을 사용하여 일시적인 네트워크 장애로 비정상 종료된 경우에는 컨테이너가 자동으로 재기동하여 SQLite 체크포인트부터 이어서 작업하고, **전체 동기화 작업이 100% 완료되어 정상 종료(`exit code 0`)되면 컨테이너가 자동으로 내려간다(Stopped).**
2. **비동기(`asyncio`) 멀티 워커 파이프라인 & Rate Limit 제어**:
   - `httpx.AsyncClient`와 `asyncio.Queue`를 활용해 다수의 워커(기본 3개, 설정 가능)가 병렬로 다운로드·해시 계산·Immich 업로드를 수행한다.
   - 토큰 버킷(Token Bucket) 방식의 Rate Limiter와 지수 백오프(Exponential Backoff, `429 Too Many Requests` / `5xx` 대응)를 적용해 네이버 MYBOX API 차단을 방지한다.
3. **스트리밍 SHA-1 해시 계산 및 중복 업로드 생략 최적화**:
   - 이미 아이폰에서 Immich로 동기화된 사진이 많으므로, MYBOX에서 임시 파일로 스트리밍 다운로드하면서 동시에 메모리에서 `SHA-1` 해시를 계산한다.
   - 파일 본문을 Immich에 업로드하기 전에 `POST /api/assets/bulk-upload-check`로 해시 중복 여부를 먼저 질의하여, 이미 존재하는 사진(`duplicate`)은 파일 업로드 과정을 생략하고 즉시 완료(`UPLOADED`) 처리한다.
4. **SQLite(`WAL` 모드) 체크포인트 및 사후 일괄 삭제 분리**:
   - 동기화(`sync`) 단계에서는 MYBOX 원본을 절대 삭제하지 않고 SQLite DB(`/data/sync_state.db`)에 파일별 `resource_id`, `sha1_hash`, `immich_asset_id`, `status = 'UPLOADED'`만 기록한다.
   - 사용자가 원하는 시점에 삭제 명령(`delete`)을 실행하면, SQLite의 `UPLOADED` 항목들을 대상으로 Immich 서버에 해당 `sha1_hash`가 실제로 존재하는지 재검증(`bulk-upload-check`)한 뒤에만 MYBOX 삭제 API(`DELETE /v1/drive/resources/{resourceId}`)를 호출해 휴지통으로 이동시키고 컨테이너를 자동 종료한다.
5. **앨범 미생성(타임라인 통합) 기본 정책**:
   - MYBOX 사진 대다수가 `자동업로드` 폴더에 위치하므로 기본적으로 별도 앨범을 생성하지 않고 파일의 EXIF 및 원본 수정 시각(`modified_time`)을 보존하여 Immich 타임라인에 통합한다.

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
│       ├── config.py          # 환경변수 및 런타임 설정 로드/검증
│       ├── db.py              # aiosqlite 기반 WAL 모드 체크포인트 DB 관리
│       ├── limiter.py         # 비동기 Token Bucket Rate Limiter & Backoff 재시도 래퍼
│       ├── mybox_client.py    # 네이버 MYBOX Open API 비동기 클라이언트
│       ├── immich_client.py   # 셀프호스팅 Immich REST API 비동기 클라이언트
│       ├── pipeline.py        # scan / sync / delete 비동기 멀티 워커 파이프라인
│       └── cli.py             # CLI 엔트리포인트 (auto, scan, sync, delete, status)
└── tests/
    ├── test_db.py
    ├── test_limiter.py
    ├── test_clients.py
    └── test_pipeline.py
```

---

## 3. SQLite 데이터베이스 스키마 (`/data/sync_state.db`)

멀티 워커 동시성 보장과 실행 중 외부에서 `status` 조회가 가능하도록 연결 시 `PRAGMA journal_mode=WAL;` 및 `PRAGMA synchronous=NORMAL;`을 활성화한다.

### 3.1 `folders` 테이블 (폴더 트리 탐색 체크포인트)
| 컬럼명 | 타입 | 설명 |
|---|---|---|
| `folder_id` | `TEXT PRIMARY KEY` | MYBOX 폴더 ID (루트는 `'ROOT'`) |
| `path` | `TEXT NOT NULL` | 전체 폴더 경로 (예: `'/자동업로드/2023'`) |
| `last_cursor` | `TEXT` | 페이지네이션 중단 시 재개할 커서 토큰 |
| `scan_status` | `TEXT NOT NULL` | `'PENDING'` \| `'SCANNED'` \| `'FAILED'` |
| `updated_at` | `TEXT NOT NULL` | ISO-8601 갱신 시각 |

### 3.2 `files` 테이블 (사진/영상 파일 동기화 및 삭제 체크포인트)
| 컬럼명 | 타입 | 설명 |
|---|---|---|
| `resource_id` | `TEXT PRIMARY KEY` | MYBOX 파일 고유 ID (원격 삭제 시 사용) |
| `name` | `TEXT NOT NULL` | 파일명 (예: `'IMG_1234.HEIC'`) |
| `parent_path` | `TEXT NOT NULL` | 상위 폴더 경로 |
| `size` | `INTEGER NOT NULL` | MYBOX상 파일 크기 (Bytes, 다운로드 무결성 검증용) |
| `created_time` | `TEXT` | MYBOX 파일 생성 시각 |
| `modified_time` | `TEXT NOT NULL` | MYBOX 파일 수정/촬영 시각 |
| `sha1_hash` | `TEXT` | 스트리밍 다운로드 중 계산된 SHA-1 해시 (Hex) |
| `immich_asset_id` | `TEXT` | Immich 서버 내 에셋 ID |
| `immich_status` | `TEXT` | `'created'` (신규 업로드) \| `'duplicate'` (기존 존재) |
| `status` | `TEXT NOT NULL` | `'PENDING'` → `'UPLOADED'` → `'DELETED'` (오류 시 `'FAILED'`) |
| `retry_count` | `INTEGER DEFAULT 0` | 실패 재시도 횟수 |
| `error_msg` | `TEXT` | 마지막 오류 메시지 |
| `updated_at` | `TEXT NOT NULL` | ISO-8601 갱신 시각 |

---

## 4. 외부 API 연동 규격

### 4.1 Naver MYBOX Open API (`https://open-api.mybox.naver.com/v1`)
- 공통 헤더: `Authorization: Bearer {MYBOX_PAT}`
1. **루트 목록 조회**: `GET /v1/drive/resources?count=1000&cursor={cursor}`
2. **하위 폴더 목록 조회**: `GET /v1/drive/folders/{folderId}/resources?count=1000&cursor={cursor}`
3. **다운로드 URL 발급**: `GET /v1/drive/files/{fileId}/download` → 반환된 임시 다운로드 URL로 스트리밍 다운로드 수행
4. **파일 삭제 (휴지통 이동)**: `DELETE /v1/drive/resources/{resourceId}` → `204 No Content` 확인

### 4.2 Self-Hosted Immich API (`{IMMICH_URL}/api`)
- 공통 헤더: `x-api-key: {IMMICH_API_KEY}`
1. **해시 기반 중복 사전/사후 일괄 확인**:
   - `POST /api/assets/bulk-upload-check`
   - Body: `{"assets": [{"id": "<resource_id>", "checksum": "<sha1_hex_or_base64>"}]}`
   - 응답 결과가 `action: "reject", reason: "duplicate"`이면 반환된 `assetId`를 기록하고 업로드 생략.
2. **신규 미디어 업로드**:
   - `POST /api/assets` (`multipart/form-data`)
   - 필드: `assetData`(파일 바이너리), `deviceAssetId`(`mybox-{resource_id}`), `deviceId`(`"MYBOX-SYNC"`), `fileCreatedAt`, `fileModifiedAt`
   - 헤더: `x-immich-checksum: {sha1_hex}`

---

## 5. 실행 모드 및 Docker Compose 생명주기

### 5.1 CLI 실행 모드 (`python -m mybox_sync.cli <command>`)
- **`auto` (Docker 기본 커맨드)**:
  1. 미완료 폴더가 있으면 `scan` 수행
  2. `PENDING` (및 재시도 가능한 `FAILED`) 파일에 대해 `sync` 수행
  3. 모든 파일이 `UPLOADED` 상태가 되면 최종 통계 로그를 출력하고 `exit(0)`으로 종료 → **컨테이너 자동 종료**
- **`scan`**: MYBOX 전체 폴더를 순회하며 미디어 파일만 SQLite에 `PENDING` 등록 후 종료
- **`sync`**: `PENDING` 파일들을 다운로드·해시 계산·Immich 업로드 후 `UPLOADED` 기록 및 임시 파일 삭제 후 종료
- **`delete` (`--dry-run` 지원)**:
  1. SQLite에서 `status = 'UPLOADED'`이고 `sha1_hash`가 존재하는 항목 조회
  2. 200개 단위 배치로 Immich `/api/assets/bulk-upload-check`를 호출하여 현재 Immich에 해당 해시가 존재하는지 재검증
  3. 검증 통과한 파일만 MYBOX `DELETE /v1/drive/resources/{resourceId}` 호출 후 `status = 'DELETED'`로 갱신
  4. 전체 삭제 완료 시 `exit(0)`으로 종료 → **컨테이너 자동 종료**
- **`status`**: 현재 DB 기준 스캔된 폴더 수, `PENDING` / `UPLOADED` (신규 vs 중복) / `DELETED` / `FAILED` 건수 및 총 용량 표 출력

### 5.2 TrueNAS SCALE (ZFS & Docker Compose) 최적화 구성
TrueNAS SCALE(Electric Eel 24.10+ 기본 Docker 엔진) 환경의 특성을 반영하여 다음 최적화를 적용한다:

1. **ZFS 디스크 쓰기(CoW) 방지를 위한 `tmpfs`(RAM 디스크) 활용**:
   - 수만 장의 사진을 임시 다운로드 후 삭제할 때 ZFS 풀(Pool)에 불필요한 쓰기/단편화나 스냅샷 용량 증가가 발생하지 않도록, 임시 다운로드 경로(`/tmp/mybox_sync`)는 호스트 디스크 대신 컨테이너 메모리(`tmpfs`)에 마운트한다. (동시 워커 3개 기준 수십 MB 메모리만 사용 후 즉시 해제됨)
2. **TrueNAS Dataset 경로 및 권한(`568:568` `apps` 계정) 호환**:
   - SQLite 체크포인트 DB(`sync_state.db`)만 영구 보존하면 되므로 `/mnt/<Pool이름>/mybox-sync/data:/data` 한 곳만 호스트 경로로 마운트한다.
   - SSH/Dockge용 `docker-compose.yml`(`build: .` 방식)과 TrueNAS 웹 UI(`Apps -> Custom App -> Install via YAML`)에서 별도 이미지 빌드 없이 바로 붙여넣어 쓸 수 있는 설정 예시를 모두 제공한다.

```yaml
services:
  mybox-sync:
    build: .
    container_name: mybox-immich-sync
    env_file: .env
    volumes:
      - ./data:/data          # TrueNAS 예: /mnt/tank/apps/mybox-sync/data:/data
    tmpfs:
      - /tmp/mybox_sync:size=1G,mode=1777  # ZFS 쓰기 방지를 위한 RAM 디스크 활용
    command: ["auto"]
    restart: on-failure:3     # 에러 시에만 최대 3회 재시작, 완료(exit 0) 시 컨테이너 자동 종료
```

---

## 6. 에러 처리 및 테스트 전략

1. **원자적 임시 파일 관리**:
   - 다운로드 중에는 `/tmp/mybox_sync/{resource_id}.part`에 기록하며, `try ... finally` 블록을 통해 업로드 성공/실패/컨테이너 종료(`SIGTERM`) 시에도 디스크에 임시 파일이 남지 않도록 즉시 삭제한다.
2. **다운로드 크기 검증**:
   - 스트리밍 다운로드 완료 후 실제 바이트 수가 MYBOX 메타데이터의 `size`(0보다 큰 경우)와 다르면 손상된 다운로드로 간주하고 업로드하지 않으며 재시도 큐로 넘긴다.
3. **안전한 종료 (Graceful Shutdown)**:
   - `SIGINT` / `SIGTERM` 수신 시 새로운 파일 작업을 중단하고, 현재 진행 중인 워커의 파일 처리가 끝나면 SQLite 커밋 후 안전하게 종료한다.
4. **단위 및 통합 테스트 (`pytest` + `pytest-asyncio` + `respx`)**:
   - SQLite 상태 전이 및 중단 후 재개(`Resume`) 테스트
   - Rate Limiter 및 `429` 응답 시 지수 백오프 동작 테스트
   - MYBOX / Immich Mock API를 활용한 `scan` → `sync`(신규 및 중복 케이스) → `delete`(해시 검증 성공 및 실패 케이스) 전체 파이프라인 검증
