# Cloud-Prism (`mybox-immich-sync`)

TrueNAS SCALE 환경에 최적화된 **멀티 클라우드(네이버 MYBOX · Google Takeout 포토/드라이브 · Google Drive) → NAS(Immich · Nextcloud) 자동 분류 이주 및 검증 기반 사후 삭제(Deferred Delete) 웹 앱 컨테이너**입니다.

---

## 주요 특징

1. **내장형 3단계 안전 워크플로우 웹 대시보드 (`:3080`)**:
   - 별도의 프론트엔드 컨테이너 없이 Rust(`axum`) 단일 바이너리에 반응형 한국어 대시보드(다크/라이트 테마)를 내장했습니다.
   - 컨테이너 기동 시 **`Idle`(대기 상태, CPU 0%)**로 시작하며, **`[1. 연결 및 설정]` → `[2. 데이터 동기화]` → `[3. 원격 정리]`** 순으로 타겟과 소스 연결이 검증된 경우에만 작업 탭이 활성화됩니다.
   - **SSE(`GET /api/events`)** 실시간 스트리밍으로 진행률(%), 초당 처리 속도, 예상 남은 시간(ETA), 현재 파일명 및 최근 로그/실패 내역을 실시간 표시합니다.
   - 샘플 테스트 실행(`1개`, `5개`, `10개`, `50개`, `전체`)을 지원하여 대량 이주 전 소량 파일로 안전하게 동작을 검증할 수 있습니다.

2. **Zero-Env 배포 & `/data/settings.json` 영구 보안 저장**:
   - 복잡한 환경변수 설정 없이 포트(`3080`)와 `/data` 볼륨만 마운트해 컨테이너를 띄우면 됩니다.
   - 웹 대시보드의 **`[연결 및 설정]`** 탭에서 MYBOX PAT 토큰, Google Takeout 경로/업로드, Immich URL/API Key, Nextcloud 계정 정보, 동시성 및 자동 실행 주기를 직접 입력·저장할 수 있습니다.
   - 저장된 설정은 `/data/settings.json`에 보관(Unix `0600` 소유자 전용 권한 자동 적용)되며, 비밀번호·토큰 원문은 브라우저로 다시 노출되지 않습니다.
   - **동적 자동 스케줄러**: 웹 UI에서 자동 동기화 주기(`sync_interval_minutes`)를 변경하면 컨테이너 재시작 없이 즉시 백그라운드 타이머에 반영됩니다.

3. **확장자 기반 자동 라우팅 (`TargetRouter`) & 소스별 폴더 분리**:
   - **사진·동영상** (`.jpg`, `.heic`, `.png`, `.dng`, `.mp4`, `.mov` 등): **Immich**로 이주 (`bulk-upload-check` SHA-1 중복 사전 검사로 중복 파일 업로드 스킵, 오래된 촬영/생성 시각 `createdAt asc` 순 처리)
   - **일반 문서·파일** (`.pdf`, `.docx`, `.xlsx`, `.hwp`, `.hwpx`, `.zip` 등): 원본 폴더 트리와 수정 시각(`X-OC-MTime`)을 보존하여 **Nextcloud WebDAV**의 소스별 전용 루트(`/MyBox/...`, `/GoogleDrive/...`)로 이주

4. **Google 포토 · 드라이브 통합 이주 (Google Takeout 아카이브)**:
   - 웹 브라우저 드래그앤드롭 업로드(`.part` 임시 파일을 통한 원자적 저장), 브라우저 다운로드 링크 직접 가져오기(`POST /api/takeout/fetch-url`), 또는 NAS 폴더(`/data/takeout`) 마운트를 모두 지원합니다.
   - 분할된 다중 `.zip` 아카이브의 Central Directory 인덱스를 메모리에 캐싱($O(1)$ 탐색)하고, 다른 분할 압축 파일에 나뉘어 들어간 `.json` 메타데이터 사이드카까지 추적해 원본 촬영 시각을 복원합니다.
   - 모든 항목이 타겟(Immich/Nextcloud)에 업로드되고 존재 재검증을 통과하면 로컬 `.zip` 아카이브를 자동으로 삭제해 NAS 디스크 공간을 회수합니다.
   - **문서 유실 방지 가드**: Nextcloud를 비활성화(`enable_nextcloud = false`)한 상태에서 Takeout ZIP 내에 일반 문서 파일이 포함되어 있으면, 사진만 Immich로 백업한 뒤 원본 ZIP 자동 삭제를 보류하고 안내 로그를 남깁니다.

5. **TrueNAS SCALE & ZFS 프로덕션 하드닝**:
   - **제로-버퍼링(Zero-Buffering) 스트리밍 업로드**: `tokio_util::io::ReaderStream` + `reqwest::Body::wrap_stream` 기반으로 수 GB의 4K 동영상도 힙 메모리(`Vec<u8>`) 전체 적재 없이 일정한 저메모리로 스트리밍 업로드합니다.
   - **하이브리드 임시 저장소 (`tmpfs` 2GB + 디스크 폴백)**: 256MB 미만 파일은 `/tmp/mybox_sync`(`tmpfs` RAM 디스크)에서 고속 처리해 ZFS CoW 마모를 방지하고, 256MB 이상 대용량 영상이나 `tmpfs` 용량 부족 시 자동으로 `/data/.tmp_sync` 디스크 볼륨으로 우회합니다. 작업 종료 시 `TempFileGuard`(RAII)가 즉시 임시 파일을 삭제하며, 컨테이너 기동 시 이전 비정상 종료 잔여 임시 파일을 자동 청소합니다.
   - **소스 vs 내부 LAN 타겟 RateLimiter 분리**: 외부 클라우드 API(`MYBOX` 등)는 설정된 `rate_limit_rps`를 엄격히 준수하고, 내부 LAN에 위치한 `Immich`/`Nextcloud` 타겟은 최소 50 RPS 이상의 별도 고속 리미터로 동작합니다.
   - **타임아웃 및 증분 스캔 정합성**: HTTP 연결(15초)·TCP Keepalive(30초)·다운로드 스트림 청크당 읽기 타임아웃(60초)을 적용하여 Half-open 연결 멈춤을 방지하고, 증분 재스캔 시 원격에서 직접 삭제된 파일 정리 및 메타데이터 0바이트 문서의 완료 상태를 보존합니다.

6. **100% 타겟 벌크 재검증 후 원격 휴지통 삭제 (`delete` 모드)**:
   - 동기화(`sync`) 단계에서는 절대 원격 클라우드 원본 파일을 삭제하지 않습니다.
   - 이주 완료 후 사용자가 명시적으로 원격 정리를 실행하면, 삭제 직전 Immich 500건 단위 벌크 해시 검증(`check_bulk_hashes`) 또는 Nextcloud 파일 크기(`HEAD Content-Length`)를 재검증한 뒤 통과한 파일만 원격 클라우드 휴지통으로 이동합니다.

---

## TrueNAS SCALE 실행 방법

```bash
docker compose up -d --build
```

브라우저에서 `http://<TrueNAS-IP>:3080`으로 접속하여 **`[1. 연결 및 설정]`** 탭에서 타겟(Immich / Nextcloud)과 소스(MYBOX / Google Takeout)의 **`[저장 및 연결 확인]`**을 수행한 뒤 동기화를 시작하세요.

### CLI 서브커맨드 호환 지원

```bash
# 웹 서버 기동 (기본값)
mybox-sync serve --port 3080

# CLI 배치 실행 (--limit N 옵션으로 샘플 테스트 가능)
mybox-sync auto --source mybox
mybox-sync scan --source mybox
mybox-sync sync --source mybox
mybox-sync delete --source mybox --dry-run
mybox-sync status --source mybox
```
