# Cloud-Prism (`mybox-immich-sync`)

TrueNAS SCALE 환경에 최적화된 **멀티 클라우드(네이버 MYBOX · Google Drive · Google Takeout) → NAS(Immich · Nextcloud) 자동 분류 이주 및 검증 기반 사후 삭제(Deferred Delete) 웹 앱 컨테이너**입니다.

---

## 주요 특징

1. **내장형 실시간 웹 대시보드 (`:3080`)**:
   - 별도의 프론트엔드 컨테이너 없이 Rust(`axum`) 단일 바이너리에 반응형 한국어 대시보드를 내장했습니다.
   - 컨테이너 기동 시 **`Idle`(대기 상태, CPU 0%)**로 시작하며, 웹 UI에서 설정 입력 · 연결 테스트 · 소스별 동기화/일시정지/원격 삭제를 명시적으로 제어할 수 있습니다.
   - **SSE(`GET /api/events`)** 실시간 스트리밍으로 진행률(%), 초당 처리 속도, 현재 파일명 및 최근 로그를 실시간 표시합니다.
2. **이중 설정 주입 (환경변수 + SQLite 영구 저장)**:
   - TrueNAS SCALE Custom App 배포 시 입력한 환경변수를 기본값으로 로드하며, 웹 UI **[⚙️ 연결 및 성능 설정]** 패널에서 수정·저장한 값은 `/data/sync_state.db`의 `settings` 테이블에 영구 보관되어 우선 적용됩니다.
3. **확장자 기반 자동 라우팅 (`TargetRouter`)**:
   - **사진·동영상** (`.jpg`, `.heic`, `.png`, `.dng`, `.mp4`, `.mov` 등): **Immich**로 이주 (`bulk-upload-check` SHA-1 중복 사전 검사로 중복 파일 업로드 스킵, Google Drive는 서버 `sha1Checksum`으로 무다운로드 즉시 스킵)
   - **일반 문서·파일** (`.pdf`, `.docx`, `.xlsx`, `.hwp`, `.hwpx`, `.zip` 등): 원본 폴더 트리와 수정 시각(`X-OC-MTime`)을 보존하여 **Nextcloud WebDAV**로 이주
4. **TrueNAS ZFS 보호 (`tmpfs` + RAII `TempFileGuard`)**:
   - 모든 임시 다운로드는 `/tmp/mybox_sync`(`tmpfs` RAM 디스크)에 스트리밍하면서 동시에 1-Pass로 SHA-1 해시를 계산하고, 작업 종료(또는 에러/패닉) 즉시 `TempFileGuard`가 파일을 자동 삭제하여 ZFS CoW 디스크 마모를 방지합니다.
5. **100% 타겟 재검증 후 원격 휴지통 삭제 (`delete` 모드)**:
   - 동기화(`sync`) 단계에서는 절대 원본 클라우드 파일을 삭제하지 않습니다.
   - 이주 완료 후 사용자가 명시적으로 원격 삭제를 실행하면, 삭제 직전 Immich 해시 존재 여부 또는 Nextcloud 파일 크기(`HEAD`)를 재검증한 뒤 통과한 파일만 원격 클라우드 휴지통으로 이동합니다.

---

## TrueNAS SCALE 실행 방법

```bash
docker compose up -d --build
```

브라우저에서 `http://<TrueNAS-IP>:3080`으로 접속하여 **[⚙️ 연결 및 성능 설정]**에서 `[🔌 전체 연결 테스트]`를 수행한 뒤 동기화를 시작하세요.

### CLI 서브커맨드 호환 지원

```bash
# 웹 서버 기동 (기본값)
mybox-sync serve --port 3080

# CLI 배치 실행
mybox-sync auto --source mybox
mybox-sync scan --source mybox
mybox-sync sync --source mybox
mybox-sync delete --source mybox --dry-run
mybox-sync status --source mybox
```
