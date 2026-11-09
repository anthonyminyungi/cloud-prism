# Cloud Prism 웹 대시보드 미니멀 UI 개편 설계서

- **작성일**: 2026-11-09
- **대상 파일**: `src/ui.html` (필요 시 `tests/web_api_test.rs` 검증 보강)
- **목표**: 과도한 이모지·네온 그라데이션·카드 남발 등 전형적인 AI 생성형 UI 느낌을 제거하고, 핵심 제어와 상태 정보만 명확히 전달하는 미니멀 워크벤치(Linear / Vercel 스타일) 인터페이스로 개편한다.

---

## 1. 디자인 원칙 및 컬러/타이포그래피 토큰

1. **장식 요소 배제**:
   - 버튼, 탭, 타이틀, 로그의 모든 이모지 제거
   - 프로그레스 바 및 배경의 그라데이션 제거, `1px solid` 실선과 단색 면으로만 시각적 위계 구성
2. **라이트/다크 테마 지원 (`prefers-color-scheme` + 수동 토글)**:
   - `html[data-theme="dark"]` / `html[data-theme="light"]` 속성 및 `localStorage('cp_theme')` 연동
   - **Dark 토큰**: `--bg: #09090b`, `--surface: #121215`, `--surface-hover: #1c1c21`, `--border: #27272a`, `--text: #fafafa`, `--muted: #a1a1aa`
   - **Light 토큰**: `--bg: #fafafa`, `--surface: #ffffff`, `--surface-hover: #f4f4f5`, `--border: #e4e4e7`, `--text: #09090b`, `--muted: #71717a`
   - **상태 색상**: `--ok: #10b981`, `--warn: #f59e0b`, `--err: #ef4444` (6px 상태 인디케이터 점 및 경고 텍스트에만 제한적 사용)
3. **타이포그래피**:
   - 본문: `-apple-system, BlinkMacSystemFont, "Pretendard", "Inter", sans-serif`
   - 수치/로그/파일 경로: `ui-monospace, SFMono-Regular, Menlo, Consolas, monospace` 및 `font-variant-numeric: tabular-nums` 적용

---

## 2. 레이아웃 및 컴포넌트 구조 (`max-width: 960px`)

### 2.1 상단 헤더 & 접이식 환경 패널
- **좌측**: 서비스 식별자(`Cloud Prism`) + 인라인 소스 세그먼트 탭 (`MYBOX`, `Google Drive`, `Takeout`)
- **우측**:
  - 작업 상태 배지 (상태 점 + `Idle` / `Scanning` / `Syncing` / `Deleting` / `Stopping`)
  - `환경 설정` 토글 버튼 (클릭 시 하단 드로어 토글)
  - 테마 전환 버튼 (`Dark` / `Light`)
- **환경 설정 드로어 (기본 숨김)**:
  - 환경변수 구성 요약 (`MYBOX`, `GDrive`, `Takeout`, `Immich`, `Nextcloud`, `Worker/RPS`, `Schedule`)
  - `연결 테스트` 버튼 및 테스트 결과 인라인 출력 (`/api/connections/test`)

### 2.2 메인 컨트롤 & 진행률 바
- **좌측 상태 요약**:
  - 현재 처리 중인 파일명 또는 대기 안내 문구 (한 줄 말줄임 `text-overflow: ellipsis`)
  - 진행률(`%`), 처리 속도(`파일/s`), 예상 남은 시간(`ETA`)
- **우측 액션 그룹**:
  - 대기(`idle`) 상태: 주 버튼 **`동기화 시작`**(`mode: 'auto'`) + 보조 드롭다운 버튼 **`더보기 ▾`**
  - 실행 중(`scanning`/`syncing`/`deleting`) 상태: 주 버튼을 **`일시정지`**(`stopJob()`)로 전환
  - **더보기 드롭다운 메뉴**:
    - `스캔만 실행` (`mode: 'scan'`)
    - `동기화만 실행` (`mode: 'sync'`)
    - 구분선
    - `원격 삭제 시뮬레이션 (Dry-run)` (`mode: 'delete', dry_run: true`)
    - `완료 항목 원격 삭제...` (확인 모달 오픈)
- **하단 프로그레스 바**: 컨트롤 바 바닥에 밀착된 `3px` 높이의 단색 바

### 2.3 인라인 메트릭 스트립 (1줄 4열)
- 단일 보더 컨테이너 내부를 세로선(`1px solid var(--border)`)으로 4분할:
  1. **대기 중 (Pending)**: 대기 파일 건수 / 스캔 폴더 수 · 대기 용량
  2. **Immich 이주 완료**: 완료 건수 / 신규 · 중복 스킵 건수
  3. **Nextcloud 이주 완료**: 완료 건수 / 누적 이주 용량
  4. **원격 삭제 · 실패**: 삭제 건수(절감 용량) 및 실패 건수 (실패 > 0일 때 인라인 `재시도` 버튼 표시)

### 2.4 하단 통합 뷰어 (실시간 로그 / 실패 목록 탭)
- **헤더 바**:
  - 좌측 탭: `실시간 로그` | `실패 항목 (N)` (실패 건수가 있을 때만 강조 배지 표시)
  - 우측: 실패 탭 선택 시 `실패 항목 전체 재시도` 버튼, 로그 탭 선택 시 `로그 지우기` 버튼
- **본문 영역 (`height: 320px`, 고정폭 폰트)**:
  - `실시간 로그`: `[HH:MM:SS]` 타임스탬프와 메시지 출력 (`/api/events` SSE 및 초기 `recent_logs` 연동)
  - `실패 항목`: `recent_failures` 목록(타겟, 경로/파일명, 재시도 횟수, 에러 메시지)을 정돈된 리스트로 표시

### 2.5 원격 삭제 확인 모달
- `완료 항목 원격 삭제...` 클릭 시 노출되는 간결한 확인 대화상자 (`취소` / `삭제 실행`)

---

## 3. 데이터 흐름 및 예외 처리
- 기존 백엔드(`src/web.rs`) API 계약을 변경 없이 그대로 사용한다.
- 작업 실행 중(`state !== 'idle'`)에는 중복 시작 방지를 위해 시작 버튼 및 드롭다운 내 작업 실행 항목을 비활성화(`disabled`) 처리한다.
- 드롭다운 메뉴는 외부 영역 클릭 또는 항목 선택 시 자동으로 닫힌다.

---

## 4. 테스트 및 검증 계획
- `cargo test`를 통해 기존 웹 라우터 및 통합 테스트(`tests/web_api_test.rs`) 통과 확인
- 루트 페이지(`GET /`) 응답에 핵심 UI 요소 및 스크립트 엔드포인트 연동이 정상 포함되는지 테스트 검증
