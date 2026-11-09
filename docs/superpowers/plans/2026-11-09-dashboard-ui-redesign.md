# Web Dashboard Minimal UI Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign `src/ui.html` into a clean, emoji-free, gradient-free, minimal single-column workbench UI supporting system/manual dark-light themes, a collapsible environment panel, a single primary control with a secondary actions dropdown, a 4-column inline metric strip, and a unified log/failure viewer.

**Architecture:** Keep `src/web.rs` API endpoints unchanged (`GET /`, `GET /api/status`, `POST /api/jobs/start`, `POST /api/jobs/stop`, `POST /api/jobs/retry-failed`, `POST /api/connections/test`, `GET /api/events`) while replacing `src/ui.html` with a semantic, minimal Zinc-palette CSS variable system and clean vanilla JS state rendering. Extend `tests/web_api_test.rs` to verify the new UI structure and ensure no decorative emojis remain.

**Tech Stack:** Rust (`axum`), HTML5, CSS3 custom properties (`prefers-color-scheme` + `data-theme`), Vanilla JavaScript (`EventSource`, `fetch`).

## Global Constraints

- Zero emojis in `src/ui.html` (no status/button/card emojis).
- Zero CSS gradients (`linear-gradient` / `radial-gradient`) in `src/ui.html`.
- Preserve `"네이버 MYBOX"` and `"Immich"` strings in `src/ui.html` for backward compatibility with existing assertions in `tests/web_api_test.rs`.
- Maintain 100% compatibility with existing backend routes in `src/web.rs`.

---

### Task 1: Add UI structure assertions in `tests/web_api_test.rs` and implement minimal `src/ui.html`

**Files:**
- Modify: `tests/web_api_test.rs:71-83`
- Modify: `src/ui.html:1-464`
- Test: `tests/web_api_test.rs`

**Interfaces:**
- Consumes: `GET /`, `GET /api/status?source={source}`, `POST /api/connections/test`, `POST /api/jobs/start`, `POST /api/jobs/stop`, `POST /api/jobs/retry-failed`, `GET /api/events`
- Produces: Minimal workbench HTML in `src/ui.html` with `id="envDrawer"`, `id="actionDropdown"`, `id="metricStrip"`, `id="viewerTabs"`, `data-theme` toggle support, and zero emojis.

- [x] **Step 1: Update `tests/web_api_test.rs` to assert the redesigned minimal UI structure**

In `tests/web_api_test.rs`, expand the `GET /` assertions (around lines 78-82) to check for the new minimal workbench elements (`id="envDrawer"`, `id="actionDropdown"`, `id="metricStrip"`, `toggleTheme`) and verify that legacy decorative emojis (`📦`, `🚀`, `🗑️`) are absent:

```rust
    assert!(html.contains("네이버 MYBOX"));
    assert!(html.contains("Immich"));
    assert!(html.contains("id=\"envDrawer\""));
    assert!(html.contains("id=\"actionDropdown\""));
    assert!(html.contains("id=\"metricStrip\""));
    assert!(html.contains("toggleTheme"));
    assert!(!html.contains("📦"));
    assert!(!html.contains("🚀"));
```

- [x] **Step 2: Run test to verify it fails before updating `src/ui.html`**

Run: `cargo test --test web_api_test`
Expected: FAIL at `assert!(html.contains("id=\"envDrawer\""))`

- [x] **Step 3: Replace `src/ui.html` with the minimal single-column workbench implementation**

Update `src/ui.html` with:
- Light/Dark CSS tokens (`--bg`, `--surface`, `--surface-hover`, `--border`, `--text`, `--muted`, `--ok`, `--warn`, `--err`)
- Header with brand title `Cloud Prism`, inline source selector (`네이버 MYBOX`, `Google Drive`, `Google Takeout`), status indicator dot + text, `환경 설정` drawer toggle, and theme toggle button
- Collapsible `#envDrawer` containing environment status items and `연결 테스트` button + inline result
- Control bar with current file / status label, speed & ETA (`tabular-nums`), primary action button (`동기화 시작` when idle, `일시정지` when active), and `더보기` dropdown (`#actionDropdown`) for `스캔만 실행`, `동기화만 실행`, `원격 삭제 시뮬레이션 (Dry-run)`, and `완료 항목 원격 삭제...`
- 3px solid progress bar attached to the bottom edge of the control bar
- 4-column inline `#metricStrip` (`대기 중`, `Immich 완료`, `Nextcloud 완료`, `삭제 / 실패` with conditional inline `재시도` button)
- Unified bottom panel with tabs (`실시간 로그` and `실패 항목 (N)`), clear log button, and monospace log/failure list views
- Clean confirmation modal for remote deletion

- [x] **Step 4: Run test suite to verify all tests pass**

Run: `cargo test`
Expected: PASS across all unit and integration test suites (`db_test`, `pipeline_integration_test`, `router_limiter_test`, `web_api_test`).
