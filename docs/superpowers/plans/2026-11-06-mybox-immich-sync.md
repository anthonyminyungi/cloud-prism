# Multi-Cloud (MYBOX, Google Drive, Google Takeout) to NAS (Immich & Nextcloud) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a modular, plugin-based async Python CLI & Docker Compose engine optimized for TrueNAS SCALE that migrates photos/videos to self-hosted Immich and general files/documents to self-hosted Nextcloud from Naver MYBOX, Google Drive, and Google Takeout archives, backed by a SQLite (`WAL`) checkpoint database and hash/size-verified deferred remote deletion.

**Architecture:**
- **Source Plugins (`SourceProvider`)**: `MyboxProvider` (Naver MYBOX Open API), `GoogleDriveProvider` (Google Drive API v3 with server-side `sha1Checksum` zero-download deduplication), and `GoogleTakeoutProvider` (zero-unzip streaming + JSON sidecar metadata restoration).
- **Router (`TargetRouter`)**: Routes media extensions to `ImmichTarget` and all other files (preserving folder hierarchy) to `NextcloudTarget`.
- **Target Plugins (`TargetProvider`)**: `ImmichTarget` (SHA-1 `bulk-upload-check` + multipart upload) and `NextcloudTarget` (WebDAV `MKCOL` + `PUT` with `X-OC-MTime` + `HEAD` verification).
- **Checkpoint Engine (`CheckpointDB` + `SyncPipeline`)**: `asyncio` multi-worker pipeline with token-bucket rate limiting, `tmpfs` RAM-disk streaming on TrueNAS SCALE, and `exit(0)` auto-shutdown upon completion.

**Tech Stack:** Python 3.11+, `httpx`, `aiosqlite`, `rich`, `python-dotenv`, `pytest`, `pytest-asyncio`, `respx`, Docker Compose.

## Global Constraints

- Python >= 3.11
- SQLite must use `PRAGMA journal_mode=WAL;` and composite primary keys `(source_type, folder_id)` and `(source_type, resource_id)`
- Temporary downloads must stream into `/tmp/mybox_sync` (`tmpfs` in Docker) and always delete `.part` files in `finally` blocks
- `sync` mode must NEVER delete source files; `delete` mode must re-verify existence in the respective target (`ImmichTarget` via SHA-1 bulk check, `NextcloudTarget` via WebDAV `HEAD` size check) before calling source delete
- Container exits with code `0` when `auto`, `scan`, `sync`, or `delete` finishes

---

### Task 1: Project Scaffolding, Multi-Cloud Config, Router, and Rate Limiter

**Files:**
- Create: `pyproject.toml`
- Create: `src/mybox_sync/__init__.py`
- Create: `src/mybox_sync/config.py`
- Create: `src/mybox_sync/router.py`
- Create: `src/mybox_sync/limiter.py`
- Test: `tests/test_limiter.py`

**Interfaces:**
- Produces:
  - `Config` dataclass in `mybox_sync.config`
  - `TargetRouter.classify(filename: str, enable_nextcloud: bool = True) -> str | None` returning `'immich'`, `'nextcloud'`, or `None`
  - `RateLimiter` in `mybox_sync.limiter`

- [ ] **Step 1: Write `pyproject.toml` and failing test `tests/test_limiter.py`**

```python
# tests/test_limiter.py
import httpx
import pytest
from mybox_sync.config import Config
from mybox_sync.router import TargetRouter
from mybox_sync.limiter import RateLimiter


def test_router_classifies_media_and_general_files() -> None:
    assert TargetRouter.classify("IMG_001.HEIC", enable_nextcloud=True) == "immich"
    assert TargetRouter.classify("video.mp4", enable_nextcloud=False) == "immich"
    assert TargetRouter.classify("report.pdf", enable_nextcloud=True) == "nextcloud"
    assert TargetRouter.classify("report.pdf", enable_nextcloud=False) is None


async def test_config_and_rate_limiter_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IMMICH_URL", "http://192.168.0.10:2283/")
    monkeypatch.setenv("IMMICH_API_KEY", "immich-key")
    monkeypatch.setenv("NEXTCLOUD_URL", "http://192.168.0.10:8080/")
    monkeypatch.setenv("NEXTCLOUD_USER", "admin")
    monkeypatch.setenv("NEXTCLOUD_APP_PASSWORD", "nc-pass")
    cfg = Config.from_env(require_credentials=False)
    assert cfg.immich_url == "http://192.168.0.10:2283"
    assert cfg.nextcloud_enabled is True

    limiter = RateLimiter(rps=50.0, max_retries=3, base_delay=0.01)
    calls = 0

    async def flaky() -> httpx.Response:
        nonlocal calls
        calls += 1
        req = httpx.Request("GET", "https://example.com")
        return httpx.Response(429 if calls < 2 else 200, request=req)

    resp = await limiter.request_with_retry(flaky)
    assert resp.status_code == 200
    assert calls == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_limiter.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `config.py`, `router.py`, and `limiter.py`**

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_limiter.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

---

### Task 2: Multi-Source & Multi-Target SQLite WAL Checkpoint Database (`db.py`)

**Files:**
- Create: `src/mybox_sync/db.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Produces: `CheckpointDB` supporting `source_type` (`'mybox'`, `'gdrive'`, `'takeout'`) and `target_type` (`'immich'`, `'nextcloud'`) with pre-populated `sha1_hash` support (for Google Drive zero-download dedup).

- [ ] **Step 1: Write failing test `tests/test_db.py`**
- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `src/mybox_sync/db.py`**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 3: Source Plugins (`MyboxProvider`, `GoogleDriveProvider`, `GoogleTakeoutProvider`) & Target Plugins (`ImmichTarget`, `NextcloudTarget`)

**Files:**
- Create: `src/mybox_sync/sources/__init__.py`
- Create: `src/mybox_sync/sources/base.py`
- Create: `src/mybox_sync/sources/mybox.py`
- Create: `src/mybox_sync/sources/gdrive.py`
- Create: `src/mybox_sync/sources/takeout.py`
- Create: `src/mybox_sync/targets/__init__.py`
- Create: `src/mybox_sync/targets/base.py`
- Create: `src/mybox_sync/targets/immich.py`
- Create: `src/mybox_sync/targets/nextcloud.py`
- Test: `tests/test_sources_and_targets.py`

**Interfaces:**
- Produces:
  - `SourceProvider` protocol and concrete providers (`MyboxProvider`, `GoogleDriveProvider`, `GoogleTakeoutProvider`)
  - `ImmichTarget` (SHA-1 bulk check & asset upload)
  - `NextcloudTarget` (WebDAV `MKCOL` recursive folder creation, `PUT` upload with `X-OC-MTime` timestamp preservation, and `HEAD` size verification)

- [ ] **Step 1: Write failing tests in `tests/test_sources_and_targets.py` covering MYBOX, Google Drive, Google Takeout ZIP+JSON, Immich, and Nextcloud WebDAV**
- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `sources/` and `targets/` modules**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 4: Unified Async Pipeline (`scan`, `sync`, `delete`) and CLI

**Files:**
- Create: `src/mybox_sync/pipeline.py`
- Create: `src/mybox_sync/cli.py`
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Produces:
  - `SyncPipeline` routing media to `ImmichTarget` (with zero-download skip if `sha1_hash` is already known from Google Drive scan!) and general files to `NextcloudTarget`, plus target-specific verification before remote deletion.
  - `cli.py` supporting `--source {mybox,gdrive,takeout}` and commands `auto`, `scan`, `sync`, `delete [--dry-run]`, `status`.

- [ ] **Step 1: Write failing end-to-end test in `tests/test_pipeline.py`**
- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `src/mybox_sync/pipeline.py` and `src/mybox_sync/cli.py`**
- [ ] **Step 4: Run test to verify it passes**
- [ ] **Step 5: Commit**

---

### Task 5: TrueNAS SCALE Docker Compose (`tmpfs` + Auto-Exit) & Documentation

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `README.md`

- [ ] **Step 1: Create `Dockerfile`, `docker-compose.yml`, `.env.example`, and `README.md`**
- [ ] **Step 2: Verify full test suite passes (`pytest -v`)**
- [ ] **Step 3: Commit**
