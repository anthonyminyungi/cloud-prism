# Naver MYBOX to Immich Async Sync & Deferred Delete Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Dockerized async multi-worker Python CLI tool (`mybox-immich-sync`) optimized for TrueNAS SCALE that scans Naver MYBOX via Open API, streams downloads into `tmpfs` while computing SHA-1 hashes, deduplicates/uploads to self-hosted Immich, checkpoints progress in SQLite (`WAL` mode), exits automatically on completion, and supports deferred batch remote deletion after hash re-verification.

**Architecture:** An `asyncio` pipeline backed by `aiosqlite` (WAL mode) and `httpx.AsyncClient` with a token-bucket rate limiter and exponential backoff. Commands are split into `scan`, `sync`, `auto` (`scan` + `sync`), `delete` (hash-verified batch trash move), and `status`, packaged with a TrueNAS SCALE ZFS-friendly `docker-compose.yml` (`tmpfs` RAM disk + `restart: on-failure:3`).

**Tech Stack:** Python 3.11+, `httpx`, `aiosqlite`, `rich`, `python-dotenv`, `pytest`, `pytest-asyncio`, `respx`, Docker Compose.

## Global Constraints

- Python >= 3.11
- Base URL for MYBOX Open API: `https://open-api.mybox.naver.com/v1` with `Authorization: Bearer {MYBOX_PAT}`
- Immich API authentication via `x-api-key: {IMMICH_API_KEY}`
- SQLite must enable `PRAGMA journal_mode=WAL;` and `PRAGMA synchronous=NORMAL;`
- Temporary downloads must use `/tmp/mybox_sync` (mounted as `tmpfs` on TrueNAS SCALE to avoid ZFS CoW disk wear) and always clean up in `finally` blocks
- Container exits with code `0` upon completing `auto`, `scan`, `sync`, or `delete` so Docker Compose stops the container automatically

---

### Task 1: Project Scaffolding, Configuration, and Rate Limiter

**Files:**
- Create: `pyproject.toml`
- Create: `src/mybox_sync/__init__.py`
- Create: `src/mybox_sync/config.py`
- Create: `src/mybox_sync/limiter.py`
- Test: `tests/test_limiter.py`

**Interfaces:**
- Consumes: Environment variables (`MYBOX_PAT`, `IMMICH_URL`, `IMMICH_API_KEY`, `CONCURRENCY`, `RATE_LIMIT_RPS`, `DB_PATH`, `TEMP_DIR`)
- Produces:
  - `Config` dataclass in `mybox_sync.config` with `from_env() -> Config`
  - `RateLimiter` class in `mybox_sync.limiter` with `async acquire() -> None` and `async request_with_retry(coro_factory: Callable[[], Awaitable[httpx.Response]]) -> httpx.Response`

- [ ] **Step 1: Write `pyproject.toml`, package init, and failing test `tests/test_limiter.py`**

```toml
# pyproject.toml
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.build_meta"

[project]
name = "mybox-immich-sync"
version = "0.1.0"
description = "Async Naver MYBOX to Immich sync and deferred batch delete tool"
readme = "README.md"
requires-python = ">=3.11"
dependencies = [
    "httpx>=0.27.0",
    "aiosqlite>=0.20.0",
    "rich>=13.7.0",
    "python-dotenv>=1.0.1",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "respx>=0.21.0",
]

[project.scripts]
mybox-sync = "mybox_sync.cli:main"

[tool.pytest.ini_options]
asyncio_mode = "auto"
pythonpath = ["src"]
```

```python
# tests/test_limiter.py
import time
import httpx
import pytest
from mybox_sync.config import Config
from mybox_sync.limiter import RateLimiter


async def test_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MYBOX_PAT", "test-pat")
    monkeypatch.setenv("IMMICH_URL", "http://192.168.0.10:2283/")
    monkeypatch.setenv("IMMICH_API_KEY", "immich-key")
    monkeypatch.setenv("CONCURRENCY", "4")
    cfg = Config.from_env()
    assert cfg.mybox_pat == "test-pat"
    assert cfg.immich_url == "http://192.168.0.10:2283"
    assert cfg.immich_api_key == "immich-key"
    assert cfg.concurrency == 4


async def test_rate_limiter_throttles_and_retries_429() -> None:
    limiter = RateLimiter(rps=20.0, max_retries=3, base_delay=0.01)
    attempts = 0

    async def flaky_call() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        req = httpx.Request("GET", "https://open-api.mybox.naver.com/v1/drive/resources")
        if attempts < 3:
            return httpx.Response(429, request=req)
        return httpx.Response(200, json={"ok": True}, request=req)

    t0 = time.monotonic()
    resp = await limiter.request_with_retry(flaky_call)
    elapsed = time.monotonic() - t0
    assert resp.status_code == 200
    assert attempts == 3
    assert elapsed >= 0.02
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_limiter.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mybox_sync'`

- [ ] **Step 3: Write `src/mybox_sync/config.py` and `src/mybox_sync/limiter.py`**

```python
# src/mybox_sync/__init__.py
__version__ = "0.1.0"
```

```python
# src/mybox_sync/config.py
import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

MEDIA_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp", ".gif", ".bmp", ".tiff", ".dng", ".cr2", ".nef", ".arw",
    ".mp4", ".mov", ".avi", ".mkv", ".webm", ".3gp", ".m4v",
}


@dataclass(frozen=True)
class Config:
    mybox_pat: str
    immich_url: str
    immich_api_key: str
    mybox_base_url: str = "https://open-api.mybox.naver.com/v1"
    concurrency: int = 3
    rate_limit_rps: float = 3.0
    db_path: Path = Path("/data/sync_state.db")
    temp_dir: Path = Path("/tmp/mybox_sync")
    max_retries: int = 5

    @classmethod
    def from_env(cls, require_credentials: bool = True) -> "Config":
        load_dotenv()
        mybox_pat = os.getenv("MYBOX_PAT", "").strip()
        immich_url = os.getenv("IMMICH_URL", "").strip().rstrip("/")
        immich_api_key = os.getenv("IMMICH_API_KEY", "").strip()

        if require_credentials and (not mybox_pat or not immich_url or not immich_api_key):
            raise ValueError(
                "Missing required environment variables: MYBOX_PAT, IMMICH_URL, IMMICH_API_KEY"
            )

        return cls(
            mybox_pat=mybox_pat,
            immich_url=immich_url,
            immich_api_key=immich_api_key,
            mybox_base_url=os.getenv("MYBOX_BASE_URL", "https://open-api.mybox.naver.com/v1").rstrip("/"),
            concurrency=max(1, int(os.getenv("CONCURRENCY", "3"))),
            rate_limit_rps=max(0.2, float(os.getenv("RATE_LIMIT_RPS", "3.0"))),
            db_path=Path(os.getenv("DB_PATH", "./data/sync_state.db")),
            temp_dir=Path(os.getenv("TEMP_DIR", "./tmp/mybox_sync")),
            max_retries=max(1, int(os.getenv("MAX_RETRIES", "5"))),
        )
```

```python
# src/mybox_sync/limiter.py
import asyncio
import time
from typing import Awaitable, Callable
import httpx


class RateLimiter:
    """Async token-bucket rate limiter with exponential backoff for 429/5xx."""

    def __init__(self, rps: float = 3.0, max_retries: int = 5, base_delay: float = 1.0) -> None:
        self.min_interval = 1.0 / max(0.1, rps)
        self.max_retries = max_retries
        self.base_delay = base_delay
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait_time = self.min_interval - (now - self._last_call)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last_call = time.monotonic()

    async def request_with_retry(
        self, coro_factory: Callable[[], Awaitable[httpx.Response]]
    ) -> httpx.Response:
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            await self.acquire()
            try:
                response = await coro_factory()
                if response.status_code == 429 or response.status_code >= 500:
                    retry_after = response.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after and retry_after.isdigit() else self.base_delay * (2 ** (attempt - 1))
                    if attempt < self.max_retries:
                        await asyncio.sleep(delay)
                        continue
                response.raise_for_status()
                return response
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                last_exc = exc
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code < 500 and exc.response.status_code != 429:
                    raise
                if attempt < self.max_retries:
                    await asyncio.sleep(self.base_delay * (2 ** (attempt - 1)))
                else:
                    raise
        raise RuntimeError("Retries exhausted") from last_exc
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_limiter.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml src/mybox_sync/__init__.py src/mybox_sync/config.py src/mybox_sync/limiter.py tests/test_limiter.py
git commit -m "feat: add config loader and async rate limiter with exponential backoff"
```

---

### Task 2: SQLite WAL Checkpoint Database (`db.py`)

**Files:**
- Create: `src/mybox_sync/db.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Consumes: `Path` to SQLite DB file
- Produces: `CheckpointDB` class with methods:
  - `async connect() -> None` / `async close() -> None`
  - `async upsert_folder(folder_id: str, path: str, scan_status: str = "PENDING", last_cursor: str | None = None) -> None`
  - `async get_pending_folders() -> list[dict]`
  - `async mark_folder_scanned(folder_id: str) -> None`
  - `async upsert_files(files: list[dict]) -> int`
  - `async get_pending_files(max_retries: int = 5) -> list[dict]`
  - `async mark_file_uploaded(resource_id: str, sha1_hash: str, immich_asset_id: str, immich_status: str) -> None`
  - `async mark_file_failed(resource_id: str, error_msg: str) -> None`
  - `async get_uploaded_files_for_delete() -> list[dict]`
  - `async mark_file_deleted(resource_id: str) -> None`
  - `async get_stats() -> dict`

- [ ] **Step 1: Write failing tests in `tests/test_db.py`**

```python
# tests/test_db.py
from pathlib import Path
from mybox_sync.db import CheckpointDB


async def test_checkpoint_db_lifecycle(tmp_path: Path) -> None:
    db_file = tmp_path / "sync_state.db"
    db = CheckpointDB(db_file)
    await db.connect()
    try:
        await db.upsert_folder("ROOT", "/", scan_status="PENDING", last_cursor="cur1")
        folders = await db.get_pending_folders()
        assert len(folders) == 1
        assert folders[0]["folder_id"] == "ROOT"
        assert folders[0]["last_cursor"] == "cur1"

        await db.mark_folder_scanned("ROOT")
        assert await db.get_pending_folders() == []

        inserted = await db.upsert_files([
            {
                "resource_id": "res-1",
                "name": "IMG_001.JPG",
                "parent_path": "/자동업로드",
                "size": 1024,
                "created_time": "2025-01-01T00:00:00+09:00",
                "modified_time": "2025-01-01T00:00:00+09:00",
            },
            {
                "resource_id": "res-2",
                "name": "IMG_002.HEIC",
                "parent_path": "/자동업로드",
                "size": 2048,
                "created_time": "2025-01-02T00:00:00+09:00",
                "modified_time": "2025-01-02T00:00:00+09:00",
            },
        ])
        assert inserted == 2

        pending = await db.get_pending_files()
        assert len(pending) == 2

        await db.mark_file_uploaded(
            resource_id="res-1",
            sha1_hash="abc123sha1",
            immich_asset_id="immich-uuid-1",
            immich_status="duplicate",
        )
        await db.mark_file_failed("res-2", "timeout")

        uploaded = await db.get_uploaded_files_for_delete()
        assert len(uploaded) == 1
        assert uploaded[0]["resource_id"] == "res-1"
        assert uploaded[0]["sha1_hash"] == "abc123sha1"

        await db.mark_file_deleted("res-1")
        stats = await db.get_stats()
        assert stats["DELETED"]["count"] == 1
        assert stats["FAILED"]["count"] == 1
    finally:
        await db.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_db.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mybox_sync.db'`

- [ ] **Step 3: Write `src/mybox_sync/db.py`**

```python
# src/mybox_sync/db.py
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import aiosqlite


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class CheckpointDB:
    """SQLite (WAL mode) checkpoint database for scan, sync, and deferred delete."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(str(self.db_path))
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA synchronous=NORMAL;")
        await self._conn.execute("PRAGMA busy_timeout=10000;")
        await self._init_schema()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        return self._conn

    async def _init_schema(self) -> None:
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS folders (
                folder_id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                last_cursor TEXT,
                scan_status TEXT NOT NULL DEFAULT 'PENDING',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS files (
                resource_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                parent_path TEXT NOT NULL,
                size INTEGER NOT NULL DEFAULT 0,
                created_time TEXT,
                modified_time TEXT NOT NULL,
                sha1_hash TEXT,
                immich_asset_id TEXT,
                immich_status TEXT,
                status TEXT NOT NULL DEFAULT 'PENDING',
                retry_count INTEGER NOT NULL DEFAULT 0,
                error_msg TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
            CREATE INDEX IF NOT EXISTS idx_files_sha1 ON files(sha1_hash);
            """
        )
        await self.conn.commit()

    async def upsert_folder(
        self,
        folder_id: str,
        path: str,
        scan_status: str = "PENDING",
        last_cursor: str | None = None,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO folders (folder_id, path, last_cursor, scan_status, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(folder_id) DO UPDATE SET
                last_cursor = COALESCE(excluded.last_cursor, folders.last_cursor),
                scan_status = CASE
                    WHEN folders.scan_status = 'SCANNED' THEN 'SCANNED'
                    ELSE excluded.scan_status
                END,
                updated_at = excluded.updated_at
            """,
            (folder_id, path, last_cursor, scan_status, _now_iso()),
        )
        await self.conn.commit()

    async def update_folder_cursor(self, folder_id: str, cursor: str | None) -> None:
        await self.conn.execute(
            "UPDATE folders SET last_cursor = ?, updated_at = ? WHERE folder_id = ?",
            (cursor, _now_iso(), folder_id),
        )
        await self.conn.commit()

    async def mark_folder_scanned(self, folder_id: str) -> None:
        await self.conn.execute(
            "UPDATE folders SET scan_status = 'SCANNED', last_cursor = NULL, updated_at = ? WHERE folder_id = ?",
            (_now_iso(), folder_id),
        )
        await self.conn.commit()

    async def get_pending_folders(self) -> list[dict[str, Any]]:
        async with self.conn.execute(
            "SELECT folder_id, path, last_cursor, scan_status FROM folders WHERE scan_status != 'SCANNED'"
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def upsert_files(self, files: list[dict[str, Any]]) -> int:
        if not files:
            return 0
        now = _now_iso()
        await self.conn.executemany(
            """
            INSERT INTO files (
                resource_id, name, parent_path, size, created_time, modified_time, status, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)
            ON CONFLICT(resource_id) DO NOTHING
            """,
            [
                (
                    f["resource_id"],
                    f["name"],
                    f["parent_path"],
                    int(f.get("size", 0)),
                    f.get("created_time") or f["modified_time"],
                    f["modified_time"],
                    now,
                )
                for f in files
            ],
        )
        await self.conn.commit()
        return len(files)

    async def get_pending_files(self, max_retries: int = 5) -> list[dict[str, Any]]:
        async with self.conn.execute(
            """
            SELECT resource_id, name, parent_path, size, created_time, modified_time, retry_count
            FROM files
            WHERE status = 'PENDING' OR (status = 'FAILED' AND retry_count < ?)
            ORDER BY modified_time ASC
            """,
            (max_retries,),
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def mark_file_uploaded(
        self,
        resource_id: str,
        sha1_hash: str,
        immich_asset_id: str,
        immich_status: str,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE files
            SET sha1_hash = ?,
                immich_asset_id = ?,
                immich_status = ?,
                status = 'UPLOADED',
                error_msg = NULL,
                updated_at = ?
            WHERE resource_id = ?
            """,
            (sha1_hash, immich_asset_id, immich_status, _now_iso(), resource_id),
        )
        await self.conn.commit()

    async def mark_file_failed(self, resource_id: str, error_msg: str) -> None:
        await self.conn.execute(
            """
            UPDATE files
            SET status = 'FAILED',
                retry_count = retry_count + 1,
                error_msg = ?,
                updated_at = ?
            WHERE resource_id = ?
            """,
            (error_msg[:500], _now_iso(), resource_id),
        )
        await self.conn.commit()

    async def get_uploaded_files_for_delete(self) -> list[dict[str, Any]]:
        async with self.conn.execute(
            """
            SELECT resource_id, name, parent_path, size, sha1_hash, immich_asset_id
            FROM files
            WHERE status = 'UPLOADED' AND sha1_hash IS NOT NULL
            ORDER BY updated_at ASC
            """
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def mark_file_deleted(self, resource_id: str) -> None:
        await self.conn.execute(
            "UPDATE files SET status = 'DELETED', updated_at = ? WHERE resource_id = ?",
            (_now_iso(), resource_id),
        )
        await self.conn.commit()

    async def get_stats(self) -> dict[str, Any]:
        stats: dict[str, Any] = {
            "PENDING": {"count": 0, "bytes": 0},
            "UPLOADED": {"count": 0, "bytes": 0, "created": 0, "duplicate": 0},
            "DELETED": {"count": 0, "bytes": 0},
            "FAILED": {"count": 0, "bytes": 0},
            "folders_total": 0,
            "folders_scanned": 0,
        }
        async with self.conn.execute(
            "SELECT status, immich_status, COUNT(*) AS cnt, COALESCE(SUM(size), 0) AS total_bytes FROM files GROUP BY status, immich_status"
        ) as cur:
            async for row in cur:
                st = row["status"]
                if st in stats:
                    stats[st]["count"] += row["cnt"]
                    stats[st]["bytes"] += row["total_bytes"]
                    if st == "UPLOADED" and row["immich_status"] in ("created", "duplicate"):
                        stats["UPLOADED"][row["immich_status"]] += row["cnt"]

        async with self.conn.execute(
            "SELECT COUNT(*) AS total, SUM(CASE WHEN scan_status = 'SCANNED' THEN 1 ELSE 0 END) AS scanned FROM folders"
        ) as cur:
            row = await cur.fetchone()
            if row:
                stats["folders_total"] = row["total"] or 0
                stats["folders_scanned"] = row["scanned"] or 0

        return stats
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_db.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/mybox_sync/db.py tests/test_db.py
git commit -m "feat: add SQLite WAL checkpoint database manager"
```

---

### Task 3: Naver MYBOX & Self-Hosted Immich Async Clients

**Files:**
- Create: `src/mybox_sync/mybox_client.py`
- Create: `src/mybox_sync/immich_client.py`
- Test: `tests/test_clients.py`

**Interfaces:**
- Consumes: `Config`, `RateLimiter`, `httpx.AsyncClient`
- Produces:
  - `MyboxClient`:
    - `async list_resources(folder_id: str = "ROOT", cursor: str | None = None, count: int = 1000) -> tuple[list[dict], str | None]`
    - `async download_to_path_with_sha1(file_id: str, dest_path: Path) -> tuple[str, int]` (streams file to `dest_path`, computes SHA-1 hex in memory on the fly, returns `(sha1_hex, bytes_written)`)
    - `async delete_resource(resource_id: str) -> None`
  - `ImmichClient`:
    - `async check_bulk_hashes(items: list[dict[str, str]]) -> dict[str, str | None]` (maps `id -> existing_asset_id` if duplicate, else `None`)
    - `async upload_asset(file_path: Path, resource_id: str, filename: str, created_time: str, modified_time: str, sha1_hex: str) -> tuple[str, str]` (returns `(asset_id, status)`)

- [ ] **Step 1: Write failing tests in `tests/test_clients.py`**

```python
# tests/test_clients.py
import hashlib
from pathlib import Path
import httpx
import respx
from mybox_sync.limiter import RateLimiter
from mybox_sync.mybox_client import MyboxClient
from mybox_sync.immich_client import ImmichClient


@respx.mock
async def test_mybox_list_download_and_delete(tmp_path: Path) -> None:
    limiter = RateLimiter(rps=100.0)
    base_url = "https://open-api.mybox.naver.com/v1"

    respx.get(f"{base_url}/drive/resources").mock(
        return_value=httpx.Response(
            200,
            json={
                "resources": [
                    {"id": "folder-1", "name": "자동업로드", "type": "folder"},
                    {
                        "id": "file-1",
                        "name": "photo.jpg",
                        "type": "file",
                        "size": 11,
                        "modifiedTime": "2025-05-01T12:00:00+09:00",
                    },
                ],
                "nextCursor": "cursor-2",
            },
        )
    )
    respx.get(f"{base_url}/drive/files/file-1/download").mock(
        return_value=httpx.Response(
            200,
            json={"downloadUrl": "https://storage.mybox.naver.com/dl/file-1"},
        )
    )
    payload = b"hello-world"
    respx.get("https://storage.mybox.naver.com/dl/file-1").mock(
        return_value=httpx.Response(200, content=payload)
    )
    respx.delete(f"{base_url}/drive/resources/file-1").mock(
        return_value=httpx.Response(204)
    )

    async with httpx.AsyncClient() as http:
        client = MyboxClient(http, base_url=base_url, pat="test-pat", limiter=limiter)
        items, next_cursor = await client.list_resources("ROOT")
        assert len(items) == 2
        assert next_cursor == "cursor-2"

        dest = tmp_path / "photo.jpg.part"
        sha1_hex, written = await client.download_to_path_with_sha1("file-1", dest)
        assert written == 11
        assert sha1_hex == hashlib.sha1(payload).hexdigest()
        assert dest.read_bytes() == payload

        await client.delete_resource("file-1")


@respx.mock
async def test_immich_bulk_check_and_upload(tmp_path: Path) -> None:
    immich_url = "http://192.168.0.10:2283"
    respx.post(f"{immich_url}/api/assets/bulk-upload-check").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [
                    {"id": "res-dup", "action": "reject", "reason": "duplicate", "assetId": "immich-existing-1"},
                    {"id": "res-new", "action": "accept"},
                ]
            },
        )
    )
    respx.post(f"{immich_url}/api/assets").mock(
        return_value=httpx.Response(
            201,
            json={"id": "immich-created-2", "status": "created"},
        )
    )

    sample = tmp_path / "new.jpg"
    sample.write_bytes(b"image-bytes")

    async with httpx.AsyncClient() as http:
        client = ImmichClient(http, immich_url=immich_url, api_key="key-1")
        dup_map = await client.check_bulk_hashes([
            {"id": "res-dup", "checksum": "aaa"},
            {"id": "res-new", "checksum": "bbb"},
        ])
        assert dup_map["res-dup"] == "immich-existing-1"
        assert dup_map["res-new"] is None

        asset_id, status = await client.upload_asset(
            file_path=sample,
            resource_id="res-new",
            filename="new.jpg",
            created_time="2025-05-01T12:00:00+09:00",
            modified_time="2025-05-01T12:00:00+09:00",
            sha1_hex="bbb",
        )
        assert asset_id == "immich-created-2"
        assert status == "created"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_clients.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mybox_sync.mybox_client'`

- [ ] **Step 3: Implement `src/mybox_sync/mybox_client.py` and `src/mybox_sync/immich_client.py`**

```python
# src/mybox_sync/mybox_client.py
import hashlib
from pathlib import Path
from typing import Any
import httpx
from mybox_sync.limiter import RateLimiter


class MyboxClient:
    """Async client for Naver MYBOX Open API."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        base_url: str,
        pat: str,
        limiter: RateLimiter,
    ) -> None:
        self.http = http
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {pat}",
            "Accept": "application/json",
        }
        self.limiter = limiter

    async def list_resources(
        self,
        folder_id: str = "ROOT",
        cursor: str | None = None,
        count: int = 1000,
    ) -> tuple[list[dict[str, Any]], str | None]:
        if folder_id == "ROOT":
            url = f"{self.base_url}/drive/resources"
        else:
            url = f"{self.base_url}/drive/folders/{folder_id}/resources"

        params: dict[str, Any] = {"count": count}
        if cursor:
            params["cursor"] = cursor

        resp = await self.limiter.request_with_retry(
            lambda: self.http.get(url, headers=self.headers, params=params)
        )
        data = resp.json()
        resources = data.get("resources") or data.get("items") or data.get("files") or []
        next_cursor = data.get("nextCursor") or data.get("cursor") or None
        return resources, next_cursor

    async def get_download_url(self, file_id: str) -> str:
        url = f"{self.base_url}/drive/files/{file_id}/download"
        resp = await self.limiter.request_with_retry(
            lambda: self.http.get(url, headers=self.headers, follow_redirects=False)
        )
        if resp.status_code in (301, 302, 303, 307, 308) and "location" in resp.headers:
            return resp.headers["location"]
        data = resp.json()
        dl_url = data.get("downloadUrl") or data.get("url")
        if not dl_url:
            raise ValueError(f"Download URL missing in response for file {file_id}: {data}")
        return dl_url

    async def download_to_path_with_sha1(
        self, file_id: str, dest_path: Path
    ) -> tuple[str, int]:
        dl_url = await self.get_download_url(file_id)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        await self.limiter.acquire()

        sha1 = hashlib.sha1()
        bytes_written = 0
        async with self.http.stream("GET", dl_url, follow_redirects=True) as resp:
            resp.raise_for_status()
            with dest_path.open("wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
                        sha1.update(chunk)
                        bytes_written += len(chunk)

        return sha1.hexdigest(), bytes_written

    async def delete_resource(self, resource_id: str) -> None:
        url = f"{self.base_url}/drive/resources/{resource_id}"
        await self.limiter.request_with_retry(
            lambda: self.http.delete(url, headers=self.headers)
        )
```

```python
# src/mybox_sync/immich_client.py
from pathlib import Path
from typing import Any
import httpx


class ImmichClient:
    """Async client for self-hosted Immich server REST API."""

    def __init__(self, http: httpx.AsyncClient, immich_url: str, api_key: str) -> None:
        self.http = http
        self.immich_url = immich_url.rstrip("/")
        self.headers = {
            "x-api-key": api_key,
            "Accept": "application/json",
        }

    async def check_bulk_hashes(self, items: list[dict[str, str]]) -> dict[str, str | None]:
        """
        Check a list of {"id": resource_id, "checksum": sha1_hex} against Immich.
        Returns mapping {resource_id: existing_asset_id_or_None}.
        """
        if not items:
            return {}
        url = f"{self.immich_url}/api/assets/bulk-upload-check"
        resp = await self.http.post(url, headers=self.headers, json={"assets": items})
        resp.raise_for_status()
        results = resp.json().get("results", [])
        mapping: dict[str, str | None] = {}
        for item in results:
            rid = item.get("id")
            if not rid:
                continue
            if item.get("action") == "reject" and item.get("reason") == "duplicate":
                mapping[rid] = item.get("assetId") or "duplicate-existing"
            else:
                mapping[rid] = None
        return mapping

    async def upload_asset(
        self,
        file_path: Path,
        resource_id: str,
        filename: str,
        created_time: str,
        modified_time: str,
        sha1_hex: str,
    ) -> tuple[str, str]:
        url = f"{self.immich_url}/api/assets"
        headers = {**self.headers, "x-immich-checksum": sha1_hex}
        data = {
            "deviceAssetId": f"mybox-{resource_id}",
            "deviceId": "MYBOX-SYNC",
            "fileCreatedAt": created_time or modified_time,
            "fileModifiedAt": modified_time,
            "isFavorite": "false",
        }
        with file_path.open("rb") as f:
            files = {"assetData": (filename, f, "application/octet-stream")}
            resp = await self.http.post(url, headers=headers, data=data, files=files)
            resp.raise_for_status()
            body: dict[str, Any] = resp.json()
            asset_id = body.get("id", "unknown-id")
            status = body.get("status", "created")
            return asset_id, status
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_clients.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/mybox_sync/mybox_client.py src/mybox_sync/immich_client.py tests/test_clients.py
git commit -m "feat: add async MYBOX and Immich API clients with streaming SHA-1"
```

---

### Task 4: Async Multi-Worker Pipeline (`scan`, `sync`, `delete`) and CLI

**Files:**
- Create: `src/mybox_sync/pipeline.py`
- Create: `src/mybox_sync/cli.py`
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `Config`, `CheckpointDB`, `MyboxClient`, `ImmichClient`
- Produces:
  - `SyncPipeline`:
    - `async run_scan() -> int`
    - `async run_sync() -> dict[str, int]`
    - `async run_delete(dry_run: bool = False) -> dict[str, int]`
  - `cli.py`: `main()` entrypoint supporting `auto`, `scan`, `sync`, `delete [--dry-run]`, `status`

- [ ] **Step 1: Write failing pipeline integration test in `tests/test_pipeline.py`**

```python
# tests/test_pipeline.py
import hashlib
from pathlib import Path
import httpx
import respx
from mybox_sync.config import Config
from mybox_sync.db import CheckpointDB
from mybox_sync.limiter import RateLimiter
from mybox_sync.mybox_client import MyboxClient
from mybox_sync.immich_client import ImmichClient
from mybox_sync.pipeline import SyncPipeline


@respx.mock
async def test_full_pipeline_scan_sync_and_deferred_delete(tmp_path: Path) -> None:
    base_url = "https://open-api.mybox.naver.com/v1"
    immich_url = "http://192.168.0.10:2283"

    img1_bytes = b"iphone-already-synced-photo"
    img1_sha1 = hashlib.sha1(img1_bytes).hexdigest()
    img2_bytes = b"mybox-only-old-photo"
    img2_sha1 = hashlib.sha1(img2_bytes).hexdigest()

    # 1. Mock MYBOX folder scan
    respx.get(f"{base_url}/drive/resources").mock(
        return_value=httpx.Response(
            200,
            json={
                "resources": [
                    {"id": "fld-auto", "name": "자동업로드", "type": "folder"},
                ],
                "nextCursor": None,
            },
        )
    )
    respx.get(f"{base_url}/drive/folders/fld-auto/resources").mock(
        return_value=httpx.Response(
            200,
            json={
                "resources": [
                    {
                        "id": "res-1",
                        "name": "IMG_001.JPG",
                        "type": "file",
                        "size": len(img1_bytes),
                        "modifiedTime": "2024-05-01T10:00:00+09:00",
                    },
                    {
                        "id": "res-2",
                        "name": "IMG_002.JPG",
                        "type": "file",
                        "size": len(img2_bytes),
                        "modifiedTime": "2023-08-15T14:30:00+09:00",
                    },
                    {
                        "id": "res-doc",
                        "name": "notes.txt",
                        "type": "file",
                        "size": 100,
                        "modifiedTime": "2023-08-15T14:30:00+09:00",
                    },
                ],
                "nextCursor": None,
            },
        )
    )

    # 2. Mock MYBOX downloads
    respx.get(f"{base_url}/drive/files/res-1/download").mock(
        return_value=httpx.Response(200, json={"downloadUrl": "https://dl.mybox/res-1"})
    )
    respx.get("https://dl.mybox/res-1").mock(return_value=httpx.Response(200, content=img1_bytes))

    respx.get(f"{base_url}/drive/files/res-2/download").mock(
        return_value=httpx.Response(200, json={"downloadUrl": "https://dl.mybox/res-2"})
    )
    respx.get("https://dl.mybox/res-2").mock(return_value=httpx.Response(200, content=img2_bytes))

    # 3. Mock Immich bulk-upload-check and upload
    uploaded_to_immich = {img1_sha1: "immich-dup-1"}

    def bulk_check_handler(request: httpx.Request) -> httpx.Response:
        import json
        body = json.loads(request.content.decode())
        results = []
        for item in body["assets"]:
            chk = item["checksum"]
            if chk in uploaded_to_immich:
                results.append({
                    "id": item["id"],
                    "action": "reject",
                    "reason": "duplicate",
                    "assetId": uploaded_to_immich[chk],
                })
            else:
                results.append({"id": item["id"], "action": "accept"})
        return httpx.Response(200, json={"results": results})

    respx.post(f"{immich_url}/api/assets/bulk-upload-check").mock(side_effect=bulk_check_handler)

    def upload_handler(request: httpx.Request) -> httpx.Response:
        uploaded_to_immich[img2_sha1] = "immich-new-2"
        return httpx.Response(201, json={"id": "immich-new-2", "status": "created"})

    respx.post(f"{immich_url}/api/assets").mock(side_effect=upload_handler)

    # 4. Mock MYBOX deletes
    del1 = respx.delete(f"{base_url}/drive/resources/res-1").mock(return_value=httpx.Response(204))
    del2 = respx.delete(f"{base_url}/drive/resources/res-2").mock(return_value=httpx.Response(204))

    cfg = Config(
        mybox_pat="pat",
        immich_url=immich_url,
        immich_api_key="key",
        concurrency=2,
        rate_limit_rps=100.0,
        db_path=tmp_path / "state.db",
        temp_dir=tmp_path / "tmp",
    )
    db = CheckpointDB(cfg.db_path)
    await db.connect()
    try:
        async with httpx.AsyncClient() as http:
            limiter = RateLimiter(rps=100.0)
            mybox = MyboxClient(http, cfg.mybox_base_url, cfg.mybox_pat, limiter)
            immich = ImmichClient(http, cfg.immich_url, cfg.immich_api_key)
            pipeline = SyncPipeline(cfg, db, mybox, immich)

            found = await pipeline.run_scan()
            assert found == 2  # only media files

            sync_res = await pipeline.run_sync()
            assert sync_res["created"] == 1
            assert sync_res["duplicate"] == 1
            assert sync_res["failed"] == 0
            # Verify temp dir has no leftover .part files
            assert list(cfg.temp_dir.glob("*")) == []
            # Verify MYBOX files were NOT deleted during sync
            assert not del1.called
            assert not del2.called

            # Deferred delete
            del_res = await pipeline.run_delete(dry_run=False)
            assert del_res["deleted"] == 2
            assert del1.called
            assert del2.called

            stats = await db.get_stats()
            assert stats["DELETED"]["count"] == 2
    finally:
        await db.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_pipeline.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mybox_sync.pipeline'`

- [ ] **Step 3: Implement `src/mybox_sync/pipeline.py` and `src/mybox_sync/cli.py`**

```python
# src/mybox_sync/pipeline.py
import asyncio
from pathlib import Path
from typing import Any
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from mybox_sync.config import Config, MEDIA_EXTENSIONS
from mybox_sync.db import CheckpointDB
from mybox_sync.mybox_client import MyboxClient
from mybox_sync.immich_client import ImmichClient

console = Console()


def _is_media_file(name: str) -> bool:
    return Path(name).suffix.lower() in MEDIA_EXTENSIONS


class SyncPipeline:
    """Async multi-worker pipeline for scan, sync (upload/dedup), and deferred delete."""

    def __init__(
        self,
        config: Config,
        db: CheckpointDB,
        mybox: MyboxClient,
        immich: ImmichClient,
    ) -> None:
        self.config = config
        self.db = db
        self.mybox = mybox
        self.immich = immich
        self._stop_event = asyncio.Event()

    def request_stop(self) -> None:
        self._stop_event.set()

    async def run_scan(self) -> int:
        await self.db.upsert_folder("ROOT", "/", scan_status="PENDING")
        total_media_found = 0

        while not self._stop_event.is_set():
            pending_folders = await self.db.get_pending_folders()
            if not pending_folders:
                break

            queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
            for folder in pending_folders:
                queue.put_nowait(folder)

            async def scan_worker() -> int:
                found_count = 0
                while not queue.empty() and not self._stop_event.is_set():
                    try:
                        folder = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    folder_id = folder["folder_id"]
                    folder_path = folder["path"]
                    cursor = folder["last_cursor"]

                    try:
                        while not self._stop_event.is_set():
                            resources, next_cursor = await self.mybox.list_resources(
                                folder_id=folder_id, cursor=cursor
                            )
                            media_batch: list[dict[str, Any]] = []
                            for res in resources:
                                r_id = str(res.get("id") or res.get("resourceId") or "")
                                r_name = str(res.get("name") or "")
                                r_type = str(res.get("type") or "").lower()
                                is_folder = r_type == "folder" or bool(res.get("isFolder"))

                                if not r_id or not r_name:
                                    continue

                                if is_folder:
                                    sub_path = f"{folder_path.rstrip('/')}/{r_name}"
                                    await self.db.upsert_folder(r_id, sub_path, scan_status="PENDING")
                                elif _is_media_file(r_name):
                                    mod_time = (
                                        res.get("modifiedTime")
                                        or res.get("createdTime")
                                        or "1970-01-01T00:00:00Z"
                                    )
                                    media_batch.append(
                                        {
                                            "resource_id": r_id,
                                            "name": r_name,
                                            "parent_path": folder_path,
                                            "size": int(res.get("size") or 0),
                                            "created_time": res.get("createdTime") or mod_time,
                                            "modified_time": mod_time,
                                        }
                                    )

                            if media_batch:
                                found_count += await self.db.upsert_files(media_batch)

                            if next_cursor:
                                cursor = next_cursor
                                await self.db.update_folder_cursor(folder_id, cursor)
                            else:
                                await self.db.mark_folder_scanned(folder_id)
                                break
                    except Exception as exc:
                        console.print(f"[red]Folder scan error ({folder_path}): {exc}[/red]")
                    finally:
                        queue.task_done()
                return found_count

            workers = [
                asyncio.create_task(scan_worker())
                for _ in range(min(self.config.concurrency, len(pending_folders)))
            ]
            results = await asyncio.gather(*workers)
            total_media_found += sum(results)

        return total_media_found

    async def run_sync(self) -> dict[str, int]:
        pending_files = await self.db.get_pending_files(max_retries=self.config.max_retries)
        summary = {"created": 0, "duplicate": 0, "failed": 0}
        if not pending_files:
            return summary

        self.config.temp_dir.mkdir(parents=True, exist_ok=True)
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        for item in pending_files:
            queue.put_nowait(item)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Syncing MYBOX -> Immich", total=len(pending_files))

            async def sync_worker() -> None:
                while not queue.empty() and not self._stop_event.is_set():
                    try:
                        item = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    r_id = item["resource_id"]
                    name = item["name"]
                    expected_size = int(item["size"] or 0)
                    temp_path = self.config.temp_dir / f"{r_id}.part"

                    try:
                        sha1_hex, written = await self.mybox.download_to_path_with_sha1(r_id, temp_path)
                        if expected_size > 0 and written != expected_size:
                            raise IOError(
                                f"Size mismatch for {name}: expected {expected_size}, got {written}"
                            )

                        # Pre-check hash against Immich to skip re-uploading duplicates
                        dup_map = await self.immich.check_bulk_hashes(
                            [{"id": r_id, "checksum": sha1_hex}]
                        )
                        existing_asset_id = dup_map.get(r_id)

                        if existing_asset_id:
                            await self.db.mark_file_uploaded(
                                resource_id=r_id,
                                sha1_hash=sha1_hex,
                                immich_asset_id=existing_asset_id,
                                immich_status="duplicate",
                            )
                            summary["duplicate"] += 1
                        else:
                            asset_id, status = await self.immich.upload_asset(
                                file_path=temp_path,
                                resource_id=r_id,
                                filename=name,
                                created_time=item["created_time"],
                                modified_time=item["modified_time"],
                                sha1_hex=sha1_hex,
                            )
                            norm_status = "duplicate" if status == "duplicate" else "created"
                            await self.db.mark_file_uploaded(
                                resource_id=r_id,
                                sha1_hash=sha1_hex,
                                immich_asset_id=asset_id,
                                immich_status=norm_status,
                            )
                            summary[norm_status] += 1
                    except Exception as exc:
                        await self.db.mark_file_failed(r_id, str(exc))
                        summary["failed"] += 1
                        console.print(f"[yellow]Failed {name} ({r_id}): {exc}[/yellow]")
                    finally:
                        if temp_path.exists():
                            try:
                                temp_path.unlink()
                            except OSError:
                                pass
                        progress.advance(task_id, 1)
                        queue.task_done()

            workers = [
                asyncio.create_task(sync_worker())
                for _ in range(min(self.config.concurrency, len(pending_files)))
            ]
            await asyncio.gather(*workers)

        return summary

    async def run_delete(self, dry_run: bool = False) -> dict[str, int]:
        candidates = await self.db.get_uploaded_files_for_delete()
        summary = {"deleted": 0, "skipped_unverified": 0, "failed": 0}
        if not candidates:
            return summary

        # Step 1: Double-check hashes in batches of 200 against Immich before deleting from MYBOX
        verified_items: list[dict[str, Any]] = []
        batch_size = 200
        for i in range(0, len(candidates), batch_size):
            batch = candidates[i : i + batch_size]
            payload = [{"id": c["resource_id"], "checksum": c["sha1_hash"]} for c in batch]
            dup_map = await self.immich.check_bulk_hashes(payload)
            for c in batch:
                if dup_map.get(c["resource_id"]):
                    verified_items.append(c)
                else:
                    summary["skipped_unverified"] += 1

        if dry_run or not verified_items:
            if dry_run:
                summary["deleted"] = len(verified_items)
            return summary

        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        for item in verified_items:
            queue.put_nowait(item)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Deleting verified photos from MYBOX", total=len(verified_items))

            async def delete_worker() -> None:
                while not queue.empty() and not self._stop_event.is_set():
                    try:
                        item = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    r_id = item["resource_id"]
                    try:
                        await self.mybox.delete_resource(r_id)
                        await self.db.mark_file_deleted(r_id)
                        summary["deleted"] += 1
                    except Exception as exc:
                        summary["failed"] += 1
                        console.print(f"[red]Delete failed for {item['name']} ({r_id}): {exc}[/red]")
                    finally:
                        progress.advance(task_id, 1)
                        queue.task_done()

            workers = [
                asyncio.create_task(delete_worker())
                for _ in range(min(self.config.concurrency, len(verified_items)))
            ]
            await asyncio.gather(*workers)

        return summary
```

```python
# src/mybox_sync/cli.py
import argparse
import asyncio
import signal
import sys
import httpx
from rich.console import Console
from rich.table import Table
from mybox_sync.config import Config
from mybox_sync.db import CheckpointDB
from mybox_sync.limiter import RateLimiter
from mybox_sync.mybox_client import MyboxClient
from mybox_sync.immich_client import ImmichClient
from mybox_sync.pipeline import SyncPipeline

console = Console()


def _format_bytes(size: int) -> str:
    val = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if val < 1024.0:
            return f"{val:.2f} {unit}"
        val /= 1024.0
    return f"{val:.2f} PB"


async def _print_status(db: CheckpointDB) -> None:
    stats = await db.get_stats()
    table = Table(title="Naver MYBOX -> Immich Sync Checkpoint Status")
    table.add_column("Metric / Status", style="cyan")
    table.add_column("Count", justify="right", style="bold")
    table.add_column("Total Size", justify="right", style="green")
    table.add_column("Details", style="dim")

    table.add_row(
        "Folders Scanned",
        f"{stats['folders_scanned']} / {stats['folders_total']}",
        "-",
        "MYBOX folder traversal",
    )
    table.add_row(
        "PENDING (Queue)",
        f"{stats['PENDING']['count']:,}",
        _format_bytes(stats["PENDING"]["bytes"]),
        "Waiting to sync",
    )
    table.add_row(
        "UPLOADED (Verified in Immich)",
        f"{stats['UPLOADED']['count']:,}",
        _format_bytes(stats["UPLOADED"]["bytes"]),
        f"New: {stats['UPLOADED']['created']:,} | Duplicate skipped: {stats['UPLOADED']['duplicate']:,}",
    )
    table.add_row(
        "DELETED (Moved to MYBOX Trash)",
        f"{stats['DELETED']['count']:,}",
        _format_bytes(stats["DELETED"]["bytes"]),
        "Remotely deleted after hash check",
    )
    table.add_row(
        "FAILED",
        f"{stats['FAILED']['count']:,}",
        _format_bytes(stats["FAILED"]["bytes"]),
        "Will retry on next sync run",
    )
    console.print(table)


async def _async_main(args: argparse.Namespace) -> int:
    require_creds = args.command != "status"
    config = Config.from_env(require_credentials=require_creds)
    db = CheckpointDB(config.db_path)
    await db.connect()

    try:
        if args.command == "status":
            await _print_status(db)
            return 0

        limiter = RateLimiter(rps=config.rate_limit_rps, max_retries=config.max_retries)
        timeout = httpx.Timeout(connect=15.0, read=120.0, write=120.0, pool=30.0)
        async with httpx.AsyncClient(timeout=timeout) as http:
            mybox = MyboxClient(http, config.mybox_base_url, config.mybox_pat, limiter)
            immich = ImmichClient(http, config.immich_url, config.immich_api_key)
            pipeline = SyncPipeline(config, db, mybox, immich)

            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, pipeline.request_stop)
                except NotImplementedError:
                    # Windows ProactorEventLoop fallback
                    signal.signal(sig, lambda *_: pipeline.request_stop())

            if args.command in ("scan", "auto"):
                console.print("[bold cyan]1. Scanning MYBOX folders...[/bold cyan]")
                found = await pipeline.run_scan()
                console.print(f"[green]Scan complete. New media files added: {found:,}[/green]")

            if args.command in ("sync", "auto"):
                console.print("[bold cyan]2. Syncing pending files to Immich...[/bold cyan]")
                res = await pipeline.run_sync()
                console.print(
                    f"[green]Sync complete! Created: {res['created']:,}, "
                    f"Duplicates skipped: {res['duplicate']:,}, Failed: {res['failed']:,}[/green]"
                )
                await _print_status(db)
                return 1 if res["failed"] > 0 else 0

            if args.command == "delete":
                console.print(
                    f"[bold magenta]3. Running deferred MYBOX deletion (dry_run={args.dry_run})...[/bold magenta]"
                )
                res = await pipeline.run_delete(dry_run=args.dry_run)
                console.print(
                    f"[green]Delete complete! Deleted: {res['deleted']:,}, "
                    f"Skipped (unverified in Immich): {res['skipped_unverified']:,}, "
                    f"Failed: {res['failed']:,}[/green]"
                )
                await _print_status(db)
                return 1 if res["failed"] > 0 else 0

        return 0
    finally:
        await db.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="mybox-sync",
        description="Naver MYBOX to Self-Hosted Immich Async Sync & Deferred Delete Tool",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("auto", help="Run scan then sync, and exit 0 when finished")
    sub.add_parser("scan", help="Scan MYBOX folders and record media files into SQLite")
    sub.add_parser("sync", help="Download, hash, deduplicate/upload to Immich without deleting")
    del_p = sub.add_parser("delete", help="Verify hashes against Immich and batch-delete from MYBOX")
    del_p.add_argument("--dry-run", action="store_true", help="Verify hashes without deleting from MYBOX")
    sub.add_parser("status", help="Display current SQLite checkpoint progress")

    args = parser.parse_args()
    sys.exit(asyncio.run(_async_main(args)))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest -v`
Expected: PASS (all 5 tests pass)

- [ ] **Step 5: Commit**

```bash
git add src/mybox_sync/pipeline.py src/mybox_sync/cli.py tests/test_pipeline.py
git commit -m "feat: implement async scan, sync, and hash-verified deferred delete pipeline"
```

---

### Task 5: TrueNAS SCALE Docker Compose, Dockerfile, and Usage Guide

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `README.md`

**Interfaces:**
- Consumes: `mybox_sync.cli` package
- Produces: TrueNAS SCALE ready container setup (`tmpfs` RAM disk + `restart: on-failure:3` auto-stop on completion)

- [ ] **Step 1: Create `Dockerfile`, `docker-compose.yml`, `.env.example`, and `README.md`**

```dockerfile
# Dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DB_PATH=/data/sync_state.db \
    TEMP_DIR=/tmp/mybox_sync

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir . && \
    mkdir -p /data /tmp/mybox_sync

ENTRYPOINT ["mybox-sync"]
CMD ["auto"]
```

```yaml
# docker-compose.yml
services:
  mybox-sync:
    build: .
    container_name: mybox-immich-sync
    env_file: .env
    volumes:
      # TrueNAS SCALE 예시: /mnt/<Pool이름>/apps/mybox-sync/data:/data
      - ./data:/data
    tmpfs:
      # ZFS 디스크 쓰기(CoW) 및 스냅샷 증가 방지를 위해 임시 다운로드는 RAM(tmpfs)에서 처리 후 즉시 삭제
      - /tmp/mybox_sync:size=1G,mode=1777
    command: ["auto"]
    # 네트워크 일시 오류(exit 1) 시 최대 3회 자동 재시작, 전체 동기화 완료(exit 0) 시 컨테이너 자동 종료!
    restart: on-failure:3
```

```dotenv
# .env.example
# 1. 네이버 MYBOX 개인 액세스 토큰 (MYBOX 웹 > 환경설정 > 계정 및 개인 액세스 토큰 관리)
MYBOX_PAT=your_naver_mybox_personal_access_token

# 2. TrueNAS SCALE 내 셀프호스팅 Immich 주소 및 API 키 (우측 상단 프로필 > 계정 설정 > API 키)
IMMICH_URL=http://192.168.0.100:2283
IMMICH_API_KEY=your_immich_api_key

# 3. 동시 처리 워커 수 및 MYBOX 초당 API 호출 제한 (Rate Limit 방지)
CONCURRENCY=3
RATE_LIMIT_RPS=3.0
MAX_RETRIES=5
```

- [ ] **Step 2: Run full test suite and CLI `--help` check**

Run: `pytest -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add Dockerfile docker-compose.yml .env.example README.md
git commit -m "chore: add TrueNAS SCALE optimized Dockerfile, docker-compose, and docs"
```
