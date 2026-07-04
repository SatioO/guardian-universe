# Scanner Data Pipeline — P1a Distribution Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the P0 producer into a runnable local **bootstrap → publish** loop: ingest N trading days locally, build a checksummed manifest, and publish the parquet + manifest as assets on a rolling GitHub Release (`data-latest`) served by GitHub's CDN — plus a `pipeline` CLI to drive it.

**Architecture:** Four small, pure/injectable modules on top of the P0 pipeline. `manifest` builds the versioned/checksummed manifest + status dicts (pure). `publish` uploads artifacts to a GitHub Release behind an injectable command-runner (data files first, manifest last, for approximate atomicity). `backfill` loops the existing `run_daily` over a trading-day window (resumable via P0's `has_day` idempotency). A `cli` wires real adapters into `python -m pipeline {daily,backfill,publish}`. The daily CI workflow (P1b) and NSE indices (P1c) are separate plans.

**Tech Stack:** Python 3.11+, pandas/pyarrow (existing), `gh` CLI (GitHub Releases), pytest + ruff + mypy --strict, `filterwarnings=["error"]`.

## Global Constraints

- **Python 3.11+**; run tooling via the existing venv: `.venv/bin/pytest -v`, `.venv/bin/ruff check .`, `.venv/bin/mypy` (from `pipeline/`).
- **Warning-free:** the suite runs under `filterwarnings=["error"]`; any warning fails. mypy `--strict` clean (narrow `# type: ignore[code]` with a comment only for genuine stub gaps).
- **Error taxonomy:** reuse existing `pipeline.errors.UnexpectedFailure` for publish failures — do NOT add new error types.
- **Single source of truth:** new constants (`GITHUB_REPO`, `RELEASE_TAG`) live in `pipeline.config`, not scattered.
- **Reuse P0, don't reimplement:** `run_daily(target, *, fetcher, holidays, base) -> RunStatus` (in `pipeline.daily_update`), `RunStatus` (fields: `status, date, symbol_count, quarantined_count, source, message`), `calendar.trading_days_back(end, n, holidays)`, `calendar.load_holidays(path)`, `config.OHLC_DIR`/`META_DIR`/`SCHEMA_VERSION`, `store.has_day`.
- **Determinism:** inject non-deterministic inputs (timestamps, sleeps, the command runner) so tests are deterministic and offline. No live network / no real `gh` calls in tests.
- **Publish target:** rolling GitHub Release tag `data-latest` on repo `SatioO/guardian-universe`; upload with `--clobber`; upload data files BEFORE the manifest.

---

### Task 1: Manifest + status builders

**Files:**
- Create: `pipeline/src/pipeline/manifest.py`
- Test: `pipeline/tests/test_manifest.py`

**Interfaces:**
- Consumes: `config.SCHEMA_VERSION`.
- Produces:
  - `manifest.file_digest(path: Path) -> tuple[str, int]` — `(sha256_hex, byte_size)`.
  - `manifest.build_manifest(ohlc_dir: Path, *, schema_version: int, latest_trading_date: date, generated_at: str) -> dict` — scans `ohlc_dir` for `ohlc_*.parquet` (sorted), one dataset `"ohlc"`.
  - `manifest.status_to_dict(status_obj) -> dict` — serializes a `RunStatus` (duck-typed: reads `.status/.date/.symbol_count/.quarantined_count/.source/.message`).
  - `manifest.write_json(obj: dict, path: Path) -> None`.

- [ ] **Step 1: Write the failing test — `pipeline/tests/test_manifest.py`**

```python
import json
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config, manifest


def _write_parquet(p: Path, n: int) -> None:
    df = pd.DataFrame({c: [0] * n for c in config.CANON_COLUMNS})
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, compression="zstd", index=False)


def test_file_digest_is_stable(tmp_path: Path):
    p = tmp_path / "a.parquet"
    _write_parquet(p, 1)
    sha1, size1 = manifest.file_digest(p)
    sha2, size2 = manifest.file_digest(p)
    assert sha1 == sha2 and len(sha1) == 64 and size1 == size2 > 0


def test_build_manifest_lists_ohlc_files_with_digests(tmp_path: Path):
    _write_parquet(tmp_path / "ohlc_2025.parquet", 2)
    _write_parquet(tmp_path / "ohlc_2026.parquet", 3)
    m = manifest.build_manifest(
        tmp_path, schema_version=1, latest_trading_date=date(2026, 7, 3),
        generated_at="2026-07-03T12:00:00Z",
    )
    assert m["schema_version"] == 1
    assert m["latest_trading_date"] == "2026-07-03"
    assert m["generated_at"] == "2026-07-03T12:00:00Z"
    ds = m["datasets"][0]
    assert ds["name"] == "ohlc"
    names = [f["name"] for f in ds["files"]]
    assert names == ["ohlc_2025.parquet", "ohlc_2026.parquet"]  # sorted
    assert all(len(f["sha256"]) == 64 and f["bytes"] > 0 for f in ds["files"])


def test_status_to_dict_serializes_run_status():
    from pipeline.daily_update import RunStatus
    d = manifest.status_to_dict(RunStatus("success", date(2026, 7, 3), symbol_count=1900,
                                          quarantined_count=2, source="nse-udiff"))
    assert d == {
        "status": "success", "date": "2026-07-03", "symbol_count": 1900,
        "quarantined_count": 2, "source": "nse-udiff", "message": "",
    }


def test_write_json_roundtrips(tmp_path: Path):
    p = tmp_path / "m.json"
    manifest.write_json({"a": 1}, p)
    assert json.loads(p.read_text()) == {"a": 1}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_manifest.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.manifest'`.

- [ ] **Step 3: Write minimal implementation — `pipeline/src/pipeline/manifest.py`**

```python
"""Build the versioned, checksummed manifest + status dicts. Pure (no network)."""
from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

_CHUNK = 1 << 20


def file_digest(path: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def build_manifest(
    ohlc_dir: Path,
    *,
    schema_version: int,
    latest_trading_date: date,
    generated_at: str,
) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for p in sorted(ohlc_dir.glob("ohlc_*.parquet")):
        sha, size = file_digest(p)
        files.append({"name": p.name, "sha256": sha, "bytes": size})
    return {
        "schema_version": schema_version,
        "generated_at": generated_at,
        "latest_trading_date": latest_trading_date.isoformat(),
        "datasets": [{"name": "ohlc", "files": files}],
    }


def status_to_dict(status_obj: Any) -> dict[str, Any]:
    return {
        "status": status_obj.status,
        "date": status_obj.date.isoformat(),
        "symbol_count": status_obj.symbol_count,
        "quarantined_count": status_obj.quarantined_count,
        "source": status_obj.source,
        "message": status_obj.message,
    }


def write_json(obj: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_manifest.py -v && .venv/bin/ruff check . && .venv/bin/mypy`
Expected: 4 PASS, ruff clean, mypy Success.

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/manifest.py pipeline/tests/test_manifest.py
git commit -m "feat(pipeline): checksummed manifest + status builders"
```

---

### Task 2: GitHub-Release publish wrapper

**Files:**
- Modify: `pipeline/src/pipeline/config.py` (add `GITHUB_REPO`, `RELEASE_TAG`)
- Create: `pipeline/src/pipeline/publish.py`
- Test: `pipeline/tests/test_publish.py`

**Interfaces:**
- Consumes: `errors.UnexpectedFailure`, `config.GITHUB_REPO`, `config.RELEASE_TAG`.
- Produces:
  - `publish.Runner` = `Callable[[list[str]], int]` (runs a command, returns exit code).
  - `publish.subprocess_runner(cmd: list[str]) -> int` (real runner).
  - `publish.publish_release(data_files: list[Path], manifest_path: Path, *, tag: str, repo: str, runner: Runner) -> None` — ensures the release exists (idempotent create, ignores failure), uploads each data file (`--clobber`), then the manifest LAST. Raises `UnexpectedFailure` if any UPLOAD returns non-zero.

- [ ] **Step 1: Write the failing test — `pipeline/tests/test_publish.py`**

```python
from pathlib import Path

import pytest

from pipeline import publish
from pipeline.errors import UnexpectedFailure


class FakeRunner:
    def __init__(self, fail_on: str | None = None):
        self.calls: list[list[str]] = []
        self._fail_on = fail_on

    def __call__(self, cmd: list[str]) -> int:
        self.calls.append(cmd)
        if self._fail_on is not None and any(self._fail_on in a for a in cmd):
            return 1
        return 0


def test_publish_uploads_data_files_before_manifest(tmp_path: Path):
    a = tmp_path / "ohlc_2026.parquet"; a.write_text("x")
    m = tmp_path / "manifest.json"; m.write_text("{}")
    r = FakeRunner()
    publish.publish_release([a], m, tag="data-latest", repo="o/r", runner=r)
    uploads = [c for c in r.calls if "upload" in c]
    assert len(uploads) == 2
    # data file uploaded before the manifest (approximate atomicity)
    assert str(a) in uploads[0] and str(m) in uploads[1]
    assert "--clobber" in uploads[0]


def test_publish_ignores_release_create_failure(tmp_path: Path):
    # A pre-existing release makes `gh release create` fail; upload must still proceed.
    a = tmp_path / "ohlc_2026.parquet"; a.write_text("x")
    m = tmp_path / "manifest.json"; m.write_text("{}")
    r = FakeRunner(fail_on="create")
    publish.publish_release([a], m, tag="data-latest", repo="o/r", runner=r)  # no raise
    assert any("upload" in c for c in r.calls)


def test_publish_raises_when_an_upload_fails(tmp_path: Path):
    a = tmp_path / "ohlc_2026.parquet"; a.write_text("x")
    m = tmp_path / "manifest.json"; m.write_text("{}")
    r = FakeRunner(fail_on="ohlc_2026")  # the data upload fails
    with pytest.raises(UnexpectedFailure):
        publish.publish_release([a], m, tag="data-latest", repo="o/r", runner=r)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_publish.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.publish'`.

- [ ] **Step 3a: Modify `pipeline/src/pipeline/config.py`** — append after the existing constants:

```python
# Distribution (P1a): rolling GitHub Release served by GitHub's CDN.
GITHUB_REPO = "SatioO/guardian-universe"
RELEASE_TAG = "data-latest"
```

- [ ] **Step 3b: Write minimal implementation — `pipeline/src/pipeline/publish.py`**

```python
"""Publish artifacts as assets on a rolling GitHub Release via the `gh` CLI.

The command runner is injected so the upload sequence is unit-tested offline;
production uses `subprocess_runner`."""
from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path

from pipeline.errors import UnexpectedFailure

Runner = Callable[[list[str]], int]


def subprocess_runner(cmd: list[str]) -> int:
    return subprocess.run(cmd, check=False).returncode


def publish_release(
    data_files: list[Path],
    manifest_path: Path,
    *,
    tag: str,
    repo: str,
    runner: Runner,
) -> None:
    # Idempotent create: fails (non-zero) if the release already exists — ignore it.
    runner(["gh", "release", "create", tag, "--repo", repo, "--title", tag,
            "--notes", "automated data release"])
    # Upload DATA files first, then the manifest LAST (approximate atomicity:
    # clients that poll the manifest only see it after the data it references).
    for f in [*data_files, manifest_path]:
        rc = runner(["gh", "release", "upload", tag, str(f), "--clobber", "--repo", repo])
        if rc != 0:
            raise UnexpectedFailure(f"gh release upload failed ({rc}) for {f.name}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_publish.py -v && .venv/bin/ruff check . && .venv/bin/mypy`
Expected: 3 PASS, ruff clean, mypy Success.

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/config.py pipeline/src/pipeline/publish.py pipeline/tests/test_publish.py
git commit -m "feat(pipeline): GitHub-Release publish wrapper (injectable runner)"
```

---

### Task 3: Backfill orchestration

**Files:**
- Create: `pipeline/src/pipeline/backfill.py`
- Test: `pipeline/tests/test_backfill.py`

**Interfaces:**
- Consumes: `calendar.trading_days_back`, `daily_update.run_daily`, `daily_update.RunStatus`, `fetch.Fetcher`.
- Produces: `backfill.backfill(end: date, n: int, *, fetcher, holidays: set[date], base: Path, sleep: Callable[[float], None] = time.sleep, delay_s: float = 1.0) -> list[RunStatus]` — runs `run_daily` over the last `n` trading days ending `end` (ascending), sleeping `delay_s` between days (skips the sleep after the last). Resumable: already-ingested days return `"skipped_idempotent"` via `run_daily`.

- [ ] **Step 1: Write the failing test — `pipeline/tests/test_backfill.py`**

```python
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import store
from pipeline.backfill import backfill

HOLIDAYS: set[date] = set()
RAW = pd.read_csv(Path(__file__).parent / "fixtures" / "bhavcopy_normal.csv")


class StubFetcher:
    def __init__(self):
        self.dates: list[date] = []

    def fetch_raw(self, d: date) -> pd.DataFrame:
        self.dates.append(d)
        return RAW


def _no_sleep(_s: float) -> None:
    return None


def test_backfill_ingests_n_trading_days(tmp_path: Path, monkeypatch):
    from pipeline import config
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    f = StubFetcher()
    out = backfill(date(2026, 7, 3), 3, fetcher=f, holidays=HOLIDAYS, base=tmp_path,
                   sleep=_no_sleep)
    assert [s.status for s in out] == ["success", "success", "success"]
    assert len(f.dates) == 3  # fetched once per day, ascending
    assert f.dates == sorted(f.dates)


def test_backfill_is_resumable(tmp_path: Path, monkeypatch):
    from pipeline import config
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    backfill(date(2026, 7, 3), 3, fetcher=StubFetcher(), holidays=HOLIDAYS,
             base=tmp_path, sleep=_no_sleep)
    # Second run: every day already present -> idempotent skips, no refetch.
    f2 = StubFetcher()
    out = backfill(date(2026, 7, 3), 3, fetcher=f2, holidays=HOLIDAYS, base=tmp_path,
                   sleep=_no_sleep)
    assert [s.status for s in out] == ["skipped_idempotent"] * 3
    assert f2.dates == []
    assert store.has_day(tmp_path, date(2026, 7, 3))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_backfill.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.backfill'`.

- [ ] **Step 3: Write minimal implementation — `pipeline/src/pipeline/backfill.py`**

```python
"""One-time bootstrap: ingest a window of trading days via run_daily. Resumable."""
from __future__ import annotations

import time
from collections.abc import Callable
from datetime import date
from pathlib import Path

from pipeline import calendar as cal
from pipeline.daily_update import RunStatus, run_daily
from pipeline.fetch import Fetcher


def backfill(
    end: date,
    n: int,
    *,
    fetcher: Fetcher,
    holidays: set[date],
    base: Path,
    sleep: Callable[[float], None] = time.sleep,
    delay_s: float = 1.0,
) -> list[RunStatus]:
    dates = cal.trading_days_back(end, n, holidays)
    results: list[RunStatus] = []
    for i, d in enumerate(dates):
        results.append(run_daily(d, fetcher=fetcher, holidays=holidays, base=base))
        if i < len(dates) - 1:
            sleep(delay_s)  # polite delay: NSE burst-blocks rapid archive requests
    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_backfill.py -v && .venv/bin/ruff check . && .venv/bin/mypy`
Expected: 2 PASS, ruff clean, mypy Success.

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/backfill.py pipeline/tests/test_backfill.py
git commit -m "feat(pipeline): resumable backfill over a trading-day window"
```

---

### Task 4: `pipeline` CLI

**Files:**
- Create: `pipeline/src/pipeline/cli.py`
- Create: `pipeline/src/pipeline/__main__.py`
- Test: `pipeline/tests/test_cli.py`

**Interfaces:**
- Consumes: everything above + `config`, `calendar.load_holidays`, `fetch.NseUdiffFetcher`, `daily_update.run_daily`, `manifest`, `publish`.
- Produces:
  - `cli.build_parser() -> argparse.ArgumentParser` with subcommands `daily [--date YYYY-MM-DD]`, `backfill --days N`, `publish`.
  - `cli.cmd_publish(*, ohlc_dir, meta_dir, repo, tag, runner, generated_at) -> None` — builds the manifest (latest date derived from the newest `ohlc_*.parquet`... see below), writes it to `meta_dir/manifest.json`, and calls `publish.publish_release`.
  - `cli.main(argv: list[str] | None = None) -> int` — parses, wires real adapters, dispatches; returns process exit code (0 ok, 1 on a failed run).

- [ ] **Step 1: Write the failing test — `pipeline/tests/test_cli.py`**

```python
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import cli, config


def _write_parquet(p: Path, n: int) -> None:
    df = pd.DataFrame({c: [0] * n for c in config.CANON_COLUMNS})
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, compression="zstd", index=False)


def test_parser_reads_backfill_days():
    args = cli.build_parser().parse_args(["backfill", "--days", "42"])
    assert args.cmd == "backfill" and args.days == 42


def test_parser_reads_daily_date():
    args = cli.build_parser().parse_args(["daily", "--date", "2026-07-03"])
    assert args.cmd == "daily" and args.date == "2026-07-03"


def test_cmd_publish_writes_manifest_and_uploads(tmp_path: Path):
    ohlc = tmp_path / "ohlc"; meta = tmp_path / "meta"
    _write_parquet(ohlc / "ohlc_2026.parquet", 3)
    calls: list[list[str]] = []
    cli.cmd_publish(
        ohlc_dir=ohlc, meta_dir=meta, repo="o/r", tag="data-latest",
        runner=lambda cmd: (calls.append(cmd), 0)[1],
        generated_at="2026-07-03T00:00:00Z",
    )
    assert (meta / "manifest.json").exists()
    uploads = [c for c in calls if "upload" in c]
    assert any("ohlc_2026.parquet" in " ".join(c) for c in uploads)
    assert str(meta / "manifest.json") in uploads[-1]  # manifest last
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.cli'`.

- [ ] **Step 3a: Write `pipeline/src/pipeline/cli.py`**

```python
"""`python -m pipeline {daily,backfill,publish}` — wires real adapters."""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from pipeline import backfill as backfill_mod
from pipeline import calendar as cal
from pipeline import config, manifest, publish
from pipeline.daily_update import run_daily
from pipeline.fetch import NseUdiffFetcher


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("daily")
    d.add_argument("--date", default=None)
    b = sub.add_parser("backfill")
    b.add_argument("--days", type=int, required=True)
    sub.add_parser("publish")
    return p


def _latest_trading_date(ohlc_dir: Path) -> date:
    latest = date.min
    for p in ohlc_dir.glob("ohlc_*.parquet"):
        col = pd.read_parquet(p, columns=["date"])["date"]
        latest = max(latest, col.max().date())
    return latest


def cmd_publish(
    *,
    ohlc_dir: Path,
    meta_dir: Path,
    repo: str,
    tag: str,
    runner: publish.Runner,
    generated_at: str,
) -> None:
    m = manifest.build_manifest(
        ohlc_dir, schema_version=config.SCHEMA_VERSION,
        latest_trading_date=_latest_trading_date(ohlc_dir), generated_at=generated_at,
    )
    manifest_path = meta_dir / "manifest.json"
    manifest.write_json(m, manifest_path)
    data_files = sorted(ohlc_dir.glob("ohlc_*.parquet"))
    publish.publish_release(data_files, manifest_path, tag=tag, repo=repo, runner=runner)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    holidays = cal.load_holidays(config.META_DIR / "holidays.json")
    fetcher = NseUdiffFetcher()
    if args.cmd == "daily":
        target = date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()
        st = run_daily(target, fetcher=fetcher, holidays=holidays, base=config.OHLC_DIR)
        print(manifest.status_to_dict(st))
        return 0 if st.status in ("success", "skipped_holiday", "skipped_idempotent",
                                  "not_yet") else 1
    if args.cmd == "backfill":
        results = backfill_mod.backfill(
            datetime.now(UTC).date(), args.days,
            fetcher=fetcher, holidays=holidays, base=config.OHLC_DIR,
        )
        return 0 if all(
            r.status in ("success", "skipped_holiday", "skipped_idempotent", "not_yet")
            for r in results
        ) else 1
    # publish
    cmd_publish(
        ohlc_dir=config.OHLC_DIR, meta_dir=config.META_DIR,
        repo=config.GITHUB_REPO, tag=config.RELEASE_TAG,
        runner=publish.subprocess_runner,
        generated_at=datetime.now(UTC).isoformat(),
    )
    return 0
```

- [ ] **Step 3b: Write `pipeline/src/pipeline/__main__.py`**

```python
import sys

from pipeline.cli import main

if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_cli.py -v && .venv/bin/pytest -q && .venv/bin/ruff check . && .venv/bin/mypy`
Expected: 3 new tests PASS; full suite green with ZERO warnings; ruff clean; mypy Success.

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/cli.py pipeline/src/pipeline/__main__.py pipeline/tests/test_cli.py
git commit -m "feat(pipeline): CLI for daily/backfill/publish"
```

---

### Task 5: Local runbook + entry point

**Files:**
- Modify: `pipeline/pyproject.toml` (add a console-script entry point)
- Create: `RUNBOOK.md` (repo root)
- Test: none (docs + packaging) — verified by the full suite still passing and an import check.

**Interfaces:**
- Produces: a `guardian-pipeline` console script mapping to `pipeline.cli:main`; a RUNBOOK documenting the local bootstrap→publish procedure.

- [ ] **Step 1: Add the entry point to `pipeline/pyproject.toml`** — under `[project]`, add:

```toml
[project.scripts]
guardian-pipeline = "pipeline.cli:main"
```

- [ ] **Step 2: Create `RUNBOOK.md`** (repo root)

```markdown
# RUNBOOK — guardian-universe

## Bootstrap the historical baseline (local, one-time)
NSE blocks datacenter IPs, so run backfill from a residential machine:
```
cd pipeline && uv run python -m pipeline backfill --days 300
```
Resumable — re-run to continue after an interruption (already-ingested days skip).

## Publish to the CDN (rolling GitHub Release)
Requires `gh` authenticated with write access to `SatioO/guardian-universe`:
```
cd pipeline && uv run python -m pipeline publish
```
Uploads `data/ohlc/ohlc_*.parquet` + `data/meta/manifest.json` to the `data-latest`
release. Clients read `manifest.json`, verify each file's `sha256`, then download.

## Alerts
- Row-count deviation / format break → run exits non-zero (fail-closed).
- Corrupt day (all rows quarantined) → `status: failed`, nothing written, retryable.

## Yearly
- Refresh `pipeline/data/meta/holidays.json` from NSE's published holiday calendar.
```

- [ ] **Step 3: Reinstall + verify the entry point resolves and the suite is green**

Run:
```bash
cd pipeline && uv pip install -e ".[dev]" && .venv/bin/pytest -q && .venv/bin/ruff check . && .venv/bin/mypy
.venv/bin/python -c "import pipeline.cli; print('cli import ok')"
```
Expected: full suite green (zero warnings), ruff clean, mypy Success, `cli import ok`.

- [ ] **Step 4: Commit**

```bash
git add pipeline/pyproject.toml RUNBOOK.md
git commit -m "docs(pipeline): console-script entry point + local RUNBOOK"
```

---

## What P1a delivers (Definition of Done)

- `python -m pipeline backfill --days N` ingests N trading days locally (resumable, fail-closed) via the P0 pipeline.
- `python -m pipeline publish` builds a checksummed `manifest.json` and uploads it + the year parquet files to the rolling `data-latest` GitHub Release (data files first, manifest last).
- All logic is deterministic-tested offline (injected runner/sleep/timestamp); the real publish is validated by running `python -m pipeline publish` once against the repo (a manual/end-of-plan step, like P0's CI).
- Full suite green, zero warnings, ruff + mypy --strict clean.

## Deferred to later P1 slices (explicitly NOT in P1a)

- **P1b — daily CI workflow** (`data-daily.yml`, two crons). Carries an open risk: NSE may block GitHub-Actions IPs on the archive fetch; resolve with the fallback chain, a proxy, or a self-hosted/residential runner before relying on CI for ingestion. (Local `backfill`/`daily` + `publish` work regardless.)
- **P1c — NSE indices** dataset (separate indices bhavcopy; `series="INDEX"`, `instrument_key`=index code, `isin` empty).
- **P2** — auto-issue-on-failure, freshness dead-man's-switch, SHA-pinned actions, Dependabot.

## Spec-coverage self-review

- Manifest with checksums + `latest_trading_date` (spec §5.1, §6.2): Task 1. ✔
- Atomic publish (data first, manifest last) to CDN via GitHub Releases (spec §5.1; user decision): Task 2 + Task 4. ✔
- Backfill 300 trading days, resumable, local, polite delay (spec §5.3): Task 3 + RUNBOOK. ✔
- `last_run_status.json` shape (spec §5.1): Task 1 `status_to_dict`. ✔ (wired into the daily/publish flow; full status-file emission lands with the CI workflow in P1b — noted.)
- CLI/entry point to run headlessly (needed by P1b workflow): Task 4 + Task 5. ✔
- Daily two-cron workflow, indices, SHA-pinning: **deferred to P1b/P1c/P2** (noted). ✔ (partial-by-design)
