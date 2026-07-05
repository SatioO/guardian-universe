# G0 — Store Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the publish/sync path physically unable to lose or tear data: content-addressed release assets with a manifest-last flip, fail-closed sha-verified sync, shrink/CAS/post-publish guards, atomic local writes, and publish-only-on-success — proven by a chaos test suite.

**Architecture:** All `gh` CLI interaction moves behind a new `ReleaseClient` seam (`release.py`) with an in-memory `FakeReleaseClient` for offline tests. Data parquet files upload under content-addressed names (`ohlc_2026.<sha8>.parquet`) and are never clobbered; `manifest.json` is the only mutable asset and is flipped last, so any manifest a client reads references complete, verifiable, still-present files. `sync` verifies every checksum and fails closed. `publish` refuses to shrink coverage, aborts on concurrent modification (compare-and-swap on `generated_at`), verifies itself after the flip, and garbage-collects unreferenced assets only after a 7-day grace.

**Tech Stack:** Python 3.11, pandas, pyarrow (present), pandera, pytest, mypy --strict, ruff. GitHub Releases via `gh` CLI.

**Spec:** `docs/superpowers/specs/2026-07-05-scanner-platform-v2-design.md` §2 (Distribution & store correctness) + §4 (atomic writes, publish-only-on-success). Copied into this repo alongside this plan.

**Branch:** `feat/g0-store-correctness` off `origin/main` (67eac36). The existing local branch `feat/p1c-indices` is parked WIP — do NOT build on it; its DatasetSpec registry work is absorbed later by G1.

## Global Constraints

1. Working directory for all commands: `pipeline/` inside the repo root `~/Desktop/projects/guardian-universe`.
2. `pytest` runs under `filterwarnings = ["error"]` — any warning is a test failure. Follow existing empty-frame concat guards.
3. `mypy --strict` and `ruff check .` must pass after every task. Run all three before every commit: `python -m pytest -q && mypy && ruff check .`
4. No live network in tests. `gh` is never invoked in tests — everything goes through `FakeReleaseClient` or a recorded fake runner.
5. `run_daily` semantics are untouched in G0 (never raises; returns `RunStatus`). Do not modify `daily_update.py`, `fetch.py`, `normalize.py`, `validate.py`, `calendar.py`, `backfill.py`, `freshness.py`.
6. Backward compatibility with the LIVE v1 manifest: live file entries may lack `"asset"` and `"rows"` keys. Reading code must use `entry.get("asset", entry["name"])` and treat missing `"rows"` as unknown (skip that check).
7. The release tag stays `data-latest` (`config.RELEASE_TAG`); repo `config.GITHUB_REPO`. `manifest.json` and `last_run_status.json` are PROTECTED assets — never GC'd, always uploaded with clobber.
8. Commit after every task with the exact message given.

---

### Task 1: `ReleaseClient` seam + `GhReleaseClient` + `FakeReleaseClient`

**Files:**
- Modify: `pipeline/src/pipeline/errors.py`
- Create: `pipeline/src/pipeline/release.py`
- Create: `pipeline/tests/fakes.py`
- Create: `pipeline/tests/test_release.py`

**Interfaces:**
- Consumes: `pipeline.errors.PipelineError` (existing base class).
- Produces (later tasks rely on these exact names):
  - `errors.ReleaseError(PipelineError)`
  - `release.CaptureRunner = Callable[[list[str]], tuple[int, str, str]]` and `release.subprocess_capture(cmd) -> tuple[int, str, str]`
  - `release.AssetInfo` frozen dataclass: `name: str`, `created_at: str`
  - `release.ReleaseClient` Protocol: `exists() -> bool`, `create() -> None`, `list_assets() -> list[AssetInfo]`, `download(names: list[str], dest: Path) -> None`, `upload(path: Path, *, clobber: bool = False) -> None`, `delete_asset(name: str) -> None`
  - `release.GhReleaseClient(*, repo: str, tag: str, runner: CaptureRunner = subprocess_capture)`
  - `tests.fakes.FakeReleaseClient(*, exists: bool = False, now_iso: str = "2026-07-05T12:00:00Z")` with `.assets: dict[str, bytes]`, `.created_at: dict[str, str]`, `.fail_after: int | None`, `.seed(name, data, created_at=None)` helper
  - `tests.fakes.assert_release_consistent(fake) -> None`

- [ ] **Step 1: Write the failing tests**

Create `pipeline/tests/test_release.py`:

```python
from pathlib import Path

import pytest

from pipeline.errors import ReleaseError
from pipeline.release import AssetInfo, GhReleaseClient
from tests.fakes import FakeReleaseClient


class RecordingRunner:
    """Records commands; returns scripted (rc, stdout, stderr) per call."""

    def __init__(self, results: list[tuple[int, str, str]]) -> None:
        self.calls: list[list[str]] = []
        self._results = list(results)

    def __call__(self, cmd: list[str]) -> tuple[int, str, str]:
        self.calls.append(cmd)
        return self._results.pop(0)


def test_exists_true_on_rc0():
    r = RecordingRunner([(0, "{}", "")])
    assert GhReleaseClient(repo="o/r", tag="t", runner=r).exists() is True
    assert r.calls[0][:2] == ["gh", "api"] and "releases/tags/t" in r.calls[0][2]


def test_exists_false_on_404():
    r = RecordingRunner([(1, "", "gh: Not Found (HTTP 404)")])
    assert GhReleaseClient(repo="o/r", tag="t", runner=r).exists() is False


def test_exists_raises_on_other_error():
    r = RecordingRunner([(1, "", "network unreachable")])
    with pytest.raises(ReleaseError):
        GhReleaseClient(repo="o/r", tag="t", runner=r).exists()


def test_list_assets_parses_names_and_dates():
    out = '[{"name": "a.parquet", "created_at": "2026-07-01T00:00:00Z"}]'
    r = RecordingRunner([(0, out, "")])
    assets = GhReleaseClient(repo="o/r", tag="t", runner=r).list_assets()
    assert assets == [AssetInfo(name="a.parquet", created_at="2026-07-01T00:00:00Z")]


def test_download_raises_when_file_absent_after_rc0(tmp_path: Path):
    # gh returns 0 for a pattern matching nothing -> must still be an error.
    r = RecordingRunner([(0, "", "")])
    with pytest.raises(ReleaseError):
        GhReleaseClient(repo="o/r", tag="t", runner=r).download(["missing.parquet"], tmp_path)


def test_upload_appends_clobber_only_when_asked(tmp_path: Path):
    f = tmp_path / "m.json"
    f.write_text("{}")
    r = RecordingRunner([(0, "", ""), (0, "", "")])
    c = GhReleaseClient(repo="o/r", tag="t", runner=r)
    c.upload(f)
    c.upload(f, clobber=True)
    assert "--clobber" not in r.calls[0]
    assert "--clobber" in r.calls[1]


def test_fake_roundtrip_and_failure_injection(tmp_path: Path):
    fake = FakeReleaseClient()
    fake.create()
    f = tmp_path / "x.bin"
    f.write_bytes(b"abc")
    fake.upload(f)
    fake.download(["x.bin"], tmp_path / "out")
    assert (tmp_path / "out" / "x.bin").read_bytes() == b"abc"
    fake.fail_after = fake.ops  # next op fails
    with pytest.raises(ReleaseError):
        fake.list_assets()


def test_fake_upload_without_clobber_rejects_existing(tmp_path: Path):
    fake = FakeReleaseClient(exists=True)
    f = tmp_path / "x.bin"
    f.write_bytes(b"abc")
    fake.upload(f)
    with pytest.raises(ReleaseError):
        fake.upload(f)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/Desktop/projects/guardian-universe/pipeline && python -m pytest tests/test_release.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.release'`

- [ ] **Step 3: Implement**

Append to `pipeline/src/pipeline/errors.py`:

```python
class ReleaseError(PipelineError):
    """A GitHub Release operation failed (network, auth, missing asset)."""
```

Create `pipeline/src/pipeline/release.py`:

```python
"""GitHub Release access layer: the single seam for `gh` CLI interaction.

`ReleaseClient` is the injectable protocol; production uses `GhReleaseClient`
(subprocess `gh`), tests use `tests.fakes.FakeReleaseClient`. Keeping every
`gh` invocation here means publish/sync logic stays pure and offline-testable."""
from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from pipeline.errors import ReleaseError

CaptureRunner = Callable[[list[str]], tuple[int, str, str]]


def subprocess_capture(cmd: list[str]) -> tuple[int, str, str]:
    p = subprocess.run(cmd, check=False, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


@dataclass(frozen=True)
class AssetInfo:
    name: str
    created_at: str  # ISO-8601 as returned by the GitHub API


class ReleaseClient(Protocol):
    def exists(self) -> bool: ...
    def create(self) -> None: ...
    def list_assets(self) -> list[AssetInfo]: ...
    def download(self, names: list[str], dest: Path) -> None: ...
    def upload(self, path: Path, *, clobber: bool = False) -> None: ...
    def delete_asset(self, name: str) -> None: ...


class GhReleaseClient:
    def __init__(self, *, repo: str, tag: str, runner: CaptureRunner = subprocess_capture) -> None:
        self._repo = repo
        self._tag = tag
        self._run = runner

    def exists(self) -> bool:
        rc, _, err = self._run(["gh", "api", f"repos/{self._repo}/releases/tags/{self._tag}"])
        if rc == 0:
            return True
        if "404" in err:
            return False
        raise ReleaseError(f"cannot determine release state: {err.strip()}")

    def create(self) -> None:
        rc, _, err = self._run([
            "gh", "release", "create", self._tag, "--repo", self._repo,
            "--title", self._tag, "--notes", "automated data release",
        ])
        if rc != 0:
            raise ReleaseError(f"release create failed: {err.strip()}")

    def list_assets(self) -> list[AssetInfo]:
        rc, out, err = self._run([
            "gh", "api", f"repos/{self._repo}/releases/tags/{self._tag}",
            "--jq", "[.assets[] | {name, created_at}]",
        ])
        if rc != 0:
            raise ReleaseError(f"asset listing failed: {err.strip()}")
        parsed: list[dict[str, str]] = json.loads(out)
        return [AssetInfo(name=a["name"], created_at=a["created_at"]) for a in parsed]

    def download(self, names: list[str], dest: Path) -> None:
        dest.mkdir(parents=True, exist_ok=True)
        for name in names:
            rc, _, err = self._run([
                "gh", "release", "download", self._tag, "--repo", self._repo,
                "--pattern", name, "--dir", str(dest), "--clobber",
            ])
            if rc != 0 or not (dest / name).exists():
                raise ReleaseError(f"download failed for {name}: {err.strip()}")

    def upload(self, path: Path, *, clobber: bool = False) -> None:
        cmd = ["gh", "release", "upload", self._tag, str(path), "--repo", self._repo]
        if clobber:
            cmd.append("--clobber")
        rc, _, err = self._run(cmd)
        if rc != 0:
            raise ReleaseError(f"upload failed for {path.name}: {err.strip()}")

    def delete_asset(self, name: str) -> None:
        rc, _, err = self._run([
            "gh", "release", "delete-asset", self._tag, name, "--repo", self._repo, "--yes",
        ])
        if rc != 0:
            raise ReleaseError(f"asset delete failed for {name}: {err.strip()}")
```

Create `pipeline/tests/fakes.py`:

```python
"""Deterministic in-memory ReleaseClient + release-consistency invariant.

`fail_after=N` makes the (N+1)-th and every later client operation raise —
the chaos-test harness for torn-publish scenarios."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pipeline.errors import ReleaseError
from pipeline.release import AssetInfo


class FakeReleaseClient:
    def __init__(self, *, exists: bool = False, now_iso: str = "2026-07-05T12:00:00Z") -> None:
        self.assets: dict[str, bytes] = {}
        self.created_at: dict[str, str] = {}
        self.now_iso = now_iso
        self._exists = exists
        self.fail_after: int | None = None
        self.ops = 0

    def _tick(self) -> None:
        self.ops += 1
        if self.fail_after is not None and self.ops > self.fail_after:
            raise ReleaseError(f"injected failure at op {self.ops}")

    def exists(self) -> bool:
        self._tick()
        return self._exists

    def create(self) -> None:
        self._tick()
        self._exists = True

    def list_assets(self) -> list[AssetInfo]:
        self._tick()
        return [AssetInfo(name=n, created_at=self.created_at[n]) for n in sorted(self.assets)]

    def download(self, names: list[str], dest: Path) -> None:
        self._tick()
        dest.mkdir(parents=True, exist_ok=True)
        for name in names:
            if name not in self.assets:
                raise ReleaseError(f"asset not found: {name}")
            (dest / name).write_bytes(self.assets[name])

    def upload(self, path: Path, *, clobber: bool = False) -> None:
        self._tick()
        if path.name in self.assets and not clobber:
            raise ReleaseError(f"asset exists: {path.name}")
        self.assets[path.name] = path.read_bytes()
        self.created_at[path.name] = self.now_iso

    def delete_asset(self, name: str) -> None:
        self._tick()
        if name not in self.assets:
            raise ReleaseError(f"asset not found: {name}")
        del self.assets[name]
        del self.created_at[name]

    # -- test helpers (not part of the ReleaseClient protocol) --
    def seed(self, name: str, data: bytes, created_at: str | None = None) -> None:
        self.assets[name] = data
        self.created_at[name] = created_at or self.now_iso
        self._exists = True


def assert_release_consistent(fake: FakeReleaseClient) -> None:
    """The G0 invariant: whatever manifest is live, every file it references
    exists on the release with a matching sha256."""
    if "manifest.json" not in fake.assets:
        return
    manifest = json.loads(fake.assets["manifest.json"])
    for ds in manifest["datasets"]:
        for entry in ds["files"]:
            asset = entry.get("asset", entry["name"])
            assert asset in fake.assets, f"manifest references missing asset {asset}"
            sha = hashlib.sha256(fake.assets[asset]).hexdigest()
            assert sha == entry["sha256"], f"sha mismatch for {asset}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_release.py -q`
Expected: 8 passed

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/errors.py src/pipeline/release.py tests/fakes.py tests/test_release.py
git commit -m "feat(g0/task-1): ReleaseClient seam — GhReleaseClient + FakeReleaseClient + release invariant"
```

---

### Task 2: Content-addressed asset names + row counts in the manifest

**Files:**
- Modify: `pipeline/src/pipeline/manifest.py`
- Modify: `pipeline/tests/test_manifest.py`

**Interfaces:**
- Consumes: existing `manifest.file_digest(path) -> tuple[str, int]`, `manifest.build_manifest(ohlc_dir, *, schema_version, latest_trading_date, generated_at) -> dict`.
- Produces:
  - `manifest.asset_name(logical: str, sha256: str) -> str` — `"ohlc_2026.parquet", "a1b2..."` → `"ohlc_2026.a1b2c3d4.parquet"` (first 8 hex chars)
  - `manifest.parquet_rows(path: Path) -> int`
  - `build_manifest` file entries now carry keys: `name`, `asset`, `sha256`, `bytes`, `rows` (signature unchanged).

- [ ] **Step 1: Write the failing tests**

Append to `pipeline/tests/test_manifest.py`:

```python
def test_asset_name_inserts_sha8_before_extension():
    from pipeline.manifest import asset_name
    sha = "a1b2c3d4" + "0" * 56
    assert asset_name("ohlc_2026.parquet", sha) == "ohlc_2026.a1b2c3d4.parquet"


def test_build_manifest_entries_have_asset_and_rows(tmp_path):
    import pandas as pd
    from datetime import date
    from pipeline import config
    from pipeline.manifest import asset_name, build_manifest

    df = pd.DataFrame({c: [0, 0, 0] for c in config.CANON_COLUMNS})
    p = tmp_path / "ohlc_2026.parquet"
    df.to_parquet(p, compression="zstd", index=False)

    m = build_manifest(tmp_path, schema_version=1,
                       latest_trading_date=date(2026, 7, 3), generated_at="g")
    entry = m["datasets"][0]["files"][0]
    assert entry["rows"] == 3
    assert entry["asset"] == asset_name("ohlc_2026.parquet", entry["sha256"])
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_manifest.py -q`
Expected: FAIL — `ImportError: cannot import name 'asset_name'`

- [ ] **Step 3: Implement**

In `pipeline/src/pipeline/manifest.py`, add after `file_digest` (and add `import pandas as pd` at the top imports):

```python
def asset_name(logical: str, sha256: str) -> str:
    """Content-addressed release asset name: sha8 spliced before the extension.

    Assets named this way are immutable by construction — new content gets a
    new name, so nothing on the release is ever clobbered except manifest.json."""
    stem, _, ext = logical.rpartition(".")
    return f"{stem}.{sha256[:8]}.{ext}"


def parquet_rows(path: Path) -> int:
    # Column-pruned read: cheap at this scale and avoids a pyarrow-stubs
    # dependency for mypy --strict.
    return int(len(pd.read_parquet(path, columns=["date"])))
```

In `build_manifest`, replace the `files.append(...)` line with:

```python
        files.append({
            "name": p.name,
            "asset": asset_name(p.name, sha),
            "sha256": sha,
            "bytes": size,
            "rows": parquet_rows(p),
        })
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_manifest.py -q`
Expected: all pass (existing manifest tests must still pass — the new keys are additive)

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/manifest.py tests/test_manifest.py
git commit -m "feat(g0/task-2): content-addressed asset names + row counts in manifest entries"
```

---

### Task 3: Atomic parquet writes in the store

**Files:**
- Modify: `pipeline/src/pipeline/store.py`
- Modify: `pipeline/tests/test_store.py`

**Interfaces:**
- Consumes/Produces: `store.append_day(df, base)` — signature unchanged; behavior gains crash-atomicity (write `*.parquet.tmp`, then `Path.replace`).

- [ ] **Step 1: Write the failing test**

Append to `pipeline/tests/test_store.py`:

```python
def test_append_day_is_atomic_on_write_crash(tmp_path, monkeypatch):
    import pandas as pd
    import pytest
    from pipeline import config, store

    def frame(day: str) -> pd.DataFrame:
        row = {c: ["x"] for c in config.CANON_COLUMNS}
        df = pd.DataFrame(row)
        df["date"] = pd.to_datetime([day])
        df["instrument_key"] = ["INE1"]
        return df

    store.append_day(frame("2026-07-02"), tmp_path)
    good = config.ohlc_path(2026, tmp_path).read_bytes()

    original = pd.DataFrame.to_parquet

    def boom(self, path, *a, **kw):  # crash mid-write: leave a torn tmp file
        Path(str(path)).write_bytes(b"torn")
        raise OSError("disk full")

    from pathlib import Path
    monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)
    with pytest.raises(OSError):
        store.append_day(frame("2026-07-03"), tmp_path)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", original)

    # The published year file is untouched by the crashed write.
    assert config.ohlc_path(2026, tmp_path).read_bytes() == good
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_store.py::test_append_day_is_atomic_on_write_crash -q`
Expected: FAIL — the final assert (current code writes directly to the target path, so the crash leaves `b"torn"` in the year file)

- [ ] **Step 3: Implement**

In `pipeline/src/pipeline/store.py`, in `append_day`, replace:

```python
        combined.to_parquet(
            config.ohlc_path(int(year), base), compression="zstd", index=False
        )
```

with:

```python
        # Crash-atomic: write to a temp sibling, then atomically replace.
        target = config.ohlc_path(int(year), base)
        tmp = target.with_suffix(".parquet.tmp")
        combined.to_parquet(tmp, compression="zstd", index=False)
        tmp.replace(target)
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_store.py -q`
Expected: all pass

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/store.py tests/test_store.py
git commit -m "fix(g0/task-3): crash-atomic parquet writes via tmp + replace"
```

---

### Task 4: Fail-closed, sha-verified sync

**Files:**
- Create: `pipeline/src/pipeline/sync.py`
- Create: `pipeline/tests/test_sync.py`
- Modify: `pipeline/src/pipeline/cli.py` (replace `cmd_sync` wiring)
- Modify: `pipeline/tests/test_cli.py` (replace the two `cmd_sync` tests)

**Interfaces:**
- Consumes: `release.ReleaseClient`, `errors.ReleaseError`, `errors.UnexpectedFailure`, `manifest.file_digest`, `manifest.write_json`.
- Produces:
  - `sync.SYNCED_STATE = "synced_manifest.json"` (module constant; file lives in `meta_dir`)
  - `sync.sync_store(client: ReleaseClient, *, ohlc_dir: Path, meta_dir: Path, work_dir: Path) -> dict[str, Any] | None` — returns the live manifest dict, or `None` when the release does not exist (first run). Raises `ReleaseError`/`UnexpectedFailure` on ANY other failure (fail-closed). Always writes `meta_dir/synced_manifest.json`: the live manifest verbatim, or `{"generated_at": None}` when no release.
  - CLI: `python -m pipeline sync` exits **1** on any sync failure (this is the P0-1 fix).

- [ ] **Step 1: Write the failing tests**

Create `pipeline/tests/test_sync.py`:

```python
import hashlib
import json
from pathlib import Path

import pytest

from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.manifest import asset_name
from pipeline.sync import SYNCED_STATE, sync_store
from tests.fakes import FakeReleaseClient


def _seeded_release(data: bytes = b"PARQUETDATA") -> tuple[FakeReleaseClient, str]:
    sha = hashlib.sha256(data).hexdigest()
    manifest = {
        "schema_version": 1,
        "generated_at": "2026-07-04T16:00:00+00:00",
        "latest_trading_date": "2026-07-03",
        "datasets": [{"name": "ohlc", "files": [{
            "name": "ohlc_2026.parquet",
            "asset": asset_name("ohlc_2026.parquet", sha),
            "sha256": sha, "bytes": len(data), "rows": 1,
        }]}],
    }
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed(asset_name("ohlc_2026.parquet", sha), data)
    return fake, sha


def test_sync_downloads_verifies_and_writes_state(tmp_path: Path):
    fake, _ = _seeded_release()
    got = sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                     work_dir=tmp_path / "work")
    assert got is not None and got["generated_at"] == "2026-07-04T16:00:00+00:00"
    # Asset is materialized under its LOGICAL name for the store to append onto.
    assert (tmp_path / "ohlc" / "ohlc_2026.parquet").read_bytes() == b"PARQUETDATA"
    state = json.loads((tmp_path / "meta" / SYNCED_STATE).read_text())
    assert state["generated_at"] == "2026-07-04T16:00:00+00:00"


def test_sync_no_release_writes_empty_state_and_returns_none(tmp_path: Path):
    fake = FakeReleaseClient(exists=False)
    got = sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                     work_dir=tmp_path / "work")
    assert got is None
    state = json.loads((tmp_path / "meta" / SYNCED_STATE).read_text())
    assert state == {"generated_at": None}


def test_sync_checksum_mismatch_is_fatal(tmp_path: Path):
    fake, sha = _seeded_release()
    fake.assets[asset_name("ohlc_2026.parquet", sha)] = b"CORRUPTED"
    with pytest.raises(UnexpectedFailure, match="checksum"):
        sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                   work_dir=tmp_path / "work")


def test_sync_download_failure_is_fatal_not_tolerated(tmp_path: Path):
    fake, _ = _seeded_release()
    fake.fail_after = 1  # exists() succeeds, first download fails
    with pytest.raises(ReleaseError):
        sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                   work_dir=tmp_path / "work")


def test_sync_legacy_manifest_without_asset_key(tmp_path: Path):
    # Live v1 manifests name assets by logical name only.
    data = b"LEGACY"
    sha = hashlib.sha256(data).hexdigest()
    manifest = {"schema_version": 1, "generated_at": "g", "latest_trading_date": "2026-07-03",
                "datasets": [{"name": "ohlc", "files": [{
                    "name": "ohlc_2026.parquet", "sha256": sha, "bytes": len(data)}]}]}
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed("ohlc_2026.parquet", data)
    got = sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                     work_dir=tmp_path / "work")
    assert got is not None
    assert (tmp_path / "ohlc" / "ohlc_2026.parquet").read_bytes() == b"LEGACY"
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_sync.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.sync'`

- [ ] **Step 3: Implement**

Create `pipeline/src/pipeline/sync.py`:

```python
"""Fail-closed sync: pull the published dataset and verify every checksum.

The P0-1 fix: any failure other than "release does not exist" aborts the run.
A transient download failure must NEVER leave an empty store that a later
publish would present as the new truth."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.errors import UnexpectedFailure
from pipeline.manifest import file_digest, write_json
from pipeline.release import ReleaseClient

SYNCED_STATE = "synced_manifest.json"


def sync_store(
    client: ReleaseClient, *, ohlc_dir: Path, meta_dir: Path, work_dir: Path
) -> dict[str, Any] | None:
    ohlc_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    if not client.exists():
        write_json({"generated_at": None}, meta_dir / SYNCED_STATE)
        return None

    client.download(["manifest.json"], work_dir)
    manifest: dict[str, Any] = json.loads((work_dir / "manifest.json").read_text())
    for ds in manifest.get("datasets", []):
        for entry in ds["files"]:
            asset = entry.get("asset", entry["name"])
            client.download([asset], work_dir)
            got = work_dir / asset
            sha, _ = file_digest(got)
            if sha != entry["sha256"]:
                raise UnexpectedFailure(
                    f"sync checksum mismatch for {asset}: got {sha}, manifest says {entry['sha256']}"
                )
            got.replace(ohlc_dir / entry["name"])
    write_json(manifest, meta_dir / SYNCED_STATE)
    return manifest
```

In `pipeline/src/pipeline/cli.py`:
- Delete the `cmd_sync` function entirely.
- Add imports: `from pipeline.errors import ReleaseError, UnexpectedFailure` (replacing the bare `UnexpectedFailure` import), `from pipeline.release import GhReleaseClient`, `from pipeline.sync import sync_store`.
- Replace the `if args.cmd == "sync":` block in `main()` with:

```python
    if args.cmd == "sync":
        client = GhReleaseClient(repo=config.GITHUB_REPO, tag=config.RELEASE_TAG)
        with tempfile.TemporaryDirectory() as tmp:
            try:
                sync_store(client, ohlc_dir=config.OHLC_DIR, meta_dir=config.META_DIR,
                           work_dir=Path(tmp))
            except (ReleaseError, UnexpectedFailure) as e:
                print(f"sync failed: {e}", file=sys.stderr)
                return 1
        return 0
```

In `pipeline/tests/test_cli.py`:
- Delete `test_cmd_sync_downloads_ohlc_pattern` and `test_cmd_sync_tolerates_missing_release` (they assert the dangerous P0-1 behavior).
- Add:

```python
def test_main_sync_returns_1_on_failure(monkeypatch):
    from pipeline.errors import ReleaseError

    def _boom(*a, **kw):
        raise ReleaseError("network down")

    monkeypatch.setattr(cli, "sync_store", _boom)
    assert cli.main(["sync"]) == 1


def test_main_sync_returns_0_on_success(monkeypatch):
    monkeypatch.setattr(cli, "sync_store", lambda *a, **kw: None)
    assert cli.main(["sync"]) == 0
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_sync.py tests/test_cli.py -q`
Expected: all pass

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/sync.py src/pipeline/cli.py tests/test_sync.py tests/test_cli.py
git commit -m "fix(g0/task-4): fail-closed sha-verified sync — kills the P0 history-wipe path"
```

---

### Task 5: Publish v2 — guards, manifest-last flip, post-publish verify, GC

**Files:**
- Rewrite: `pipeline/src/pipeline/publish.py`
- Modify: `pipeline/src/pipeline/cli.py` (rewire `cmd_publish` + `check-freshness` runner import; move `_latest_trading_date` out)
- Rewrite: `pipeline/tests/test_publish.py`
- Modify: `pipeline/tests/test_cli.py` (rewire publish tests)

**Interfaces:**
- Consumes: `release.ReleaseClient`, `manifest.build_manifest/asset_name/file_digest/write_json`, `sync.SYNCED_STATE`.
- Produces:
  - `publish.PROTECTED_ASSETS = frozenset({"manifest.json", "last_run_status.json"})`
  - `publish.GC_GRACE = timedelta(days=7)`
  - `publish.latest_trading_date(ohlc_dir: Path) -> date` — moved from `cli._latest_trading_date`, now raising `UnexpectedFailure` on an empty/dateless store (fixes the NaT crash).
  - `publish.check_no_shrink(new: dict, live: dict | None) -> None`
  - `publish.check_cas(live: dict | None, synced: dict) -> None`
  - `publish.publish_dataset(*, ohlc_dir: Path, meta_dir: Path, stage_dir: Path, client: ReleaseClient, schema_version: int, generated_at: str, now: datetime) -> None`
  - The old `publish.publish_release`, `publish.Runner`, `publish.subprocess_runner` are DELETED. `cmd_check_freshness` in `cli.py` switches its runner type to a local shim (see Step 3).
  - CLI: `python -m pipeline publish` exits 1 on `ReleaseError | UnexpectedFailure`.

**Publish order (the whole point — implement exactly):**
1. Refuse empty store; build new manifest (with `asset`/`rows`).
2. `client.exists()` else `client.create()`.
3. Read live manifest (download `manifest.json`; any `ReleaseError` → treat as `None` — CAS below still protects, because a live release whose manifest can't be read will mismatch the synced state).
4. Read synced state from `meta_dir/synced_manifest.json`; missing file → `UnexpectedFailure("run sync before publish")`.
5. `check_cas(live, synced)` — abort if the release changed since our sync.
6. `check_no_shrink(new, live)` — abort if coverage regresses.
7. Upload data assets **not already on the release** (stage each under its content-addressed name; NO clobber).
8. Upload `last_run_status.json` (clobber) if present.
9. Write + upload `manifest.json` (clobber) — **THE FLIP, strictly last**.
10. Verify: re-download manifest → parsed-equal to what we published; re-download the smallest data asset → sha matches.
11. GC: delete assets not referenced by the new manifest, not PROTECTED, and older than `GC_GRACE` (parse `created_at`; failures print a warning, never fail the run).
12. Update `meta_dir/synced_manifest.json` to the new manifest.

- [ ] **Step 1: Write the failing tests**

Replace `pipeline/tests/test_publish.py` entirely:

```python
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from pipeline import config
from pipeline.errors import UnexpectedFailure
from pipeline.manifest import asset_name, write_json
from pipeline.publish import (
    check_cas,
    check_no_shrink,
    latest_trading_date,
    publish_dataset,
)
from pipeline.sync import SYNCED_STATE
from tests.fakes import FakeReleaseClient, assert_release_consistent

NOW = datetime(2026, 7, 5, 16, 0, tzinfo=UTC)


def _store(tmp_path: Path, days: list[str]) -> tuple[Path, Path, Path]:
    ohlc, meta, stage = tmp_path / "ohlc", tmp_path / "meta", tmp_path / "stage"
    ohlc.mkdir(); meta.mkdir()
    rows = {c: ["x"] * len(days) for c in config.CANON_COLUMNS}
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(days)
    df["instrument_key"] = [f"INE{i}" for i in range(len(days))]
    df.to_parquet(ohlc / "ohlc_2026.parquet", compression="zstd", index=False)
    return ohlc, meta, stage


def _synced(meta: Path, generated_at: str | None) -> None:
    write_json({"generated_at": generated_at}, meta / SYNCED_STATE)


def test_first_publish_creates_release_flips_manifest_last(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")
    publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                    schema_version=1, generated_at="2026-07-05T16:00:00+00:00", now=NOW)
    assert_release_consistent(fake)
    live = json.loads(fake.assets["manifest.json"])
    entry = live["datasets"][0]["files"][0]
    assert entry["asset"].startswith("ohlc_2026.") and entry["asset"] != "ohlc_2026.parquet"


def test_publish_requires_prior_sync(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    fake = FakeReleaseClient(exists=False)
    with pytest.raises(UnexpectedFailure, match="sync"):
        publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                        schema_version=1, generated_at="g", now=NOW)


def test_cas_aborts_when_release_moved_since_sync(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, "2026-07-04T16:00:00+00:00")  # what we synced
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps({
        "generated_at": "2026-07-05T09:00:00+00:00",  # someone published since
        "latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": []}],
    }).encode())
    with pytest.raises(UnexpectedFailure, match="changed since sync"):
        publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                        schema_version=1, generated_at="g", now=NOW)
    assert_release_consistent(fake)


def test_shrink_guard_blocks_row_regression():
    new = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2026.parquet", "rows": 1, "sha256": "s", "bytes": 1, "asset": "a"}]}]}
    live = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2026.parquet", "rows": 5000, "sha256": "t", "bytes": 9, "asset": "b"}]}]}
    with pytest.raises(UnexpectedFailure, match="shrink"):
        check_no_shrink(new, live)


def test_shrink_guard_blocks_missing_year_and_date_regression():
    live = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2025.parquet", "sha256": "t", "bytes": 9}]}]}
    new_missing = {"latest_trading_date": "2026-07-03",
                   "datasets": [{"name": "ohlc", "files": []}]}
    with pytest.raises(UnexpectedFailure, match="shrink"):
        check_no_shrink(new_missing, live)
    new_regress = {"latest_trading_date": "2026-07-01", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2025.parquet", "rows": 1, "sha256": "s", "bytes": 1, "asset": "a"}]}]}
    with pytest.raises(UnexpectedFailure, match="regress"):
        check_no_shrink(new_regress, live)


def test_shrink_guard_tolerates_legacy_live_without_rows():
    live = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2026.parquet", "sha256": "t", "bytes": 9}]}]}  # no "rows"
    new = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2026.parquet", "rows": 1, "sha256": "s", "bytes": 1, "asset": "a"}]}]}
    check_no_shrink(new, live)  # must not raise


def test_check_cas_passes_when_both_none():
    check_cas(None, {"generated_at": None})


def test_second_publish_skips_existing_assets_and_gcs_old(tmp_path: Path):
    # Day 1 publish
    ohlc, meta, stage = _store(tmp_path, ["2026-07-02"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-06-20T16:00:00Z")  # old uploads
    publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                    schema_version=1, generated_at="gen-1", now=NOW)
    old_assets = set(fake.assets) - {"manifest.json"}

    # Day 2: store grows; re-sync state to match live
    df = pd.read_parquet(ohlc / "ohlc_2026.parquet")
    row = df.iloc[[0]].copy()
    row["date"] = pd.to_datetime(["2026-07-03"])
    row["instrument_key"] = ["INE9"]
    pd.concat([df, row], ignore_index=True).to_parquet(
        ohlc / "ohlc_2026.parquet", compression="zstd", index=False)
    _synced(meta, "gen-1")
    fake.now_iso = "2026-07-05T16:00:00Z"
    publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                    schema_version=1, generated_at="gen-2", now=NOW)

    assert_release_consistent(fake)
    # Old day-1 asset was uploaded >7 days ago and is unreferenced -> GC'd.
    assert not (old_assets & set(fake.assets))


def test_gc_spares_young_and_protected_assets(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T15:00:00Z")  # 1h old
    publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                    schema_version=1, generated_at="gen-1", now=NOW)
    fake.seed("stray.parquet", b"stray", created_at="2026-07-05T15:30:00Z")  # young stray
    _synced(meta, "gen-1")
    publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                    schema_version=1, generated_at="gen-2", now=NOW)
    assert "stray.parquet" in fake.assets  # younger than grace -> spared
    assert "manifest.json" in fake.assets


def test_post_publish_verify_detects_manifest_tamper(tmp_path: Path, monkeypatch):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False)

    real_upload = fake.upload

    def tampering_upload(path: Path, *, clobber: bool = False) -> None:
        real_upload(path, clobber=clobber)
        if path.name == "manifest.json":  # simulate a racing writer post-flip
            fake.assets["manifest.json"] = json.dumps({"generated_at": "evil",
                                                       "latest_trading_date": "2026-07-03",
                                                       "datasets": []}).encode()

    monkeypatch.setattr(fake, "upload", tampering_upload)
    with pytest.raises(UnexpectedFailure, match="verification"):
        publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                        schema_version=1, generated_at="gen-1", now=NOW)


def test_latest_trading_date_raises_on_empty_store(tmp_path: Path):
    with pytest.raises(UnexpectedFailure):
        latest_trading_date(tmp_path)


def test_latest_trading_date_reads_max(tmp_path: Path):
    ohlc, _, _ = _store(tmp_path, ["2026-07-02", "2026-07-03"])
    assert latest_trading_date(ohlc) == date(2026, 7, 3)
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_publish.py -q`
Expected: FAIL — `ImportError` (new names not defined)

- [ ] **Step 3: Implement**

Replace `pipeline/src/pipeline/publish.py` entirely:

```python
"""Publish v2: content-addressed data assets, manifest flipped last, guarded.

Invariant delivered to clients: ANY manifest readable from the release
references only complete, sha-verifiable, still-present assets. Data assets
are immutable (content-addressed, never clobbered); `manifest.json` is the
single mutable pointer and is uploaded strictly last."""
from __future__ import annotations

import json
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.manifest import build_manifest, file_digest, write_json
from pipeline.release import ReleaseClient
from pipeline.sync import SYNCED_STATE

PROTECTED_ASSETS = frozenset({"manifest.json", "last_run_status.json"})
GC_GRACE = timedelta(days=7)


def latest_trading_date(ohlc_dir: Path) -> date:
    latest = date.min
    for p in sorted(ohlc_dir.glob("ohlc_*.parquet")):
        col = pd.to_datetime(pd.read_parquet(p, columns=["date"])["date"])
        if col.empty:
            continue
        latest = max(latest, col.max().date())
    if latest == date.min:
        raise UnexpectedFailure("refusing to publish: store has no dated rows")
    return latest


def check_cas(live: dict[str, Any] | None, synced: dict[str, Any]) -> None:
    live_gen = live.get("generated_at") if live else None
    if live_gen != synced.get("generated_at"):
        raise UnexpectedFailure(
            f"live release changed since sync (live={live_gen!r}, "
            f"synced={synced.get('generated_at')!r}); re-run the pipeline"
        )


def check_no_shrink(new: dict[str, Any], live: dict[str, Any] | None) -> None:
    if live is None:
        return
    new_files = {f["name"]: f for f in new["datasets"][0]["files"]}
    for lf in live["datasets"][0]["files"]:
        nf = new_files.get(lf["name"])
        if nf is None:
            raise UnexpectedFailure(
                f"shrink-guard: {lf['name']} is on the live release but missing locally"
            )
        if "rows" in lf and nf["rows"] < lf["rows"]:
            raise UnexpectedFailure(
                f"shrink-guard: {lf['name']} rows {nf['rows']} < live {lf['rows']}"
            )
    if new["latest_trading_date"] < live["latest_trading_date"]:
        raise UnexpectedFailure("shrink-guard: latest_trading_date would regress")


def _read_live_manifest(client: ReleaseClient, work: Path) -> dict[str, Any] | None:
    try:
        client.download(["manifest.json"], work)
    except ReleaseError:
        # No manifest yet (fresh release) — or transiently unreadable, in which
        # case check_cas will mismatch the synced state and abort. Fail-safe.
        return None
    loaded: dict[str, Any] = json.loads((work / "manifest.json").read_text())
    return loaded


def _verify(client: ReleaseClient, new_manifest: dict[str, Any], work: Path) -> None:
    client.download(["manifest.json"], work)
    live = json.loads((work / "manifest.json").read_text())
    if live != new_manifest:
        raise UnexpectedFailure(
            "post-publish verification failed: live manifest is not the one just published"
        )
    files = [e for ds in new_manifest["datasets"] for e in ds["files"]]
    smallest = min(files, key=lambda e: int(e["bytes"]))
    client.download([smallest["asset"]], work)
    sha, _ = file_digest(work / smallest["asset"])
    if sha != smallest["sha256"]:
        raise UnexpectedFailure(
            f"post-publish verification failed: {smallest['asset']} sha mismatch"
        )


def _gc(client: ReleaseClient, new_manifest: dict[str, Any], now: datetime) -> None:
    referenced = {e["asset"] for ds in new_manifest["datasets"] for e in ds["files"]}
    for a in client.list_assets():
        if a.name in referenced or a.name in PROTECTED_ASSETS:
            continue
        created = datetime.fromisoformat(a.created_at.replace("Z", "+00:00"))
        if now - created < GC_GRACE:
            continue
        try:
            client.delete_asset(a.name)
        except ReleaseError as e:  # GC must never fail a good publish
            print(f"gc: could not delete {a.name}: {e}", file=sys.stderr)


def publish_dataset(
    *,
    ohlc_dir: Path,
    meta_dir: Path,
    stage_dir: Path,
    client: ReleaseClient,
    schema_version: int,
    generated_at: str,
    now: datetime,
) -> None:
    data_files = sorted(ohlc_dir.glob("ohlc_*.parquet"))
    if not data_files:
        raise UnexpectedFailure("refusing to publish: no data files (empty store)")
    new_manifest = build_manifest(
        ohlc_dir, schema_version=schema_version,
        latest_trading_date=latest_trading_date(ohlc_dir), generated_at=generated_at,
    )

    if not client.exists():
        client.create()

    stage_dir.mkdir(parents=True, exist_ok=True)
    live = _read_live_manifest(client, stage_dir / "_live")

    synced_path = meta_dir / SYNCED_STATE
    if not synced_path.exists():
        raise UnexpectedFailure("no synced state found: run sync before publish")
    synced: dict[str, Any] = json.loads(synced_path.read_text())

    check_cas(live, synced)
    check_no_shrink(new_manifest, live)

    # Upload new content-addressed data assets (immutable: no clobber).
    existing = {a.name for a in client.list_assets()}
    by_name = {p.name: p for p in data_files}
    for entry in new_manifest["datasets"][0]["files"]:
        if entry["asset"] in existing:
            continue
        staged = stage_dir / entry["asset"]
        shutil.copyfile(by_name[entry["name"]], staged)
        client.upload(staged)

    status_path = meta_dir / "last_run_status.json"
    if status_path.exists():
        client.upload(status_path, clobber=True)

    manifest_path = meta_dir / "manifest.json"
    write_json(new_manifest, manifest_path)
    client.upload(manifest_path, clobber=True)  # THE FLIP — strictly last

    _verify(client, new_manifest, stage_dir / "_verify")
    _gc(client, new_manifest, now)
    write_json(new_manifest, synced_path)  # our publish is now the synced baseline
```

In `pipeline/src/pipeline/cli.py`:
- Delete `_latest_trading_date` and the old `cmd_publish`.
- `cmd_check_freshness` currently takes `runner: publish.Runner` — change its annotation to a local alias defined in `cli.py`: `Runner = Callable[[list[str]], int]` (add `from collections.abc import Callable`) and add a module-level

```python
def _plain_runner(cmd: list[str]) -> int:
    import subprocess
    return subprocess.run(cmd, check=False).returncode
```

and use `_plain_runner` where `publish.subprocess_runner` was passed (the `check-freshness` branch only). Remove `from pipeline import publish`-era imports accordingly; import `from pipeline.publish import publish_dataset` and `from pipeline.release import GhReleaseClient`.
- Replace the trailing publish block of `main()` with:

```python
    # publish
    client = GhReleaseClient(repo=config.GITHUB_REPO, tag=config.RELEASE_TAG)
    try:
        publish_dataset(
            ohlc_dir=config.OHLC_DIR, meta_dir=config.META_DIR,
            stage_dir=config.DATA_DIR / "stage", client=client,
            schema_version=config.SCHEMA_VERSION,
            generated_at=datetime.now(UTC).isoformat(),
            now=datetime.now(UTC),
        )
    except (ReleaseError, UnexpectedFailure) as e:
        print(f"publish failed: {e}", file=sys.stderr)
        return 1
    return 0
```

In `pipeline/tests/test_cli.py`:
- Delete `test_cmd_publish_writes_manifest_and_uploads` (superseded by `test_publish.py`).
- Update `test_main_publish_returns_1_on_failure` / `test_main_publish_returns_0_on_success` to monkeypatch `cli.publish_dataset` instead of `cli.cmd_publish`:

```python
def test_main_publish_returns_1_on_failure(monkeypatch):
    def _boom(**_kw):
        from pipeline.errors import UnexpectedFailure
        raise UnexpectedFailure("no data")
    monkeypatch.setattr(cli, "publish_dataset", _boom)
    assert cli.main(["publish"]) == 1


def test_main_publish_returns_0_on_success(monkeypatch):
    calls = {}
    monkeypatch.setattr(cli, "publish_dataset", lambda **kw: calls.update(kw))
    assert cli.main(["publish"]) == 0
    assert calls["schema_version"] == config.SCHEMA_VERSION
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_publish.py tests/test_cli.py -q`
Expected: all pass

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/publish.py src/pipeline/cli.py tests/test_publish.py tests/test_cli.py
git commit -m "feat(g0/task-5): publish v2 — content-addressed flip, shrink/CAS guards, verify, aged GC"
```

---

### Task 6: Chaos suite — the G0 exit criteria

**Files:**
- Create: `pipeline/tests/test_chaos.py`

**Interfaces:**
- Consumes: everything from Tasks 1–5 (`publish_dataset`, `sync_store`, `FakeReleaseClient`, `assert_release_consistent`). Produces nothing new — this task proves the invariant.

- [ ] **Step 1: Write the tests (they should pass if Tasks 1–5 are correct — any failure here is a real bug to fix in the earlier modules, not in the test)**

Create `pipeline/tests/test_chaos.py`:

```python
"""G0 exit criteria: no interruption or race can lose or tear published data.

Strategy: drive real publish/sync against FakeReleaseClient, injecting a hard
failure after EVERY possible client operation, and assert the release-
consistency invariant after each crash."""
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from pipeline import config
from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.manifest import write_json
from pipeline.publish import publish_dataset
from pipeline.sync import SYNCED_STATE, sync_store
from tests.fakes import FakeReleaseClient, assert_release_consistent

NOW = datetime(2026, 7, 5, 16, 0, tzinfo=UTC)


def _write_store(ohlc: Path, days: list[str]) -> None:
    ohlc.mkdir(parents=True, exist_ok=True)
    rows = {c: ["x"] * len(days) for c in config.CANON_COLUMNS}
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(days)
    df["instrument_key"] = [f"INE{i}" for i in range(len(days))]
    df.to_parquet(ohlc / "ohlc_2026.parquet", compression="zstd", index=False)


def _published_fixture(tmp_path: Path) -> tuple[FakeReleaseClient, Path, Path, Path]:
    """A release with one good day published, synced state in agreement."""
    ohlc, meta, stage = tmp_path / "ohlc", tmp_path / "meta", tmp_path / "stage"
    meta.mkdir(parents=True, exist_ok=True)
    _write_store(ohlc, ["2026-07-02"])
    write_json({"generated_at": None}, meta / SYNCED_STATE)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T15:00:00Z")
    publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                    schema_version=1, generated_at="gen-1", now=NOW)
    assert_release_consistent(fake)
    return fake, ohlc, meta, stage


def test_publish_killed_after_every_op_never_tears_the_release(tmp_path: Path):
    baseline, *_ = _published_fixture(tmp_path)
    baseline_snapshot = dict(baseline.assets)

    k = 0
    while True:
        k += 1
        fake, ohlc, meta, stage = _published_fixture(tmp_path / f"run{k}")
        _write_store(ohlc, ["2026-07-02", "2026-07-03"])  # day-2 grows the store
        fake.ops = 0
        fake.fail_after = k
        try:
            publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                            schema_version=1, generated_at="gen-2", now=NOW)
            break  # k exceeded total ops -> publish completed
        except (ReleaseError, UnexpectedFailure):
            # However far we got, whatever manifest is live references only
            # complete, sha-correct assets.
            assert_release_consistent(fake)
    assert_release_consistent(fake)
    assert k > 3  # sanity: we actually exercised multiple kill points
    assert baseline_snapshot  # silence unused warning; baseline stays valid


def test_interrupted_publish_leaves_old_manifest_serving_old_data(tmp_path: Path):
    fake, ohlc, meta, stage = _published_fixture(tmp_path)
    old_manifest = json.loads(fake.assets["manifest.json"].decode())
    _write_store(ohlc, ["2026-07-02", "2026-07-03"])
    fake.ops = 0
    fake.fail_after = 4  # dies before reaching the manifest flip
    with pytest.raises((ReleaseError, UnexpectedFailure)):
        publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                        schema_version=1, generated_at="gen-2", now=NOW)
    # The pointer never flipped: clients still read the old, fully-valid set.
    assert json.loads(fake.assets["manifest.json"].decode()) == old_manifest
    assert_release_consistent(fake)


def test_failed_sync_then_publish_cannot_wipe_history(tmp_path: Path):
    """The exact P0-1 scenario, end to end."""
    fake, ohlc, meta, stage = _published_fixture(tmp_path)

    # Fresh runner: empty local store, sync fails transiently.
    runner2 = tmp_path / "runner2"
    ohlc2, meta2, stage2 = runner2 / "ohlc", runner2 / "meta", runner2 / "stage"
    fake.ops = 0
    fake.fail_after = 1  # exists() ok, manifest download dies
    with pytest.raises(ReleaseError):
        sync_store(fake, ohlc_dir=ohlc2, meta_dir=meta2, work_dir=runner2 / "work")
    fake.fail_after = None

    # Even if an operator force-runs publish afterwards, guards refuse:
    # (a) empty store -> refuse; (b) one-day store -> no synced state / CAS / shrink.
    with pytest.raises(UnexpectedFailure):
        publish_dataset(ohlc_dir=ohlc2, meta_dir=meta2, stage_dir=stage2, client=fake,
                        schema_version=1, generated_at="gen-evil", now=NOW)
    _write_store(ohlc2, ["2026-07-05"])  # a lone new day, no history
    with pytest.raises(UnexpectedFailure):
        publish_dataset(ohlc_dir=ohlc2, meta_dir=meta2, stage_dir=stage2, client=fake,
                        schema_version=1, generated_at="gen-evil", now=NOW)
    assert_release_consistent(fake)  # history intact throughout


def test_concurrent_publisher_is_detected_by_cas(tmp_path: Path):
    fake, ohlc, meta, stage = _published_fixture(tmp_path)
    _write_store(ohlc, ["2026-07-02", "2026-07-03"])

    # Simulate another publisher flipping the manifest after our sync:
    live = json.loads(fake.assets["manifest.json"].decode())
    live["generated_at"] = "someone-else"
    fake.assets["manifest.json"] = json.dumps(live).encode()

    with pytest.raises(UnexpectedFailure, match="changed since sync"):
        publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                        schema_version=1, generated_at="gen-2", now=NOW)
    assert_release_consistent(fake)
```

- [ ] **Step 2: Run the chaos suite**

Run: `python -m pytest tests/test_chaos.py -q -x`
Expected: 4 passed. If any fails, the bug is in `publish.py`/`sync.py` ordering — fix there (the invariant is the spec), re-run.

- [ ] **Step 3: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add tests/test_chaos.py
git commit -m "test(g0/task-6): chaos suite — kill-at-every-op, sync-wipe, CAS race cannot lose or tear data"
```

---

### Task 7: Workflow gating, hygiene, docs

**Files:**
- Modify: `.github/workflows/data-daily.yml`
- Modify: `.gitignore`
- Modify: `RUNBOOK.md`
- Modify: `pipeline/README.md` (only if it mentions jsDelivr/mirrors — make distribution wording honest: "GitHub Releases CDN")

**Interfaces:**
- Consumes: CLI exit codes from Tasks 4–5 (`sync` exits 1 on failure; `publish` exits 1 on guard/verify failure); `last_run_status.json` written by the `daily` command (existing behavior).
- Produces: publish step runs only when `last_run_status.json.status == "success"`; dispatch date flows via `env:`.

- [ ] **Step 1: Rewrite the workflow steps**

In `.github/workflows/data-daily.yml`, replace the three steps `Sync baseline…`, `Ingest…`, `Publish…` with:

```yaml
      - name: Sync baseline from the data-latest release (fail-closed)
        run: python -m pipeline sync
      - name: Ingest the trading day (today, or the dispatch date)
        env:
          DISPATCH_DATE: ${{ github.event.inputs.date }}
        run: |
          if [ -n "$DISPATCH_DATE" ]; then
            python -m pipeline daily --date "$DISPATCH_DATE"
          else
            python -m pipeline daily
          fi
      - name: Decide whether to publish
        id: decide
        run: |
          status=$(jq -r '.status // "missing"' data/meta/last_run_status.json)
          echo "status=$status" >> "$GITHUB_OUTPUT"
          echo "Ingest status: $status"
      - name: Publish the updated dataset
        if: steps.decide.outputs.status == 'success'
        run: python -m pipeline publish
```

(The `Alert on failure` step stays as-is. The `env:` indirection removes the `${{ }}`-in-bash injection pattern.)

- [ ] **Step 2: Update `.gitignore`**

Append:

```
pipeline/data/stage/
pipeline/data/meta/synced_manifest.json
```

- [ ] **Step 3: Update `RUNBOOK.md`**

Add a section (adapt to the file's existing style):

```markdown
## Sync / publish semantics (G0)

- `python -m pipeline sync` is FAIL-CLOSED: any failure other than "release
  does not exist" exits 1 and stops the run. Never bypass it — publishing from
  an unsynced store is blocked by the guards below anyway.
- Data assets are content-addressed (`ohlc_2026.<sha8>.parquet`) and immutable;
  `manifest.json` is the only mutable asset and is flipped last. Unreferenced
  assets are garbage-collected 7 days after upload.
- `publish` refuses to: shrink coverage (fewer rows/years, older
  latest_trading_date), publish over a release that changed since sync
  (re-run the pipeline), or leave an unverified manifest (it re-downloads and
  checks itself after the flip).
- Recovery from a failed publish: nothing to clean up — the old manifest is
  still live and consistent; just re-run sync → daily → publish.
```

- [ ] **Step 4: Verify + commit**

```bash
cd ~/Desktop/projects/guardian-universe/pipeline && python -m pytest -q && mypy && ruff check .
cd ~/Desktop/projects/guardian-universe
git add .github/workflows/data-daily.yml .gitignore RUNBOOK.md pipeline/README.md
git commit -m "ci(g0/task-7): gate publish on ingest success; env-safe dispatch input; RUNBOOK for G0 semantics"
```

---

## Post-plan validation (manual, once merged)

1. `workflow_dispatch` `data-daily` with no date on a trading evening → watch: sync verifies, ingest runs, publish flips; release shows content-addressed assets + fresh `manifest.json`.
2. Immediately re-dispatch → `skipped_idempotent` → publish step skipped (decide=not success).
3. Confirm the legacy plain-named `ohlc_2026.parquet` asset disappears from the release ~7 days later (GC) while `manifest.json` clients keep working throughout.

## Self-review notes

- **Spec coverage:** §2.1 content-addressing+flip (T2/T5), §2.2 fail-closed sync (T4), §2.3 three guards (T5), §2.4 delta upload — *deliberately deferred to G1* (delta listing requires manifest v2; spec §2.4 notes this), §2.5 snapshots — *deferred to G3 per roadmap*, §4 atomic writes (T3), publish-only-on-success (T7), empty-store guard (T5), injection hygiene (T7). Chaos exit criteria (T6).
- **Type consistency:** `publish_dataset(ohlc_dir, meta_dir, stage_dir, client, schema_version, generated_at, now)` and `sync_store(client, ohlc_dir=…, meta_dir=…, work_dir=…)` used identically across T4/T5/T6. `FakeReleaseClient.seed/fail_after/ops` used identically across T1/T4/T5/T6.
- **Known judgment call:** `_read_live_manifest` treats a download error as `None`; CAS then mismatches any non-None synced state and aborts — fail-safe, documented in code.
