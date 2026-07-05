# G3 — The Real Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Do the two things the roadmap reserved for G3 — an in-process read cache so a 300-trading-day sequential backfill doesn't re-read a growing parquet file dozens of times per day, and a GitHub-native disaster-recovery mechanism (monthly immutable snapshots + a tested restore path) — then execute the one real backfill this pipeline should ever need, against the final G1b/G2 schema.

**Architecture:** An opt-in `ReadCache` (a thin dict keyed by `(base, year, prefix)`) is threaded through `store.py`'s read functions and `daily_update.run_daily` as an optional keyword defaulting to `None` — every existing call site and all 393 tests keep today's always-read-fresh behavior unchanged; only `backfill.backfill()`'s loop (and, incidentally, the CLI's nightly catch-up-window loop) opt in by constructing one cache and reusing it across the run, invalidating an entry the instant `append_keyed` writes that year back to disk. Snapshots reuse the existing `ReleaseClient` seam unchanged — a snapshot is just a second, immutable-by-convention release tag (`data-snapshot-YYYYMM`) that `create_snapshot` populates by downloading the verified `data-latest` assets and re-uploading them under the new tag; restore is a new, independent two-phase verify-then-materialize module (deliberately NOT a refactor of `sync.py`, to avoid touching the pipeline's most load-bearing, heavily-tested file) that can point at any tag and any target directory — a scratch directory for a drill, the real `data/` tree for an actual recovery.

**Tech Stack:** unchanged (Python 3.11+, pandas, pandera, pytest filterwarnings=error, mypy --strict, ruff; `gh` via ReleaseClient; pinned `.venv`).

**Spec:** roadmap line "G3 — 300 trading days, all series + indices, against final schema; monthly snapshot + DR drill" (exit criteria: "52w/EMA-200 computable for every active instrument; restore-from-snapshot rehearsed once"). Design spec §2.5 (GitHub-native DR: monthly snapshot tag, keep 6, manifest history).

**Branch:** `feat/g3-backfill` off `main` (f916d55, G2 merged + the holidays-refresh GH_REPO hotfix).

**Confirmed dead code (verified this session, zero production callers, grep-checked against `src/pipeline/` before writing this plan):** `validate.check_rowcount` (superseded by `check_rowcount_by_series`, G1b task 4) and `store.day_symbol_count` (superseded by `day_series_counts`, same task). Both were explicitly flagged for G3 cleanup in the G2 final-review ledger.

## Global Constraints

1. Working dir `pipeline/`; full gate before every commit: `python -m pytest -q && mypy && ruff check .` (warning-free, pinned `.venv`; report the true suite count from the pytest summary).
2. No live network in tests; `gh`/NSE faked (`FakeReleaseClient`, stub fetchers) — the ONE exception is the controller-run Task 6/7/8 steps at the end of this plan, which are explicitly live and NOT subagent-dispatched.
3. `ReadCache` is opt-in everywhere: every store/daily_update signature change adds a `cache: ReadCache | None = None` keyword with the default preserving byte-identical behavior. No existing test may need modification to keep passing.
4. No dataset/source name hardcoded in shared code; registry/spec flow only.
5. Manifest v2 client contract untouched. Snapshot/restore add no new manifest fields — a snapshot's manifest is a byte-identical copy of the `data-latest` manifest at the moment of the snapshot.
6. Tests never write outside tmp dirs.
7. Commit after every task with the exact message given.

---

### Task 1: `ReadCache` — opt-in in-process year-file cache

**Files:**
- Modify: `pipeline/src/pipeline/store.py`
- Modify: `pipeline/tests/test_store.py`

**Interfaces:**
- Produces:

```python
class ReadCache:
    """Opt-in in-process cache for _read_year, keyed by (base, year, prefix).

    Every store read function accepts `cache: ReadCache | None = None`;
    `None` (the default everywhere) preserves today's always-read-fresh
    behavior exactly. A caller that expects to make many sequential reads
    against the same growing file within one process (the 300-day backfill;
    the nightly catch-up window) constructs ONE ReadCache and threads it
    through every call, so each year-file is read from disk once per
    *version* of that file rather than once per read call. `append_keyed`
    invalidates the entry for whatever (base, year, prefix) it just wrote,
    so a cached reader can never observe stale data within the same run."""

    def __init__(self) -> None: ...
    def get(self, base: Path, year: int, prefix: str) -> pd.DataFrame | None: ...
    def put(self, base: Path, year: int, prefix: str, df: pd.DataFrame) -> None: ...
    def invalidate(self, base: Path, year: int, prefix: str) -> None: ...
```

  Key is `(str(base), year, prefix)` (path stringified for hashability/equality across `Path` instances referring to the same location).
- `_read_year(base, year, prefix="ohlc", *, columns=None, cache: ReadCache | None = None)` — `cache is None` → today's behavior (always `pd.read_parquet`/empty-frame construction, never touches any cache). `cache` given → check `cache.get(...)` first; on miss, read from disk as today AND `cache.put(...)` before returning.
- `append_keyed(df, base, *, prefix, key_cols=(...), cache: ReadCache | None = None)` — same read-path change for its own internal `_read_year` call, PLUS after the atomic `tmp.replace(target)`, if `cache is not None`: `cache.invalidate(base, year, prefix)` (the on-disk file just changed; the cache must not serve the pre-write version to a later call in the same run). `append_day` gains the same passthrough keyword.
- `has_day`, `day_series_counts`, `read_trailing_window` each gain `cache: ReadCache | None = None`, passed straight through to their internal `_read_year` call(s).

- [ ] **Step 1: Write the failing tests**

Append to `pipeline/tests/test_store.py`:

```python
def test_read_cache_serves_repeated_reads_without_touching_disk(tmp_path, monkeypatch):
    import pandas as pd
    from datetime import date
    from pipeline import config, store

    df = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
    df["date"] = pd.to_datetime(["2026-07-03"])
    df["instrument_key"] = ["INE1"]
    store.append_day(df, tmp_path)

    cache = store.ReadCache()
    assert store.has_day(tmp_path, date(2026, 7, 3), cache=cache)  # primes the cache

    calls = {"n": 0}
    real_read_parquet = pd.read_parquet

    def counting_read_parquet(*a, **kw):
        calls["n"] += 1
        return real_read_parquet(*a, **kw)

    monkeypatch.setattr(pd, "read_parquet", counting_read_parquet)
    assert store.has_day(tmp_path, date(2026, 7, 3), cache=cache)
    assert store.day_series_counts(tmp_path, date(2026, 7, 3), cache=cache)
    assert calls["n"] == 0  # both served from cache, zero disk reads


def test_read_cache_is_invalidated_by_append(tmp_path):
    import pandas as pd
    from datetime import date
    from pipeline import config, store

    def frame(day: str, key: str) -> pd.DataFrame:
        df = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
        df["date"] = pd.to_datetime([day])
        df["instrument_key"] = [key]
        return df

    cache = store.ReadCache()
    store.append_day(frame("2026-07-03", "INE1"), tmp_path, cache=cache)
    # Prime the cache with the one-row state, then append a second row under cache.
    assert store.has_day(tmp_path, date(2026, 7, 3), cache=cache)
    store.append_day(frame("2026-07-03", "INE2"), tmp_path, cache=cache)
    # A read through the SAME cache after the second append must see both rows,
    # not the stale one-row snapshot -- proving invalidation, not just a cold cache.
    window = store.read_trailing_window(tmp_path, date(2026, 7, 3), 5, cache=cache)
    assert sorted(window["instrument_key"]) == ["INE1", "INE2"]


def test_read_cache_none_default_is_unchanged_behavior(tmp_path):
    # No cache argument anywhere -- must behave exactly as before this task.
    import pandas as pd
    from datetime import date
    from pipeline import config, store

    df = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
    df["date"] = pd.to_datetime(["2026-07-03"])
    df["instrument_key"] = ["INE1"]
    store.append_day(df, tmp_path)
    assert store.has_day(tmp_path, date(2026, 7, 3))
    assert store.day_series_counts(tmp_path, date(2026, 7, 3))
```

(Remove the dead `if False` line above before implementing — it's a placeholder marker only; write a clean version.)

- [ ] **Step 2: Run to verify failure**

Run: `cd ~/Desktop/projects/guardian-universe/pipeline && .venv/bin/python -m pytest tests/test_store.py -q`
Expected: FAIL — `AttributeError: module 'pipeline.store' has no attribute 'ReadCache'`

- [ ] **Step 3: Implement**

In `store.py`, add near the top (after imports):

```python
class ReadCache:
    """Opt-in in-process cache for _read_year -- see module docstring addendum."""

    def __init__(self) -> None:
        self._data: dict[tuple[str, int, str], pd.DataFrame] = {}

    def get(self, base: Path, year: int, prefix: str) -> pd.DataFrame | None:
        return self._data.get((str(base), year, prefix))

    def put(self, base: Path, year: int, prefix: str, df: pd.DataFrame) -> None:
        self._data[(str(base), year, prefix)] = df

    def invalidate(self, base: Path, year: int, prefix: str) -> None:
        self._data.pop((str(base), year, prefix), None)
```

Update `_read_year`:

```python
def _read_year(
    base: Path, year: int, prefix: str = "ohlc", *,
    columns: list[str] | None = None, cache: ReadCache | None = None,
) -> pd.DataFrame:
    if cache is not None:
        cached = cache.get(base, year, prefix)
        if cached is not None:
            return cached
    p = config.dataset_path(year, base, prefix=prefix)
    df = pd.read_parquet(p) if p.exists() else pd.DataFrame(
        columns=columns if columns is not None else config.CANON_COLUMNS
    )
    if cache is not None:
        cache.put(base, year, prefix, df)
    return df
```

Update `append_keyed`'s signature to add `cache: ReadCache | None = None`, pass `cache=cache` into its internal `_read_year(...)` call, and after `tmp.replace(target)` add:

```python
        if cache is not None:
            cache.invalidate(base, int(year), prefix)
```

Update `append_day`, `has_day`, `day_series_counts`, `read_trailing_window` to accept and forward `cache: ReadCache | None = None` to their internal `_read_year`/`append_keyed` calls.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_store.py -q` → all pass (existing tests unchanged, pass no `cache` argument).

- [ ] **Step 5: Full gate + commit**

```bash
.venv/bin/python -m pytest -q && .venv/bin/python -m mypy && .venv/bin/python -m ruff check .
git add src/pipeline/store.py tests/test_store.py
git commit -m "feat(g3/task-1): opt-in ReadCache for store year-file reads (zero behavior change by default)"
```

---

### Task 2: Wire the cache into `run_daily` and `backfill`

**Files:**
- Modify: `pipeline/src/pipeline/daily_update.py`
- Modify: `pipeline/src/pipeline/backfill.py`
- Modify: `pipeline/src/pipeline/cli.py` (catch-up window loop)
- Modify: `pipeline/tests/test_daily_update.py`, `pipeline/tests/test_backfill.py`, `pipeline/tests/test_cli.py`

**Interfaces:**
- `run_daily(spec, target, *, fetcher, holidays, special_sessions=None, is_target_day=True, cache: store.ReadCache | None = None)` — every one of its internal `store.has_day`/`store.day_series_counts`/`store.append_day`/`store.write_delta`-adjacent calls that reads/writes the year file threads `cache=cache` through (note: `write_delta` is delta-file I/O, not year-file I/O — it does NOT take a cache; only `_read_year`-backed calls do). Default `None` preserves current behavior for every existing caller/test.
- `backfill.backfill(spec, end, n, *, fetcher, holidays, special_sessions=None, sleep=time.sleep, delay_s=1.0)` — constructs exactly ONE `store.ReadCache()` at the top of the function and passes it to every `run_daily(...)` call in its loop. This is the actual perf payoff: across a 300-iteration backfill, each year's file is read from disk once per version instead of up to ~10× per day-check.
- `cli.py`'s per-spec catch-up-window loop (the `for d in cal.trading_days_back(target, config.CATCHUP_WINDOW_DAYS, ...)` loop from G2 Task 4) similarly constructs one `store.ReadCache()` per spec per `daily` invocation and threads it through each `run_daily` call in that spec's window — this is the same win applied to the nightly cron's 7-day window, not just the one-time backfill.

- [ ] **Step 1: Write the failing tests**

Append to `pipeline/tests/test_backfill.py`:

```python
def test_backfill_reuses_one_cache_across_the_whole_run(tmp_path, monkeypatch):
    from datetime import date
    from pipeline import store
    from pipeline.backfill import backfill
    from pipeline.datasets import EQUITIES
    import dataclasses

    seen_caches: list[object] = []
    real_read_year = store._read_year

    def spying_read_year(base, year, prefix="ohlc", *, columns=None, cache=None):
        seen_caches.append(cache)
        return real_read_year(base, year, prefix, columns=columns, cache=cache)

    monkeypatch.setattr(store, "_read_year", spying_read_year)

    class _StubFetcher:
        def fetch_raw(self, d):
            from pipeline.fetch import FetchResult
            import pandas as pd
            df = pd.DataFrame({
                "TradDt": [d.isoformat()], "ISIN": ["INE1"], "TckrSymb": ["A"],
                "SctySrs": ["EQ"], "FinInstrmTp": ["STK"], "SsnId": ["F1"],
                "OpnPric": [1.0], "HghPric": [1.0], "LwPric": [1.0], "ClsPric": [1.0],
                "PrvsClsgPric": [1.0], "TtlTradgVol": [1], "TtlTrfVal": [1.0],
                "TtlNbOfTxsExctd": [1],
            })
            return FetchResult(df, "nse-udiff")

    spec = dataclasses.replace(EQUITIES, base_dir=tmp_path, abs_rowcount_range=(0, 10**9))
    backfill(spec, date(2026, 7, 3), 2, fetcher=_StubFetcher(), holidays=set(), sleep=lambda s: None)

    non_none = [c for c in seen_caches if c is not None]
    assert non_none, "expected at least one cache-bearing _read_year call"
    assert len({id(c) for c in non_none}) == 1  # every call shared the SAME cache instance
```

Append to `pipeline/tests/test_cli.py`, named `test_daily_catchup_window_shares_one_cache_across_the_window`. Before writing it, read the neighboring G2-task-4 catch-up-window test in this same file (the one that seeds a multi-day tmp-scoped registry and a StubFetcher recording requested dates, then runs `cli.main(["daily", "--date", ...])`) and reuse its EXACT registry/fetcher/monkeypatch setup verbatim — do not invent a new fixture shape. The only addition on top of that existing setup: monkeypatch `pipeline.store._read_year` with a spy (identical pattern to the `spying_read_year` wrapper in the Task 2 `test_backfill.py` test above — same wrapper, same call-through to the real function) that appends every `cache` argument it receives to a list; after `cli.main([...])` returns, assert (a) at least one recorded call had `cache is not None`, and (b) every non-`None` recorded cache for that spec's window shares one `id(...)` — i.e. `len({id(c) for c in seen if c is not None}) == 1`. This proves the CLI's per-spec loop constructs exactly one `ReadCache` and reuses it across every day in the window, not a fresh one per day.

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_backfill.py tests/test_cli.py -q`
Expected: FAIL (either `TypeError: unexpected keyword argument 'cache'` or the shared-instance assertion failing because no cache is threaded yet).

- [ ] **Step 3: Implement**

In `daily_update.py`, add `cache: "store.ReadCache | None" = None` to `run_daily`'s signature (import `store` already present), thread it into every `store.has_day(...)`, `store.day_series_counts(...)`, `store.append_day(...)` call inside the function (leave `store.write_delta(...)` untouched — it has no `cache` parameter). In `_trailing_series_counts` (or wherever the per-series trailing loop lives), thread `cache` into its internal `store.has_day`/`store.day_series_counts` calls too.

In `backfill.py`:

```python
from pipeline import store
...
def backfill(spec, end, n, *, fetcher, holidays, special_sessions=None, sleep=time.sleep, delay_s=1.0):
    dates = cal.trading_days_back(end, n, holidays, special_sessions)
    cache = store.ReadCache()
    results: list[RunStatus] = []
    for i, d in enumerate(dates):
        results.append(
            run_daily(spec, d, fetcher=fetcher, holidays=holidays,
                     special_sessions=special_sessions,
                     is_target_day=(d == dates[-1]), cache=cache)
        )
        if i < len(dates) - 1:
            sleep(delay_s)
    return results
```

In `cli.py`, locate the per-spec catch-up-window loop from G2 Task 4 and construct `cache = store.ReadCache()` once before the `for d in cal.trading_days_back(...)` loop, passing `cache=cache` into each `run_daily(...)` call inside it.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest -q` → all pass.

- [ ] **Step 5: Full gate + commit**

```bash
.venv/bin/python -m pytest -q && .venv/bin/python -m mypy && .venv/bin/python -m ruff check .
git add src/pipeline/daily_update.py src/pipeline/backfill.py src/pipeline/cli.py tests/test_backfill.py tests/test_cli.py
git commit -m "feat(g3/task-2): thread ReadCache through run_daily/backfill/catch-up window — one disk read per year-file version"
```

---

### Task 3: Dead-code cleanup — `check_rowcount` and `day_symbol_count`

**Files:**
- Modify: `pipeline/src/pipeline/validate.py`
- Modify: `pipeline/src/pipeline/store.py`
- Modify: `pipeline/tests/test_validate.py`, `pipeline/tests/test_store.py`, `pipeline/tests/test_daily_update.py`

**Interfaces:**
- Removes: `validate.check_rowcount` (and its now-orphaned direct unit tests — `check_rowcount_by_series` keeps its own full test coverage, untouched). Removes: `store.day_symbol_count` (and its direct unit tests in `test_store.py`).
- `test_daily_update.py` currently asserts `store.day_symbol_count(tmp_path, target) == N` in several completeness-gate tests (grep-confirmed at lines referencing "topped up to full" etc.) — these become `store.day_series_counts(tmp_path, target)` with an equivalent total-sum assertion (`sum(store.day_series_counts(tmp_path, target).values()) == N`), preserving the exact same behavioral check without the removed helper.

- [ ] **Step 1: Confirm zero production callers (already done for this plan, re-verify at implementation time)**

Run: `grep -rn "day_symbol_count\|check_rowcount(" src/pipeline/ | grep -v "check_rowcount_by_series\|def day_symbol_count\|def check_rowcount"`
Expected: no output (confirms nothing in `src/` calls either function outside its own definition).

- [ ] **Step 2: Remove the dead functions**

Delete `check_rowcount` from `validate.py` (keep `check_rowcount_by_series` and its helpers untouched). Delete `day_symbol_count` from `store.py` (keep `day_series_counts`, `has_day`, `read_trailing_window` untouched).

- [ ] **Step 3: Update dependent tests**

Delete `test_check_rowcount_*` tests from `test_validate.py` that test the removed function specifically (keep every `check_rowcount_by_series` test as-is). Delete `test_day_symbol_count` from `test_store.py`. In `test_daily_update.py`, replace every `store.day_symbol_count(tmp_path, target) == N` assertion with `sum(store.day_series_counts(tmp_path, target).values()) == N` (same check, different helper).

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest -q` → all pass, count decreases by exactly the number of deleted tests (report the before/after counts).

- [ ] **Step 5: Full gate + commit**

```bash
.venv/bin/python -m pytest -q && .venv/bin/python -m mypy && .venv/bin/python -m ruff check .
git add src/pipeline/validate.py src/pipeline/store.py tests/test_validate.py tests/test_store.py tests/test_daily_update.py
git commit -m "refactor(g3/task-3): remove check_rowcount + day_symbol_count — zero production callers, superseded by the per-series equivalents"
```

---

### Task 4: `ReleaseClient.list_releases`/`delete_release` + monthly snapshot mechanism

**Files:**
- Modify: `pipeline/src/pipeline/release.py`
- Modify: `pipeline/tests/fakes.py`
- Create: `pipeline/src/pipeline/snapshot.py`
- Modify: `pipeline/src/pipeline/cli.py` (`snapshot` subcommand)
- Create: `pipeline/tests/test_snapshot.py`
- Modify: `pipeline/tests/test_release.py`
- Create: `.github/workflows/data-snapshot.yml`
- Modify: `RUNBOOK.md`

**Interfaces:**
- `ReleaseClient` protocol gains two methods (implement on `GhReleaseClient` and `FakeReleaseClient`):
  - `list_releases(self) -> list[str]` — all release tag names in the repo (`gh api repos/{repo}/releases --jq '[.[].tag_name]'`).
  - `delete_release(self, tag: str) -> None` — `gh release delete {tag} --repo {repo} --yes` (deletes the release AND its assets).
- `snapshot.SNAPSHOT_TAG_PREFIX = "data-snapshot-"`; `snapshot.tag_for(now: datetime) -> str` → `f"{SNAPSHOT_TAG_PREFIX}{now:%Y%m}"`.
- `snapshot.create_snapshot(source_client: ReleaseClient, dest_client_factory: Callable[[str], ReleaseClient], *, work_dir: Path, now: datetime) -> str` — downloads `manifest.json` from `source_client`, verifies+downloads every asset it references (baseline + deltas, across all datasets — reuse the sha-verify pattern, NOT `sync.py`'s dataset-routing logic, since a snapshot copies bytes verbatim regardless of dataset registry membership: iterate `manifest["datasets"]`, for each `dataset_files(ds) + ds.get("deltas", [])`, verify+download by `asset` name), then `dest = dest_client_factory(tag_for(now))`; if `dest.exists()` → raise `UnexpectedFailure` (a snapshot tag is immutable — never re-create the same month twice; the monthly cadence means this should never fire in practice, but must fail loud, not silently overwrite, if it ever does); else `dest.create()`, upload every downloaded asset (no clobber needed — brand new release) plus the manifest itself; return the tag created.
- `snapshot.prune_snapshots(client_factory: Callable[[str], ReleaseClient], list_client: ReleaseClient, *, keep: int = 6) -> list[str]` — `sorted(t for t in list_client.list_releases() if t.startswith(SNAPSHOT_TAG_PREFIX))`; delete every tag but the newest `keep` (lexical sort on `YYYYMM` is chronological); each delete via `client_factory(tag).delete_release()`; return the list of deleted tags.
- CLI `snapshot` subcommand: `create_snapshot` against the live `data-latest` release, then `prune_snapshots`, printing what was created and what was pruned; exit 1 on any `ReleaseError`/`UnexpectedFailure`.
- `.github/workflows/data-snapshot.yml`: monthly cron (1st, 04:00 UTC) + dispatch; SHA-pinned actions (match the other 6 workflows' pins), `persist-credentials: false`, install via `requirements.lock`, run `python -m pipeline snapshot`, alert-on-failure step (same dedupe pattern as the other alerting workflows).
- RUNBOOK: new "Monthly snapshots (disaster recovery)" section — what a snapshot is, the keep-6 retention, how to list current snapshots (`gh release list --repo SatioO/guardian-universe | grep data-snapshot-`), and a forward-reference to Task 5's restore drill.

- [ ] **Step 1: Write the failing tests**

In `pipeline/tests/test_release.py`, add tests for the two new `GhReleaseClient` methods (mirror the file's existing `RecordingRunner` pattern):

```python
def test_list_releases_parses_tag_names():
    out = '["data-latest", "data-snapshot-202606", "data-snapshot-202607"]'
    r = RecordingRunner([(0, out, "")])
    tags = GhReleaseClient(repo="o/r", tag="t", runner=r).list_releases()
    assert tags == ["data-latest", "data-snapshot-202606", "data-snapshot-202607"]


def test_delete_release_raises_on_failure():
    import pytest
    from pipeline.errors import ReleaseError
    r = RecordingRunner([(1, "", "some error")])
    with pytest.raises(ReleaseError):
        GhReleaseClient(repo="o/r", tag="t", runner=r).delete_release("data-snapshot-202601")
```

In `pipeline/tests/fakes.py`'s `FakeReleaseClient`, extend the test double (it currently models ONE release/tag; snapshot tests need a fake that can model MULTIPLE tags sharing one underlying "repo"). Add a module-level `FakeReleaseRepo` that holds `dict[str, FakeReleaseClient]` keyed by tag, with a `client_for(tag) -> FakeReleaseClient` factory and a `list_releases() -> list[str]`/`delete_release(tag)` pair usable as the "list client". Write this so existing single-tag `FakeReleaseClient` usage across 393 existing tests is completely untouched (this is an additive class, not a modification to `FakeReleaseClient` itself) — verify by running the full suite after this step alone, before writing `snapshot.py`.

Create `pipeline/tests/test_snapshot.py`:

```python
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.errors import UnexpectedFailure
from pipeline.manifest import asset_name
from pipeline.snapshot import create_snapshot, prune_snapshots, tag_for
from tests.fakes import FakeReleaseClient, FakeReleaseRepo, assert_release_consistent


def _seeded_source() -> FakeReleaseClient:
    import hashlib
    data = b"OHLCDATA"
    sha = hashlib.sha256(data).hexdigest()
    manifest = {
        "manifest_version": 2, "generated_at": "g", "latest_trading_date": "2026-07-03",
        "datasets": [{"name": "ohlc", "schema_version": 2, "latest_date": "2026-07-03",
            "baseline": [{"name": "ohlc_2026.parquet", "asset": asset_name("ohlc_2026.parquet", sha),
                          "sha256": sha, "bytes": len(data), "rows": 1}],
            "deltas": []}],
    }
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed(asset_name("ohlc_2026.parquet", sha), data)
    return fake


def test_create_snapshot_copies_manifest_and_assets(tmp_path: Path):
    source = _seeded_source()
    repo = FakeReleaseRepo()
    tag = create_snapshot(source, repo.client_for, work_dir=tmp_path, now=datetime(2026, 7, 6, tzinfo=UTC))
    assert tag == "data-snapshot-202607"
    dest = repo.client_for(tag)
    assert_release_consistent(dest)
    assert json.loads(dest.assets["manifest.json"]) == json.loads(source.assets["manifest.json"])


def test_create_snapshot_refuses_to_recreate_same_month(tmp_path: Path):
    source = _seeded_source()
    repo = FakeReleaseRepo()
    now = datetime(2026, 7, 6, tzinfo=UTC)
    create_snapshot(source, repo.client_for, work_dir=tmp_path, now=now)
    with pytest.raises(UnexpectedFailure, match="already exists"):
        create_snapshot(source, repo.client_for, work_dir=tmp_path / "again", now=now)


def test_prune_snapshots_keeps_newest_n():
    repo = FakeReleaseRepo()
    for ym in ["202601", "202602", "202603", "202604", "202605", "202606", "202607"]:
        repo.client_for(f"data-snapshot-{ym}").seed("manifest.json", b"{}")
    repo.client_for("data-latest").seed("manifest.json", b"{}")  # must never be pruned
    deleted = prune_snapshots(repo.client_for, repo.as_list_client(), keep=6)
    assert deleted == ["data-snapshot-202601"]
    remaining = sorted(repo.tags())
    assert "data-latest" in remaining
    assert "data-snapshot-202601" not in remaining
    assert len([t for t in remaining if t.startswith("data-snapshot-")]) == 6


def test_tag_for_formats_year_month():
    assert tag_for(datetime(2026, 3, 6, tzinfo=UTC)) == "data-snapshot-202603"
```

(`FakeReleaseRepo.client_for`/`as_list_client`/`tags()` are the additive test-double surface from Step 1 above — design them to make these four tests pass naturally.)

Add a CLI test in `test_cli.py`: `snapshot` subcommand parses, calls `create_snapshot`+`prune_snapshots` (monkeypatched), exits 0/1 correctly on success/`ReleaseError`.

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_release.py tests/test_snapshot.py -q`
Expected: FAIL (`ImportError`/`AttributeError` — nothing implemented yet).

- [ ] **Step 3: Implement** per the Interfaces block. `list_releases`/`delete_release` on `GhReleaseClient` mirror the existing methods' error-handling style exactly. `create_snapshot`/`prune_snapshots`/`tag_for` in new `snapshot.py`, importing `manifest.dataset_files`, `manifest.file_digest`, `errors.{ReleaseError,UnexpectedFailure}`. CLI: add `snapshot` to `build_parser()` (no arguments) and a `cmd_snapshot`/dispatch branch in `main()` mirroring the existing subcommand pattern (construct a real `GhReleaseClient` for `data-latest` as source, a factory building `GhReleaseClient(repo=config.GITHUB_REPO, tag=t)` for `dest_client_factory`/list client, call `create_snapshot` then `prune_snapshots`, print results, catch `(ReleaseError, UnexpectedFailure)` → stderr + exit 1).

- [ ] **Step 4: Run to verify pass**, then **Step 5: full gate + workflow YAML parse check + commit**

```bash
.venv/bin/python -m pytest -q && .venv/bin/python -m mypy && .venv/bin/python -m ruff check .
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/data-snapshot.yml')); print('YAML OK')"
git add src/pipeline/release.py src/pipeline/snapshot.py src/pipeline/cli.py tests/fakes.py tests/test_release.py tests/test_snapshot.py tests/test_cli.py .github/workflows/data-snapshot.yml RUNBOOK.md
git commit -m "feat(g3/task-4): monthly immutable snapshot releases (create + keep-6 prune) + workflow"
```

---

### Task 5: DR restore tooling + drill documentation

**Files:**
- Create: `pipeline/src/pipeline/restore.py`
- Modify: `pipeline/src/pipeline/cli.py` (`restore-from-snapshot` subcommand)
- Create: `pipeline/tests/test_restore.py`
- Modify: `RUNBOOK.md`

**Interfaces:**
- `restore.restore_from_tag(client: ReleaseClient, *, target_root: Path, work_dir: Path) -> dict[str, Any]` — downloads+verifies `manifest.json`; for each dataset entry, materializes its `baseline` files (two-phase: verify all, then write all, mirroring `sync.py`'s own crash-safety discipline but deliberately re-implemented here rather than importing `sync.py` internals, to keep this module independently correct and reviewable without coupling to `sync.py`'s dataset-registry routing — a restore target is an arbitrary directory tree, not necessarily today's live `DATASETS` registry) into `target_root / manifest_dataset_name / logical_name` (deltas are NOT restored — same posture as `sync.py`: a restore rebuilds from baselines, deltas are a live-client catch-up mechanism only). Returns the parsed manifest dict for the caller to report on (dataset names, latest dates, row-ish counts from `bytes`/`rows` fields).
- CLI `restore-from-snapshot --tag <tag> [--target <path>]` — `--target` defaults to a fresh subdirectory under a scratch location (e.g. `config.DATA_DIR / "_restore_drill"`), NEVER the live `data/` tree, unless the operator explicitly passes `--target <path to data dir>` — this default-to-scratch behavior is the safety rail that makes a "drill" (rehearsal) safe to run against the real `data-latest`/any snapshot tag without risk of clobbering production data. Prints a summary (datasets restored, latest dates, byte totals) and exits 0/1 on success/failure.
- RUNBOOK: "Disaster recovery drill" section — the exact command to rehearse a restore (`pipeline restore-from-snapshot --tag data-snapshot-YYYYMM`, scratch target by default), what a successful drill output looks like, and the SEPARATE, explicitly-flagged real-recovery procedure (pointing `--target` at the actual `data/` directory, only ever done by a human, only after confirming the live release is actually unusable).

- [ ] **Step 1: Write the failing tests**

Create `pipeline/tests/test_restore.py`:

```python
import hashlib
import json
from pathlib import Path

import pytest

from pipeline.errors import UnexpectedFailure
from pipeline.manifest import asset_name
from pipeline.restore import restore_from_tag
from tests.fakes import FakeReleaseClient


def _seeded_snapshot() -> FakeReleaseClient:
    data = b"OHLCDATA"
    sha = hashlib.sha256(data).hexdigest()
    manifest = {
        "manifest_version": 2, "generated_at": "g", "latest_trading_date": "2026-07-03",
        "datasets": [{"name": "ohlc", "schema_version": 2, "latest_date": "2026-07-03",
            "baseline": [{"name": "ohlc_2026.parquet", "asset": asset_name("ohlc_2026.parquet", sha),
                          "sha256": sha, "bytes": len(data), "rows": 1}],
            "deltas": [{"date": "2026-07-03", "name": "ohlc_2026-07-03.parquet",
                       "asset": "delta_ohlc_2026-07-03." + sha[:8] + ".parquet",
                       "sha256": "irrelevant", "bytes": 1}]}],
    }
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed(asset_name("ohlc_2026.parquet", sha), data)
    return fake


def test_restore_materializes_baseline_under_target_root(tmp_path: Path):
    client = _seeded_snapshot()
    result = restore_from_tag(client, target_root=tmp_path / "restored", work_dir=tmp_path / "work")
    restored_file = tmp_path / "restored" / "ohlc" / "ohlc_2026.parquet"
    assert restored_file.read_bytes() == b"OHLCDATA"
    assert result["datasets"][0]["name"] == "ohlc"


def test_restore_does_not_download_deltas(tmp_path: Path):
    client = _seeded_snapshot()
    restore_from_tag(client, target_root=tmp_path / "restored", work_dir=tmp_path / "work")
    assert not (tmp_path / "restored" / "ohlc" / "ohlc_2026-07-03.parquet").exists()


def test_restore_checksum_mismatch_is_fatal_and_leaves_target_empty(tmp_path: Path):
    client = _seeded_snapshot()
    corrupted_name = next(iter(client.assets))
    for name in list(client.assets):
        if name != "manifest.json":
            client.assets[name] = b"CORRUPTED"
    target = tmp_path / "restored"
    with pytest.raises(UnexpectedFailure, match="checksum"):
        restore_from_tag(client, target_root=target, work_dir=tmp_path / "work")
    assert not target.exists()  # two-phase: verify-all-before-materialize-any
```

Add a CLI test in `test_cli.py`: `restore-from-snapshot --tag X` with no `--target` defaults under `config.DATA_DIR / "_restore_drill"` (assert the resolved path, don't actually touch real dirs — monkeypatch `config.DATA_DIR` to a tmp path first); with `--target /explicit/path` uses exactly that path.

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_restore.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.restore'`

- [ ] **Step 3: Implement**

Create `pipeline/src/pipeline/restore.py`:

```python
"""Disaster-recovery restore: materialize any release/snapshot tag's baseline
datasets under an arbitrary target directory tree.

Deliberately independent of sync.py's dataset-registry routing -- a restore
target is an arbitrary directory (a scratch dir for a drill, or the real
data/ tree for an actual recovery), not necessarily today's live DATASETS
registry, so this re-implements the two-phase verify-then-materialize
discipline rather than importing sync.py internals. Restores baselines only;
deltas are a live-client catch-up mechanism, not a DR concern."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.errors import UnexpectedFailure
from pipeline.manifest import dataset_files, file_digest
from pipeline.release import ReleaseClient


def restore_from_tag(
    client: ReleaseClient, *, target_root: Path, work_dir: Path
) -> dict[str, Any]:
    work_dir.mkdir(parents=True, exist_ok=True)
    client.download(["manifest.json"], work_dir)
    manifest: dict[str, Any] = json.loads((work_dir / "manifest.json").read_text())

    verified: list[tuple[Path, str, str]] = []  # (downloaded_path, dataset_name, logical_name)
    for ds in manifest.get("datasets", []):
        for entry in dataset_files(ds):
            asset = entry.get("asset", entry["name"])
            client.download([asset], work_dir)
            got = work_dir / asset
            sha, _ = file_digest(got)
            if sha != entry["sha256"]:
                raise UnexpectedFailure(
                    f"restore checksum mismatch for {asset}: got {sha}, "
                    f"manifest says {entry['sha256']}"
                )
            verified.append((got, str(ds["name"]), entry["name"]))

    for got, dataset_name, logical_name in verified:
        dest_dir = target_root / dataset_name
        dest_dir.mkdir(parents=True, exist_ok=True)
        got.replace(dest_dir / logical_name)

    return manifest
```

CLI: add `restore-from-snapshot` to `build_parser()` with `--tag` (required) and `--target` (optional, default `None` meaning "resolve to `config.DATA_DIR / '_restore_drill' / tag`" at dispatch time); dispatch branch builds `GhReleaseClient(repo=config.GITHUB_REPO, tag=args.tag)`, calls `restore_from_tag`, prints a per-dataset summary, catches `(ReleaseError, UnexpectedFailure)` → exit 1.

- [ ] **Step 4: Run to verify pass**, then **Step 5: full gate + commit**

```bash
.venv/bin/python -m pytest -q && .venv/bin/python -m mypy && .venv/bin/python -m ruff check .
git add src/pipeline/restore.py src/pipeline/cli.py tests/test_restore.py tests/test_cli.py RUNBOOK.md
git commit -m "feat(g3/task-5): restore-from-snapshot — scratch-safe-by-default DR restore tooling"
```

---

### Task 6 (CONTROLLER-RUN — small-scale live feasibility test, NOT a subagent task)

Before committing to the full 300-day run, the controller directly:
1. Runs `python -m pipeline backfill --dataset equities --days 10` (and `--dataset indices --days 10`) against the REAL NSE endpoints from the controller's own environment, observing whether sustained sequential requests (not just the single-day fetches already proven in G1b/G2's live-validation passes) trigger any blocking/rate-limiting.
2. If clean: proceeds to Task 7 directly. If blocked partway: documents exactly where/how, and switches to instructing the user to run the backfill locally on a residential connection per RUNBOOK's existing guidance, resuming from wherever the controller's partial run left off (the backfill is idempotent — `has_day` skips already-present days at no cost).

### Task 7 (CONTROLLER-RUN, GATED ON EXPLICIT USER CONFIRMATION before the live publish step)

1. Execute the real 300-trading-day backfill for BOTH fetched datasets (equities, indices) against the final G1b/G2 schema (all cash series, per-series gates, `NSE:` sentinel keys, `IDX:` slugs, provenance-stamped, wrong-date-guarded, cache-accelerated per Task 2).
2. Verify: total row counts per dataset, per-series distribution sanity, zero unexpected quarantine spikes, `validate_ohlc` clean across the whole backfilled range.
3. **Stop and confirm with the user before this step**: run `pipeline publish` to push the full 300-day history to the live `data-latest` release for the first time (this is the first-ever full-history publish — a significant, hard-to-reverse production action per the project's own risk-handling guidance, distinct from every prior small-scale live-validation publish this roadmap has done).
4. Post-publish: confirm the manifest's `latest_date`/row counts match the backfill; run `check-freshness` and `cross-check` once by hand against the freshly-published release.

### Task 8 (CONTROLLER-RUN — exit criteria closure)

1. Dispatch `data-snapshot` once live (`gh workflow run data-snapshot`), confirm a `data-snapshot-YYYYMM` release appears with a manifest byte-identical to `data-latest`'s.
2. Rehearse the DR drill: `pipeline restore-from-snapshot --tag <that snapshot tag>` (scratch target, default), confirm the restored files' shas match the snapshot's manifest — this closes the roadmap's stated G3 exit criterion "restore-from-snapshot rehearsed once."
3. Record both outcomes in the progress ledger.

---

## Self-review notes

- **Spec coverage:** roadmap's "300 trading days, all series + indices, against final schema" → Tasks 6-7 (controller-run, gated). "Monthly snapshot + DR drill" → Tasks 4-5 (code) + Task 8 (live rehearsal, the stated exit criterion). Ledger-mandated dead-code cleanup → Task 3. Ledger-mandated store-perf work ("belongs with backfill-scale testing") → Tasks 1-2, sequenced BEFORE the real backfill so it actually benefits from the speedup.
- **Type consistency:** `ReadCache` methods (`get`/`put`/`invalidate`) used identically across Tasks 1-2; `cache: ReadCache | None = None` keyword is uniform across every store/daily_update signature it touches; `ReleaseClient` protocol extension (`list_releases`/`delete_release`) implemented identically on `GhReleaseClient` and the new `FakeReleaseRepo`-backed fakes; `dataset_files`/`file_digest` reused from `manifest.py` unchanged in both `snapshot.py` and `restore.py`.
- **Known judgment calls:** `restore.py` deliberately duplicates a small amount of two-phase-verify logic from `sync.py` rather than refactoring `sync.py` to share it — the trade-off (a few dozen duplicated lines vs touching the pipeline's most heavily-fought-over, highest-blast-radius module) is stated explicitly in the module docstring. Snapshot creation refuses to overwrite an existing same-month tag rather than silently no-op or clobber — loud failure is deliberate since it should never fire under the monthly cadence and any occurrence signals something worth a human's attention (a mis-scheduled dispatch, a clock issue, etc.).
