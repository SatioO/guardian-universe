# G1a — Dataset Mechanism + Manifest v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the single-dataset (ohlc-hardcoded) pipeline into a registry-driven multi-dataset platform with the frozen manifest-v2 client contract: `DatasetSpec` registry, per-dataset manifest sections with baselines + deltas, registry-driven sync/publish, quarantine persistence, and special-session calendar — while equities behavior stays byte-identical.

**Architecture:** A `DatasetSpec` frozen dataclass registry (`datasets.py`) is the single seam: store/validate/orchestration become prefix/range-parametric with equity-preserving defaults; manifest v2 emits one registry entry per dataset (`name`, `schema_version`, `latest_date`, `baseline[]`, `deltas[]`) plus top-level `manifest_version`/`min_client_version`; sync and publish iterate the registry and skip unknown manifest datasets (forward-compat). Daily runs additionally emit a per-day delta parquet and persist quarantined rows as self-GC'ing diagnostic release assets. G0's invariants (content-addressed assets, manifest-last flip, shrink/CAS/verify, non-fatal GC) are preserved and extended per-dataset.

**Tech Stack:** Python 3.11, pandas, pyarrow, pandera, pytest (filterwarnings=error), mypy --strict, ruff. GitHub Releases via the G0 `ReleaseClient` seam.

**Spec:** `docs/superpowers/specs/2026-07-05-scanner-platform-v2-design.md` §2.4 (deltas), §3 (data model & manifest v2), §4 (calendar). **G1b** (indices/reference/ca_flags/universe-widening datasets) builds on this mechanism — this plan deliberately registers only `EQUITIES`.

**Branch:** `feat/g1a-dataset-mechanism` off `main` (214dfa7, G0 merged). The parked `feat/p1c-indices` branch is design REFERENCE ONLY (its plan file + T1/T2 commits informed Tasks 1–3) — do not merge or cherry-pick from it; it predates G0's rewrites. It is deleted after G1b lands.

## Global Constraints

1. Working directory for all commands: `pipeline/` inside `~/Desktop/projects/guardian-universe`.
2. `pytest` runs under `filterwarnings = ["error"]`; `mypy --strict` and `ruff check .` must pass. Full gate before every commit: `python -m pytest -q && mypy && ruff check .`
3. No live network in tests; `gh` never invoked in tests (use `tests.fakes.FakeReleaseClient` / recorded runners / StubFetcher).
4. **Equities backward compatibility is mandatory:** on-disk layout `ohlc_{YYYY}.parquet` in `data/ohlc/`, rowcount gate `(1800, 3000)`, `source="nse-udiff"`, and every existing equities test's behavior unchanged. New parameters get equity-preserving defaults.
5. **No dataset name hardcoded in shared/dispatch code.** Identity flows through `DatasetSpec` resolved from `datasets.DATASETS`/`DATASET_ORDER`/`by_manifest_name()`. `cli.py` may map CLI strings to registry keys (the one allowed edge). Grep-check before each commit: no new `== "ohlc"` / `== "equities"` outside `datasets.py`/`cli.py`/tests.
6. **Live-release compatibility:** the current live manifest is G0-format (`schema_version: 1` top-level; `datasets:[{name:"ohlc", files:[{name,asset,rows,sha256,bytes}]}]`). All readers (sync, `_read_live_manifest` consumers, `check_no_shrink`, `assert_release_consistent`) must accept BOTH formats: per-dataset file list is `ds.get("baseline", ds.get("files", []))`; per-file asset is `entry.get("asset", entry["name"])`; `rows` optional.
7. G0 invariants unchanged: content-addressed data assets never clobbered; `manifest.json` flipped strictly last; `_verify` after flip; `_gc` after verify, fully non-fatal; CAS on `generated_at`; `run_daily` never raises; atomic tmp+replace writes.
8. `PROTECTED_ASSETS` stays `{"manifest.json", "last_run_status.json"}`. Quarantine/delta assets are content-addressed or date-named and self-GC after the 7-day grace once unreferenced — that is intended.
9. Commit after every task with the exact message given.

---

### Task 1: Dataset-prefix generalization of config + store

**Files:**
- Modify: `pipeline/src/pipeline/config.py`
- Modify: `pipeline/src/pipeline/store.py`
- Modify: `pipeline/tests/test_store.py`

**Interfaces:**
- Consumes: existing `config.ohlc_path(year, base)`, `store.append_day/has_day/day_symbol_count/read_trailing_window`, G0's atomic tmp+replace write.
- Produces:
  - `config.dataset_path(year: int, base: Path, *, prefix: str = "ohlc") -> Path` → `base / f"{prefix}_{year}.parquet"`
  - `config.ohlc_path(year, base=None)` becomes a thin shim delegating to `dataset_path` (signature unchanged; existing callers untouched)
  - `store.append_day(df, base, *, prefix: str = "ohlc")`, `store.has_day(base, d, *, prefix: str = "ohlc")`, `store.day_symbol_count(base, d, *, prefix: str = "ohlc")`, `store.read_trailing_window(base, end, n_rows_per_key, *, prefix: str = "ohlc")` — defaults preserve equity behavior exactly; atomic write preserved.

- [ ] **Step 1: Write the failing test**

Append to `pipeline/tests/test_store.py`:

```python
def test_prefix_writes_independent_dataset_files(tmp_path):
    import pandas as pd
    from datetime import date
    from pipeline import config, store

    def frame(day: str, key: str) -> pd.DataFrame:
        df = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
        df["date"] = pd.to_datetime([day])
        df["instrument_key"] = [key]
        return df

    store.append_day(frame("2026-07-03", "INE1"), tmp_path)
    store.append_day(frame("2026-07-03", "NIFTY50"), tmp_path, prefix="indices")

    assert (tmp_path / "ohlc_2026.parquet").exists()
    assert (tmp_path / "indices_2026.parquet").exists()
    # No cross-contamination: each file holds only its own dataset's row.
    assert store.day_symbol_count(tmp_path, date(2026, 7, 3)) == 1
    assert store.day_symbol_count(tmp_path, date(2026, 7, 3), prefix="indices") == 1
    assert store.has_day(tmp_path, date(2026, 7, 3), prefix="indices")
    w = store.read_trailing_window(tmp_path, date(2026, 7, 3), 5, prefix="indices")
    assert list(w["instrument_key"]) == ["NIFTY50"]
```

- [ ] **Step 2: Run to verify failure**

Run: `cd ~/Desktop/projects/guardian-universe/pipeline && python -m pytest tests/test_store.py -q`
Expected: FAIL — `TypeError: append_day() got an unexpected keyword argument 'prefix'`

- [ ] **Step 3: Implement**

`pipeline/src/pipeline/config.py` — replace `ohlc_path` with:

```python
def dataset_path(year: int, base: Path, *, prefix: str = "ohlc") -> Path:
    """Path to a dataset's year-partitioned parquet file: {prefix}_{YYYY}.parquet."""
    return base / f"{prefix}_{year}.parquet"


def ohlc_path(year: int, base: Path | None = None) -> Path:
    """Equities shim (kept for existing callers): ohlc_{YYYY}.parquet."""
    return dataset_path(year, base if base is not None else OHLC_DIR, prefix="ohlc")
```

`pipeline/src/pipeline/store.py` — thread `*, prefix: str = "ohlc"` through all four public functions and `_read_year`, replacing every `config.ohlc_path(year, base)` with `config.dataset_path(year, base, prefix=prefix)`. `_read_year(base, year, prefix)` becomes positional-third. The atomic write block in `append_day` becomes:

```python
        target = config.dataset_path(int(year), base, prefix=prefix)
        tmp = target.with_suffix(".parquet.tmp")
        combined.to_parquet(tmp, compression="zstd", index=False)
        tmp.replace(target)
```

- [ ] **Step 4: Run to verify pass** — `python -m pytest tests/test_store.py -q` → all pass (existing tests unchanged).

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/config.py src/pipeline/store.py tests/test_store.py
git commit -m "feat(g1a/task-1): dataset-prefix generalization of config+store (equity behavior invariant)"
```

---

### Task 2: Parametric rowcount gate

**Files:**
- Modify: `pipeline/src/pipeline/validate.py`
- Modify: `pipeline/tests/test_validate.py`

**Interfaces:**
- Produces: `validate.check_rowcount(count, trailing, *, abs_range: tuple[int, int] | None = None)` — `None` binds `config.ROWCOUNT_ABS_RANGE` at CALL time (None-sentinel, not def-time default — a def-time bind broke monkeypatch-based tests once before on this repo; see ledger P1c T2). `ROWCOUNT_DEVIATION` stays global.

- [ ] **Step 1: Write the failing test**

Append to `pipeline/tests/test_validate.py`:

```python
def test_check_rowcount_custom_abs_range():
    import pytest
    from pipeline import validate
    from pipeline.errors import UnexpectedFailure

    validate.check_rowcount(120, [], abs_range=(50, 500))  # accepted
    with pytest.raises(UnexpectedFailure, match="absolute range"):
        validate.check_rowcount(20, [], abs_range=(50, 500))
```

- [ ] **Step 2: Run to verify failure** — `python -m pytest tests/test_validate.py -q` → FAIL (`unexpected keyword argument 'abs_range'`)

- [ ] **Step 3: Implement** — in `validate.check_rowcount`, change the signature to `def check_rowcount(count: int, trailing: list[int], *, abs_range: tuple[int, int] | None = None) -> None:` and the first lines to:

```python
    lo, hi = abs_range if abs_range is not None else config.ROWCOUNT_ABS_RANGE
```

- [ ] **Step 4: Run to verify pass** — `python -m pytest tests/test_validate.py -q` → all pass.

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/validate.py tests/test_validate.py
git commit -m "feat(g1a/task-2): parametric abs_range on check_rowcount (call-time None-sentinel)"
```

---

### Task 3: DatasetSpec registry + spec-driven run_daily/backfill

**Files:**
- Create: `pipeline/src/pipeline/datasets.py`
- Create: `pipeline/tests/test_datasets.py`
- Modify: `pipeline/src/pipeline/daily_update.py`
- Modify: `pipeline/src/pipeline/backfill.py`
- Modify: `pipeline/tests/test_daily_update.py`, `pipeline/tests/test_backfill.py` (thread the spec; behavior identical)
- Modify: `pipeline/src/pipeline/cli.py` (pass `datasets.EQUITIES` at the two `run_daily`/`backfill` call sites)

**Interfaces:**
- Produces (G1b registers more specs against exactly this):

```python
# datasets.py
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipeline import config
from pipeline.fetch import Fetcher, NseUdiffFetcher
from pipeline.normalize import normalize_equity_bhavcopy


@dataclass(frozen=True)
class DatasetSpec:
    key: str                    # registry key: "equities"
    file_prefix: str            # {prefix}_{YYYY}.parquet
    base_dir: Path
    source_label: str           # provenance recorded in RunStatus.source
    normalizer: Callable[[pd.DataFrame], pd.DataFrame]
    make_fetcher: Callable[[], Fetcher]
    abs_rowcount_range: tuple[int, int]
    manifest_name: str          # dataset name in manifest.json
    schema_version: int


EQUITIES = DatasetSpec(
    key="equities", file_prefix="ohlc", base_dir=config.OHLC_DIR,
    source_label="nse-udiff", normalizer=normalize_equity_bhavcopy,
    make_fetcher=NseUdiffFetcher, abs_rowcount_range=config.ROWCOUNT_ABS_RANGE,
    manifest_name="ohlc", schema_version=1,
)

DATASETS: dict[str, DatasetSpec] = {"equities": EQUITIES}
DATASET_ORDER: list[str] = ["equities"]


def by_manifest_name(name: str) -> DatasetSpec | None:
    for spec in DATASETS.values():
        if spec.manifest_name == name:
            return spec
    return None
```

- `run_daily(spec: DatasetSpec, target: date, *, fetcher: Fetcher, holidays: set[date]) -> RunStatus` — the old `base` param is REPLACED by the spec (base = `spec.base_dir`, prefix = `spec.file_prefix` threaded into every store call incl. `_trailing_counts`; `check_rowcount(..., abs_range=spec.abs_rowcount_range)`; `spec.normalizer(raw)`; success `source=spec.source_label`). Fail-closed guards byte-identical.
- `backfill(spec: DatasetSpec, end, n, *, fetcher, holidays, sleep=time.sleep, delay_s=1.0)` — passes spec through.

- [ ] **Step 1: Write the failing tests**

Create `pipeline/tests/test_datasets.py`:

```python
from pipeline import config, datasets


def test_equities_spec_fields():
    s = datasets.EQUITIES
    assert s.key == "equities" and s.file_prefix == "ohlc"
    assert s.base_dir == config.OHLC_DIR and s.source_label == "nse-udiff"
    assert s.abs_rowcount_range == config.ROWCOUNT_ABS_RANGE
    assert s.manifest_name == "ohlc" and s.schema_version == 1
    assert datasets.DATASETS["equities"] is s
    assert datasets.DATASET_ORDER == ["equities"]


def test_by_manifest_name():
    assert datasets.by_manifest_name("ohlc") is datasets.EQUITIES
    assert datasets.by_manifest_name("nope") is None
```

Update every `run_daily(target, fetcher=..., holidays=..., base=tmp_path)` call in `tests/test_daily_update.py` to `run_daily(datasets_spec(tmp_path), target, fetcher=..., holidays=...)` using this helper added at the top of the file (keeps tests on tmp dirs):

```python
import dataclasses

from pipeline import datasets


def datasets_spec(base):
    return dataclasses.replace(datasets.EQUITIES, base_dir=base)
```

Same helper + threading in `tests/test_backfill.py`.

- [ ] **Step 2: Run to verify failure** — `python -m pytest tests/test_datasets.py tests/test_daily_update.py -q` → FAIL (`No module named 'pipeline.datasets'`)

- [ ] **Step 3: Implement** — create `datasets.py` exactly as the Interfaces block; rewrite `run_daily`/`_trailing_counts`/`backfill` signatures per the Interfaces block (every `store.has_day(base, d)` → `store.has_day(spec.base_dir, d, prefix=spec.file_prefix)` etc.); update the two cli call sites (`run_daily(datasets.EQUITIES, target, fetcher=fetcher, holidays=holidays)` and the backfill equivalent; add `from pipeline import datasets` import).

- [ ] **Step 4: Run to verify pass** — `python -m pytest -q` → all pass (equities orchestration behavior identical).

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/datasets.py src/pipeline/daily_update.py src/pipeline/backfill.py src/pipeline/cli.py tests/test_datasets.py tests/test_daily_update.py tests/test_backfill.py
git commit -m "feat(g1a/task-3): DatasetSpec registry; spec-driven run_daily/backfill (equities-only registered)"
```

---

### Task 4: Special-session calendar (Muhurat)

**Files:**
- Create: `pipeline/data/meta/special_sessions.json`
- Modify: `pipeline/src/pipeline/calendar.py`
- Modify: `pipeline/tests/test_calendar.py`
- Modify: `pipeline/src/pipeline/cli.py` (load + thread)

**Interfaces:**
- Produces:
  - `calendar.load_special_sessions(path: Path) -> set[date]` — file format `{"sessions": [{"date": "2026-11-08", "label": "muhurat"}]}`; missing file → empty set (additive, non-breaking).
  - `calendar.is_trading_day(d, holidays, special_sessions: set[date] | None = None)` — a date in `special_sessions` is a trading day REGARDLESS of weekend/holiday status.
  - `calendar.previous_trading_day(d, holidays, special_sessions=None)` and `calendar.trading_days_back(end, n, holidays, special_sessions=None)` — same override, threaded.
  - `freshness.is_stale(latest, today, holidays, special_sessions=None)` — passes through to `previous_trading_day`.
  - CLI: `daily`, `backfill`, `check-freshness` load `config.META_DIR / "special_sessions.json"` and pass the set to `run_daily`... note `run_daily` takes `holidays` only — ADD `special_sessions: set[date] | None = None` keyword to `run_daily` and thread it into its `cal.is_trading_day` + `_trailing_counts` calls.

- [ ] **Step 1: Create the data file**

`pipeline/data/meta/special_sessions.json`:

```json
{
  "sessions": [
    { "date": "2026-11-08", "label": "muhurat-2026 (VERIFY against NSE circular before November)" }
  ]
}
```

- [ ] **Step 2: Write the failing tests**

Append to `pipeline/tests/test_calendar.py`:

```python
def test_special_session_overrides_weekend_and_holiday(tmp_path):
    import json
    from datetime import date
    from pipeline import calendar as cal

    muhurat = date(2026, 11, 8)  # a Sunday
    assert not cal.is_trading_day(muhurat, set())
    assert cal.is_trading_day(muhurat, set(), special_sessions={muhurat})
    assert cal.is_trading_day(muhurat, {muhurat}, special_sessions={muhurat})  # beats holiday too

    p = tmp_path / "special_sessions.json"
    p.write_text(json.dumps({"sessions": [{"date": "2026-11-08", "label": "muhurat"}]}))
    assert cal.load_special_sessions(p) == {muhurat}
    assert cal.load_special_sessions(tmp_path / "absent.json") == set()


def test_previous_trading_day_sees_special_session():
    from datetime import date
    from pipeline import calendar as cal

    muhurat = date(2026, 11, 8)  # Sunday
    monday = date(2026, 11, 9)
    assert cal.previous_trading_day(monday, set()) == date(2026, 11, 6)
    assert cal.previous_trading_day(monday, set(), special_sessions={muhurat}) == muhurat
```

- [ ] **Step 3: Run to verify failure** — `python -m pytest tests/test_calendar.py -q` → FAIL

- [ ] **Step 4: Implement**

In `calendar.py` add:

```python
def load_special_sessions(path: Path) -> set[date]:
    """Special trading sessions (e.g. Muhurat) that trade despite weekend/holiday."""
    if not path.exists():
        return set()
    raw = json.loads(path.read_text())
    return {date.fromisoformat(s["date"]) for s in raw.get("sessions", [])}
```

and change the three calendar functions to accept `special_sessions: set[date] | None = None`, with `is_trading_day` starting:

```python
    if special_sessions and d in special_sessions:
        return True
```

(`previous_trading_day`/`trading_days_back` just pass the param through to `is_trading_day`.) In `freshness.py` add the pass-through param. In `daily_update.run_daily` add `special_sessions: set[date] | None = None` and pass into `cal.is_trading_day(target, holidays, special_sessions=special_sessions)` and `_trailing_counts` (which passes into `previous_trading_day`/`trading_days_back`). In `cli.py` load once per command: `special = cal.load_special_sessions(config.META_DIR / "special_sessions.json")` and thread into `run_daily`, `backfill` (add the same keyword there), and `freshness.is_stale` inside `cmd_check_freshness` (add the param to that helper's signature too).

- [ ] **Step 5: Run to verify pass** — `python -m pytest -q` → all pass.

- [ ] **Step 6: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add data/meta/special_sessions.json src/pipeline/calendar.py src/pipeline/freshness.py src/pipeline/daily_update.py src/pipeline/backfill.py src/pipeline/cli.py tests/test_calendar.py
git commit -m "feat(g1a/task-4): special-session calendar — Muhurat trades, monitor stays calendar-aware"
```

---

### Task 5: Per-day delta emission

**Files:**
- Modify: `pipeline/src/pipeline/store.py`
- Modify: `pipeline/src/pipeline/daily_update.py`
- Modify: `pipeline/tests/test_store.py`, `pipeline/tests/test_daily_update.py`

**Interfaces:**
- Produces:
  - `store.write_delta(df, base, d, *, prefix: str = "ohlc", keep: int = 35) -> Path` — writes the day's clean frame to `base / "deltas" / f"{prefix}_{d.isoformat()}.parquet"` (atomic tmp+replace, zstd), then prunes the deltas dir to the newest `keep` files for that prefix (sorted by filename == by date). Returns the path.
  - `store.list_deltas(base, *, prefix: str = "ohlc") -> list[Path]` — sorted ascending by filename.
  - `run_daily` calls `store.write_delta(clean, spec.base_dir, target, prefix=spec.file_prefix)` immediately AFTER the successful `append_day` and before returning success. Delta write failure maps to `failed` status like any other post-fetch error (the boundary guard already catches it).

- [ ] **Step 1: Write the failing tests**

Append to `pipeline/tests/test_store.py`:

```python
def test_write_delta_and_prune(tmp_path):
    import pandas as pd
    from datetime import date, timedelta
    from pipeline import config, store

    def frame(day: str) -> pd.DataFrame:
        df = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
        df["date"] = pd.to_datetime([day])
        return df

    start = date(2026, 1, 1)
    for i in range(40):
        d = start + timedelta(days=i)
        p = store.write_delta(frame(d.isoformat()), tmp_path, d, keep=35)
        assert p.exists() and p.parent.name == "deltas"

    deltas = store.list_deltas(tmp_path)
    assert len(deltas) == 35                          # pruned to keep
    assert deltas[0].name == "ohlc_2026-01-06.parquet"  # oldest 5 pruned
    assert deltas[-1].name == "ohlc_2026-02-09.parquet"
```

Append to `pipeline/tests/test_daily_update.py` (uses the file's existing StubFetcher/fixture pattern and the `datasets_spec` helper from Task 3):

```python
def test_success_emits_delta_file(tmp_path, normal_fetcher, holidays_2026):
    # normal_fetcher/holidays fixtures: reuse whatever this file already uses for the
    # happy-path success test; the assertion below is the only new content.
    from datetime import date
    from pipeline import store
    from pipeline.daily_update import run_daily

    st = run_daily(datasets_spec(tmp_path), date(2026, 7, 3),
                   fetcher=normal_fetcher, holidays=holidays_2026)
    assert st.status == "success"
    deltas = store.list_deltas(tmp_path)
    assert [p.name for p in deltas] == ["ohlc_2026-07-03.parquet"]
```

(If the existing file's happy-path test builds its fetcher/holidays inline rather than via fixtures, mirror that construction instead of fixture params — keep the delta assertions identical.)

- [ ] **Step 2: Run to verify failure** — `python -m pytest tests/test_store.py tests/test_daily_update.py -q` → FAIL (`no attribute 'write_delta'`)

- [ ] **Step 3: Implement**

Append to `store.py`:

```python
def write_delta(
    df: pd.DataFrame, base: Path, d: date, *, prefix: str = "ohlc", keep: int = 35
) -> Path:
    """Persist one day's clean frame as a delta artifact (client catch-up unit).

    Prunes to the newest `keep` per prefix; release-side copies self-GC once
    they drop out of the manifest's delta window."""
    delta_dir = base / "deltas"
    delta_dir.mkdir(parents=True, exist_ok=True)
    target = delta_dir / f"{prefix}_{d.isoformat()}.parquet"
    tmp = target.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, compression="zstd", index=False)
    tmp.replace(target)
    existing = sorted(delta_dir.glob(f"{prefix}_*.parquet"))
    for old in existing[:-keep]:
        old.unlink()
    return target


def list_deltas(base: Path, *, prefix: str = "ohlc") -> list[Path]:
    delta_dir = base / "deltas"
    if not delta_dir.exists():
        return []
    return sorted(delta_dir.glob(f"{prefix}_*.parquet"))
```

In `run_daily`, after `store.append_day(clean, spec.base_dir, prefix=spec.file_prefix)` add:

```python
        store.write_delta(clean, spec.base_dir, target, prefix=spec.file_prefix)
```

- [ ] **Step 4: Run to verify pass** — `python -m pytest -q` → all pass.

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/store.py src/pipeline/daily_update.py tests/test_store.py tests/test_daily_update.py
git commit -m "feat(g1a/task-5): per-day delta emission with pruned window"
```

---

### Task 6: Quarantine persistence

**Files:**
- Modify: `pipeline/src/pipeline/daily_update.py`
- Modify: `pipeline/tests/test_daily_update.py`
- Modify: `.gitignore` (repo root)

**Interfaces:**
- Produces: on any run where `len(bad) > 0`, `run_daily` writes the quarantined rows to `config.META_DIR / "quarantine" / f"{spec.file_prefix}_{target.isoformat()}.parquet"` (atomic tmp+replace) BEFORE the all-rows-corrupt check, so both the partial-quarantine success path and the all-corrupt failed path persist evidence. `RunStatus` unchanged. Task 8 publishes the current day's file as a diagnostic extra.
- `.gitignore` gains `pipeline/data/meta/quarantine/`.

- [ ] **Step 1: Write the failing test**

Append to `pipeline/tests/test_daily_update.py` (mirror the file's existing dirty-fixture quarantine test construction):

```python
def test_quarantined_rows_are_persisted(tmp_path, monkeypatch):
    import pandas as pd
    from datetime import date
    from pipeline import config
    from pipeline.daily_update import run_daily

    monkeypatch.setattr(config, "META_DIR", tmp_path / "meta")
    # Build a fetcher whose frame yields exactly one quarantined row (e.g. high < low)
    # using the same raw-fixture helper the existing quarantine test in this file uses.
    st = run_daily(datasets_spec(tmp_path), date(2026, 7, 3),
                   fetcher=dirty_fetcher_one_bad_row(), holidays=set())
    assert st.status == "success" and st.quarantined_count == 1
    qfile = tmp_path / "meta" / "quarantine" / "ohlc_2026-07-03.parquet"
    assert qfile.exists()
    assert len(pd.read_parquet(qfile)) == 1
```

(`dirty_fetcher_one_bad_row` = whatever constructor the existing quarantine test uses; reuse it verbatim. If the existing test builds the dirty frame inline, extract or repeat that construction.)

- [ ] **Step 2: Run to verify failure** — FAIL (`qfile.exists()` is False)

- [ ] **Step 3: Implement**

In `run_daily`, right after `clean, bad = validate.quarantine_bad_rows(df)` add:

```python
        if len(bad) > 0:
            qdir = config.META_DIR / "quarantine"
            qdir.mkdir(parents=True, exist_ok=True)
            qtarget = qdir / f"{spec.file_prefix}_{target.isoformat()}.parquet"
            qtmp = qtarget.with_suffix(".parquet.tmp")
            bad.to_parquet(qtmp, compression="zstd", index=False)
            qtmp.replace(qtarget)
```

(add `from pipeline import config` import if absent). Append `pipeline/data/meta/quarantine/` to `.gitignore`.

- [ ] **Step 4: Run to verify pass** — `python -m pytest -q` → all pass.

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/daily_update.py tests/test_daily_update.py ../.gitignore
git commit -m "feat(g1a/task-6): persist quarantined rows as dated parquet evidence"
```

---

### Task 7: Manifest v2

**Files:**
- Modify: `pipeline/src/pipeline/manifest.py`
- Modify: `pipeline/src/pipeline/config.py` (add `MANIFEST_VERSION = 2`, `MIN_CLIENT_VERSION = "0.1.0"`)
- Modify: `pipeline/tests/test_manifest.py`
- Modify: `pipeline/tests/fakes.py` (`assert_release_consistent` learns v2)

**Interfaces:**
- Produces:

```python
manifest.build_manifest(specs: list[DatasetSpec], *, latest_trading_date: date, generated_at: str) -> dict
```

emitting:

```json
{ "manifest_version": 2, "min_client_version": "0.1.0",
  "generated_at": "...", "latest_trading_date": "...",
  "datasets": [
    { "name": "ohlc", "schema_version": 1, "latest_date": "<max date in baseline files, ISO>",
      "baseline": [ {"name": "ohlc_2026.parquet", "asset": "ohlc_2026.<sha8>.parquet", "sha256": "...", "bytes": 0, "rows": 0} ],
      "deltas":   [ {"date": "2026-07-03", "name": "ohlc_2026-07-03.parquet", "asset": "delta_ohlc_2026-07-03.<sha8>.parquet", "sha256": "...", "bytes": 0} ] } ] }
```

Rules: baseline = `spec.base_dir.glob(f"{spec.file_prefix}_*.parquet")` sorted; deltas = `store.list_deltas(spec.base_dir, prefix=spec.file_prefix)` newest-30, ascending, delta asset name = `"delta_" + asset_name(p.name, sha)`; a spec with NO baseline files is omitted entirely; `latest_date` per dataset = max `date` column across its baseline files (reuse the column-pruned read; empty → omit dataset). Old top-level `schema_version` key is DROPPED (no clients exist; producer readers get compat in Tasks 8–9).
  - `manifest.dataset_files(ds: dict) -> list[dict]` helper: `ds.get("baseline", ds.get("files", []))` — the ONE place v1/v2 reader compat lives; sync/publish/fakes all use it.
  - `fakes.assert_release_consistent` iterates `dataset_files(ds)` + each `ds.get("deltas", [])` entry, verifying presence + sha of every referenced asset (unchanged invariant, wider coverage).

- [ ] **Step 1: Write the failing tests**

Rewrite the two G0-era `build_manifest` tests in `tests/test_manifest.py` and add:

```python
def _write_year(dirpath, prefix, year, days):
    import pandas as pd
    from pipeline import config
    dirpath.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({c: ["x"] * len(days) for c in config.CANON_COLUMNS})
    df["date"] = pd.to_datetime(days)
    df["instrument_key"] = [f"K{i}" for i in range(len(days))]
    df.to_parquet(dirpath / f"{prefix}_{year}.parquet", compression="zstd", index=False)


def test_build_manifest_v2_shape(tmp_path):
    import dataclasses
    from datetime import date
    from pipeline import datasets, store
    from pipeline.manifest import asset_name, build_manifest

    spec = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path)
    _write_year(tmp_path, "ohlc", 2026, ["2026-07-02", "2026-07-03"])
    import pandas as pd
    from pipeline import config
    day = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
    day["date"] = pd.to_datetime(["2026-07-03"])
    store.write_delta(day, tmp_path, date(2026, 7, 3))

    m = build_manifest([spec], latest_trading_date=date(2026, 7, 3), generated_at="g")
    assert m["manifest_version"] == 2 and m["min_client_version"] == "0.1.0"
    assert m["latest_trading_date"] == "2026-07-03"
    (ds,) = m["datasets"]
    assert ds["name"] == "ohlc" and ds["schema_version"] == 1
    assert ds["latest_date"] == "2026-07-03"
    (b,) = ds["baseline"]
    assert b["name"] == "ohlc_2026.parquet" and b["rows"] == 2
    assert b["asset"] == asset_name("ohlc_2026.parquet", b["sha256"])
    (d,) = ds["deltas"]
    assert d["date"] == "2026-07-03" and d["asset"].startswith("delta_ohlc_2026-07-03.")


def test_build_manifest_omits_empty_dataset(tmp_path):
    import dataclasses
    from datetime import date
    from pipeline import datasets
    from pipeline.manifest import build_manifest

    empty = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "nothing")
    m = build_manifest([empty], latest_trading_date=date(2026, 7, 3), generated_at="g")
    assert m["datasets"] == []


def test_dataset_files_reads_v1_and_v2():
    from pipeline.manifest import dataset_files
    assert dataset_files({"files": [1]}) == [1]      # v1 (G0 live manifest)
    assert dataset_files({"baseline": [2]}) == [2]   # v2
    assert dataset_files({}) == []
```

- [ ] **Step 2: Run to verify failure** — `python -m pytest tests/test_manifest.py -q` → FAIL

- [ ] **Step 3: Implement** per the Interfaces block. `build_manifest` core:

```python
def build_manifest(
    specs: list[DatasetSpec], *, latest_trading_date: date, generated_at: str
) -> dict[str, Any]:
    out_datasets: list[dict[str, Any]] = []
    for spec in specs:
        baseline: list[dict[str, Any]] = []
        latest: date | None = None
        for p in sorted(spec.base_dir.glob(f"{spec.file_prefix}_*.parquet")):
            sha, size = file_digest(p)
            baseline.append({"name": p.name, "asset": asset_name(p.name, sha),
                             "sha256": sha, "bytes": size, "rows": parquet_rows(p)})
            col = pd.to_datetime(pd.read_parquet(p, columns=["date"])["date"])
            if not col.empty:
                d = col.max().date()
                latest = d if latest is None or d > latest else latest
        if not baseline or latest is None:
            continue
        deltas: list[dict[str, Any]] = []
        for p in store.list_deltas(spec.base_dir, prefix=spec.file_prefix)[-30:]:
            sha, size = file_digest(p)
            deltas.append({"date": p.stem.removeprefix(f"{spec.file_prefix}_"),
                           "name": p.name, "asset": "delta_" + asset_name(p.name, sha),
                           "sha256": sha, "bytes": size})
        out_datasets.append({"name": spec.manifest_name, "schema_version": spec.schema_version,
                             "latest_date": latest.isoformat(), "baseline": baseline,
                             "deltas": deltas})
    return {"manifest_version": config.MANIFEST_VERSION,
            "min_client_version": config.MIN_CLIENT_VERSION,
            "generated_at": generated_at,
            "latest_trading_date": latest_trading_date.isoformat(),
            "datasets": out_datasets}


def dataset_files(ds: dict[str, Any]) -> list[dict[str, Any]]:
    """v1/v2 reader compat: G0 manifests use 'files', v2 uses 'baseline'."""
    files: list[dict[str, Any]] = ds.get("baseline", ds.get("files", []))
    return files
```

(`from pipeline import config, store` + `from pipeline.datasets import DatasetSpec` + `import pandas as pd` imports; `DatasetSpec` import is type-only — guard against circularity: datasets.py must NOT import manifest.py — it doesn't.) Update `fakes.assert_release_consistent` to use `dataset_files(ds)` and additionally iterate `ds.get("deltas", [])` with the same presence+sha assertions. Add the two constants to `config.py`. NOTE: `cli.py`/`publish.py` still call the old signature after this task — update the `build_manifest` call in `publish.py` minimally (`[datasets.EQUITIES]` hardcoded is FORBIDDEN by constraint 5; pass through a `specs` parameter added to `publish_dataset` in Task 8 — so in THIS task, keep publish compiling by changing its internal call to `build_manifest([spec for spec in (datasets.DATASETS[k] for k in datasets.DATASET_ORDER)], ...)` via a module-level helper `datasets.all_specs() -> list[DatasetSpec]` added here: `def all_specs() -> list[DatasetSpec]: return [DATASETS[k] for k in DATASET_ORDER]`. `publish.py` calls `build_manifest(datasets.all_specs(), latest_trading_date=..., generated_at=...)`. Existing publish tests will need their manifest-shape expectations updated to v2 (baseline key) — update `tests/test_publish.py` and `tests/test_chaos.py` assertions accordingly in this task if they break; prefer switching them to `dataset_files()`.)

- [ ] **Step 4: Run to verify pass** — `python -m pytest -q` → all pass (publish/chaos suites green against v2 shape).

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/manifest.py src/pipeline/config.py src/pipeline/datasets.py src/pipeline/publish.py tests/test_manifest.py tests/fakes.py tests/test_publish.py tests/test_chaos.py
git commit -m "feat(g1a/task-7): manifest v2 — per-dataset registry with baselines + delta window; v1-reader compat"
```

---

### Task 8: Registry-driven publish (multi-dataset, deltas, quarantine extras)

**Files:**
- Modify: `pipeline/src/pipeline/publish.py`
- Modify: `pipeline/src/pipeline/cli.py`
- Modify: `pipeline/tests/test_publish.py`

**Interfaces:**
- Produces: `publish_dataset(*, specs: list[DatasetSpec], meta_dir, stage_dir, client, generated_at, now) -> None` — replaces `ohlc_dir`/`schema_version` params. Changes vs G0, in the same 12-step frame:
  1. Empty-store guard: refuse if the PRIMARY dataset (`specs[0]`, equities) has no baseline files; other specs may be empty (omitted from manifest).
  2. `latest_trading_date(specs[0])` (rename existing helper param from dir to spec: glob `spec.base_dir/f"{spec.file_prefix}_*.parquet"`).
  3. `build_manifest(specs, ...)` (Task 7 signature).
  4. `check_no_shrink(new, live)` becomes per-dataset: for each live dataset entry, match new dataset by `name`; a live dataset MISSING from new → fail (unless its files list — via `dataset_files` — is empty); per-file checks use `dataset_files()` for both sides; deltas are NOT shrink-checked (they roll). `latest_trading_date` regression check unchanged.
  5. Upload set = every baseline entry + every delta entry across all manifest datasets (skip assets already on the release; stage under `entry["asset"]`; the source path for a delta entry is `spec.base_dir/"deltas"/entry["name"]` — build an upload worklist of `(src_path, asset_name)` tuples while constructing the manifest datasets... simplest correct approach: after `build_manifest`, reconstruct src paths: for each manifest dataset resolve `spec = datasets.by_manifest_name(ds["name"])`, baseline src = `spec.base_dir/entry["name"]`, delta src = `spec.base_dir/"deltas"/entry["name"]`.
  6. Quarantine extras: for each spec, if `config.META_DIR/"quarantine"/f"{spec.file_prefix}_{<manifest latest_trading_date>}.parquet"` exists, upload it (clobber ok, not in manifest — self-GCs after grace).
  7. `_verify` smallest-asset check operates over all baseline+delta entries (flatten).
  8. `_gc` referenced-set = all baseline+delta assets across datasets.
- CLI publish branch: `publish_dataset(specs=datasets.all_specs(), meta_dir=config.META_DIR, stage_dir=config.DATA_DIR / "stage", client=client, generated_at=..., now=...)`.

- [ ] **Step 1: Write the failing tests**

Update `tests/test_publish.py`: change every `publish_dataset(ohlc_dir=..., meta_dir=..., ...)` call to the new signature using a spec helper at the top of the file:

```python
import dataclasses

from pipeline import datasets


def specs_for(base):
    return [dataclasses.replace(datasets.EQUITIES, base_dir=base)]
```

(`_store(tmp_path, days)` keeps writing into `tmp_path/"ohlc"` — pass `specs_for(tmp_path/"ohlc")`.) Add three new tests:

```python
def test_publish_uploads_delta_assets(tmp_path):
    from datetime import date
    import pandas as pd
    from pipeline import config, store
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    day = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
    day["date"] = pd.to_datetime(["2026-07-03"])
    store.write_delta(day, ohlc, date(2026, 7, 3))
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False)
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="g1", now=NOW)
    assert_release_consistent(fake)
    assert any(n.startswith("delta_ohlc_2026-07-03.") for n in fake.assets)


def test_shrink_guard_blocks_missing_live_dataset():
    from pipeline.publish import check_no_shrink
    from pipeline.errors import UnexpectedFailure
    import pytest
    live = {"latest_trading_date": "2026-07-03", "datasets": [
        {"name": "ohlc", "files": [{"name": "ohlc_2026.parquet", "sha256": "s", "bytes": 1}]},
        {"name": "indices", "baseline": [{"name": "indices_2026.parquet", "sha256": "t",
                                          "bytes": 1, "rows": 5, "asset": "a"}]}]}
    new = {"latest_trading_date": "2026-07-03", "datasets": [
        {"name": "ohlc", "baseline": [{"name": "ohlc_2026.parquet", "sha256": "s",
                                       "bytes": 1, "rows": 9, "asset": "b"}], "deltas": []}]}
    with pytest.raises(UnexpectedFailure, match="shrink"):
        check_no_shrink(new, live)  # live 'indices' dataset vanished locally


def test_publish_uploads_quarantine_extra(tmp_path, monkeypatch):
    import pandas as pd
    from pipeline import config
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    monkeypatch.setattr(config, "META_DIR", meta)
    qdir = meta / "quarantine"
    qdir.mkdir()
    pd.DataFrame({"x": [1]}).to_parquet(qdir / "ohlc_2026-07-03.parquet")
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False)
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="g1", now=NOW)
    assert "ohlc_2026-07-03.parquet" in fake.assets  # diagnostic extra, unreferenced -> self-GCs
```

- [ ] **Step 2: Run to verify failure** — `python -m pytest tests/test_publish.py -q` → FAIL

- [ ] **Step 3: Implement** per the Interfaces block, preserving the 12-step frame exactly (guards → uploads → status extra → manifest flip LAST → verify → gc → synced-state). `check_no_shrink` new core:

```python
def check_no_shrink(new: dict[str, Any], live: dict[str, Any] | None) -> None:
    if live is None:
        return
    new_by_name = {ds["name"]: ds for ds in new["datasets"]}
    for lds in live["datasets"]:
        lfiles = dataset_files(lds)
        if not lfiles:
            continue
        nds = new_by_name.get(lds["name"])
        if nds is None:
            raise UnexpectedFailure(
                f"shrink-guard: dataset {lds['name']!r} is on the live release but missing locally"
            )
        nfiles = {f["name"]: f for f in dataset_files(nds)}
        for lf in lfiles:
            nf = nfiles.get(lf["name"])
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
```

Upload worklist construction (inside `publish_dataset`, after guards):

```python
    worklist: list[tuple[Path, str]] = []
    for ds in new_manifest["datasets"]:
        spec = datasets.by_manifest_name(ds["name"])
        assert spec is not None  # manifest was built from these specs
        for entry in ds["baseline"]:
            worklist.append((spec.base_dir / entry["name"], entry["asset"]))
        for entry in ds["deltas"]:
            worklist.append((spec.base_dir / "deltas" / entry["name"], entry["asset"]))
    existing = {a.name for a in client.list_assets()}
    stage_dir.mkdir(parents=True, exist_ok=True)
    for src, asset in worklist:
        if asset in existing:
            continue
        staged = stage_dir / asset
        shutil.copyfile(src, staged)
        client.upload(staged)
```

Quarantine extras (after the worklist uploads, before the status upload):

```python
    for spec in specs:
        qfile = (config.META_DIR / "quarantine"
                 / f"{spec.file_prefix}_{new_manifest['latest_trading_date']}.parquet")
        if qfile.exists():
            client.upload(qfile, clobber=True)
```

`_verify`/`_gc` flatten baseline+deltas per dataset for the smallest-asset spot check and the referenced set. `latest_trading_date(spec)` reads `spec.base_dir.glob(f"{spec.file_prefix}_*.parquet")`.

- [ ] **Step 4: Run to verify pass** — `python -m pytest -q` → all pass (chaos suite must stay green — it exercises the same entry points via the new signature; update its `publish_dataset` calls with the same `specs_for` helper pattern, in `tests/test_chaos.py`).

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/publish.py src/pipeline/cli.py tests/test_publish.py tests/test_chaos.py
git commit -m "feat(g1a/task-8): registry-driven publish — multi-dataset baselines+deltas, per-dataset shrink guard, quarantine extras"
```

---

### Task 9: Registry-driven sync

**Files:**
- Modify: `pipeline/src/pipeline/sync.py`
- Modify: `pipeline/tests/test_sync.py`

**Interfaces:**
- Produces: `sync_store(client, *, meta_dir: Path, work_dir: Path) -> dict[str, Any] | None` — the `ohlc_dir` param is REMOVED; destinations resolve from the registry: for each manifest dataset, `spec = datasets.by_manifest_name(ds["name"])`; **unknown dataset → skip with a stderr note** (forward-compat: a future producer version may publish datasets this code predates); known → two-phase verify-then-materialize its `dataset_files(ds)` into `spec.base_dir` under logical names (baselines only — deltas are a client concern, the producer re-derives them). Synced-state semantics, fail-closed contract, and v1 `files`-key compat unchanged.
- CLI sync branch drops `ohlc_dir=` from the call.

- [ ] **Step 1: Write the failing tests**

Update `tests/test_sync.py` call sites (`sync_store(fake, meta_dir=..., work_dir=...)`; materialization asserted at `datasets.EQUITIES.base_dir`? NO — tests must not write into the real data dir: monkeypatch the registry instead). Add at the top of the file:

```python
import dataclasses

import pytest

from pipeline import datasets


@pytest.fixture()
def routed_equities(tmp_path, monkeypatch):
    spec = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "ohlc")
    monkeypatch.setattr(datasets, "DATASETS", {"equities": spec})
    monkeypatch.setattr(datasets, "DATASET_ORDER", ["equities"])
    return spec
```

Convert existing tests to use `routed_equities` (assert files land in `routed_equities.base_dir`). Add:

```python
def test_sync_skips_unknown_dataset(tmp_path, routed_equities, capsys):
    import hashlib, json
    from pipeline.manifest import asset_name
    from pipeline.sync import sync_store

    data = b"OHLC"
    sha = hashlib.sha256(data).hexdigest()
    manifest = {"manifest_version": 2, "generated_at": "g", "latest_trading_date": "2026-07-03",
                "datasets": [
                    {"name": "ohlc", "baseline": [{"name": "ohlc_2026.parquet",
                        "asset": asset_name("ohlc_2026.parquet", sha),
                        "sha256": sha, "bytes": len(data), "rows": 1}], "deltas": []},
                    {"name": "breadth", "baseline": [{"name": "breadth_2026.parquet",
                        "sha256": "whatever", "bytes": 3}]}]}
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed(asset_name("ohlc_2026.parquet", sha), data)
    got = sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")
    assert got is not None
    assert (routed_equities.base_dir / "ohlc_2026.parquet").read_bytes() == b"OHLC"
    # Unknown dataset skipped: never downloaded, noted on stderr, sync still succeeds.
    assert "breadth" in capsys.readouterr().err
```

- [ ] **Step 2: Run to verify failure** — `python -m pytest tests/test_sync.py -q` → FAIL

- [ ] **Step 3: Implement** — in `sync.py`, replace the `ds.get("name") != "ohlc"` filter with registry resolution:

```python
    for ds in manifest.get("datasets", []):
        spec = datasets.by_manifest_name(str(ds.get("name", "")))
        if spec is None:
            print(f"sync: skipping unknown dataset {ds.get('name')!r}", file=sys.stderr)
            continue
        for entry in dataset_files(ds):
            ...  # existing two-phase download+verify, dest = spec.base_dir / entry["name"]
```

(imports: `import sys`, `from pipeline import datasets`, `from pipeline.manifest import dataset_files, file_digest, write_json`; drop the `ohlc_dir` param and mkdir each `spec.base_dir` lazily in phase 2). Update `cli.py`'s sync call.

- [ ] **Step 4: Run to verify pass** — `python -m pytest -q` → all pass.

- [ ] **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/sync.py src/pipeline/cli.py tests/test_sync.py
git commit -m "feat(g1a/task-9): registry-driven sync — routes datasets by manifest name, skips unknown"
```

---

### Task 10: CLI `--dataset`, docs, migration check

**Files:**
- Modify: `pipeline/src/pipeline/cli.py`
- Modify: `pipeline/tests/test_cli.py`
- Modify: `RUNBOOK.md`

**Interfaces:**
- Produces: `daily` and `backfill` gain `--dataset <key>|all` (default `all`); resolution: `all` → `datasets.DATASET_ORDER`, else the single key (unknown key → argparse error via `choices=[*datasets.DATASETS, "all"]`). The command loops the resolved specs, each with `spec.make_fetcher()`; exit non-zero if ANY spec's status is not in the OK set. `last_run_status.json` is written from the PRIMARY (first-in-order) dataset's status — preserves the monitor/publish-gate contract.
- RUNBOOK: G1a section — manifest v2 shape, delta window, quarantine evidence assets, special-sessions file maintenance (annual, alongside holidays), `--dataset` usage.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_cli.py`:

```python
def test_parser_daily_dataset_choices():
    args = cli.build_parser().parse_args(["daily", "--dataset", "equities"])
    assert args.dataset == "equities"
    assert cli.build_parser().parse_args(["daily"]).dataset == "all"
    import pytest
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["daily", "--dataset", "bogus"])


def test_main_daily_runs_all_registered_specs(monkeypatch, tmp_path):
    from datetime import date
    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    seen = []

    def fake_run_daily(spec, target, **kw):
        seen.append(spec.key)
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    assert cli.main(["daily", "--date", "2026-07-03"]) == 0
    assert seen == ["equities"]  # DATASET_ORDER today; G1b extends this
    assert (tmp_path / "last_run_status.json").exists()
```

- [ ] **Step 2: Run to verify failure** — FAIL (`no attribute 'dataset'`)

- [ ] **Step 3: Implement** — parser: `d.add_argument("--dataset", choices=[*datasets.DATASETS, "all"], default="all")` (same on backfill). `main()` daily branch:

```python
        keys = datasets.DATASET_ORDER if args.dataset == "all" else [args.dataset]
        statuses = []
        for key in keys:
            spec = datasets.DATASETS[key]
            st = run_daily(spec, target, fetcher=spec.make_fetcher(), holidays=holidays,
                           special_sessions=special)
            statuses.append(st)
            print(manifest.status_to_dict(st))
        manifest.write_status(statuses[0], config.META_DIR)  # primary drives monitor/publish gate
        ok = ("success", "skipped_holiday", "skipped_idempotent", "not_yet")
        return 0 if all(s.status in ok for s in statuses) else 1
```

(backfill branch mirrors this loop). RUNBOOK section per the Interfaces block.

- [ ] **Step 4: Run to verify pass** — `python -m pytest -q` → all pass.

- [ ] **Step 5: Migration sanity note** — confirm by reading (no code): first G1a publish reads the live G0 manifest (v1 `files` key) via `dataset_files()` in shrink-guard and sync; new manifest v2 is written; G0-named delta-less baseline assets remain referenced (same content → same asset names) so nothing GC-eligible except superseded year files, as in any daily publish. Record this paragraph in the report.

- [ ] **Step 6: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/cli.py tests/test_cli.py RUNBOOK.md
git commit -m "feat(g1a/task-10): --dataset CLI, primary-status contract, RUNBOOK for manifest v2"
```

---

## Post-plan validation (manual, once merged)

1. Dispatch `data-daily` → confirm: v2 manifest live (`manifest_version: 2`, `baseline` + `deltas` keys), delta asset uploaded, monitor still green.
2. Re-dispatch → idempotent skip, publish gated off.
3. Confirm a quarantine file (if any day produces one) appears as a release asset and disappears ~7 days later.

## Self-review notes

- **Spec coverage:** §2.4 deltas (T5+T7+T8) · §3.4 manifest v2 registry incl. min_client_version + per-dataset schema_version/latest_date (T7) · §3.2 quarantine persistence (T6+T8) · §4 special sessions (T4) · registry mechanism for G1b's datasets (T1–T3, T8–T9) · forward-compat skip-unknown (T9). Deferred to G1b by design: universe widening + per-series gates + null-ISIN sentinel + indices/reference/ca_flags datasets + ohlc schema_version bump.
- **Type consistency:** `DatasetSpec` fields used identically in T3/T7/T8/T9; `dataset_files()` is the single v1/v2 compat point used by fakes, shrink-guard, and sync; `publish_dataset(specs=...)` and `sync_store(client, *, meta_dir, work_dir)` signatures consistent across T8/T9/T10 and chaos-suite updates.
- **Known judgment calls:** deltas are producer-emitted but not producer-synced (re-derived daily; client-only consumption); `last_run_status.json` stays single-primary to avoid breaking the monitor contract (per-dataset statuses printed to the step log); quarantine/delta release assets rely on G0's aged GC for cleanup — intended.
