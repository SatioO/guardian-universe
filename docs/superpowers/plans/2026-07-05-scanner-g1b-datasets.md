# G1b — Datasets on the Mechanism Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put real datasets on the G1a mechanism: NSE indices as the second fetched dataset, the full cash-series universe (EQ-only filter dropped, per-series gates), and the first two *derived* datasets — `reference/instruments` (point-in-time symbol master) and `ca_flags/` (corporate-action ex-date detection) — plus the six items carried in from G1a's final review.

**Architecture:** Indices are just a second `DatasetSpec` (new source adapter + normalizer; registry does manifest/publish/sync/CLI for free — that's the G1a payoff). Universe widening replaces the total-deviation gate with per-series trailing deviation. Derived datasets get an additive `derived: bool` spec field: the CLI daily loop fetches non-derived specs, then runs `builders.py` functions for derived specs off the local store — same manifest/publish path, no fetcher. Failure semantics are resolved: publish gates on the PRIMARY dataset only; secondary failures surface via per-dataset status files and a post-publish workflow check that reddens the job without blocking a healthy equities publish.

**Tech Stack:** unchanged (Python 3.11, pandas, pandera, pytest filterwarnings=error, mypy --strict, ruff; gh Releases via ReleaseClient).

**Spec:** `docs/superpowers/specs/2026-07-05-scanner-platform-v2-design.md` §3.1–§3.3. **Deferred out of G1b:** `reference/index_constituents` (needed by P5 breadth, not scanner v1; niftyindices sourcing is its own adapter — moved to P5-prep). Full `suspended` status discrimination (needs an exchange reference feed — P4a/G2; v1 emits `active|inactive`).

**Decisions resolved (carried from G1a review):**
1. `--dataset <single non-primary>` never writes `last_run_status.json` — the primary status file is written only when `DATASET_ORDER[0]` is among the resolved keys.
2. Non-primary dataset failure does NOT block publishing the healthy primary: `daily` exits by PRIMARY status; secondaries write `last_run_status_<key>.json`; a post-publish workflow step fails the job if any secondary failed (visibility after availability).
3. `source_label` wiring: specs bind their normalizer via `functools.partial(normalize_x, source=<label>)` so the per-row `source` column and `RunStatus.source` can never diverge.
4. Indices `instrument_key` = `"IDX:" + name.upper().replace(" ", "")` per spec §3.1 (supersedes the parked P1c plan's bare upper-cased name).

**Branch:** `feat/g1b-datasets` off `main` (3f9e4d8, G1a merged). The parked `feat/p1c-indices` branch is superseded by this plan (its T4–T7 design is absorbed below) — delete it after G1b merges.

## Global Constraints

1. Working directory: `pipeline/` in `~/Desktop/projects/guardian-universe`. Full gate before every commit: `python -m pytest -q && mypy && ruff check .` (warning-free; report the true suite count from the pytest summary line).
2. No live network in tests; `gh` never invoked in tests.
3. No dataset name hardcoded in shared/dispatch code (registry only; `cli.py` maps CLI strings — the one allowed edge). `builders.py` receives specs/paths, never names.
4. The manifest-v2 CLIENT contract is FROZEN (G1a): only additive changes. `DatasetSpec` may gain fields with defaults (producer-internal), never lose/rename existing fields.
5. Equities EXISTING history must remain readable: widening changes what NEW days ingest; stored rows are untouched. `ohlc` `schema_version` bumps 1→2 in the spec registration (sentinel keys + multi-series semantics are a client-visible change).
6. Tests must never write outside tmp dirs (use monkeypatched registries/META_DIR per the established `routed_equities`/`datasets_spec` patterns).
7. Delta lists remain gap-tolerant (never assert delta-per-day completeness).
8. Commit after every task with the exact message given.

---

### Task 1: NSE indices source adapter + fetcher

**Files:**
- Create: `pipeline/src/pipeline/sources/nse_indices.py`
- Modify: `pipeline/src/pipeline/fetch.py`
- Create: `pipeline/tests/test_fetch_indices.py`

**Interfaces:**
- Produces:
  - `nse_indices.build_indices_url(d: date) -> str` = `f"https://nsearchives.nseindia.com/content/indices/ind_close_all_{d:%d%m%Y}.csv"`
  - `nse_indices.INDICES_RAW_COLUMNS: list[str]` = `["Index Name", "Index Date", "Open Index Value", "High Index Value", "Low Index Value", "Closing Index Value", "Points Change", "Volume", "Turnover (Rs. Cr.)"]` — the ONE place the header strings live (verified live in Task 9; adjust there if reality differs).
  - `fetch.NseIndicesFetcher(session=None, fallbacks=())` implementing the `Fetcher` protocol: warm `nseindia.com` (suppress transient warm-up errors), GET the CSV (plain, NOT zip) with the same 3× exponential-backoff retry; 404 → `NotYetPublished`; exhausted → `UnexpectedFailure`; parse via `pd.read_csv(io.BytesIO(resp.content))`. Extract the shared warm+retry+fallback logic from `NseUdiffFetcher` into a private `_fetch_with_retry(session, url, *, parse)` helper used by BOTH fetchers ONLY if it stays clean — otherwise duplicate minimally and note it.

- [ ] **Step 1: Write the failing tests**

Create `pipeline/tests/test_fetch_indices.py` (mirror `tests/test_fetch.py`'s fake-session pattern — read that file first and reuse its `FakeResponse`/`FakeSession` style exactly):

```python
from datetime import date

import pytest

from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.fetch import NseIndicesFetcher
from pipeline.sources.nse_indices import INDICES_RAW_COLUMNS, build_indices_url

CSV = (
    '"Index Name","Index Date","Open Index Value","High Index Value",'
    '"Low Index Value","Closing Index Value","Points Change","Volume","Turnover (Rs. Cr.)"\n'
    '"Nifty 50","03-07-2026","24500.10","24700.55","24450.00","24650.25","150.15","350000000","45000.50"\n'
    '"Nifty Bank","03-07-2026","52000.00","52500.00","51800.00","52300.75","300.75","120000000","23000.10"\n'
).encode()


def test_url_builder_encodes_ddmmyyyy():
    assert build_indices_url(date(2026, 7, 3)) == (
        "https://nsearchives.nseindia.com/content/indices/ind_close_all_03072026.csv"
    )


def test_raw_columns_cover_the_csv_header():
    header = CSV.decode().splitlines()[0]
    for col in INDICES_RAW_COLUMNS:
        assert f'"{col}"' in header


def test_fetch_parses_200_csv():
    # Build the fake session exactly like test_fetch.py does for the zip case,
    # but returning CSV bytes with status 200.
    ...


def test_404_raises_not_yet_published():
    ...


def test_repeated_500_exhausts_to_unexpected_failure():
    ...
```

(The three `...` bodies: transcribe the corresponding tests from `tests/test_fetch.py`, swapping the fetcher class, the canned bytes, and dropping zip handling. Assert the parsed frame has 2 rows and the `Index Name` column.)

- [ ] **Step 2: Run to verify failure** — `python -m pytest tests/test_fetch_indices.py -q` → FAIL (`No module named 'pipeline.sources.nse_indices'`)

- [ ] **Step 3: Implement** per the Interfaces block. `NseIndicesFetcher` differences from `NseUdiffFetcher`: URL from `build_indices_url`, response parsed with `pd.read_csv(io.BytesIO(resp.content))` (no zip). Same `_MAX_RETRIES`/`_TIMEOUT`/UA/warm-up constants.

- [ ] **Step 4: Run to verify pass**, then **Step 5: Full gate + commit**

```bash
python -m pytest -q && mypy && ruff check .
git add src/pipeline/sources/nse_indices.py src/pipeline/fetch.py tests/test_fetch_indices.py
git commit -m "feat(g1b/task-1): NSE indices source adapter + fetcher (CSV, shared retry contract)"
```

---

### Task 2: Indices normalizer

**Files:**
- Create: `pipeline/src/pipeline/normalize_indices.py`
- Create: `pipeline/tests/test_normalize_indices.py`

**Interfaces:**
- Produces: `normalize_indices(raw: pd.DataFrame, source: str = "nse-indices") -> pd.DataFrame` returning exactly `config.CANON_COLUMNS`:
  - guard: missing any `INDICES_RAW_COLUMNS` → `UnexpectedFailure` (mirror `normalize._REQUIRED_RAW` pattern)
  - `date` ← `Index Date` via `pd.to_datetime(..., format="%d-%m-%Y")` (strict format — a silent format change must fail loudly)
  - `open/high/low/close` ← the four value columns, `float` (coerce `pd.to_numeric(..., errors="coerce")` then require non-null close: rows with unparseable close are dropped here and will be counted by quarantine downstream — no, simpler and fail-loud: `astype(float)` directly; a non-numeric value raises, matching the equities normalizer's posture)
  - `prevclose` ← `close - Points Change` (float)
  - `symbol` ← `Index Name` stripped; `instrument_key` ← `"IDX:" + symbol.upper().replace(" ", "")`
  - `isin` ← `""`; `series` ← `"INDEX"`; `trades` ← `0` (int64)
  - `volume` ← `Volume` NaN→0 int64; `value` ← `Turnover (Rs. Cr.)` NaN→0.0 float
  - `source` ← `source` param

- [ ] **Step 1: Write the failing tests**

```python
import pandas as pd
import pytest

from pipeline import config
from pipeline.errors import UnexpectedFailure
from pipeline.normalize_indices import normalize_indices


def _raw() -> pd.DataFrame:
    return pd.DataFrame({
        "Index Name": ["Nifty 50 ", "Nifty Bank"],
        "Index Date": ["03-07-2026", "03-07-2026"],
        "Open Index Value": [24500.10, 52000.00],
        "High Index Value": [24700.55, 52500.00],
        "Low Index Value": [24450.00, 51800.00],
        "Closing Index Value": [24650.25, 52300.75],
        "Points Change": [150.15, 300.75],
        "Volume": [350000000.0, float("nan")],
        "Turnover (Rs. Cr.)": [45000.50, float("nan")],
    })


def test_normalize_indices_canonical():
    df = normalize_indices(_raw())
    assert list(df.columns) == config.CANON_COLUMNS
    assert df["series"].tolist() == ["INDEX", "INDEX"]
    assert df["instrument_key"].tolist() == ["IDX:NIFTY50", "IDX:NIFTYBANK"]
    assert df["symbol"].tolist() == ["Nifty 50", "Nifty Bank"]
    assert df["isin"].tolist() == ["", ""]
    assert df["trades"].tolist() == [0, 0]
    assert df["volume"].tolist() == [350000000, 0]
    assert df["prevclose"].iloc[0] == pytest.approx(24650.25 - 150.15)
    assert str(df["date"].iloc[0])[:10] == "2026-07-03"
    assert df["source"].tolist() == ["nse-indices", "nse-indices"]
    assert df["volume"].dtype == "int64" and df["trades"].dtype == "int64"


def test_normalize_indices_missing_column_fails_loud():
    with pytest.raises(UnexpectedFailure, match="missing"):
        normalize_indices(_raw().drop(columns=["Points Change"]))
```

- [ ] **Step 2–4: fail → implement → pass**, then **Step 5: Full gate + commit**

```bash
git add src/pipeline/normalize_indices.py tests/test_normalize_indices.py
git commit -m "feat(g1b/task-2): indices normalizer — IDX: slug keys, prevclose from points change"
```

---

### Task 3: INDICES registration + source wiring + second-dataset regression tests

**Files:**
- Modify: `pipeline/src/pipeline/datasets.py`
- Modify: `pipeline/src/pipeline/config.py` (`INDICES_DIR = DATA_DIR / "indices"`, `INDICES_ROWCOUNT_ABS_RANGE: tuple[int, int] = (50, 500)`)
- Modify: `pipeline/tests/test_datasets.py`, `pipeline/tests/test_sync.py`, `pipeline/tests/test_chaos.py`

**Interfaces:**
- Produces:
  - `datasets.INDICES = DatasetSpec(key="indices", file_prefix="indices", base_dir=config.INDICES_DIR, source_label="nse-indices", normalizer=functools.partial(normalize_indices, source="nse-indices"), make_fetcher=NseIndicesFetcher, abs_rowcount_range=config.INDICES_ROWCOUNT_ABS_RANGE, manifest_name="indices", schema_version=1)`
  - `EQUITIES.normalizer` also rebound via `functools.partial(normalize_equity_bhavcopy, source="nse-udiff")` (M-1 carry-in: per-row source can never diverge from `source_label`).
  - `DATASETS` gains `"indices"`; `DATASET_ORDER = ["equities", "indices"]` (equities stays primary).
  - Module docstring gains the reserved-names note (I-1 carry-in): `Reserved manifest dataset names for future phases: corporate_actions, breadth, fundamentals, reference, ca_flags. Client adjustment enum (P4b): raw | split | total_return.`
- Carry-in regression tests (M-3) — now that a real second dataset exists:
  - `test_sync.py::test_two_dataset_failure_rolls_back_everything`: registry monkeypatched to two specs (tmp base dirs); manifest with both datasets; second dataset's asset corrupt → `UnexpectedFailure` AND the FIRST dataset's file was NOT materialized (whole-manifest atomicity, in-suite at last).
  - `test_chaos.py`: extend the `_published_fixture` registry/specs to include a second spec with one baseline file + one delta (tmp dirs), so the kill-at-every-op sweep traverses a genuinely multi-dataset publish. The loop self-terminates — no constant changes expected; assert the invariant at every k as before.

- [ ] **Step 1: failing tests** (registration assertions in test_datasets.py mirroring the EQUITIES test; the two regression tests above)
- [ ] **Step 2–4: fail → implement → pass** (existing `test_main_daily_runs_all_registered_specs` asserts `seen == ["equities"]` — update to `["equities", "indices"]`; check for other DATASET_ORDER-sensitive tests)
- [ ] **Step 5: Full gate + commit**

```bash
git add src/pipeline/datasets.py src/pipeline/config.py tests/test_datasets.py tests/test_sync.py tests/test_chaos.py tests/test_cli.py
git commit -m "feat(g1b/task-3): register INDICES; bind source via partial; two-dataset regression + chaos coverage"
```

---

### Task 4: Universe widening + per-series gates

**Files:**
- Modify: `pipeline/src/pipeline/normalize.py` (drop EQ filter; sentinel keys)
- Modify: `pipeline/src/pipeline/validate.py` (per-series gate)
- Modify: `pipeline/src/pipeline/store.py` (`day_series_counts`)
- Modify: `pipeline/src/pipeline/daily_update.py` (trailing per-series)
- Modify: `pipeline/src/pipeline/config.py` (`ROWCOUNT_ABS_RANGE = (2000, 10000)`)
- Modify: `pipeline/src/pipeline/datasets.py` (`EQUITIES.schema_version = 2`)
- Modify: tests: `test_normalize.py`, `test_validate.py`, `test_store.py`, `test_daily_update.py`

**Interfaces:**
- `normalize_equity_bhavcopy`: keeps `FinInstrmTp == "STK"` and `SsnId ∈ {F1, F2}`; **drops** the `SctySrs == "EQ"` filter — all cash series stored with their `series` value. Null/empty-ISIN rows get `instrument_key = "NSE:" + symbol` sentinel instead of the ISIN (rows no longer die in quarantine for missing ISIN; the quarantine `key_ok` check keeps rejecting rows where BOTH isin and symbol are empty).
- `store.day_series_counts(base, d, *, prefix="ohlc") -> dict[str, int]` — per-series row counts for one day.
- `validate.check_rowcount_by_series(total: int, series_counts: dict[str, int], trailing: dict[str, list[int]], *, abs_range: tuple[int, int] | None = None) -> None`:
  - total outside abs_range (call-time None-sentinel to `config.ROWCOUNT_ABS_RANGE`) → fail
  - for each series with trailing data (non-empty list): deviation of today's count vs trailing mean > `config.ROWCOUNT_DEVIATION` → fail; series absent today with trailing mean ≥ 50 → fail (a major series vanishing is a truncation signal)
  - series new today (no trailing) → pass (accumulates history)
- `daily_update`: `_trailing_counts` → `_trailing_series_counts(spec, target, holidays, special_sessions) -> dict[str, list[int]]` using `day_series_counts`; `run_daily` calls `check_rowcount_by_series(len(df), today_series_counts_from_df, trailing, abs_range=spec.abs_rowcount_range)`. The old total-deviation behavior is REPLACED (the widening day would otherwise trip +80% vs EQ-only history). `check_rowcount` (total-only) stays for any external callers but `run_daily` stops using it.
- Migration property (test it): a widened day (~2× rows, new series) against EQ-only trailing history PASSES (EQ series deviation small; new series exempt; total within new abs range); a TRUNCATED file (EQ count halved) still FAILS.

- [ ] **Step 1: failing tests** (normalize: BE/BM rows survive with their series; null-ISIN row gets `NSE:SYM` key and survives; validate: the migration property + truncated-EQ failure + vanished-major-series failure; store: `day_series_counts`; daily_update: end-to-end widened-day success against EQ-only trailing fixture)
- [ ] **Step 2–4: fail → implement → pass** — several existing tests assert EQ-only filtering (`test_normalize.py` multi-series drop test) — INVERT their assertions deliberately (this is the spec change); the mixed-session and STK-filter tests stay.
- [ ] **Step 5: Full gate + commit**

```bash
git add src/pipeline/normalize.py src/pipeline/validate.py src/pipeline/store.py src/pipeline/daily_update.py src/pipeline/config.py src/pipeline/datasets.py tests/
git commit -m "feat(g1b/task-4): full cash-series universe — per-series gates, NSE: sentinel keys, ohlc schema_version 2"
```

---

### Task 5: Derived-dataset mechanism + failure-semantics decisions

**Files:**
- Modify: `pipeline/src/pipeline/datasets.py` (`derived: bool = False` field — additive, default preserves all existing constructions)
- Modify: `pipeline/src/pipeline/cli.py`
- Modify: `pipeline/src/pipeline/manifest.py` (`write_status(status, meta_dir, *, filename="last_run_status.json")`)
- Modify: `pipeline/tests/test_cli.py`, `pipeline/tests/test_datasets.py`

**Interfaces:**
- `DatasetSpec.derived: bool = False`. Derived specs: `normalizer`/`make_fetcher` never called by the CLI (register with `normalizer=lambda df: df` and a `make_fetcher` that raises `RuntimeError("derived dataset has no fetcher")` — plus a test asserting the CLI never calls them).
- CLI `daily` loop becomes two phases:
  1. FETCHED specs (in `DATASET_ORDER`, filtered `not spec.derived`, respecting `--dataset`): as today.
  2. DERIVED specs (only when running `all` AND the primary status is in the OK set): call `builders.BUILDERS[spec.key](spec, target)` (Task 6/7 populate the registry; this task lands the loop with an empty `BUILDERS` dict in a new `builders.py` stub module — `BUILDERS: dict[str, Callable[[DatasetSpec, date], RunStatus]] = {}`).
- `--dataset <derived-key>` (e.g. a lone `--dataset reference`) is NOT supported in v1: the CLI prints a clear error ("derived datasets build automatically after a successful `--dataset all` run") and exits 2. Test this.
- Status files (decision 2): primary status → `last_run_status.json` **only if** `DATASET_ORDER[0]` is among the resolved keys (decision 1 guard); every OTHER spec (fetched or derived) → `last_run_status_<key>.json`. Exit code: PRIMARY status drives it when primary ran; a run not including the primary exits by its own statuses. Non-primary failures never fail the run when the primary succeeded (workflow surfaces them — Task 8).
- `manifest.write_status` gains the `filename` keyword (default preserves all callers).

- [ ] **Step 1: failing tests** — parser unchanged; new tests: derived spec skipped by the fetch loop (fake registry with a derived spec whose make_fetcher raises if called); primary+secondary status files written; `--dataset indices` does NOT write `last_run_status.json` but DOES write `last_run_status_indices.json`; secondary failure → exit 0 when primary succeeded, and vice-versa primary failure → exit 1.
- [ ] **Step 2–4: fail → implement → pass**
- [ ] **Step 5: Full gate + commit**

```bash
git add src/pipeline/datasets.py src/pipeline/cli.py src/pipeline/manifest.py src/pipeline/builders.py tests/test_cli.py tests/test_datasets.py
git commit -m "feat(g1b/task-5): derived-dataset mechanism; per-dataset status files; primary-only gate guard"
```

---

### Task 6: `reference/instruments` builder + spec

**Files:**
- Modify: `pipeline/src/pipeline/builders.py` (stub → real)
- Modify: `pipeline/src/pipeline/datasets.py` (REFERENCE spec)
- Modify: `pipeline/src/pipeline/config.py` (`REFERENCE_DIR = DATA_DIR / "reference"`)
- Create: `pipeline/tests/test_builders_reference.py`

**Interfaces:**
- `datasets.REFERENCE = DatasetSpec(key="reference", file_prefix="instruments", base_dir=config.REFERENCE_DIR, source_label="derived", normalizer=<identity>, make_fetcher=<raiser>, abs_rowcount_range=(0, 10**9), manifest_name="reference", schema_version=1, derived=True)`; appended to `DATASETS`/`DATASET_ORDER` (after indices).
- `builders.build_reference(spec: DatasetSpec, target: date) -> RunStatus`:
  - Reads the EQUITIES store (column-pruned: date, instrument_key, isin, symbol, series) across all years via the primary spec resolved from the registry — NO: constraint 3 forbids name lookups in builders; instead `build_reference(spec, target, *, source_spec: DatasetSpec)` and the CLI passes `DATASETS[DATASET_ORDER[0]]`. (cli is the allowed edge.)
  - Emits SCD2 rows: one row per distinct `(instrument_key, symbol, series)` version: columns `instrument_key, isin, symbol, name (=symbol, v1), series, first_seen, last_seen, status, valid_from (=first_seen), valid_to (=last_seen), date (=last_seen — REQUIRED: the manifest's latest_date machinery reads a "date" column)`. `status`: `active` if `last_seen` within 10 trading days of the store's max date, else `inactive` (v1 subset of the spec's status enum — `suspended`/`delisted` need an exchange feed, deferred; docstring says so).
  - Writes `instruments_all.parquet` (atomic tmp+replace) in `spec.base_dir` — glob-compatible with `{file_prefix}_*.parquet`. Full rewrite each run (idempotent; ~few thousand rows).
  - Returns `RunStatus("success", target, symbol_count=<row count>, source="derived")`; any exception → the CLI's builder wrapper maps to a failed secondary status (never raises out).
  - Registered: `BUILDERS["reference"] = build_reference` (partial-bound source_spec in cli, or two-arg call from cli — pick the simplest signature that keeps builders name-free).
- Rename/renumber safety test: same `instrument_key` appearing with two symbols across dates yields TWO rows with correct valid_from/valid_to windows.

- [ ] **Step 1: failing tests** — seed a tmp equities store (3 days, one key renamed on day 3, one key absent for >10 sessions given a longer fixture, one sentinel `NSE:` key); assert row shapes, status values, rename windows, `date` column present; assert `build_manifest([REFERENCE-with-tmp-base])` picks the file up with a correct `latest_date`.
- [ ] **Step 2–4: fail → implement → pass**
- [ ] **Step 5: Full gate + commit**

```bash
git add src/pipeline/builders.py src/pipeline/datasets.py src/pipeline/config.py tests/test_builders_reference.py
git commit -m "feat(g1b/task-6): reference/instruments derived dataset — SCD2 symbol master from store presence"
```

---

### Task 7: `ca_flags/` detector builder + spec

**Files:**
- Modify: `pipeline/src/pipeline/builders.py`
- Modify: `pipeline/src/pipeline/datasets.py` (CA_FLAGS spec)
- Modify: `pipeline/src/pipeline/config.py` (`CA_FLAGS_DIR = DATA_DIR / "ca_flags"`, `CA_DISCONTINUITY_THRESHOLD = 0.005`)
- Modify: `pipeline/src/pipeline/store.py` (generalize `_read_year`'s empty-frame columns; add `append_keyed(df, base, *, prefix, key_cols=("date", "instrument_key"))` — `append_day` becomes a thin wrapper)
- Create: `pipeline/tests/test_builders_ca_flags.py`
- Modify: `pipeline/tests/test_store.py` (append_keyed with non-canonical columns)

**Interfaces:**
- `datasets.CA_FLAGS = DatasetSpec(key="ca_flags", file_prefix="ca_flags", base_dir=config.CA_FLAGS_DIR, source_label="derived", normalizer=<identity>, make_fetcher=<raiser>, abs_rowcount_range=(0, 10**9), manifest_name="ca_flags", schema_version=1, derived=True)`; appended to registry (after reference).
- `builders.build_ca_flags(spec, target, *, source_spec) -> RunStatus`:
  - For `target`: join today's `(instrument_key, prevclose)` against the previous trading day's `(instrument_key, close)` from the source store (both from `read_trailing_window` or direct year reads; only keys present BOTH days).
  - Flag rows where `abs(prevclose_today / close_prev - 1) > config.CA_DISCONTINUITY_THRESHOLD`: columns `date (=target), instrument_key, close_prev, prevclose_today, implied_ratio (= close_prev / prevclose_today)`.
  - Append via `store.append_keyed(flags, spec.base_dir, prefix=spec.file_prefix)` — year-partitioned, deduped on (date, instrument_key), idempotent.
  - Zero flags → still success (`symbol_count=0`); a day with no previous day in store (first backfill day) → success with 0 flags.
- `store` generalization: `_read_year` empty-case columns come from the incoming frame (parameter), not `config.CANON_COLUMNS`; `append_keyed` is column-agnostic (needs only `date` + the key cols). `append_day` delegates: `append_keyed(df, base, prefix=prefix)`. All existing store tests must pass unchanged.
- Detector fixture test: a 2:1 split (close_prev=1000, prevclose_today=500) → flagged with implied_ratio 2.0; a normal day (prevclose == close_prev) → not flagged; a 0.4% drift → not flagged.

- [ ] **Step 1: failing tests** → **Step 2–4: fail → implement → pass** → **Step 5: Full gate + commit**

```bash
git add src/pipeline/builders.py src/pipeline/datasets.py src/pipeline/config.py src/pipeline/store.py tests/test_builders_ca_flags.py tests/test_store.py
git commit -m "feat(g1b/task-7): ca_flags derived dataset — prevclose-discontinuity ex-date detector"
```

---

### Task 8: Workflow secondaries check + docs

**Files:**
- Modify: `.github/workflows/data-daily.yml`
- Modify: `RUNBOOK.md`
- Modify: `docs/superpowers/specs/2026-07-05-scanner-platform-v2-design.md` (I-2 reconcile: annotate the §3.4 example's `"schema_version": 2` as illustrative — actual value tracks each dataset's registration and bumps only on client-visible change)

**Interfaces:**
- Workflow: after the publish step, add:

```yaml
      - name: Surface secondary-dataset failures
        if: always() && steps.decide.outputs.status == 'success'
        run: |
          bad=0
          for f in data/meta/last_run_status_*.json; do
            [ -e "$f" ] || continue
            s=$(jq -r '.status // "missing"' "$f")
            case "$s" in success|skipped_holiday|skipped_idempotent|not_yet) ;; *)
              echo "::error::secondary dataset failed: $f status=$s"; bad=1 ;;
            esac
          done
          exit $bad
```

  (Runs AFTER publish so a healthy primary always publishes; job still goes red on a secondary failure → the existing Alert-on-failure step fires.)
- RUNBOOK: G1b section — indices dataset; widened universe + per-series gates; derived datasets (reference/ca_flags) and their status files; the primary-vs-secondary publish semantics (decisions 1+2, explicitly incl. `--dataset <non-primary>` never touching the primary status file); ca_flags meaning for clients (exclude flagged instruments from level-based scans until P4b adjusts).

- [ ] **Step 1: implement** (YAML-parse check: `python -c "import yaml; yaml.safe_load(open('.github/workflows/data-daily.yml'))"`), **Step 2: full gate** (unchanged Python — run anyway), **Step 3: commit**

```bash
git add .github/workflows/data-daily.yml RUNBOOK.md docs/superpowers/specs/2026-07-05-scanner-platform-v2-design.md
git commit -m "ci(g1b/task-8): secondary-failure surfacing after publish; RUNBOOK for datasets; spec example reconciled"
```

---

### Task 9: Live calibration checkpoint (CONTROLLER-RUN — not a subagent task)

No plan code. The controller (main session) runs, post-merge or on-branch via workflow_dispatch:
1. Fetch one real `ind_close_all_{DDMMYYYY}.csv` (local machine): verify URL, exact headers vs `INDICES_RAW_COLUMNS`, real row count → finalize `INDICES_ROWCOUNT_ABS_RANGE` (commit a calibration fix if needed — precedent: the equities (1800,3000) live fix).
2. Run `pipeline daily --date <recent trading day>` locally against real NSE: confirm the widened equities day passes per-series gates and records the true all-series row count → confirm `(2000, 10000)`.
3. Dispatch `data-daily` on the branch or post-merge: confirm 4-dataset manifest (ohlc, indices, reference, ca_flags), deltas for both fetched datasets, monitor green.
4. Record outcomes in the ledger; adjust constants via a `fix(g1b): calibrate ...` commit if reality differs.

---

## Self-review notes

- **Spec coverage:** §3.1 indices adapter (T1–T3) + all-series universe + per-series gates (T4) + sentinel keys (T4) · §3.3 reference/instruments (T6, constituents deferred to P5-prep with rationale) + ca_flags (T7) · §3.4 schema_version bump (T4) + example reconcile (T8) · carry-ins I-1 (T3), I-2 (T8), M-1 (T3), M-3 (T3), decisions 1+2 (T5+T8).
- **Type consistency:** `DatasetSpec.derived` default False everywhere; `builders.BUILDERS: dict[str, Callable[..., RunStatus]]` registered in T6/T7 against the T5 stub; `write_status(..., filename=...)` default preserves all pre-T5 callers; `append_keyed` wraps `append_day` semantics without behavior change.
- **Judgment calls:** per-series deviation REPLACES total deviation in run_daily (migration-day necessity, tested); derived builders run only on `--dataset all` with healthy primary (a lone `--dataset reference` run is not supported in v1 — argparse still accepts the key for future use; the loop simply finds no fetched specs and runs the builder IF primary status file... NO — keep it simple: `--dataset <derived-key>` errors out with a clear message in T5; document); reference `date` column = last_seen keeps manifest machinery untouched.
