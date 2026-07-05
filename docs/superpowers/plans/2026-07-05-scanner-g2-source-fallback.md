# G2 — Source Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the producer survive source failures and self-heal holes: provenance-accurate fetch results, a second independent NSE adapter (`sec_bhavdata_full`), a manual Kite day-rebuilder, a catch-up loop that repairs missed days, completeness-aware idempotency, a continuity monitor with keep-alive, a weekly source cross-check, and the deferred workflow-hardening items.

**Architecture:** `Fetcher.fetch_raw` returns a `FetchResult(frame, source)` so the per-row `source` column and `RunStatus.source` reflect the source that ACTUALLY served the day (primary or a fallback). Fallbacks become `(label, callable)` pairs on the existing fetcher chain; `sec_bhavdata_full` (different NSE endpoint, no ISIN column) keys its rows via the `reference/instruments` symbol→ISIN map — sentinel `NSE:` keys when reference is unavailable. The daily CLI walks a 7-trading-day catch-up window per fetched spec (idempotency makes present days free; a past-day 404 is a `failed`, not `not_yet`). Idempotency upgrades from "day present" to "day complete vs trailing". The monitor gains per-day continuity over the last 10 trading days and a monthly keep-alive defeats GitHub's 60-day cron auto-disable.

**Tech Stack:** unchanged (Python 3.11+, pandas, pandera, pytest filterwarnings=error, mypy --strict, ruff; `gh` via ReleaseClient). No new runtime deps (Kite rebuilder uses `requests` directly; no `kiteconnect`).

**Spec:** `docs/superpowers/specs/2026-07-05-scanner-platform-v2-design.md` §5.1 (producer-side chain: NSE → alt-route → Kite → hold-last-good), §4 (catch-up loop, continuity monitor, calendar hygiene, workflow hardening). The spec's "jugaad-data" alt-route is superseded by `sec_bhavdata_full` (jugaad wraps the same UDiFF URLs — no real redundancy; sec_bhavdata is an independent endpoint/format). Kite-in-CI stays OFF by default per the spec's manual-first decision.

**Branch:** `feat/g2-source-fallback` off `main` (111dfa9, G1b merged).

**Deferred G0/G1 items folded in (ledger):** `exists()` 404 substring → strict `"HTTP 404"` match (T8); `.tmp` orphan sweep (T8); data-monitor `if:` fires on setup failure (T7); holiday yearly refresh (T8). Explicitly NOT in G2: pandas-3.x CI matrix; `check_rowcount`/`day_symbol_count` dead-code cleanup (do in G3's touch of those files); reference-remap linking (P4a).

## Global Constraints

1. Working dir `pipeline/`; full gate before every commit: `python -m pytest -q && mypy && ruff check .` (warning-free; report the true suite count).
2. No live network in tests; `gh`/Kite/NSE all faked.
3. No dataset/source name hardcoded in shared code (registry/spec/parameter flow only; `cli.py` remains the one allowed mapping edge).
4. Manifest v2 client contract untouched. `DatasetSpec` may gain fields with defaults only.
5. Equities/indices stored data and G1b behavior unchanged except where this plan explicitly upgrades it (idempotency, provenance).
6. Tests never write outside tmp dirs (`routed_*` fixture patterns; remember: monkeypatching `datasets.DATASETS` does NOT redirect the import-time-bound BUILDERS partials — fake `cli.builders.BUILDERS` where builders would run).
7. Commit after every task with the exact message given.

---

### Task 1: `FetchResult` — provenance-accurate fetching

**Files:**
- Modify: `pipeline/src/pipeline/fetch.py`
- Modify: `pipeline/src/pipeline/daily_update.py`
- Modify: `pipeline/tests/test_fetch.py`, `pipeline/tests/test_fetch_indices.py`, `pipeline/tests/test_daily_update.py`, `pipeline/tests/test_backfill.py` (StubFetcher updates)

**Interfaces:**
- Produces (all later tasks build on this EXACTLY):

```python
@dataclass(frozen=True)
class FetchResult:
    frame: pd.DataFrame
    source: str          # provenance label of the source that actually served


class Fetcher(Protocol):
    def fetch_raw(self, d: date) -> FetchResult: ...


Fallback = tuple[str, Callable[[date], pd.DataFrame]]   # (label, fetch_fn)
```

- `NseUdiffFetcher(session=None, fallbacks: Sequence[Fallback] = ())` — primary success → `FetchResult(df, "nse-udiff")`; fallback `(label, fn)` success → `FetchResult(df, label)`. Same for `NseIndicesFetcher` with `"nse-indices"`. Primary labels come from a `primary_label` attribute set per class (`_PRIMARY_LABEL: str` class attr) — NOT hardcoded at call sites.
- `run_daily`: `res = fetcher.fetch_raw(target)`; `df = spec.normalizer(res.frame)`; then stamp actual provenance: `df["source"] = res.source` (overrides the normalizer's partial-bound default — the partial stays as the no-fallback fast default); success `RunStatus.source = res.source`.
- Every test `StubFetcher` returns `FetchResult(frame, "stub")` (or a per-test label); daily tests asserting `source == "nse-udiff"` keep passing by having stubs return that label where the assertion matters.

- [ ] **Step 1: failing tests** — update one fetch test to assert `fetch_raw(...).source == "nse-udiff"` on primary and `== "secondary-label"` when the injected fallback serves; add a daily test asserting a fallback-served day stores rows with `source == "secondary-label"` AND `RunStatus.source == "secondary-label"` (frame column checked via the store after success).
- [ ] **Step 2–4: fail → implement → pass** (mechanical sweep of StubFetchers; keep diffs minimal).
- [ ] **Step 5: full gate + commit**

```bash
git commit -m "feat(g2/task-1): FetchResult provenance — stored rows and RunStatus reflect the source that served"
```

---

### Task 2: `sec_bhavdata_full` fallback adapter

**Files:**
- Create: `pipeline/src/pipeline/sources/nse_secfull.py`
- Create: `pipeline/src/pipeline/normalize_secfull.py`
- Modify: `pipeline/src/pipeline/datasets.py` (EQUITIES `make_fetcher` wires the fallback)
- Create: `pipeline/tests/test_secfull.py`

**Interfaces:**
- `nse_secfull.build_secfull_url(d) -> f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv"`; `SECFULL_RAW_COLUMNS = ["SYMBOL", "SERIES", "DATE1", "PREV_CLOSE", "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "CLOSE_PRICE", "TTL_TRD_QNTY", "TURNOVER_LACS", "NO_OF_TRADES"]` (subset we depend on; real file has more — verified live in T9; headers/values carry stray whitespace: strip both).
- `normalize_secfull(raw, *, isin_map: Mapping[str, str] | None = None, source: str = "nse-secfull") -> canonical`:
  - strip whitespace from column names AND string values; filter nothing by series (wide-universe policy; fill null series `""`)
  - `date` ← `DATE1` strict `format="%d-%b-%Y"` (e.g. `03-Jul-2026` — VERIFY live in T9, adjust format there if reality differs)
  - prices: `PREV_CLOSE/OPEN_PRICE/HIGH_PRICE/LOW_PRICE/CLOSE_PRICE` → prevclose/open/high/low/close; `-` placeholders → coerce like the indices fix (open/high/low fill from close; close strict)
  - `volume` ← `TTL_TRD_QNTY` coerce→0 int64; `value` ← `TURNOVER_LACS × 100_000` float (lakhs → rupees, matching UDiFF's `TtlTrfVal` unit — VERIFY the UDiFF unit assumption in T9 against one real symbol's turnover both ways); `trades` ← `NO_OF_TRADES` coerce→0 int64
  - `isin` ← `isin_map.get(symbol, "")`; `instrument_key` ← isin if non-empty else `"NSE:" + symbol`; `series` from file; `source` ← param
- `datasets.py`: a module-level `def _equities_fetcher() -> Fetcher:` builds `NseUdiffFetcher(fallbacks=[("nse-secfull", _secfull_fallback)])` where `_secfull_fallback(d)` fetches+parses the CSV (reusing `fetch._fetch_with_retry` with a CSV parser) and returns the RAW frame pre-normalized to UDiFF-compatible canonical via `normalize_secfull`… **NO** — the fetcher contract returns RAW frames that `spec.normalizer` then normalizes; a fallback returning secfull-raw would hit the UDiFF normalizer and crash. RESOLUTION (implement exactly this): fallback functions return frames ALREADY in canonical form and `run_daily` must skip re-normalizing… too invasive. Simplest correct seam: the fallback fn returns a raw frame TRANSFORMED to the UDiFF raw column shape (`_secfull_to_udiff_shape(raw, isin_map) -> DataFrame with TradDt/ISIN/TckrSymb/SctySrs/OpnPric/…/SsnId="F1"/FinInstrmTp="STK"` columns) so the existing `normalize_equity_bhavcopy` consumes it unchanged. `normalize_secfull` is then just this shape-adapter (rename + isin join + placeholder handling at the raw layer); provenance still lands via Task 1's `res.source` stamp. Document this "fallbacks emit primary-raw-shaped frames" rule in `fetch.py`'s docstring — it is the fallback CONTRACT.
  - `isin_map` for production: loaded lazily inside `_secfull_fallback` from `config.REFERENCE_DIR / "instruments_all.parquet"` when present (active rows, symbol→isin); absent → `{}` + stderr note (sentinel keys, self-heals when reference lands).
- Tests: shape-adapter unit tests (whitespace, `-` placeholders, lakhs conversion, isin join hit + miss→sentinel, date format); an integration test: `NseUdiffFetcher` primary 500s ×3 → secfull fallback serves → `run_daily` succeeds with `RunStatus.source == "nse-secfull"` and stored `source` column all `"nse-secfull"`.

- [ ] **Step 1–4: TDD** → **Step 5: full gate + commit**

```bash
git commit -m "feat(g2/task-2): sec_bhavdata_full fallback — independent NSE endpoint, reference-keyed, primary-shaped"
```

---

### Task 3: Kite manual day-rebuilder

**Files:**
- Create: `pipeline/src/pipeline/sources/kite_rebuild.py`
- Modify: `pipeline/src/pipeline/cli.py` (new `rebuild-day` subcommand)
- Create: `pipeline/tests/test_kite_rebuild.py`
- Modify: `RUNBOOK.md`

**Interfaces:**
- `kite_rebuild.KiteDayRebuilder(api_key, access_token, session=None, *, sleep=time.sleep, rate_delay_s=0.35)`:
  - `instruments()` → GET `https://api.kite.trade/instruments/NSE` (CSV; auth header `X-Kite-Version: 3`, `Authorization: token {api_key}:{access_token}`) → map `tradingsymbol → instrument_token` for `segment == "NSE"` equities
  - `day_frame(d, symbols: Mapping[str, str]) -> pd.DataFrame` — for each (symbol, isin): GET `/instruments/historical/{token}/day?from={d}&to={d}` → one UDiFF-raw-shaped row (same fallback contract as Task 2: emit primary-raw shape; `SsnId="F1"`, `FinInstrmTp="STK"`, series from the reference row, prevclose from the API's previous close if absent → close of d-1 unavailable → set prevclose=open as documented degradation); rate-limited via `sleep(rate_delay_s)`; per-symbol failures collected, not fatal (report count).
- CLI: `pipeline rebuild-day --date YYYY-MM-DD` — requires `KITE_API_KEY` + `KITE_ACCESS_TOKEN` env (absent → clear error, exit 2); universe = reference instruments (active, non-INDEX) — absent reference → error exit 2 (rebuild needs the map); runs the frame through the NORMAL `run_daily` path via a one-shot `Fetcher` wrapper returning `FetchResult(frame, "kite-rebuild")`; prints the RunStatus; exit per status. NEVER wired into cron/fallback chains — manual recovery only (docstring + RUNBOOK say so).
- Tests: fully faked session (canned instruments CSV + per-token candles); rate-limit sleep called N-1 times; missing-env exit 2; missing-reference exit 2; per-symbol failure tolerance; end-to-end fake rebuild lands rows with `source == "kite-rebuild"`.
- RUNBOOK: recovery procedure (when both NSE sources are down / a hole predates archives): generate access token (link to Kite docs), run command, expected duration ~15 min for ~2400 symbols, then `pipeline publish`.

- [ ] **Step 1–4: TDD** → **Step 5: full gate + commit**

```bash
git commit -m "feat(g2/task-3): Kite manual day-rebuilder — credential-gated recovery path, never in cron"
```

---

### Task 4: Catch-up loop

**Files:**
- Modify: `pipeline/src/pipeline/cli.py`
- Modify: `pipeline/src/pipeline/config.py` (`CATCHUP_WINDOW_DAYS = 7`)
- Modify: `pipeline/src/pipeline/daily_update.py` (past-day `not_yet` mapping)
- Modify: `pipeline/tests/test_cli.py`, `pipeline/tests/test_daily_update.py`

**Interfaces:**
- CLI daily Phase 1 per fetched spec becomes: `for d in cal.trading_days_back(target, config.CATCHUP_WINDOW_DAYS, holidays, special_sessions): st = run_daily(spec, d, ...)` — ascending order; present days cost one `has_day` read (idempotent skip). Per-spec status = the TARGET day's status (the last element); catch-up-day failures are carried: if any non-target day in the window is `failed`, the spec's effective status stays the target's BUT the failure is printed and (for the primary) makes the exit code 1 (a repaired-hole failure must not silently pass). Derived phase and status files unchanged (target-day statuses).
- `run_daily` gains `is_target_day: bool = True` keyword: `NotYetPublished` maps to `not_yet` only when `is_target_day`; else `failed` with message "archive missing for past trading day" (a past-day 404 is a hole, not lateness). CLI passes `is_target_day=(d == target)`.
- Tests: a 3-day window with the middle day absent → exactly the missing day fetched + appended, others idempotent-skipped, exit 0; a past-day 404 → failed + exit 1 while the target day still ingests; window respects holidays/special sessions.

- [ ] **Step 1–4: TDD** → **Step 5: full gate + commit**

```bash
git commit -m "feat(g2/task-4): catch-up loop — missed days self-heal within a 7-trading-day window"
```

---

### Task 5: Completeness-aware idempotency

**Files:**
- Modify: `pipeline/src/pipeline/daily_update.py`
- Modify: `pipeline/src/pipeline/config.py` (`COMPLETENESS_SHORTFALL = 0.15`)
- Modify: `pipeline/tests/test_daily_update.py`

**Interfaces:**
- `run_daily`'s idempotency gate upgrades: when `has_day`, compute the day's stored TOTAL (`day_symbol_count` — this revives the G1a-era helper, note it in the commit) and the trailing per-series counts (already computed later — reorder so trailing computes before the skip decision); if stored total ≥ `(1 − COMPLETENESS_SHORTFALL) ×` trailing TOTAL mean (sum of per-series means; empty trailing → any stored rows count as complete) → `skipped_idempotent` as today; else PROCEED to re-fetch (append_keyed dedupe keep="last" makes the merge safe) with a status message noting the top-up (`message="re-ingested short day (stored N vs trailing mean M)"`).
- Tests: a deliberately truncated stored day (half the trailing mean) → re-fetches and tops up to full; a complete stored day → still `skipped_idempotent` with zero fetch calls (assert the stub fetcher was NOT called); empty trailing (fresh store) → present day skips as today.

- [ ] **Step 1–4: TDD** → **Step 5: full gate + commit**

```bash
git commit -m "feat(g2/task-5): completeness-aware idempotency — short days re-ingest and merge"
```

---

### Task 6: Weekly source cross-check

**Files:**
- Create: `pipeline/src/pipeline/crosscheck.py`
- Modify: `pipeline/src/pipeline/cli.py` (`cross-check` subcommand)
- Create: `.github/workflows/data-crosscheck.yml`
- Create: `pipeline/tests/test_crosscheck.py`

**Interfaces:**
- `crosscheck.compare_sources(primary_df, secondary_df, *, sample_n=50, tolerance=0.001, seed_symbols: list[str] | None = None) -> CrossCheckResult` — pure: joins the two CANONICAL frames on instrument_key, samples deterministically (sorted keys, every k-th — no RNG), compares close within relative tolerance; `CrossCheckResult(compared, mismatched, worst: list[tuple[key, close_a, close_b]])`.
- CLI `cross-check [--date YYYY-MM-DD]` (default: previous trading day): fetches the day from BOTH the primary UDiFF path and the secfull path (both live), normalizes both to canonical, runs `compare_sources`; mismatches > 0 → exit 1 with a printed table (the workflow's alert step opens the standard issue). No store writes.
- `.github/workflows/data-crosscheck.yml`: weekly (Saturday 03:00 UTC), same shape as data-monitor (permissions read + issues:write, alert-on-failure step, timeout 15).
- Tests: pure `compare_sources` (agreement, one mismatch, tolerance boundary, deterministic sampling); CLI wiring with stub fetchers.

- [ ] **Step 1–4: TDD** → **Step 5: full gate + commit** (YAML parse check included)

```bash
git commit -m "feat(g2/task-6): weekly source cross-check — sampled close comparison across independent NSE endpoints"
```

---

### Task 7: Continuity monitor + keep-alive

**Files:**
- Modify: `pipeline/src/pipeline/freshness.py`
- Modify: `pipeline/src/pipeline/cli.py` (`check-freshness` gains continuity)
- Modify: `.github/workflows/data-monitor.yml` (alert `if:` fires on ANY failure)
- Create: `.github/workflows/keepalive.yml`
- Modify: `pipeline/tests/test_freshness.py`, `pipeline/tests/test_cli.py`

**Interfaces:**
- `freshness.missing_days(dates_present: set[date], today: date, holidays, special_sessions=None, *, window: int = 10) -> list[date]` — pure: expected = `trading_days_back(previous_trading_day(today, …), window, …)`; return sorted expected − present.
- `cmd_check_freshness` continuity: after the existing latest-date staleness check, download the primary dataset's CURRENT-year baseline asset (resolve via manifest `dataset_files` + `asset` name; ~few MB), column-read `date`, compute `missing_days`; any missing → print them, exit 1. (Year-boundary window straddling: also download the previous-year asset when the window crosses Jan — implement via the same year-selection logic as `read_trailing_window`.)
- `data-monitor.yml`: alert step condition becomes `if: failure()` (fires even when the freshness step itself never ran — setup failures alert too; dedupe unchanged).
- `keepalive.yml`: monthly cron (1st, 05:00 UTC) + workflow_dispatch; single step: `gh workflow enable data-daily.yml data-monitor.yml data-crosscheck.yml || true` then `gh api repos/${{ github.repository }}/actions/workflows --jq '.workflows[] | select(.state != "active") | .name'` printed to the step summary (visibility if anything is disabled). `permissions: actions: write`.
- Tests: `missing_days` pure cases (hole mid-window, weekend/special-session awareness, clean window → empty); CLI continuity wiring with FakeReleaseClient (manifest + baseline parquet seeded; a missing middle day → exit 1).

- [ ] **Step 1–4: TDD** → **Step 5: full gate + commit** (YAML parse checks)

```bash
git commit -m "feat(g2/task-7): continuity monitor (holes alert, not just staleness) + cron keep-alive"
```

---

### Task 8: Workflow hardening + calendar refresh + hygiene

**Files:**
- Modify: all four `.github/workflows/*.yml` (SHA-pin actions, `persist-credentials: false`)
- Create: `.github/workflows/holidays-refresh.yml`
- Create: `pipeline/requirements.lock` (via `uv pip compile` or `pip-compile` — implementer picks what's installed, documents choice)
- Modify: workflows install `pip install -r requirements.lock -e .` (dev extras stay for CI test steps: compile a second `requirements-dev.lock`)
- Modify: `pipeline/src/pipeline/release.py` (strict `"HTTP 404"` match in `exists()`)
- Modify: `pipeline/src/pipeline/store.py` (`sweep_orphan_tmp(base, *, older_than_hours=24)` called from `append_day`/`append_keyed` entry; unlink + stderr warn)
- Modify: `pipeline/tests/test_release.py`, `pipeline/tests/test_store.py`
- Modify: `pipeline/src/pipeline/freshness.py` or `cli.py` (holidays-staleness rule: after Dec 1, `holidays.json` lacking next-year entries → check-freshness exit 1 with clear message)

**Interfaces & steps:**
- SHA-pinning: resolve each action tag to its commit SHA at implementation time (`gh api repos/{owner}/{repo}/git/ref/tags/{tag} --jq .object.sha`), pin as `uses: actions/checkout@<sha> # v7`.
- `holidays-refresh.yml`: yearly cron (Dec 1) + dispatch; opens/refreshes a `pipeline-maintenance` issue: "Refresh holidays.json + special_sessions.json for {next year} from the NSE circulars" (no scraping — manual with a nag; dedupe like the failure alerts).
- `exists()`: `if "HTTP 404" in err` (keep a fallback `" 404" in err` guard? NO — strict only; test updated; document that gh api stderr format is the contract).
- `sweep_orphan_tmp`: glob `*.parquet.tmp` under the target dir; mtime older than threshold → unlink + warn; tested with a planted stale tmp + a fresh tmp (spared).
- Holidays-staleness: pure helper `holidays_need_refresh(holidays: set[date], today: date) -> bool` (today ≥ Dec 1 AND no holiday dated next-year) wired into `cmd_check_freshness`; tests both sides of Dec 1.

- [ ] **Step 1–4: TDD where testable; YAML parse checks** → **Step 5: full gate + commit**

```bash
git commit -m "ci(g2/task-8): SHA-pinned actions, locked deps, holiday-refresh nag, 404-match + tmp-sweep hygiene"
```

---

### Task 9: Live validation (CONTROLLER-RUN — not a subagent task)

1. Live-fetch one real `sec_bhavdata_full_{DDMMYYYY}.csv`: verify URL, headers vs `SECFULL_RAW_COLUMNS`, `DATE1` format, the lakhs↔rupees turnover assumption (compare one symbol's `value` against the UDiFF file), `-` placeholder handling; run the shape-adapter + full chain end-to-end; commit calibration fixes if reality differs (precedent: T9 G1b).
2. Simulate the fallback live: run `daily --date <recent>` locally with the primary URL builder monkeypatched to 404 → confirm secfull serves, provenance lands (`source="nse-secfull"` rows).
3. Dispatch `data-daily` post-merge on a trading evening: confirm catch-up + 4-dataset publish + monitor green; then `data-crosscheck` dispatch → agreement expected.
4. Record all outcomes in the ledger; calibration commits as needed.

---

## Self-review notes

- **Spec coverage:** §5.1 chain — primary→secfull automatic, Kite manual (superseding jugaad, documented) + provenance per row (T1–T3) · §4 catch-up (T4), completeness (T5), continuity+keep-alive (T7), calendar refresh + hardening (T8) · cross-check gate (T6) · ledger-deferred hygiene (T7/T8).
- **Type consistency:** `FetchResult`/`Fallback` from T1 used by T2/T3/T6; fallback frames are PRIMARY-RAW-SHAPED (the fallback contract, documented in fetch.py) so `spec.normalizer` stays single-format; `run_daily(spec, d, *, fetcher, holidays, special_sessions=None, is_target_day=True)` final signature consistent across T4/T5 and all test updates.
- **Judgment calls:** past-day 404 = failed (hole ≠ lateness); completeness measured on TOTAL vs trailing total (per-series completeness deferred — one live short-day class at a time); cross-check is a separate weekly command (both-sources-healthy comparison), not an inline daily gate; keep-alive prints disabled-workflow visibility rather than silently re-enabling only.
