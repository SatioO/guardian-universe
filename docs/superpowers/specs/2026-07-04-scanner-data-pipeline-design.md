# Scanner Data Pipeline — Design Spec

**Date:** 2026-07-04
**Status:** Draft for review
**Author:** Vaibhav Satam (with Claude)
**Scope:** The EOD market-data ingestion, distribution, and client-cache layer that feeds a future in-app stock **scanner** for NSE + BSE equities and indices.

---

## 1. Purpose & context

We want to build a scalable, loosely-coupled stock **scanner** (screen thousands of criteria over the full NSE/BSE universe). A scanner is only as good as its data layer, so this spec covers **just the data pipeline** — where EOD data comes from, how it is produced, distributed, cached, validated, and served — as a self-contained, independently-shippable subsystem.

We studied [PKScreener](https://github.com/pkjmesra/PKScreener) / PKBrokers as a reference. Their core idea — **produce data centrally once, distribute cheap snapshots to credential-free clients** — is sound. Their execution (Python God-class, uncompressed pickle of the whole universe loaded into RAM, git branch as a database, no validation/versioning) is not. We take the idea and rebuild it properly.

### Why we are NOT PKScreener
- PKScreener's users have **no broker access**, so one person's Kite credentials feed everyone via GitHub snapshots. **Our users each connect their own broker (Kite/Dhan).** We do not need a central credential-holding producer just to *give* users price data.
- There is a **cleaner bulk source than broker APIs: exchange bhavcopy.** NSE/BSE publish official daily EOD files — the entire market's OHLCV in one file per trading day — free, public, redistributable, no broker credentials. Backfilling the universe is ~250 files/year, one file/day to maintain — not N×2000 API calls.

## 2. Goals & non-goals

### Goals
- A **fault-tolerant, self-healing, observable** EOD data pipeline that guarantees the user always has the latest validated EOD data.
- **Completely decoupled** from the existing chart historical-data path — a separate module, adopted elsewhere later only by opt-in.
- **Loosely coupled, testable, flexible** — mirror the `src/watchlist/` hexagonal rearchitecture and its fake-Clock/fake-feed testing framework.
- **Every failure scenario tested and hooked into CI**; the running pipeline validates itself and alerts only on genuine anomalies (solo-maintainer friendly).
- **Extensible data structure** so future datasets (market breadth, features, fundamentals) slot in with zero changes to ingestion or existing clients.
- Solve an existing pain as a side effect: charts/watchlist re-fetching historical data from brokers repeatedly (future opt-in adoption of this cache).

### Non-goals (v1 — explicitly deferred)
- **Corporate-action price adjustment** — v1 stores raw (unadjusted) OHLCV; adjustment is phase 2 (design below keeps the seam ready).
- **Intraday / real-time** breadth or scanning — EOD only. Live/intraday is a later per-user broker path.
- **Market-breadth metrics** (A/D ratio, %>EMA, net new highs/lows) — these are a *downstream consumer* of this pipeline, not part of it. The data structure is built to enable them later.
- **BSE-only stocks not listed on NSE** — v1 covers the NSE EQ universe + indices; BSE bhavcopy ingestion is designed-for but its unique-symbol subset is deferred.
- **Central feature precomputation** — features (EMA, 52w high/low, etc.) are computed client-side per scan for flexibility.

## 3. Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Topology | **Hybrid: serverless central baseline + local top-up** | Central authority for correctness; local cache for performance/offline; no always-on server |
| Producer host | **GitHub Actions (Python)** | Isolated in CI; best-in-class bhavcopy/pandas/parquet libs; never runs in the app |
| Distribution | **CDN artifacts (GitHub Releases + jsDelivr), not git-committed blobs** | Real versioning, checksums, atomicity; no repo bloat |
| Primary source | **NSE UDiFF CM bhavcopy** (+ BSE bhavcopy) | Free, redistributable, whole-market, no creds |
| Fallback sources | **jugaad-data / nsefin → Kite (backup)** | Defense in depth; exchange-official stays canonical |
| Storage format | **Parquet, partitioned by year, long format, zstd** | Columnar, compressed, git/CDN-friendly, append-per-year |
| Identity key | **`instrument_key`** = ISIN (equities) / stable index code (indices); `symbol` is display | Survives renames/reuse/mergers; indices have no ISIN |
| Client module | **Rust, hexagonal, mirrors `src/watchlist/`** | Performance (rayon/columnar); consistent, testable pattern |
| Separation | **Independent from `src-tauri/src/historical/`** | Scanner-specific; opt-in adoption elsewhere later |

## 4. Architecture — three loosely-coupled parts, one contract

```
┌── PRODUCER (Python, GitHub Actions — CI-only, isolated) ───────────────┐
│  fetch_bhavcopy(date) → filter EQ + F-session → validate/quarantine     │
│  → append parquet(by year, ISIN-keyed) → manifest + last_run_status      │
│  sources: NSE UDiFF ▶ jugaad-data/nsefin ▶ Kite (backup, CI creds)       │
└───────────────────────────────┬────────────────────────────────────────┘
                    publishes    │   THE ONLY INTERFACE:
                                 ▼   parquet column schema + manifest.json (versioned, checksummed)
┌── DISTRIBUTION — CDN (GitHub Releases + jsDelivr; multi-mirror) ─────────┐
│  baseline snapshot · daily delta · reference (holidays, ISIN map) · manifest │
└───────────────────────────────┬────────────────────────────────────────┘
                    syncs        │
                                 ▼
┌── CLIENT — NEW scanner_data module (Rust, hexagonal) ───────────────────┐
│  sync+verify → local columnar cache → getCandles() → scanner feature/compute │
│  fallback: CDN delta ▶ baseline ▶ mirror ▶ direct exchange ▶ user's broker    │
│  *** completely separate from src-tauri/src/historical/ (charts) ***          │
└─────────────────────────────────────────────────────────────────────────┘
```

The **only coupling** between producer and app is the **artifact contract** (parquet column schema + `manifest.json`). The producer can be rewritten in any language without touching the client.

## 5. Producer pipeline

### 5.1 Daily update workflow (recurring, GitHub Actions)

Two scheduled runs (19:30 & 21:30 IST → 14:00 & 16:00 UTC; second is a no-op if first succeeded).

1. **Resolve target trading date** (IST "today").
2. **Trading-day gate** — `is_trading_day(date)` via `holidays.json` + weekend check. Non-trading day → log "skip", **exit 0 (green)**, no artifacts.
3. **Idempotency check** — already published for `date`? → log "idempotent skip", exit 0. (Absorbs the second cron.)
4. **Acquire prior state** — download latest published `ohlc_{year}.parquet` + `manifest.json` from CDN to append onto.
5. **Fetch** — `fetch_bhavcopy(date)` (§5.2). Branch on **typed errors**:
   - `NotYetPublished` → first cron: exit 0 (let 21:30 retry); second cron past cutoff: **ALERT** (late).
   - `UnexpectedFormat` / unexpected 404 (not in holiday list) → **FAIL loudly + ALERT**.
6. **Filter** — equity file: `SERIES == 'EQ'` only; **F-session rows only** (drop I1/I2 pre-open/interim — silent-corruption trap since Nov 2025). Indices file: kept as-is, tagged `series='INDEX'`.
7. **Normalize** — canonical long schema (§6), key by ISIN, tag `source` (provenance).
8. **Validate (fail-closed)**:
   - Schema/dtypes present (pandera).
   - Row count in ~1,800–2,200 **and** deviation <15% vs trailing-10-day average (else FAIL — signals partial file / format break).
   - Per-row quarantine: `price≤0`, negative volume, missing ISIN, `high<low`, `close∉[low,high]` → move to quarantine log, count, proceed with clean rows.
9. **Append + dedupe** — append to correct `ohlc_{year}.parquet`; `drop_duplicates(subset=[date, instrument_key], keep='last')`; sort. Year-boundary aware.
10. **Corporate actions** — phase 1: raw only (documented limitation). Optionally record NSE CA events into `corporate_actions/` now for phase-2 use.
11. **Build outputs** — `delta_{date}.parquet`; update `manifest.json` (latest_trading_date, per-file sha256, schema_version, sizes/counts); update `last_run_status.json` (status, symbol_count, quarantined_count, deviation flags, source_used, duration).
12. **Publish (atomic)** — upload artifacts to CDN (Release + jsDelivr), then **flip the `manifest.json` "latest" pointer LAST** so clients never see a half-updated set.
13. **Alert on failure only** — status=failed or flagged → webhook/Slack/issue. Success/holiday = silent.

Properties: **idempotent, fail-closed, trading-calendar-driven**.

### 5.2 `fetch_bhavcopy(date)` adapter (the single isolated source boundary)

All NSE URL/format/anti-bot quirks live here so a format change is a one-file fix.

1. **Warm session** — GET nseindia.com homepage with a browser `User-Agent`, collect cookies.
2. **Build archive URL** from date — the ONLY place the UDiFF filename pattern is encoded (NSE renamed it in the 2024 UDiFF migration and again Oct 2025 to four-digit year).
3. **GET** the CSV.zip with UA + cookies; retry 3× exponential backoff on transient (timeout/5xx/conn-reset).
4. Success → unzip → parse CSV → DataFrame.
5. Fail after retries → **Fallback #1**: `jugaad-data`/`nsefin` wrapper.
6. Fail → **Fallback #2**: Kite historical API (CI service creds) for that day.
7. All fail → raise **typed error** (`NotYetPublished` vs `UnexpectedFailure`) for the caller to branch on.

**Indices:** the same adapter also fetches the NSE **indices bhavcopy** — a separate daily file (indices are not in the equity bhavcopy and carry no `series`/ISIN). Index rows are tagged `series='INDEX'` and keyed by a stable index code via `instrument_key`.

### 5.3 Backfill workflow (`backfill.py`, one-time, run locally not CI)

1. Target = **300 trading days** back (holiday-aware count) — guarantees ≥252 clean days for the 52-week window plus runway so EMA-200 isn't at its raw seed.
2. For each trading day ascending: already in parquet → skip (resumable); else fetch (with polite delay to avoid NSE burst-blocking), validate per-day row count (~1,800–2,200), append, checkpoint to disk.
3. Write `ohlc_{year}.parquet` files + initial manifest.
4. **Publish the baseline snapshot** to CDN (the bootstrap clients download once).

Runs from a residential IP (CI IPs get blocked on 300 rapid requests) and is crash-resumable.

## 6. Data model & extensibility

### 6.1 Canonical OHLC schema (long format, one row per date×ISIN)

`date, instrument_key, isin, symbol, series, open, high, low, close, prevclose, volume, value, trades, source`

- **`instrument_key`** is the canonical primary-key component (`(date, instrument_key)`): `ISIN` for equities, a stable exchange **index code** for indices (which have no ISIN). `symbol` is a mutable display label — never join/dedupe on `symbol` alone; `isin` is nullable (empty for indices).
- `source` records provenance (`nse-udiff` / `nse-indices` / `jugaad` / `kite`).
- Parquet, **partitioned by calendar year** (`ohlc_2025.parquet`, `ohlc_2026.parquet`), zstd-compressed. Not one-file-per-symbol (git-hostile) nor one unpartitioned all-time file (rewrites everything daily).

### 6.2 Layered datasets + manifest registry (the flexibility mechanism)

```
datasets/
  ohlc/               ← canonical base: raw EOD OHLCV        [ingestion owns]
  corporate_actions/  ← CA events (phase 2)                  [ingestion owns]
  reference/          ← symbol master, ISIN map, index constituents, holidays
  breadth/            ← FUTURE: A/D, %>EMA, net new H/L       [separate consumer job]
  features/           ← FUTURE: precomputed indicators        [separate consumer job]
  fundamentals/       ← FUTURE                                [separate source]
manifest.json → { schema_version, datasets: [ { name, schema_version, files[], sha256, latest_date } … ] }
```

Three properties:
1. **Base vs derived separation.** `ohlc/` is the single source of truth. **Market breadth becomes a new consumer job** that reads `ohlc/`, emits `breadth/`, registers in the manifest — ingestion pipeline and client don't change.
2. **Additive schema evolution.** New columns are added, never repurposed; `schema_version` + backward-compatible reads mean new columns never break old clients.
3. **Manifest as a registry.** Clients consume datasets they understand and **ignore ones they don't** (forward-compatible) — publishing `breadth/` can't break a client that predates it. ISIN lets any future dataset join to OHLCV.

## 7. Corporate actions strategy (phase 2, seam ready in v1)

Raw bhavcopy is unadjusted; a 2:1 split looks like a −50% crash and breaks every MA/52w/VCP scanner on the ex-date. The chosen pattern: **store raw prices + a corporate-actions/adjustment-factor table; derive the adjusted series on read.**

- Two layers: (a) raw OHLCV (never lossy), (b) `corporate_actions` table `(isin, ex_date, type, ratio)`.
- Source: NSE/BSE official corporate-actions feed; broker/Yahoo adjusted series as cross-check only.
- Derive: cumulative back-adjustment factor per ISIN, applied to rows before each ex-date. Splits/bonuses always; dividend adjustment a toggle. Scanner requests raw or adjusted per criterion.
- A wrong adjustment is a **one-row fix** in the factor table; every series re-derives — no re-download.

v1: document unadjusted as a known limitation; do not let a CA distortion fail the run (per-symbol data-quality issue, not run-blocking).

## 8. Client `scanner_data` module (Rust, hexagonal — mirrors `src/watchlist/`)

New module, structurally identical to the watchlist rearchitecture, **independent from the chart path**.

```
src-tauri/src/scanner_data/
  domain/      pure: candle model, feature calc (EMA/252-hi-lo/…), reducers/selectors
               (recompute-from-history, never carry incremental state) — heavy unit tests
  ports/       traits: Clock · SnapshotSource (CDN fetch+verify) · LocalStore (columnar r/w) · CalendarProvider
  infra/       real: CdnSnapshotSource, ParquetLocalStore, SystemClock, NseCalendar
  infra/fakes/ FakeClock, InMemorySnapshotSource(canned parquet), InMemoryLocalStore, StaticCalendar
  controller/  ScannerDataStore: sync() / status() / getCandles() — command-style, no-op-guard, emits on change
  __tests__/   makeStore() helper + scenario fixtures (see §10)
```

- **Ports = injection seams** exactly like `WatchlistRepository`/`Clock`. Tests wire fakes via `makeStore()`; production wires real adapters. No network in tests.
- **Stateless/self-healing compute** — features recomputed from stored history each scan (the anti-incremental-EMA principle), sub-second over ~2000×300 with rayon.
- **`getCandles()` deliberately generic** so charts/watchlist *could* adopt it later — but nothing is wired into them now.

### 8.1 Client sync loop

```
on launch / schedule / before a scan:
  read manifest.json → local latest < manifest latest_trading_date?
    yes → pull missing delta_{date}.parquet (or baseline if too far behind / cache corrupt)
        → verify sha256 → atomically append to local columnar cache
    no  → already fresh
  fallback if CDN unreachable: Release URL ▶ jsDelivr ▶ direct NSE ▶ user's own broker
```

## 9. Resilience — failure scenarios & mitigations

Two independent fallback dimensions mean no single failure leaves a user without data; worst case is slower or one day stale, with the UI showing true freshness.

```
PRODUCER (build the day)                 CLIENT (obtain the day)
  bhavcopy → jugaad/nsefin → Kite →        CDN delta → CDN baseline → mirror →
  hold-last-good + ALERT                   direct exchange → user's OWN broker
```

| # | Scenario | Mitigation |
|---|---|---|
| 1 | Bhavcopy late / not yet published | Backoff-retry to deadline → Kite fallback; 2nd cron absorbs late days |
| 2 | Bhavcopy schema change / truncated | Schema + row-count validation; reject + alert + fallback |
| 3 | Bad ticks / outliers | Outlier gate (unexplained >25% vs prevclose w/o a CA on record — threshold tunable); quarantine + flag |
| 4 | NSE blocks CI IP / needs cookies+UA | Browser-like warm session + retries + fallback lib (NSE hostile to server IPs) |
| 5 | Kite token expired in CI | Automated TOTP login + alert; else hold-last-good; client self-heals from own broker |
| 6 | Missed / late / wrong corporate action | Gap-vs-CA cross-check; correction channel republishes one factor row |
| 7 | Symbol rename / ISIN change / merger | Key by ISIN; symbol is a mutable label |
| 8 | Delisting | Freeze + mark inactive; keep history (avoids backtest survivorship bias) |
| 9 | Suspension / halt (absent that day) | Mark no-trade; never fabricate a candle |
| 10 | CDN outage / stale edge | Multi-mirror; ETag/conditional; client fallback chain |
| 11 | Partial / corrupt download | Per-file + manifest sha256; atomic apply; re-download |
| 12 | Client/bundle schema-version skew | Version negotiation; support N and N−1; forward-compatible parse |
| 13 | Local store corruption / disk full / power loss | Atomic temp+rename, checksums, rebuild-from-central; graceful disk-full |
| 14 | Client offline for days | Delta-chain catch-up; baseline reset if gap too large |
| 15 | Concurrent windows / scan-during-sync | Single-writer, snapshot reads |
| 16 | Clock skew / wrong device TZ | UTC canonical; IST trading calendar central; ignore device clock for data logic |
| 17 | Holidays / Muhurat special session / half-days | Authoritative trading calendar; freshness logic calendar-aware |
| 18 | Series/segment (EQ vs BE/BZ) | Capture `series`; scanners exclude surveillance/illiquid |
| 19 | IPOs (short history) + index composition changes | Handle short series gracefully; versioned index-constituent membership |

## 10. Testing strategy — every scenario, CI-gated (no manual testing)

**Solo-maintainer principle:** two automated safety layers — exhaustive CI before anything ships, and the running pipeline validating itself in production — plus a dead-man's-switch for silent non-runs. **The pandera schema + row-count/quarantine logic is one module used in BOTH the unit tests and the live runtime gate** (tests = production validation).

Injectable seams (fetch adapter, clock, calendar) → deterministic tests with canned fixtures, no live network (`responses`/`vcrpy`, `freezegun`).

| Scenario | Fixture | Test | Layer |
|---|---|---|---|
| Normal trading day | EQ+F bhavcopy CSV | integration | CI |
| Weekend / holiday (no file) | date + holidays.json | `is_trading_day` unit + skip-path | CI |
| Late / not-yet publish | mocked 404 | typed-error → 1st-cron exit-0 | CI |
| Unexpected 404 / format break | malformed zip | fail-loud + alert | CI + prod gate |
| Truncated / partial file | short fixture | row-count deviation >15% fires | CI + prod gate |
| Pre-open I1/I2 rows present | mixed-session | F-session filter | CI + prod gate |
| Non-EQ series present | multi-series | EQ filter | CI + prod gate |
| New listing <200d | short-history symbol | feature null/excluded | CI |
| Corporate-action day | split fixture | no crash; only that symbol distorts | CI |
| Delisting | day N vs N+1 | denominator policy | CI |
| Rename / ISIN continuity | same ISIN, new symbol | join on ISIN | CI |
| Zero-vol / unchanged | flat fixture | explicit unchanged bucket | CI |
| Divide-by-zero (declines==0) | one-sided | null, never Infinity | CI |
| Bad row (neg vol, high<low) | dirty fixture | quarantine + keep rest | CI + prod gate |
| Year-boundary window | cross-year | trailing window across files | CI |
| Idempotent re-run | run twice | no dup, no-op | CI + prod idempotency |
| NSE blocks / UA missing | mocked block | fallback chain engages | CI |
| Corrupt/partial CDN download | bad sha256 | client re-download / self-heal | CI (Rust) |

Test types: pure-domain unit tests, fixture integration tests, pandera data-contract tests, golden/characterization tests, optional `hypothesis` property tests for dedupe/append invariants. Branch protection requires the full matrix green before merge.

## 11. CI/CD — GitHub Actions best practices

Four single-purpose workflows:

| Workflow | Trigger | Purpose | permissions |
|---|---|---|---|
| `ci.yml` | PR, push | ruff + mypy + pytest + coverage → blocks merge | `contents: read` |
| `data-daily.yml` | schedule ×2 (14:00 & 16:00 UTC) + dispatch | the pipeline; publish | `contents: write`, `issues: write` |
| `data-monitor.yml` | schedule (post-cutoff) | freshness dead-man's-switch | `issues: write` |
| `backfill.yml` | dispatch only | manual bootstrap | `contents: write` |

**Hardening:** least-privilege `permissions` per workflow · `concurrency:` groups (no overlapping scheduled runs) · `timeout-minutes` on every job · **actions pinned to commit SHA** · Python deps locked with hashes (uv/pip-tools) · **Dependabot** (actions + pip) · pip cache · `$GITHUB_STEP_SUMMARY` per-run report · **environment protection** on publish · artifact retention limits · atomic publish (flip manifest last) · README status badges.

## 12. Observability, alerting & tooling

**Silence is success.** The only signals should be a failure issue or a staleness issue — both actionable.

- **Auto-open a GitHub Issue on failure** (`gh api`/github-script) — labeled `pipeline-failure`, deduped per day, with typed error + `last_run_status.json` + run link. Native, no external account.
- **Freshness dead-man's-switch** (`data-monitor.yml`) — runs after expected publish; if `manifest.latest_trading_date` < expected trading date → open issue + alert. Catches the silent non-run (expired token, GitHub incident, disabled schedule).
- **`$GITHUB_STEP_SUMMARY`** — human-readable per-run card (counts, quarantine, source, flags, duration).
- **`last_run_status.json`** — machine-readable history trail.
- **README badges** — CI status + last successful data date.

| Concern | Tool | Catches | Where |
|---|---|---|---|
| Lint + format | ruff | bugs, unused, complexity, style | pre-commit + CI |
| Types | mypy --strict / pyright | type errors, None-safety | pre-commit + CI |
| Tests + coverage | pytest + pytest-cov | logic regressions; coverage floor | CI |
| Data contract | pandera | schema/dtype/range violations | CI + prod runtime gate |
| HTTP isolation | responses / vcrpy | no live network in tests | CI |
| Time determinism | freezegun | calendar/date bugs | CI |
| Invariants (opt) | hypothesis | dedupe/append edge inputs | CI |
| Secret leaks | gitleaks / detect-secrets | committed creds | pre-commit + CI |
| Structured logs | structlog | machine-parseable run logs | prod |
| Error context (opt) | Sentry | exceptions w/ context | prod |

## 13. Maintainability

- **`RUNBOOK.md`** — "alert X fired → do Y" (e.g., row-count deviation → check NSE format change in `fetch_bhavcopy.py`).
- **ADRs** (`docs/adr/`) — why bhavcopy, why ISIN key, why CDN-not-git.
- **Schema versioning + CHANGELOG** — `schema_version` in manifest; additive-only; migration note on bump.
- **`justfile`/`Makefile`** — `test`, `lint`, `backfill`, `run-local`.
- Hexagonal structure + typed errors + small modules keep each unit understandable in isolation.

## 14. Phased roadmap

| Phase | Deliverable |
|---|---|
| **P0 — Producer skeleton** | Repo/dir layout, `fetch_bhavcopy.py` adapter (NSE + fallback lib), `holidays.json`, pandera schema, pytest harness + fixtures, `ci.yml`. One trading day fetched → validated → parquet locally. |
| **P1 — Daily pipeline + distribution** | `daily_update.py` full workflow, `backfill.py`, `data-daily.yml` (2 crons), manifest + `last_run_status.json`, publish to CDN (Releases + jsDelivr), atomic pointer flip. |
| **P2 — Observability + monitoring** | Auto-issue-on-failure, `data-monitor.yml` freshness switch, `$GITHUB_STEP_SUMMARY`, structured logging, badges, RUNBOOK, Dependabot. |
| **P3 — Client `scanner_data` module** | Rust hexagonal module: ports/adapters/fakes, sync loop + self-heal, local Parquet cache, `getCandles()`, scenario tests mirroring §10. |
| **P4 — Corporate actions (phase 2)** | NSE CA feed ingestion → `corporate_actions/` dataset → adjustment factors → adjusted-series derivation + correction channel. |
| **P5 — First derived dataset (proof of flexibility)** | Market-breadth consumer job reads `ohlc/`, emits `breadth/`, registers in manifest — zero ingestion/client changes. |

## 15. Deliverables (producer, P0–P2)

`fetch_bhavcopy.py` (adapter) · `daily_update.py` · `backfill.py` · `holidays.json` · pandera schema module (shared test + runtime) · `ci.yml`, `data-daily.yml`, `data-monitor.yml`, `backfill.yml` · pytest suite (all §10 scenarios) · `RUNBOOK.md` · `justfile` · Dependabot config.

## 16. Open questions / to confirm on review

- CDN choice: GitHub Releases + jsDelivr (free, simple) vs Cloudflare R2 (more control, needs account) — default to Releases+jsDelivr for v1.
- Backfill depth: 300 trading days for v1 (extendable later without schema change).
- Failure alert channel: GitHub Issue (default) vs also Slack/email webhook — Issue-only for v1.
- Producer repo location: **recommend a separate repo** (fully isolates CI/data concerns, secrets, and Actions minutes from the app); a `pipeline/` directory in this repo is the simpler fallback if you'd rather keep one repo for now.
