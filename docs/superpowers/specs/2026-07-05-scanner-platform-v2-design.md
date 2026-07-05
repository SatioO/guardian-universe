# Scanner Platform v2 — Design Spec

**Date:** 2026-07-05
**Status:** Approved (sections reviewed and approved in brainstorming session)
**Author:** Vaibhav Satam (with Claude)
**Scope:** End-to-end scanner platform: the guardian-universe EOD data pipeline (hardened), its loosely-coupled integration into traderview, the Rust scan engine, and the scanner UI/UX.
**Supersedes/amends:** `guardian-universe/docs/superpowers/specs/2026-07-04-scanner-data-pipeline-design.md` (the original pipeline design). That spec's architecture stands; this spec closes its spec-vs-implementation gaps, redesigns distribution for true atomicity, freezes the client contract, and adds the scan-engine + UX layers.

---

## 1. Purpose & context

guardian-universe (P0–P2) shipped a working EOD producer: NSE UDiFF bhavcopy → validate → year-partitioned zstd parquet → GitHub Release `data-latest` + manifest, with CI gates and a freshness monitor. A deep review (2026-07-05, two independent review passes + full source read) found:

- **The engineering quality is high** (hexagonal seams, fail-closed orchestration, shared pandera contract, deterministic tests) — the architecture is kept, not rewritten.
- **Two P0 data-loss/correctness bugs** in the store/publish path.
- **A significant spec-vs-implementation gap**: the original design promises a platform (multi-mirror CDN, deltas, fallback chain, indices, reference data, outlier gate, 19 CI-tested failure scenarios); the implementation is one mutable release of one EQ-only dataset with ~10 of 19 scenarios actually covered.
- **The published dataset holds ~3 days of data** — the 300-day backfill never ran.

Decisions locked during review (user-confirmed):
1. **Correctness first, then client** — harden the store before building consumers.
2. **GitHub-native only** — no external infra accounts; DR via snapshot releases (R2 mirror can be added later without design change).
3. **Widest universe** — all NSE cash series + indices in the (one and only) backfill.
4. **Scanner scope: deterministic scans + composability + backtest; AI/ML parked** (PKScreener's ML verified as a single-candle LSTM of ~coin-flip credibility).

### 1.1 Review findings being fixed (condensed)

| ID | Severity | Finding |
|---|---|---|
| P0-1 | data loss | `sync` failure ignored (`cli.py` returns 0 unconditionally) → empty store → `publish --clobber` replaces the year parquet with a one-day file; the release is the only copy |
| P0-2 | correctness | Publish is not atomic: `--clobber` delete-then-reupload gives sha-mismatch/404 windows during every publish; a mid-sequence death leaves the release torn until the next success |
| P1-1 | reliability | Zero fallbacks wired (`NseUdiffFetcher()` bare); daily fetch runs from datacenter IPs the RUNBOOK itself says NSE blocks |
| P1-2 | reliability | A missed day is never repaired: `daily` only targets today; monitor checks max(date) only → permanent invisible holes |
| P1-3 | reliability | Idempotency = "≥1 row exists"; a partial day is locked in forever |
| P1-4 | reliability | `sync` does no sha verification; corruption launders itself into the next manifest |
| P1-5 | reliability | Local backfill publish vs cron publish race — no compare-and-swap |
| P1-6 | correctness | Null-ISIN new listings silently dropped; quarantined rows discarded (counted only) |
| P1-7 | durability | No atomic temp+rename parquet writes (spec claimed it) |
| P2-x | hygiene | 2026-only holiday calendar; Muhurat session actively skipped; monitor `if:` misses setup failures; 60-day cron auto-disable; dispatch-input shell interpolation; unpinned actions; no lockfile; jsDelivr mirror claim is factually broken (jsDelivr cannot serve Release assets); `publish` re-clobbers on no-op days; zip member and date parsing too trusting; empty-store crash in `_latest_trading_date` |

---

## 2. Distribution & store correctness (G0)

### 2.1 Content-addressed assets, one tiny mutable pointer

The single `data-latest` release remains, but its contents change:

- **Data files are immutable**: every parquet uploads under a content-addressed name — `ohlc_2026.<sha8>.parquet` (first 8 hex of its sha256). New data → new name. Nothing is ever clobbered.
- **`manifest.json` is the only mutable asset.** It references data files by content-addressed name. Publish = upload all new data files first, then flip the manifest. Any manifest a client reads (old or new) references complete, verifiable, still-present files — **no torn state is observable**. The manifest is <50 KB; its own replace window is sub-second and a 404 is retried.
- **GC**: assets unreferenced by the current manifest are deleted only after 7 days (a client mid-download always finishes).

### 2.2 Fail-closed sync

`sync` downloads the manifest first, then every referenced file, then **verifies each sha256**. Any failure other than an explicit "release does not exist" (distinguished via `gh release view` semantics, not exit-code guessing) **aborts the run with exit 1**. The workflow step fails; no ingest, no publish.

### 2.3 Publish guards

1. **Shrink-guard** — before uploading, compare the new manifest against the live one: refuse if row counts per year decrease, a year disappears, or `latest_trading_date` goes backwards (override flag for deliberate corrections).
2. **Compare-and-swap** — re-fetch the live manifest immediately before the flip; abort if its `generated_at` differs from the one synced at run start (kills the local-backfill-vs-cron race).
3. **Post-publish verification** — re-download the manifest and spot-verify referenced shas; mismatch fails the run loudly.

### 2.4 Deltas

Each successful day also publishes `delta_{date}.<sha8>.parquet` (~150 KB). The manifest lists the last 30 deltas; clients catch up in KBs. Baseline (year files) is the fallback when the gap exceeds the delta window. (Delta *listing* is part of manifest v2, so this lands with G1; the content-addressed upload mechanics land in G0.)

### 2.5 Disaster recovery (GitHub-native)

- **Monthly snapshot releases** `data-snapshot-YYYYMM`: immutable copy of the then-current file set; keep 6.
- **Manifest history** appended to a git-tracked `manifest-log.jsonl` (audit trail of every publish).
- Recovery from any bad state = repoint at the last snapshot + replay deltas. Residential re-backfill becomes emergency-only.
- A restore drill is rehearsed once as part of G3 exit criteria.

---

## 3. Data model, universe & manifest v2 (G1)

### 3.1 Universe: widen at ingest, filter at read

- `normalize` keeps `FinInstrmTp=='STK'` + F-session filters but **drops the EQ-only filter**: all cash series (EQ, BE, BZ, SM, ST, …) stored with their `series` value. The scanner defaults to EQ client-side.
- Row-count gates become **per-series trailing deviation** (>15% vs trailing-10-day mean per series) with loose absolute total bounds (1,000–10,000) — universe growth never requires a code change.
- **Indices adapter** `sources/nse_indices.py`: NSE daily indices file; rows tagged `series='INDEX'`, `instrument_key` = stable slug (`IDX:NIFTY50`), `isin` empty. Same store, same schema.

### 3.2 Identity & quarantine

- Null-ISIN equities key as `NSE:{symbol}` sentinel; when the ISIN appears, a remap row in `reference/` links histories.
- Quarantined rows are **persisted** as `quarantine_{date}.parquet` (published as diagnostic extras), not just counted.

### 3.3 New datasets (additive)

- **`reference/instruments`** — point-in-time symbol master (SCD2, append-only): `(instrument_key, isin, symbol, name, series, status[active|suspended|delisted], first_seen, last_seen, valid_from, valid_to)`. Derived from bhavcopy presence + NSE `EQUITY_L.csv`. Gives listing age, delisting exclusion, rename history.
- **`reference/index_constituents`** — `(index_code, instrument_key, effective_from, effective_to)` from NSE indices CSVs. Enables universe filters ("NIFTY500 only") and P5 breadth denominators.
- **`ca_flags/`** — corporate-action detection pulled forward from P4: daily comparison of `prevclose(t)` vs stored `close(t−1)` per instrument; discontinuity >0.5% ⇒ ex-date event `(date, instrument_key, implied_ratio)`. The client excludes flagged instruments from level-based scans until P4b adjusts them. The detector later becomes P4b's cross-check.

### 3.4 Manifest v2 (frozen before any client parses it)

```json
{ "manifest_version": 2, "generated_at": "…", "latest_trading_date": "…",
  "min_client_version": "0.1.0",
  "datasets": [
    { "name": "ohlc", "schema_version": 2, "latest_date": "…",
      "baseline": [ {"name": "ohlc_2026.a1b2c3d4.parquet", "sha256": "…", "bytes": 0, "rows": 0} ],
      "deltas":   [ {"date": "…", "name": "…", "sha256": "…", "bytes": 0} ] },
    { "name": "reference", "schema_version": 1, "latest_date": "…", "baseline": ["…"] },
    { "name": "ca_flags",  "schema_version": 1, "latest_date": "…", "baseline": ["…"] }
  ] }
```

- Per-dataset `schema_version`; clients ignore unknown datasets (forward-compatible).
- Reserved dataset names: `corporate_actions`, `breadth`, `fundamentals`.
- Reserved adjustment enum: `raw | split | total_return`.

### 3.5 The one real backfill (G3)

300 trading days, **all series + indices**, against this final schema; residential, resumable, atomic writes. Run once; snapshots make it the last backfill ever needed.

---

## 4. Producer runtime & operational hardening (G0/G2)

- **Catch-up loop**: `daily` walks `trading_days_back(today, 7)` and ingests every missing day (idempotency makes present days free). Late bhavcopy or failed runs self-heal.
- **Completeness idempotency**: a day >15% below the trailing mean is re-fetched and merged, not skipped.
- **Continuity monitor**: `check-freshness` verifies every expected trading day in the last 10 exists with plausible row counts; the workflow `if:` also fires on setup failure; a monthly keep-alive defeats GitHub's 60-day cron auto-disable.
- **Calendar**: multi-year `holidays.json` + **`special_sessions.json`** (Muhurat date + window) consulted by `is_trading_day` — the Muhurat candle is ingested. A yearly scheduled job fetches NSE's holiday list and opens a PR; monitor alerts if next-year holidays are missing by Dec 1.
- **Atomic writes**: `*.tmp` + `os.replace` in `store.append_day`.
- **Publish only on new data** (status == success) — no clobber-churn on no-op days.
- **Small fixes**: explicit `.csv` member selection in zip; strict `format=` date parsing; empty-store guards in `_latest_trading_date`; trailing counts read via one windowed read, not 20 full-file reads.
- **Workflow hygiene**: dispatch inputs via `env:`; actions SHA-pinned; deps locked with hashes; `persist-credentials: false`; jsDelivr claim deleted from docs (distribution is GitHub Releases' own CDN, stated honestly).

---

## 5. Source resilience: the fallback registry (G2)

Two independent layers, mirroring traderview's broker-registry pattern (dispatch via registry + capability, never a hardcoded source name).

### 5.1 Producer side — "build the day" (central, canonical)

```
SourceAdapter protocol: fetch_raw(date) -> DataFrame   (+ declared capabilities)
Chain (ordered, config-driven; provenance recorded per row in `source`):
  1. NSE UDiFF bhavcopy                (canonical)
  2. NSE alt-route (jugaad-data / alternate NSE endpoints)
  3. Kite historical API               (last-resort day rebuild; credential-gated)
  4. hold-last-good + ALERT            (never fabricate a candle)
```

- One adapter per file in `sources/`; adding a source = implement + register, zero orchestration edits.
- **Kite adapter**: enabled only when `KITE_*` secrets exist. Primary mode = **manual recovery** (`pipeline daily --date X --source kite` run locally with a live session; documented in RUNBOOK). Automated TOTP-in-CI is an optional switch, not a dependency.
- **Cross-check gate**: when two sources are available for a day, a sampled price comparison catches silent corruption of the primary.
- The daily run **records which source path succeeded** from Actions IPs — the "does NSE block CI?" question is answered with evidence, and the chain is tuned on facts.

### 5.2 Client side — "obtain the day" (per user, self-healing)

```
1. CDN manifest + deltas (sha256-verified)
2. Prior snapshot release (manifest torn/unreachable)
3. Broker top-up: if central data is stale > 3 trading days, synthesize missing
   recent days from the user's OWN connected broker; provenance 'local-topup';
   auto-replaced when central catches up
```

Step 3 uses a `TopUpSource` **port** declared by `scanner_data`; the app injects an adapter built on the existing broker/historical registry. `scanner_data` has zero broker imports. The pipeline being down never blanks the scanner — worst case is per-user data with a freshness caveat in the UI.

---

## 6. App integration: three modules, three contracts (A0/A1)

```
┌─ guardian-universe (Python, CI) ─────── contract #1: manifest v2 + parquet schema
▼
┌─ src-tauri/src/scanner_data/ (Rust) ─── DATA layer (hexagonal, mirrors src/watchlist/)
│   domain/    candle model, adjustment application (P4b-ready)
│   ports/     Clock · SnapshotSource · LocalStore · Calendar · TopUpSource
│   infra/     GhReleaseSource, DuckDB/parquet LocalStore, SystemClock; infra/fakes/
│   controller/ ScannerDataStore: sync() · status() · get_candles(key, range, adj)
│               · volume_baseline(key, n) · latest_eod_date()
│                                         contract #2: CandleSource trait + events
▼
┌─ src-tauri/src/scan_engine/ (Rust) ──── COMPUTE layer (pure domain)
│   primitives/  EMA·SMA·RSI·MACD·ATR·CCI·MFI·ADX·BB·Keltner·SuperTrend·PSAR·
│                Ichimoku·Aroon·StochRSI·AVWAP·rVol·52w hi/lo·consolidation·
│                RS-vs-index·RS-Rating·RVM·trend-slope·pivots·candle patterns
│   criteria/    serializable predicate AST — a scan = a JSON expression tree
│   presets/     built-in catalog = named ASTs (+ named detectors: VCP, cup&handle,
│                trendline support, TTM squeeze)
│   backtest/    run any AST as-of historical dates → forward returns → win-rates
│   Depends ONLY on CandleSource — testable with canned candles, no store
│                                         contract #3: tauri commands + events
▼
┌─ src/scanner/ (React) ────────────────── UX layer (mirrors src/watchlist/)
    domain/ ports/ controller/ components/ — TanStack Query over commands;
    sync/freshness events drive the UI
```

Loose-coupling wins: the producer can be rewritten without touching Rust (contract #1); the scan engine never knows where candles come from (contract #2) — which is also what lets **rVol and future chart-history adoption** plug in opt-in; the UI speaks only commands/events (contract #3). Each layer tests in isolation with fakes.

**Local store choice**: CDN parquet files kept verbatim on disk as the verified cache (re-hash = verify), queried via **DuckDB (duckdb-rs)** — zero ETL, SQL for cross-sectional ranking; per-scan feature computation in Rust (rayon), recompute-from-history, never incremental state.

**First-run UX**: baseline ~25–60 MB downloads in background with progress events; scanner becomes usable per-year-file as each verifies; "data as of {date}" freshness chip everywhere.

**Sync cadence**: on launch + schedule + before a scan; ETag/If-None-Match on the manifest.

### 6.1 Opt-in adoption seams (A4)

- `volume_baseline(key, n)` serves the existing rVol feature its 20-day average-volume denominator from the local EOD store — zero broker calls, works pre-market. Opt-in rewiring, not a v1 dependency.
- `get_candles()` stays generic so charts/watchlist could warm-start from the store later. Nothing is wired into them now.

---

## 7. Scan engine feature space (A1/A3) — PKScreener-informed

Full source-verified map of PKScreener's ~50 scanners informed this scope. Verdicts: adopt the deterministic core; redesign composition and backtest; skip the ML and cloud-alert plumbing.

### 7.1 Preset catalog (~35 presets, 6 groups)

| Group | Presets (source-verified formulas + defaults carried over) |
|---|---|
| **Breakouts** | Probable breakout (close×1.05 > 200-candle high + vol gates) · Today's breakout (22d BO level + resistance + vol ≥2.5×) · 52-wk-high breakout (vol ≥1.5×50d, tight pre-BO range, strength score) · 10-day-high breakout · 52-wk-high approaching (within 5%, 20d range ≤10%, volume drying) · Breaking-out-now (body ≥3× 10-candle avg body) |
| **Trend / MA** | MA confluence & super-confluence (EMA8×21, EMA55 within 1–10% of SMA200) · Golden/death cross · Price-action MA cross (user MA/length) · Reversal-at-MA (±2% of turning MA) · Pivot-point crosses · Ichimoku short-term bullish · Higher-highs/lows + SuperTrend(7,3) |
| **Momentum** | 2%+ up 3 days · DEEL high momentum (RSI≥68, MFI≥68, CCI≥110; loose variant) · Bullish-for-tomorrow (MACD hist dip-rise) · Super gainers/losers (±15%) · Rising RSI from oversold · RSI-MA cross |
| **Volume** | Volume gainers (rVol ≥2.5×) · Lowest volume in 30d (pre-breakout) · Volume-spread analysis (supply drought / demand rise) |
| **Patterns** | VCP — PKScreener variant (3 legs, ≤70% successive contraction, RVM<60, ≤20% off ATH) · VCP — Minervini weekly (stage-2 template, pullback vol <70% rally vol) · Cup & handle (depth 8–45%, U-check, width 15–180) · TTM squeeze (BB(20,2) in Keltner 1.5×ATR, release direction) · Inside-bar flag · NR4/NRx · Trendline support (iterative regression on lows) · ~10 candle patterns (hand-rolled, no TA-Lib dep) |
| **Reversals / Risk** | ATR trailing stop UT-Bot (sensitivity×ATR(10), strength 1–5) · ATR cross (body ≥ ATR14 + RSI≥55 + vol>SMA7) · PSAR+RSI reversal · Momentum gainers (3 ascending green candles) · Multi-indicator composite score (weighted 8-indicator 0–100 with reasons breakdown — shown as an opinionated summary column) |

Universal pre-filters (config): LTP range, minimum volume 10k, stage-2 gate (≥2× yearly low AND ≥0.75× yearly high), CA-flag exclusion for level-based scans. Defaults carried over: lookback 22d, consolidation ≤10%, vol ratio 2.5×, RSI(14), CCI(14), ATR(10/14), BB(20,2).

### 7.2 Composition (redesigned)

A scan is a serializable **criteria AST** (predicates over primitives, AND/OR groups, per-node params). "Piping" = AND-composition evaluated in **one in-memory pass** (vs PKScreener's string-rewriting re-scan per stage). PKScreener's 37 curated pipes (liquidity → momentum → pattern → volatility confirmation) seed the preset library. Saved scans = persisted ASTs.

### 7.3 Backtest (redesigned)

Run any AST as-of each of the past N (default 120) trading days over the store; measure forward returns at {1,2,3,4,5,10,15,22,30} bars; report per-period win-rate ("73% of 1520") + per-stock overall % + growth-of-10k. Computed numerically over parquet — never via rendered output.

### 7.4 Parked

AI/ML predictions (PKScreener's is a single-candle LSTM, ~coin-flip; can slot in later as one more signal column) · Lorentzian classifier (optional "experimental" later — deterministic kNN, feasible) · MF/FII ownership + fair value (needs its own sourcing decision) · F&O short-sell recipes · intraday scanning (belongs to the live broker layer, composed over `latest_eod_date()`).

---

## 8. UI/UX arc (A2/A3)

- **v1 — Preset library + results**: ScannerPage becomes: preset catalog grouped as §7.1, one-click run, virtualized results table (sortable criteria columns, per-scan default sort keys, sparklines), **"data as of {date}" freshness chip**, CA-flagged symbols badged/excluded, row actions → open chart / add to watchlist / tag (existing flows). Design-harness aesthetic pass applies.
- **v2 — Composability**: visual criteria builder over the AST (condition rows, AND/OR groups), save/pin scans, "pipe" = start from preset + add conditions.
- **v3 — Evidence + automation**: backtest panel per scan (win-rates, forward-return distribution, growth-of-10k), scheduled scan-on-data-refresh with native notifications, scan-diff ("new entrants today") — PKScreener's MarketMonitor concept, desktop-native.

Result-column vocabulary adopted from PKScreener (battle-tested trader-facing labels): rVol as "2.5x", Trend(22Prds) buckets, "BO: x R: y", 52Wk-H/L proximity, MA-Signal.

---

## 9. Testing strategy

- **Producer (G0–G3)**: existing deterministic harness extended. New mandatory scenarios: chaos publish (kill mid-sequence → release never torn), sync failure → run fails (not empty-store), shrink-guard trips, CAS race, delta chain apply, catch-up repairs a hole, per-series gates, CA detector on split fixture, Muhurat ingested, indices adapter, null-ISIN sentinel keying. The 19-scenario table from the original spec becomes an honest checklist — each row marked implemented+tested / deferred-to-phase.
- **scanner_data (A0)**: all client scenarios with fakes (torn publish, stale edge, offline, corrupt cache self-heal, top-up path, first-run bootstrap). No network in tests.
- **scan_engine (A1/A3)**: known-answer fixtures per primitive (validated against reference implementations); preset golden tests on canned universes; property tests for AST evaluation (AND/OR/param edge cases); backtest determinism.
- **CI**: branch protection on the full matrix, both repos.

---

## 10. Roadmap

| # | Phase | Repo | Delivers | Exit criteria | Size |
|---|---|---|---|---|---|
| **G0** | Store correctness | guardian-universe | Fail-closed sync + sha verify · content-addressed assets + manifest-last flip · shrink-guard, CAS, post-publish verify · atomic writes · publish-only-on-success | Chaos test (kill publish mid-run, fail sync, concurrent publish) cannot lose or tear data | M |
| **G1** | Contract v2 + data model | guardian-universe | Manifest v2 · all-series universe · indices adapter · `reference/` · `ca_flags/` detector · quarantine persistence · calendar w/ special sessions | Client contract frozen; CA ex-dates detected on fixture; Muhurat ingested | M |
| **G2** | Source-fallback registry | guardian-universe | SourceAdapter registry · jugaad alt-route · Kite adapter (manual + optional CI) · cross-check gate · catch-up loop · continuity monitor · workflow hardening | Simulated NSE outage self-heals via fallback; missed day auto-repairs | M |
| **G3** | The real backfill | guardian-universe | 300 trading days, all series + indices, final schema; monthly snapshot + DR drill | 52w/EMA-200 computable for every active instrument; restore-from-snapshot rehearsed | S |
| **A0** | `scanner_data` module | traderview | Hexagonal data layer (§6) · sync/verify/self-heal · TopUpSource port · first-run bootstrap with progress | All client scenarios green with fakes; cold start → scan-ready without blocking UI | L |
| **A1** | `scan_engine` | traderview | Primitive library · criteria AST · preset catalog (§7) · rank/score model | Full-universe scan <1s; presets pass known-answer fixtures | L |
| **A2** | Scanner UI v1 | traderview | Preset library + results + freshness + chart/watchlist actions (design-harness pass) | Usable end-to-end scanner in-app on real data | M |
| **A3** | Composability + backtest | traderview | Criteria builder UI · saved/piped scans · win-rate backtest panel · scan-diff | Custom scan built, saved, backtested in-app | L |
| **A4** | Opt-in adoption seams | traderview | rVol reads `volume_baseline()` · (optional) chart history warm-start | rVol works pre-market with zero broker history calls | S |
| **P4b** | Corporate actions | both | NSE CA feed → `corporate_actions/` → back-adjustment factors · detector cross-check · correction channel · `adjustment` live | A split day produces correct adjusted 52w/EMA series | M |
| **P5** | Breadth (platform proof) | guardian-universe | Breadth consumer reads `ohlc/`+constituents → emits `breadth/` — zero ingestion/client changes | New dataset consumed with no contract edits | M |

**Ordering logic**: G0 before everything (no point enriching data a flaky publish can destroy) → G1 before G3 (backfill once, against the final schema) → G1 before A0 (client parses a frozen contract) → A1 parallelizable with A0 (pure domain vs data layer) → P4b after A2 (detector + exclusion protect correctness meanwhile) → P5 last (extensibility proof).

**Parked explicitly**: AI/ML signals · intraday scanning · BSE-unique symbols · Lorentzian (experimental, later) · MF/FII + fair-value data (own sourcing decision).

---

## 11. Resolved questions

| Question | Resolution |
|---|---|
| Priority order | Correctness first, then client, then UI |
| External infra | GitHub-native only; R2 mirror addable later without design change |
| Universe | All NSE cash series + indices, locked in the one G3 backfill |
| Scanner scope | Deterministic presets + composability + backtest; AI parked |
| Kite fallback in CI | Adapter credential-gated; manual-first, TOTP-in-CI an optional switch |
| jsDelivr | Removed — cannot serve Release assets; GitHub Releases CDN is the honest single origin |
| Client local store | Verified CDN parquet on disk + DuckDB query layer |
| Repo split | Producer stays in guardian-universe; scanner_data/scan_engine/UI in traderview |
