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

## Manual daily run (single day)
Ingest one trading day (defaults to today) — the same pipeline as backfill, one date:
```
cd pipeline && uv run python -m pipeline daily [--date YYYY-MM-DD]
```
Non-trading days skip cleanly; an already-ingested day is an idempotent no-op.

## Alerts
- Row-count deviation / format break → run exits non-zero (fail-closed).
- Corrupt day (all rows quarantined) → `status: failed`, nothing written, retryable.

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

## Yearly
- Refresh `pipeline/data/meta/holidays.json` from NSE's published trading-holiday
  calendar (https://www.nseindia.com/resources/exchange-communication-holidays).
  Format: `{"YYYY": ["YYYY-MM-DD", ...]}` — one array of ISO dates per year.
- Alongside holidays, refresh `pipeline/data/meta/special_sessions.json` with the
  coming year's special trading sessions (e.g. the Diwali Muhurat session, which
  trades despite falling on a weekend/holiday). Format:
  `{"sessions": [{"date": "YYYY-MM-DD", "label": "muhurat"}, ...]}`. A missing
  file is treated as "no special sessions" (`load_special_sessions` tolerates
  absence), but a stale file silently omits the year's Muhurat session from the
  calendar, so review both files in the same pass.

## G1a: multi-dataset mechanism (registry, manifest v2, `--dataset`)

### Registry
Datasets are registered as `DatasetSpec` entries in `pipeline/src/pipeline/datasets.py`
(`DATASETS: dict[str, DatasetSpec]`, ordering via `DATASET_ORDER`). Today only
`equities` is registered; the mechanism is built to add more (indices, reference
data, corporate actions) in G1b without touching `cli.py`, `daily_update.py`,
`backfill.py`, `publish.py`, or `sync.py` — those all loop/dispatch over specs.
Each spec carries its own fetcher factory (`make_fetcher`), normalizer, row-count
sanity range, and manifest identity (`manifest_name`, `schema_version`).

Note: `DatasetSpec.abs_rowcount_range` (and other config-derived spec fields)
bind once, at spec-construction (module-import) time. Editing the underlying
`config.py` value at runtime has no effect on an already-running process —
changes require a fresh process (restart) to take effect.

### `--dataset` CLI usage
Both `daily` and `backfill` accept `--dataset <key>|all` (default `all`):
```
cd pipeline && uv run python -m pipeline daily --dataset equities
cd pipeline && uv run python -m pipeline backfill --days 300 --dataset equities
```
- `all` (default) resolves to `datasets.DATASET_ORDER` — every registered
  dataset, in registry order.
- An unknown key is rejected by argparse itself (`choices=[*datasets.DATASETS, "all"]`)
  before any pipeline code runs.
- `daily` loops the resolved specs, running one `run_daily` per spec (each
  threaded with the same `holidays`/`special_sessions` calendars), printing
  each dataset's status dict to stdout, and exits non-zero if **any** dataset's
  status falls outside the OK set (`success`, `skipped_holiday`,
  `skipped_idempotent`, `not_yet`).
- `backfill` mirrors the same per-dataset loop over the same resolved keys.

### Primary-status contract (important: read before changing exit-code logic)
`last_run_status.json` (in `META_DIR`) is written from **only the first dataset
in `DATASET_ORDER`** (the "primary" dataset — `equities` today), regardless of
how many datasets `--dataset all` ran. This is deliberate: the freshness
monitor (`check-freshness`) and the GitHub Actions workflow's publish gate both
read this single file as the pipeline's health signal, and that contract
predates multi-dataset support. Per-dataset statuses are printed to stdout
(and to the CI step log) for visibility, but are not persisted individually.

**Consequence:** if `--dataset all` runs equities successfully but a
secondary dataset fails, `main()`'s own return code is non-zero (CI step goes
red) — but `last_run_status.json` still reflects the primary's success, so the
publish gate (which only checks that file) is not blocked by the secondary
failure. This is by design for G1a: the primary dataset's health is the
publish-blocking signal; secondary-dataset failures surface as a failed CI
step (visible, actionable) without stopping equities publishing. Revisit this
if/when a secondary dataset's freshness becomes equally publish-critical.

**`window_failures.json` marker (G2 final-review fix, C1):** a THIRD signal,
alongside `last_run_status.json` (target-day health, publish-gating) and
`last_run_status_<key>.json` (per-secondary-dataset target-day health,
CI-step-red-but-non-blocking). `data/meta/window_failures.json` carries
non-target catch-up-window failures (primary or secondary dataset), written
only when at least one occurred, and is explicitly NOT publish-gating and
NOT part of `main()`'s return code at all — it is a pure alert-only side
channel, checked exclusively by `data-daily.yml`'s dedicated post-publish
"Surface window (catch-up) failures" step. A healthy target day always
publishes now regardless of this file's presence; see "G2: catch-up loop"
above for the full rationale and the incident this fixes.

### Manifest v2 shape
`manifest.json` (`manifest_version: 2`) has one entry per published dataset:
```json
{
  "manifest_version": 2,
  "min_client_version": "0.1.0",
  "generated_at": "...",
  "latest_trading_date": "YYYY-MM-DD",
  "datasets": [
    {
      "name": "ohlc",
      "schema_version": 1,
      "latest_date": "YYYY-MM-DD",
      "baseline": [{"name": ..., "asset": ..., "sha256": ..., "bytes": ..., "rows": ...}, ...],
      "deltas": [{"date": ..., "name": ..., "asset": ..., "sha256": ..., "bytes": ...}, ...]
    }
  ]
}
```
- `baseline` is the full, year-partitioned, content-addressed file set (what
  G0's `files` key held — `dataset_files()` reads either key transparently, so
  v1 and v2 manifests are both understood by shrink-guard and sync).
- `deltas` is a rolling window of the most recent per-day catch-up files (last
  30, see `store.list_deltas`/`build_manifest`), each also content-addressed.
  **Deltas are explicitly gap-tolerant / best-effort, not a complete per-day
  record**: a crash between the store append and the delta write can leave a
  permanent gap for that one day while the baseline stays complete and
  correct. Deltas are producer-emitted but not producer-synced — `sync_store`
  only materializes `baseline` files into the local store; a client wanting
  delta catch-up re-derives/consumes deltas itself. Do not treat a missing
  delta as a data-integrity failure; the baseline is always the source of
  truth.

### Quarantine evidence assets
When a day's fetch produces rows that fail validation, the bad rows are
written to `pipeline/data/meta/quarantine/{prefix}_{date}.parquet` (not part
of the dataset store). `publish` additionally uploads the **current**
`latest_trading_date`'s quarantine file (if present) as a plain release asset
(clobber-uploaded, not content-addressed, not referenced by the manifest) —
useful for debugging a day's row-count/schema anomalies from the release
directly. Because it's never added to the GC `referenced` set, it self-GCs
under the same 7-day aged-asset sweep (`publish._gc`) as any other
unreferenced asset — no separate cleanup step is needed.

## G1b: indices, widened universe, derived datasets (reference, ca_flags)

### Indices dataset
`indices` is the second fetched dataset (`datasets.INDICES`, `source_label
"nse-indices"`), sourced from NSE's daily `ind_close_all_{DDMMYYYY}.csv`.
Rows are tagged `series='INDEX'`, keyed as `instrument_key = "IDX:" +
NAME.upper().replace(" ", "")` (e.g. `IDX:NIFTY50`), with `isin=""`. Same
store/manifest/publish mechanism as equities — nothing dataset-specific in
`cli.py`/`publish.py`/`sync.py`. Row-count sanity uses its own absolute range,
`config.INDICES_ROWCOUNT_ABS_RANGE`.

### Widened cash-series universe + per-series gates
`normalize_equity_bhavcopy` no longer filters to `SctySrs == "EQ"` — every
cash series (EQ, BE, BZ, SM, ST, …) is stored under its own `series` value
(the scanner still defaults to EQ client-side). Rows with a null/empty ISIN
key as an `"NSE:" + symbol` sentinel instead of being quarantined for a
missing ISIN (quarantine still rejects rows where BOTH isin and symbol are
empty). Rows with a missing/null `SctySrs` are stored with `series=""` and
are excluded from EQ-default scans by construction (calibrated live against
the real 2026-07-03 bhavcopy, which had 5 such STK rows).

Because the widened universe roughly doubles daily row counts and adds
series with no prior history, the OLD **total**-deviation gate would trip on
every widening day. `run_daily` now gates on **per-series trailing
deviation** (`validate.check_rowcount_by_series`): each series with trailing
history is checked against its own trailing mean (>15% deviation, or a major
series — trailing mean ≥ 50 — vanishing entirely, both fail); a series with
no trailing history yet (new to the store) passes and starts accumulating
history. The absolute total-rowcount bounds are `config.ROWCOUNT_ABS_RANGE =
(2000, 10000)` (widened from the EQ-only `(1800, 3000)`). This is a
client-visible change to `ohlc`'s multi-series semantics, hence
`EQUITIES.schema_version` bumped `1 -> 2` (see `datasets.py`).

### Derived datasets: `reference/instruments` and `ca_flags/`
Two more registry entries, `REFERENCE` and `CA_FLAGS`, both `derived=True`.
Derived specs have **no fetcher** — `daily`'s CLI loop runs in two phases:
first the fetched specs (equities, indices) as before, then — only for a
full `--dataset all` run, and only when the primary (equities) status came
back healthy — the derived specs, built from the local store via
`builders.BUILDERS[key](spec, target)`:

- **`reference/instruments`** (`builders.build_reference`) — a full-rewrite
  SCD2 symbol master: one row per distinct `(instrument_key, symbol, series)`
  version seen in the equities store, with `first_seen`/`last_seen`/
  `valid_from`/`valid_to` and a v1 `status` (`active` if seen within the last
  10 trading days, else `inactive`; `suspended`/`delisted` need an exchange
  reference feed and are deferred to P4a).
- **`ca_flags/`** (`builders.build_ca_flags`) — corporate-action ex-date
  detector: for each trading day, joins today's `prevclose` against the
  previous trading day's stored `close` per `instrument_key` and flags a
  discontinuity beyond `config.CA_DISCONTINUITY_THRESHOLD` (a split, bonus,
  or other ex-date event — not ordinary price movement). Appended
  year-partitioned via `store.append_keyed`, deduped on `(date,
  instrument_key)` — idempotent re-runs replace rather than duplicate a
  day's flags.

`--dataset <derived-key>` (e.g. a lone `--dataset reference`) is **not
supported** — the CLI rejects it with an explanatory message and exit code 2.
Derived datasets only build as part of a full `--dataset all` daily run.

### Per-dataset status files: primary vs. secondary
Exactly one dataset — `DATASET_ORDER[0]` (`equities`) — is "primary" and
drives the publish gate and exit code, same contract as G1a. G1b adds:

- **`last_run_status.json`** is written **only if the primary key
  (`equities`) is among the resolved dataset keys for that run.** A run of
  `--dataset indices`, `--dataset reference`, or `--dataset ca_flags` alone
  **never touches** `last_run_status.json` — the file some other, earlier
  run (of `--dataset all`, or of `equities` alone) left behind stays exactly
  as-is.
- Every OTHER resolved dataset — fetched (`indices`) or derived
  (`reference`, `ca_flags`) — gets its own **`last_run_status_<key>.json`**
  (e.g. `last_run_status_indices.json`), written every run that includes it.
- `main()`'s exit code is driven by the primary's status when the primary
  ran; a run that doesn't include the primary exits by its own dataset(s)'
  statuses instead.
- A secondary/derived dataset failing does **not** block publishing a
  healthy primary: `daily`'s exit code still goes non-zero (so the CI step
  is visibly red), but `publish`'s gate only reads `last_run_status.json`
  (the primary), so equities keeps publishing. The workflow's **"Surface
  secondary-dataset failures"** step (after the publish step in
  `data-daily.yml`) loops every `last_run_status_*.json`, and fails the job
  if any secondary's status is outside the OK set
  (`success|skipped_holiday|skipped_idempotent|not_yet`) — this reddens the
  job (firing the existing Alert-on-failure step) without having blocked a
  healthy primary's publish. It only runs when the ingest step's own
  primary-status decision (`steps.decide`) was `success`, so a primary
  failure is reported once, not twice.

**STALENESS WARNING — read before trusting the gate after a manual
non-primary run.** Because a lone `--dataset <non-primary>` run (e.g.
`--dataset reference` to rebuild the symbol master, or `--dataset indices`
to retry a late indices file) never writes `last_run_status.json`, that file
keeps reflecting whatever the **last equities run** recorded — which may be
from an earlier day. If you then run `publish` (or the workflow does), it
will proceed — CAS content-addressing and the shrink-guard make that
publish itself benign/idempotent (no data corruption, nothing goes
backwards) — but the publish gate is now trusting a **stale** status file
that may not reflect today's actual equities health. **Operators must
re-run `daily` (all, or at least `equities`) before trusting the gate again**
whenever a manual non-primary run has intervened. This is a monitoring/trust
gap, not a data-integrity one.

### ca_flags: dual-key limitation (until reference-remap linking, P4a)
`build_ca_flags` joins today's and the previous trading day's rows **by
`instrument_key`**. An instrument that switches its key between those two
days — most commonly the `"NSE:" + symbol` sentinel resolving to the
instrument's real ISIN once one appears in the bhavcopy — has no matching
row on the other side of the join *on the day of the switch*, so if a
corporate action happens to coincide with that same-day key switch, it is
**silently missed** that day (no flag emitted, no error either — this is not
distinguishable from "new listing, nothing to compare against" in v1).
This is a known, accepted gap until `reference/`'s remap-linking work lands
(P4a wires the ISIN-appears event back into the detector's join). It affects
only the rare day where a sentinel-to-ISIN remap and a real corporate action
land on the exact same day for the exact same instrument.

### ca_flags: what a flag means for clients
A flagged instrument (`ca_flags` row for a given `(date, instrument_key)`)
signals a probable ex-date discontinuity (split, bonus, or similar) in the
`ohlc` series, not a genuine price move. **Clients should exclude flagged
instruments from level-based scans** (support/resistance, breakout,
%-from-52w-high, and similar absolute-price criteria) for that date until
P4b's adjustment-factor machinery (`raw | split | total_return`) is in place
to properly rebase the series across the ex-date.

### Migration note (G0 -> G1a manifest v2)
The first G1a publish reads the live G0 manifest (v1, `files` key) via
`dataset_files()` in both the shrink-guard (`check_no_shrink`) and `sync.py`;
`dataset_files()` is the one place that understands both `files` (v1) and
`baseline` (v2), so no special-casing is needed at the cutover. The new v2
manifest is written and flipped in as normal. Since data assets are
content-addressed (same content -> same asset name, `asset_name()`), the
G0-named baseline assets remain referenced by name in the new v2 manifest —
nothing is GC-eligible at the cutover except assets that would be superseded
in any ordinary daily publish (e.g. a year file rewritten with a new day's
rows gets a new sha8 and thus a new asset name; the old one ages out after 7
days, same as always).

## G2: catch-up loop (self-healing)

Every `daily` run doesn't just ingest the target day — for each **fetched**
dataset it also re-checks the trailing `config.CATCHUP_WINDOW_DAYS = 7`
trading days (ascending, ending at the target day) via the same `run_daily`
gates. A day already in the store costs one cheap idempotent-skip read; a day
that's missing (both crons failed earlier, a late bhavcopy, a transient
outage, ...) gets fetched and appended right there — no separate command,
no manual step. This means a single missed day self-heals automatically the
next time `daily` runs, instead of becoming a permanent hole. The window uses
ONE fetcher instance per spec (constructed once, reused for every day in that
spec's window), and threads the same holiday/special-session calendar as the
target day, so a holiday inside the window is correctly never requested.

**What a past-day failure means.** A 404 for the TARGET day is ordinary
lateness — the bhavcopy for today just isn't published yet (`not_yet`,
non-alerting, exit 0). A 404 for any OTHER day in the window is different:
that day is strictly in the past relative to the target, so NSE's archive
should already have it — a 404 there means the archive genuinely has a hole,
not that the day is running late. This is why `run_daily` takes an
`is_target_day` keyword (`True` for the target day, `False` for every other
window day): a past-day `NotYetPublished` maps to `"failed"` with a message
like `"archive missing for past trading day 2026-07-01"`, never `"not_yet"`.

**A repaired-hole failure is never silent — but it no longer blocks publish
either (G2 final-review fix, C1).** The dataset's own status (what gets
written to `last_run_status.json` / `last_run_status_<key>.json`, and what
Phase 2's derived builders key off) is always the **target day's** status —
catch-up days are a side effect, not what's reported as "the run." If a
non-target day in the window comes back `"failed"` (primary OR secondary
dataset), it's printed explicitly (`catch-up: <key> <date> failed: <message>`,
stderr) AND persisted to `data/meta/window_failures.json` (shape:
`{"failures": [{"dataset": ..., "date": ..., "message": ...}, ...]}`,
written only when at least one window failure occurred; removed
unconditionally at the start of every `daily` run so a clean run never
inherits a stale marker from a prior one).

Critically, **window failures (primary or secondary) no longer affect
`daily`'s own exit code at all** — only the TARGET day's own primary status
does (unchanged: primary target unhealthy still exits 1). Before this fix, a
primary-dataset window failure forced exit 1 even when the target day
succeeded; combined with `data-daily.yml`'s un-guarded "Ingest" step (no
`if:` condition), a non-zero `daily` exit failed the whole CI job, which made
GitHub Actions **skip** the downstream "Decide"/"Publish" steps entirely —
so a single permanent past-day archive hole silently froze publishing
indefinitely (the pipeline kept reporting the target healthy in its logs,
but nothing new ever actually reached the release) even though the target
day itself was fine every single night. The fix decouples the two signals:
`daily` now exits 0 whenever the target succeeds, regardless of window
failures, so `data-daily.yml`'s "Publish" step always runs for a healthy
target; a separate "Surface window (catch-up) failures" step (added AFTER
"Publish the updated dataset", mirroring "Surface secondary-dataset
failures"'s shape) checks for `window_failures.json` post-publish and reds
the job (`::error::` per entry, then exit 1, firing "Alert on failure") if
it's present — so the alert still fires, just after publish has already
completed instead of blocking it. `window_failures.json` is a
runner-local signal for that one workflow step; it is deliberately never
uploaded as a publish/release artifact (out of scope for this fix — keep it
minimal).

**Holes older than the window.** The catch-up loop only looks back 7 trading
days from the target — inclusive: the target day itself counts as one of
the 7, so the oldest self-repairable hole is 6 trading days before the
target, not 7. A hole older than that — the pipeline was down for
over a week, or a hole predates this mechanism entirely — is NOT
self-healed by `daily`; use `backfill --days N` (with N covering the gap) or,
if both NSE sources are down for that day, `rebuild-day` (see below).

**Short days self-repair too (`config.COMPLETENESS_SHORTFALL = 0.15`).**
Before this, `has_day` alone (">=1 row present") permanently locked in
whatever the first successful run happened to store — a day truncated by a
mid-fetch failure or a fallback that only partially served the universe
would sit there forever, "present" but incomplete, immune to every future
catch-up revisit. Now, when a window day is present, `run_daily` also
compares its stored rows against the trailing history before deciding to
skip. A day at or above `(1 - COMPLETENESS_SHORTFALL)` of the comparable
trailing mean skips exactly as today; a day below it is **re-fetched and
merged**, not just re-fetched from scratch — `append_keyed`'s existing
`keep="last"` dedupe on `(date, instrument_key)` means rows already stored
are simply refreshed by the new fetch and rows the short fetch missed the
first time are newly added. This applies to every day the catch-up loop
revisits (all 7, not just the target), so a short day up to 6 trading days
back self-heals the same way a fully missing day does.

**Completeness is measured over series shared between the stored day and
trailing history (regime-consistent across universe changes).** A first
version of this gate compared the stored day's flat TOTAL against the
trailing mean summed over ALL trailing series — that breaks the moment the
universe widens (e.g. an EQ-only store gains a new series such as BE):
inside the catch-up window, a pre-widening day that is genuinely EQ-only and
complete would have its correct EQ total compared against a trailing mean
that also includes the new series it never had, permanently reading as
"short" and re-fetching every single night that day stays inside the window
— a live incident reproduced exactly this (seven consecutive false
failed/exit-1 nights) before being fixed. The fix keeps the comparison
inside the stored day's own regime: `shared` = the series present in BOTH
the stored day (`store.day_series_counts`) and the trailing dict (a
non-empty trailing entry for that series); both `stored_shared_total` and
`trailing_shared_mean` are summed over `shared` only, so a series the stored
day predates (like BE for an old EQ-only day) never enters either side of
the ratio, while a series genuinely truncated *within* the stored day's own
regime (e.g. EQ itself halved) still fails the ratio exactly as before,
since EQ remains in `shared` either way. No shared series at all (fresh
store, every trailing day a miss, or the stored day's whole regime predates
all trailing history) still means "nothing to compare against" — any stored
rows count as complete, same as pre-Task-5. A successful top-up now names
this shared-series basis explicitly in the status message, e.g. `"re-ingested
short day (stored 1800 vs trailing mean 2400 over shared series)"` — read it
as: *only the series this day actually had are counted on both sides, so a
short reading here means a genuine shortfall within this day's own regime,
not an artifact of comparing against series the day never had.*

## G2: manual day-rebuild (recovery)

### When to use this
Last resort ONLY: both NSE sources (the UDiFF primary and the
`sec_bhavdata_full` fallback) are down for a trading day, or a data hole
predates what either NSE archive still serves. This command is **never**
wired into cron or the automatic fallback chain (`NseUdiffFetcher.fallbacks`)
— it is credential-gated and exists purely for a human operator to invoke on
demand. If a normal `daily`/catch-up run would eventually self-heal the hole
(NSE archives typically stay available for many days), prefer that; reach
for this only when NSE itself is the problem.

### Rebuild via any registered broker source (currently: kite)
`rebuild-day` is **broker-agnostic**: it resolves which broker actually
serves the rebuild through a registry (`pipeline/src/pipeline/rebuild.py`,
`RebuildSource` Protocol + `REBUILDERS` dict), never a hardcoded broker name.
Today exactly one broker is registered — Kite Connect (`id = "kite"`,
implemented in `pipeline/src/pipeline/sources/kite_rebuild.py`) — but the CLI
itself has zero Kite-specific logic; adding a second broker later is purely
additive (see "Adding a new broker source" below).

1. Generate a fresh Kite Connect access token (tokens expire daily at 6 AM
   IST): follow Kite Connect's login flow —
   https://kite.trade/docs/connect/v3/user/#login-flow — to obtain
   `KITE_API_KEY` and a same-day `KITE_ACCESS_TOKEN`.
2. Export both as environment variables:
   ```
   export KITE_API_KEY=...
   export KITE_ACCESS_TOKEN=...
   ```
3. Run the rebuild for the missing date:
   ```
   cd pipeline && uv run python -m pipeline rebuild-day --date YYYY-MM-DD
   ```
   Optionally pin the broker explicitly with `--via kite` (useful once a
   second broker is registered and you want to force one over the other;
   omitting `--via` picks the first currently-available registered source).
   Requires the `reference/instruments` derived dataset to already exist
   locally (it supplies the symbol -> (ISIN, series) universe map) — run a
   normal `daily` cycle at least once first if it's missing.
4. Expect roughly **~15 minutes for the full ~2400-symbol NSE universe** at
   the default 0.35s per-symbol rate limit (one HTTP call per symbol against
   Kite's historical-candle endpoint).
5. On success, publish as normal:
   ```
   cd pipeline && uv run python -m pipeline publish
   ```

The rebuilt frame runs through the **exact same** `run_daily` gates as any
regular ingest (wrong-date guard, per-series rowcount deviation, quarantine,
schema validation) — a rebuild that only manages to recover a small fraction
of the universe will legitimately **fail** the absolute rowcount floor rather
than silently landing a partial day. That is by design: a partial day is
worse than a clearly-failed one (it would otherwise look "complete" to
`has_day`/idempotency checks). Per-symbol failures (a handful of bad/missing
symbols) are tolerated and reported as a count + list on stderr; they do not
block the rest of the day from landing.

`rebuild-day` deliberately does **not** run the derived-dataset builders
(`reference`, `ca_flags`) that a full `daily --dataset all` run would —
those are rebuilt from the regular daily cadence's full multi-dataset run,
and a one-off manual equities rebuild is not that; run a normal `daily`
afterward (or wait for the next scheduled one) to refresh derived datasets
against the now-repaired store.

### Known degradations (accepted trade-offs for a recovery path)
Kite's historical day-candle endpoint doesn't expose two UDiFF fields for a
single day, so they're approximated rather than sourced correctly:
- **`PrvsClsgPric` (previous close)** degrades to the day's own **open**
  price (fetching a second, d-1 candle per symbol would double an already
  slow per-symbol HTTP volume for ~2400 symbols).
- **`TtlTrfVal` (turnover)** is not present in the day-candle payload at all
  and defaults to **0.0**.

Both are documented, intentional gaps: the goal of this path is getting
*some* OHLCV data back into the store for a hole, not byte-for-byte
reproducing the official bhavcopy. Do not rely on turnover or previous-close
figures for any day rebuilt this way.

### Adding a new broker source
Implement `rebuild.RebuildSource` (`id`, `available()`, `day_frame()`
returning the same PRIMARY-RAW UDiFF shape as the Kite/secfull adapters —
see `fetch.py`'s fallback-contract docstring) in a new module under
`pipeline/src/pipeline/sources/<broker>_rebuild.py`, and self-register it at
import time with a module-bottom `rebuild.register(YourBroker.from_env())`
call (mirroring `kite_rebuild.py`'s bottom-of-file registration). Add **one**
import line for that module to `pipeline/src/pipeline/sources/__init__.py`
(the broker-source registration aggregator — broker names belong there, and
only there) so it runs at import time alongside every other registered
source. **Zero edits to `cli.py`** — it only ever imports the `sources`
package as a whole and never names a broker; `--via`'s choices and
provenance labels (`"<id>-rebuild"`) are derived from the registry
automatically. Proof: `grep -i kite src/pipeline/cli.py` returns nothing.

## G2: weekly source cross-check (silent-divergence detector)

### Why this exists
Both NSE sources (the UDiFF primary and the `sec_bhavdata_full` fallback)
individually pass their own gates (rowcount deviation, quarantine, schema),
so a source that is **subtly, silently wrong** — a corrupted/stale bhavcopy
file that is still well-formed and within normal rowcount range — produces
no signal today: two "healthy" sources agreeing with themselves each is not
the same as the two sources agreeing with *each other*. The weekly
cross-check closes exactly this gap by comparing a deterministic sample of
`close` prices between the two **independent** endpoints, fetched **in
isolation** (each constructed with no fallback chain — see
`cli._cross_check_fetch_primary_raw` / `_cross_check_fetch_secondary_raw`),
so a divergence can only mean the two sources actually disagree, not that
one silently fell back to the other.

### What it compares
`pipeline/src/pipeline/crosscheck.py`'s `compare_sources(primary_df,
secondary_df, *, sample_n=50, tolerance=0.001)` joins the two CANONICAL
frames on `instrument_key` (inner join — see coverage limitation below),
samples deterministically (sorted keys, every k-th where `k = max(1,
len(joined) // sample_n)` — **no randomness anywhere in this path**, so the
same two inputs always produce the identical sampled set and result), and
flags a pair as a mismatch when its relative divergence (`|primary_close -
secondary_close| / |primary_close|`) exceeds `tolerance`. The result
(`CrossCheckResult`) reports `compared` (sample size), `mismatched` (count
over tolerance), and `worst` (up to 5 worst mismatches by relative
divergence, descending, as `(instrument_key, primary_close,
secondary_close)` tuples).

**Coverage limitation — read before trusting a "clean" (mismatched=0)
result.** The secondary (secfull) side has no ISIN column of its own; its
`instrument_key` is resolved via the same `isin_map`
(`datasets._load_isin_map`, symbol → ISIN from the `reference/instruments`
store) used by the real secfull fallback path, so keys align with the
primary side's real-ISIN keys. A symbol **missing from that map** gets an
`"NSE:" + symbol` **sentinel** key instead — which essentially never equals
the primary side's real-ISIN key for the same instrument, so that row
**silently drops out of the inner join**. It is not "compared and found to
agree" — it is simply never compared. The cross-check therefore only
covers the **mapped intersection** of the two sources (whatever
`reference/instruments` currently maps to a real ISIN); that is the
meaningful set for this check, but a clean result says nothing about
instruments outside that mapped intersection.

### Weekly cadence
`.github/workflows/data-crosscheck.yml` runs Saturday 03:00 UTC (+
`workflow_dispatch` for on-demand runs), invoking `python -m pipeline
cross-check` against the previous trading day. No store writes — this is
pure detection, never ingestion, and never touches `data/`.

### What a divergence alert means
The CLI prints one of three explicit, prefixed outcome markers, so an
operator scanning CI/alert output never has to infer which happened from
stdout-vs-stderr or prose wording alone:

- **`cross-check: CANNOT-RUN — ...`** (stderr, exit 1) — one of the two
  sources' fetch/normalize itself failed; the comparison never ran at all.
  This is itself alert-worthy on its own weekly cadence, since it means the
  safety net wasn't actually checked that week.
- **`cross-check: DIVERGENCE compared=N mismatched=M`** (stdout, exit 1) —
  both sources fetched fine, but `M` of the `N` sampled closes disagreed
  beyond tolerance (a small table of the worst divergences prints
  immediately after this line).
- **`cross-check: OK compared=N mismatched=0`** (stdout, exit 0) — both
  sources fetched fine and agreed on every sampled close.

The workflow's alert-on-failure step opens/appends to the standard
`pipeline-failure`-labeled issue on any non-zero exit (same dedupe shape as
`data-monitor.yml`/`data-daily.yml`), so both `CANNOT-RUN` and `DIVERGENCE`
page the same way — the marker text in the issue body is what tells you
which one happened and drives which of the two operator actions below
applies.

**`CANNOT-RUN` → check source availability/connectivity.** The printed
message names which source failed (primary/UDiFF or secondary/secfull) and
the underlying exception. Start there: is the NSE endpoint reachable right
now (a manual curl/browser check), is this a transient anti-bot/rate-limit
block (the same class of failure the daily pipeline's own retry+fallback
chain already handles for that source in normal ingest), or has the
endpoint's URL/format changed? This is a tooling/connectivity failure of
the cross-check itself, not evidence that the two sources disagree — do
not treat a `CANNOT-RUN` as a data-quality signal on its own.

**`DIVERGENCE` → investigate BOTH sources before trusting either.** A
divergence alone does not tell you which source is wrong — it only tells
you they disagree. Do not assume the primary (UDiFF) is automatically
correct just because it's the primary in the daily ingest chain;
cross-check both against a third reference (e.g. the exchange's own
website for a couple of the flagged symbols, or a manual `rebuild-day --via
kite` comparison) before deciding which source to trust or route around.
If a real divergence is confirmed, treat the daily pipeline's
currently-stored days for the affected instrument(s) as suspect until the
root cause is identified.

### Running manually
```
cd pipeline && uv run python -m pipeline cross-check [--date YYYY-MM-DD]
```
`--date` defaults to the previous trading day (via the same trading
calendar + special-sessions logic used elsewhere). Exit 0 pairs with the
`OK` marker (agreement); exit 1 pairs with either the `DIVERGENCE` marker
(comparison ran, disagreement found — see the printed table) or the
`CANNOT-RUN` marker (comparison never ran — see the printed error naming
which source failed and why).

## G2: continuity monitor (holes, not just staleness) + cron keep-alive

### Why this exists
`check-freshness`'s pre-existing staleness check only ever asks one
question: "is the manifest's `latest_trading_date` current as of today?"
That question is blind to a hole sitting a few sessions BEHIND the latest
date — e.g. a day the daily catch-up loop's own 7-trading-day reach never
covered (see the G2 catch-up-loop section above, "Holes older than the
window"), or a day that was present, silently corrupted/deleted on the
release side, and never re-checked because the latest date kept advancing
normally on top of it. A dataset can look perfectly healthy by the
staleness check alone while quietly missing a day. The continuity check
closes this gap by re-deriving which trading days SHOULD be present over a
trailing window and diffing that against what's actually in the published
baseline.

### Scope: every FETCHED dataset, not just the primary
**Continuity covers every dataset the registry resolves as fetched
(`not spec.derived`) — today that's both `equities` (primary) AND `indices`
(secondary) — not only the primary dataset.** This was a controller-level
scope amendment during implementation: the original design considered
checking only the primary, but that leaves a secondary dataset's hole
completely invisible to every other layer. Concretely: the primary's own
staleness AND continuity can both read perfectly clean while `indices` is
sitting on a hole several sessions back, because nothing else in the
monitoring stack ever looks at a secondary dataset's day-level completeness
— `last_run_status.json` (the publish-gate signal) is written only from the
primary's status (see the G1b "Primary-status contract" section above), and
the daily workflow's "Surface secondary-dataset failures" step only checks
the TARGET day's status, not day-level history. A hole confined to
`indices` would otherwise go undetected indefinitely. **Derived datasets
(`reference`, `ca_flags`) are deliberately NOT continuity-checked** — they
are rebuilt wholesale from the store on every full `daily --dataset all`
run, so a hole in a derived dataset is always a downstream symptom of an
underlying fetched-dataset hole (which continuity already catches), never
an independent fact worth alerting on by itself.

### What it checks
For each fetched spec, `check-freshness` resolves that dataset's entry in
the downloaded release manifest by `spec.manifest_name` (e.g. `"ohlc"`,
`"indices"`), downloads whichever baseline asset(s) cover the trailing
10-trading-day window (both the current- and previous-year asset when the
window straddles Jan 1 — the same year-selection logic
`store.read_trailing_window` uses), reads just the `date` column, and calls
`freshness.missing_days(dates_present, today, holidays, special_sessions)`
— a pure function that re-derives the expected trading days ending at the
last COMPLETED trading day before `today`, clamps that window to the
dataset's own available history (see below), and returns
`sorted(clamped_expected - dates_present)`. **Any missing day in ANY
fetched dataset** fails the check (exit 1), and each affected dataset's
missing day(s) are printed on their own line so an operator can see
immediately which dataset(s) and which day(s) are involved without digging
through logs.

### Clamped to available history (Critical fix — never expect days before a dataset's first stored day)
**The 10-trading-day window is intersected with days `>= min(dates_present)`
before it is diffed against what's actually stored — a dataset is never
expected to have data from before its own earliest stored day.** Without
this clamp, a young or still-catching-up dataset (fewer than 10 trading
days of history — e.g. right after a fresh bootstrap, or a new dataset a
few days into its first week) reports every pre-existence day as a false
"hole": a live store with only 3 days on record ({2026-07-01..03}) would
report the 7 trading days *before* 2026-07-01 as missing every single
morning — a false exit-1 alert with no real problem to fix, persisting for
roughly two weeks until depth reaches 10 (or until a backfill runs). The
exact same defect shows up a second way at a year boundary: a dataset less
than a year old simply has no previous-year baseline asset at all (it
didn't exist yet), and the pre-clamp window would treat that entire
previous-year portion as missing too.

**This clamp never hides a REAL hole inside a dataset's own available
depth** — only days strictly *before* the dataset's earliest stored day are
excused; a gap sitting between two stored days (e.g. day 3 present, day 5
present, day 4 silently missing) still fails the check by name exactly as
before. An empty `dates_present` (nothing stored/verifiable at all) has no
floor to clamp against, so it reports zero holes — this is by design, not a
blind spot: lag in that state is independently governed by the STALENESS
check above (`is_stale`, driven off the manifest's own
`latest_trading_date`), which stays exit-1 until the manifest's latest date
itself catches up.

**When the window is actually truncated this way** (the dataset's own
history doesn't yet reach the full 10 trading days), `check-freshness`
prints an informational, NON-failing note so the reduced-coverage state
stays visible instead of looking identical to a full pass:
```
continuity: ohlc verified over 3 of 10 days (history begins 2026-07-01)
```
This line never affects the exit code by itself. As the dataset's history
accumulates past 10 trading days, the clamp becomes a no-op automatically
(the same behavior as before this fix) and the note stops appearing — full
10-day verification arms itself with no code change or manual step
required.

**A manifest-listed-but-missing release asset** (the manifest names a
baseline file that the release itself doesn't actually have — a real
release/manifest consistency break, distinct from the dataset being absent
from the manifest entirely) is caught and reported as a clean, named
per-dataset error on stderr and fails the check (exit 1) — it is never
allowed to crash the monitor with a raw traceback.

### The arming rule (read before panicking about a "missing" brand-new dataset)
**A fetched dataset with NO entry in the manifest at all is a WARNING, not
a failure — it does not fail the check.** This matters for the exact moment
a new dataset is registered in code but hasn't published for the first
time yet (e.g. the window between `indices` landing in `DATASET_ORDER` and
its first successful `daily`+`publish` cycle): a dataset that has never
published isn't meaningfully "stale" or "full of holes" — there's nothing
to compare against yet. Without this grace rule, a hard exit-1 here would
false-alarm on every monitor run from the moment new dataset code merges
until its first real publish, which is exactly backwards (alerting on the
system working as designed, before it's had a chance to run once).

**Once a dataset's first baseline lands in the manifest, this grace period
ends permanently — from that point on, holes in that dataset are enforced
exactly like any other fetched dataset.** In short: *a never-published
dataset does not alert; its first publish arms continuity checking for it.*
There is no way back into the grace period once armed — do not expect a
dataset that publishes once and then goes dark to fall back to a warning
instead of an alert; a dataset with a manifest entry but a hole inside it is
always a hard failure, never a warning, regardless of how that hole came
to exist.

### Running manually
```
cd pipeline && uv run python -m pipeline check-freshness
```
Exit 0: both the staleness check and every fetched dataset's continuity
check passed (or a not-yet-armed dataset only produced a warning; or a
young/still-catching-up dataset only produced the informational "verified
over N of 10 days" note — neither a warning nor a note affects the exit
code). Exit 1: either the staleness check failed (the manifest's
`latest_trading_date` is behind the last completed trading day), the
release/manifest couldn't be downloaded at all, at least one ARMED fetched
dataset is missing at least one trading day inside its (possibly clamped)
trailing window (printed explicitly, per-dataset), or a dataset's manifest
entry names a release asset that doesn't actually exist (printed as a
clean per-dataset error, never a traceback).

### Cron keep-alive (`.github/workflows/keepalive.yml`)
GitHub automatically disables a scheduled workflow's cron trigger after 60
days with no repository activity — a real risk for a low-traffic repo where
the only activity IS the scheduled crons themselves creating a silent
bootstrapping problem (crons stop firing, so there's no more activity, so
they never get a chance to re-trigger). `keepalive.yml` runs monthly (1st,
05:00 UTC, plus `workflow_dispatch` for an on-demand check) and:
1. Runs `gh workflow enable data-daily.yml data-monitor.yml
   data-crosscheck.yml || true` — a no-op if all three are already active,
   and idempotently re-enables any that GitHub auto-disabled.
2. Prints the name of any workflow still NOT in the `active` state (via
   `gh api repos/{repo}/actions/workflows --jq '.workflows[] | select(.state
   != "active") | .name'`) to the job's step summary — visibility that
   something needs manual attention (e.g. a workflow disabled for a reason
   OTHER than the 60-day auto-disable, which `gh workflow enable` alone
   would silently paper over without this visibility step).

This is the one workflow in the repo that needs `permissions: actions:
write` (every other workflow only needs `contents`/`issues` — see each
workflow's own `permissions:` block) since re-enabling a workflow is an
Actions-API write, not a repo-content or issue write.

## G2: workflow hardening (SHA-pinned actions, locked deps, calendar-refresh nag, hygiene)

### SHA-pinned actions — what's pinned and why

Every workflow step that uses a third-party GitHub Action (`actions/checkout`,
`actions/setup-python`) is pinned to a full commit SHA, not a mutable tag
(`@v7`), across all five workflows that have any `uses:` steps
(`data-daily.yml`, `data-monitor.yml`, `data-crosscheck.yml`,
`pipeline-ci.yml`; `keepalive.yml` and `holidays-refresh.yml` have no
`uses:` steps at all — both are pure `gh`-CLI jobs with nothing to check
out, so there is nothing to pin in either):

```yaml
- uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7
  with:
    persist-credentials: false
- uses: actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1 # v6
  with:
    python-version: "3.11"
    cache: pip
```

**Why:** a mutable version tag (`@v7`) can be force-moved by the action's
maintainer (or, in a supply-chain-attack scenario, by anyone who compromises
the action repo) to point at different, potentially malicious code without
the pin in this repo ever changing — the workflow would silently start
running whatever `v7` points to *today*, not what it pointed to when this
was reviewed and merged. A commit SHA is immutable: `actions/checkout@9c091b...`
always resolves to the exact same reviewed code, forever. The trailing
`# v7` comment is purely for human readability (which tag the SHA
corresponds to at pin time) and has no functional effect.

`persist-credentials: false` is set on every `actions/checkout` step: by
default, checkout leaves the ephemeral `GITHUB_TOKEN` credential configured
in the local git config after checkout completes, which any subsequent step
in the job (including a compromised dependency's install/build script) could
read and use to push to the repo. None of this pipeline's steps need git
push access from the checkout's own credential (the `gh` CLI steps
authenticate via their own explicit `GH_TOKEN` env var instead), so it's
disabled outright.

**How each SHA was derived (verify-before-pin procedure):** resolve the tag
to its underlying git object, then confirm that object is a COMMIT (not an
annotated tag object one level removed from the commit):

```bash
gh api repos/actions/checkout/git/ref/tags/v7 --jq '.object'
# {"sha":"9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0","type":"commit",...}
```

If `.object.type` were `"tag"` (an annotated tag), the SHA returned there is
the annotated-tag OBJECT's own sha, not the commit — a second dereference is
needed: `gh api repos/{owner}/{repo}/git/tags/{that_sha} --jq '.object.sha'`
to reach the actual commit. Both `actions/checkout@v7` and
`actions/setup-python@v6` resolved directly to `type: commit` (both are
lightweight tags), so no second deref was needed for either at pin time —
but always check `.object.type` when re-pinning, since a project can switch
tagging styles between releases. Cross-verified independently via
`git ls-remote https://github.com/{owner}/{repo} "refs/tags/{tag}"` (and
`"refs/tags/{tag}^{}"` to peel an annotated tag if `ls-remote` shows one) —
both methods agreed on both SHAs above.

**Re-pinning to a newer release:** repeat the same `gh api` (or
`git ls-remote`) resolution against the new tag, replace the SHA in every
`uses:` line across every workflow that references that action, keep the
`# vN` comment in sync with the new tag, and re-run the YAML-parse check
below before committing.

**YAML-parse verification (always run after touching any workflow file):**

```bash
cd pipeline && uv run python -c "
import yaml, pathlib
for f in sorted(pathlib.Path('../.github/workflows').glob('*.yml')):
    yaml.safe_load(f.open())
    print(f.name, 'OK')
"
```

This is a syntax check only (confirms the YAML itself parses); it does not
validate GitHub Actions semantics (job/step schema, expression syntax) — a
`workflow_dispatch` run or a real push is still the actual correctness
signal for that.

### Locked dependencies

`pipeline/requirements.lock` (runtime-only) and `pipeline/requirements-dev.lock`
(runtime + the `dev` extra — pytest, mypy, ruff, pandas-stubs,
types-requests, etc.) are both generated FROM `pipeline/pyproject.toml`'s
declared dependencies, not from `pip freeze`-ing a venv's incidental
installed extras. Workflows install from whichever lock matches what that
job actually does:

- **Runtime jobs** (`data-daily.yml`, `data-monitor.yml`,
  `data-crosscheck.yml` — these only ever run `python -m pipeline <cmd>`,
  never pytest/mypy/ruff): `pip install -r requirements.lock -e . --no-deps`.
  `--no-deps` is required here — without it, `pip install -e .` would
  re-resolve `pyproject.toml`'s dependency ranges itself (ignoring the lock
  entirely for the package's own direct deps) the moment it installs the
  local editable package; `-r requirements.lock` first satisfies every
  pinned version, and `--no-deps` on the `-e .` install means "register this
  package in editable mode, trust the lock for its dependencies, don't
  second-guess it."
- **CI test job** (`pipeline-ci.yml` — runs `ruff check`, `mypy`, `pytest`):
  `pip install -r requirements-dev.lock -e . --no-deps`, same shape, dev
  lock instead — this preserves the exact same installed set that
  `pip install -e ".[dev]"` used to produce (proven by installing the dev
  lock into a brand-new venv and running the full gate against it before
  this landed: full suite + mypy + ruff all passed identically).

**Tool choice: `uv pip compile`** (not `pip-compile`/`pip-tools`) — `uv` was
already present on the implementation machine and in the pinned `.venv`'s
toolchain (see the RUNBOOK's own `uv run`/`uv venv` usage above), whereas
`pip-compile` was not installed and would have needed a separate
`pip install pip-tools` step; `uv pip compile` reads a `pyproject.toml`
directly (no separate `requirements.in` needed) and produces the same kind
of pinned, `pip install -r`-installable output.

**Regenerating the locks (`just relock`-style procedure — no justfile in
this repo, so this IS the command; run both from `pipeline/`):**

```bash
cd pipeline
uv pip compile pyproject.toml -o requirements.lock --python-version 3.11 \
    --no-header --no-annotate
uv pip compile pyproject.toml --extra dev -o requirements-dev.lock --python-version 3.11 \
    --no-header --no-annotate
```

`--no-header`/`--no-annotate` keep the lockfiles as a flat, portable list of
`name==version` pins with no embedded absolute paths or "resolved via"
provenance comments (which would otherwise leak whatever local path the
command happened to be run from into a checked-in file).

**Read this before blindly re-running the bare command above the day a real
dependency bump is needed:** the bare `uv pip compile pyproject.toml` command
resolves to the NEWEST version satisfying each range in `pyproject.toml`
(`uv`'s default resolution strategy is `highest`) — at the time this hardening
task was done, that resolved `pandas` to `3.0.3`, a MAJOR version bump over
the `2.2.3` this codebase has actually ever been tested against (the G2 plan
explicitly defers "pandas-3.x CI matrix" work — this codebase is not yet
validated against pandas 3.x). Locking that unvalidated version would have
meant "hygiene: lock the dependencies" silently became "ship an untested
major-version bump" the next time CI ran. **The lock therefore pins to the
exact versions already proven by the full gate in the pinned `.venv`**
(`pandas==2.2.3`, `pandera==0.32.1`, etc.) via a constraints file passed to
`uv pip compile --constraints <file>`, so the dependency GRAPH still comes
from `pyproject.toml` (transitive deps, extras, markers — all real
resolution) while the specific versions match what's actually been run.
**When you intentionally want to bump a dependency** (e.g. adopting pandas
3.x once that CI matrix work lands), do it deliberately: bump the version in
a scratch constraints file (or temporarily tighten the range in
`pyproject.toml`), regenerate, run the FULL gate
(`pytest -q && mypy && ruff check .`) against a FRESH venv installed purely
from the new lock, and only commit the new lock once that's green — never
let an unreviewed transitive bump ride in silently via a bare re-lock.

**Verifying a regenerated lock before committing it** (install into a
throwaway venv, never the working `.venv`, and run the full gate):

```bash
uv venv /tmp/lock-verify --python 3.11
uv pip install -r requirements-dev.lock -e . --no-deps --python /tmp/lock-verify/bin/python
/tmp/lock-verify/bin/python -m pytest -q
/tmp/lock-verify/bin/python -m mypy
/tmp/lock-verify/bin/python -m ruff check .
rm -rf /tmp/lock-verify
```

### Holiday/calendar-refresh — automated nag (new) + existing manual procedure

The manual refresh procedure itself is unchanged — see the "Yearly" section
near the top of this document for exactly what to edit and the
`holidays.json`/`special_sessions.json` formats. What's new in G2 task 8 is
two independent tripwires that make it much harder to forget:

1. **`.github/workflows/holidays-refresh.yml`** — a yearly cron (December
   1st, 06:00 UTC, plus `workflow_dispatch` for an on-demand check) that
   opens (or comments on, if already open) a deduped GitHub issue labeled
   `pipeline-maintenance` naming next year explicitly ("refresh
   holidays.json for {next year}"). This is a NAG, not an automatic fix —
   it deliberately does not scrape NSE or write the JSON files itself (NSE's
   holiday circular is a PDF/webpage, not a stable API — see the "Yearly"
   section's own manual-refresh instructions); it exists purely so the
   reminder shows up in the repo's Issues tab once a year without anyone
   having to remember the date.
2. **`freshness.holidays_need_refresh(holidays, today) -> bool`** (pure
   function, `pipeline/src/pipeline/freshness.py`) — wired into
   `cli.cmd_check_freshness`, so the DAILY/regular `check-freshness` monitor
   run (`data-monitor.yml`, 07:30 IST every day) ALSO fails (exit 1, clear
   message naming the missing year) once `today` is on/after December 1st
   AND `holidays.json` has no entry dated in `today.year + 1` yet. This is
   independent of and in addition to the yearly nag issue above — the nag
   nudges a human once a year; this check pages the SAME on-call channel
   (`data-monitor`'s existing `pipeline-failure` alert issue) every day
   starting Dec 1 until the refresh actually lands, so it can't be
   dismissed/forgotten the way a single once-a-year issue comment might be.
   Rule recap: before Dec 1, never flagged (too early — NSE's circular for
   next year may not even be published yet, so nagging earlier would just be
   noise nobody can act on); on/after Dec 1, flagged until a next-year
   holiday is present in `holidays.json`, then silent again until next
   December.

**Operator note:** refreshing `holidays.json` (and `special_sessions.json`
alongside it, per the existing "Yearly" instructions) is the ONE action that
silences both of the above simultaneously — there is no separate
acknowledgment step for either.

### Hygiene: strict release-404 match + orphaned `.tmp` sweep

**`release.py`'s `GhReleaseClient.exists()`** now matches the literal
substring `"HTTP 404"` in `gh`'s stderr (not a bare `"404"`) to decide "the
release genuinely does not exist" versus "gh failed for some other reason
that happens to mention 404 in passing" (a rate-limit retry-after value, a
request id, etc.) — the bare substring match was too loose and risked
silently downgrading a real, unrelated failure into a false "release absent"
result. `"HTTP 404"` is `gh api`'s actual, observed stderr format for a
not-found response (e.g. `gh: Not Found (HTTP 404)`) and is treated as the
contract; there is no looser fallback guard by design — anything else
raises `ReleaseError` rather than being silently absorbed.

**`store.sweep_orphan_tmp(base, *, older_than_hours=24) -> int`** cleans up
orphaned crash-write `.tmp` siblings: `append_keyed`'s (and therefore
`append_day`'s, which delegates to it) atomic-write pattern always writes to
a `*.parquet.tmp` sibling before atomically replacing the real target, so a
crash between those two steps (killed process, disk full, ...) can leave a
torn `.tmp` file behind forever — nothing in the normal write path ever
revisits it otherwise. `sweep_orphan_tmp` is called automatically at the top
of every `append_keyed` call (both direct callers and `append_day` callers,
since `append_day` is a thin wrapper over `append_keyed` — the sweep is
implemented once, at the shared entry point, not duplicated in both), so a
stale orphan is cleaned up as a side effect of the very next normal write,
with no separate maintenance step required. It recursively globs
`*.parquet.tmp` under `base` (reaching the `deltas/` subdirectory too),
unlinks anything older than `older_than_hours` (default 24h) by mtime, and
is deliberately best-effort: an unlink failure for one file is warned to
stderr and skipped, never raised — a sweep hiccup must never block an
otherwise-healthy ingest write. A fresh `.tmp` file (younger than the
threshold) is always left alone, since it may be the CURRENT in-flight write
of a still-running process.

## G3 Task 4: Monthly snapshots (disaster recovery)

### What a snapshot is
A monthly snapshot is a point-in-time, **immutable** copy of whatever the
live `data-latest` release references at the moment it's taken, published as
its own GitHub Release tagged `data-snapshot-YYYYMM` (e.g.
`data-snapshot-202607`). `pipeline/src/pipeline/snapshot.py`'s
`create_snapshot` downloads `data-latest`'s current `manifest.json`, verifies
every asset it references (baseline files AND deltas, across every dataset
the manifest lists — sha256-checked byte-for-byte, the same verify discipline
`sync.py`/`publish.py` use) into a scratch work directory, then uploads that
exact same set of files plus the manifest itself to a brand-new release under
the month's tag. This is a **verbatim copy of the manifest's own references**,
not a re-derivation through today's registered dataset specs — a snapshot
taken before a dataset was renamed/retired in code still faithfully preserves
what was actually live at snapshot time.

**Immutable by design:** `create_snapshot` refuses to recreate a month's tag
that already exists (raises `UnexpectedFailure`) rather than silently
overwriting it. The monthly cadence means two runs should never target the
same `YYYYMM` in practice, but if it ever did (a bug, a manual re-run, clock
skew), the run fails loudly instead of corrupting an archival copy that later
disaster recovery may depend on.

### Keep-6 retention
Every snapshot run also calls `prune_snapshots`, which deletes every
`data-snapshot-*` tag except the **newest 6** (a lexical sort on the
zero-padded `YYYYMM` suffix is already chronological order — no date parsing
needed). `data-latest` (and anything else not prefixed `data-snapshot-`) is
never a candidate for deletion; the prefix filter excludes it before any
delete logic runs. At steady state this keeps roughly six months of monthly
recovery points available at all times.

### Automation
`.github/workflows/data-snapshot.yml` runs monthly (1st of the month, 04:00
UTC, plus `workflow_dispatch` for an on-demand run), invoking `python -m
pipeline snapshot` — which runs `create_snapshot` against the live
`data-latest` release, then `prune_snapshots`, printing what was created and
what was pruned. Exits 1 (and opens/appends the standard
`pipeline-failure`-labeled issue, same dedupe pattern as
`data-daily.yml`/`data-monitor.yml`/`data-crosscheck.yml`) on any
`ReleaseError`/`UnexpectedFailure` — e.g. a transient `gh` failure, or (should
it ever happen) an attempt to recreate an existing month's tag.

### Listing current snapshots
```
gh release list --repo SatioO/guardian-universe | grep data-snapshot-
```

### Running manually
```
cd pipeline && uv run python -m pipeline snapshot
```

### Restoring from a snapshot (forward reference)
A snapshot is only useful if it can actually be restored — that tooling
(`restore-from-snapshot --tag data-snapshot-YYYYMM`, plus a rehearsable DR
drill procedure) is **Task 5**, not this task. See the "Disaster recovery
drill" section (added by Task 5) for the exact restore command, what a
successful drill looks like, and the separate, explicitly-flagged real-
recovery procedure. This task (4) only produces and retains the snapshots
themselves.

## G3 Task 5: Disaster recovery drill

### What `restore-from-snapshot` does
`pipeline/src/pipeline/restore.py`'s `restore_from_tag` downloads and
sha256-verifies a release/snapshot tag's `manifest.json`, then two-phase
restores that manifest's **baseline** files (only — deltas are never
downloaded or restored, same posture as `sync.py`: a restore rebuilds from
baselines, deltas are a live-client catch-up mechanism, not a DR concern)
into `target_root / dataset_name / logical_name`. It is deliberately
independent of `sync.py`'s dataset-registry routing (see `restore.py`'s
module docstring) — a restore target is an arbitrary directory tree, a
scratch dir for a drill or the real `data/` tree for an actual recovery, not
necessarily today's live `DATASETS` registry.

**Two-phase, verify-all-before-materialize-any:** every baseline asset across
every dataset is downloaded and sha256-checked into a scratch work directory
*before* anything is written to `target_root`. If even one asset fails its
checksum, `restore_from_tag` raises `UnexpectedFailure` and `target_root` is
never created/touched at all — a torn restore (some datasets materialized,
others silently missing) would itself be a disaster-recovery disaster, so
this guarantees either every verified file lands or none do.

### The scratch-safe-by-default rail
`pipeline restore-from-snapshot --tag <tag>` run with **no `--target`**
resolves the restore destination to a fresh scratch subdirectory —
`config.DATA_DIR / "_restore_drill" / <tag>` — **never** the live `data/`
tree. This is what makes a "drill" (rehearsal) safe to run against
`data-latest` or any `data-snapshot-YYYYMM` tag at any time, by anyone, with
zero risk of clobbering production data: the default always lands somewhere
disposable. Pointing `--target` at the real `data/` directory is an explicit,
deliberate opt-in — see "Real recovery procedure" below — never something
that happens by omission.

### Running a drill
```
cd pipeline && uv run python -m pipeline restore-from-snapshot --tag data-snapshot-202607
```

A successful drill prints the resolved scratch target followed by one line
per restored dataset, e.g.:
```
restore-from-snapshot: target /path/to/guardian-universe/pipeline/data/_restore_drill/data-snapshot-202607
  ohlc: latest_date=2026-07-03 bytes=48213112
  indices: latest_date=2026-07-03 bytes=1882044
  reference: latest_date=2026-07-03 bytes=934211
  ca_flags: latest_date=2026-07-03 bytes=51022
```
Exit code 0. To confirm the drill actually restored trustworthy bytes (not
just "some files landed"), spot-check that the restored parquet files' own
shas match the snapshot's manifest — `restore_from_tag`'s own checksum gate
already guarantees this for every file that landed (any mismatch would have
raised `UnexpectedFailure` before any file was written), so a clean exit 0 is
itself sufficient evidence of a passing drill; the manual spot-check is a
belt-and-suspenders confirmation, not a requirement for the drill to "count".
The roadmap's G3 exit criterion — "restore-from-snapshot rehearsed once" — is
satisfied the first time this command completes cleanly against a real tag.

A failed drill (a corrupted asset, a network/`gh` failure, an unreachable
tag) exits 1 with `restore-from-snapshot failed: ...` on stderr, and —
because of the two-phase guarantee above — leaves **no** `_restore_drill/
<tag>/` directory behind at all. Re-running the same command is always safe:
each drill run targets a tag-scoped subdirectory, so nothing needs cleaning
up between attempts, and a from-scratch retry after fixing the underlying
issue (network, `gh` auth, ...) starts from a clean slate automatically.

### Real recovery procedure (separate — human-only, explicitly flagged)
**This is a distinct, higher-stakes procedure from the drill above — it is
NEVER automated, never run from cron/CI, and only ever performed by a human
after confirming the live release is genuinely unusable** (e.g. `data-latest`
itself is corrupted, deleted, or has been overwritten with bad data, and
`pipeline sync`/normal client consumption is actively broken as a result).
Do not reach for this because a drill "seemed like a good time to actually
restore" — the scratch-by-default drill above exists specifically so real
recovery is never needed just to rehearse.

1. **Confirm the live release is actually unusable** before doing anything
   else — check `gh release view data-latest --repo SatioO/guardian-universe`
   and/or `pipeline check-freshness`/`pipeline sync` output. If `data-latest`
   is merely stale (a missed cron cycle) rather than corrupted, the fix is a
   normal `daily`/backfill catch-up run, NOT a restore.
2. Pick the most recent trustworthy tag — normally the newest
   `data-snapshot-YYYYMM` (see "Listing current snapshots" above), or
   `data-latest` itself if IT is intact but the *local* `data/` tree is what's
   damaged.
3. **Explicitly pass `--target` at the real data directory** — this is the
   ONLY thing that distinguishes this from a drill:
   ```
   cd pipeline && uv run python -m pipeline restore-from-snapshot \
     --tag data-snapshot-202607 --target /absolute/path/to/guardian-universe/pipeline/data
   ```
   Double-check the printed `--target` path before confirming — there is no
   additional interactive confirmation prompt in the tool itself; the
   explicit `--target` flag IS the deliberate, hard-to-do-by-accident opt-in.
4. After it exits 0, verify: re-run `pipeline check-freshness`, and confirm
   the restored `latest_date`s printed in the summary match what's expected
   for the tag chosen. Remember deltas were NOT restored — the restored store
   only reaches the snapshot's baseline `latest_date`; run a normal `daily`/
   backfill catch-up afterward to bring it current again.
5. Record the incident (what broke, which tag was restored, who performed
   it, when) — a real recovery is a significant operational event, not a
   routine action.
