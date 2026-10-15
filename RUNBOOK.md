# RUNBOOK — guardian-universe

## Price datasets retired (2026-10-13) — read first
guardian-universe no longer publishes NSE prices. The desktop app builds price
history on the trader's own device and never downloads them.

- **Gone from `manifest.json`:** `ohlc` (`ohlc_*` year files and deltas),
  `indices` (`indices_*`), and `ca_flags` (derived only from those prices).
  `datasets.RETIRED_MANIFEST_NAMES` names them. The first publish after the
  change drops them, printing `publish: dropping retired dataset '<name>'`.
  They are never carried forward as another producer's dataset, and their
  absence is not a shrink-guard fault.
- **Still fetched, never published:** the equities and indices specs
  (`DatasetSpec.published=False`). `daily` fetches the 7-session catch-up
  window into `data/ohlc` and `data/indices`, stores that live only for the
  run. `sync` skips them, so the stores start empty on every runner and each
  rung re-fetches the window. The bhavcopy's status still gates the publish.
  They feed two kept lists:
  - `reference`: the window's sessions are merged onto the prior published
    `instruments_all.parquet` (see the next section);
  - `index_constituents`: the `IDX:` keys come from the latest indices
    session in the run-local store.
- **Manifest date:** `reference` is now the publish's primary
  (`datasets.published_specs()[0]`). The empty-store guard and the top-level
  `latest_trading_date` both read its latest session: the session the app
  judges freshness by. The publish ladder (crons, gate, timings) is unchanged.
- **Removed:** the `ca_flags` builder, the weekly `cross-check` command and
  workflow (`data-crosscheck.yml`), and the trailing-window continuity half
  of `check-freshness`. Staleness and the calendar nag remain.
- **Rowcount gate:** the per-series rowcount gate's trailing history is now
  only the sessions fetched earlier in the same run's window (up to 6).
  - Still caught: a truncated day (the absolute bounds, plus a per-series
    collapse against the window's earlier days) and a wrong-dated file.
  - What changes: a window's oldest session is checked against the absolute
    bounds alone.
- **Orphaned assets (NOT deleted, by design):** GC claims assets only by a
  published spec's prefix, so these stay until someone deletes them by hand:
  - on `data-latest`: `ohlc_*`, `indices_*`, `ca_flags_*`, `delta_ohlc_*` and
    `delta_indices_*`;
  - inside every `data-snapshot-*` release taken before 2026-10-13.

  They are the rollback and bootstrap source below. Delete them only once
  nobody needs that path.

## Reference bootstrap (cold start, broken prior, rollback)
`daily` builds `reference` by merging the run's sessions onto the PRIOR
published instrument list. It refuses (`failed`, nothing written, job red)
when that prior is:
- missing, unreadable, missing a column, empty, or undated;
- truncated: it lacks more than `config.REFERENCE_PRIOR_MAX_NEW_SHARE` (25%)
  of the versions the run fetched. The historical worst 7-session churn is
  10.3%.

A list built from the window alone would drop every listing that did not trade
that week and reset every `first_seen`. The way back is a full rebuild from the
price files still on a release:

- **From CI (preferred, under the publish lock):** dispatch `data-daily` with
  `bootstrap_reference_from` set to a tag, e.g. `data-latest` or
  `data-snapshot-202610`, and `date` set to the last trading day if today is
  not one. It runs sync → `bootstrap-reference` → `daily` → publish.
- **Locally:**
  ```
  cd pipeline
  python -m pipeline sync
  python -m pipeline bootstrap-reference --tag data-latest   # or a data-snapshot-* tag
  python -m pipeline daily [--date YYYY-MM-DD]
  python -m pipeline publish
  ```

How it works:
- **Which files:** a release whose manifest still lists `ohlc` (a snapshot)
  is read through that manifest, each file sha256-verified. `data-latest`
  (whose manifest no longer lists it) is read from its asset list: per year,
  the newest `ohlc_{YYYY}.{sha8}.parquet`, verified against the sha8 in its
  name.
- **What it writes:** only the local `instruments_all.parquet`; it never
  uploads.
- **The prior:** by default, a well-shaped prior is merged in, so listings
  first seen after the price files end survive. `--ignore-prior` replaces it.
- **The gap:** sessions between the price files' last day and the catch-up
  window are not in either source. If that gap is longer than 7 sessions, run
  `python -m pipeline backfill --days N --dataset equities` before `daily`, in
  the same local workspace.
- **Proof** (2026-10-13, real data): the incremental merge (prior ≤
  2026-10-09, plus 2026-10-12) and this full rebuild both reproduce the
  published `instruments_all` byte for byte (sha256 `078501a5…`).

**Rollback of the retirement:** revert the change. Then put the orphaned
history back: either `restore-from-snapshot` from the last snapshot, or a
manifest reinstatement by hand from the orphaned `data-latest` assets. Then
backfill the gap since then. This is why no price asset was deleted.

## NSE token on the instrument list (2026-10-14)
`reference/instruments_all.parquet` (manifest `schema_version` 1 -> 2) gains
`nse_token`: nullable int64, appended as the last column, NSE's exchange-issued
`FinInstrmId`. A share gets a new token when it changes series (EQ -> BE), so
the token is per version row, beside `isin` and `series`. The app uses it to
link a listing's old and new versions.

- **Source:** only the UDiFF CM bhavcopy carries `FinInstrmId`. The
  `sec_bhavdata_full` fallback, the legacy CM bhavcopy and the broker rebuild
  have no token, so a day they serve records null. A null never erases a token
  already recorded; a new non-null one replaces it (a re-issue).
- **Malformed value** (not an integer, zero, negative, over 18 digits): set to
  null, a `nse_token:` warning in the run log, and the count in the status
  message (`N malformed nse_token value(s) nulled`, visible in
  `last_run_status.json`). It never fails the day: null only means lineage
  unknown.
- **Structural change fails the run:** a UDiFF file with no `FinInstrmId`
  column stops that day (`bhavcopy missing required columns`), because it
  would silently null every token.
- **History starts 2026-10-14.** The first run fills every version that traded
  in its 7-session window; older versions stay null. No backfill was run. To
  fill more, from a residential machine (NSE blocks datacenter IPs; UDiFF
  files exist from 2024-01-01, earlier days come from sources without tokens):
  ```
  cd pipeline
  python -m pipeline sync
  python -m pipeline backfill --days 300 --dataset equities   # N sessions back
  python -m pipeline daily
  python -m pipeline publish
  ```
  `daily` rebuilds `reference` from every session in that local store, merged
  onto the synced prior.
- **Old prior and old price files:** a prior without the column is still valid
  (it gains the column); `bootstrap-reference` reads price files that predate
  it, with every token null.

## Bootstrap the historical baseline (local, one-time)
Historical: the price store is run-local since 2026-10-13 (see above), so a
backfill only matters inside a reference bootstrap. NSE blocks datacenter IPs,
so run backfill from a residential machine:
```
cd pipeline && uv run python -m pipeline backfill --days 300
```
Resumable — re-run to continue after an interruption (already-ingested days skip).

## Publish to the CDN (rolling GitHub Release)
Requires `gh` authenticated with write access to `SatioO/guardian-universe`:
```
cd pipeline && uv run python -m pipeline publish
```
Uploads the published reference lists (`datasets.published_specs()`) +
`data/meta/manifest.json` to the `data-latest` release. No price file is
published since 2026-10-13. Clients read `manifest.json`, verify each file's
`sha256`, then download.

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
  assets are garbage-collected 7 days after upload — but only assets in *this*
  producer's namespace (see shared-release ownership below).
- `publish` refuses to: shrink coverage (fewer rows/years, older
  latest_trading_date), publish over a release that changed since sync
  (re-run the pipeline), or leave an unverified manifest (it re-downloads and
  checks itself after the flip).
- Recovery from a failed publish: nothing to clean up — the old manifest is
  still live and consistent; just re-run sync → daily → publish.
- **Shared-release ownership.** `manifest.json` is a WHOLE-RELEASE document
  that several producers write (data-daily, fundamentals-daily, and any
  sibling repo publishing into `data-latest`), but each runner rebuilds it
  from its own store scoped to its own dataset registry. **Ownership is
  registry membership**: a runner verifies and rewrites the datasets its specs
  describe, and copies every other producer's entries through *verbatim*
  (`carry_forward_foreign_datasets`, the dataset-granularity twin of
  `carry_forward_deltas`). So the shrink-guard's "missing locally" error means
  a dataset **we** produce vanished — always a real fault, never someone
  else's dataset. Consequences to know:
  - Each carried dataset prints a `publish: carrying foreign dataset '<name>'
    forward` line to stderr on **every** publish. That is the only signal a
    foreign producer has been abandoned — if a name shows up there for weeks
    with a frozen `latest_date`, chase the producer, don't ignore the line.
  - A foreign dataset is carried only while **every** asset it references is
    still on the release. A producer retires a dataset by dropping its entry
    (or its assets); we never resurrect or permanently pin one. A
    `publish: NOT carrying foreign dataset '<name>'` line means the entry is
    unusable, so clients stop seeing that dataset until its producer repairs
    it — the bytes are not deleted, but nobody else will notice, so chase it.
  - **GC is scoped the same way**, one level down, in the flat asset
    namespace (`owns_asset`): it deletes an unreferenced, aged-out asset only
    if a registered spec's `file_prefix` claims the name
    (`{prefix}_*`, or `delta_{prefix}_*`). `manifest.json` is *not* the only
    index on `data-latest` — a sibling producer keeps its own
    (`producer_manifest.json`, covering `earnings_announcements` and
    `board_meetings`), and those assets are unreferenced by us permanently and
    by design. "Unreferenced by our manifest" is therefore not evidence an
    asset is dead. A `gc: leaving N unreferenced asset(s) outside this
    runner's namespace alone` line each run is normal; **N climbing steadily**
    means a producer stopped cleaning up after itself and those bytes are now
    a manual sweep.
  - Corollary: **retiring** one of our datasets from `datasets.DATASETS`
    orphans its assets forever, because nothing then claims their prefix.
    Delete them by hand as part of the retirement. (Leaking bytes is
    recoverable; deleting another producer's is not — the rule errs that way
    deliberately.)
  - The rule self-retires per dataset: register the dataset in
    `datasets.DATASETS` and `sync` materializes it, `build_manifest` emits it,
    ownership flips (both halves — the carry stops, and GC starts claiming its
    assets) — the `fundamentals` pattern. Nothing to delete afterwards.

## Yearly
- Refresh `pipeline/data/meta/market_calendar.json`, the ONE market calendar: the
  pipeline's own trading days and the `market_calendar` dataset the desktop app
  syncs both read it (see "Market calendar" below). Curate it by hand from the
  exchanges' circulars; never scrape.
  - **Closures**, per venue (`venues.NSE/BSE/MCX.closures["YYYY"]`), from each
    exchange's trading-holiday circular for the coming year (NSE capital market,
    BSE equity, MCX commodity). MCX lists partial days separately: a morning- or
    evening-session closure is a `session` with that day's hours, not a closure.
  - **Special sessions** (`sessions`): every day that trades on a weekend or a
    closure (Union Budget, Diwali Muhurat) or on other hours, with exchange-local
    `open`/`close` and, where there is one, the call-auction `preOpen`. List the
    day as soon as the holiday circular names it; the exchanges notify Muhurat
    timings only weeks before, so add the hours then. Until they are added the
    builder names the day ("awaiting hours: NSE 2026-11-08") on every run, and
    the app treats it as closed.
  - **`coveredYears`**: add the year once its closures are complete and every
    special session is listed. The app reads any year not covered as unknown.
- The `holidays-refresh` workflow opens a reminder issue every December 1st, and
  `check-freshness` starts failing then if next year's NSE closures are missing.

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

### Derived datasets: `reference/instruments` (and the retired `ca_flags/`)
Two more registry entries, `REFERENCE` and `CA_FLAGS` (retired 2026-10-13,
see the top of this file), both `derived=True`.
Derived specs have **no fetcher** — `daily`'s CLI loop runs in two phases:
first the fetched specs (equities, indices) as before, then — only for a
full `--dataset all` run, and only when the primary (equities) status came
back healthy — the derived specs, built from the local store via
`builders.BUILDERS[key](spec, target)`:

- **`reference/instruments`** (`builders.build_reference`) — the SCD2
  symbol master, rewritten whole each run. Since 2026-10-13 it is built by
  merging the run's sessions onto the prior published list; see "Reference
  bootstrap". One row per distinct `(instrument_key, symbol, series)`
  version seen in the equities sessions, with `first_seen`/`last_seen`/
  `valid_from`/`valid_to` and a v1 `status` (`active` if seen within the last
  10 trading days, else `inactive`; `suspended`/`delisted` need an exchange
  reference feed and are deferred to P4a).
- **`ca_flags/`** (retired 2026-10-13; was `builders.build_ca_flags`) —
  corporate-action ex-date detector: for each trading day, joins today's `prevclose` against the
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

### ca_flags (retired 2026-10-13)
`ca_flags` was derived from the published prices only, and went with them.
The client's ex-date signal is the `corporate_actions` dataset (NSE's
book-closure list, below).

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

Since 2026-10-13 the equities store is run-local and never published, so a
rebuilt day only reaches anything published (the `reference` merge) if
`daily` and `publish` run in the same workspace afterwards. Retiring
`rebuild-day` and `backfill` is a planned follow-up.

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

## G2: weekly source cross-check and continuity monitor (retired 2026-10-13)
Both checked the published price datasets: the weekly cross-check compared
closes between the two NSE sources, and the continuity half of
`check-freshness` looked for missing sessions in the published `ohlc` and
`indices` files. With prices no longer published, neither guards anything a
client reads, so `data-crosscheck.yml`, the `cross-check` command and the
continuity check are gone. `check-freshness` (data-monitor) keeps its
staleness check on the manifest's `latest_trading_date` (now the reference
list's session) and the yearly calendar nag.

### Cron keep-alive (`.github/workflows/keepalive.yml`)
GitHub automatically disables a scheduled workflow's cron trigger after 60
days with no repository activity — a real risk for a low-traffic repo where
the only activity IS the scheduled crons themselves creating a silent
bootstrapping problem (crons stop firing, so there's no more activity, so
they never get a chance to re-trigger). `keepalive.yml` runs monthly (1st,
05:00 UTC, plus `workflow_dispatch` for an on-demand check) and:
1. Runs `gh workflow enable data-daily.yml data-monitor.yml
   data-snapshot.yml || true` — a no-op if all three are already active,
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
(`data-daily.yml`, `data-monitor.yml`, `data-snapshot.yml`,
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
  `data-snapshot.yml` — these only ever run `python -m pipeline <cmd>`,
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
`data-daily.yml`/`data-monitor.yml`) on any
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
<tag>/` directory behind at all, **for a phase-1 (checksum/verification)
failure**. Re-running the same command is always safe in that case: each
drill run targets a tag-scoped subdirectory, so nothing needs cleaning up
between attempts, and a from-scratch retry after fixing the underlying issue
(network, `gh` auth, a corrupted asset, ...) starts from a clean slate
automatically.

A materialize-time I/O failure (disk full, permissions — phase 2, after
every file already verified) is a different case: it prints
`restore-from-snapshot failed (materialize error): ... — delete <target> and
retry` on stderr and exits 1, and it **can** leave a partially-materialized
`_restore_drill/<tag>/` directory (some files landed before the failure,
later ones missing). Delete that directory before retrying — the restore
itself is idempotent (a clean re-run re-verifies and re-materializes
everything), but the leftover partial directory is not otherwise cleaned up
for you.

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

   If it instead exits 1 with `restore-from-snapshot failed (materialize
   error): ...` (a real recovery hit a phase-2 disk-full/permissions failure
   against the live `data/` tree), do NOT assume `data/` is untouched — the
   same partial-materialize caveat as the drill above applies, except here
   the partial directory IS the production `data/` tree. Delete/clear it and
   re-run the exact same command from step 3 before doing anything else; the
   restore is idempotent, so a clean re-run fully repairs it.
5. Record the incident (what broke, which tag was restored, who performed
   it, when) — a real recovery is a significant operational event, not a
   routine action.

## P5 Phases 2+3: fundamentals-daily (Rust producer → shared publish)

`fundamentals-daily.yml` runs the Rust producer (`fundamentals/`) nightly at
20:30 IST Mon–Fri and publishes `fundamentals_all.parquet` into the SAME
`data-latest` manifest as everything else (spec `fundamentals`, registered in
`pipeline/src/pipeline/datasets.py`). It shares the `data-pipeline-publish`
concurrency group with data-daily, so two publishes can never interleave.

- **First full run (once, manual):** `gh workflow run fundamentals-daily.yml
  -f window=3m` — fetches the current quarter's filings for the full covered
  universe (~700–1200 symbols, ≥1.5 s throttle, expect 40–70 min). The nightly
  cron then runs `window=week` incrementally.
- **Coverage:** mcap floor ≥₹800 cr (scrip-master `Mktcap`), hysteresis exit
  at ₹720 cr. Below-floor / no-mcap (NSE-only) counts are disclosed in the run
  summary, never silently dropped.
- **State:** `fundamentals_state.json` on the release (mutable, clobbered,
  GC-protected via `publish.PROTECTED_ASSETS`) holds locator-level dedup keys.
  Uploaded strictly AFTER a successful publish. Safe to delete: the producer's
  merge is byte-idempotent, so a stateless run re-fetches its window politely
  and changes nothing.
- **Quiet day:** producer exits 0 with `parquet_written=false` → publish step
  is skipped → zero release churn.
- **Red run:** canary failures (truncated scrip master / empty filing-season
  discovery), ALL-selected-filings-failed, or a publish error — all alert via
  the shared `pipeline-failure` issue. One bad symbol never fails the run
  (logged + skipped, retried next run if the failure was a fetch).
- **Smoke test locally:** `cargo run --release -- --limit 100 --window week
  --out /tmp/fnd` from `fundamentals/` (never point `--out` at
  `pipeline/data/fundamentals` locally unless you intend to merge into the
  synced store).
- **Phase 3 (sector parsers):** bank / NBFC / insurance instances route
  through the app's vendored per-sector builders and publish rows with the
  nullable sector-union columns (`interest_earned`, `net_interest_income`,
  `gross_npa_pct`, premium lines, …). The only remaining sector skip is an
  unclassifiable fingerprint (`skipped_unclassified` — still design D6:
  never guess). The state file is **v2** (`version: 2`): every processed
  dedup key records its outcome (`published` / `skipped_unclassified` /
  `identity_mismatch` / `parse_error` / `gate_blocked`) so future bumps can
  invalidate one class only. On the first Phase-3 run over a v1 state the
  producer migrates automatically — invalidating exactly the entries of
  symbols with no published rows (the Phase-2 non-general backlog) and
  re-fetching only those; already-published general filings are not
  re-fetched. Instance-ISIN mismatches: O↔0 typos normalize silently; a
  scrip-anchored same-issuer mismatch (corporate-action ISIN change)
  publishes under the universe key with `identity_isin_mismatch(...)` in
  `dq_flags`; anything unanchorable is skipped. No workflow change: same
  parquet file, same state asset, nullable-column schema evolution (the
  producer reads pre-Phase-3 parquets leniently; clients read by name and
  fail closed on NULL).

## P5 Phase 4: fundamentals-backfill (historical quarters + growth columns)

`fundamentals-backfill.yml` (`workflow_dispatch` only) walks the full filing
history of one hash-mod shard of the covered universe — per-symbol
`Corp_FinanceResult_ng` `SCRIP_CD=<scrip>&FlagDur=7` discovery (one ~40–80 KB
request per symbol returns its COMPLETE broadcast history, SOURCE-CONTRACT
§14) — and fetches every era filing missing from the accumulated parquet.
Merge semantics are identical to the daily run (existing rows never dropped;
restatements replace in place), and it publishes into the same manifest under
the same `data-pipeline-publish` lock.

**Era depth (probed 2026-07-12, §14):** the integrated-filing (in-capmkt) era
starts with the March-2025 quarter; before that, filings are
`FourOneUploadDocument/Main_*.xml` instances in BSE's own `in-bse-fin`
2020-03-31 taxonomy — which the SAME parser handles (identical OneD/FourD
contexts + local element names; proven on a real RELIANCE Q3-FY25 instance
with values matching the published results). The default era floor
`from=2023-04-01` therefore yields **~12–14 quarters + 2–3 FY rows** per
symbol. Filings broadcast before `from` are skipped WITHOUT fetching and
recorded per symbol (`BackfillMark.pre_era_skipped`) — never null-published.
Going deeper than FY24 is an explicit `-f from=…` choice; taxonomy
generations before 2020 are UNVERIFIED (expect more `parse_errors` /
`skipped_unclassified`, all outcome-recorded, never guessed).

### Running a backfill campaign

Shards run **sequentially** (they'd serialize on the publish lock anyway, and
politeness is per-run). Prefer dispatching outside the 20:30-IST daily window
so the nightly run isn't queued behind a shard.

```bash
# 1. Smoke first (one tiny slice, ~10 min):
gh workflow run fundamentals-backfill.yml -f shard_index=0 -f total_shards=4 -f limit=15

# 2. The campaign — one shard at a time, waiting for each to finish:
for i in 0 1 2 3; do
  gh workflow run fundamentals-backfill.yml -f shard_index=$i -f total_shards=4
  sleep 10
  gh run watch "$(gh run list -w fundamentals-backfill.yml -L1 --json databaseId -q '.[0].databaseId')"
done
```

- **Keep `total_shards` constant across the campaign** — sharding is FNV-1a
  hash-mod over `instrument_key`, so a changed total remaps symbols (safe —
  `BackfillMark`s are per symbol, not per shard — but it muddies "shard 2 is
  done" bookkeeping).
- **Resume:** the producer stops itself cleanly at `--max-runtime-mins 300`
  (< the 360-min job timeout) and publishes what it got. If the run summary
  shows `backfill_complete: false` (the workflow emits a warning), re-dispatch
  the SAME `shard_index` — completed symbols are skipped at zero requests via
  their `BackfillMark` in `fundamentals_state.json`; only unfinished symbols
  are re-scanned.
- **Re-running a completed shard** is a cheap no-op (every symbol
  short-circuits on its mark; parquet byte-identical → publish skipped).
- **Deeper era later:** dispatch with an older `-f from=…`; symbols re-scan
  because their recorded mark's `from` is newer than the requested one, and
  already-processed filings are still skipped per dedup key — only newly
  in-era documents are fetched.
- **Failure modes:** a transient fetch error leaves the symbol unmarked (fully
  retried on re-dispatch); an HTTP 404 on a historical locator is recorded as
  `instance_unavailable` (permanent — never re-requested); pre-2020-taxonomy
  or exotic instances land in `parse_errors` / `skipped_unclassified` with
  their outcome recorded. All are disclosed in `run_summary.json`.

### Verifying a shard

```bash
python3 - <<'EOF'
import duckdb
con = duckdb.connect()
print(con.sql("""
  SELECT symbol,
         count(*) FILTER (fiscal_quarter != 'FY') AS quarters,
         min(period_end) AS oldest, max(period_end) AS newest,
         count(revenue_growth_yoy_pct) AS yoy_populated
  FROM read_parquet('pipeline/data/fundamentals/fundamentals_all.parquet')
  GROUP BY symbol ORDER BY quarters DESC LIMIT 20
"""))
EOF
```

Expect covered symbols to gain up to ~12–14 quarter rows;
`revenue_growth_yoy_pct` populates wherever the year-ago quarter exists
(first-era quarters stay NULL — correct, not a bug). `ttm_eps_method` says
how each TTM was derived (`sum4q` = true 4-quarter sum; `fy_eps` = FY-row
fallback). Growth columns and TTM are derived, recomputed from the full
history on EVERY producer run (the daily run too) — they carry no state and
can never go stale. Guard semantics: growth is NULL when the base period is
missing or ≤ 0 (loss→profit is NULL, never +∞); profit→loss publishes as a
decline below -100%.

## NSE classification registry

`.github/workflows/sector-refresh.yml` now maintains the Screener-backed
four-tier Classification Registry. The first production run must be dispatched
as `mode=baseline` and is intentionally limited to one sequential batch of at
most 25 active NSE EQ/BE symbols. Re-dispatch that same mode to advance the
backlog; do not use the daily mode until the first baseline state has been
published.

The scheduled daily run only handles newly active symbols, renames,
reactivations, and due retries. The quarterly run performs up to 100 paced
25-symbol batches and reports `audit_complete` in its persisted release status
asset. A `false` value, a collector deferral, or an unexpected sector-artifact
failure opens/appends the standard `pipeline-failure` issue; re-dispatch the
quarterly mode to resume. Scanner sector data is published only after the
active-universe coverage and taxonomy gates pass; registry state and raw
Screener provenance are still retained on every successful collection.

## CAS-eligible stocks (`cas_eligible`)

`cas_eligible_all.parquet` (manifest name `cas_eligible`, schema v1) lists every
stock NSE's Closing Auction Session covers — each cash stock with derivative
contracts (CAS live 2026-08-03; circulars CMTR73362, CMTR74466, CMTR75479).
Columns: `symbol`, `isin`, `date` (the trading date the list is as of),
`source` (`nse-fo-mktlots`). One current snapshot, rewritten whole.

`builders.build_cas_eligible` runs in the daily Phase-2 loop after `reference`.
It fetches NSE's F&O market-lot file
(`nsearchives.nseindia.com/content/fo/fo_mktlots.csv`) on every trading-day run,
keeps the stock underlyings (below NSE's "Derivatives on Individual Securities"
marker, or in the pipeline's equity universe; index underlyings are in neither),
and joins each symbol's current ISIN from `reference/instruments_all.parquet`.
Non-trading days skip with `skipped_holiday`.

| Outcome | Status | File |
|---|---|---|
| fetch error, HTML/PDF served with 200, < `CAS_MIN_ROWS` (100) stocks, < 90% of rows resolve an ISIN | `skipped_idempotent` (`failed` on a first run) | prior kept |
| fewer stocks than the published file (shrink-guard) | **`failed`**, names the symbols that left | prior kept |

A shrink is loud on purpose. A genuine F&O exit (a stock's last contract
expired) is the usual way this list gets shorter, and it repeats on every run
until accepted. The publish shrink-guard would block the whole release if the
shorter file were published, so the builder holds it back and the daily job
goes red. If the named symbols really have left F&O (check NSE's exclusion
circular), accept the shorter list from a machine NSE does not block, outside
the data-daily cron window:
```
cd pipeline && uv run python -m pipeline sync
rm data/cas/cas_eligible_all.parquet
uv run python -m pipeline daily            # on a weekend/holiday: --date <last trading day>
uv run python -m pipeline publish --allow-shrink
```
`--allow-shrink` relaxes the row check for every file in that publish, so read
its warnings: only `cas_eligible_all.parquet` should shrink. If many symbols
"left" at once and NSE announced nothing, it is a bad response: wait for the
next run.

## F&O freeze limits (`fno_freeze_limits`)

`fno_freeze_limits_all.parquet` (manifest name `fno_freeze_limits`, schema v1)
holds, per F&O underlying, the largest single order NSE takes (its quantity
freeze). The desktop app slices orders and protective stops by it: a stop above
the freeze is rejected at the moment it fires, so a limit too HIGH is the one
failure that matters. Columns: `symbol`, `kind` (`index` | `stock`),
`lot_size`, `max_order_quantity` (units, a whole number of `lot_size`, except a
stand-in, which may be less than a lot; the app rounds it down to its own
contract's lot and reads anything below one lot as one lot), `basis`
(`exchange`, or `stand_in` where the file's limit cannot be trusted), `date` (the day of the
contract file: NSE's file D lists the contracts of session D+1; carried rows
keep their own older date, so read the file's date as its maximum), `source`
(`nse-fo-contract`). One current snapshot, rewritten whole.

NSE stopped its separate quantity-freeze report on 2024-04-15 (circular
NSE/FAOP/61157; the old `qtyfreeze` path now redirects to it). The limit is in
the daily MII contract file,
`nsearchives.nseindia.com/content/fo/NSE_FO_contract_DDMMYYYY.csv.gz`, posted in
the evening of D (18:30-20:00 IST seen), as `MaxTradQty`: the first quantity
that freezes, so the largest order is one less (NIFTY 3,511 → 3,510 units, 54
lots of 65). Every doubt resolves to a smaller limit: two values for one
underlying keep the smaller, two lot sizes round down to the larger lot, a limit
that is not a whole number of lots rounds down to one, and an underlying with
any row it cannot read (a lot, a limit, an instrument code), a limit below one
lot, or option series none of which is permitted to trade stands in at one lot,
or at the smaller limit the file does show. Every symbol the file lists gets a
row: only one absent from it is carried forward. `PrtdToTrad` is set on option series
only (every future reads 0). NSE's own test series (011NSETEST...181NSETEST)
list futures only and are never published; a real underlying that loses its
options keeps its row.

`builders.build_fno_freeze_limits` runs in the daily Phase-2 loop. It reads the
latest contract file on or before the run date, walking back up to
`FNO_CONTRACT_LOOKBACK_DAYS` (7) days on a 404 only. Non-trading days skip with
`skipped_holiday`.

| Outcome | Status | File |
|---|---|---|
| a file of the published day or newer (an evening correction included) | `success` | rewritten |
| an older file than the published one (a back-dated `daily --date`) | `skipped_idempotent` | kept |
| underlyings no longer listed, up to `FNO_FREEZE_LIMITS_MAX_CHURN` (3, or 3% of the file) newly gone this run | `success`, names them | their rows carried forward (never shrinks); rows carried before do not count again |
| stand-ins up to the same churn | `success`, names them | written at one lot |
| more stand-ins or missing underlyings than that (a format change, a partial file) | **`failed`** | nothing written, prior kept |
| fetch error, 403/500, any document that is not the contract file, < `FNO_FREEZE_LIMITS_MIN_ROWS` (100) underlyings | `skipped_idempotent` (`failed` on a first run) | prior kept |
| limits a trading day or more behind (file D serves session D+1), whatever the cause | **`failed`** | the newest available kept |
| no contract file at all in the lookback window | **`failed`** | prior kept |

A carried row belongs to an underlying with no contracts left, so it can size
nothing; it stays only so the publish shrink-guard never blocks the release
over a routine F&O exit. Purge them by hand when they pile up (remove the file
and run `daily`, then `publish --allow-shrink`, as for `cas_eligible`). A run
that fails on churn names the symbols: if they really changed (a large F&O
exit), accept the same way.

## BSE F&O freeze limits (`bse_fno_freeze_limits`)

`bse_fno_freeze_limits_all.parquet` (manifest name `bse_fno_freeze_limits`,
schema v1, in `data/fno` beside NSE's file) holds, per BSE F&O underlying AND
product, the largest single order BSE takes (its "Single Order Quantity
Limit"; notices 20230526-54, 20230621-48, 20231201-16). It is a separate file
on purpose: installed apps read `fno_freeze_limits` as one NSE limit per
symbol, so BSE rows there would hand NSE contracts BSE's limits. Never change
that file's shape for BSE.

Columns: `symbol` (BSE's `TckrSymb`), `kind` (`index` | `stock`), `product`
(`futures` | `options`), `underlying_id` (BSE's code for the underlying,
`UndrlygFinInstrmId`: SENSEX 1, BANKEX 12, SENSEX50 47, FOCIT 75, a stock's
scrip code; 0 when unreadable), `lot_size`, `max_order_quantity` (units, AS
BSE GIVES IT: often not a whole number of lots, e.g. ADANIENT lot 309, limit
9,000; the app rounds down to its own contract's lot), `basis`, `date` (the
day the contract file names; file D serves session D+1), `source`
(`bse-eqd-contract`). The key is (`symbol`, `product`). One current snapshot,
rewritten whole. **`ISIN` is blank in every row of BSE's file**, so match an
underlying by `underlying_id` (or symbol), never by ISIN.

The source is the rolling zip
`https://www.bseindia.com/downloads1/Live_Masters_for_Equity_Derivatives.zip`
(about 2.4 MB, written around 17:40 IST; there is no dated archive), member
`BSE_EQD_CONTRACT_<ddmmyyyy>.csv`, column `MaxTradQty`. Fetching: bseindia.com
answers python-requests' own User-Agent with **HTTP 403**; a browser User-Agent
gets the file. No Referer or cookie is needed (measured 2026-10-15; curl's own
User-Agent also gets 200).

How it differs from NSE's leg:
- `MaxTradQty` is the largest order BSE TAKES (orders "more than" it are
  rejected), so nothing is subtracted. NSE's is the first quantity that freezes.
- Futures and options keep their own limits (SENSEX 500/1000, BANKEX 600/900,
  FOCIT 900/1350 on 2026-10-14); they are never merged by keeping the smaller.
- BSE's "not set" value (100,000,000 on 75 stocks on 2026-10-14; anything from
  10,000,000 up) is never a limit: the product is published at one lot with
  `basis` = `no_exchange_limit`. Those rows are not stand-ins and do not count
  against churn. The run names how many underlyings show it.
- `PrtdToTrad` is not read (0 on every future and most options), and BSE lists
  no test series.

Within a product the same doubts resolve smaller as on NSE: two limits keep the
smaller, two lots take the larger, and a product with an unreadable row, a
limit below one lot, an unknown instrument code on the underlying (either
product) or a missing or split `underlying_id` stands in at one lot
(`basis` = `stand_in`).

`builders.build_bse_fno_freeze_limits` runs in the daily Phase-2 loop, right
after `fno_freeze_limits`, under the same guards (`_build_freeze_limits`),
with churn counted in rows (underlying × product). The NSE table above applies,
except that a missing or refused file is a fetch failure (there is no
walk-back), and `BSE_FNO_FREEZE_LIMITS_MIN_ROWS` (100 underlyings; 217 on
2026-10-14) is the floor. The file holds one row per (`underlying_id`,
`product`), since consumers match by it: a row the file no longer lists is
never carried forward when its `underlying_id` and product are listed under
another symbol (BSE renamed the `TckrSymb`; the run names them), so an old,
possibly higher limit never sits beside the new one. Three more refusals
(**`failed`**, nothing written, prior kept):

| Anomaly | Why |
|---|---|
| an index shows the "not set" value | an index always has a limit: a broken file |
| more products newly show "not set" than churn allows (vs. the published file) | a broken file must not read as "no limits" |
| two rows share an `underlying_id` (> 0) and product | a consumer could pick either limit (0 is "unreadable" and may repeat) |

If BSE really did clear many limits at once, accept it as for a churn failure:
remove `bse_fno_freeze_limits_all.parquet`, run `daily`, then publish.

## Market calendar (`market_calendar`)

`market_calendar_all.parquet` (manifest name `market_calendar`, schema v1) is the
exchange calendar the desktop app fires price alerts and draws sessions by. It is
built from the one curated file `pipeline/data/meta/market_calendar.json`, which is
also the pipeline's own trading calendar (`calendar.load_trading_calendar`).

Columns: `venue` (NSE, BSE, MCX; the app's NFO shares NSE), `day` (the calendar
date), `kind`, `pre_open_seconds`/`open_seconds`/`close_seconds` (exchange-local
seconds, sessions only), `label`, `source` (the circular), and `date` (the run's
as-of date, which the manifest reads). Kinds:

- `closed`: the venue is shut that whole day.
- `session`: the day trades these hours instead of the regular ones (a special
  session on a weekend or a closure, or a partial day). It replaces a closure on
  the same day. A day may hold several windows.
- `covered`: the venue's closures are complete for that year (`day` = 1 January).

`builders.build_market_calendar` rewrites the file on every daily run, holiday or
not; nothing is fetched. It fails closed: a malformed or self-contradicting
calendar keeps the prior file (`skipped_idempotent`, or `failed` when there is
none), naming the problem.

**Accepting a shorter calendar.** A run whose calendar has fewer rows than the
published one reports `failed`, keeps the prior file and names the rows that went
away: a dropped closure or session silently changes when the app's alerts fire, and
the publish shrink-guard would block the whole release. When the removal is
deliberate (a wrongly listed holiday), delete
`pipeline/data/calendar/market_calendar_all.parquet` on the runner's store and
re-run; the next publish carries the corrected calendar.


## Corporate actions (`corporate_actions`)

`corporate_actions_all.parquet` (manifest name `corporate_actions`, schema v1) lists
the capital actions that re-base a stock's price, with their ex-dates. The desktop
app pauses a price alert before its stock opens ex, because its level was set
against the old prices (a bonus 1:1 halves the price and would fire every "below"
alert).

Source: the book-closure file `bc{DDMMYYYY}.csv` in NSE's daily PR bundle
(`nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{DDMMYY}.zip`, the
CI-safe archives host). NSE lists an action about one to two weeks before its
ex-date.

Columns: `symbol`, `ex_date`, `kind`, `price_factor` (post ÷ pre price ratio when
the purpose states it: a bonus 1:1 is 0.5, a split from ₹10 to ₹2 is 0.2; null for
rights, demergers and capital reductions), `purpose` (NSE's text, verbatim),
`first_seen` (the trading day NSE first listed it), and `date` (the run's as-of
date, which the manifest reads). Kinds: `split`, `consolidation`, `bonus`,
`rights`, `demerger`, `capital_reduction`. Dividends, interest, buy-backs,
distributions and partly-paid calls are left out, because the exchange does not
re-base a price for them.

`builders.build_corporate_actions` runs in the daily Phase-2 loop on trading days
and **accumulates**: each run adds what that day's file lists, keyed (symbol,
ex_date, kind), and an action keeps its first row. The table only grows, so the
publish shrink-guard never trips. A first build (no prior file) also reads the 20
trading days before it (`CORPORATE_ACTIONS_SEED_DAYS`), best effort, and names any
seed day it could not read.

| Outcome | Status | File |
|---|---|---|
| the day's bundle is missing (404), not a zip, or holds no well-formed `bc*.csv` | `skipped_idempotent` (`failed` on a first run) | prior kept |
| a purpose the classifier does not recognise | `success`, the purpose named in the message | written, without it |
| non-trading day | `skipped_holiday` | untouched |

**A purpose not recognised.** When the message names a purpose, decide whether it
re-bases a price. If it does, add its wording to
`sources/nse_corporate_actions.classify`, with a test in
`tests/test_builders_corporate_actions.py`. If it does not, add it to
`_NOT_A_RE_BASING`. A missed re-basing action is an alert that fires on the new
prices, so when in doubt, classify it.

**An action NSE withdraws** stays in the table. The app's only response is a pause
the trader resumes, so a stale row costs one click, while a deleted one could cost
a false fire.

## Corporate actions history (`corporate_actions_history`)

`ca_history/ca_history_{YYYY}.parquet` (manifest name `corporate_actions_history`,
schema v1, one file per ex-date year) lists **every** NSE corporate action on an
equity share (main board EQ/BE/BZ and Emerge SM/ST/SZ) since 2021-10-01:
dividends, meetings and buy-backs too, one row per action a purpose names. The
trader's app joins a stock's history across a break (a bar wholly outside the
price band) when an action for that ISIN falls on the break's day. It is **not**
`corporate_actions`: every row of that one pauses a price alert.

Columns: `symbol`, `series`, `isin` (see below), `isin_nse` (the ISIN NSE's API
printed, when it is a valid one), `company`, `ex_date`, `record_date`, `purpose`
(NSE's text, verbatim), `part` (0, 1, ... for the actions one purpose names),
`kind` (`split`, `consolidation`, `bonus`, `rights`, `demerger`, `scheme`,
`capital_reduction`, `dividend`, `other`), `ratio_new`/`ratio_held` (bonus,
rights, scheme "a:b"), `fv_from`/`fv_to` (split, consolidation), `premium`
(rights, Rs), `dividend_ps` (Rs per share), `source` (`nse-api` or
`nse-book-closure`), `first_seen` (the day this pipeline first recorded the row),
`date` (the run that last rewrote that year's file; the manifest reads it). A
number is only ever read from the purpose's own text; one it does not state
plainly is null, never guessed.

**`isin` is the ISIN that traded under the symbol on the ex-date**, from
`reference/instruments_all.parquet` (equity series only; an issuer's bonds trade
under its symbol too). NSE's API prints a company's original ISIN on about a
quarter of its rows (ONGC's INE213A01011, replaced in 2011; the pre-split ISIN
on a split's own ex-date) and a 6-digit number on Emerge rows, so its value is
kept as `isin_nse` and never trusted on its own. A share that did not trade on
its ex-date takes the first listing within 31 days after it (a split's new ISIN
often lists a few sessions later). A run that cannot resolve a row keeps the
ISIN it already had. About 1% of rows stay null (symbols the instrument list
never saw near that day); the status message counts them.

### First fill (residential machine, once)

NSE's corporate-actions API (`www.nseindia.com/api/corporates-corporateActions`)
blocks datacenter IPs, as every `www.nseindia.com/api/*` does, so the history is
filled from a machine NSE does not block, outside the data-daily cron window:
```
cd pipeline
uv run python -m pipeline sync                      # the instrument list it resolves ISINs from
uv run python -m pipeline backfill-corporate-actions   # --from 2021-10-01 --to today
uv run python -m pipeline publish
```
It reads month windows of both indexes (`equities`, `sme`), 3 s apart: about
130 requests, 7 minutes. Every window must parse before anything is written; a
malformed answer, an HTML page, an ex-date outside its window or an ended month
of the main board with no action at all fails the run with nothing written.
Expect about 13,800 rows in 6 files, under 0.5 MB. `publish` checks the live
release has not moved since `sync` (re-run all three if it has).

### Daily (CI)

`build_corporate_actions_history` runs in the Phase-2 loop after `reference`. It
reads the book-closure file `bc{DDMMYYYY}.csv` in the PR bundles of the last 5
trading days (`CA_HISTORY_CATCHUP_DAYS`) on the CI-safe archives host, adds an
action no row holds yet (`source = nse-book-closure`, the file's 25-character
purpose), and re-resolves every row's ISIN against the day's instrument list.
The file lists an action from as little as two days before its ex-date, so the
catch-up covers a run or two that never happened. Only year files whose rows
changed are rewritten.

| Outcome | Status | Files |
|---|---|---|
| no history yet (before the first fill is published) | `skipped_idempotent`, names the backfill | none written |
| the day's bundle missing (404), not a zip, no well-formed `bc*.csv` | `skipped_idempotent` | kept |
| an earlier catch-up day's bundle missing | `success`, the day named | written |
| instrument list missing or unreadable | `skipped_idempotent` | kept |
| a year file unreadable or missing a column | **`failed`** | kept |
| a purpose the classifier does not recognise | `success`, the purpose named; kept as `other` | written |
| non-trading day | `skipped_holiday` | kept |

### Top-up (residential machine, weekly)

Run it **once a week** (any day after the close, outside the cron window), from
the first day of the previous month:
```
cd pipeline
uv run python -m pipeline sync
uv run python -m pipeline backfill-corporate-actions --from 2026-09-01
uv run python -m pipeline publish
```
Within the window the API is the record. For the series the fetched indexes
answer for, its current rows replace the table's earlier API rows (a corrected
purpose replaces the old one, a withdrawn action goes, a row NSE left alone
keeps its `first_seen`), and a book-closure row whose ex-date has passed goes
unless the API lists it. Book-closure rows whose ex-date is still ahead stay:
the API lists only a day or so ahead.

**Between top-ups a consumer sees** the book-closure rows for the most recent
actions: the right ex-date, kind and ISIN, but NSE's abbreviated purpose
(`INTDIV - RS 12 PER SH`), and ratios only where those 25 characters hold them.
An action that appears only in the book-closure file (its ex-date later moved,
or it was worded differently) stays until the next top-up removes it. A week
keeps both small; skipping top-ups for months leaves the purposes abbreviated
and any withdrawn action in place.

**A top-up refuses** a month whose API rows would shrink by more than a
twentieth (and more than 3 rows): that is a truncated answer, not withdrawals.
Re-run later. A top-up that does withdraw rows makes that year's file shorter,
so `publish` stops at the shrink-guard. Read the backfill's `N rows replaced or
withdrawn` and, if it is small and NSE really dropped those actions, publish
with `--allow-shrink` (its warnings should name only `ca_history_*.parquet`).

**A purpose not recognised.** Add its wording to
`sources/nse_ca_history.classify` and a line to
`tests/fixtures/nse_ca_purpose_forms.csv` (the test pins every real wording's
kinds and numbers). A purpose the classifier misses is kept as `other`, which
the app would not use to join a history, so when in doubt, classify it.
