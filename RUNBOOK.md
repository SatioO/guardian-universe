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
