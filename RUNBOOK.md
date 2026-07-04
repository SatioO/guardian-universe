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

## Alerts
- Row-count deviation / format break → run exits non-zero (fail-closed).
- Corrupt day (all rows quarantined) → `status: failed`, nothing written, retryable.

## Yearly
- Refresh `pipeline/data/meta/holidays.json` from NSE's published holiday calendar.
