# traderview pipeline

EOD data producer for the scanner. See
`docs/superpowers/specs/2026-07-04-scanner-data-pipeline-design.md`.

Dev: `pip install -e ".[dev]"` then `pytest`, `ruff check .`, `mypy`.

## NSE classification registry

`python -m pipeline classification-collect` reads NSE's active EQ/BE list and
collects the four-tier Screener classification only for new symbols, renamed
symbols, and due retries. It never drains the initial backlog on the daily
schedule.

The first production run must be an explicit `--mode baseline` dispatch. This
creates and publishes the registry state that lets every later daily run tell
the initial backlog apart from genuinely new listings.

- `--mode baseline --batch-size 25` advances the approved, rate-limited
  initial baseline by one bounded batch.
- `--mode quarterly` runs a resumable full-active-universe audit (up to 100
  25-symbol batches by default), retaining one-second pacing across batches
  and stopping on a block/rate-limit response.

The command persists ISIN-keyed registry state and a JSON health report under
`data/meta/`. It then attempts the scanner artifact build, but publication is
intentionally blocked until the existing coverage and taxonomy guards pass.
`.github/workflows/sector-refresh.yml` runs the daily and quarterly modes,
syncs/publishes registry state under the shared release lock, and opens the
standard failure issue for a block or unexpected build failure.
