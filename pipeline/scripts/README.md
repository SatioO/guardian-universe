# pipeline/scripts

Offline, human-run scripts. Not imported by the pipeline; not run in CI.

## `harvest_bse_industry.py` — full-universe sector/industry seed (PRIMARY)

### Why BSE (and why not NSE)
The scanner classifies each stock by its sector tiers from `sector_industry_all.parquet`.
Historically that parquet was built from NSE's **Nifty-Total-Market** index CSV,
which lists only ~750 index constituents — so ~1500 of the ~2360 tradable
symbols had **no** classification.

The full **SEBI/AMFI 4-tier** classification (macro → sector → industry →
basicIndustry) is the same on both exchanges. NSE only exposes it via its
per-symbol `quote-equity` API, which is behind **Akamai Bot Manager's JavaScript
sensor** — unreachable from *any* HTTP client (`requests`, `curl_cffi` with a
real Chrome TLS fingerprint, and even a headed Chromium's in-page fetch all get
403; see the diagnostics in `harvest_nse_industry.py --dump/--probe`).

**BSE exposes the identical 4-tier data via a plain JSON endpoint that is NOT
JS-gated.** So we harvest from BSE and key by **ISIN**, which is exchange-neutral.

### Tier mapping (BSE field → our column; identical to NSE's tiers)
| our column       | BSE field (`ComHeadernew`) | NSE tier      | example (RELIANCE)          |
|------------------|----------------------------|---------------|-----------------------------|
| `sector`         | `IndustryNew`              | sector        | Oil, Gas & Consumable Fuels |
| `industry`       | `IGroup`                   | industry      | Petroleum Products          |
| `basic_industry` | `ISubGroup`                | basicIndustry | Refineries & Marketing      |

BSE `Sector` ("Energy") is NSE's coarsest `macro` tier and is dropped, exactly as
the NSE mapping drops macro. `is_cyclical` is derived from `sector`
(punctuation-tolerantly, so BSE's `"Oil, Gas & …"` matches the cyclical set).

### Run it
```bash
cd pipeline
source .venv/bin/activate

python scripts/harvest_bse_industry.py --limit 25 --out /tmp/smoke.csv  # smoke test
python scripts/harvest_bse_industry.py                                  # full run (~30–45 min)
```
- One bulk call lists every active equity scrip (BSE code + ISIN + symbol); a
  per-scrip call fetches its tiers. **Resumable** (skips ISINs already written);
  failures → `seeds/sector_industry_seed.failures.txt`, retry with `--retry-failed`.
- Writes `seeds/sector_industry_seed.csv` (= `config.SECTOR_SEED_PATH`), the SAME
  file the pipeline reads. `sector` is the required tier; finer tiers may be NULL.

### Coverage check (how much of NSE is covered)
BSE is a near-superset of NSE by ISIN (most stocks dual-list), but a few NSE-only
listings may be missing. Measure it exactly:
```bash
python scripts/check_nse_coverage.py     # covered / missing NSE symbols vs the seed
```
For any residual NSE-only gap, top up from NSE's Total-Market CSV (reachable CDN,
coarser sector-tier only) or accept it (those stay unclassified, fail-closed).

### After the run — review, then commit
The harvest prints a distinct-`sector` + cyclical summary. Eyeball it, then:
```bash
git add seeds/sector_industry_seed.csv
git commit -m "chore(sector): full-universe industry seed (BSE, SEBI 4-tier)"
```

> **Ordering matters.** The pipeline's default sector source is the seed. On a
> *fresh* build with no seed committed and no prior parquet, `sector_industry`
> fail-closes to empty (honest "no data", never a bad overwrite). Commit the seed
> **before/with** deploying the code change.

### Refresh cadence — automated (you no longer run this locally)
The **`sector-refresh`** GitHub Actions workflow
(`.github/workflows/sector-refresh.yml`) owns the refresh: **weekly** (Sundays)
+ **monthly** (1st, baseline) + manual dispatch, in its own 60-min job off the
daily critical path.
It does a **full** re-harvest (`--fresh` — re-fetches *every* scrip, so
reclassifications of existing stocks are caught, not just new listings),
shrink-guards against a flaky BSE run, commits the refreshed
`seeds/sector_industry_seed.csv` to `main`, and the next `data-daily` run reads
it and publishes the updated `sector_industry_all.parquet` to the release. The
local `python scripts/harvest_bse_industry.py` commands above remain only for
ad-hoc testing.

## `harvest_nse_industry.py` — NSE harvester (DEPRECATED: Akamai-blocked)
Kept for reference and for its `--dump`/`--probe` diagnostics, which document why
the NSE API is unreachable (Akamai Bot Manager JS sensor). Do not use for
harvesting — it returns 403. Use `harvest_bse_industry.py`.

## `try_bse.py` / `try_nsepython.py` — one-off probes
Diagnostics used to find a reachable source. `try_bse.py` confirms BSE's list +
per-scrip tier fields; `try_nsepython.py` confirms the NSE library is also
Akamai-blocked.
