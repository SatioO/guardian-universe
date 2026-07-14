# pipeline/scripts

Offline, human-run scripts. Not imported by the pipeline; not run in CI.

## `harvest_nse_industry.py` — full-universe sector/industry seed

### Why
The scanner classifies each stock by industry from `sector_industry_all.parquet`.
Historically that parquet was built from NSE's **Nifty-Total-Market** index CSV,
which lists only ~750 index constituents — so ~1500 of the ~2258 tradable
symbols had **no** industry and were excluded from every industry/sector filter
(the app's "Industry known for 743 of 2258 symbols" disclosure).

NSE publishes **no bulk per-security classification file**. The full 4-tier
classification (macro → sector → industry → basicIndustry) exists only in the
per-symbol `quote-equity` API, which is anti-bot gated and unreliable from CI
IPs. So we harvest it **once, offline, from a machine that can reach NSE**
(your laptop) and commit the result as a static seed CSV. `build_sector_industry`
reads that seed on every pipeline run — classifying the **whole** universe.

Classification is near-static, so re-running is infrequent (see *Refresh* below).

### Run it
```bash
cd pipeline
source .venv/bin/activate          # or: rye run / uv run — your project venv

python scripts/harvest_nse_industry.py --limit 25   # smoke test first (~25 symbols)
python scripts/harvest_nse_industry.py              # full run (~2000 symbols, ~15–25 min)
```
- Writes `seeds/sector_industry_seed.csv` (path = `config.SECTOR_SEED_PATH`).
- **Resumable**: re-running skips symbols already in the output. Failures are
  logged to `seeds/sector_industry_seed.failures.txt`; retry just those with
  `--retry-failed`.
- `--sleep 0.6` if NSE rate-limits you (default 0.4s between requests).

### After the run — review, then commit
The script prints a summary: total rows, and every distinct `industry` value
with its cyclical flag. **Eyeball it** — confirm coverage jumped to ~full
universe and the cyclical sectors (Metals & Mining, Oil Gas & Consumable Fuels,
Automobile and Auto Components, …) are flagged. Then:
```bash
git add seeds/sector_industry_seed.csv
git commit -m "chore(sector): harvest full-universe NSE 4-tier industry seed"
```

> **Ordering matters.** The pipeline's default sector source is now the seed. On
> a *fresh* build with no seed committed and no prior parquet, `sector_industry`
> fail-closes to empty (an honest "no data", never a bad overwrite). So **commit
> the seed before/with deploying** the code change — then the next pipeline run
> publishes a full-coverage `sector_industry_all.parquet`.

### Tier mapping (NSE's 4 tiers → our 3 columns)
Chosen to keep the app's existing behaviour bit-for-bit while filling the two
columns that used to ship NULL:

| parquet column   | NSE tier        | example                        |
|------------------|-----------------|--------------------------------|
| `sector`         | macro           | `Energy` (was NULL)            |
| `industry`       | **sector**      | `Oil, Gas & Consumable Fuels`  |
| `basic_industry` | basicIndustry   | `Refineries & Marketing` (was NULL) |

`industry` keeps the NSE **sector**-tier vocabulary the Total-Market CSV used, so
the app's industry filter and the `is_cyclical` derivation are unchanged — only
their coverage grows. NSE's 3rd "industry" tier is dropped (3 columns can't hold
4 tiers). `is_cyclical` is derived punctuation-tolerantly
(`nse_sector.is_cyclical_seed`) so the API's `"Oil, Gas & …"` still matches the
Total-Market `"Oil Gas & …"` cyclical set.

### Refresh cadence
Re-run when you want new listings classified (they're absent until harvested) —
monthly, quarterly, or after a batch of IPOs. It's just: run → review → commit.
The nightly pipeline itself never fetches this; it only reads the committed seed.

### Follow-up (optional, client-side)
`sector_industry_all.parquet` now carries a populated `sector` column, but the
app currently reads only `industry` + `is_cyclical`. Surfacing the coarser
`sector` (and finer `basic_industry`) in the scanner UI is a separate,
app-side change in `traderview` — the data is ready when you want it.
