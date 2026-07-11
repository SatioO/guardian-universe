"""Project-wide constants and path helpers."""
from __future__ import annotations

from pathlib import Path

SCHEMA_VERSION = 1
MANIFEST_VERSION = 2
MIN_CLIENT_VERSION = "0.1.0"

# Canonical long-format columns, in exact order.
CANON_COLUMNS: list[str] = [
    "date",
    "instrument_key",
    "isin",
    "symbol",
    "series",
    "open",
    "high",
    "low",
    "close",
    "prevclose",
    "volume",
    "value",
    "trades",
    "source",
]

# Validation thresholds. G1b task 4 widens the stored universe from EQ-only to
# ALL cash series (STK + final session), so the abs range widens accordingly —
# it remains only a coarse full-market sanity bound (a truncated file is far
# smaller); the per-series deviation gate does the fine day-to-day anomaly
# detection (see validate.check_rowcount_by_series).
ROWCOUNT_ABS_RANGE: tuple[int, int] = (2000, 10000)
ROWCOUNT_DEVIATION: float = 0.15

# G3 backfill live finding: a series with a trailing mean below this floor is
# too small for the deviation/absence gates to be statistically meaningful;
# real truncations manifest in the large series (e.g. EQ ~2384) and the
# abs-total gate, never in a sub-floor bucket's day-to-day wobble. Calibrated
# against real NSE bhavcopy (~55 tiny bond/misc series + the empty-series
# null-SctySrs bucket, all single-to-low-double-digit daily counts) during
# the G3 backfill.
SERIES_MIN_FOR_GATE: int = 50

# G3 300-day backfill live finding (Task 7): the real 300-day backfill
# failed the per-series DEVIATION gate on 68/300 days (23%) -- all on
# MID-SIZE series (50 <= trailing mean < 1000), a deeper layer than the
# SERIES_MIN_FOR_GATE sub-50 exemption above. These are real, observed
# values of natural policy-driven membership churn in NSE's
# surveillance/trade-to-trade/govt segments, NOT truncations:
#   BE ~266 -> 164..187 (up to -38%)
#   ST ~67-143 -> 78..120 (+-17-30%)
#   GS ~51 -> 36 (-29%)
#   GB ~51 -> 42 (-18%)
# Meanwhile the large, stable anchor series never wobbled beyond the tight
# band: EQ ~2384, SM ~302 -- zero failures at 15%. A single flat 15% band
# is statistically wrong across this size range: 15% of EQ's 2384 rows is
# a ~7-sigma event (a real truncation signal), while 15% of a 51-row
# surveillance segment is business-as-usual churn. Fix: size-tiered
# deviation tolerance. SERIES_LARGE_MEAN is the tight-band cutoff -- at or
# above this trailing mean, ROWCOUNT_DEVIATION (0.15) applies; today only
# EQ (~2384) qualifies, the dominant stable anchor where a 15% drop IS a
# real truncation signal.
SERIES_LARGE_MEAN: int = 1000

# G3 300-day backfill live finding (Task 7, continued): the loose band for
# 50 <= trailing mean < SERIES_LARGE_MEAN. Calibrated to tolerate the
# observed <=38% natural churn of surveillance/trade-to-trade/govt
# segments (BE/ST/GS/GB, see SERIES_LARGE_MEAN comment above) while still
# catching a >50% collapse as a real anomaly.
#
# Tightened from 0.60 -> 0.50 (Task 7 reviewer follow-up): 0.60 left a
# [38%, 60%) window where a real isolated mid-size truncation (e.g. a lone
# BE 266->130, -51%) would slip through silently -- neither the abs-total
# gate (thousands-scale total, unmoved by a ~266-row change) nor the
# completeness check (aggregate across all series, not per-series) would
# backstop it. 0.50 still clears the observed 38.3% max natural churn
# (BE) with ~12-point margin, while halving the undetected-truncation
# window this gate governs ALL future daily ingests, so a tighter band
# that occasionally requires manual review beats a looser one that can
# silently store bad data.
ROWCOUNT_DEVIATION_SMALL: float = 0.50

# Coarse full-market sanity bound for the indices dataset -- calibrated live
# in Task 9 against a real ind_close_all CSV (precedent: the equities
# (1800, 3000) live fix).
INDICES_ROWCOUNT_ABS_RANGE: tuple[int, int] = (50, 500)

# G2 Task 4: catch-up loop. Each `daily` run for a fetched spec re-checks the
# trailing N trading days (ascending, ending at the target day) via run_daily,
# not just the target day itself -- a missed day (both crons failed, a late
# bhavcopy, etc.) self-heals the next time `daily` runs, instead of becoming a
# permanent hole. Present days cost one cheap `has_day` idempotent-skip read.
CATCHUP_WINDOW_DAYS: int = 7

# G2 Task 5: completeness-aware idempotency. `has_day` alone ("*>=1* row
# exists for this day") used to lock a partial day in forever -- a short day
# (mid-fetch truncation, a fallback that only partially served the universe,
# etc.) would never be topped up because ANY stored row short-circuited every
# subsequent run. A present day now only skips when its stored total is
# within this fraction of the trailing total mean (sum of per-series trailing
# means); short days fall through to re-fetch instead, and append_keyed's
# keep="last" dedupe merges the top-up in safely. 0.15 mirrors
# ROWCOUNT_DEVIATION's existing tolerance band.
COMPLETENESS_SHORTFALL: float = 0.15

# Project root = the pipeline/ directory (two parents up from this file's src/pipeline/).
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
DATA_DIR: Path = PROJECT_ROOT / "data"
OHLC_DIR: Path = DATA_DIR / "ohlc"
INDICES_DIR: Path = DATA_DIR / "indices"
REFERENCE_DIR: Path = DATA_DIR / "reference"
CA_FLAGS_DIR: Path = DATA_DIR / "ca_flags"
SECTOR_DIR: Path = DATA_DIR / "sector"
# P5 fundamentals: produced by the EXTERNAL Rust producer
# (fundamentals/fundamentals-producer) into this directory; the pipeline
# syncs/publishes it like any other dataset but never builds it itself.
FUNDAMENTALS_DIR: Path = DATA_DIR / "fundamentals"
META_DIR: Path = DATA_DIR / "meta"

# Sector/industry reference (P4). Slow-moving NSE index-constituent list, so
# the daily run only re-fetches when the stored file's as-of date is older than
# this TTL -- a weekly refresh piggybacked on the daily cron (see
# builders.build_sector_industry). A fetch that returns fewer rows than the
# floor is treated as truncated/suspect and NOT written over a good prior file.
SECTOR_REFRESH_TTL_DAYS: int = 7
SECTOR_MIN_ROWS: int = 400

# Corporate-action ex-date detector (G1b task 7): flag an instrument when
# abs(prevclose_today / close_prev - 1) exceeds this fraction -- a split,
# bonus, or other ex-date discontinuity, not ordinary price movement.
CA_DISCONTINUITY_THRESHOLD: float = 0.005


def dataset_path(year: int, base: Path, *, prefix: str = "ohlc") -> Path:
    """Path to a dataset's year-partitioned parquet file: {prefix}_{YYYY}.parquet."""
    return base / f"{prefix}_{year}.parquet"


def ohlc_path(year: int, base: Path | None = None) -> Path:
    """Equities shim (kept for existing callers): ohlc_{YYYY}.parquet."""
    return dataset_path(year, base if base is not None else OHLC_DIR, prefix="ohlc")


# Distribution (P1a): rolling GitHub Release served by GitHub's CDN.
GITHUB_REPO = "SatioO/guardian-universe"
RELEASE_TAG = "data-latest"
