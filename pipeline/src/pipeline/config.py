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

# Coarse full-market sanity bound for the indices dataset -- calibrated live
# in Task 9 against a real ind_close_all CSV (precedent: the equities
# (1800, 3000) live fix).
INDICES_ROWCOUNT_ABS_RANGE: tuple[int, int] = (50, 500)

# Project root = the pipeline/ directory (two parents up from this file's src/pipeline/).
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
DATA_DIR: Path = PROJECT_ROOT / "data"
OHLC_DIR: Path = DATA_DIR / "ohlc"
INDICES_DIR: Path = DATA_DIR / "indices"
REFERENCE_DIR: Path = DATA_DIR / "reference"
CA_FLAGS_DIR: Path = DATA_DIR / "ca_flags"
META_DIR: Path = DATA_DIR / "meta"

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
