"""Project-wide constants and path helpers."""
from __future__ import annotations

from pathlib import Path

SCHEMA_VERSION = 1

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

# Validation thresholds. The real NSE EQ universe is ~2384 symbols (2026); the abs
# range is only a coarse full-market sanity bound (a truncated file is far smaller) —
# the deviation gate does the fine day-to-day anomaly detection.
ROWCOUNT_ABS_RANGE: tuple[int, int] = (1800, 3000)
ROWCOUNT_DEVIATION: float = 0.15

# Project root = the pipeline/ directory (two parents up from this file's src/pipeline/).
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
DATA_DIR: Path = PROJECT_ROOT / "data"
OHLC_DIR: Path = DATA_DIR / "ohlc"
META_DIR: Path = DATA_DIR / "meta"


def dataset_path(year: int, base: Path, *, prefix: str = "ohlc") -> Path:
    """Path to a dataset's year-partitioned parquet file: {prefix}_{YYYY}.parquet."""
    return base / f"{prefix}_{year}.parquet"


def ohlc_path(year: int, base: Path | None = None) -> Path:
    """Equities shim (kept for existing callers): ohlc_{YYYY}.parquet."""
    return dataset_path(year, base if base is not None else OHLC_DIR, prefix="ohlc")


# Distribution (P1a): rolling GitHub Release served by GitHub's CDN.
GITHUB_REPO = "SatioO/guardian-universe"
RELEASE_TAG = "data-latest"
