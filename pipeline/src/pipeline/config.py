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

# Validation thresholds.
ROWCOUNT_ABS_RANGE: tuple[int, int] = (1800, 2200)
ROWCOUNT_DEVIATION: float = 0.15

# Project root = the pipeline/ directory (two parents up from this file's src/pipeline/).
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
DATA_DIR: Path = PROJECT_ROOT / "data"
OHLC_DIR: Path = DATA_DIR / "ohlc"
META_DIR: Path = DATA_DIR / "meta"


def ohlc_path(year: int, base: Path | None = None) -> Path:
    """Path to the year-partitioned OHLC parquet file."""
    root = (base if base is not None else OHLC_DIR)
    return root / f"ohlc_{year}.parquet"
