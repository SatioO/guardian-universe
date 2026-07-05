"""Raw NSE indices CSV -> canonical long OHLC frame. Pure."""
from __future__ import annotations

import pandas as pd

from pipeline import config
from pipeline.errors import UnexpectedFailure
from pipeline.sources.nse_indices import INDICES_RAW_COLUMNS

# Raw columns this normalizer depends on.
_REQUIRED_RAW = set(INDICES_RAW_COLUMNS)


def normalize_indices(raw: pd.DataFrame, source: str = "nse-indices") -> pd.DataFrame:
    """Normalize raw NSE indices CSV to canonical schema.

    Args:
        raw: Raw indices DataFrame with columns from INDICES_RAW_COLUMNS
        source: Data source identifier (default: "nse-indices")

    Returns:
        Normalized DataFrame with exactly config.CANON_COLUMNS

    Raises:
        UnexpectedFailure: If required columns are missing
    """
    missing = _REQUIRED_RAW - set(raw.columns)
    if missing:
        raise UnexpectedFailure(f"indices missing required columns: {sorted(missing)}")

    df = raw.copy()

    # Parse date with strict format
    df["date"] = pd.to_datetime(df["Index Date"], format="%d-%m-%Y")

    # Extract and clean symbol (strip whitespace)
    df["symbol"] = df["Index Name"].str.strip()

    # Create instrument_key: IDX: + symbol.upper().replace(" ", "")
    df["instrument_key"] = "IDX:" + df["symbol"].str.upper().str.replace(" ", "")

    # OHLC prices: convert to float (fail loud on non-numeric)
    df["open"] = df["Open Index Value"].astype(float)
    df["high"] = df["High Index Value"].astype(float)
    df["low"] = df["Low Index Value"].astype(float)
    df["close"] = df["Closing Index Value"].astype(float)

    # prevclose = close - Points Change
    # May go negative when Points Change > close (small-base indices); such rows
    # are quarantined upstream of the schema gate — see
    # test_negative_prevclose_index_row_is_quarantined_not_fatal.
    df["prevclose"] = df["close"] - df["Points Change"].astype(float)

    # Volume: NaN -> 0, int64
    df["volume"] = df["Volume"].fillna(0).astype("int64")

    # Value (Turnover): NaN -> 0.0, float
    df["value"] = df["Turnover (Rs. Cr.)"].fillna(0.0).astype(float)

    # Fixed fields
    df["isin"] = ""
    df["series"] = "INDEX"
    df["trades"] = 0
    df["source"] = source

    return df[config.CANON_COLUMNS].reset_index(drop=True)
