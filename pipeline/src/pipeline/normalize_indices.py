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

    Note:
        Close-only indices (total-return indices like Nifty50 TRI, and
        fixed-income/G-Sec indices) don't trade intraday, so NSE publishes a
        literal "-" placeholder for Open/High/Low Index Value and often for
        Volume/Turnover on those rows. These are normalized to flat bars
        (open = high = low = close) with volume/value 0; provenance
        (instrument_key, symbol, source) is unchanged. Closing Index Value
        itself is never "-" in practice and stays a strict float parse: a
        row with no usable close is genuinely bad data and must fail loudly.
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

    # Close: strict float parse (fail loud on non-numeric). Close-only indices
    # never omit this in practice; a "-" here means the row is unusable.
    df["close"] = df["Closing Index Value"].astype(float)

    # OHLC open/high/low: close-only indices (TRI/G-Sec) publish literal "-"
    # here since they don't trade intraday. Coerce non-numeric -> NaN, then
    # fall back to the (already-strict) close, i.e. a flat bar.
    df["open"] = pd.to_numeric(df["Open Index Value"], errors="coerce").fillna(df["close"])
    df["high"] = pd.to_numeric(df["High Index Value"], errors="coerce").fillna(df["close"])
    df["low"] = pd.to_numeric(df["Low Index Value"], errors="coerce").fillna(df["close"])

    # prevclose = close - Points Change
    # May go negative when Points Change > close (small-base indices); such rows
    # are quarantined upstream of the schema gate — see
    # test_negative_prevclose_index_row_is_quarantined_not_fatal.
    df["prevclose"] = df["close"] - df["Points Change"].astype(float)

    # Volume: "-" (close-only indices) or NaN -> 0, int64
    df["volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype("int64")

    # Value (Turnover): "-" (close-only indices) or NaN -> 0.0, float
    df["value"] = pd.to_numeric(df["Turnover (Rs. Cr.)"], errors="coerce").fillna(0.0).astype(float)

    # Fixed fields
    df["isin"] = ""
    df["series"] = "INDEX"
    df["trades"] = 0
    df["source"] = source

    return df[config.CANON_COLUMNS].reset_index(drop=True)
