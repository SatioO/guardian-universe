"""NSE `sec_bhavdata_full` adapter — an independent NSE endpoint used as the
equities fallback source when the UDiFF primary is unavailable.

`secfull_to_udiff_shape` is a shape-adapter, not a normalizer: per the G2
Task 2 fallback contract (see fetch.py), it reshapes the secfull raw frame
into the PRIMARY's raw column shape (UDiFF's TradDt/ISIN/TckrSymb/... plus
SsnId="F1"/FinInstrmTp="STK") so the EXISTING `normalize_equity_bhavcopy`
consumes it unchanged. Provenance ("nse-secfull") is stamped later via
FetchResult.source (Task 1) -- this module never sets it."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date

import pandas as pd

from pipeline.errors import UnexpectedFailure

_BASE = "https://nsearchives.nseindia.com/products/content"

# Raw columns we depend on (subset of the real file -- it has more; verified
# live in G2 Task 9). Headers and string values carry stray whitespace in
# practice: both are stripped before use.
SECFULL_RAW_COLUMNS: list[str] = [
    "SYMBOL",
    "SERIES",
    "DATE1",
    "PREV_CLOSE",
    "OPEN_PRICE",
    "HIGH_PRICE",
    "LOW_PRICE",
    "CLOSE_PRICE",
    "TTL_TRD_QNTY",
    "TURNOVER_LACS",
    "NO_OF_TRADES",
]

_REQUIRED_RAW = set(SECFULL_RAW_COLUMNS)

# secfull raw column -> UDiFF raw column (the shape we adapt TO).
_UDIFF_COLMAP = {
    "SYMBOL": "TckrSymb",
    "SERIES": "SctySrs",
    "DATE1": "TradDt",
    "PREV_CLOSE": "PrvsClsgPric",
    "OPEN_PRICE": "OpnPric",
    "HIGH_PRICE": "HghPric",
    "LOW_PRICE": "LwPric",
    "CLOSE_PRICE": "ClsPric",
    "TTL_TRD_QNTY": "TtlTradgVol",
    "TURNOVER_LACS": "TtlTrfVal",
    "NO_OF_TRADES": "TtlNbOfTxsExctd",
}

_DATE1_FORMAT = "%d-%b-%Y"  # e.g. "03-Jul-2026" -- VERIFY-LIVE(T9)


def build_secfull_url(d: date) -> str:
    stamp = d.strftime("%d%m%Y")
    return f"{_BASE}/sec_bhavdata_full_{stamp}.csv"


def secfull_to_udiff_shape(
    raw: pd.DataFrame, *, isin_map: Mapping[tuple[str, str], str] | None = None
) -> pd.DataFrame:
    """Reshape a raw sec_bhavdata_full frame into the UDiFF primary's raw
    column shape so `normalize_equity_bhavcopy` can consume it unchanged.

    Args:
        raw: Raw secfull DataFrame with (at least) SECFULL_RAW_COLUMNS.
        isin_map: (symbol, series) -> ISIN lookup (secfull has no ISIN column
            of its own). Keying on (symbol, series) -- NOT symbol alone -- is
            essential: an issuer's bonds/NCDs (series N1-N9/NA-NE etc.) trade
            under the SAME symbol as its equity but have their OWN ISINs, so a
            symbol-only map would collapse every series onto the equity ISIN
            and the bond rows would displace the equity row for that date. A
            miss yields "" -- the downstream normalizer's "NSE:"+symbol
            sentinel takes over from there; this adapter never invents a key.

    Raises:
        UnexpectedFailure: a required raw column is missing.
    """
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]

    missing = _REQUIRED_RAW - set(df.columns)
    if missing:
        raise UnexpectedFailure(f"sec_bhavdata_full missing required columns: {sorted(missing)}")

    isin_map = isin_map or {}

    symbol = df["SYMBOL"].astype(str).str.strip()
    series = df["SERIES"].astype(str).str.strip()
    date1 = df["DATE1"].astype(str).str.strip()

    out = pd.DataFrame(index=df.index)
    out["TckrSymb"] = symbol
    out["SctySrs"] = series
    # Strict format: DATE1 must parse as %d-%b-%Y (mirrors the indices
    # normalizer's strict-date precedent) -- a wrong format fails loud rather
    # than silently misparsing. Emitted as ISO date strings (TradDt).
    out["TradDt"] = pd.to_datetime(date1, format=_DATE1_FORMAT).dt.strftime("%Y-%m-%d")

    # CLOSE is strict: a "-" here means the row is genuinely unusable data.
    close = df["CLOSE_PRICE"].astype(float)
    out["ClsPric"] = close

    # OPEN/HIGH/LOW: "-" placeholders coerce to NaN, then fill from the
    # (already-strict) close -- a flat bar, mirroring the indices adapter.
    out["OpnPric"] = pd.to_numeric(df["OPEN_PRICE"], errors="coerce").fillna(close)
    out["HghPric"] = pd.to_numeric(df["HIGH_PRICE"], errors="coerce").fillna(close)
    out["LwPric"] = pd.to_numeric(df["LOW_PRICE"], errors="coerce").fillna(close)

    # PREV_CLOSE is not in the '-'-fill-from-close list -- plain numeric passthrough.
    out["PrvsClsgPric"] = df["PREV_CLOSE"].astype(float)

    out["TtlTradgVol"] = (
        pd.to_numeric(df["TTL_TRD_QNTY"], errors="coerce").fillna(0).astype("int64")
    )
    out["TtlNbOfTxsExctd"] = (
        pd.to_numeric(df["NO_OF_TRADES"], errors="coerce").fillna(0).astype("int64")
    )

    # TURNOVER_LACS is in lakhs; UDiFF's TtlTrfVal is in rupees.
    # VERIFY-LIVE(T9): lakhs -> rupees assumption, checked against one real
    # symbol's turnover both ways.
    out["TtlTrfVal"] = pd.to_numeric(df["TURNOVER_LACS"], errors="coerce").fillna(0.0) * 100_000

    out["ISIN"] = [
        isin_map.get((s, sr), "") for s, sr in zip(symbol, series, strict=True)
    ]

    # Fixed fields the UDiFF normalizer filters on -- secfull has no session
    # concept (it's always the final settled day file), so these are constant.
    out["SsnId"] = "F1"
    out["FinInstrmTp"] = "STK"

    return out.reset_index(drop=True)
