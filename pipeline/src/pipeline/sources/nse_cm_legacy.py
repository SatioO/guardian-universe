"""NSE legacy CM bhavcopy adapter (`cmDDMMMYYYYbhav.csv`) — the pre-UDiFF
capital-market EOD file, archived back to well before 2021.

Why this exists: the `sec_bhavdata_full` fallback carries NO per-row ISIN, so
its shape-adapter had to resolve ISIN by SYMBOL — which collapses every series
of an issuer (EQ + its N-series bonds/NCDs) onto the single equity ISIN, so a
bond row displaces the equity row for that date. The legacy CM bhavcopy carries
the correct per-row ISIN (bond rows have their own INE…07/08… ISIN), exactly
like UDiFF, so it is the correct pre-2024 source for historical backfill.

`legacy_to_udiff_shape` is a shape-adapter (same contract as
`secfull_to_udiff_shape`): it reshapes the legacy raw frame into the UDiFF
primary's raw column shape (TradDt/ISIN/TckrSymb/… plus SsnId="F1"/
FinInstrmTp="STK") so the existing `normalize_equity_bhavcopy` consumes it
unchanged. Provenance ("nse-cm-legacy") is stamped later via FetchResult.source.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from pipeline.errors import UnexpectedFailure

_BASE = "https://nsearchives.nseindia.com/content/historical/EQUITIES"

# Raw columns we depend on (the file has a couple more, incl. a trailing
# unnamed column; headers carry stray whitespace and are stripped before use).
LEGACY_RAW_COLUMNS: list[str] = [
    "SYMBOL",
    "SERIES",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "PREVCLOSE",
    "TOTTRDQTY",
    "TOTTRDVAL",
    "TIMESTAMP",
    "TOTALTRADES",
    "ISIN",
]

_REQUIRED_RAW = set(LEGACY_RAW_COLUMNS)

_TIMESTAMP_FORMAT = "%d-%b-%Y"  # e.g. "05-JUL-2022" (month upper-cased in file)


def build_legacy_url(d: date) -> str:
    mon = d.strftime("%b").upper()
    stamp = d.strftime("%d%b%Y").upper()
    return f"{_BASE}/{d.year}/{mon}/cm{stamp}bhav.csv.zip"


def legacy_to_udiff_shape(raw: pd.DataFrame) -> pd.DataFrame:
    """Reshape a raw legacy CM bhavcopy frame into the UDiFF primary's raw
    column shape so `normalize_equity_bhavcopy` can consume it unchanged.

    Unlike secfull, the ISIN is taken from the row's OWN `ISIN` column — the
    whole point of preferring this source — so bond rows key off their own
    ISIN and never displace the issuer's equity row.

    Raises:
        UnexpectedFailure: a required raw column is missing.
    """
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]

    missing = _REQUIRED_RAW - set(df.columns)
    if missing:
        raise UnexpectedFailure(
            f"legacy CM bhavcopy missing required columns: {sorted(missing)}"
        )

    symbol = df["SYMBOL"].astype(str).str.strip()
    series = df["SERIES"].astype(str).str.strip()
    isin = df["ISIN"].astype(str).str.strip()
    timestamp = df["TIMESTAMP"].astype(str).str.strip()

    out = pd.DataFrame(index=df.index)
    out["TckrSymb"] = symbol
    out["SctySrs"] = series
    out["ISIN"] = isin
    # Strict date parse (mirrors secfull's DATE1 handling) -- a wrong format
    # fails loud rather than silently misparsing. Emitted as ISO TradDt.
    out["TradDt"] = pd.to_datetime(timestamp, format=_TIMESTAMP_FORMAT).dt.strftime(
        "%Y-%m-%d"
    )

    # CLOSE is strict; OPEN/HIGH/LOW coerce then fill from close (flat bar).
    close = df["CLOSE"].astype(float)
    out["ClsPric"] = close
    out["OpnPric"] = pd.to_numeric(df["OPEN"], errors="coerce").fillna(close)
    out["HghPric"] = pd.to_numeric(df["HIGH"], errors="coerce").fillna(close)
    out["LwPric"] = pd.to_numeric(df["LOW"], errors="coerce").fillna(close)
    out["PrvsClsgPric"] = df["PREVCLOSE"].astype(float)

    out["TtlTradgVol"] = (
        pd.to_numeric(df["TOTTRDQTY"], errors="coerce").fillna(0).astype("int64")
    )
    out["TtlNbOfTxsExctd"] = (
        pd.to_numeric(df["TOTALTRADES"], errors="coerce").fillna(0).astype("int64")
    )
    # TOTTRDVAL is already in rupees (unlike secfull's TURNOVER_LACS) -- passthrough.
    out["TtlTrfVal"] = pd.to_numeric(df["TOTTRDVAL"], errors="coerce").fillna(0.0)

    # Fixed fields the UDiFF normalizer filters on -- the legacy file is always
    # the final settled day file, so these are constant.
    out["SsnId"] = "F1"
    out["FinInstrmTp"] = "STK"

    return out.reset_index(drop=True)
