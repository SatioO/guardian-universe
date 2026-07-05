"""Raw UDiFF bhavcopy -> canonical long OHLC frame. Pure."""
from __future__ import annotations

import pandas as pd

from pipeline import config
from pipeline.errors import UnexpectedFailure

_FINAL_SESSIONS = {"F1", "F2"}

# UDiFF raw column -> canonical column.
_COLMAP = {
    "TradDt": "date",
    "ISIN": "isin",
    "TckrSymb": "symbol",
    "SctySrs": "series",
    "OpnPric": "open",
    "HghPric": "high",
    "LwPric": "low",
    "ClsPric": "close",
    "PrvsClsgPric": "prevclose",
    "TtlTradgVol": "volume",
    "TtlTrfVal": "value",
    "TtlNbOfTxsExctd": "trades",
}

# Raw columns this normalizer depends on (filters + mapped columns).
_REQUIRED_RAW = set(_COLMAP) | {"FinInstrmTp", "SctySrs", "SsnId"}


def normalize_equity_bhavcopy(raw: pd.DataFrame, source: str = "nse-udiff") -> pd.DataFrame:
    missing = _REQUIRED_RAW - set(raw.columns)
    if missing:
        raise UnexpectedFailure(f"bhavcopy missing required columns: {sorted(missing)}")
    # G1b task 4 (spec change): the SctySrs == "EQ" filter is DROPPED — all cash
    # series (STK + final-session) are stored, each tagged with its own
    # `series` value. Only the FinInstrmTp/session filters remain.
    df = raw[
        (raw["FinInstrmTp"] == "STK")
        & (raw["SsnId"].isin(_FINAL_SESSIONS))
    ].copy()

    df = df.rename(columns=_COLMAP)
    df["date"] = pd.to_datetime(df["date"])

    # ISIN normalized to a plain string first ("" for null/NaN) so downstream
    # logic and dtype stay simple.
    isin = df["isin"].fillna("").astype(str)
    symbol = df["symbol"].fillna("").astype(str)
    df["isin"] = isin

    # Live-data finding (Calibration fix 2, real 2026-07-03 bhavcopy): 5
    # STK/F-session rows had a NULL SctySrs post-universe-widening -- these
    # survive the filters above with series=NaN and previously failed the
    # non-nullable `series` schema column, killing the entire day. Null/NaN
    # series is filled with "" (consistent with the isin="" sentinel).
    df["series"] = df["series"].fillna("").astype(str)

    # Sentinel keys: null/empty-ISIN rows key off "NSE:" + symbol instead of
    # dying in quarantine for a missing ISIN. Rows where BOTH isin and symbol
    # are empty get an empty instrument_key so quarantine's key_ok check still
    # rejects them (no fake key is invented from nothing).
    df["instrument_key"] = isin.where(isin != "", "NSE:" + symbol)
    df.loc[(isin == "") & (symbol == ""), "instrument_key"] = ""

    df["source"] = source

    df["volume"] = df["volume"].astype("int64")
    df["trades"] = df["trades"].astype("int64")
    for col in ("open", "high", "low", "close", "prevclose", "value"):
        df[col] = df[col].astype(float)

    return df[config.CANON_COLUMNS].reset_index(drop=True)
