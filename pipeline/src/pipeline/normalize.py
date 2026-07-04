"""Raw UDiFF bhavcopy -> canonical long OHLC frame. Pure."""
from __future__ import annotations

import pandas as pd

from pipeline import config

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


def normalize_equity_bhavcopy(raw: pd.DataFrame, source: str = "nse-udiff") -> pd.DataFrame:
    df = raw[
        (raw["FinInstrmTp"] == "STK")
        & (raw["SctySrs"] == "EQ")
        & (raw["SsnId"].isin(_FINAL_SESSIONS))
    ].copy()

    df = df.rename(columns=_COLMAP)
    df["date"] = pd.to_datetime(df["date"])
    df["instrument_key"] = df["isin"]
    df["source"] = source

    df["volume"] = df["volume"].astype("int64")
    df["trades"] = df["trades"].astype("int64")
    for col in ("open", "high", "low", "close", "prevclose", "value"):
        df[col] = df[col].astype(float)

    return df[config.CANON_COLUMNS].reset_index(drop=True)
