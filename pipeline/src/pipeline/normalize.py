"""Raw UDiFF bhavcopy -> canonical long OHLC frame. Pure."""
from __future__ import annotations

import logging

import pandas as pd

from pipeline import config
from pipeline.errors import UnexpectedFailure

_log = logging.getLogger(__name__)

_FINAL_SESSIONS = {"F1", "F2"}

# A plain positive integer, optionally written as a whole float ("2885.0", how
# a NaN-bearing CSV column reads back). Anything else is a malformed token.
_TOKEN_PATTERN = r"[0-9]{1,18}(?:\.0+)?"

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
# FinInstrmId is required of EVERY frame: the UDiFF file carries it, and the
# fallback adapters (secfull, legacy CM, broker rebuild) emit it as null, so a
# UDiFF file that loses the column fails the run instead of silently losing
# every token.
_REQUIRED_RAW = set(_COLMAP) | {"FinInstrmTp", "SctySrs", "SsnId", "FinInstrmId"}


def _parse_nse_token(raw: pd.Series) -> tuple[pd.Series, int]:
    """(nullable Int64 tokens, how many present values were malformed).

    Blank/missing is "no token" (null, not counted). A present value that is
    not a positive integer ("abc", "1.5", 0, -7, "1e3", overflow) is nulled
    and counted: a null token only means lineage unknown, so a bad value must
    never fail the day."""
    text = raw.astype("string").str.strip()
    present = text.notna() & (text != "")
    valid = present & text.str.fullmatch(_TOKEN_PATTERN).fillna(False)
    number = pd.to_numeric(text.where(valid), errors="coerce")
    valid = valid & (number > 0).fillna(False)
    tokens = number.where(valid).astype("Int64")
    return tokens, int((present & ~valid).sum())


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

    df["nse_token"], nulled = _parse_nse_token(df["FinInstrmId"])
    if nulled:
        _log.warning(
            "nse_token: %d malformed FinInstrmId value(s) set to null (lineage unknown)", nulled
        )

    df["source"] = source

    df["volume"] = df["volume"].astype("int64")
    df["trades"] = df["trades"].astype("int64")
    for col in ("open", "high", "low", "close", "prevclose", "value"):
        df[col] = df[col].astype(float)

    out = df[config.CANON_COLUMNS].reset_index(drop=True)
    out.attrs["nse_token_nulled"] = nulled
    return out
