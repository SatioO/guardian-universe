"""Validation gates: row-count sanity + per-row quarantine. Pure."""
from __future__ import annotations

import pandas as pd

from pipeline import config
from pipeline.errors import UnexpectedFailure


def check_rowcount(count: int, trailing: list[int]) -> None:
    lo, hi = config.ROWCOUNT_ABS_RANGE
    if not (lo <= count <= hi):
        raise UnexpectedFailure(f"row count {count} outside absolute range {lo}..{hi}")
    if trailing:
        mean = sum(trailing) / len(trailing)
        if mean > 0 and abs(count - mean) / mean > config.ROWCOUNT_DEVIATION:
            raise UnexpectedFailure(
                f"row count {count} deviates >{config.ROWCOUNT_DEVIATION:.0%} "
                f"from trailing mean {mean:.0f}"
            )


def quarantine_bad_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    price_cols = ["open", "high", "low", "close", "prevclose"]
    positive = (df[price_cols] > 0).all(axis=1)
    vol_ok = df["volume"] >= 0
    hilo_ok = df["high"] >= df["low"]
    close_ok = (df["close"] >= df["low"]) & (df["close"] <= df["high"])
    key_ok = df["instrument_key"].notna() & (df["instrument_key"].astype(str) != "")

    good_mask = positive & vol_ok & hilo_ok & close_ok & key_ok
    clean = df[good_mask].reset_index(drop=True)
    bad = df[~good_mask].reset_index(drop=True)
    return clean, bad
