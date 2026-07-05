"""Validation gates: row-count sanity + per-row quarantine. Pure."""
from __future__ import annotations

import pandas as pd

from pipeline import config
from pipeline.errors import UnexpectedFailure


def check_rowcount_by_series(
    total: int,
    series_counts: dict[str, int],
    trailing: dict[str, list[int]],
    *,
    abs_range: tuple[int, int] | None = None,
) -> None:
    """Per-series row-count gate (G1b task 4): REPLACES the total-deviation
    check in run_daily. A universe-widening day would otherwise trip the old
    total-deviation gate (a ~2x jump vs EQ-only history); per-series deviation
    only flags an anomaly in a series that already has trailing history, while
    a brand-new series is exempt until it accumulates its own baseline.

    - total outside abs_range (None-sentinel to config.ROWCOUNT_ABS_RANGE) -> fail
    - each series with non-empty trailing data: deviation of today's count vs
      its trailing mean > config.ROWCOUNT_DEVIATION -> fail
    - a series with trailing mean >= 50 absent from today's series_counts ->
      fail (a major series vanishing is a truncation signal)
    - a series new today (no trailing data) -> pass (accumulates history)
    """
    lo, hi = abs_range if abs_range is not None else config.ROWCOUNT_ABS_RANGE
    if not (lo <= total <= hi):
        raise UnexpectedFailure(f"row count {total} outside absolute range {lo}..{hi}")

    for series, series_trailing in trailing.items():
        if not series_trailing:
            continue  # no history yet for this series -- nothing to compare
        mean = sum(series_trailing) / len(series_trailing)
        today_count = series_counts.get(series)
        if today_count is None:
            if mean >= 50:
                raise UnexpectedFailure(
                    f"series {series!r} absent today but trailing mean is "
                    f"{mean:.0f} (>=50) -- possible truncation"
                )
            continue
        if mean <= 0:
            raise UnexpectedFailure(
                f"series {series!r} trailing row-count mean is non-positive "
                f"({mean:.0f}); cannot validate deviation"
            )
        if abs(today_count - mean) / mean > config.ROWCOUNT_DEVIATION:
            raise UnexpectedFailure(
                f"series {series!r} row count {today_count} deviates "
                f">{config.ROWCOUNT_DEVIATION:.0%} from trailing mean {mean:.0f}"
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
