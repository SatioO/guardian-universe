"""Year-partitioned Parquet store with append+dedupe and trailing-window reads."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config


def _read_year(base: Path, year: int) -> pd.DataFrame:
    p = config.ohlc_path(year, base)
    if p.exists():
        return pd.read_parquet(p)
    return pd.DataFrame(columns=config.CANON_COLUMNS)


def append_day(df: pd.DataFrame, base: Path) -> None:
    base.mkdir(parents=True, exist_ok=True)
    for year, chunk in df.groupby(df["date"].dt.year):
        existing = _read_year(base, int(year))
        # Warning-free concat: skip concat entirely when existing is empty to avoid
        # pandas 2.x FutureWarning about concatenating empty/all-NA frames.
        combined = chunk if existing.empty else pd.concat([existing, chunk], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["date", "instrument_key"], keep="last"
        )
        combined = combined.sort_values(["date", "instrument_key"]).reset_index(drop=True)
        # Crash-atomic: write to a temp sibling, then atomically replace.
        target = config.ohlc_path(int(year), base)
        tmp = target.with_suffix(".parquet.tmp")
        combined.to_parquet(tmp, compression="zstd", index=False)
        tmp.replace(target)


def has_day(base: Path, d: date) -> bool:
    df = _read_year(base, d.year)
    if df.empty:
        return False
    return bool((df["date"] == pd.Timestamp(d)).any())


def day_symbol_count(base: Path, d: date) -> int:
    df = _read_year(base, d.year)
    if df.empty:
        return 0
    return int((df["date"] == pd.Timestamp(d)).sum())


def read_trailing_window(base: Path, end: date, n_rows_per_key: int) -> pd.DataFrame:
    # Warning-free concat: drop empty year frames before concatenating to avoid
    # pandas 2.x FutureWarning about concatenating empty/all-NA frames.
    frames = [f for f in (_read_year(base, y) for y in (end.year - 1, end.year)) if not f.empty]
    if not frames:
        return pd.DataFrame(columns=config.CANON_COLUMNS)
    df = pd.concat(frames, ignore_index=True)
    df = df[df["date"] <= pd.Timestamp(end)]
    if df.empty:
        return df
    df = df.sort_values(["instrument_key", "date"])
    return (
        df.groupby("instrument_key", group_keys=False)
        .tail(n_rows_per_key)
        .reset_index(drop=True)
    )
