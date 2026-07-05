"""Year-partitioned Parquet store with append+dedupe and trailing-window reads."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config


def _read_year(base: Path, year: int, prefix: str = "ohlc") -> pd.DataFrame:
    p = config.dataset_path(year, base, prefix=prefix)
    if p.exists():
        return pd.read_parquet(p)
    return pd.DataFrame(columns=config.CANON_COLUMNS)


def append_day(df: pd.DataFrame, base: Path, *, prefix: str = "ohlc") -> None:
    base.mkdir(parents=True, exist_ok=True)
    for year, chunk in df.groupby(df["date"].dt.year):
        existing = _read_year(base, int(year), prefix)
        # Warning-free concat: skip concat entirely when existing is empty to avoid
        # pandas 2.x FutureWarning about concatenating empty/all-NA frames.
        combined = chunk if existing.empty else pd.concat([existing, chunk], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["date", "instrument_key"], keep="last"
        )
        combined = combined.sort_values(["date", "instrument_key"]).reset_index(drop=True)
        # Crash-atomic: write to a temp sibling, then atomically replace.
        target = config.dataset_path(int(year), base, prefix=prefix)
        tmp = target.with_suffix(".parquet.tmp")
        combined.to_parquet(tmp, compression="zstd", index=False)
        tmp.replace(target)


def has_day(base: Path, d: date, *, prefix: str = "ohlc") -> bool:
    df = _read_year(base, d.year, prefix)
    if df.empty:
        return False
    return bool((df["date"] == pd.Timestamp(d)).any())


def day_symbol_count(base: Path, d: date, *, prefix: str = "ohlc") -> int:
    df = _read_year(base, d.year, prefix)
    if df.empty:
        return 0
    return int((df["date"] == pd.Timestamp(d)).sum())


def day_series_counts(base: Path, d: date, *, prefix: str = "ohlc") -> dict[str, int]:
    """Per-series row counts for one day (G1b task 4 per-series gate input)."""
    df = _read_year(base, d.year, prefix)
    if df.empty:
        return {}
    day_df = df[df["date"] == pd.Timestamp(d)]
    if day_df.empty:
        return {}
    return {str(k): int(v) for k, v in day_df.groupby("series").size().items()}


def read_trailing_window(
    base: Path, end: date, n_rows_per_key: int, *, prefix: str = "ohlc"
) -> pd.DataFrame:
    # Warning-free concat: drop empty year frames before concatenating to avoid
    # pandas 2.x FutureWarning about concatenating empty/all-NA frames.
    frames = [
        f
        for f in (_read_year(base, y, prefix) for y in (end.year - 1, end.year))
        if not f.empty
    ]
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


def write_delta(
    df: pd.DataFrame, base: Path, d: date, *, prefix: str = "ohlc", keep: int = 35
) -> Path:
    """Persist one day's clean frame as a delta artifact (client catch-up unit).

    Prunes to the newest `keep` per prefix; release-side copies self-GC once
    they drop out of the manifest's delta window."""
    delta_dir = base / "deltas"
    delta_dir.mkdir(parents=True, exist_ok=True)
    target = delta_dir / f"{prefix}_{d.isoformat()}.parquet"
    tmp = target.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, compression="zstd", index=False)
    tmp.replace(target)
    existing = sorted(delta_dir.glob(f"{prefix}_*.parquet"))
    for old in existing[:-keep]:
        old.unlink()
    return target


def list_deltas(base: Path, *, prefix: str = "ohlc") -> list[Path]:
    delta_dir = base / "deltas"
    if not delta_dir.exists():
        return []
    return sorted(delta_dir.glob(f"{prefix}_*.parquet"))
