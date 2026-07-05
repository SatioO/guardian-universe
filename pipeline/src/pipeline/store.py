"""Year-partitioned Parquet store with append+dedupe and trailing-window reads."""
from __future__ import annotations

import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config


def sweep_orphan_tmp(base: Path, *, older_than_hours: float = 24) -> int:
    """Best-effort cleanup of orphaned crash-write `.tmp` siblings (G2 task 8
    hygiene). `append_keyed`'s own atomic-write pattern always writes to
    `target.with_suffix(".parquet.tmp")` before `tmp.replace(target)` -- a
    crash between those two steps (process killed, disk full, ...) leaves a
    torn `.tmp` file behind forever, since nothing else in the normal write
    path ever revisits it. Left alone, these accumulate as disk-space
    litter across every run.

    Recursively globs `*.parquet.tmp` under `base` (this reaches `deltas/`
    too, since it's a subdirectory of `base` -- see `write_delta`). Any match
    older than `older_than_hours` (by mtime) is unlinked; anything fresher is
    left alone, since it may be the CURRENT in-flight write of a still-running
    process and must never be swept out from under it.

    Best-effort by design: an unlink failure for one file (permissions, a
    filesystem race, a locked file, ...) is warned to stderr and skipped, not
    raised -- this is called at the top of every `append_keyed`/`append_day`
    write, and a sweep hiccup must never block an otherwise-healthy ingest.
    Returns the count of files actually removed (never the count attempted).
    A `base` that doesn't exist yet has nothing to sweep and returns 0."""
    if not base.exists():
        return 0
    threshold_seconds = older_than_hours * 3600
    now = time.time()
    removed = 0
    for path in base.rglob("*.parquet.tmp"):
        try:
            age_seconds = now - path.stat().st_mtime
        except OSError as e:
            print(f"sweep_orphan_tmp: could not stat {path}: {e}", file=sys.stderr)
            continue
        if age_seconds < threshold_seconds:
            continue  # fresh enough to be a current in-flight write -- spare it
        try:
            path.unlink()
            removed += 1
        except OSError as e:
            print(f"sweep_orphan_tmp: could not remove stale tmp {path}: {e}", file=sys.stderr)
    return removed


def _read_year(
    base: Path, year: int, prefix: str = "ohlc", *, columns: list[str] | None = None
) -> pd.DataFrame:
    """Read one year-partitioned file, or an empty frame if it doesn't exist yet.

    The empty-case columns come from the caller (`columns`), not a hardcoded
    schema -- `append_keyed` is column-agnostic (G1b task 7 generalization),
    so it passes the incoming frame's own columns; `append_day` still passes
    `config.CANON_COLUMNS` explicitly to keep its existing contract."""
    p = config.dataset_path(year, base, prefix=prefix)
    if p.exists():
        return pd.read_parquet(p)
    return pd.DataFrame(columns=columns if columns is not None else config.CANON_COLUMNS)


def append_keyed(
    df: pd.DataFrame,
    base: Path,
    *,
    prefix: str,
    key_cols: tuple[str, ...] = ("date", "instrument_key"),
) -> None:
    """Column-agnostic year-partitioned append+dedupe+atomic-write.

    Needs only a `date` column (for year partitioning) plus `key_cols` (for
    dedup identity) -- no dependency on `config.CANON_COLUMNS`, so callers
    with entirely different schemas (e.g. ca_flags' close_prev/implied_ratio
    columns) can use the same store mechanics as the equities/indices OHLC
    data. `append_day` is a thin wrapper calling this with the historical
    (date, instrument_key) default -- this sweep therefore covers both entry
    points; it is not duplicated in `append_day` itself.

    G2 task 8 hygiene: sweeps orphaned crash-write `.tmp` siblings under
    `base` (see `sweep_orphan_tmp`) before doing anything else -- best-effort,
    never raises, so a sweep hiccup never blocks this write."""
    sweep_orphan_tmp(base)
    base.mkdir(parents=True, exist_ok=True)
    key_cols_list = list(key_cols)
    for year, chunk in df.groupby(df["date"].dt.year):
        existing = _read_year(base, int(year), prefix, columns=list(chunk.columns))
        # Warning-free concat: skip concat entirely when existing is empty to avoid
        # pandas 2.x FutureWarning about concatenating empty/all-NA frames.
        combined = chunk if existing.empty else pd.concat([existing, chunk], ignore_index=True)
        combined = combined.drop_duplicates(subset=key_cols_list, keep="last")
        combined = combined.sort_values(key_cols_list).reset_index(drop=True)
        # Crash-atomic: write to a temp sibling, then atomically replace.
        target = config.dataset_path(int(year), base, prefix=prefix)
        tmp = target.with_suffix(".parquet.tmp")
        combined.to_parquet(tmp, compression="zstd", index=False)
        tmp.replace(target)


def append_day(df: pd.DataFrame, base: Path, *, prefix: str = "ohlc") -> None:
    append_keyed(df, base, prefix=prefix, key_cols=("date", "instrument_key"))


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
