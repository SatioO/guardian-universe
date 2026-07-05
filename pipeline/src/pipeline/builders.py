"""Registry of derived-dataset builders, keyed by DatasetSpec.key.

Populated by later G1b tasks (6: reference/instruments, 7: ca_flags). This
module intentionally imports daily_update.RunStatus and datasets.DatasetSpec
only for typing -- builders themselves must stay name-free (no hardcoded
dataset-key lookups); the CLI is the allowed edge that resolves specs by name
and passes them in.

Builder functions that need a *source* spec (e.g. build_reference reads the
equities store) take it as a keyword-only `source_spec` argument rather than
looking it up by name -- the CLI resolves `DATASETS[DATASET_ORDER[0]]` and
binds it via `functools.partial` when it populates BUILDERS, so this module
never hardcodes "equities" (or any other dataset key) anywhere. The bound
partial still satisfies BUILDERS' `Callable[[DatasetSpec, date], RunStatus]`
signature -- `source_spec` is filled in, leaving exactly the two positional
parameters (`spec`, `target`) the registry and `_run_builder` expect.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline.daily_update import RunStatus
from pipeline.datasets import DatasetSpec

BUILDERS: dict[str, Callable[[DatasetSpec, date], RunStatus]] = {}

# 10 most-recent DISTINCT trading dates present in the source store. v1 keeps
# this holiday-free and calendar-free (builders have no `holidays` input,
# unlike run_daily): "trading day" here means "a date that actually appears
# in the store", which by construction only ever contains trading days.
_ACTIVE_WINDOW = 10

_REFERENCE_COLUMNS = ["date", "instrument_key", "isin", "symbol", "series"]


def _read_all_years(source_spec: DatasetSpec) -> pd.DataFrame:
    """Column-pruned read of every `{prefix}_{year}.parquet` file under the
    source spec's base_dir. Missing/empty store -> empty frame (a legitimate
    state, e.g. the very first backfill day)."""
    base = source_spec.base_dir
    if not base.exists():
        return pd.DataFrame(columns=_REFERENCE_COLUMNS)
    frames = [
        pd.read_parquet(p, columns=_REFERENCE_COLUMNS)
        for p in sorted(base.glob(f"{source_spec.file_prefix}_*.parquet"))
    ]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=_REFERENCE_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def build_reference(
    spec: DatasetSpec, target: date, *, source_spec: DatasetSpec
) -> RunStatus:
    """Build the `reference/instruments` SCD2 symbol master from the source
    (equities) store's own presence -- one row per distinct
    `(instrument_key, symbol, series)` version.

    v1 status subset: only `active`/`inactive` are ever emitted. `active`
    means `last_seen` falls among the store's own 10 most recent distinct
    dates (no holiday calendar dependency -- the store's date values ARE
    trading days by construction). `suspended`/`delisted` need an external
    exchange feed and are deferred to a later phase.

    Full rewrite each run: `instruments_all.parquet` is atomically replaced
    (tmp+rename) under `spec.base_dir`, never appended to -- idempotent at
    the scale of a few thousand rows.
    """
    df = _read_all_years(source_spec)

    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"

    if df.empty:
        out = _empty_reference_frame()
        _write_atomic(out, out_path)
        return RunStatus("success", target, symbol_count=0, source="derived")

    recent_dates = sorted(df["date"].drop_duplicates(), reverse=True)[:_ACTIVE_WINDOW]
    active_dates = set(recent_dates)

    grouped = (
        df.groupby(["instrument_key", "symbol", "series"], as_index=False)
        .agg(first_seen=("date", "min"), last_seen=("date", "max"),
             isin=("isin", "last"))
    )
    grouped["name"] = grouped["symbol"]
    grouped["status"] = grouped["last_seen"].apply(
        lambda d: "active" if d in active_dates else "inactive"
    )
    grouped["valid_from"] = grouped["first_seen"]
    grouped["valid_to"] = grouped["last_seen"]
    grouped["date"] = grouped["last_seen"]

    out = grouped[[
        "instrument_key", "isin", "symbol", "name", "series",
        "first_seen", "last_seen", "status", "valid_from", "valid_to", "date",
    ]].sort_values(["instrument_key", "first_seen"]).reset_index(drop=True)

    _write_atomic(out, out_path)

    return RunStatus("success", target, symbol_count=len(out), source="derived")


def _empty_reference_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "instrument_key", "isin", "symbol", "name", "series",
        "first_seen", "last_seen", "status", "valid_from", "valid_to", "date",
    ])


def _write_atomic(df: pd.DataFrame, target: Path) -> None:
    tmp = target.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, compression="zstd", index=False)
    tmp.replace(target)
