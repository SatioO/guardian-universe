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

WARNING (carried in from the task-6/task-7 review): the `cli.py` BUILDERS
bindings (`BUILDERS["reference"]`, `BUILDERS["ca_flags"]`) bind `source_spec`
to the REAL registry spec (`datasets.DATASETS[datasets.DATASET_ORDER[0]]`) at
CLI *import time* -- not at call time. Monkeypatching `datasets.DATASETS`
alone in a test does NOT redirect the `source_spec` a bound partial already
captured; the partial keeps pointing at whatever spec was live when
`pipeline.cli` was first imported. Tests that want a real builder run against
tmp dirs via the registered `BUILDERS` entries must monkeypatch
`cli.builders.BUILDERS` directly (e.g. replace the dict entry with a fresh
`functools.partial(build_x, source_spec=<tmp-scoped spec>)`), not
`datasets.DATASETS`.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config, store
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


_CA_FLAGS_JOIN_COLUMNS = ["date", "instrument_key", "close", "prevclose"]
_CA_FLAGS_OUTPUT_COLUMNS = [
    "date", "instrument_key", "close_prev", "prevclose_today", "implied_ratio",
]


def build_ca_flags(
    spec: DatasetSpec, target: date, *, source_spec: DatasetSpec
) -> RunStatus:
    """Corporate-action ex-date detector: flag instruments whose today's
    prevclose implies a discontinuity vs the previous trading day's close --
    a split, bonus, or other ex-date event, not ordinary price movement.

    "Previous trading day" = the max date present in the source store that is
    strictly less than `target` (store dates ARE trading days by
    construction -- no holiday calendar dependency, same v1 posture as the
    reference builder). Only instrument_keys present on BOTH days are joined;
    a key with no prior day (new listing, or the very first backfill day
    overall) is simply never flagged. Zero flags, or no previous day at all,
    are both a clean `success` with `symbol_count=0` -- not a failure.

    Appends (never overwrites) via `store.append_keyed`, deduped on
    (date, instrument_key) -- idempotent re-runs for the same target date
    replace that date's flags rather than duplicating them.

    Known limitation (dual-key join, until reference-remap linking lands in
    P4a): an instrument that switches its `instrument_key` between days (e.g.
    the `NSE:{symbol}` sentinel resolving to its real ISIN once one appears)
    is absent from the same-key join on the day of the switch, so a
    corporate action coinciding with that switch is silently missed that day.
    """
    df = _read_all_years_for_ca_flags(source_spec)

    if df.empty:
        return RunStatus("success", target, symbol_count=0, source="derived")

    target_ts = pd.Timestamp(target)
    prior_dates = df.loc[df["date"] < target_ts, "date"]
    if prior_dates.empty:
        return RunStatus("success", target, symbol_count=0, source="derived")
    prev_day = prior_dates.max()

    today = df[df["date"] == target_ts][["instrument_key", "prevclose"]]
    prev = df[df["date"] == prev_day][["instrument_key", "close"]]
    if today.empty or prev.empty:
        return RunStatus("success", target, symbol_count=0, source="derived")

    joined = today.merge(prev, on="instrument_key", how="inner", suffixes=("_today", "_prev"))
    if joined.empty:
        return RunStatus("success", target, symbol_count=0, source="derived")

    joined["implied_ratio"] = joined["close"] / joined["prevclose"]
    deviation = (joined["prevclose"] / joined["close"] - 1).abs()
    flagged = joined[deviation > config.CA_DISCONTINUITY_THRESHOLD]

    if flagged.empty:
        return RunStatus("success", target, symbol_count=0, source="derived")

    out = pd.DataFrame({
        "date": target_ts,
        "instrument_key": flagged["instrument_key"].to_numpy(),
        "close_prev": flagged["close"].to_numpy(),
        "prevclose_today": flagged["prevclose"].to_numpy(),
        "implied_ratio": flagged["implied_ratio"].to_numpy(),
    })[_CA_FLAGS_OUTPUT_COLUMNS]

    store.append_keyed(out, spec.base_dir, prefix=spec.file_prefix)

    return RunStatus("success", target, symbol_count=len(out), source="derived")


def _read_all_years_for_ca_flags(source_spec: DatasetSpec) -> pd.DataFrame:
    """Column-pruned read of the source store restricted to the columns
    build_ca_flags needs. Missing/empty store -> empty frame (a legitimate
    state, e.g. the very first backfill day)."""
    base = source_spec.base_dir
    if not base.exists():
        return pd.DataFrame(columns=_CA_FLAGS_JOIN_COLUMNS)
    frames = [
        pd.read_parquet(p, columns=_CA_FLAGS_JOIN_COLUMNS)
        for p in sorted(base.glob(f"{source_spec.file_prefix}_*.parquet"))
    ]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=_CA_FLAGS_JOIN_COLUMNS)
    return pd.concat(frames, ignore_index=True)
