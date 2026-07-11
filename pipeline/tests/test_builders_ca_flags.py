"""Tests for the ca_flags derived-dataset builder (G1b task 7): the
prevclose-discontinuity ex-date detector."""
from __future__ import annotations

import dataclasses
import functools
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipeline import config, datasets, store
from pipeline.daily_update import RunStatus


def _row(d: str, key: str, close: float, prevclose: float, symbol: str = "AAA") -> dict:
    return {
        "date": pd.Timestamp(d), "instrument_key": key, "isin": key,
        "symbol": symbol, "series": "EQ",
        "open": close, "high": close, "low": close, "close": close,
        "prevclose": prevclose, "volume": 1, "value": 1.0, "trades": 1,
        "source": "nse-udiff",
    }


def _seed_equities_store(base: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)[config.CANON_COLUMNS]
    store.append_day(df, base, prefix="ohlc")


def _source_spec(base_dir: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.EQUITIES, base_dir=base_dir)


def _target_spec(base_dir: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.CA_FLAGS, base_dir=base_dir)


def test_ca_flags_spec_fields():
    s = datasets.CA_FLAGS
    assert s.key == "ca_flags"
    assert s.file_prefix == "ca_flags"
    assert s.base_dir == config.CA_FLAGS_DIR
    assert s.source_label == "derived"
    assert s.abs_rowcount_range == (0, 10**9)
    assert s.manifest_name == "ca_flags"
    assert s.schema_version == 1
    assert s.derived is True
    assert datasets.DATASETS["ca_flags"] is s
    assert datasets.DATASET_ORDER == [
        "equities", "indices", "reference", "ca_flags", "sector_industry",
        "fundamentals",
    ]
    with pytest.raises(RuntimeError, match="derived dataset has no fetcher"):
        s.make_fetcher()
    assert s.normalizer(pd.DataFrame({"a": [1]})).equals(pd.DataFrame({"a": [1]}))


def test_ca_flags_registered_in_builders():
    """builders.py stays name-free: the CLI (the allowed edge) binds
    source_spec=DATASETS[DATASET_ORDER[0]] and populates BUILDERS["ca_flags"]
    at import time -- same pattern as BUILDERS["reference"]."""
    from pipeline import cli
    assert "ca_flags" in cli.builders.BUILDERS
    bound = cli.builders.BUILDERS["ca_flags"]
    assert isinstance(bound, functools.partial)
    assert bound.func is cli.builders.build_ca_flags
    assert bound.keywords == {"source_spec": datasets.DATASETS[datasets.DATASET_ORDER[0]]}


def test_build_ca_flags_2to1_split_is_flagged(tmp_path: Path):
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ca_base = tmp_path / "ca_flags"
    _seed_equities_store(src_base, [
        _row("2026-07-02", "INE001", close=1000.0, prevclose=990.0),
        # Split day: prevclose_today (500) implies a 2:1 split vs prior close (1000).
        _row("2026-07-03", "INE001", close=505.0, prevclose=500.0),
    ])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ca_base)

    result = builders.build_ca_flags(target_spec, date(2026, 7, 3), source_spec=source_spec)

    assert isinstance(result, RunStatus)
    assert result.status == "success"
    assert result.source == "derived"
    assert result.symbol_count == 1

    out = pd.read_parquet(ca_base / "ca_flags_2026.parquet")
    assert len(out) == 1
    row = out.iloc[0]
    assert row["date"] == pd.Timestamp("2026-07-03")
    assert row["instrument_key"] == "INE001"
    assert row["close_prev"] == 1000.0
    assert row["prevclose_today"] == 500.0
    assert row["implied_ratio"] == pytest.approx(2.0)


def test_build_ca_flags_normal_day_not_flagged(tmp_path: Path):
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ca_base = tmp_path / "ca_flags"
    _seed_equities_store(src_base, [
        _row("2026-07-02", "INE001", close=1000.0, prevclose=990.0),
        # Normal day: today's prevclose equals yesterday's close.
        _row("2026-07-03", "INE001", close=1010.0, prevclose=1000.0),
    ])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ca_base)

    result = builders.build_ca_flags(target_spec, date(2026, 7, 3), source_spec=source_spec)
    assert result.status == "success"
    assert result.symbol_count == 0
    # Zero flags -> nothing appended, so no year file is created at all.
    assert not (ca_base / "ca_flags_2026.parquet").exists()


def test_build_ca_flags_small_drift_not_flagged(tmp_path: Path):
    """A 0.4% drift is below the 0.5% discontinuity threshold -- not flagged."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ca_base = tmp_path / "ca_flags"
    _seed_equities_store(src_base, [
        _row("2026-07-02", "INE001", close=1000.0, prevclose=990.0),
        _row("2026-07-03", "INE001", close=1000.0, prevclose=996.0),  # 0.4% drift
    ])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ca_base)

    result = builders.build_ca_flags(target_spec, date(2026, 7, 3), source_spec=source_spec)
    assert result.status == "success"
    assert result.symbol_count == 0


def test_build_ca_flags_idempotent_rerun_no_dup(tmp_path: Path):
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ca_base = tmp_path / "ca_flags"
    _seed_equities_store(src_base, [
        _row("2026-07-02", "INE001", close=1000.0, prevclose=990.0),
        _row("2026-07-03", "INE001", close=505.0, prevclose=500.0),
    ])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ca_base)

    builders.build_ca_flags(target_spec, date(2026, 7, 3), source_spec=source_spec)
    result = builders.build_ca_flags(target_spec, date(2026, 7, 3), source_spec=source_spec)
    assert result.symbol_count == 1

    out = pd.read_parquet(ca_base / "ca_flags_2026.parquet")
    assert len(out) == 1  # re-run does not duplicate the row


def test_build_ca_flags_no_previous_day_in_store_is_success_zero(tmp_path: Path):
    """First backfill day: no previous trading day exists in the store yet --
    still a success with zero flags, not a failure."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ca_base = tmp_path / "ca_flags"
    _seed_equities_store(src_base, [
        _row("2026-07-03", "INE001", close=1000.0, prevclose=990.0),
    ])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ca_base)

    result = builders.build_ca_flags(target_spec, date(2026, 7, 3), source_spec=source_spec)
    assert result.status == "success"
    assert result.symbol_count == 0


def test_build_ca_flags_only_keys_present_both_days(tmp_path: Path):
    """A key present today but absent on the previous trading day (new
    listing) is never flagged -- no prior close to compare against."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ca_base = tmp_path / "ca_flags"
    _seed_equities_store(src_base, [
        _row("2026-07-02", "INE001", close=1000.0, prevclose=990.0),
        _row("2026-07-03", "INE001", close=1010.0, prevclose=1000.0),
        _row("2026-07-03", "INE_NEW", close=50.0, prevclose=50.0),  # new listing, no prior day
    ])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ca_base)

    result = builders.build_ca_flags(target_spec, date(2026, 7, 3), source_spec=source_spec)
    assert result.status == "success"
    assert result.symbol_count == 0


def test_build_manifest_picks_up_ca_flags_output(tmp_path: Path):
    from pipeline import builders, manifest

    src_base = tmp_path / "ohlc"
    ca_base = tmp_path / "ca_flags"
    _seed_equities_store(src_base, [
        _row("2026-07-02", "INE001", close=1000.0, prevclose=990.0),
        _row("2026-07-03", "INE001", close=505.0, prevclose=500.0),
    ])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ca_base)
    builders.build_ca_flags(target_spec, date(2026, 7, 3), source_spec=source_spec)

    m = manifest.build_manifest(
        [target_spec], latest_trading_date=date(2026, 7, 3), generated_at="g",
    )
    (ds,) = m["datasets"]
    assert ds["name"] == "ca_flags"
    assert ds["latest_date"] == "2026-07-03"
    (b,) = ds["baseline"]
    assert b["name"] == "ca_flags_2026.parquet"


def test_build_ca_flags_returns_success_zero_on_missing_store(tmp_path: Path):
    """A completely empty/missing source store degrades to a clean success
    with zero flags rather than raising."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"  # never seeded
    ca_base = tmp_path / "ca_flags"
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ca_base)

    result = builders.build_ca_flags(target_spec, date(2026, 7, 3), source_spec=source_spec)
    assert result.status == "success"
    assert result.symbol_count == 0
