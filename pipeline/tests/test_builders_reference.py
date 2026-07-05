"""Tests for the reference/instruments derived-dataset builder (G1b task 6)."""
from __future__ import annotations

import dataclasses
import functools
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipeline import config, datasets, store
from pipeline.daily_update import RunStatus


def _row(d: str, key: str, symbol: str, series: str = "EQ", isin: str | None = None) -> dict:
    return {
        "date": pd.Timestamp(d), "instrument_key": key, "isin": isin or key,
        "symbol": symbol, "series": series,
        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
        "prevclose": 1.0, "volume": 1, "value": 1.0, "trades": 1,
        "source": "nse-udiff",
    }


def _seed_equities_store(base: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)[config.CANON_COLUMNS]
    store.append_day(df, base, prefix="ohlc")


def _source_spec(base_dir: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.EQUITIES, base_dir=base_dir)


def _target_spec(base_dir: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.REFERENCE, base_dir=base_dir)


def test_reference_spec_fields():
    s = datasets.REFERENCE
    assert s.key == "reference"
    assert s.file_prefix == "instruments"
    assert s.base_dir == config.REFERENCE_DIR
    assert s.source_label == "derived"
    assert s.abs_rowcount_range == (0, 10**9)
    assert s.manifest_name == "reference"
    assert s.schema_version == 1
    assert s.derived is True
    assert datasets.DATASETS["reference"] is s
    assert datasets.DATASET_ORDER == ["equities", "indices", "reference"]
    with pytest.raises(RuntimeError, match="derived dataset has no fetcher"):
        s.make_fetcher()
    assert s.normalizer(pd.DataFrame({"a": [1]})).equals(pd.DataFrame({"a": [1]}))


def test_reference_registered_in_builders():
    """builders.py stays name-free: the CLI (the allowed edge) is what binds
    source_spec=DATASETS[DATASET_ORDER[0]] and populates BUILDERS["reference"]
    at import time."""
    from pipeline import cli
    assert "reference" in cli.builders.BUILDERS
    bound = cli.builders.BUILDERS["reference"]
    assert isinstance(bound, functools.partial)
    assert bound.func is cli.builders.build_reference
    assert bound.keywords == {"source_spec": datasets.DATASETS[datasets.DATASET_ORDER[0]]}


def test_build_reference_basic_row_shape_and_date_column(tmp_path: Path):
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    _seed_equities_store(src_base, [
        _row("2026-07-01", "INE001", "AAA"),
        _row("2026-07-02", "INE001", "AAA"),
    ])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ref_base)

    result = builders.build_reference(target_spec, date(2026, 7, 2), source_spec=source_spec)

    assert isinstance(result, RunStatus)
    assert result.status == "success"
    assert result.source == "derived"
    assert result.symbol_count == 1  # one distinct (instrument_key, symbol, series)

    out_path = ref_base / "instruments_all.parquet"
    assert out_path.exists()
    out = pd.read_parquet(out_path)
    assert set(out.columns) >= {
        "instrument_key", "isin", "symbol", "name", "series",
        "first_seen", "last_seen", "status", "valid_from", "valid_to", "date",
    }
    row = out.iloc[0]
    assert row["instrument_key"] == "INE001"
    assert row["symbol"] == "AAA"
    assert row["name"] == "AAA"
    assert row["series"] == "EQ"
    assert row["first_seen"] == pd.Timestamp("2026-07-01")
    assert row["last_seen"] == pd.Timestamp("2026-07-02")
    assert row["valid_from"] == pd.Timestamp("2026-07-01")
    assert row["valid_to"] == pd.Timestamp("2026-07-02")
    assert row["date"] == pd.Timestamp("2026-07-02")  # REQUIRED for manifest latest_date
    assert row["status"] == "active"


def test_build_reference_rename_window_yields_two_rows(tmp_path: Path):
    """Same instrument_key, symbol renamed on day 3 -> two SCD2 rows with
    correct valid_from/valid_to windows."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    _seed_equities_store(src_base, [
        _row("2026-07-01", "INE001", "OLDNAME"),
        _row("2026-07-02", "INE001", "OLDNAME"),
        _row("2026-07-03", "INE001", "NEWNAME"),  # renamed
    ])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ref_base)

    result = builders.build_reference(target_spec, date(2026, 7, 3), source_spec=source_spec)
    assert result.symbol_count == 2

    out = pd.read_parquet(ref_base / "instruments_all.parquet")
    out = out.sort_values("first_seen").reset_index(drop=True)
    assert list(out["symbol"]) == ["OLDNAME", "NEWNAME"]

    old_row = out.iloc[0]
    assert old_row["first_seen"] == pd.Timestamp("2026-07-01")
    assert old_row["last_seen"] == pd.Timestamp("2026-07-02")
    assert old_row["valid_from"] == pd.Timestamp("2026-07-01")
    assert old_row["valid_to"] == pd.Timestamp("2026-07-02")

    new_row = out.iloc[1]
    assert new_row["first_seen"] == pd.Timestamp("2026-07-03")
    assert new_row["last_seen"] == pd.Timestamp("2026-07-03")
    assert new_row["valid_from"] == pd.Timestamp("2026-07-03")
    assert new_row["valid_to"] == pd.Timestamp("2026-07-03")


def test_build_reference_active_vs_inactive_status(tmp_path: Path):
    """status: active iff last_seen is among the 10 most recent DISTINCT
    trading dates present in the store; else inactive."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"

    rows = []
    # 12 distinct trading dates: 2026-06-01 .. 2026-06-12 (weekdays not
    # required -- store dates ARE the trading days per the v1 definition).
    all_dates = [f"2026-06-{d:02d}" for d in range(1, 13)]
    # Key that appears on every date -> last_seen = last date -> active.
    for d in all_dates:
        rows.append(_row(d, "INE_ACTIVE", "ACTIVE"))
    # Key that disappears after the 1st date only (absent for the remaining
    # 11 distinct dates, i.e. well outside the most-recent-10 window) -> inactive.
    rows.append(_row(all_dates[0], "INE_GONE", "GONE"))
    _seed_equities_store(src_base, rows)

    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ref_base)
    target = date(2026, 6, 12)

    result = builders.build_reference(target_spec, target, source_spec=source_spec)
    assert result.status == "success"

    out = pd.read_parquet(ref_base / "instruments_all.parquet")
    active_row = out[out["instrument_key"] == "INE_ACTIVE"].iloc[0]
    gone_row = out[out["instrument_key"] == "INE_GONE"].iloc[0]
    assert active_row["status"] == "active"
    assert gone_row["status"] == "inactive"


def test_build_reference_includes_nse_sentinel_keys(tmp_path: Path):
    """Sentinel NSE:<symbol> instrument_keys (null/empty ISIN rows, task 4)
    are treated like any other instrument_key -- included in the output."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    _seed_equities_store(src_base, [
        _row("2026-07-01", "NSE:WEIRD", "WEIRD", isin="NSE:WEIRD"),
    ])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ref_base)

    result = builders.build_reference(target_spec, date(2026, 7, 1), source_spec=source_spec)
    assert result.symbol_count == 1

    out = pd.read_parquet(ref_base / "instruments_all.parquet")
    assert "NSE:WEIRD" in set(out["instrument_key"])


def test_build_reference_writes_atomically_tmp_and_replace(tmp_path: Path, monkeypatch):
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    _seed_equities_store(src_base, [_row("2026-07-01", "INE001", "AAA")])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ref_base)

    builders.build_reference(target_spec, date(2026, 7, 1), source_spec=source_spec)
    out_path = ref_base / "instruments_all.parquet"
    good = out_path.read_bytes()

    original = pd.DataFrame.to_parquet

    def boom(self, path, *a, **kw):
        Path(str(path)).write_bytes(b"torn")
        raise OSError("disk full")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)
    with pytest.raises(OSError):
        builders.build_reference(target_spec, date(2026, 7, 1), source_spec=source_spec)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", original)

    assert out_path.read_bytes() == good  # untouched by the crashed write


def test_build_reference_full_rewrite_each_run(tmp_path: Path):
    """A second run with a longer store history fully replaces the previous
    output rather than accumulating stale rows."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    _seed_equities_store(src_base, [_row("2026-07-01", "INE001", "AAA")])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ref_base)
    builders.build_reference(target_spec, date(2026, 7, 1), source_spec=source_spec)

    _seed_equities_store(src_base, [_row("2026-07-02", "INE002", "BBB")])
    result = builders.build_reference(target_spec, date(2026, 7, 2), source_spec=source_spec)
    assert result.symbol_count == 2

    out = pd.read_parquet(ref_base / "instruments_all.parquet")
    assert len(out) == 2
    assert set(out["instrument_key"]) == {"INE001", "INE002"}


def test_build_manifest_picks_up_reference_output(tmp_path: Path):
    from pipeline import builders, manifest

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    _seed_equities_store(src_base, [
        _row("2026-07-01", "INE001", "AAA"),
        _row("2026-07-02", "INE001", "AAA"),
    ])
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ref_base)
    builders.build_reference(target_spec, date(2026, 7, 2), source_spec=source_spec)

    m = manifest.build_manifest(
        [target_spec], latest_trading_date=date(2026, 7, 2), generated_at="g",
    )
    (ds,) = m["datasets"]
    assert ds["name"] == "reference"
    assert ds["latest_date"] == "2026-07-02"
    (b,) = ds["baseline"]
    assert b["name"] == "instruments_all.parquet"


def test_build_reference_returns_failed_never_raises_on_missing_store(tmp_path: Path):
    """Exception boundary lives in the CLI wrapper (_run_builder), not here --
    but a missing/empty source store should still degrade to a clean success
    with zero rows rather than raising, since an empty store is a legitimate
    (e.g. very first backfill day) state."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"  # never seeded -- empty store
    ref_base = tmp_path / "reference"
    source_spec = _source_spec(src_base)
    target_spec = _target_spec(ref_base)

    result = builders.build_reference(target_spec, date(2026, 7, 1), source_spec=source_spec)
    assert result.status == "success"
    assert result.symbol_count == 0
