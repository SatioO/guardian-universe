"""Tests for `bootstrap-reference`: the one-off rebuild of the reference symbol
master from price files left on a release (the cold-start and rollback path
of the incremental reference build)."""
from __future__ import annotations

import dataclasses
import hashlib
import io
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipeline import bootstrap, builders, config, datasets
from pipeline.errors import UnexpectedFailure
from pipeline.manifest import asset_name
from tests.fakes import FakeReleaseClient
from tests.test_builders_reference import _history, _legacy_full_rebuild, _row

SESSION_COLUMNS = ["date", "instrument_key", "isin", "symbol", "series", "nse_token"]


def _sessions(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows).astype({"nse_token": "Int64"})[SESSION_COLUMNS]


def _year_bytes(rows: list[dict]) -> bytes:
    buf = io.BytesIO()
    pd.DataFrame(rows)[config.CANON_COLUMNS].to_parquet(buf, compression="zstd", index=False)
    return buf.getvalue()


def _by_year(rows: list[dict]) -> dict[int, bytes]:
    years = sorted({r["date"].year for r in rows})
    return {y: _year_bytes([r for r in rows if r["date"].year == y]) for y in years}


def _specs(tmp_path: Path) -> tuple[datasets.DatasetSpec, datasets.DatasetSpec]:
    return (
        dataclasses.replace(datasets.REFERENCE, base_dir=tmp_path / "reference"),
        dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "unused-ohlc"),
    )


def _weekdays_only() -> tuple[set[date], set[date]]:
    return set(), set()


@pytest.fixture(autouse=True)
def _no_holiday_calendar(monkeypatch):
    """Fixture sessions are plain weekdays; the bootstrap's default calendar
    (the repo's market_calendar.json) is swapped for one with no closures."""
    original = builders.rebuild_reference
    monkeypatch.setattr(
        builders, "rebuild_reference",
        lambda *a, **kw: original(*a, trading_calendar=_weekdays_only, **kw),
    )


def _history_across_years() -> list[dict]:
    """The reference-builder fixture history, plus a listing from the year
    before, so the price files span two year partitions."""
    return [_row("2025-12-30", "INE_OLDYEAR", "OLDYEAR"), *_history()]


def _snapshot_release(rows: list[dict]) -> FakeReleaseClient:
    """A data-snapshot-* release: its manifest still lists the ohlc dataset."""
    fake = FakeReleaseClient(exists=True)
    baseline = []
    for year, data in _by_year(rows).items():
        sha = hashlib.sha256(data).hexdigest()
        logical = f"ohlc_{year}.parquet"
        fake.seed(asset_name(logical, sha), data)
        baseline.append({"name": logical, "asset": asset_name(logical, sha),
                         "sha256": sha, "bytes": len(data), "rows": 1})
    manifest = {"manifest_version": 2, "generated_at": "g", "latest_trading_date": "2026-07-10",
                "datasets": [{"name": "ohlc", "baseline": baseline, "deltas": []}]}
    fake.seed("manifest.json", json.dumps(manifest).encode())
    return fake


def test_bootstrap_from_a_snapshot_equals_the_legacy_full_rebuild(tmp_path: Path):
    rows = _history_across_years()
    reference_spec, source_spec = _specs(tmp_path)

    status = bootstrap.bootstrap_reference(
        _snapshot_release(rows), reference_spec=reference_spec, source_spec=source_spec,
        work_dir=tmp_path / "work", target=date(2026, 10, 13),
    )

    assert status.status == "success"
    assert "no prior merged" in status.message
    pd.testing.assert_frame_equal(
        pd.read_parquet(reference_spec.base_dir / "instruments_all.parquet"),
        _legacy_full_rebuild(_sessions(rows)),
    )
    # The bootstrap never writes into the daily run's own price store.
    assert not source_spec.base_dir.exists()


def test_bootstrap_from_data_latest_after_the_retirement(tmp_path: Path):
    """data-latest no longer lists ohlc, so the files are found by name: per
    year the newest `ohlc_{YYYY}.{sha8}.parquet`, checked against its sha8.
    An older superseded 2026 file beside it must lose."""
    rows = _history_across_years()
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps({"datasets": [{"name": "reference"}]}).encode())
    for year, data in _by_year(rows).items():
        sha = hashlib.sha256(data).hexdigest()
        fake.seed(asset_name(f"ohlc_{year}.parquet", sha), data, created_at="2026-10-12T20:00:00Z")
    stale = _year_bytes([_row("2026-06-01", "INE_STALE", "STALE")])
    fake.seed(asset_name("ohlc_2026.parquet", hashlib.sha256(stale).hexdigest()), stale,
              created_at="2026-10-06T16:00:00Z")
    fake.seed("delta_ohlc_2026-10-12.0badcafe.parquet", b"a delta, never read",
              created_at="2026-10-12T20:00:00Z")
    reference_spec, source_spec = _specs(tmp_path)

    status = bootstrap.bootstrap_reference(
        fake, reference_spec=reference_spec, source_spec=source_spec,
        work_dir=tmp_path / "work", target=date(2026, 10, 13),
    )

    assert status.status == "success"
    out = pd.read_parquet(reference_spec.base_dir / "instruments_all.parquet")
    assert "INE_STALE" not in set(out["instrument_key"])
    pd.testing.assert_frame_equal(out, _legacy_full_rebuild(_sessions(rows)))


def test_bootstrap_merges_the_synced_prior_unless_told_not_to(tmp_path: Path):
    """A listing first seen after the price files end survives a bootstrap
    (keep_prior), and is replaced with --ignore-prior."""
    rows = _history_across_years()
    reference_spec, source_spec = _specs(tmp_path)
    later = tmp_path / "later"
    later_rows = pd.DataFrame([_row("2026-10-12", "INE_LATE", "LATE")])[config.CANON_COLUMNS]
    later.mkdir()
    later_rows.to_parquet(later / "ohlc_2026.parquet", index=False)
    builders.rebuild_reference(
        reference_spec, date(2026, 10, 12),
        source_spec=dataclasses.replace(source_spec, base_dir=later),
    )

    bootstrap.bootstrap_reference(
        _snapshot_release(rows), reference_spec=reference_spec, source_spec=source_spec,
        work_dir=tmp_path / "work1", target=date(2026, 10, 13),
    )
    out = pd.read_parquet(reference_spec.base_dir / "instruments_all.parquet")
    assert "INE_LATE" in set(out["instrument_key"])

    bootstrap.bootstrap_reference(
        _snapshot_release(rows), reference_spec=reference_spec, source_spec=source_spec,
        work_dir=tmp_path / "work2", target=date(2026, 10, 13), keep_prior=False,
    )
    out = pd.read_parquet(reference_spec.base_dir / "instruments_all.parquet")
    assert "INE_LATE" not in set(out["instrument_key"])


def test_bootstrap_then_daily_merge_works_on_the_bootstrapped_list(tmp_path: Path):
    """The point of the bootstrap: the next daily build has a prior again."""
    rows = _history_across_years()
    days = sorted({r["date"] for r in rows})
    reference_spec, source_spec = _specs(tmp_path)
    bootstrap.bootstrap_reference(
        _snapshot_release([r for r in rows if r["date"] <= days[-2]]),
        reference_spec=reference_spec, source_spec=source_spec,
        work_dir=tmp_path / "work", target=date(2026, 10, 13),
    )
    run_store = tmp_path / "ohlc"
    run_store.mkdir()
    pd.DataFrame([r for r in rows if r["date"] >= days[-7]])[config.CANON_COLUMNS].to_parquet(
        run_store / "ohlc_2026.parquet", index=False
    )

    status = builders.build_reference(
        reference_spec, days[-1].date(),
        source_spec=dataclasses.replace(source_spec, base_dir=run_store),
        trading_calendar=_weekdays_only,
    )

    assert status.status == "success"
    pd.testing.assert_frame_equal(
        pd.read_parquet(reference_spec.base_dir / "instruments_all.parquet"),
        _legacy_full_rebuild(_sessions(rows)),
    )


def test_bootstrap_fails_closed_on_a_checksum_mismatch(tmp_path: Path):
    rows = _history_across_years()
    fake = _snapshot_release(rows)
    victim = next(a for a in fake.assets if a.startswith("ohlc_2026."))
    fake.assets[victim] = b"tampered"
    reference_spec, source_spec = _specs(tmp_path)

    with pytest.raises(UnexpectedFailure, match="checksum mismatch"):
        bootstrap.bootstrap_reference(
            fake, reference_spec=reference_spec, source_spec=source_spec,
            work_dir=tmp_path / "work", target=date(2026, 10, 13),
        )
    assert not (reference_spec.base_dir / "instruments_all.parquet").exists()


def test_bootstrap_fails_closed_on_a_sha8_mismatch_by_name(tmp_path: Path):
    fake = FakeReleaseClient(exists=True)
    fake.seed("ohlc_2026.0badcafe.parquet", _year_bytes(_history()))
    reference_spec, source_spec = _specs(tmp_path)

    with pytest.raises(UnexpectedFailure, match="checksum mismatch"):
        bootstrap.bootstrap_reference(
            fake, reference_spec=reference_spec, source_spec=source_spec,
            work_dir=tmp_path / "work", target=date(2026, 10, 13),
        )


def test_bootstrap_with_no_price_files_fails_closed(tmp_path: Path):
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps({"datasets": []}).encode())
    fake.seed("instruments_all.078501a5.parquet", b"x")
    reference_spec, source_spec = _specs(tmp_path)

    with pytest.raises(UnexpectedFailure, match="no ohlc_"):
        bootstrap.bootstrap_reference(
            fake, reference_spec=reference_spec, source_spec=source_spec,
            work_dir=tmp_path / "work", target=date(2026, 10, 13),
        )
    assert not (reference_spec.base_dir / "instruments_all.parquet").exists()
