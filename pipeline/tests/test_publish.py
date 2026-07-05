import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from pipeline import config
from pipeline.errors import UnexpectedFailure
from pipeline.manifest import write_json
from pipeline.publish import (
    check_cas,
    check_no_shrink,
    latest_trading_date,
    publish_dataset,
)
from pipeline.sync import SYNCED_STATE
from tests.fakes import FakeReleaseClient, assert_release_consistent

NOW = datetime(2026, 7, 5, 16, 0, tzinfo=UTC)


def _store(tmp_path: Path, days: list[str]) -> tuple[Path, Path, Path]:
    ohlc, meta, stage = tmp_path / "ohlc", tmp_path / "meta", tmp_path / "stage"
    ohlc.mkdir()
    meta.mkdir()
    rows = {c: ["x"] * len(days) for c in config.CANON_COLUMNS}
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(days)
    df["instrument_key"] = [f"INE{i}" for i in range(len(days))]
    df.to_parquet(ohlc / "ohlc_2026.parquet", compression="zstd", index=False)
    return ohlc, meta, stage


def _synced(meta: Path, generated_at: str | None) -> None:
    write_json({"generated_at": generated_at}, meta / SYNCED_STATE)


def test_first_publish_creates_release_flips_manifest_last(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")
    publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                    schema_version=1, generated_at="2026-07-05T16:00:00+00:00", now=NOW)
    assert_release_consistent(fake)
    live = json.loads(fake.assets["manifest.json"])
    entry = live["datasets"][0]["files"][0]
    assert entry["asset"].startswith("ohlc_2026.") and entry["asset"] != "ohlc_2026.parquet"


def test_publish_requires_prior_sync(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    fake = FakeReleaseClient(exists=False)
    with pytest.raises(UnexpectedFailure, match="sync"):
        publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                        schema_version=1, generated_at="g", now=NOW)


def test_cas_aborts_when_release_moved_since_sync(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, "2026-07-04T16:00:00+00:00")  # what we synced
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps({
        "generated_at": "2026-07-05T09:00:00+00:00",  # someone published since
        "latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": []}],
    }).encode())
    with pytest.raises(UnexpectedFailure, match="changed since sync"):
        publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                        schema_version=1, generated_at="g", now=NOW)
    assert_release_consistent(fake)


def test_shrink_guard_blocks_row_regression():
    new = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2026.parquet", "rows": 1, "sha256": "s", "bytes": 1, "asset": "a"}]}]}
    live = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2026.parquet", "rows": 5000, "sha256": "t", "bytes": 9, "asset": "b"}]}]}
    with pytest.raises(UnexpectedFailure, match="shrink"):
        check_no_shrink(new, live)


def test_shrink_guard_blocks_missing_year_and_date_regression():
    live = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2025.parquet", "sha256": "t", "bytes": 9}]}]}
    new_missing = {"latest_trading_date": "2026-07-03",
                   "datasets": [{"name": "ohlc", "files": []}]}
    with pytest.raises(UnexpectedFailure, match="shrink"):
        check_no_shrink(new_missing, live)
    new_regress = {"latest_trading_date": "2026-07-01", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2025.parquet", "rows": 1, "sha256": "s", "bytes": 1, "asset": "a"}]}]}
    with pytest.raises(UnexpectedFailure, match="regress"):
        check_no_shrink(new_regress, live)


def test_shrink_guard_tolerates_legacy_live_without_rows():
    live = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2026.parquet", "sha256": "t", "bytes": 9}]}]}  # no "rows"
    new = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2026.parquet", "rows": 1, "sha256": "s", "bytes": 1, "asset": "a"}]}]}
    check_no_shrink(new, live)  # must not raise


def test_check_cas_passes_when_both_none():
    check_cas(None, {"generated_at": None})


def test_second_publish_skips_existing_assets_and_gcs_old(tmp_path: Path):
    # Day 1 publish
    ohlc, meta, stage = _store(tmp_path, ["2026-07-02"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-06-20T16:00:00Z")  # old uploads
    publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                    schema_version=1, generated_at="gen-1", now=NOW)
    old_assets = set(fake.assets) - {"manifest.json"}

    # Day 2: store grows; re-sync state to match live
    df = pd.read_parquet(ohlc / "ohlc_2026.parquet")
    row = df.iloc[[0]].copy()
    row["date"] = pd.to_datetime(["2026-07-03"])
    row["instrument_key"] = ["INE9"]
    pd.concat([df, row], ignore_index=True).to_parquet(
        ohlc / "ohlc_2026.parquet", compression="zstd", index=False)
    _synced(meta, "gen-1")
    fake.now_iso = "2026-07-05T16:00:00Z"
    publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                    schema_version=1, generated_at="gen-2", now=NOW)

    assert_release_consistent(fake)
    # Old day-1 asset was uploaded >7 days ago and is unreferenced -> GC'd.
    assert not (old_assets & set(fake.assets))


def test_gc_spares_young_and_protected_assets(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T15:00:00Z")  # 1h old
    publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                    schema_version=1, generated_at="gen-1", now=NOW)
    fake.seed("stray.parquet", b"stray", created_at="2026-07-05T15:30:00Z")  # young stray
    _synced(meta, "gen-1")
    publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                    schema_version=1, generated_at="gen-2", now=NOW)
    assert "stray.parquet" in fake.assets  # younger than grace -> spared
    assert "manifest.json" in fake.assets


def test_post_publish_verify_detects_manifest_tamper(tmp_path: Path, monkeypatch):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False)

    real_upload = fake.upload

    def tampering_upload(path: Path, *, clobber: bool = False) -> None:
        real_upload(path, clobber=clobber)
        if path.name == "manifest.json":  # simulate a racing writer post-flip
            fake.assets["manifest.json"] = json.dumps({"generated_at": "evil",
                                                       "latest_trading_date": "2026-07-03",
                                                       "datasets": []}).encode()

    monkeypatch.setattr(fake, "upload", tampering_upload)
    with pytest.raises(UnexpectedFailure, match="verification"):
        publish_dataset(ohlc_dir=ohlc, meta_dir=meta, stage_dir=stage, client=fake,
                        schema_version=1, generated_at="gen-1", now=NOW)


def test_latest_trading_date_raises_on_empty_store(tmp_path: Path):
    with pytest.raises(UnexpectedFailure):
        latest_trading_date(tmp_path)


def test_latest_trading_date_reads_max(tmp_path: Path):
    ohlc, _, _ = _store(tmp_path, ["2026-07-02", "2026-07-03"])
    assert latest_trading_date(ohlc) == date(2026, 7, 3)
