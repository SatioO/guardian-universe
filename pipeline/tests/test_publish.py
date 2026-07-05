import dataclasses
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from pipeline import config, datasets, store
from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.manifest import dataset_files, write_json
from pipeline.publish import (
    check_cas,
    check_no_shrink,
    latest_trading_date,
    publish_dataset,
)
from pipeline.sync import SYNCED_STATE
from tests.fakes import FakeReleaseClient, assert_release_consistent

NOW = datetime(2026, 7, 5, 16, 0, tzinfo=UTC)


def specs_for(base: Path) -> list[datasets.DatasetSpec]:
    return [dataclasses.replace(datasets.EQUITIES, base_dir=base)]


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
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="2026-07-05T16:00:00+00:00", now=NOW)
    assert_release_consistent(fake)
    live = json.loads(fake.assets["manifest.json"])
    entry = dataset_files(live["datasets"][0])[0]
    assert entry["asset"].startswith("ohlc_2026.") and entry["asset"] != "ohlc_2026.parquet"


def test_publish_requires_prior_sync(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    fake = FakeReleaseClient(exists=False)
    with pytest.raises(UnexpectedFailure, match="sync"):
        publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="g", now=NOW)


def test_cas_aborts_when_release_moved_since_sync(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, "2026-07-04T16:00:00+00:00")  # what we synced
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps({
        "generated_at": "2026-07-05T09:00:00+00:00",  # someone published since
        "latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": []}],
    }).encode())
    with pytest.raises(UnexpectedFailure, match="changed since sync"):
        publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="g", now=NOW)
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


def test_shrink_guard_matches_datasets_by_name_not_position():
    # Live has TWO datasets ("reference" first, "ohlc" second). Positional
    # indexing (live["datasets"][0]) would compare "reference" against the
    # new manifest's "ohlc" entry and silently miss a real ohlc regression --
    # matching must be by dataset "name" across ALL live datasets.
    live = {"latest_trading_date": "2026-07-03", "datasets": [
        {"name": "reference", "files": [
            {"name": "instruments.parquet", "sha256": "r", "bytes": 1}]},
        {"name": "ohlc", "files": [
            {"name": "ohlc_2026.parquet", "rows": 5000, "sha256": "t", "bytes": 9}]},
    ]}
    new = {"latest_trading_date": "2026-07-03", "datasets": [
        {"name": "reference", "files": [
            {"name": "instruments.parquet", "sha256": "r", "bytes": 1}]},
        {"name": "ohlc", "files": [
            {"name": "ohlc_2026.parquet", "rows": 1, "sha256": "s", "bytes": 1, "asset": "a"}]},
    ]}
    with pytest.raises(UnexpectedFailure, match="shrink"):
        check_no_shrink(new, live)


def test_shrink_guard_blocks_missing_live_dataset_dropped_entirely():
    # A live dataset with non-empty files that's absent from the new manifest
    # entirely (not just shrunk) must also trip the shrink guard.
    live = {"latest_trading_date": "2026-07-03", "datasets": [
        {"name": "ohlc", "files": [
            {"name": "ohlc_2026.parquet", "rows": 5000, "sha256": "t", "bytes": 9}]},
        {"name": "reference", "files": [
            {"name": "instruments.parquet", "sha256": "r", "bytes": 1}]},
    ]}
    new = {"latest_trading_date": "2026-07-03", "datasets": [
        {"name": "ohlc", "files": [
            {"name": "ohlc_2026.parquet", "rows": 5000, "sha256": "t", "bytes": 9, "asset": "a"}]},
    ]}
    with pytest.raises(UnexpectedFailure, match="shrink"):
        check_no_shrink(new, live)


def test_shrink_guard_tolerates_legacy_live_without_rows():
    live = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2026.parquet", "sha256": "t", "bytes": 9}]}]}  # no "rows"
    new = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "files": [
        {"name": "ohlc_2026.parquet", "rows": 1, "sha256": "s", "bytes": 1, "asset": "a"}]}]}
    check_no_shrink(new, live)  # must not raise


def test_shrink_guard_blocks_missing_live_dataset():
    live = {"latest_trading_date": "2026-07-03", "datasets": [
        {"name": "ohlc", "files": [{"name": "ohlc_2026.parquet", "sha256": "s", "bytes": 1}]},
        {"name": "indices", "baseline": [{"name": "indices_2026.parquet", "sha256": "t",
                                          "bytes": 1, "rows": 5, "asset": "a"}]}]}
    new = {"latest_trading_date": "2026-07-03", "datasets": [
        {"name": "ohlc", "baseline": [{"name": "ohlc_2026.parquet", "sha256": "s",
                                       "bytes": 1, "rows": 9, "asset": "b"}], "deltas": []}]}
    with pytest.raises(UnexpectedFailure, match="shrink"):
        check_no_shrink(new, live)  # live 'indices' dataset vanished locally


def test_check_cas_passes_when_both_none():
    check_cas(None, {"generated_at": None})


def test_second_publish_skips_existing_assets_and_gcs_old(tmp_path: Path):
    # Day 1 publish
    ohlc, meta, stage = _store(tmp_path, ["2026-07-02"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-06-20T16:00:00Z")  # old uploads
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)
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
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-2", now=NOW)

    assert_release_consistent(fake)
    # Old day-1 asset was uploaded >7 days ago and is unreferenced -> GC'd.
    assert not (old_assets & set(fake.assets))


def test_gc_spares_young_and_protected_assets(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T15:00:00Z")  # 1h old
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)
    fake.seed("stray.parquet", b"stray", created_at="2026-07-05T15:30:00Z")  # young stray
    _synced(meta, "gen-1")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-2", now=NOW)
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
        publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)


def test_gc_list_assets_failure_does_not_fail_publish(tmp_path: Path, monkeypatch):
    # Day 1 baseline publish, succeeds normally.
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)

    # Day 2: re-sync to match live, then make list_assets raise ONLY on the
    # GC-internal call (the second call during the publish run) so the flip
    # still succeeds but GC's listing is transiently unavailable.
    _synced(meta, "gen-1")
    real_list_assets = fake.list_assets
    calls = {"n": 0}

    def flaky_list_assets():
        calls["n"] += 1
        if calls["n"] == 2:  # the GC call
            raise ReleaseError("transient listing failure")
        return real_list_assets()

    monkeypatch.setattr(fake, "list_assets", flaky_list_assets)

    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-2", now=NOW)

    live = json.loads(fake.assets["manifest.json"])
    assert live["generated_at"] == "gen-2"
    synced = json.loads((meta / SYNCED_STATE).read_text())
    assert synced["generated_at"] == "gen-2"


def test_manifest_upload_is_always_last(tmp_path: Path, monkeypatch):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")

    uploads: list[str] = []
    real_upload = fake.upload

    def recording_upload(path: Path, *, clobber: bool = False) -> None:
        uploads.append(path.name)
        real_upload(path, clobber=clobber)

    monkeypatch.setattr(fake, "upload", recording_upload)

    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)

    assert uploads[-1] == "manifest.json"
    manifest_index = uploads.index("manifest.json")
    data_asset_indices = [i for i, name in enumerate(uploads) if name != "manifest.json"]
    assert data_asset_indices, "expected at least one data-asset upload"
    assert all(i < manifest_index for i in data_asset_indices)


def test_publish_uploads_manifest_listed_deltas(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)

    # Write a real delta artifact via store.write_delta -- this is what
    # build_manifest's per-dataset "deltas" list is populated from.
    rows = {c: ["x"] for c in config.CANON_COLUMNS}
    delta_df = pd.DataFrame(rows)
    delta_df["date"] = pd.to_datetime(["2026-07-03"])
    delta_df["instrument_key"] = ["INE0"]
    store.write_delta(delta_df, ohlc, date(2026, 7, 3))

    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="2026-07-05T16:00:00+00:00", now=NOW)

    assert_release_consistent(fake)
    live = json.loads(fake.assets["manifest.json"])
    ohlc_ds = next(d for d in live["datasets"] if d["name"] == "ohlc")
    assert ohlc_ds["deltas"], "expected at least one delta entry in the manifest"
    delta_asset = ohlc_ds["deltas"][0]["asset"]
    assert delta_asset.startswith("delta_ohlc_")
    assert delta_asset in fake.assets


def test_latest_trading_date_raises_on_empty_store(tmp_path: Path):
    with pytest.raises(UnexpectedFailure):
        latest_trading_date(specs_for(tmp_path)[0])


def test_latest_trading_date_reads_max(tmp_path: Path):
    ohlc, _, _ = _store(tmp_path, ["2026-07-02", "2026-07-03"])
    assert latest_trading_date(specs_for(ohlc)[0]) == date(2026, 7, 3)


def test_publish_uploads_quarantine_extra(tmp_path: Path):
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    qdir = meta / "quarantine"
    qdir.mkdir()
    pd.DataFrame({"x": [1]}).to_parquet(qdir / "ohlc_2026-07-03.parquet")
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False)
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="g1", now=NOW)
    assert "ohlc_2026-07-03.parquet" in fake.assets  # diagnostic extra, unreferenced -> self-GCs
