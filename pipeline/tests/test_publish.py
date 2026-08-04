import dataclasses
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from pipeline import config, datasets, store
from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.manifest import dataset_files, write_json
from pipeline.publish import (
    carry_forward_deltas,
    carry_forward_foreign_datasets,
    check_cas,
    check_no_shrink,
    latest_trading_date,
    owns_asset,
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


def _write_store(base: Path, days: list[str], *, prefix: str = "ohlc") -> None:
    # Mirrors _store's row-building logic but writes into an arbitrary
    # directory under an arbitrary file_prefix, so it can populate a second
    # (non-equities) dataset's base_dir -- e.g. indices -- without touching
    # _store's fixed ohlc/meta/stage shape used by the rest of this file.
    base.mkdir(parents=True, exist_ok=True)
    rows = {c: ["x"] * len(days) for c in config.CANON_COLUMNS}
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(days)
    df["instrument_key"] = [f"INE{i}" for i in range(len(days))]
    df.to_parquet(base / f"{prefix}_2026.parquet", compression="zstd", index=False)


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


def test_allow_shrink_permits_row_shrink_but_keeps_other_guards():
    # A deliberate correction that reduces a file's row count is allowed under
    # allow_shrink=True (no raise) ...
    new = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ca_flags", "files": [
        {"name": "ca_flags_2023.parquet", "rows": 7, "sha256": "s", "bytes": 1, "asset": "a"}]}]}
    live = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ca_flags", "files": [
        {"name": "ca_flags_2023.parquet", "rows": 3011, "sha256": "t", "bytes": 9, "asset": "b"}]}]}
    check_no_shrink(new, live, allow_shrink=True)  # no raise
    with pytest.raises(UnexpectedFailure, match="shrink"):
        check_no_shrink(new, live)  # default still blocks

    # ... but allow_shrink NEVER relaxes a missing file or a date regression.
    new_missing = {"latest_trading_date": "2026-07-03",
                   "datasets": [{"name": "ca_flags", "files": []}]}
    with pytest.raises(UnexpectedFailure, match="missing locally"):
        check_no_shrink(new_missing, live, allow_shrink=True)
    new_regress = {"latest_trading_date": "2026-07-01", "datasets": [{"name": "ca_flags", "files": [
        {"name": "ca_flags_2023.parquet", "rows": 7, "sha256": "s", "bytes": 1, "asset": "a"}]}]}
    with pytest.raises(UnexpectedFailure, match="regress"):
        check_no_shrink(new_regress, live, allow_shrink=True)


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


def test_publish_with_registered_but_empty_second_dataset(tmp_path: Path):
    # The exact merge-day state: BOTH specs are registered (equities +
    # indices) but the indices base_dir is empty/nonexistent -- the real
    # live state right after Task 3's DATASETS registration, before Task 9's
    # indices backfill goes live. Mechanism is correct by trace
    # (build_manifest omits empty datasets; the shrink-guard never sees an
    # unpublished dataset because it isn't in the live manifest either) --
    # this proves it end-to-end instead of by trace alone.
    eq = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "ohlc")
    idx = dataclasses.replace(datasets.INDICES, base_dir=tmp_path / "indices")
    meta, stage = tmp_path / "meta", tmp_path / "stage"
    meta.mkdir()

    _write_store(eq.base_dir, ["2026-07-03"])
    # idx.base_dir ("indices") is deliberately never created.

    _synced(meta, None)
    fake = FakeReleaseClient(exists=False)
    publish_dataset(specs=[eq, idx], meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="g1", now=NOW)

    assert_release_consistent(fake)
    live = json.loads(fake.assets["manifest.json"])
    assert [d["name"] for d in live["datasets"]] == ["ohlc"]  # indices omitted
    assert not any(name.startswith("indices_") for name in fake.assets)

    # Go-live: indices gets its first year file: additive, not disruptive.
    _write_store(idx.base_dir, ["2026-07-03"], prefix="indices")
    _synced(meta, "g1")
    publish_dataset(specs=[eq, idx], meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="g2", now=NOW)

    assert_release_consistent(fake)
    live2 = json.loads(fake.assets["manifest.json"])
    assert {d["name"] for d in live2["datasets"]} == {"ohlc", "indices"}


# ── P5 Phase 2: fundamentals via all_specs + multi-runner publish hygiene ──


def test_fundamentals_state_asset_is_protected_from_gc(tmp_path: Path):
    # The Rust producer's incremental state lives on the release as a
    # mutable, clobbered asset -- data-daily's GC must never collect it even
    # once it ages past the grace window (a superseded asset of the same age
    # in the SAME namespace IS collected, proving the protection is the name,
    # not the age; `owns_asset` would spare anything outside the namespace for
    # a different reason, so the comparator has to be one of ours to isolate
    # PROTECTED_ASSETS as the thing under test).
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)
    fake.seed("fundamentals_state.json", b"{}", created_at="2026-06-01T00:00:00Z")
    fake.seed("ohlc_2019.dead1234.parquet", b"stray", created_at="2026-06-01T00:00:00Z")
    _synced(meta, "gen-1")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-2", now=NOW)
    assert "fundamentals_state.json" in fake.assets   # protected, any age
    assert "ohlc_2019.dead1234.parquet" not in fake.assets  # ours, unprotected -> GC'd


def _fundamentals_frame() -> pd.DataFrame:
    # Minimal shape the manifest machinery needs: a `date` column (the Rust
    # producer mirrors as_of into it) + arbitrary payload columns.
    return pd.DataFrame({
        "date": pd.to_datetime(["2026-07-03", "2026-07-03"]),
        "instrument_key": ["INE002A01018", "INE040A01034"],
        "period_end": ["2026-03-31", "2026-03-31"],
        "net_profit": [100.0, 200.0],
    })


def test_fundamentals_publishes_via_all_specs_pattern(tmp_path: Path):
    # The sector_industry precedent: an externally-produced
    # fundamentals_all.parquet in the spec's base_dir is picked up by
    # build_manifest through the ordinary specs list -- no special-casing.
    eq = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "ohlc")
    fnd = dataclasses.replace(datasets.FUNDAMENTALS, base_dir=tmp_path / "fundamentals")
    meta, stage = tmp_path / "meta", tmp_path / "stage"
    meta.mkdir()
    _write_store(eq.base_dir, ["2026-07-03"])
    fnd.base_dir.mkdir()
    _fundamentals_frame().to_parquet(
        fnd.base_dir / "fundamentals_all.parquet", compression="zstd", index=False)

    _synced(meta, None)
    fake = FakeReleaseClient(exists=False)
    publish_dataset(specs=[eq, fnd], meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="g1", now=NOW)

    assert_release_consistent(fake)
    live = json.loads(fake.assets["manifest.json"])
    fnd_ds = next(d for d in live["datasets"] if d["name"] == "fundamentals")
    entry = dataset_files(fnd_ds)[0]
    assert entry["name"] == "fundamentals_all.parquet"
    assert entry["rows"] == 2
    assert entry["asset"] in fake.assets


def test_carry_forward_deltas_pure_rules():
    live = {"datasets": [
        {"name": "ohlc", "deltas": [
            {"name": "ohlc_2026-07-03.parquet", "asset": "delta_ohlc_2026-07-03.abc.parquet"},
            {"name": "ohlc_2026-07-02.parquet", "asset": "delta_ohlc_2026-07-02.gone.parquet"},
        ]},
    ]}
    existing = {"delta_ohlc_2026-07-03.abc.parquet"}

    # Local empty -> live deltas carried, minus assets no longer on the release.
    new = {"datasets": [{"name": "ohlc", "deltas": []}]}
    carry_forward_deltas(new, live, existing=existing)
    assert [d["asset"] for d in new["datasets"][0]["deltas"]] == [
        "delta_ohlc_2026-07-03.abc.parquet"
    ]

    # Local non-empty -> untouched (the data-daily runner's own list wins).
    own = [{"name": "ohlc_2026-07-04.parquet", "asset": "delta_ohlc_2026-07-04.new.parquet"}]
    new = {"datasets": [{"name": "ohlc", "deltas": list(own)}]}
    carry_forward_deltas(new, live, existing=existing)
    assert new["datasets"][0]["deltas"] == own

    # No live manifest / dataset absent from live -> no-op.
    new = {"datasets": [{"name": "ohlc", "deltas": []}]}
    carry_forward_deltas(new, None, existing=existing)
    assert new["datasets"][0]["deltas"] == []
    carry_forward_deltas(new, {"datasets": []}, existing=existing)
    assert new["datasets"][0]["deltas"] == []


def test_second_runner_publish_carries_live_deltas_forward(tmp_path: Path):
    # Runner A (data-daily) publishes a delta; runner B (fundamentals-daily,
    # a different ephemeral machine) syncs baselines only -- its publish must
    # NOT erase the live delta window.
    import shutil

    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    rows = {c: ["x"] for c in config.CANON_COLUMNS}
    delta_df = pd.DataFrame(rows)
    delta_df["date"] = pd.to_datetime(["2026-07-03"])
    delta_df["instrument_key"] = ["INE0"]
    store.write_delta(delta_df, ohlc, date(2026, 7, 3))
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)
    live1 = json.loads(fake.assets["manifest.json"])
    delta_asset = next(d for d in live1["datasets"] if d["name"] == "ohlc")["deltas"][0]["asset"]

    # Runner B: same baseline (sync materializes baselines only), no deltas dir.
    shutil.rmtree(ohlc / "deltas")
    _synced(meta, "gen-1")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-2", now=NOW)

    assert_release_consistent(fake)
    live2 = json.loads(fake.assets["manifest.json"])
    ohlc_ds = next(d for d in live2["datasets"] if d["name"] == "ohlc")
    assert [d["asset"] for d in ohlc_ds["deltas"]] == [delta_asset]
    assert delta_asset in fake.assets  # still referenced -> never GC'd


# ── Shared release: datasets THIS runner does not produce ──


def _seed_foreign_dataset(
    fake: FakeReleaseClient,
    *,
    name: str,
    generated_at: str,
    payload: bytes = b"foreign-parquet-bytes",
    created_at: str | None = None,
    with_assets: bool = True,
) -> dict[str, Any]:
    """Simulate ANOTHER producer publishing into the same `data-latest`
    release: put its content-addressed asset there and rewrite the live
    manifest so its dataset entry sits alongside ours (which is what the real
    foreign publish did -- it preserved every one of our datasets and only
    added its own). No spec of ours describes it, so `build_manifest` can
    never emit it.

    `with_assets=False` models the entry lingering after its asset is gone.
    Returns the dataset entry written, for verbatim comparison afterwards."""
    sha = hashlib.sha256(payload).hexdigest()
    asset = f"{name}_all.{sha[:8]}.parquet"
    if with_assets:
        fake.seed(asset, payload, created_at=created_at)
    ds: dict[str, Any] = {
        "name": name, "schema_version": 1, "latest_date": "2026-07-03",
        "baseline": [{"name": f"{name}_all.parquet", "asset": asset,
                      "sha256": sha, "bytes": len(payload), "rows": 3}],
        "deltas": [],
    }
    live = json.loads(fake.assets["manifest.json"])
    live["datasets"].append(ds)
    live["generated_at"] = generated_at
    fake.assets["manifest.json"] = json.dumps(live).encode()
    return ds


def test_shrink_guard_skips_a_dataset_this_runner_does_not_own():
    # `earnings_results` is on the live release but belongs to another
    # producer. It can never appear in a manifest built from OUR specs, so its
    # absence is not a shrink -- carry_forward_foreign_datasets owns its fate.
    # The SAME live/new pair is still a hard error the moment that name is
    # ours, and with owned=None (the pre-shared-release default).
    live = {"latest_trading_date": "2026-07-03", "datasets": [
        {"name": "ohlc", "baseline": [{"name": "ohlc_2026.parquet", "rows": 5000,
                                       "sha256": "t", "bytes": 9, "asset": "b"}]},
        {"name": "earnings_results", "baseline": [
            {"name": "earnings_results_all.parquet", "rows": 12, "sha256": "e",
             "bytes": 4, "asset": "e1"}]},
    ]}
    new = {"latest_trading_date": "2026-07-03", "datasets": [
        {"name": "ohlc", "baseline": [{"name": "ohlc_2026.parquet", "rows": 5000,
                                       "sha256": "t", "bytes": 9, "asset": "b"}],
         "deltas": []},
    ]}
    check_no_shrink(new, live, owned={"ohlc"})  # foreign -> not a shrink
    with pytest.raises(UnexpectedFailure, match="missing locally"):
        check_no_shrink(new, live, owned={"ohlc", "earnings_results"})
    with pytest.raises(UnexpectedFailure, match="missing locally"):
        check_no_shrink(new, live)


def test_owned_never_relaxes_the_row_shrink_or_date_regression_guards():
    # `owned` narrows exactly ONE branch (a whole dataset absent from `new`).
    # Every other protection is untouched for a dataset we do own.
    live = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "baseline": [
        {"name": "ohlc_2026.parquet", "rows": 5000, "sha256": "t", "bytes": 9, "asset": "b"}]}]}
    shrunk = {"latest_trading_date": "2026-07-03", "datasets": [{"name": "ohlc", "baseline": [
        {"name": "ohlc_2026.parquet", "rows": 1, "sha256": "s", "bytes": 1, "asset": "a"}]}]}
    with pytest.raises(UnexpectedFailure, match="rows 1 < live 5000"):
        check_no_shrink(shrunk, live, owned={"ohlc"})
    regressed = {"latest_trading_date": "2026-07-01", "datasets": [{"name": "ohlc", "baseline": [
        {"name": "ohlc_2026.parquet", "rows": 5000, "sha256": "t", "bytes": 9, "asset": "b"}]}]}
    with pytest.raises(UnexpectedFailure, match="regress"):
        check_no_shrink(regressed, live, owned={"ohlc"})
    lost_file = {"latest_trading_date": "2026-07-03",
                 "datasets": [{"name": "ohlc", "baseline": [], "deltas": []}]}
    with pytest.raises(UnexpectedFailure, match="missing locally"):
        check_no_shrink(lost_file, live, owned={"ohlc"})


def test_carry_forward_foreign_datasets_pure_rules():
    live = {"datasets": [
        {"name": "ohlc", "baseline": [{"name": "ohlc_2026.parquet", "asset": "ohlc.aaa.parquet",
                                       "sha256": "o", "bytes": 9}], "deltas": []},
        {"name": "earnings_results", "schema_version": 3, "latest_date": "2026-07-31",
         "baseline": [{"name": "earnings_results_all.parquet", "asset": "er.abc.parquet",
                       "sha256": "e", "bytes": 4}],
         "deltas": [{"date": "2026-07-31", "name": "earnings_results_2026-07-31.parquet",
                     "asset": "delta_er.def.parquet", "sha256": "d", "bytes": 2}]},
        {"name": "half_gone", "baseline": [{"name": "hg_all.parquet", "asset": "hg.abc.parquet",
                                            "sha256": "h", "bytes": 4}],
         "deltas": [{"date": "2026-07-31", "name": "hg_2026-07-31.parquet",
                     "asset": "delta_hg.gone.parquet", "sha256": "g", "bytes": 2}]},
        {"name": "legacy_no_asset", "files": [{"name": "legacy.parquet",
                                               "sha256": "l", "bytes": 4}]},
    ]}
    existing = {"ohlc.aaa.parquet", "er.abc.parquet", "delta_er.def.parquet", "hg.abc.parquet"}

    new: dict[str, Any] = {"datasets": [
        {"name": "ohlc", "baseline": [{"name": "ohlc_2026.parquet", "asset": "ohlc.bbb.parquet",
                                       "sha256": "o2", "bytes": 11}], "deltas": []}]}
    carry_forward_foreign_datasets(new, live, owned={"ohlc"}, existing=existing)

    # Owned -> left alone (build_manifest already described it, from newer
    # local bytes). Foreign + every asset present -> carried. A foreign
    # dataset with ONE absent asset ("half_gone") or an entry missing the
    # asset/sha256/bytes keys `_verify` and `_gc` index ("legacy_no_asset") ->
    # dropped whole rather than published in a shape its producer never wrote.
    assert [ds["name"] for ds in new["datasets"]] == ["ohlc", "earnings_results"]
    assert new["datasets"][0]["baseline"][0]["asset"] == "ohlc.bbb.parquet"
    assert new["datasets"][1] == live["datasets"][1]  # verbatim, deltas included

    # Deep copy: the carried entry must not alias the live manifest.
    new["datasets"][1]["baseline"][0]["bytes"] = 0
    assert live["datasets"][1]["baseline"][0]["bytes"] == 4

    # A foreign entry we cannot even READ is dropped, never raised on: this is
    # the one place another repo's JSON reaches publish, and an exception here
    # would stall the pipeline exactly like the shrink-guard did.
    malformed: dict[str, Any] = {"datasets": [{"name": "ohlc", "baseline": [], "deltas": []}]}
    carry_forward_foreign_datasets(
        malformed,
        {"datasets": [
            {"name": "null_deltas", "baseline": [{"name": "a", "asset": "er.abc.parquet",
                                                  "sha256": "e", "bytes": 4}], "deltas": None},
            {"name": "string_baseline", "baseline": "oops", "deltas": []},
            {"name": "entry_not_a_dict", "baseline": ["oops"], "deltas": []},
            # `bytes` is indexed by _verify's smallest-asset min() AFTER the
            # flip, where a failure is a red run on a good release that every
            # re-run reproduces. Refuse it before the flip instead.
            {"name": "unusable_bytes", "baseline": [{"name": "b", "asset": "er.abc.parquet",
                                                     "sha256": "e", "bytes": None}],
             "deltas": []},
        ]},
        owned={"ohlc"},
        existing=existing,
    )
    assert [ds["name"] for ds in malformed["datasets"]] == ["ohlc"]

    # No live manifest / nothing foreign on it -> no-op.
    ours: dict[str, Any] = {"datasets": [{"name": "ohlc", "baseline": [], "deltas": []}]}
    carry_forward_foreign_datasets(ours, None, owned={"ohlc"}, existing=existing)
    carry_forward_foreign_datasets(ours, {"datasets": []}, owned={"ohlc"}, existing=existing)
    assert [ds["name"] for ds in ours["datasets"]] == ["ohlc"]


def test_publish_carries_a_foreign_dataset_forward(tmp_path: Path, capsys):
    # The failure that stalled data-daily: another producer wrote
    # `earnings_results` onto the shared release. This runner has no such spec
    # and never builds one, so before the carry its publish died in the
    # shrink-guard -- and simply relaxing the guard would have flipped in a
    # manifest that unreferenced the other producer's data.
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)

    foreign = _seed_foreign_dataset(fake, name="earnings_results",
                                    generated_at="foreign-gen",
                                    created_at="2026-06-01T00:00:00Z")  # past GC_GRACE
    fake.seed("ohlc_2019.dead1234.parquet", b"stray", created_at="2026-06-01T00:00:00Z")
    _synced(meta, "foreign-gen")

    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-2", now=NOW)

    # _verify ran over the carried entry (its asset is the smallest on the
    # release, so it IS the post-flip sha-check target) and the flipped
    # manifest still references only present, sha-matching assets.
    assert_release_consistent(fake)
    live = json.loads(fake.assets["manifest.json"])
    by_name = {ds["name"]: ds for ds in live["datasets"]}
    assert by_name["earnings_results"] == foreign  # verbatim, not rebuilt
    assert dataset_files(by_name["ohlc"])[0]["asset"] in fake.assets  # ours still published
    # Referenced by the manifest we just flipped in -> GC spares it despite
    # aging out; an unreferenced asset of OURS of the SAME age proves the
    # protection is the reference, not the age.
    assert foreign["baseline"][0]["asset"] in fake.assets
    assert "ohlc_2019.dead1234.parquet" not in fake.assets
    # Announced on every publish: an abandoned producer would otherwise freeze
    # its latest_date on the release with nothing to notice it.
    assert "carrying foreign dataset 'earnings_results' forward" in capsys.readouterr().err


def test_publish_does_not_resurrect_a_foreign_dataset_whose_assets_are_gone(
    tmp_path: Path, capsys
):
    # The producer deleted its assets (or our GC already took them) but left
    # the entry on the manifest. Carrying it would republish a reference to
    # bytes that no longer exist -- the rule carry_forward_deltas already
    # follows via `existing`.
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)

    _seed_foreign_dataset(fake, name="earnings_results", generated_at="foreign-gen",
                          with_assets=False)
    _synced(meta, "foreign-gen")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-2", now=NOW)

    assert_release_consistent(fake)  # would fail if the dead asset were re-referenced
    live = json.loads(fake.assets["manifest.json"])
    assert [ds["name"] for ds in live["datasets"]] == ["ohlc"]
    assert "NOT carrying foreign dataset 'earnings_results'" in capsys.readouterr().err


def test_publish_still_fails_when_an_owned_dataset_vanishes(tmp_path: Path):
    # Ownership is registry membership: `indices` IS one of this runner's
    # specs, so its disappearance from the local store is precisely the
    # accident the shrink-guard exists to catch. The foreign carry must not
    # soften it -- publish must still hard-fail, before the flip.
    eq = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "ohlc")
    idx = dataclasses.replace(datasets.INDICES, base_dir=tmp_path / "indices")
    meta, stage = tmp_path / "meta", tmp_path / "stage"
    meta.mkdir()
    _write_store(eq.base_dir, ["2026-07-03"])
    _write_store(idx.base_dir, ["2026-07-03"], prefix="indices")
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False)
    publish_dataset(specs=[eq, idx], meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="g1", now=NOW)
    live1 = json.loads(fake.assets["manifest.json"])

    (idx.base_dir / "indices_2026.parquet").unlink()
    _synced(meta, "g1")
    with pytest.raises(UnexpectedFailure, match="missing locally"):
        publish_dataset(specs=[eq, idx], meta_dir=meta, stage_dir=stage, client=fake,
                        generated_at="g2", now=NOW)

    assert_release_consistent(fake)
    assert json.loads(fake.assets["manifest.json"]) == live1  # never flipped


def test_foreign_carry_forward_is_idempotent_across_publishes(tmp_path: Path):
    # Nothing is pinned: every publish recomputes the carry from live. The
    # second one reads back the entry WE wrote, so it must reproduce the same
    # manifest -- not duplicate, drop, or rewrite the foreign dataset.
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)

    _seed_foreign_dataset(fake, name="earnings_results", generated_at="foreign-gen")
    _synced(meta, "foreign-gen")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-2", now=NOW)
    live2 = json.loads(fake.assets["manifest.json"])

    _synced(meta, "gen-2")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-3", now=NOW)
    live3 = json.loads(fake.assets["manifest.json"])

    assert_release_consistent(fake)
    assert live3["datasets"] == live2["datasets"]
    assert [ds["name"] for ds in live3["datasets"]] == ["ohlc", "earnings_results"]


def test_gc_never_collects_assets_outside_this_runners_namespace(tmp_path: Path, capsys):
    # The destructive half of the shared-release assumption. `manifest.json`
    # is not the only index on the release: a sibling producer tracks datasets
    # of its own in its own index (`producer_manifest.json`), so its assets are
    # unreferenced BY US permanently and by design. Age them past the grace
    # window and the old GC deletes another producer's LIVE baselines -- data
    # loss, not a stalled workflow. Carrying the manifest entry forward does
    # not help here: these datasets are not in our manifest to carry.
    ohlc, meta, stage = _store(tmp_path, ["2026-07-03"])
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)

    aged = "2026-06-01T00:00:00Z"  # well past GC_GRACE
    for name in ("board_meetings_2026.7ac886cc.parquet",
                 "earnings_announcements_2026.5ce26a24.parquet",
                 "delta_board_meetings_2026-07-31.abc12345.parquet",
                 "producer_manifest.json"):
        fake.seed(name, b"theirs", created_at=aged)
    fake.seed("ohlc_2019.dead1234.parquet", b"ours", created_at=aged)
    _synced(meta, "gen-1")

    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-2", now=NOW)

    # Same age, same unreferenced-ness: only the one in OUR namespace goes.
    assert "ohlc_2019.dead1234.parquet" not in fake.assets
    assert "board_meetings_2026.7ac886cc.parquet" in fake.assets
    assert "earnings_announcements_2026.5ce26a24.parquet" in fake.assets
    assert "delta_board_meetings_2026-07-31.abc12345.parquet" in fake.assets  # delta_ too
    assert "producer_manifest.json" in fake.assets  # their index, not in PROTECTED_ASSETS
    # One bounded line whose COUNT is the signal, not one line per asset.
    err = capsys.readouterr().err
    assert "gc: leaving 4 unreferenced asset(s) outside this runner's namespace" in err


def test_owns_asset_covers_every_name_publish_writes():
    # The namespace rule is only safe if it recognises everything publish
    # uploads -- an asset of ours it failed to claim would leak forever.
    prefixes = frozenset({"ohlc", "indices", "index_constituents"})
    assert owns_asset("ohlc_2026.c4f18702.parquet", file_prefixes=prefixes)  # baseline
    assert owns_asset("delta_ohlc_2026-07-31.85d6ea40.parquet", file_prefixes=prefixes)  # delta
    assert owns_asset("ohlc_2026-07-03.parquet", file_prefixes=prefixes)  # quarantine extra
    assert owns_asset("index_constituents_all.44f061d2.parquet", file_prefixes=prefixes)
    # The `_` boundary is load-bearing, and getting it wrong is DESTRUCTIVE
    # (it claims a foreign asset, so GC deletes it). A sibling producer
    # publishing `ohlcv_*` on a market-data release is the obvious collision;
    # bare startswith would swallow it under our `ohlc`.
    assert not owns_asset("ohlcv_2026.5ce26a24.parquet", file_prefixes=prefixes)
    assert not owns_asset("delta_ohlcv_2026-07-31.abc12345.parquet", file_prefixes=prefixes)
    assert not owns_asset("indices2_all.parquet", file_prefixes=prefixes)
    # Foreign namespaces -- the real ones sharing data-latest today.
    for name in ("earnings_results_all.9aae4b2b.parquet",
                 "earnings_schedule_all.9acb50d1.parquet",
                 "earnings_announcements_2026.5ce26a24.parquet",
                 "board_meetings_2023.4ff221de.parquet",
                 "delta_board_meetings_2026-07-31.abc12345.parquet",
                 "producer_manifest.json"):
        assert not owns_asset(name, file_prefixes=prefixes), name
    # A registry with no specs claims nothing (so GC deletes nothing).
    assert not owns_asset("ohlc_2026.c4f18702.parquet", file_prefixes=frozenset())


def test_gc_still_collects_our_own_superseded_assets_across_every_spec(tmp_path: Path):
    # The namespace rule must not turn GC off. Every registered spec's own
    # superseded content-addressed assets, plus its deltas and its
    # diagnostic quarantine extra, still age out and get collected.
    eq = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "ohlc")
    idx = dataclasses.replace(datasets.INDICES, base_dir=tmp_path / "indices")
    meta, stage = tmp_path / "meta", tmp_path / "stage"
    meta.mkdir()
    _write_store(eq.base_dir, ["2026-07-03"])
    _write_store(idx.base_dir, ["2026-07-03"], prefix="indices")
    _synced(meta, None)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T16:00:00Z")
    publish_dataset(specs=[eq, idx], meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="g1", now=NOW)

    aged = "2026-06-01T00:00:00Z"
    ours = ["ohlc_2025.0badcafe.parquet", "indices_2025.0badcafe.parquet",
            "delta_ohlc_2026-06-30.0badcafe.parquet", "indices_2026-06-30.parquet"]
    for name in ours:
        fake.seed(name, b"superseded", created_at=aged)
    _synced(meta, "g1")
    publish_dataset(specs=[eq, idx], meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="g2", now=NOW)

    assert not (set(ours) & set(fake.assets))
