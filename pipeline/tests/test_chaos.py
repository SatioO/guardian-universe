"""G0 exit criteria: no interruption or race can lose or tear published data.

Strategy: drive real publish/sync against FakeReleaseClient, injecting a hard
failure after EVERY possible client operation, and assert the release-
consistency invariant after each crash.

Also includes two probes (flagged during Task 5 review) for scenarios the
brief's 4 core tests don't directly exercise: a never-synced CAS race against
a live release whose manifest download fails, and a malformed created_at
timestamp on a stray asset encountered during GC."""
import dataclasses
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from pipeline import config, datasets, store
from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.manifest import write_json
from pipeline.publish import publish_dataset
from pipeline.sync import SYNCED_STATE, sync_store
from tests.fakes import FakeReleaseClient, assert_release_consistent

NOW = datetime(2026, 7, 5, 16, 0, tzinfo=UTC)


def specs_for(base: Path) -> list[datasets.DatasetSpec]:
    return [dataclasses.replace(datasets.EQUITIES, base_dir=base)]


def _write_store(ohlc: Path, days: list[str]) -> None:
    ohlc.mkdir(parents=True, exist_ok=True)
    rows = {c: ["x"] * len(days) for c in config.CANON_COLUMNS}
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(days)
    df["instrument_key"] = [f"INE{i}" for i in range(len(days))]
    df.to_parquet(ohlc / "ohlc_2026.parquet", compression="zstd", index=False)


def _published_fixture(tmp_path: Path) -> tuple[FakeReleaseClient, Path, Path, Path]:
    """A release with one good day published (plus one delta day, so the
    chaos loop's kill-at-every-op sweep also exercises delta uploads), synced
    state in agreement."""
    ohlc, meta, stage = tmp_path / "ohlc", tmp_path / "meta", tmp_path / "stage"
    meta.mkdir(parents=True, exist_ok=True)
    _write_store(ohlc, ["2026-07-02"])
    day = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
    day["date"] = pd.to_datetime(["2026-07-02"])
    day["instrument_key"] = ["INE0"]
    store.write_delta(day, ohlc, date(2026, 7, 2))
    write_json({"generated_at": None}, meta / SYNCED_STATE)
    fake = FakeReleaseClient(exists=False, now_iso="2026-07-05T15:00:00Z")
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                    generated_at="gen-1", now=NOW)
    assert_release_consistent(fake)
    return fake, ohlc, meta, stage


def test_publish_killed_after_every_op_never_tears_the_release(tmp_path: Path):
    baseline, *_ = _published_fixture(tmp_path)
    baseline_snapshot = dict(baseline.assets)

    k = 0
    while True:
        k += 1
        fake, ohlc, meta, stage = _published_fixture(tmp_path / f"run{k}")
        _write_store(ohlc, ["2026-07-02", "2026-07-03"])  # day-2 grows the store
        day2 = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
        day2["date"] = pd.to_datetime(["2026-07-03"])
        day2["instrument_key"] = ["INE1"]
        store.write_delta(day2, ohlc, date(2026, 7, 3))  # day-2 also uploads a delta
        fake.ops = 0
        fake.fail_after = k
        try:
            publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                        generated_at="gen-2", now=NOW)
            # publish can now complete successfully even with a late-op
            # injection: _gc is fully non-fatal on ReleaseError (including
            # its internal list_assets), so a failure injected deep enough
            # to only hit GC's listing is swallowed and publish still
            # finishes normally. That is a legitimate completion, not a
            # loop bug -- assert the invariant and stop.
            assert_release_consistent(fake)
            break
        except (ReleaseError, UnexpectedFailure):
            # However far we got, whatever manifest is live references only
            # complete, sha-correct assets.
            assert_release_consistent(fake)
        if k > 100:  # safety valve: something is wrong if we never terminate
            raise AssertionError("chaos loop did not terminate within 100 kill points")
    assert_release_consistent(fake)
    assert k > 3  # sanity: we actually exercised multiple kill points
    assert baseline_snapshot  # silence unused warning; baseline stays valid


def test_interrupted_publish_leaves_old_manifest_serving_old_data(tmp_path: Path):
    fake, ohlc, meta, stage = _published_fixture(tmp_path)
    old_manifest = json.loads(fake.assets["manifest.json"].decode())
    _write_store(ohlc, ["2026-07-02", "2026-07-03"])
    day2 = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
    day2["date"] = pd.to_datetime(["2026-07-03"])
    day2["instrument_key"] = ["INE1"]
    store.write_delta(day2, ohlc, date(2026, 7, 3))  # day-2 also uploads a delta
    fake.ops = 0
    fake.fail_after = 4  # dies before reaching the manifest flip
    with pytest.raises((ReleaseError, UnexpectedFailure)):
        publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                        generated_at="gen-2", now=NOW)
    # The pointer never flipped: clients still read the old, fully-valid set.
    assert json.loads(fake.assets["manifest.json"].decode()) == old_manifest
    assert_release_consistent(fake)


def test_failed_sync_then_publish_cannot_wipe_history(tmp_path: Path):
    """The exact P0-1 scenario, end to end."""
    fake, ohlc, meta, stage = _published_fixture(tmp_path)

    # Fresh runner: empty local store, sync fails transiently.
    runner2 = tmp_path / "runner2"
    ohlc2, meta2, stage2 = runner2 / "ohlc", runner2 / "meta", runner2 / "stage"
    fake.ops = 0
    fake.fail_after = 1  # exists() ok, manifest download dies
    with pytest.raises(ReleaseError):
        sync_store(fake, ohlc_dir=ohlc2, meta_dir=meta2, work_dir=runner2 / "work")
    fake.fail_after = None

    # Even if an operator force-runs publish afterwards, guards refuse:
    # (a) empty store -> refuse; (b) one-day store -> no synced state / CAS / shrink.
    with pytest.raises(UnexpectedFailure):
        publish_dataset(specs=specs_for(ohlc2), meta_dir=meta2, stage_dir=stage2, client=fake,
                        generated_at="gen-evil", now=NOW)
    _write_store(ohlc2, ["2026-07-05"])  # a lone new day, no history
    with pytest.raises(UnexpectedFailure):
        publish_dataset(specs=specs_for(ohlc2), meta_dir=meta2, stage_dir=stage2, client=fake,
                        generated_at="gen-evil", now=NOW)
    assert_release_consistent(fake)  # history intact throughout


def test_concurrent_publisher_is_detected_by_cas(tmp_path: Path):
    fake, ohlc, meta, stage = _published_fixture(tmp_path)
    _write_store(ohlc, ["2026-07-02", "2026-07-03"])

    # Simulate another publisher flipping the manifest after our sync:
    live = json.loads(fake.assets["manifest.json"].decode())
    live["generated_at"] = "someone-else"
    fake.assets["manifest.json"] = json.dumps(live).encode()

    with pytest.raises(UnexpectedFailure, match="changed since sync"):
        publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                        generated_at="gen-2", now=NOW)
    assert_release_consistent(fake)


def test_probe_never_synced_cas_race_aborts_instead_of_clobbering(
    tmp_path: Path, monkeypatch
):
    """Probe A: a never-synced runner (synced state {"generated_at": None})
    races against a live release that already has a real, published manifest.
    If the manifest download happens to fail transiently on this run, the old
    `_read_live_manifest` swallowed ANY ReleaseError into `None` -- indistin-
    guishable from "no release yet". That let CAS(None, None) pass and
    check_no_shrink(new, None) no-op, so publish would proceed to overwrite a
    live, populated release with a runner that has no knowledge of history.

    This is a real hole: the fix in publish.py distinguishes "manifest is
    listed as present but failed to download" (raise UnexpectedFailure) from
    "manifest is genuinely absent from list_assets()" (return None, business
    as usual for a fresh release)."""
    ohlc, meta, stage = tmp_path / "ohlc", tmp_path / "meta", tmp_path / "stage"
    meta.mkdir(parents=True, exist_ok=True)

    # A prior publish exists live: seed a real manifest + its asset directly
    # (bypassing publish_dataset, since we want a fully-formed prior release
    # without entangling this runner's own history with it).
    fake = FakeReleaseClient(exists=True, now_iso="2026-07-01T00:00:00Z")
    prior_data = b"prior-day-parquet-bytes"
    import hashlib
    sha = hashlib.sha256(prior_data).hexdigest()
    asset_name = f"ohlc_2026.{sha[:8]}.parquet"
    fake.seed(asset_name, prior_data)
    prior_manifest = {
        "schema_version": 1,
        "generated_at": "prior-publisher",
        "latest_trading_date": "2026-06-30",
        "datasets": [{"name": "ohlc", "files": [
            {"name": "ohlc_2026.parquet", "asset": asset_name,
             "sha256": sha, "bytes": len(prior_data), "rows": 1},
        ]}],
    }
    fake.seed("manifest.json", json.dumps(prior_manifest).encode())

    # This runner never synced: {"generated_at": None}, simulating a sync
    # that raced the creator and saw no release, or a fresh checkout.
    write_json({"generated_at": None}, meta / SYNCED_STATE)
    _write_store(ohlc, ["2026-07-05"])  # a lone new day, no history of the prior release

    # Make the manifest download fail exactly once, ONLY for manifest.json,
    # simulating a transient read failure during _read_live_manifest. Any
    # other download (e.g. during _verify, post-fix-abort shouldn't reach
    # there) is unaffected.
    real_download = fake.download
    state = {"manifest_download_calls": 0}

    def flaky_download(names: list[str], dest: Path) -> None:
        if names == ["manifest.json"] and state["manifest_download_calls"] == 0:
            state["manifest_download_calls"] += 1
            raise ReleaseError("transient network failure reading manifest.json")
        real_download(names, dest)

    monkeypatch.setattr(fake, "download", flaky_download)

    with pytest.raises(UnexpectedFailure):
        publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                        generated_at="gen-evil", now=NOW)

    # The prior release must be untouched: same manifest, same asset.
    assert json.loads(fake.assets["manifest.json"].decode()) == prior_manifest
    assert fake.assets[asset_name] == prior_data
    assert_release_consistent(fake)


def test_probe_malformed_created_at_is_skipped_by_gc_not_fatal(tmp_path: Path):
    """Probe B: a stray unreferenced asset has a malformed created_at
    ("not-a-timestamp"). The old `_gc` called
    `datetime.fromisoformat(a.created_at.replace(...))` unguarded -- a
    ValueError from a genuinely malformed timestamp is not a ReleaseError, so
    it escaped `_gc`'s try/except and failed the publish AFTER the manifest
    had already flipped (i.e. it would report failure on a run that actually
    succeeded from clients' point of view).

    Fix: treat an unparseable created_at as "too young to GC" (skip, warn on
    stderr) via a narrow ValueError catch around just the parse."""
    fake, ohlc, meta, stage = _published_fixture(tmp_path)

    # A stray, unreferenced asset with a malformed timestamp.
    fake.seed("stray.parquet", b"stray-bytes", created_at="not-a-timestamp")

    _write_store(ohlc, ["2026-07-02", "2026-07-03"])  # new day -> triggers another publish
    write_json({"generated_at": "gen-1"}, meta / SYNCED_STATE)

    # Must not raise -- GC must treat the bad timestamp as non-fatal.
    publish_dataset(specs=specs_for(ohlc), meta_dir=meta, stage_dir=stage, client=fake,
                        generated_at="gen-2", now=NOW)

    assert "stray.parquet" in fake.assets  # spared, not GC'd, not fatal
    assert_release_consistent(fake)
    live = json.loads(fake.assets["manifest.json"].decode())
    assert live["generated_at"] == "gen-2"
