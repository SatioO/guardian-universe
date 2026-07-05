import dataclasses
import hashlib
import json
from pathlib import Path

import pytest

from pipeline import datasets
from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.manifest import asset_name
from pipeline.sync import SYNCED_STATE, sync_store
from tests.fakes import FakeReleaseClient


@pytest.fixture()
def routed_equities(tmp_path, monkeypatch):
    spec = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "ohlc")
    monkeypatch.setattr(datasets, "DATASETS", {"equities": spec})
    monkeypatch.setattr(datasets, "DATASET_ORDER", ["equities"])
    return spec


def _seeded_release(data: bytes = b"PARQUETDATA") -> tuple[FakeReleaseClient, str]:
    sha = hashlib.sha256(data).hexdigest()
    manifest = {
        "schema_version": 1,
        "generated_at": "2026-07-04T16:00:00+00:00",
        "latest_trading_date": "2026-07-03",
        "datasets": [{"name": "ohlc", "files": [{
            "name": "ohlc_2026.parquet",
            "asset": asset_name("ohlc_2026.parquet", sha),
            "sha256": sha, "bytes": len(data), "rows": 1,
        }]}],
    }
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed(asset_name("ohlc_2026.parquet", sha), data)
    return fake, sha


def test_sync_downloads_verifies_and_writes_state(tmp_path: Path, routed_equities):
    fake, _ = _seeded_release()
    got = sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")
    assert got is not None and got["generated_at"] == "2026-07-04T16:00:00+00:00"
    # Asset is materialized under its LOGICAL name for the store to append onto.
    assert (routed_equities.base_dir / "ohlc_2026.parquet").read_bytes() == b"PARQUETDATA"
    state = json.loads((tmp_path / "meta" / SYNCED_STATE).read_text())
    assert state["generated_at"] == "2026-07-04T16:00:00+00:00"


def test_sync_reads_v2_manifest_with_baseline_key(tmp_path: Path, routed_equities):
    # v2 manifests (publish.py now emits) key each dataset's files under
    # "baseline" instead of "files" -- sync must read via dataset_files()
    # rather than indexing ds["files"] directly, or it fails closed with a
    # bare KeyError on every real publish.
    data = b"PARQUETDATA"
    sha = hashlib.sha256(data).hexdigest()
    manifest = {
        "manifest_version": 2,
        "generated_at": "2026-07-04T16:00:00+00:00",
        "latest_trading_date": "2026-07-03",
        "datasets": [{"name": "ohlc", "schema_version": 1, "latest_date": "2026-07-03",
                      "baseline": [{
                          "name": "ohlc_2026.parquet",
                          "asset": asset_name("ohlc_2026.parquet", sha),
                          "sha256": sha, "bytes": len(data), "rows": 1,
                      }], "deltas": []}],
    }
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed(asset_name("ohlc_2026.parquet", sha), data)

    got = sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")

    assert got is not None and got["generated_at"] == "2026-07-04T16:00:00+00:00"
    assert (routed_equities.base_dir / "ohlc_2026.parquet").read_bytes() == b"PARQUETDATA"
    state = json.loads((tmp_path / "meta" / SYNCED_STATE).read_text())
    assert state["generated_at"] == "2026-07-04T16:00:00+00:00"


def test_sync_no_release_writes_empty_state_and_returns_none(tmp_path: Path, routed_equities):
    fake = FakeReleaseClient(exists=False)
    got = sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")
    assert got is None
    state = json.loads((tmp_path / "meta" / SYNCED_STATE).read_text())
    assert state == {"generated_at": None}


def test_sync_checksum_mismatch_is_fatal(tmp_path: Path, routed_equities):
    fake, sha = _seeded_release()
    fake.assets[asset_name("ohlc_2026.parquet", sha)] = b"CORRUPTED"
    with pytest.raises(UnexpectedFailure, match="checksum"):
        sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")


def test_sync_download_failure_is_fatal_not_tolerated(tmp_path: Path, routed_equities):
    fake, _ = _seeded_release()
    fake.fail_after = 1  # exists() succeeds, first download fails
    with pytest.raises(ReleaseError):
        sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")


def test_sync_legacy_manifest_without_asset_key(tmp_path: Path, routed_equities):
    # Live v1 manifests name assets by logical name only.
    data = b"LEGACY"
    sha = hashlib.sha256(data).hexdigest()
    manifest = {"schema_version": 1, "generated_at": "g", "latest_trading_date": "2026-07-03",
                "datasets": [{"name": "ohlc", "files": [{
                    "name": "ohlc_2026.parquet", "sha256": sha, "bytes": len(data)}]}]}
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed("ohlc_2026.parquet", data)
    got = sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")
    assert got is not None
    assert (routed_equities.base_dir / "ohlc_2026.parquet").read_bytes() == b"LEGACY"


def test_sync_skips_unknown_dataset(tmp_path, routed_equities, capsys):
    data = b"OHLC"
    sha = hashlib.sha256(data).hexdigest()
    manifest = {"manifest_version": 2, "generated_at": "g", "latest_trading_date": "2026-07-03",
                "datasets": [
                    {"name": "ohlc", "baseline": [{"name": "ohlc_2026.parquet",
                        "asset": asset_name("ohlc_2026.parquet", sha),
                        "sha256": sha, "bytes": len(data), "rows": 1}], "deltas": []},
                    {"name": "breadth", "baseline": [{"name": "breadth_2026.parquet",
                        "sha256": "whatever", "bytes": 3}]}]}
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed(asset_name("ohlc_2026.parquet", sha), data)
    got = sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")
    assert got is not None
    assert (routed_equities.base_dir / "ohlc_2026.parquet").read_bytes() == b"OHLC"
    # Unknown dataset skipped: never downloaded, noted on stderr, sync still succeeds.
    assert "breadth" in capsys.readouterr().err


def test_sync_failure_on_second_file_leaves_ohlc_dir_untouched(tmp_path: Path, routed_equities):
    # Two ohlc files; the second's manifest sha doesn't match its bytes.
    # base_dir is pre-seeded with an existing file that must survive untouched,
    # and no new file may appear -- verify-all-then-materialize-all.
    data1 = b"GOODDATA1"
    sha1 = hashlib.sha256(data1).hexdigest()
    data2 = b"CORRUPTED-ON-DISK"
    correct_sha2 = hashlib.sha256(b"REAL-DATA-2").hexdigest()

    manifest = {
        "schema_version": 1, "generated_at": "g", "latest_trading_date": "2026-07-03",
        "datasets": [{"name": "ohlc", "files": [
            {
                "name": "ohlc_2024.parquet",
                "asset": asset_name("ohlc_2024.parquet", sha1),
                "sha256": sha1, "bytes": len(data1), "rows": 1,
            },
            {
                "name": "ohlc_2025.parquet",
                "asset": asset_name("ohlc_2025.parquet", correct_sha2),
                "sha256": correct_sha2, "bytes": len(data2), "rows": 1,
            },
        ]}],
    }
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed(asset_name("ohlc_2024.parquet", sha1), data1)
    # Seed corrupted bytes under the expected asset name -- sha won't match.
    fake.seed(asset_name("ohlc_2025.parquet", correct_sha2), data2)

    base_dir = routed_equities.base_dir
    base_dir.mkdir(parents=True)
    old_bytes = b"OLD-2025-DATA"
    (base_dir / "ohlc_2025.parquet").write_bytes(old_bytes)

    with pytest.raises(UnexpectedFailure):
        sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")

    # Directory completely untouched: old file has old bytes, no new files.
    assert (base_dir / "ohlc_2025.parquet").read_bytes() == old_bytes
    assert sorted(p.name for p in base_dir.iterdir()) == ["ohlc_2025.parquet"]


def test_sync_malformed_manifest_json_fails_closed(tmp_path: Path, routed_equities):
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", b"not json{")
    with pytest.raises(UnexpectedFailure):
        sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")


def test_sync_manifest_missing_required_key_fails_closed(tmp_path: Path, routed_equities):
    # "files" entries missing "sha256" must fail closed as UnexpectedFailure,
    # not escape as a raw KeyError, even once the asset itself downloads fine.
    data = b"SOMEDATA"
    manifest = {"schema_version": 1, "generated_at": "g", "latest_trading_date": "2026-07-03",
                "datasets": [{"name": "ohlc", "files": [{"name": "ohlc_2026.parquet"}]}]}
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed("ohlc_2026.parquet", data)
    with pytest.raises(UnexpectedFailure):
        sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")


def test_two_dataset_failure_rolls_back_everything(tmp_path: Path, monkeypatch):
    # Whole-manifest atomicity spans datasets, not just files within one
    # dataset: registry monkeypatched to two specs (equities + indices, tmp
    # base dirs); the manifest lists both. The SECOND dataset's asset is
    # corrupt on the release -- sync must raise UnexpectedFailure AND the
    # FIRST dataset's file must NOT be materialized (phase 1 verifies
    # everything before phase 2 touches any base_dir).
    equities_spec = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "ohlc")
    indices_spec = dataclasses.replace(datasets.INDICES, base_dir=tmp_path / "indices")
    monkeypatch.setattr(
        datasets, "DATASETS", {"equities": equities_spec, "indices": indices_spec}
    )
    monkeypatch.setattr(datasets, "DATASET_ORDER", ["equities", "indices"])

    good_data = b"GOODOHLCDATA"
    good_sha = hashlib.sha256(good_data).hexdigest()
    bad_data = b"CORRUPTED-ON-RELEASE"
    correct_bad_sha = hashlib.sha256(b"REAL-INDICES-DATA").hexdigest()

    manifest = {
        "manifest_version": 2, "generated_at": "g", "latest_trading_date": "2026-07-03",
        "datasets": [
            {"name": "ohlc", "baseline": [{
                "name": "ohlc_2026.parquet",
                "asset": asset_name("ohlc_2026.parquet", good_sha),
                "sha256": good_sha, "bytes": len(good_data), "rows": 1,
            }], "deltas": []},
            {"name": "indices", "baseline": [{
                "name": "indices_2026.parquet",
                "asset": asset_name("indices_2026.parquet", correct_bad_sha),
                "sha256": correct_bad_sha, "bytes": len(bad_data), "rows": 1,
            }], "deltas": []},
        ],
    }
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed(asset_name("ohlc_2026.parquet", good_sha), good_data)
    # Seed corrupted bytes under the expected asset name for indices -- sha
    # won't match what the manifest claims.
    fake.seed(asset_name("indices_2026.parquet", correct_bad_sha), bad_data)

    with pytest.raises(UnexpectedFailure):
        sync_store(fake, meta_dir=tmp_path / "meta", work_dir=tmp_path / "work")

    # Whole-manifest atomicity: the FIRST dataset's (equities) file must NOT
    # have been materialized even though it verified fine on its own.
    assert not equities_spec.base_dir.exists()
    assert not indices_spec.base_dir.exists()
