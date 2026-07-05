import hashlib
import json
from pathlib import Path

import pytest

from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.manifest import asset_name
from pipeline.sync import SYNCED_STATE, sync_store
from tests.fakes import FakeReleaseClient


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


def test_sync_downloads_verifies_and_writes_state(tmp_path: Path):
    fake, _ = _seeded_release()
    got = sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                     work_dir=tmp_path / "work")
    assert got is not None and got["generated_at"] == "2026-07-04T16:00:00+00:00"
    # Asset is materialized under its LOGICAL name for the store to append onto.
    assert (tmp_path / "ohlc" / "ohlc_2026.parquet").read_bytes() == b"PARQUETDATA"
    state = json.loads((tmp_path / "meta" / SYNCED_STATE).read_text())
    assert state["generated_at"] == "2026-07-04T16:00:00+00:00"


def test_sync_no_release_writes_empty_state_and_returns_none(tmp_path: Path):
    fake = FakeReleaseClient(exists=False)
    got = sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                     work_dir=tmp_path / "work")
    assert got is None
    state = json.loads((tmp_path / "meta" / SYNCED_STATE).read_text())
    assert state == {"generated_at": None}


def test_sync_checksum_mismatch_is_fatal(tmp_path: Path):
    fake, sha = _seeded_release()
    fake.assets[asset_name("ohlc_2026.parquet", sha)] = b"CORRUPTED"
    with pytest.raises(UnexpectedFailure, match="checksum"):
        sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                   work_dir=tmp_path / "work")


def test_sync_download_failure_is_fatal_not_tolerated(tmp_path: Path):
    fake, _ = _seeded_release()
    fake.fail_after = 1  # exists() succeeds, first download fails
    with pytest.raises(ReleaseError):
        sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                   work_dir=tmp_path / "work")


def test_sync_legacy_manifest_without_asset_key(tmp_path: Path):
    # Live v1 manifests name assets by logical name only.
    data = b"LEGACY"
    sha = hashlib.sha256(data).hexdigest()
    manifest = {"schema_version": 1, "generated_at": "g", "latest_trading_date": "2026-07-03",
                "datasets": [{"name": "ohlc", "files": [{
                    "name": "ohlc_2026.parquet", "sha256": sha, "bytes": len(data)}]}]}
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed("ohlc_2026.parquet", data)
    got = sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                     work_dir=tmp_path / "work")
    assert got is not None
    assert (tmp_path / "ohlc" / "ohlc_2026.parquet").read_bytes() == b"LEGACY"


def test_sync_ignores_non_ohlc_datasets(tmp_path: Path):
    # A manifest may carry a "reference" dataset (e.g. instruments) alongside
    # "ohlc" -- sync must only materialize the ohlc dataset's files.
    fake, _ = _seeded_release()
    ref_data = b"INSTRUMENTS"
    ref_sha = hashlib.sha256(ref_data).hexdigest()
    manifest = json.loads(fake.assets["manifest.json"])
    manifest["datasets"].append({
        "name": "reference",
        "files": [{
            "name": "instruments.parquet",
            "asset": asset_name("instruments.parquet", ref_sha),
            "sha256": ref_sha, "bytes": len(ref_data), "rows": 1,
        }],
    })
    fake.assets["manifest.json"] = json.dumps(manifest).encode()
    # Deliberately do NOT seed the reference asset -- it must never be requested.

    got = sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                     work_dir=tmp_path / "work")

    assert got is not None
    assert (tmp_path / "ohlc" / "ohlc_2026.parquet").read_bytes() == b"PARQUETDATA"
    assert not (tmp_path / "ohlc" / "instruments.parquet").exists()


def test_sync_failure_on_second_file_leaves_ohlc_dir_untouched(tmp_path: Path):
    # Two ohlc files; the second's manifest sha doesn't match its bytes.
    # ohlc_dir is pre-seeded with an existing file that must survive untouched,
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

    ohlc_dir = tmp_path / "ohlc"
    ohlc_dir.mkdir(parents=True)
    old_bytes = b"OLD-2025-DATA"
    (ohlc_dir / "ohlc_2025.parquet").write_bytes(old_bytes)

    with pytest.raises(UnexpectedFailure):
        sync_store(fake, ohlc_dir=ohlc_dir, meta_dir=tmp_path / "meta",
                   work_dir=tmp_path / "work")

    # Directory completely untouched: old file has old bytes, no new files.
    assert (ohlc_dir / "ohlc_2025.parquet").read_bytes() == old_bytes
    assert sorted(p.name for p in ohlc_dir.iterdir()) == ["ohlc_2025.parquet"]


def test_sync_malformed_manifest_json_fails_closed(tmp_path: Path):
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", b"not json{")
    with pytest.raises(UnexpectedFailure):
        sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                   work_dir=tmp_path / "work")


def test_sync_manifest_missing_required_key_fails_closed(tmp_path: Path):
    # "files" entries missing "sha256" must fail closed as UnexpectedFailure,
    # not escape as a raw KeyError, even once the asset itself downloads fine.
    data = b"SOMEDATA"
    manifest = {"schema_version": 1, "generated_at": "g", "latest_trading_date": "2026-07-03",
                "datasets": [{"name": "ohlc", "files": [{"name": "ohlc_2026.parquet"}]}]}
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed("ohlc_2026.parquet", data)
    with pytest.raises(UnexpectedFailure):
        sync_store(fake, ohlc_dir=tmp_path / "ohlc", meta_dir=tmp_path / "meta",
                   work_dir=tmp_path / "work")
