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
