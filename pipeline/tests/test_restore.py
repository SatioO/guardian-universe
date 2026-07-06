import hashlib
import json
from pathlib import Path

import pytest

from pipeline.errors import UnexpectedFailure
from pipeline.manifest import asset_name
from pipeline.restore import restore_from_tag
from tests.fakes import FakeReleaseClient


def _seeded_snapshot() -> FakeReleaseClient:
    data = b"OHLCDATA"
    sha = hashlib.sha256(data).hexdigest()
    manifest = {
        "manifest_version": 2, "generated_at": "g", "latest_trading_date": "2026-07-03",
        "datasets": [{"name": "ohlc", "schema_version": 2, "latest_date": "2026-07-03",
            "baseline": [{"name": "ohlc_2026.parquet",
                          "asset": asset_name("ohlc_2026.parquet", sha),
                          "sha256": sha, "bytes": len(data), "rows": 1}],
            "deltas": [{"date": "2026-07-03", "name": "ohlc_2026-07-03.parquet",
                       "asset": "delta_ohlc_2026-07-03." + sha[:8] + ".parquet",
                       "sha256": "irrelevant", "bytes": 1}]}],
    }
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed(asset_name("ohlc_2026.parquet", sha), data)
    return fake


def test_restore_materializes_baseline_under_target_root(tmp_path: Path):
    client = _seeded_snapshot()
    result = restore_from_tag(client, target_root=tmp_path / "restored", work_dir=tmp_path / "work")
    restored_file = tmp_path / "restored" / "ohlc" / "ohlc_2026.parquet"
    assert restored_file.read_bytes() == b"OHLCDATA"
    assert result["datasets"][0]["name"] == "ohlc"


def test_restore_does_not_download_deltas(tmp_path: Path):
    client = _seeded_snapshot()
    restore_from_tag(client, target_root=tmp_path / "restored", work_dir=tmp_path / "work")
    assert not (tmp_path / "restored" / "ohlc" / "ohlc_2026-07-03.parquet").exists()


def test_restore_checksum_mismatch_is_fatal_and_leaves_target_empty(tmp_path: Path):
    client = _seeded_snapshot()
    for name in list(client.assets):
        if name != "manifest.json":
            client.assets[name] = b"CORRUPTED"
    target = tmp_path / "restored"
    with pytest.raises(UnexpectedFailure, match="checksum"):
        restore_from_tag(client, target_root=target, work_dir=tmp_path / "work")
    assert not target.exists()  # two-phase: verify-all-before-materialize-any
