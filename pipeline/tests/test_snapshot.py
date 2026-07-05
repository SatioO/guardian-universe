import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.errors import UnexpectedFailure
from pipeline.manifest import asset_name
from pipeline.snapshot import create_snapshot, prune_snapshots, tag_for
from tests.fakes import FakeReleaseClient, FakeReleaseRepo, assert_release_consistent


def _seeded_source() -> FakeReleaseClient:
    import hashlib
    data = b"OHLCDATA"
    sha = hashlib.sha256(data).hexdigest()
    manifest = {
        "manifest_version": 2, "generated_at": "g", "latest_trading_date": "2026-07-03",
        "datasets": [{"name": "ohlc", "schema_version": 2, "latest_date": "2026-07-03",
            "baseline": [{"name": "ohlc_2026.parquet",
                          "asset": asset_name("ohlc_2026.parquet", sha),
                          "sha256": sha, "bytes": len(data), "rows": 1}],
            "deltas": []}],
    }
    fake = FakeReleaseClient(exists=True)
    fake.seed("manifest.json", json.dumps(manifest).encode())
    fake.seed(asset_name("ohlc_2026.parquet", sha), data)
    return fake


def test_create_snapshot_copies_manifest_and_assets(tmp_path: Path):
    source = _seeded_source()
    repo = FakeReleaseRepo()
    tag = create_snapshot(
        source, repo.client_for, work_dir=tmp_path, now=datetime(2026, 7, 6, tzinfo=UTC)
    )
    assert tag == "data-snapshot-202607"
    dest = repo.client_for(tag)
    assert_release_consistent(dest)
    assert json.loads(dest.assets["manifest.json"]) == json.loads(source.assets["manifest.json"])


def test_create_snapshot_refuses_to_recreate_same_month(tmp_path: Path):
    source = _seeded_source()
    repo = FakeReleaseRepo()
    now = datetime(2026, 7, 6, tzinfo=UTC)
    create_snapshot(source, repo.client_for, work_dir=tmp_path, now=now)
    with pytest.raises(UnexpectedFailure, match="already exists"):
        create_snapshot(source, repo.client_for, work_dir=tmp_path / "again", now=now)


def test_prune_snapshots_keeps_newest_n():
    repo = FakeReleaseRepo()
    for ym in ["202601", "202602", "202603", "202604", "202605", "202606", "202607"]:
        repo.client_for(f"data-snapshot-{ym}").seed("manifest.json", b"{}")
    repo.client_for("data-latest").seed("manifest.json", b"{}")  # must never be pruned
    deleted = prune_snapshots(repo.client_for, repo.as_list_client(), keep=6)
    assert deleted == ["data-snapshot-202601"]
    remaining = sorted(repo.tags())
    assert "data-latest" in remaining
    assert "data-snapshot-202601" not in remaining
    assert len([t for t in remaining if t.startswith("data-snapshot-")]) == 6


def test_tag_for_formats_year_month():
    assert tag_for(datetime(2026, 3, 6, tzinfo=UTC)) == "data-snapshot-202603"
