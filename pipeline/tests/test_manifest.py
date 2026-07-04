import json
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config, manifest


def _write_parquet(p: Path, n: int) -> None:
    df = pd.DataFrame({c: [0] * n for c in config.CANON_COLUMNS})
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, compression="zstd", index=False)


def test_file_digest_is_stable(tmp_path: Path):
    p = tmp_path / "a.parquet"
    _write_parquet(p, 1)
    sha1, size1 = manifest.file_digest(p)
    sha2, size2 = manifest.file_digest(p)
    assert sha1 == sha2 and len(sha1) == 64 and size1 == size2 > 0


def test_build_manifest_lists_ohlc_files_with_digests(tmp_path: Path):
    _write_parquet(tmp_path / "ohlc_2025.parquet", 2)
    _write_parquet(tmp_path / "ohlc_2026.parquet", 3)
    m = manifest.build_manifest(
        tmp_path, schema_version=1, latest_trading_date=date(2026, 7, 3),
        generated_at="2026-07-03T12:00:00Z",
    )
    assert m["schema_version"] == 1
    assert m["latest_trading_date"] == "2026-07-03"
    assert m["generated_at"] == "2026-07-03T12:00:00Z"
    ds = m["datasets"][0]
    assert ds["name"] == "ohlc"
    names = [f["name"] for f in ds["files"]]
    assert names == ["ohlc_2025.parquet", "ohlc_2026.parquet"]  # sorted
    assert all(len(f["sha256"]) == 64 and f["bytes"] > 0 for f in ds["files"])


def test_status_to_dict_serializes_run_status():
    from pipeline.daily_update import RunStatus
    d = manifest.status_to_dict(RunStatus("success", date(2026, 7, 3), symbol_count=1900,
                                          quarantined_count=2, source="nse-udiff"))
    assert d == {
        "status": "success", "date": "2026-07-03", "symbol_count": 1900,
        "quarantined_count": 2, "source": "nse-udiff", "message": "",
    }


def test_write_json_roundtrips(tmp_path: Path):
    p = tmp_path / "m.json"
    manifest.write_json({"a": 1}, p)
    assert json.loads(p.read_text()) == {"a": 1}


def test_write_status_writes_last_run_status(tmp_path: Path):
    from pipeline.daily_update import RunStatus
    p = manifest.write_status(
        RunStatus("success", date(2026, 7, 3), symbol_count=2406, source="nse-udiff"),
        tmp_path,
    )
    assert p == tmp_path / "last_run_status.json"
    import json
    assert json.loads(p.read_text())["symbol_count"] == 2406
