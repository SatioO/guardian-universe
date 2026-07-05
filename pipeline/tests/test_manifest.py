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


def _write_year(dirpath, prefix, year, days):
    import pandas as pd

    from pipeline import config
    dirpath.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({c: ["x"] * len(days) for c in config.CANON_COLUMNS})
    df["date"] = pd.to_datetime(days)
    df["instrument_key"] = [f"K{i}" for i in range(len(days))]
    df.to_parquet(dirpath / f"{prefix}_{year}.parquet", compression="zstd", index=False)


def test_build_manifest_v2_shape(tmp_path):
    import dataclasses
    from datetime import date

    from pipeline import datasets, store
    from pipeline.manifest import asset_name, build_manifest

    spec = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path)
    _write_year(tmp_path, "ohlc", 2026, ["2026-07-02", "2026-07-03"])
    import pandas as pd

    from pipeline import config
    day = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
    day["date"] = pd.to_datetime(["2026-07-03"])
    store.write_delta(day, tmp_path, date(2026, 7, 3))

    m = build_manifest([spec], latest_trading_date=date(2026, 7, 3), generated_at="g")
    assert m["manifest_version"] == 2 and m["min_client_version"] == "0.1.0"
    assert m["latest_trading_date"] == "2026-07-03"
    (ds,) = m["datasets"]
    assert ds["name"] == "ohlc" and ds["schema_version"] == 2
    assert ds["latest_date"] == "2026-07-03"
    (b,) = ds["baseline"]
    assert b["name"] == "ohlc_2026.parquet" and b["rows"] == 2
    assert b["asset"] == asset_name("ohlc_2026.parquet", b["sha256"])
    (d,) = ds["deltas"]
    assert d["date"] == "2026-07-03" and d["asset"].startswith("delta_ohlc_2026-07-03.")


def test_build_manifest_omits_empty_dataset(tmp_path):
    import dataclasses
    from datetime import date

    from pipeline import datasets
    from pipeline.manifest import build_manifest

    empty = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "nothing")
    m = build_manifest([empty], latest_trading_date=date(2026, 7, 3), generated_at="g")
    assert m["datasets"] == []


def test_dataset_files_reads_v1_and_v2():
    from pipeline.manifest import dataset_files
    assert dataset_files({"files": [1]}) == [1]      # v1 (G0 live manifest)
    assert dataset_files({"baseline": [2]}) == [2]   # v2
    assert dataset_files({}) == []


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


def test_write_status_accepts_custom_filename(tmp_path: Path):
    from pipeline.daily_update import RunStatus
    p = manifest.write_status(
        RunStatus("success", date(2026, 7, 3), symbol_count=12, source="derived"),
        tmp_path, filename="last_run_status_reference.json",
    )
    assert p == tmp_path / "last_run_status_reference.json"
    import json
    assert json.loads(p.read_text())["symbol_count"] == 12


def test_asset_name_inserts_sha8_before_extension():
    from pipeline.manifest import asset_name
    sha = "a1b2c3d4" + "0" * 56
    assert asset_name("ohlc_2026.parquet", sha) == "ohlc_2026.a1b2c3d4.parquet"


