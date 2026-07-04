from pathlib import Path

import pandas as pd

from pipeline import cli, config


def _write_parquet(p: Path, n: int) -> None:
    df = pd.DataFrame({c: [0] * n for c in config.CANON_COLUMNS})
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, compression="zstd", index=False)


def test_parser_reads_backfill_days():
    args = cli.build_parser().parse_args(["backfill", "--days", "42"])
    assert args.cmd == "backfill" and args.days == 42


def test_parser_reads_daily_date():
    args = cli.build_parser().parse_args(["daily", "--date", "2026-07-03"])
    assert args.cmd == "daily" and args.date == "2026-07-03"


def test_cmd_publish_writes_manifest_and_uploads(tmp_path: Path):
    ohlc = tmp_path / "ohlc"
    meta = tmp_path / "meta"
    _write_parquet(ohlc / "ohlc_2026.parquet", 3)
    calls: list[list[str]] = []
    cli.cmd_publish(
        ohlc_dir=ohlc, meta_dir=meta, repo="o/r", tag="data-latest",
        runner=lambda cmd: (calls.append(cmd), 0)[1],
        generated_at="2026-07-03T00:00:00Z",
    )
    assert (meta / "manifest.json").exists()
    uploads = [c for c in calls if "upload" in c]
    assert any("ohlc_2026.parquet" in " ".join(c) for c in uploads)
    assert str(meta / "manifest.json") in uploads[-1]  # manifest last
