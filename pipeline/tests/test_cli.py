from datetime import date
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


def test_main_publish_returns_1_on_failure(monkeypatch):
    def _boom(**_kw):
        from pipeline.errors import UnexpectedFailure
        raise UnexpectedFailure("no data")
    monkeypatch.setattr(cli, "publish_dataset", _boom)
    assert cli.main(["publish"]) == 1


def test_main_publish_returns_0_on_success(monkeypatch):
    calls = {}
    monkeypatch.setattr(cli, "publish_dataset", lambda **kw: calls.update(kw))
    assert cli.main(["publish"]) == 0
    assert calls["schema_version"] == config.SCHEMA_VERSION


def test_parser_has_sync():
    args = cli.build_parser().parse_args(["sync"])
    assert args.cmd == "sync"


def test_main_sync_returns_1_on_failure(monkeypatch):
    from pipeline.errors import ReleaseError

    def _boom(*a, **kw):
        raise ReleaseError("network down")

    monkeypatch.setattr(cli, "sync_store", _boom)
    assert cli.main(["sync"]) == 1


def test_main_sync_returns_0_on_success(monkeypatch):
    monkeypatch.setattr(cli, "sync_store", lambda *a, **kw: None)
    assert cli.main(["sync"]) == 0


def test_parser_has_check_freshness():
    assert cli.build_parser().parse_args(["check-freshness"]).cmd == "check-freshness"


def test_cmd_check_freshness_reads_manifest_and_reports_fresh(tmp_path: Path):
    import json
    (tmp_path / "manifest.json").write_text(json.dumps({"latest_trading_date": "2026-07-03"}))
    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(),
        today=date(2026, 7, 6), runner=lambda _cmd: 0, work_dir=tmp_path,
    )
    assert rc == 0  # Fri 2026-07-03 published, today Mon -> fresh


def test_cmd_check_freshness_flags_missing_release(tmp_path: Path):
    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(),
        today=date(2026, 7, 6), runner=lambda _cmd: 1, work_dir=tmp_path,
    )
    assert rc == 1  # download failed -> treated as stale/missing
