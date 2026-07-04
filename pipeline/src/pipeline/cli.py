"""`python -m pipeline {daily,backfill,publish}` — wires real adapters."""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from pipeline import backfill as backfill_mod
from pipeline import calendar as cal
from pipeline import config, freshness, manifest, publish
from pipeline.daily_update import run_daily
from pipeline.errors import UnexpectedFailure
from pipeline.fetch import NseUdiffFetcher


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("daily")
    d.add_argument("--date", default=None)
    b = sub.add_parser("backfill")
    b.add_argument("--days", type=int, required=True)
    sub.add_parser("publish")
    sub.add_parser("sync")
    sub.add_parser("check-freshness")
    return p


def _latest_trading_date(ohlc_dir: Path) -> date:
    latest = date.min
    for p in ohlc_dir.glob("ohlc_*.parquet"):
        col = pd.to_datetime(pd.read_parquet(p, columns=["date"])["date"])
        latest = max(latest, col.max().date())
    return latest


def cmd_sync(*, ohlc_dir: Path, repo: str, tag: str, runner: publish.Runner) -> int:
    # Download the current published parquet(s) so a fresh runner appends TODAY to
    # accumulated history. A missing release (first ever run) is tolerated: the
    # non-zero exit is returned, not raised — daily+publish will then create it.
    ohlc_dir.mkdir(parents=True, exist_ok=True)
    return runner([
        "gh", "release", "download", tag, "--repo", repo,
        "--pattern", "ohlc_*.parquet", "--dir", str(ohlc_dir), "--clobber",
    ])


def cmd_check_freshness(
    *,
    repo: str,
    tag: str,
    holidays: set[date],
    today: date,
    runner: publish.Runner,
    work_dir: Path,
) -> int:
    work_dir.mkdir(parents=True, exist_ok=True)
    rc = runner(["gh", "release", "download", tag, "--repo", repo,
                 "--pattern", "manifest.json", "--dir", str(work_dir), "--clobber"])
    manifest_path = work_dir / "manifest.json"
    if rc != 0 or not manifest_path.exists():
        return 1  # no release / download failed -> stale
    latest = date.fromisoformat(json.loads(manifest_path.read_text())["latest_trading_date"])
    return 1 if freshness.is_stale(latest, today, holidays) else 0


def cmd_publish(
    *,
    ohlc_dir: Path,
    meta_dir: Path,
    repo: str,
    tag: str,
    runner: publish.Runner,
    generated_at: str,
) -> None:
    m = manifest.build_manifest(
        ohlc_dir, schema_version=config.SCHEMA_VERSION,
        latest_trading_date=_latest_trading_date(ohlc_dir), generated_at=generated_at,
    )
    manifest_path = meta_dir / "manifest.json"
    manifest.write_json(m, manifest_path)
    data_files = sorted(ohlc_dir.glob("ohlc_*.parquet"))
    status_path = meta_dir / "last_run_status.json"
    extra = [status_path] if status_path.exists() else []
    publish.publish_release(data_files, manifest_path, tag=tag, repo=repo,
                            runner=runner, extra_files=extra)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "daily":
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        fetcher = NseUdiffFetcher()
        target = date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()
        st = run_daily(target, fetcher=fetcher, holidays=holidays, base=config.OHLC_DIR)
        manifest.write_status(st, config.META_DIR)
        print(manifest.status_to_dict(st))
        return 0 if st.status in ("success", "skipped_holiday", "skipped_idempotent",
                                  "not_yet") else 1
    if args.cmd == "backfill":
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        fetcher = NseUdiffFetcher()
        results = backfill_mod.backfill(
            datetime.now(UTC).date(), args.days,
            fetcher=fetcher, holidays=holidays, base=config.OHLC_DIR,
        )
        return 0 if all(
            r.status in ("success", "skipped_holiday", "skipped_idempotent", "not_yet")
            for r in results
        ) else 1
    if args.cmd == "sync":
        cmd_sync(ohlc_dir=config.OHLC_DIR, repo=config.GITHUB_REPO,
                 tag=config.RELEASE_TAG, runner=publish.subprocess_runner)
        return 0
    if args.cmd == "check-freshness":
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        with tempfile.TemporaryDirectory() as tmp:
            return cmd_check_freshness(
                repo=config.GITHUB_REPO, tag=config.RELEASE_TAG, holidays=holidays,
                today=datetime.now(UTC).date(), runner=publish.subprocess_runner,
                work_dir=Path(tmp),
            )
    # publish
    try:
        cmd_publish(
            ohlc_dir=config.OHLC_DIR, meta_dir=config.META_DIR,
            repo=config.GITHUB_REPO, tag=config.RELEASE_TAG,
            runner=publish.subprocess_runner,
            generated_at=datetime.now(UTC).isoformat(),
        )
    except UnexpectedFailure as e:
        print(f"publish failed: {e}", file=sys.stderr)
        return 1
    return 0
