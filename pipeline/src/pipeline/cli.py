"""`python -m pipeline {daily,backfill,publish}` — wires real adapters."""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

from pipeline import backfill as backfill_mod
from pipeline import calendar as cal
from pipeline import config, freshness, manifest
from pipeline.daily_update import run_daily
from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.fetch import NseUdiffFetcher
from pipeline.publish import publish_dataset
from pipeline.release import GhReleaseClient
from pipeline.sync import sync_store

Runner = Callable[[list[str]], int]


def _plain_runner(cmd: list[str]) -> int:
    import subprocess
    return subprocess.run(cmd, check=False).returncode


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


def cmd_check_freshness(
    *,
    repo: str,
    tag: str,
    holidays: set[date],
    today: date,
    runner: Runner,
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
        client = GhReleaseClient(repo=config.GITHUB_REPO, tag=config.RELEASE_TAG)
        with tempfile.TemporaryDirectory() as tmp:
            try:
                sync_store(client, ohlc_dir=config.OHLC_DIR, meta_dir=config.META_DIR,
                           work_dir=Path(tmp))
            except (ReleaseError, UnexpectedFailure) as e:
                print(f"sync failed: {e}", file=sys.stderr)
                return 1
        return 0
    if args.cmd == "check-freshness":
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        with tempfile.TemporaryDirectory() as tmp:
            return cmd_check_freshness(
                repo=config.GITHUB_REPO, tag=config.RELEASE_TAG, holidays=holidays,
                today=datetime.now(UTC).date(), runner=_plain_runner,
                work_dir=Path(tmp),
            )
    # publish
    client = GhReleaseClient(repo=config.GITHUB_REPO, tag=config.RELEASE_TAG)
    try:
        publish_dataset(
            ohlc_dir=config.OHLC_DIR, meta_dir=config.META_DIR,
            stage_dir=config.DATA_DIR / "stage", client=client,
            schema_version=config.SCHEMA_VERSION,
            generated_at=datetime.now(UTC).isoformat(),
            now=datetime.now(UTC),
        )
    except (ReleaseError, UnexpectedFailure) as e:
        print(f"publish failed: {e}", file=sys.stderr)
        return 1
    return 0
