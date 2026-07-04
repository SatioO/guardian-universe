"""`python -m pipeline {daily,backfill,publish}` — wires real adapters."""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from pipeline import backfill as backfill_mod
from pipeline import calendar as cal
from pipeline import config, manifest, publish
from pipeline.daily_update import run_daily
from pipeline.fetch import NseUdiffFetcher


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("daily")
    d.add_argument("--date", default=None)
    b = sub.add_parser("backfill")
    b.add_argument("--days", type=int, required=True)
    sub.add_parser("publish")
    return p


def _latest_trading_date(ohlc_dir: Path) -> date:
    latest = date.min
    for p in ohlc_dir.glob("ohlc_*.parquet"):
        col = pd.to_datetime(pd.read_parquet(p, columns=["date"])["date"])
        latest = max(latest, col.max().date())
    return latest


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
    publish.publish_release(data_files, manifest_path, tag=tag, repo=repo, runner=runner)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    holidays = cal.load_holidays(config.META_DIR / "holidays.json")
    fetcher = NseUdiffFetcher()
    if args.cmd == "daily":
        target = date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()
        st = run_daily(target, fetcher=fetcher, holidays=holidays, base=config.OHLC_DIR)
        print(manifest.status_to_dict(st))
        return 0 if st.status in ("success", "skipped_holiday", "skipped_idempotent",
                                  "not_yet") else 1
    if args.cmd == "backfill":
        results = backfill_mod.backfill(
            datetime.now(UTC).date(), args.days,
            fetcher=fetcher, holidays=holidays, base=config.OHLC_DIR,
        )
        return 0 if all(
            r.status in ("success", "skipped_holiday", "skipped_idempotent", "not_yet")
            for r in results
        ) else 1
    # publish
    cmd_publish(
        ohlc_dir=config.OHLC_DIR, meta_dir=config.META_DIR,
        repo=config.GITHUB_REPO, tag=config.RELEASE_TAG,
        runner=publish.subprocess_runner,
        generated_at=datetime.now(UTC).isoformat(),
    )
    return 0
