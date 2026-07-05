"""`python -m pipeline {daily,backfill,publish}` — wires real adapters."""
from __future__ import annotations

import argparse
import functools
import json
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

from pipeline import backfill as backfill_mod
from pipeline import builders, config, datasets, freshness, manifest
from pipeline import calendar as cal
from pipeline.daily_update import RunStatus, run_daily
from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.publish import publish_dataset
from pipeline.release import GhReleaseClient
from pipeline.sync import sync_store

Runner = Callable[[list[str]], int]

# cli is the allowed name edge (per the derived-dataset mechanism's design):
# builders.py stays name-free, so the source spec a builder reads from (here,
# the primary/equities spec) is resolved by position (DATASET_ORDER[0]) and
# bound in as a keyword-only argument via functools.partial. The bound
# partial still matches BUILDERS' `Callable[[DatasetSpec, date], RunStatus]`
# signature -- source_spec is filled in, leaving exactly the (spec, target)
# two positional params _run_builder calls with.
builders.BUILDERS["reference"] = functools.partial(
    builders.build_reference, source_spec=datasets.DATASETS[datasets.DATASET_ORDER[0]]
)


def _plain_runner(cmd: list[str]) -> int:
    import subprocess
    return subprocess.run(cmd, check=False).returncode


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("daily")
    d.add_argument("--date", default=None)
    d.add_argument("--dataset", choices=[*datasets.DATASETS, "all"], default="all")
    b = sub.add_parser("backfill")
    b.add_argument("--days", type=int, required=True)
    b.add_argument("--dataset", choices=[*datasets.DATASETS, "all"], default="all")
    sub.add_parser("publish")
    sub.add_parser("sync")
    sub.add_parser("check-freshness")
    return p


def _run_builder(spec: datasets.DatasetSpec, target: date) -> RunStatus:
    """Run a derived spec's builder, guarded so a builder bug never crashes
    the CLI: any exception, or a missing BUILDERS entry, maps to a failed
    RunStatus instead of propagating."""
    build = builders.BUILDERS.get(spec.key)
    if build is None:
        return RunStatus("failed", target, message=f"no builder registered for '{spec.key}'")
    try:
        return build(spec, target)
    except Exception as e:  # boundary guard: builders must never crash the CLI
        return RunStatus("failed", target, message=f"builder error for '{spec.key}': {e}")


def cmd_check_freshness(
    *,
    repo: str,
    tag: str,
    holidays: set[date],
    today: date,
    runner: Runner,
    work_dir: Path,
    special_sessions: set[date] | None = None,
) -> int:
    work_dir.mkdir(parents=True, exist_ok=True)
    rc = runner(["gh", "release", "download", tag, "--repo", repo,
                 "--pattern", "manifest.json", "--dir", str(work_dir), "--clobber"])
    manifest_path = work_dir / "manifest.json"
    if rc != 0 or not manifest_path.exists():
        return 1  # no release / download failed -> stale
    latest = date.fromisoformat(json.loads(manifest_path.read_text())["latest_trading_date"])
    return 1 if freshness.is_stale(latest, today, holidays, special_sessions) else 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ok = ("success", "skipped_holiday", "skipped_idempotent", "not_yet")
    if args.cmd == "daily":
        if args.dataset != "all" and datasets.DATASETS[args.dataset].derived:
            print("derived datasets build automatically after a successful "
                  "`--dataset all` run", file=sys.stderr)
            return 2
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        special = cal.load_special_sessions(config.META_DIR / "special_sessions.json")
        target = date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()
        keys = datasets.DATASET_ORDER if args.dataset == "all" else [args.dataset]
        primary_key = datasets.DATASET_ORDER[0]

        # Phase 1: FETCHED specs (never touch derived specs' normalizer/make_fetcher).
        statuses: dict[str, RunStatus] = {}
        for key in keys:
            spec = datasets.DATASETS[key]
            if spec.derived:
                continue
            st = run_daily(spec, target, fetcher=spec.make_fetcher(), holidays=holidays,
                           special_sessions=special)
            statuses[key] = st
            print(manifest.status_to_dict(st))

        # Phase 2: DERIVED specs -- only for a full `all` run, and only when
        # the primary fetched status is healthy.
        if args.dataset == "all" and statuses.get(primary_key) is not None \
                and statuses[primary_key].status in ok:
            for key in datasets.DATASET_ORDER:
                spec = datasets.DATASETS[key]
                if not spec.derived:
                    continue
                st = _run_builder(spec, target)
                statuses[key] = st
                print(manifest.status_to_dict(st))

        for key, st in statuses.items():
            if key == primary_key:
                manifest.write_status(st, config.META_DIR)  # drives monitor/publish gate
            else:
                manifest.write_status(st, config.META_DIR, filename=f"last_run_status_{key}.json")

        if primary_key in statuses:
            return 0 if statuses[primary_key].status in ok else 1
        return 0 if all(s.status in ok for s in statuses.values()) else 1
    if args.cmd == "backfill":
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        special = cal.load_special_sessions(config.META_DIR / "special_sessions.json")
        keys = datasets.DATASET_ORDER if args.dataset == "all" else [args.dataset]
        all_results = []
        for key in keys:
            spec = datasets.DATASETS[key]
            if spec.derived:
                continue  # derived datasets are never fetched/backfilled
            all_results.extend(backfill_mod.backfill(
                spec, datetime.now(UTC).date(), args.days,
                fetcher=spec.make_fetcher(), holidays=holidays, special_sessions=special,
            ))
        return 0 if all(r.status in ok for r in all_results) else 1
    if args.cmd == "sync":
        client = GhReleaseClient(repo=config.GITHUB_REPO, tag=config.RELEASE_TAG)
        with tempfile.TemporaryDirectory() as tmp:
            try:
                sync_store(client, meta_dir=config.META_DIR, work_dir=Path(tmp))
            except (ReleaseError, UnexpectedFailure) as e:
                print(f"sync failed: {e}", file=sys.stderr)
                return 1
        return 0
    if args.cmd == "check-freshness":
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        special = cal.load_special_sessions(config.META_DIR / "special_sessions.json")
        with tempfile.TemporaryDirectory() as tmp:
            return cmd_check_freshness(
                repo=config.GITHUB_REPO, tag=config.RELEASE_TAG, holidays=holidays,
                today=datetime.now(UTC).date(), runner=_plain_runner,
                work_dir=Path(tmp), special_sessions=special,
            )
    # publish
    client = GhReleaseClient(repo=config.GITHUB_REPO, tag=config.RELEASE_TAG)
    try:
        publish_dataset(
            specs=datasets.all_specs(), meta_dir=config.META_DIR,
            stage_dir=config.DATA_DIR / "stage", client=client,
            generated_at=datetime.now(UTC).isoformat(),
            now=datetime.now(UTC),
        )
    except (ReleaseError, UnexpectedFailure) as e:
        print(f"publish failed: {e}", file=sys.stderr)
        return 1
    return 0
