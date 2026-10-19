"""`python -m pipeline {daily,backfill,publish}` — wires real adapters."""

from __future__ import annotations

import argparse
import functools
import json
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from pipeline import backfill as backfill_mod

# `rebuild.py` stays fully broker-neutral: importing `pipeline.sources` here
# triggers every registered RebuildSource's import-time self-registration
# side effect (see sources/__init__.py, the broker-source registration
# aggregator -- broker names live there, never here). Never construct or call
# a concrete RebuildSource implementation directly below -- always go through
# rebuild.resolve()/rebuild.REBUILDERS.
from pipeline import (
    builders,
    config,
    datasets,
    freshness,
    manifest,
    rebuild,
    sources,  # noqa: F401  # broker-source registration side-effects
    store,
)
from pipeline import calendar as cal
from pipeline.bootstrap import bootstrap_reference
from pipeline.corporate_actions_history import (
    ApiClient,
    backfill_corporate_actions_history,
    build_corporate_actions_history,
)
from pipeline.daily_update import RunStatus, run_daily
from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.fetch import FetchResult
from pipeline.publish import publish_dataset
from pipeline.release import GhReleaseClient, ReleaseClient
from pipeline.restore import restore_from_tag
from pipeline.snapshot import create_snapshot, prune_snapshots
from pipeline.sources.classification_baseline import BaselineRun, run_approved_baseline
from pipeline.sources.nse_universe import RegistryStatus, registry_needs_baseline_migration
from pipeline.sources.screener_collector import CollectionDeferred, ScreenerClassificationCollector
from pipeline.special_pre_open import build_special_pre_open
from pipeline.sync import sync_store

Runner = Callable[[list[str]], int]

_NSE_EQUITY_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
_CLASSIFICATION_STATUS = "classification_collection_status.json"


@dataclass(frozen=True)
class ClassificationCollectionReport:
    mode: str
    batches: int
    snapshot_valid: bool
    selected: int
    attempted: int
    classified: int
    pending: int
    baseline_pending: int
    retry_pending: int
    audit_pending: int
    deferred_reason: str | None
    publication: RunStatus

    def as_dict(self) -> dict[str, Any]:
        active = self.classified + self.pending
        return {
            "mode": self.mode,
            "batches": self.batches,
            "snapshot_valid": self.snapshot_valid,
            "selected": self.selected,
            "attempted": self.attempted,
            "classified": self.classified,
            "pending": self.pending,
            "baseline_pending": self.baseline_pending,
            "retry_pending": self.retry_pending,
            "audit_pending": self.audit_pending,
            "audit_complete": self.audit_pending == 0,
            "coverage_pct": round(100 * self.classified / active, 2) if active else 0.0,
            "deferred_reason": self.deferred_reason,
            "publication": manifest.status_to_dict(self.publication),
        }


# cli is the allowed name edge (per the derived-dataset mechanism's design):
# builders.py stays name-free, so the source spec a builder reads from (here,
# the primary/equities spec) is resolved by position (DATASET_ORDER[0]) and
# bound in as a keyword-only argument via functools.partial. The bound
# partial still matches BUILDERS' `Callable[[DatasetSpec, date], RunStatus]`
# signature -- source_spec is filled in, leaving exactly the (spec, target)
# two positional params _run_builder calls with.
#
# WARNING: this binds source_spec to the REAL registry spec at *import time*
# (whatever datasets.DATASETS[datasets.DATASET_ORDER[0]] resolves to when this
# module is first imported) -- not at call time. Tests that want a builder
# run against tmp dirs must monkeypatch `cli.builders.BUILDERS` directly
# (replace the entry with a fresh partial bound to a tmp-scoped spec);
# monkeypatching `datasets.DATASETS` alone will NOT redirect the source_spec
# already captured here. See the matching warning in builders.py's docstring.
builders.BUILDERS["reference"] = functools.partial(
    builders.build_reference, source_spec=datasets.DATASETS[datasets.DATASET_ORDER[0]]
)
# sector_industry fetches its own external CSV (no store `source_spec` to bind);
# registered directly. Its keyword-only fetch/TTL/floor args carry defaults, so
# it still satisfies BUILDERS' `Callable[[DatasetSpec, date], RunStatus]`.
builders.BUILDERS["sector_industry"] = builders.build_sector_industry
# index_constituents likewise fetches its own external CSVs (one per index);
# registered bare, its keyword-only fetch/calendar/floor args carry defaults.
builders.BUILDERS["index_constituents"] = builders.build_index_constituents
# cas_eligible fetches NSE's F&O market-lot file itself and joins ISINs from the
# `reference` symbol master, bound here as `universe_spec` (this module is the
# name edge). DATASET_ORDER runs `reference` first in Phase 2, so the master it
# reads is the one rebuilt this run. Same import-time binding WARNING as above.
builders.BUILDERS["cas_eligible"] = functools.partial(
    builders.build_cas_eligible, universe_spec=datasets.DATASETS["reference"]
)
# fno_freeze_limits fetches NSE's daily F&O contract file itself; registered
# bare, its keyword-only fetch/calendar/floor args carry defaults.
builders.BUILDERS["fno_freeze_limits"] = builders.build_fno_freeze_limits
# bse_fno_freeze_limits fetches BSE's rolling contract master itself; registered
# bare, its keyword-only fetch/calendar/floor args carry defaults.
builders.BUILDERS["bse_fno_freeze_limits"] = builders.build_bse_fno_freeze_limits
# market_calendar reads the curated meta files (config.META_DIR); registered bare.
builders.BUILDERS["market_calendar"] = builders.build_market_calendar
# corporate_actions fetches NSE's PR bundle itself; registered bare, its
# keyword-only fetch/calendar/seed args carry defaults.
builders.BUILDERS["corporate_actions"] = builders.build_corporate_actions
# corporate_actions_history adds the PR bundle's book-closure rows and resolves
# every row's ISIN against the `reference` instrument list, bound here as
# `universe_spec` (DATASET_ORDER runs `reference` first). Same import-time
# binding WARNING as above.
# special_pre_open fetches NSE's security master itself; registered bare.
builders.BUILDERS["special_pre_open"] = build_special_pre_open
builders.BUILDERS["corporate_actions_history"] = functools.partial(
    build_corporate_actions_history, universe_spec=datasets.DATASETS["reference"]
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
    classification = sub.add_parser("classification-collect")
    classification.add_argument(
        "--mode", choices=("daily", "baseline", "quarterly"), default="daily"
    )
    classification.add_argument("--batch-size", type=int, default=25)
    classification.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="bounded batches in this run (quarterly defaults to 100)",
    )
    classification.add_argument("--date", default=None)
    pub = sub.add_parser("publish")
    # Operator opt-in for a DELIBERATE data-reducing correction (e.g. rebuilding
    # a dataset after fixing a bug that had inflated it). Off by default so the
    # nightly cron always keeps the full shrink-guard; downgrades only the
    # per-file row-shrink check to a warning (a missing file/dataset THIS
    # runner owns and a latest_trading_date-regression stay hard errors). See
    # check_no_shrink.
    pub.add_argument("--allow-shrink", action="store_true")
    sub.add_parser("sync")
    sub.add_parser("check-freshness")
    sub.add_parser("snapshot")
    rs = sub.add_parser("restore-from-snapshot")
    rs.add_argument("--tag", required=True)
    # --target defaults to None here, meaning "resolve to
    # config.DATA_DIR / '_restore_drill' / tag" at dispatch time (below) --
    # NEVER the live data/ tree unless the operator explicitly passes
    # --target. This scratch-by-default resolution is the safety rail that
    # makes a drill safe to run against any real tag without risking a
    # production clobber.
    rs.add_argument("--target", default=None)
    r = sub.add_parser("rebuild-day")
    r.add_argument("--date", required=True)
    # choices derived from the registry (populated by this module's broker
    # self-registration imports above) -- never a hardcoded broker-name list.
    r.add_argument("--via", choices=[*rebuild.REBUILDERS], default=None)
    # One-off: rebuild the reference symbol master from the price files left on
    # a release (see bootstrap.py and RUNBOOK.md 'Reference bootstrap').
    bs = sub.add_parser("bootstrap-reference")
    bs.add_argument("--tag", default=config.RELEASE_TAG)
    bs.add_argument(
        "--ignore-prior",
        action="store_true",
        help="replace the synced instrument list instead of merging it in",
    )
    # Residential only (NSE's API blocks datacenter IPs): fill or top up the
    # corporate_actions_history dataset from NSE's corporate-actions API. See
    # RUNBOOK.md 'Corporate actions history'.
    ca = sub.add_parser("backfill-corporate-actions")
    ca.add_argument("--from", dest="start", default=config.CA_HISTORY_START.isoformat())
    ca.add_argument("--to", dest="end", default=None, help="last ex-date (default: today)")
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


def _fetch_nse_equity_list(session: requests.Session) -> bytes:
    response = session.get(_NSE_EQUITY_LIST_URL, timeout=30)
    response.raise_for_status()
    return response.content


def cmd_classification_collect(
    *,
    mode: str,
    snapshot_csv: bytes,
    collector: ScreenerClassificationCollector,
    registry_path: Path,
    target: date,
    batch_size: int,
    max_batches: int = 1,
    build: Callable[[datasets.DatasetSpec, date], RunStatus] = _run_builder,
) -> ClassificationCollectionReport:
    """Run one persisted classification collection batch and guarded build."""
    if max_batches < 1:
        raise ValueError("max_batches must be positive")
    if mode == "baseline" and max_batches != 1:
        raise ValueError("baseline mode permits exactly one batch per dispatch")
    if mode == "daily" and (
        not registry_path.exists() or registry_needs_baseline_migration(registry_path)
    ):
        raise ValueError("classification baseline is not initialized; run --mode baseline first")
    batches: list[BaselineRun] = []
    for batch_index in range(max_batches):
        baseline = run_approved_baseline(
            snapshot_csv,
            registry_path,
            collector,
            today=target,
            batch_size=batch_size,
            manual_batch=mode == "baseline",
            full_audit=mode == "quarterly",
        )
        batches.append(baseline)
        if (
            not baseline.result.valid_snapshot
            or baseline.deferred_reason is not None
            or not baseline.result.candidates
        ):
            break
        if batch_index + 1 < max_batches:
            collector.begin_next_batch()

    latest = batches[-1]
    entries = latest.result.records.values()
    active = [entry for entry in entries if entry.status is not RegistryStatus.INACTIVE]
    classified = sum(entry.last_known_good is not None for entry in active)
    pending_entries = [entry for entry in active if entry.last_known_good is None]
    quarter_start = _quarter_start(target)
    publication = (
        build(datasets.SECTOR_INDUSTRY, target)
        if latest.result.valid_snapshot
        else RunStatus("failed", target, message="invalid NSE equity snapshot")
    )
    return ClassificationCollectionReport(
        mode=mode,
        batches=len(batches),
        snapshot_valid=latest.result.valid_snapshot,
        selected=sum(len(batch.result.candidates) for batch in batches),
        attempted=sum(len(batch.result.attempted) for batch in batches),
        classified=classified,
        pending=len(pending_entries),
        baseline_pending=sum(entry.baseline_pending for entry in pending_entries),
        retry_pending=sum(not entry.baseline_pending for entry in pending_entries),
        audit_pending=sum(
            entry.last_audit_on is None or entry.last_audit_on < quarter_start for entry in active
        ),
        deferred_reason=latest.deferred_reason,
        publication=publication,
    )


def _quarter_start(target: date) -> date:
    return date(target.year, 3 * ((target.month - 1) // 3) + 1, 1)


class _OneShotFetcher:
    """Wraps an already-built frame as a `Fetcher` so `rebuild-day` can route
    through the NORMAL `run_daily` path (every gate applies: wrong-date guard,
    rowcount, quarantine, schema) instead of a bespoke store-write path.

    `fetch_raw` ignores its `d` argument -- the frame was already built by the
    caller for the requested date before this wrapper was constructed."""

    def __init__(self, frame: pd.DataFrame, source: str) -> None:
        self._frame = frame
        self._source = source

    def fetch_raw(self, d: date) -> FetchResult:  # noqa: ARG002 - frame is precomputed
        return FetchResult(self._frame, self._source)


def _load_rebuild_universe(reference_dir: Path) -> dict[str, tuple[str, str]]:
    """symbol -> (isin, series) for whichever RebuildSource `rebuild-day`
    resolves to, read from the reference/instruments store: active rows
    only, index rows excluded (an index has no tradable broker instrument
    token). Reference is SCD2 (multiple rows per symbol over time) --
    dedupe to the latest by `last_seen`, same as `datasets._load_isin_map`."""
    df = pd.read_parquet(reference_dir / "instruments_all.parquet")
    df = df[(df["status"] == "active") & (df["series"] != "INDEX")]
    df = df.sort_values("last_seen").drop_duplicates(subset="symbol", keep="last")
    return {str(row.symbol): (str(row.isin), str(row.series)) for row in df.itertuples()}


def cmd_rebuild_day(
    target: date,
    *,
    holidays: set[date],
    special_sessions: set[date] | None = None,
    via: str | None = None,
) -> int:
    """Manual last-resort recovery: rebuild one day's equities OHLCV directly
    from a registered broker source (`--via <id>`, or the first available one
    when omitted) when both NSE sources are down or the hole predates either
    NSE archive. NEVER invoked from cron or the automatic fallback chain --
    see rebuild.py's module docstring and RUNBOOK.md.

    Broker-agnostic by construction: this function never mentions a broker by
    name -- `rebuild.resolve(via)` is the only place that decides which
    registered `RebuildSource` serves the request, and that source's own
    `available()` is what actually gates on credentials (see rebuild.py).

    Deliberately does NOT run Phase 2 (derived builders, e.g. `reference`):
    those are built from the regular daily cadence's full multi-dataset run,
    and a single manual equities rebuild is not that.

    Since 2026-10-13 the equities store is run-local and never published, so
    a rebuilt day only reaches anything published if `daily` runs in the same
    workspace afterwards (its `reference` merge reads the store)."""
    try:
        source = rebuild.resolve(via)
    except ValueError as e:
        print(f"rebuild-day: {e}", file=sys.stderr)
        return 2

    reference_path = config.REFERENCE_DIR / "instruments_all.parquet"
    if not reference_path.exists():
        print(
            f"rebuild-day requires the reference dataset at {reference_path} "
            "(the symbol universe map) -- run a normal `daily` cycle at "
            "least once first",
            file=sys.stderr,
        )
        return 2

    universe = _load_rebuild_universe(config.REFERENCE_DIR)
    frame = source.day_frame(target, universe)

    spec = datasets.DATASETS[datasets.DATASET_ORDER[0]]
    # Provenance is derived from the resolved source's own id, never a
    # hardcoded broker name -- a second registered broker gets
    # "<its-id>-rebuild" for free.
    fetcher = _OneShotFetcher(frame, f"{source.id}-rebuild")
    status = run_daily(
        spec, target, fetcher=fetcher, holidays=holidays, special_sessions=special_sessions
    )
    print(manifest.status_to_dict(status))
    # RebuildSource itself only guarantees id/available/day_frame; a
    # `failures` list is a (currently universal across every registered
    # source) convention, not a Protocol requirement, so read it defensively.
    failures = getattr(source, "failures", [])
    print(f"per-symbol failures: {len(failures)}", file=sys.stderr)
    for f in failures:
        print(f"  - {f}", file=sys.stderr)

    ok = ("success", "skipped_holiday", "skipped_idempotent", "not_yet")
    return 0 if status.status in ok else 1


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
    """Freshness monitor: two independent checks, run in order.

    1. STALENESS: is the manifest's `latest_trading_date` current as of
       `today`? Since 2026-10-13 that date is the `reference` symbol master's
       latest session (the publish's primary, see datasets.published_specs),
       which is also the session the desktop app judges freshness by.
    2. CALENDAR HYGIENE (G2 task 8): is `market_calendar.json` (the `holidays`
       set passed in by the caller) due for its yearly refresh? On/after
       December 1st, a `holidays` set with no entry dated in NEXT year is
       flagged -- see `freshness.holidays_need_refresh` for the exact rule.
       This is about the trading-CALENDAR input going stale, not about
       published data falling behind (see the RUNBOOK's yearly procedure and
       `.github/workflows/holidays-refresh.yml`, which nags via an issue on
       the same cadence, so this is a second, independent tripwire).

    The trailing-window continuity check over the fetched price datasets is
    gone with them: they are no longer published, so the release has no
    per-session history left to find holes in."""
    work_dir.mkdir(parents=True, exist_ok=True)
    rc = runner(
        [
            "gh",
            "release",
            "download",
            tag,
            "--repo",
            repo,
            "--pattern",
            "manifest.json",
            "--dir",
            str(work_dir),
            "--clobber",
        ]
    )
    manifest_path = work_dir / "manifest.json"
    if rc != 0 or not manifest_path.exists():
        return 1  # no release / download failed -> stale
    manifest_obj = json.loads(manifest_path.read_text())
    latest = date.fromisoformat(manifest_obj["latest_trading_date"])
    if freshness.is_stale(latest, today, holidays, special_sessions):
        return 1

    if freshness.holidays_need_refresh(holidays, today):
        print(
            f"check-freshness: market_calendar.json needs its yearly refresh -- "
            f"today ({today.isoformat()}) is on/after December 1st and no "
            f"holiday dated in {today.year + 1} is present yet; refresh "
            "market_calendar.json (closures and special sessions of every venue) from "
            "the NSE trading-holiday circular -- see RUNBOOK.md 'Yearly'"
        )
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ok = ("success", "skipped_holiday", "skipped_idempotent", "not_yet")
    if args.cmd == "classification-collect":
        target = date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()
        session = requests.Session()
        session.headers.update({"User-Agent": "guardian-universe classification registry/1.0"})
        try:
            snapshot_csv = _fetch_nse_equity_list(session)
            collector = ScreenerClassificationCollector(session)
            report = cmd_classification_collect(
                mode=args.mode,
                snapshot_csv=snapshot_csv,
                collector=collector,
                registry_path=config.SECTOR_DIR / "classification_registry_all.parquet",
                target=target,
                batch_size=args.batch_size,
                max_batches=(
                    args.max_batches
                    if args.max_batches is not None
                    else (100 if args.mode == "quarterly" else 1)
                ),
            )
        except (requests.RequestException, CollectionDeferred, ValueError) as error:
            print(f"classification-collect failed: {error}", file=sys.stderr)
            return 1
        report_payload = report.as_dict()
        manifest.write_json(report_payload, config.META_DIR / _CLASSIFICATION_STATUS)
        print(json.dumps(report_payload, sort_keys=True))
        return 0 if report.snapshot_valid else 1
    if args.cmd == "daily":
        if args.dataset != "all" and datasets.DATASETS[args.dataset].derived:
            print(
                "derived datasets build automatically after a successful `--dataset all` run",
                file=sys.stderr,
            )
            return 2
        holidays, special = cal.load_trading_calendar(config.META_DIR)
        target = date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()
        keys = datasets.DATASET_ORDER if args.dataset == "all" else [args.dataset]
        primary_key = datasets.DATASET_ORDER[0]

        # G2 final-review fix (C1): a stale window_failures.json from a PRIOR
        # run must never survive into this one -- a clean run has to actively
        # clear yesterday's marker, not just skip re-writing it, otherwise a
        # since-repaired hole would keep alerting forever via a leftover file
        # nothing else ever cleans up. Removed unconditionally, before the
        # fetch loop even starts, regardless of what this run's own outcome
        # turns out to be.
        _window_failures_path = config.META_DIR / "window_failures.json"
        _window_failures_path.unlink(missing_ok=True)

        # Phase 1: FETCHED specs (never touch derived specs' normalizer/make_fetcher).
        #
        # G2 Task 4 (catch-up loop): each fetched spec is run not just for
        # `target` but for the trailing CATCHUP_WINDOW_DAYS-day trading
        # window ending at `target` (ascending order) -- a day missed by an
        # earlier failed/skipped run (both crons down, a late bhavcopy, ...)
        # self-heals here instead of becoming a permanent hole. A day
        # already in the store costs one cheap `has_day` idempotent-skip
        # read via run_daily's existing gate.
        #
        # ONE fetcher per spec, reused across every day in that spec's
        # window (not reconstructed per day) -- session/connection reuse,
        # and consistent with backfill's existing per-spec fetcher lifetime.
        #
        # The spec's status recorded into `statuses` (and therefore written
        # to its status file, and what Phase 2/the exit-code logic keys off)
        # is always the TARGET day's status -- catch-up days are a side
        # channel. A catch-up day's own `failed` (primary OR secondary spec)
        # must not be silently swallowed just because the target day
        # succeeded: it's printed explicitly AND collected here for
        # persisting to window_failures.json below.
        #
        # G2 final-review fix (C1): window failures are DELIBERATELY never
        # folded into the overall exit code anymore -- see the final return
        # below. A permanent past-day archive hole used to force exit 1 even
        # when the target day was perfectly healthy, and data-daily.yml's
        # un-guarded "Ingest" step turned that into a skipped "Decide"/
        # "Publish" (publishing never ran while the pipeline still reported
        # the target healthy). The alert survives instead as this persisted
        # marker file, checked by a separate AFTER-publish workflow step, so
        # a healthy target always publishes and a real archive hole still
        # reds the job (just after publish, not instead of it).
        statuses: dict[str, RunStatus] = {}
        window_failures: list[dict[str, str]] = []
        for key in keys:
            spec = datasets.DATASETS[key]
            if spec.derived:
                continue
            fetcher = spec.make_fetcher()
            # `trading_days_back`'s `end` is only included in its own result
            # when `end` IS a trading day -- if `target` itself falls on a
            # holiday/weekend (e.g. cron misfire), the window would silently
            # exclude it and this loop's "last element" would be some OTHER
            # day, never target. There is nothing to catch up FOR relative
            # to a non-trading-day target anyway (run_daily's own calendar
            # gate would just no-op it), so fall back to the pre-existing
            # single-day behavior unchanged: call run_daily on `target` alone
            # (-> "skipped_holiday", exactly as before this task).
            if not cal.is_trading_day(target, holidays, special_sessions=special):
                window = [target]
            else:
                window = cal.trading_days_back(
                    target, config.CATCHUP_WINDOW_DAYS, holidays, special
                )
            # G3 Task 2: ONE ReadCache per spec per `daily` invocation,
            # shared across every day in that spec's catch-up window -- the
            # same perf win as backfill.backfill applied to the nightly
            # cron's 7-day window, not just the one-time backfill. A fresh
            # cache per spec (not shared across specs) mirrors the existing
            # one-fetcher-per-spec lifetime above.
            cache = store.ReadCache()
            st = None
            for d in window:
                st = run_daily(
                    spec,
                    d,
                    fetcher=fetcher,
                    holidays=holidays,
                    special_sessions=special,
                    is_target_day=(d == target),
                    cache=cache,
                )
                if d != target and st.status == "failed":
                    print(
                        f"catch-up: {key} {d.isoformat()} failed: {st.message}",
                        file=sys.stderr,
                    )
                    window_failures.append(
                        {"dataset": key, "date": d.isoformat(), "message": st.message}
                    )
            assert st is not None  # window always has >=1 day (target itself)
            statuses[key] = st
            print(manifest.status_to_dict(st))

        # Phase 2: DERIVED specs -- only for a full `all` run, and only when
        # the primary fetched status is healthy.
        if (
            args.dataset == "all"
            and statuses.get(primary_key) is not None
            and statuses[primary_key].status in ok
        ):
            for key in datasets.DATASET_ORDER:
                spec = datasets.DATASETS[key]
                if not spec.derived:
                    continue
                if spec.external:
                    # Produced outside this pipeline (e.g. the Rust
                    # fundamentals producer via fundamentals-daily.yml) — no
                    # BUILDERS entry exists, and running _run_builder would
                    # emit a spurious `failed` status that data-daily's
                    # secondary-dataset check turns into a red job.
                    continue
                st = _run_builder(spec, target)
                statuses[key] = st
                print(manifest.status_to_dict(st))

        for key, st in statuses.items():
            if key == primary_key:
                manifest.write_status(st, config.META_DIR)  # drives monitor/publish gate
            else:
                manifest.write_status(st, config.META_DIR, filename=f"last_run_status_{key}.json")

        # G2 final-review fix (C1): persist collected window failures (primary
        # or secondary) ONLY when there are any -- written unconditionally as
        # a side channel, never gating the return code below. This file is a
        # runner-local signal consumed by a dedicated AFTER-publish step in
        # data-daily.yml (never uploaded as a publish/release artifact -- see
        # that workflow step's own comment for why scope stays minimal here).
        if window_failures:
            manifest.write_json({"failures": window_failures}, _window_failures_path)

        if primary_key in statuses:
            return 0 if statuses[primary_key].status in ok else 1
        return 0 if all(s.status in ok for s in statuses.values()) else 1
    if args.cmd == "backfill":
        holidays, special = cal.load_trading_calendar(config.META_DIR)
        keys = datasets.DATASET_ORDER if args.dataset == "all" else [args.dataset]
        all_results = []
        for key in keys:
            spec = datasets.DATASETS[key]
            if spec.derived:
                continue  # derived datasets are never fetched/backfilled
            all_results.extend(
                backfill_mod.backfill(
                    spec,
                    datetime.now(UTC).date(),
                    args.days,
                    fetcher=spec.make_fetcher(),
                    holidays=holidays,
                    special_sessions=special,
                )
            )
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
    if args.cmd == "snapshot":
        source_client = GhReleaseClient(repo=config.GITHUB_REPO, tag=config.RELEASE_TAG)

        def _dest_client_factory(t: str) -> ReleaseClient:
            return GhReleaseClient(repo=config.GITHUB_REPO, tag=t)

        list_client = _dest_client_factory(config.RELEASE_TAG)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                created = create_snapshot(
                    source_client,
                    _dest_client_factory,
                    work_dir=Path(tmp),
                    now=datetime.now(UTC),
                )
            pruned = prune_snapshots(_dest_client_factory, list_client)
        except (ReleaseError, UnexpectedFailure) as e:
            print(f"snapshot failed: {e}", file=sys.stderr)
            return 1
        print(f"snapshot: created {created}")
        print(f"snapshot: pruned {pruned}")
        return 0
    if args.cmd == "restore-from-snapshot":
        # --target unset -> scratch-drill default: config.DATA_DIR /
        # "_restore_drill" / tag. NEVER the live data/ tree unless the
        # operator explicitly passed --target -- this is the safety rail
        # that makes a rehearsal drill safe to run against any real
        # release/snapshot tag without risk of clobbering production data.
        target_root = (
            Path(args.target)
            if args.target is not None
            else config.DATA_DIR / "_restore_drill" / args.tag
        )
        client = GhReleaseClient(repo=config.GITHUB_REPO, tag=args.tag)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                restored_manifest = restore_from_tag(
                    client, target_root=target_root, work_dir=Path(tmp)
                )
        except (ReleaseError, UnexpectedFailure) as e:
            print(f"restore-from-snapshot failed: {e}", file=sys.stderr)
            return 1
        except OSError as e:
            # Phase-2-only failure (disk full, permissions, an OS race
            # mid-materialize) -- restore_from_tag's two-phase guarantee only
            # covers phase-1 (verification) failures; this can leave
            # target_root partially materialized. See restore.py's module
            # docstring and RUNBOOK.md's DR-drill section for the full
            # scoping discussion. Presented as a clean, actionable message
            # (never a raw traceback) with the exact remediation: the restore
            # is idempotent, so deleting the partial target and re-running
            # the same command fully repairs it.
            print(
                f"restore-from-snapshot failed (materialize error): {e} — "
                f"delete {target_root} and retry",
                file=sys.stderr,
            )
            return 1
        print(f"restore-from-snapshot: target {target_root}")
        for ds in restored_manifest.get("datasets", []):
            total_bytes = sum(f.get("bytes", 0) for f in ds.get("baseline", []))
            print(f"  {ds.get('name')}: latest_date={ds.get('latest_date')} bytes={total_bytes}")
        return 0
    if args.cmd == "check-freshness":
        holidays, special = cal.load_trading_calendar(config.META_DIR)
        with tempfile.TemporaryDirectory() as tmp:
            return cmd_check_freshness(
                repo=config.GITHUB_REPO,
                tag=config.RELEASE_TAG,
                holidays=holidays,
                today=datetime.now(UTC).date(),
                runner=_plain_runner,
                work_dir=Path(tmp),
                special_sessions=special,
            )
    if args.cmd == "rebuild-day":
        holidays, special = cal.load_trading_calendar(config.META_DIR)
        target = date.fromisoformat(args.date)
        return cmd_rebuild_day(target, holidays=holidays, special_sessions=special, via=args.via)
    if args.cmd == "backfill-corporate-actions":
        today = datetime.now(UTC).date()
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end) if args.end else today
        if start > end:
            print("backfill-corporate-actions: --from is after --to", file=sys.stderr)
            return 2
        status = backfill_corporate_actions_history(
            datasets.CORPORATE_ACTIONS_HISTORY,
            universe_spec=datasets.REFERENCE,
            start=start,
            end=end,
            today=today,
            get=ApiClient().get,
        )
        print(manifest.status_to_dict(status))
        return 0 if status.status == "success" else 1
    if args.cmd == "bootstrap-reference":
        client = GhReleaseClient(repo=config.GITHUB_REPO, tag=args.tag)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                status = bootstrap_reference(
                    client,
                    reference_spec=datasets.REFERENCE,
                    source_spec=datasets.EQUITIES,
                    work_dir=Path(tmp),
                    target=datetime.now(UTC).date(),
                    keep_prior=not args.ignore_prior,
                )
        except (ReleaseError, UnexpectedFailure) as e:
            print(f"bootstrap-reference failed: {e}", file=sys.stderr)
            return 1
        print(manifest.status_to_dict(status))
        return 0 if status.status == "success" else 1
    # publish
    client = GhReleaseClient(repo=config.GITHUB_REPO, tag=config.RELEASE_TAG)
    try:
        publish_dataset(
            specs=datasets.published_specs(),
            meta_dir=config.META_DIR,
            stage_dir=config.DATA_DIR / "stage",
            client=client,
            generated_at=datetime.now(UTC).isoformat(),
            now=datetime.now(UTC),
            allow_shrink=getattr(args, "allow_shrink", False),
            retired=datasets.RETIRED_MANIFEST_NAMES,
        )
    except (ReleaseError, UnexpectedFailure) as e:
        print(f"publish failed: {e}", file=sys.stderr)
        return 1
    return 0
