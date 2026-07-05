"""`python -m pipeline {daily,backfill,publish}` — wires real adapters."""
from __future__ import annotations

import argparse
import functools
import io
import json
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

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
)
from pipeline import calendar as cal
from pipeline.crosscheck import CrossCheckResult, compare_sources
from pipeline.daily_update import RunStatus, run_daily
from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.fetch import _BROWSER_UA, FetchResult, NseUdiffFetcher, _fetch_with_retry
from pipeline.normalize import normalize_equity_bhavcopy
from pipeline.publish import publish_dataset
from pipeline.release import GhReleaseClient
from pipeline.sources.nse_secfull import build_secfull_url, secfull_to_udiff_shape
from pipeline.sync import sync_store

Runner = Callable[[list[str]], int]

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
builders.BUILDERS["ca_flags"] = functools.partial(
    builders.build_ca_flags, source_spec=datasets.DATASETS[datasets.DATASET_ORDER[0]]
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
    r = sub.add_parser("rebuild-day")
    r.add_argument("--date", required=True)
    # choices derived from the registry (populated by this module's broker
    # self-registration imports above) -- never a hardcoded broker-name list.
    r.add_argument("--via", choices=[*rebuild.REBUILDERS], default=None)
    x = sub.add_parser("cross-check")
    x.add_argument("--date", default=None)
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
    return {
        str(row.symbol): (str(row.isin), str(row.series))
        for row in df.itertuples()
    }


def cmd_rebuild_day(target: date, *, holidays: set[date],
                     special_sessions: set[date] | None = None,
                     via: str | None = None) -> int:
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
    and a single manual equities rebuild is not that -- running them here
    would rebuild `reference`/`ca_flags` from a store that may still be
    missing surrounding days, which is out of scope for what this command is
    for (getting one day's raw OHLCV back into the store)."""
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
    status = run_daily(spec, target, fetcher=fetcher, holidays=holidays,
                        special_sessions=special_sessions)
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


def _today_for_cli() -> date:
    """Extracted so cross-check's --date default (and any future
    default-to-today CLI path) can be monkeypatched in tests without faking
    the whole `datetime` module."""
    return datetime.now(UTC).date()


def _cross_check_fetch_primary_raw(d: date) -> pd.DataFrame:
    """Production primary fetch for `cross-check`: the SAME NseUdiffFetcher
    used by the daily pipeline, but constructed WITHOUT fallbacks
    (`fallbacks=()`) -- cross-check tests the primary NSE endpoint in total
    isolation, never silently falling through to the secondary if the
    primary is down (that fallthrough would defeat the entire point of an
    independent cross-check)."""
    result = NseUdiffFetcher(fallbacks=()).fetch_raw(d)
    return result.frame


def _cross_check_fetch_secondary_raw(d: date) -> pd.DataFrame:
    """Production secondary fetch for `cross-check`: the sec_bhavdata_full
    endpoint, fetched and shape-adapted independently of the primary chain
    (mirrors `datasets._secfull_fallback`'s logic exactly, but is never
    routed through the primary's fallback list here -- this IS the isolated
    secondary source cross-check tests against). Uses the SAME isin_map
    loader (`datasets._load_isin_map`) as the real fallback path, so
    instrument_key values align with the primary side's ISIN keys."""
    session = requests.Session()
    session.headers.update({"User-Agent": _BROWSER_UA})
    url = build_secfull_url(d)
    raw = _fetch_with_retry(session, url, d, parse=_secfull_csv_to_df)
    isin_map = datasets._load_isin_map()
    return secfull_to_udiff_shape(raw, isin_map=isin_map)


def _secfull_csv_to_df(csv_bytes: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(csv_bytes))


def _print_cross_check_table(result: CrossCheckResult) -> None:
    print(f"cross-check: compared={result.compared} mismatched={result.mismatched}")
    if result.worst:
        print(f"{'instrument_key':<20}{'primary_close':>16}{'secondary_close':>18}")
        for key, primary_close, secondary_close in result.worst:
            print(f"{key:<20}{primary_close:>16.4f}{secondary_close:>18.4f}")


def cmd_cross_check(
    target: date,
    *,
    fetch_primary_raw: Callable[[date], pd.DataFrame],
    fetch_secondary_raw: Callable[[date], pd.DataFrame],
    sample_n: int = 50,
    tolerance: float = 0.001,
) -> int:
    """Weekly source cross-check: fetch `target` from BOTH independent NSE
    endpoints (isolated -- no fallback chain on either side), normalize both
    to canonical, and compare a deterministic sample of closes.

    No store writes -- this command is pure detection, never ingestion.

    Any fetch/normalize failure on EITHER source is itself alert-worthy (a
    cross-check that can't run is a signal on its own weekly cadence) -- it
    is caught here and reported as a clear, exit-1 error rather than
    propagating a raw traceback.
    """
    try:
        primary_raw = fetch_primary_raw(target)
        primary_canon = normalize_equity_bhavcopy(primary_raw, source="nse-udiff")
    except Exception as e:  # noqa: BLE001 - any primary failure is alert-worthy
        print(f"cross-check: primary source failed for {target.isoformat()}: {e}",
              file=sys.stderr)
        return 1

    try:
        secondary_raw = fetch_secondary_raw(target)
        secondary_canon = normalize_equity_bhavcopy(secondary_raw, source="nse-secfull")
    except Exception as e:  # noqa: BLE001 - any secondary failure is alert-worthy
        print(f"cross-check: secondary source failed for {target.isoformat()}: {e}",
              file=sys.stderr)
        return 1

    result = compare_sources(
        primary_canon, secondary_canon, sample_n=sample_n, tolerance=tolerance
    )
    _print_cross_check_table(result)
    return 1 if result.mismatched > 0 else 0


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
        # channel. But a catch-up day's own `failed` must not be silently
        # swallowed just because the target day succeeded: it's printed
        # explicitly, and for the PRIMARY spec it forces the overall exit
        # code to 1 -- a repaired hole that failed to repair must never look
        # like a clean run.
        statuses: dict[str, RunStatus] = {}
        primary_window_failure = False
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
            st = None
            for d in window:
                st = run_daily(
                    spec, d, fetcher=fetcher, holidays=holidays,
                    special_sessions=special, is_target_day=(d == target),
                )
                if d != target and st.status == "failed":
                    print(
                        f"catch-up: {key} {d.isoformat()} failed: {st.message}",
                        file=sys.stderr,
                    )
                    if key == primary_key:
                        primary_window_failure = True
            assert st is not None  # window always has >=1 day (target itself)
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
            if primary_window_failure:
                return 1  # a repaired-hole failure must never silently pass
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
    if args.cmd == "rebuild-day":
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        special = cal.load_special_sessions(config.META_DIR / "special_sessions.json")
        target = date.fromisoformat(args.date)
        return cmd_rebuild_day(target, holidays=holidays, special_sessions=special, via=args.via)
    if args.cmd == "cross-check":
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        special = cal.load_special_sessions(config.META_DIR / "special_sessions.json")
        target = (
            date.fromisoformat(args.date) if args.date
            else cal.previous_trading_day(_today_for_cli(), holidays, special)
        )
        return cmd_cross_check(
            target,
            fetch_primary_raw=_cross_check_fetch_primary_raw,
            fetch_secondary_raw=_cross_check_fetch_secondary_raw,
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
