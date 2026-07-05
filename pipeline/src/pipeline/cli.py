"""`python -m pipeline {daily,backfill,publish}` — wires real adapters."""
from __future__ import annotations

import argparse
import functools
import io
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
from pipeline.crosscheck import CrossCheckResult, compare_sources
from pipeline.daily_update import RunStatus, run_daily
from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.fetch import _BROWSER_UA, FetchResult, NseUdiffFetcher, _fetch_with_retry
from pipeline.normalize import normalize_equity_bhavcopy
from pipeline.publish import publish_dataset
from pipeline.release import GhReleaseClient, ReleaseClient
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
    # Explicit outcome marker (DIVERGENCE vs OK) so the summary line is
    # unambiguous on its own -- previously distinguishable from the
    # CANNOT-RUN path only by channel (stdout vs stderr) and prose wording,
    # which is easy to miss when scanning CI/alert output quickly.
    marker = "DIVERGENCE" if result.mismatched > 0 else "OK"
    print(f"cross-check: {marker} compared={result.compared} mismatched={result.mismatched}")
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

    Three distinguishable outcomes, each with an explicit prefix so a reader
    scanning CI/alert output never has to infer which happened from channel
    or wording alone: `cross-check: CANNOT-RUN — ...` (stderr, exit 1 -- a
    source's fetch/normalize itself failed), `cross-check: DIVERGENCE
    compared=N mismatched=M` (stdout, exit 1 -- both sources fetched fine but
    disagree on `M` of the `N` sampled closes), `cross-check: OK compared=N
    mismatched=0` (stdout, exit 0 -- both sources fetched fine and agreed).
    """
    try:
        primary_raw = fetch_primary_raw(target)
        primary_canon = normalize_equity_bhavcopy(primary_raw, source="nse-udiff")
    except Exception as e:  # noqa: BLE001 - any primary failure is alert-worthy
        # Explicit CANNOT-RUN marker: this path (stderr, exit 1) must read as
        # unmistakably distinct from the DIVERGENCE/OK summary line (stdout)
        # at a glance, not merely by which stream it landed on.
        print(f"cross-check: CANNOT-RUN — primary source failed for "
              f"{target.isoformat()}: {e}", file=sys.stderr)
        return 1

    try:
        secondary_raw = fetch_secondary_raw(target)
        secondary_canon = normalize_equity_bhavcopy(secondary_raw, source="nse-secfull")
    except Exception as e:  # noqa: BLE001 - any secondary failure is alert-worthy
        print(f"cross-check: CANNOT-RUN — secondary source failed for "
              f"{target.isoformat()}: {e}", file=sys.stderr)
        return 1

    result = compare_sources(
        primary_canon, secondary_canon, sample_n=sample_n, tolerance=tolerance
    )
    _print_cross_check_table(result)
    return 1 if result.mismatched > 0 else 0


def _continuity_window_years(today: date, holidays: set[date],
                              special_sessions: set[date] | None) -> list[int]:
    """Which calendar year(s) the continuity window's baseline asset(s) must
    be downloaded from. Same year-selection logic as
    `store.read_trailing_window`: the window is computed exactly as
    `freshness.missing_days` computes its own `expected` set, and if it
    straddles Jan 1 both the previous and current year are needed (ascending,
    de-duplicated, so a window fully inside one year downloads only that
    one)."""
    window = cal.trading_days_back(
        cal.previous_trading_day(today, holidays, special_sessions),
        10, holidays, special_sessions,
    )
    years = sorted({d.year for d in window})
    return years


def _dataset_manifest_entry(
    manifest_obj: dict[str, Any], manifest_name: str
) -> dict[str, Any] | None:
    entries: list[dict[str, Any]] = manifest_obj.get("datasets", [])
    for ds in entries:
        if ds.get("name") == manifest_name:
            return ds
    return None


@dataclass(frozen=True)
class ContinuityResult:
    """One fetched dataset's continuity outcome. `holes` empty means clean
    (subject to the same clamp `freshness.missing_days` applies -- a hole
    predating the dataset's own earliest stored day is never in here).

    `clamp_note` is set only when the trailing window was actually truncated
    to less than the full `window` size (i.e. `min(dates_present)` sits
    AFTER the window's own start) -- a young/still-catching-up dataset, not
    an error condition. `None` means the dataset's history already reaches
    the full window (the common/steady-state case) and nothing needs
    calling out beyond the normal hole report, if any."""

    holes: list[date]
    clamp_note: str | None


def _check_dataset_continuity(
    spec: datasets.DatasetSpec,
    manifest_obj: dict[str, Any],
    *,
    client: ReleaseClient,
    work_dir: Path,
    today: date,
    holidays: set[date],
    special_sessions: set[date] | None,
) -> ContinuityResult:
    """Downloads whichever baseline asset(s) cover the continuity window for
    one FETCHED dataset spec and returns its missing trading days (empty
    when clean) plus an informational clamp note when the dataset's own
    history is shallower than the full window. Raises `ReleaseError` if the
    manifest names an asset that the release doesn't actually have (a real
    inconsistency, distinct from the dataset being absent from the manifest
    entirely -- see the absent-dataset grace rule in the caller)."""
    ds = _dataset_manifest_entry(manifest_obj, spec.manifest_name)
    if ds is None:
        # Grace rule: a fetched dataset that has never appeared in the
        # manifest is NOT a continuity failure -- see RUNBOOK ("a
        # never-published dataset does not alert; first publish arms it").
        # The caller treats an empty list here identically to "clean", but
        # prints its own WARNING (it, not this function, knows the dataset
        # NAME to name in that warning without re-deriving it).
        raise _DatasetNotYetPublished(spec.manifest_name)

    years = _continuity_window_years(today, holidays, special_sessions)
    by_name = {f["name"]: f for f in manifest.dataset_files(ds)}
    dates_present: set[date] = set()
    for year in years:
        name = f"{spec.file_prefix}_{year}.parquet"
        entry = by_name.get(name)
        if entry is None:
            continue  # that year's baseline doesn't exist yet (e.g. brand-new dataset)
        asset = entry.get("asset", entry["name"])
        client.download([asset], work_dir)
        got = work_dir / asset
        col = pd.to_datetime(pd.read_parquet(got, columns=["date"])["date"])
        dates_present.update(d.date() for d in col)

    holes = freshness.missing_days(dates_present, today, holidays, special_sessions)

    # Clamp note: re-derive the SAME full-size expected window `missing_days`
    # itself computes, so this stays in lockstep with the pure function's own
    # windowing rather than re-implementing/duplicating it -- only the
    # comparison against `dates_present`'s floor is new here, purely for the
    # informational message (never for the hole computation above, which
    # `missing_days` already owns end-to-end).
    clamp_note: str | None = None
    if dates_present:
        window_size = 10  # mirrors `missing_days`'s own default -- see its docstring
        full_expected = cal.trading_days_back(
            cal.previous_trading_day(today, holidays, special_sessions),
            window_size, holidays, special_sessions,
        )
        floor = min(dates_present)
        if floor > full_expected[0]:
            n_verified = sum(1 for d in full_expected if d >= floor)
            clamp_note = (
                f"continuity: {spec.manifest_name} verified over {n_verified} "
                f"of {window_size} days (history begins {floor.isoformat()})"
            )

    return ContinuityResult(holes=holes, clamp_note=clamp_note)


class _DatasetNotYetPublished(Exception):
    """Internal signal (never escapes cmd_check_freshness): the dataset has
    no manifest entry at all yet -- the absent-from-manifest grace rule."""

    def __init__(self, manifest_name: str) -> None:
        super().__init__(manifest_name)
        self.manifest_name = manifest_name


def cmd_check_freshness(
    *,
    repo: str,
    tag: str,
    holidays: set[date],
    today: date,
    runner: Runner,
    work_dir: Path,
    client: ReleaseClient,
    special_sessions: set[date] | None = None,
) -> int:
    """Freshness monitor: three independent checks, run in order.

    1. STALENESS (unchanged from pre-G2 behavior): is the manifest's
       `latest_trading_date` current as of `today`? This only ever looks at
       a single date field -- it says nothing about holes further back.
    2. CALENDAR HYGIENE (G2 task 8): is `holidays.json` (the `holidays` set
       passed in by the caller) due for its yearly refresh? On/after
       December 1st, a `holidays` set with no entry dated in NEXT year is
       flagged -- see `freshness.holidays_need_refresh` for the exact rule
       and rationale. This is independent of dataset staleness/continuity:
       it is about the trading-CALENDAR input going stale, not about
       published data falling behind. Failing this prints a clear message
       naming the missing year so an operator knows exactly what to refresh
       (see the RUNBOOK's yearly holiday-refresh procedure and
       `.github/workflows/holidays-refresh.yml`, which nags via a GitHub
       issue on the same yearly cadence so this CLI-level check is a second,
       independent tripwire rather than the only signal).
    3. CONTINUITY (G2 task 7): for EVERY FETCHED dataset in the registry
       (equities AND indices -- see the scope-amendment note at the top of
       the continuity test suite in test_cli.py for why this covers more
       than just the primary), are there any missing trading days inside the
       trailing 10-trading-day window? A dataset can have a perfectly
       current latest date while still hiding a hole a few sessions back
       (e.g. a hole outside the daily catch-up loop's own 7-day reach, or a
       secondary dataset's hole that the primary's own health never
       reflects). Derived datasets (reference, ca_flags) are never checked
       here -- they rebuild from the store, so a hole there is a symptom of
       an underlying fetched-dataset hole, not an independent fact.

       CLAMPED TO AVAILABLE HISTORY (Critical fix): the window is never
       expected to reach further back than a dataset's own first stored
       day -- a young/still-catching-up dataset (depth < 10 trading days)
       is NEVER treated as having holes for the portion of the window that
       predates its own earliest day; see `freshness.missing_days`'s
       docstring for the exact rule. When a dataset's window is truncated
       this way, an informational (non-failing) "verified over N of 10
       days (history begins ...)" line is printed so reduced coverage stays
       visible -- this line is never itself a failure signal, and full
       10-day verification is armed automatically as the dataset's history
       accumulates past 10 trading days, with no code change or manual step
       required. A manifest-listed-but-missing release asset (a real
       release/manifest inconsistency, not absence-from-manifest) is caught
       as a clean per-dataset error, never a raw traceback.

    ANY missing day in ANY fetched dataset exits 1, with each dataset's
    holes printed on their own line(s). A fetched dataset with NO manifest
    entry at all (never published yet) is a WARNING on stderr, not a
    failure -- see `_check_dataset_continuity`'s grace rule."""
    work_dir.mkdir(parents=True, exist_ok=True)
    rc = runner(["gh", "release", "download", tag, "--repo", repo,
                 "--pattern", "manifest.json", "--dir", str(work_dir), "--clobber"])
    manifest_path = work_dir / "manifest.json"
    if rc != 0 or not manifest_path.exists():
        return 1  # no release / download failed -> stale
    manifest_obj = json.loads(manifest_path.read_text())
    latest = date.fromisoformat(manifest_obj["latest_trading_date"])
    if freshness.is_stale(latest, today, holidays, special_sessions):
        return 1

    ok = True
    if freshness.holidays_need_refresh(holidays, today):
        ok = False
        print(
            f"check-freshness: holidays.json needs its yearly refresh -- "
            f"today ({today.isoformat()}) is on/after December 1st and no "
            f"holiday dated in {today.year + 1} is present yet; refresh "
            "holidays.json (and special_sessions.json alongside it) from "
            "the NSE trading-holiday circular -- see RUNBOOK.md 'Yearly'"
        )

    for key in datasets.DATASET_ORDER:
        spec = datasets.DATASETS[key]
        if spec.derived:
            continue
        try:
            result = _check_dataset_continuity(
                spec, manifest_obj, client=client, work_dir=work_dir,
                today=today, holidays=holidays, special_sessions=special_sessions,
            )
        except _DatasetNotYetPublished as e:
            print(
                f"check-freshness: WARNING dataset '{e.manifest_name}' has no "
                "manifest entry yet (never published) -- not treated as stale; "
                "holes will be enforced once it publishes for the first time",
                file=sys.stderr,
            )
            continue
        except ReleaseError as e:
            # A manifest-listed-but-missing asset is a REAL alarm (a
            # release/manifest consistency break), not a crash -- never let a
            # raw traceback surface here. Distinct from the absent-from-
            # manifest grace rule above: this is the dataset's entry naming
            # an asset the release itself doesn't actually have.
            ok = False
            print(
                f"check-freshness: cross-dataset consistency error for "
                f"'{spec.manifest_name}': {e}",
                file=sys.stderr,
            )
            continue
        if result.clamp_note:
            print(result.clamp_note)
        if result.holes:
            ok = False
            days = ", ".join(d.isoformat() for d in result.holes)
            print(f"check-freshness: dataset '{spec.manifest_name}' missing "
                  f"trading day(s): {days}")
    return 0 if ok else 1


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
                    spec, d, fetcher=fetcher, holidays=holidays,
                    special_sessions=special, is_target_day=(d == target),
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
        client = GhReleaseClient(repo=config.GITHUB_REPO, tag=config.RELEASE_TAG)
        with tempfile.TemporaryDirectory() as tmp:
            return cmd_check_freshness(
                repo=config.GITHUB_REPO, tag=config.RELEASE_TAG, holidays=holidays,
                today=datetime.now(UTC).date(), runner=_plain_runner,
                work_dir=Path(tmp), special_sessions=special, client=client,
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
