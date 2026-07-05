"""One-day pipeline orchestration: gate -> idempotency -> fetch -> normalize ->
validate -> quarantine -> schema -> append. Fail-closed and idempotent."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
from pandera.errors import SchemaError

from pipeline import calendar as cal
from pipeline import config, store, validate
from pipeline.datasets import DatasetSpec
from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.fetch import Fetcher
from pipeline.schema import validate_ohlc

_TRAILING_DAYS = 10


@dataclass(frozen=True)
class RunStatus:
    status: str
    date: date
    symbol_count: int = 0
    quarantined_count: int = 0
    source: str = ""
    message: str = ""


def run_daily(
    spec: DatasetSpec,
    target: date,
    *,
    fetcher: Fetcher,
    holidays: set[date],
    special_sessions: set[date] | None = None,
    is_target_day: bool = True,
    cache: store.ReadCache | None = None,
) -> RunStatus:
    if not cal.is_trading_day(target, holidays, special_sessions=special_sessions):
        return RunStatus("skipped_holiday", target, message="non-trading day")

    # G2 Task 5: completeness-aware idempotency. `has_day` alone ("*>=1* row
    # present") used to lock a partial day in forever -- a short day (mid-fetch
    # truncation, a fallback that only partially served the universe, etc.)
    # would never be topped up because the mere PRESENCE of any row
    # short-circuited every later run, including the catch-up loop's (T4)
    # nightly re-visits. Cost-conscious ordering: the stored per-series counts
    # are one cheap year-read, checked FIRST -- the (up to 10-year-read)
    # trailing computation only runs for a day that is actually present, never
    # for an absent day (an absent day falls through to fetch exactly as
    # before, at zero extra idempotency-gate cost; its own trailing read for
    # the per-series gate below is unaffected and unchanged).
    #
    # Fix round 1 (regime-consistent INTERSECTION completeness): comparing the
    # stored day's TOTAL against the trailing mean summed over ALL trailing
    # series breaks across a universe-widening event -- a pre-widening,
    # EQ-only day inside the catch-up window would compare its own (correct,
    # complete) EQ-only total against a trailing mean that also includes a
    # series (e.g. BE) the stored day never had, making a genuinely complete
    # day look falsely short forever. The comparison must stay within the
    # REGIME the stored day itself belongs to: only series present in BOTH the
    # stored day and the trailing history (a non-empty trailing entry) count
    # toward either side of the ratio. A series the stored day never had
    # (because it predates that series' introduction) is correctly excluded
    # instead of inflating the trailing denominator; a series genuinely
    # missing/truncated WITHIN the stored day's own regime still fails the
    # ratio exactly as before, since it remains in `shared`.
    topup_note = ""
    if store.has_day(spec.base_dir, target, prefix=spec.file_prefix, cache=cache):
        stored_series_counts = store.day_series_counts(
            spec.base_dir, target, prefix=spec.file_prefix, cache=cache
        )
        completeness_trailing = _trailing_series_counts(
            spec, target, holidays, special_sessions, cache=cache
        )
        # shared = series present in BOTH the stored day (any stored rows) AND
        # the trailing dict (a non-empty trailing entry -- i.e. it actually
        # appeared on at least one trailing day). A series stored today but
        # absent from all trailing days (brand new today) is excluded from
        # `shared` on the trailing side already (no entry, or an empty list);
        # a series with trailing history but absent from today is likewise
        # excluded (not in `stored_series_counts`) -- that absence is instead
        # the per-series gate's job (`validate.check_rowcount_by_series`,
        # below), not this idempotency gate's.
        shared = stored_series_counts.keys() & {
            series for series, c in completeness_trailing.items() if c
        }
        stored_shared_total = sum(stored_series_counts[s] for s in shared)
        trailing_shared_mean = sum(
            sum(completeness_trailing[s]) / len(completeness_trailing[s])
            for s in shared
        )
        # No comparable regime at all (no series shared between the stored
        # day and trailing history -- a fresh store, every trailing day a
        # miss, or a stored day whose entire series set predates all trailing
        # history) -- nothing to compare the stored day against, so any
        # stored rows count as complete: preserves pre-Task-5 behavior exactly.
        if not trailing_shared_mean or stored_shared_total >= (
            1 - config.COMPLETENESS_SHORTFALL
        ) * trailing_shared_mean:
            return RunStatus("skipped_idempotent", target, message="already present")
        # Short day: fall through to re-fetch. append_keyed's keep="last"
        # dedupe (on date, instrument_key) makes the merge safe -- rows already
        # stored are simply overwritten by the freshly re-fetched copy, and
        # rows the short fetch missed the first time are newly added.
        topup_note = (
            f"re-ingested short day (stored {stored_shared_total} vs "
            f"trailing mean {trailing_shared_mean:.0f} over shared series)"
        )

    try:
        res = fetcher.fetch_raw(target)
    except NotYetPublished as e:
        # A 404 on the day we're actually trying to publish today (the
        # target) is ordinary lateness -- the bhavcopy simply isn't out yet.
        # A 404 on any OTHER day in the catch-up window (G2 Task 4) is a
        # different thing entirely: that day is in the past relative to the
        # target, so NSE's archive should already have it -- a 404 there
        # means the archive has a HOLE, not that the day is running late.
        # Treating it as "not_yet" (the CLI's non-alerting ok-set) would let
        # a real, permanent hole in the catch-up window quietly re-appear as
        # lateness on every subsequent run, forever -- so it must be
        # "failed" instead (retryable, and alertable via the exit code).
        if is_target_day:
            return RunStatus("not_yet", target, message=str(e))
        return RunStatus(
            "failed", target,
            message=f"archive missing for past trading day {target.isoformat()}",
        )
    except UnexpectedFailure as e:
        return RunStatus("failed", target, message=str(e))

    # Post-fetch processing is fully guarded: run_daily must ALWAYS return a
    # RunStatus, never raise. Expected data-quality failures (UnexpectedFailure,
    # SchemaError) AND unexpected ones (a malformed frame -> KeyError, a store
    # OSError, ...) all map to a "failed" status so the scheduler never crashes.
    try:
        df = spec.normalizer(res.frame)
        # Stamp ACTUAL provenance over the normalizer's partial-bound default:
        # a fallback-served day must never carry the primary's source label.
        df["source"] = res.source
        # Wrong-date guard (data-corruption gate): a source can serve a
        # wrong-dated file (stale republish, fallback date-stamp bug) that
        # otherwise passes every downstream gate. Unchecked, that either (a)
        # reports "success" for `target` while writing zero target rows --
        # endless refetch -- or (b) if the embedded date matches an
        # already-stored day, silently overwrites correct historical rows via
        # append_keyed's keep="last" dedupe. Reject before any other gate or
        # store write -- cheapest early exit, and no partial/quarantine state
        # for a day that isn't even the one we asked for. Empty frames pass
        # through untouched (the existing abs-floor gate fails them as today).
        if len(df) > 0 and not (df["date"] == pd.Timestamp(target)).all():
            fetched_dates = sorted(df["date"].dt.date.unique())[:3]
            return RunStatus(
                "failed", target,
                message=(
                    f"fetched frame dates {fetched_dates}... do not match "
                    f"requested target {target}"
                ),
            )
        trailing = _trailing_series_counts(
            spec, target, holidays, special_sessions, cache=cache
        )
        # NOTE: the per-series deviation gate compares today's PRE-quarantine
        # row counts against the POST-quarantine counts of stored days; the
        # difference is negligible in steady state (few rows are ever
        # quarantined). This REPLACES the old total-deviation gate (a
        # universe-widening day would otherwise trip a large total deviation
        # vs EQ-only trailing history).
        today_series_counts = (
            {str(k): int(v) for k, v in df.groupby("series").size().items()}
            if len(df) > 0
            else {}
        )
        validate.check_rowcount_by_series(
            len(df), today_series_counts, trailing, abs_range=spec.abs_rowcount_range
        )
        clean, bad = validate.quarantine_bad_rows(df)
        if len(bad) > 0:
            qdir = config.META_DIR / "quarantine"
            qdir.mkdir(parents=True, exist_ok=True)
            qtarget = qdir / f"{spec.file_prefix}_{target.isoformat()}.parquet"
            qtmp = qtarget.with_suffix(".parquet.tmp")
            bad.to_parquet(qtmp, compression="zstd", index=False)
            qtmp.replace(qtarget)
        if len(clean) == 0 and len(df) > 0:
            # A day where every row is corrupt is a data failure, not a
            # successful no-op — return "failed" so it is NOT recorded as done
            # (has_day stays False; the day can be retried).
            return RunStatus(
                "failed", target, quarantined_count=len(bad),
                message=f"all {len(df)} rows failed quarantine",
            )
        clean = validate_ohlc(clean)  # runtime contract gate (same schema as tests)
        store.append_day(clean, spec.base_dir, prefix=spec.file_prefix, cache=cache)
        store.write_delta(clean, spec.base_dir, target, prefix=spec.file_prefix)
        return RunStatus(
            status="success",
            date=target,
            symbol_count=len(clean),
            quarantined_count=len(bad),
            source=res.source,
            message=topup_note,
        )
    except (UnexpectedFailure, SchemaError) as e:
        return RunStatus("failed", target, message=str(e))
    except Exception as e:  # boundary guard: run_daily must never raise
        return RunStatus("failed", target, message=f"unexpected pipeline error: {e}")


def _trailing_series_counts(
    spec: DatasetSpec,
    target: date,
    holidays: set[date],
    special_sessions: set[date] | None = None,
    *,
    cache: store.ReadCache | None = None,
) -> dict[str, list[int]]:
    """Per-series trailing row counts over the trailing window, keyed by
    series. A series absent on a given trailing day simply contributes no
    entry for that day (not a zero) -- `check_rowcount_by_series` treats an
    empty list as "no history yet" and a present-but-short list on its own
    merits (mean over however many days it actually appeared).

    `cache` (G3 Task 2) is opt-in and threaded straight through to every
    internal `store.has_day`/`store.day_series_counts` call: `None` (the
    default) preserves the always-read-fresh behavior exactly, while a
    caller iterating many days in one process (backfill; the CLI's
    catch-up window) can share one `store.ReadCache()` across every one of
    these trailing lookups so each year-file is read from disk once per
    version instead of once per lookup."""
    counts: dict[str, list[int]] = {}
    prev = cal.previous_trading_day(target, holidays, special_sessions)
    for d in cal.trading_days_back(prev, _TRAILING_DAYS, holidays, special_sessions):
        if store.has_day(spec.base_dir, d, prefix=spec.file_prefix, cache=cache):
            day_counts = store.day_series_counts(
                spec.base_dir, d, prefix=spec.file_prefix, cache=cache
            )
            for series, n in day_counts.items():
                counts.setdefault(series, []).append(n)
    return counts
