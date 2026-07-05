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
) -> RunStatus:
    if not cal.is_trading_day(target, holidays, special_sessions=special_sessions):
        return RunStatus("skipped_holiday", target, message="non-trading day")

    # G2 Task 5: completeness-aware idempotency. `has_day` alone ("*>=1* row
    # present") used to lock a partial day in forever -- a short day (mid-fetch
    # truncation, a fallback that only partially served the universe, etc.)
    # would never be topped up because the mere PRESENCE of any row
    # short-circuited every later run, including the catch-up loop's (T4)
    # nightly re-visits. Cost-conscious ordering: the stored total is one
    # cheap year-read, checked FIRST -- the (up to 10-year-read) trailing
    # computation only runs for a day that is actually present, never for an
    # absent day (an absent day falls through to fetch exactly as before, at
    # zero extra idempotency-gate cost; its own trailing read for the
    # per-series gate below is unaffected and unchanged).
    topup_note = ""
    if store.has_day(spec.base_dir, target, prefix=spec.file_prefix):
        stored_total = store.day_symbol_count(spec.base_dir, target, prefix=spec.file_prefix)
        completeness_trailing = _trailing_series_counts(
            spec, target, holidays, special_sessions
        )
        # sum of per-series trailing MEANS (not a flat total-row-count mean) --
        # consistent with the per-series gate's own per-series means below
        # (a separate call further down, at its original post-fetch site: an
        # absent day must not pay this cost at all here, so the two branches
        # each compute their own trailing dict rather than sharing one).
        trailing_total_mean = sum(
            sum(c) / len(c) for c in completeness_trailing.values() if c
        )
        # No trailing history at all (fresh store, or every trailing day was a
        # miss) -- nothing to compare the stored day against, so any stored
        # rows count as complete: preserves pre-Task-5 behavior exactly.
        if not trailing_total_mean or stored_total >= (
            1 - config.COMPLETENESS_SHORTFALL
        ) * trailing_total_mean:
            return RunStatus("skipped_idempotent", target, message="already present")
        # Short day: fall through to re-fetch. append_keyed's keep="last"
        # dedupe (on date, instrument_key) makes the merge safe -- rows already
        # stored are simply overwritten by the freshly re-fetched copy, and
        # rows the short fetch missed the first time are newly added.
        topup_note = (
            f"re-ingested short day (stored {stored_total} vs "
            f"trailing mean {trailing_total_mean:.0f})"
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
        trailing = _trailing_series_counts(spec, target, holidays, special_sessions)
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
        store.append_day(clean, spec.base_dir, prefix=spec.file_prefix)
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
) -> dict[str, list[int]]:
    """Per-series trailing row counts over the trailing window, keyed by
    series. A series absent on a given trailing day simply contributes no
    entry for that day (not a zero) -- `check_rowcount_by_series` treats an
    empty list as "no history yet" and a present-but-short list on its own
    merits (mean over however many days it actually appeared)."""
    counts: dict[str, list[int]] = {}
    prev = cal.previous_trading_day(target, holidays, special_sessions)
    for d in cal.trading_days_back(prev, _TRAILING_DAYS, holidays, special_sessions):
        if store.has_day(spec.base_dir, d, prefix=spec.file_prefix):
            day_counts = store.day_series_counts(spec.base_dir, d, prefix=spec.file_prefix)
            for series, n in day_counts.items():
                counts.setdefault(series, []).append(n)
    return counts
