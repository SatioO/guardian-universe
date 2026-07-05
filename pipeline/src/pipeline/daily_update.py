"""One-day pipeline orchestration: gate -> idempotency -> fetch -> normalize ->
validate -> quarantine -> schema -> append. Fail-closed and idempotent."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

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
) -> RunStatus:
    if not cal.is_trading_day(target, holidays, special_sessions=special_sessions):
        return RunStatus("skipped_holiday", target, message="non-trading day")

    if store.has_day(spec.base_dir, target, prefix=spec.file_prefix):
        return RunStatus("skipped_idempotent", target, message="already present")

    try:
        res = fetcher.fetch_raw(target)
    except NotYetPublished as e:
        return RunStatus("not_yet", target, message=str(e))
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
