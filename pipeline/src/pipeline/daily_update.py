"""One-day pipeline orchestration: gate -> idempotency -> fetch -> normalize ->
validate -> quarantine -> schema -> append. Fail-closed and idempotent."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pipeline import calendar as cal
from pipeline import store, validate
from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.fetch import Fetcher
from pipeline.normalize import normalize_equity_bhavcopy
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
    target: date,
    *,
    fetcher: Fetcher,
    holidays: set[date],
    base: Path,
) -> RunStatus:
    if not cal.is_trading_day(target, holidays):
        return RunStatus("skipped_holiday", target, message="non-trading day")

    if store.has_day(base, target):
        return RunStatus("skipped_idempotent", target, message="already present")

    try:
        raw = fetcher.fetch_raw(target)
    except NotYetPublished as e:
        return RunStatus("not_yet", target, message=str(e))
    except UnexpectedFailure as e:
        return RunStatus("failed", target, message=str(e))

    df = normalize_equity_bhavcopy(raw)

    trailing = _trailing_counts(base, target, holidays)
    try:
        validate.check_rowcount(len(df), trailing)
    except UnexpectedFailure as e:
        return RunStatus("failed", target, message=str(e))

    clean, bad = validate.quarantine_bad_rows(df)
    clean = validate_ohlc(clean)  # runtime contract gate (same schema as tests)

    store.append_day(clean, base)
    return RunStatus(
        status="success",
        date=target,
        symbol_count=len(clean),
        quarantined_count=len(bad),
        source="nse-udiff",
    )


def _trailing_counts(base: Path, target: date, holidays: set[date]) -> list[int]:
    counts: list[int] = []
    prev = cal.previous_trading_day(target, holidays)
    for d in cal.trading_days_back(prev, _TRAILING_DAYS, holidays):
        if store.has_day(base, d):
            counts.append(store.day_symbol_count(base, d))
    return counts
