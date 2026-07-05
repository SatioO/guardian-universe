"""Freshness detection for the published dataset. Pure."""
from __future__ import annotations

from datetime import date

from pipeline import calendar as cal


def is_stale(
    latest_trading_date: date,
    today: date,
    holidays: set[date],
    special_sessions: set[date] | None = None,
) -> bool:
    # Stale when the most recent COMPLETED trading day is not yet published.
    return latest_trading_date < cal.previous_trading_day(today, holidays, special_sessions)


def missing_days(
    dates_present: set[date],
    today: date,
    holidays: set[date],
    special_sessions: set[date] | None = None,
    *,
    window: int = 10,
) -> list[date]:
    """Continuity check (G2 task 7): unlike `is_stale` (which only asks "is
    the LATEST published day current?"), this asks "are there any HOLES
    inside the trailing window?" -- a dataset can have a perfectly current
    latest date while still missing a day buried a few sessions back (e.g. a
    repaired-but-then-re-broken day, or a hole predating the catch-up
    window's own 7-day reach).

    Pure: `expected` is the `window` trading days ending at the last
    COMPLETED trading day before `today` (mirrors `is_stale`'s own
    "completed trading day" framing -- `today` itself is never expected,
    even if `today` is a trading day, since it may not have published yet).
    Returns `sorted(expected - dates_present)`."""
    expected = cal.trading_days_back(
        cal.previous_trading_day(today, holidays, special_sessions),
        window, holidays, special_sessions,
    )
    return sorted(set(expected) - dates_present)
