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

    CLAMPED to available history (Critical fix, G2 task 7 follow-up): a
    dataset younger than `window` trading days deep (a brand-new store, or
    one still catching up to full depth) has no data at all for the portion
    of `expected` that predates its own first stored day -- that portion is
    NOT a hole, it is simply history the dataset has never had. `expected` is
    therefore intersected with days `>= min(dates_present)` before diffing,
    so only days within the dataset's OWN available depth can ever be
    flagged. `dates_present` empty (nothing verifiable yet, e.g. before a
    dataset's first publish) clamps `expected` to the empty set and this
    returns `[]` -- staleness/lag in that case is governed independently by
    `is_stale` (driven off `latest_trading_date`), not by this function.
    Once a dataset's depth reaches `window`, the clamp is a no-op and this
    behaves exactly as before. Returns `sorted(clamped_expected -
    dates_present)`."""
    expected = cal.trading_days_back(
        cal.previous_trading_day(today, holidays, special_sessions),
        window, holidays, special_sessions,
    )
    if not dates_present:
        return []
    floor = min(dates_present)
    clamped_expected = {d for d in expected if d >= floor}
    return sorted(clamped_expected - dates_present)
