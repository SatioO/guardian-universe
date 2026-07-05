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


def holidays_need_refresh(holidays: set[date], today: date) -> bool:
    """Calendar-hygiene nag rule (G2 task 8): is `holidays.json` due for its
    yearly refresh?

    True when BOTH: (1) `today` is on or after December 1st of `today`'s own
    year (the point at which NSE's next-year trading-holiday circular is
    normally published, so refreshing earlier would just be premature noise
    the operator can't act on yet), AND (2) `holidays` contains no entry
    dated in `today.year + 1` -- i.e. the refresh genuinely hasn't happened
    yet for the upcoming year.

    Pure and independent of `is_stale`/`missing_days`: this says nothing
    about whether any PUBLISHED dataset is behind, only whether the
    trading-calendar INPUT the pipeline relies on (holidays.json) is about
    to go stale for next year. Deliberately keyed off `today`'s own year (not
    a fixed calendar constant) so the same rule works correctly across any
    year boundary, including a holidays.json that was never refreshed across
    a PRIOR year-end (in which case this keeps returning True every day
    after the following Dec 1 too, since `today.year + 1` still has no
    entry)."""
    dec_1_this_year = date(today.year, 12, 1)
    if today < dec_1_this_year:
        return False
    next_year = today.year + 1
    return not any(d.year == next_year for d in holidays)
