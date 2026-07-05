from datetime import date

from pipeline import calendar as cal
from pipeline.freshness import is_stale, missing_days

HOLIDAYS: set[date] = set()


def test_fresh_when_latest_is_the_last_completed_trading_day():
    # today Mon 2026-07-06; last completed trading day is Fri 2026-07-03.
    assert is_stale(date(2026, 7, 3), date(2026, 7, 6), HOLIDAYS) is False


def test_stale_when_latest_is_behind_the_last_completed_trading_day():
    # today Mon 2026-07-06; latest only Thu 2026-07-02 -> Friday was missed.
    assert is_stale(date(2026, 7, 2), date(2026, 7, 6), HOLIDAYS) is True


def test_not_stale_over_a_weekend():
    # today Sun 2026-07-05; last completed trading day is Fri 2026-07-03.
    assert is_stale(date(2026, 7, 3), date(2026, 7, 5), HOLIDAYS) is False


# -- missing_days (G2 task 7: continuity, not just staleness) --

def _expected_window(today: date, holidays: set[date], window: int = 10,
                      special_sessions: set[date] | None = None) -> list[date]:
    return cal.trading_days_back(
        cal.previous_trading_day(today, holidays, special_sessions),
        window, holidays, special_sessions,
    )


def test_missing_days_clean_window_returns_empty():
    today = date(2026, 7, 6)  # Monday
    expected = _expected_window(today, HOLIDAYS)
    assert missing_days(set(expected), today, HOLIDAYS) == []


def test_missing_days_hole_mid_window_is_detected():
    today = date(2026, 7, 6)
    expected = _expected_window(today, HOLIDAYS)
    present = set(expected)
    hole = expected[len(expected) // 2]  # a day squarely inside the window
    present.discard(hole)
    assert missing_days(present, today, HOLIDAYS) == [hole]


def test_missing_days_multiple_holes_returned_sorted():
    today = date(2026, 7, 6)
    expected = _expected_window(today, HOLIDAYS)
    present = set(expected)
    hole_a, hole_b = expected[1], expected[-2]
    present.discard(hole_a)
    present.discard(hole_b)
    assert missing_days(present, today, HOLIDAYS) == sorted([hole_a, hole_b])


def test_missing_days_is_weekend_aware_never_expects_a_weekend_day():
    # A naive calendar-day diff would expect a Saturday/Sunday; the trading
    # calendar must never include one in `expected`, so a present set that
    # only has trading days is never flagged for the weekend gap itself.
    today = date(2026, 7, 6)  # Monday
    expected = _expected_window(today, HOLIDAYS)
    assert all(d.weekday() < 5 for d in expected)
    assert missing_days(set(expected), today, HOLIDAYS) == []


def test_missing_days_is_holiday_aware():
    holidays = {date(2026, 1, 26)}  # Republic Day, a Monday in 2026
    today = date(2026, 1, 29)  # Thursday
    expected = _expected_window(today, holidays)
    assert date(2026, 1, 26) not in expected  # holiday never expected
    assert missing_days(set(expected), today, holidays) == []


def test_missing_days_is_special_session_aware():
    # A special session (e.g. Muhurat) trades despite falling on a weekend --
    # it belongs in `expected`, and a present set lacking it is a real hole.
    muhurat = date(2026, 11, 8)  # a Sunday
    today = date(2026, 11, 10)  # Tuesday
    special = {muhurat}
    expected = _expected_window(today, set(), special_sessions=special)
    assert muhurat in expected
    present = set(expected)
    present.discard(muhurat)
    assert missing_days(present, today, set(), special) == [muhurat]


def test_missing_days_window_straddles_year_boundary():
    # today early Jan -> the trailing window reaches back into December.
    today = date(2026, 1, 5)  # Monday
    expected = _expected_window(today, HOLIDAYS)
    assert expected[0].year == 2025 and expected[-1].year == 2026
    # Hole picked strictly AFTER expected[0] (not expected[0] itself): with
    # the Critical-fix clamp (below), `min(dates_present)` IS the floor a
    # young store's earliest day is measured against, so a present set whose
    # only gap is the window's own first slot is indistinguishable from "this
    # dataset's history simply starts one day later than the naive window" --
    # exactly the state the clamp exists to excuse, not flag. Picking the
    # hole one slot in keeps `expected[0]` present, so it unambiguously tests
    # "a real internal hole survives the clamp," independent of that concern.
    hole = expected[1]
    present = set(expected)
    present.discard(hole)
    assert missing_days(present, today, HOLIDAYS) == [hole]


def test_missing_days_respects_custom_window_size():
    today = date(2026, 7, 6)
    expected5 = _expected_window(today, HOLIDAYS, window=5)
    assert len(expected5) == 5
    # See the year-boundary test above for why the hole is picked at index 1,
    # not 0, post-Critical-fix clamp.
    hole = expected5[1]
    present = set(expected5)
    present.discard(hole)
    assert missing_days(present, today, HOLIDAYS, window=5) == [hole]
    # The same hole outside a shorter window doesn't leak into a wider one's
    # absence check when the wider window's present set is fully seeded.
    expected10 = _expected_window(today, HOLIDAYS, window=10)
    assert missing_days(set(expected10), today, HOLIDAYS, window=10) == []


# -- clamp to available history (Critical fix: no false holes predating the
# dataset's own first stored day) --
#
# THE BUG: a fixed 10-trading-day `expected` window diffed against
# `dates_present` with no floor at the dataset's earliest available date
# reports every expected day before the store's first day as a "hole" --
# for a brand-new/young store (e.g. depth 3, matching the real live store's
# actual shape at time of writing) this is 7 FALSE alarms every single
# morning until depth reaches `window` (or a backfill runs). These tests are
# RED against the pre-clamp implementation: the same present set reproduced
# above (`{2026-06-22..30}` minus the 3 real days) is exactly what a naive
# diff would return.

def test_missing_days_clamps_to_available_history_reproduces_real_3day_store():
    # THE REPRODUCTION: mirrors the real live store's actual shape at the
    # time this bug was found -- 3 stored days, {2026-07-01, 07-02, 07-03},
    # today = 2026-07-06 (Monday), full 10-day window. Pre-clamp, this
    # returns the 7 days {2026-06-22..30} -- all predating the store's own
    # earliest date -- as false holes. Post-clamp: the window is intersected
    # with days >= min(dates_present) (2026-07-01), so those 3 days ARE the
    # entire clamped window and nothing is missing.
    present = {date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)}
    today = date(2026, 7, 6)
    assert missing_days(present, today, HOLIDAYS) == []


def test_missing_days_hole_within_available_depth_still_flagged_after_clamp():
    # The clamp must never hide a REAL hole inside the dataset's own
    # available depth -- only days predating the earliest stored date are
    # excused. present = {earliest, latest} with a genuine gap between them
    # (both >= earliest) must still surface every day in that gap.
    today = date(2026, 7, 6)
    expected = _expected_window(today, HOLIDAYS)
    d_early, d_late = expected[-3], expected[-1]  # both well within depth
    present = {d_early, d_late}  # the day(s) between them are a real hole
    gap = [d for d in expected if d_early < d < d_late]
    assert gap, "fixture must have at least one day strictly between d_early and d_late"
    assert missing_days(present, today, HOLIDAYS) == gap


def test_missing_days_empty_dates_present_returns_empty():
    # Nothing verifiable yet -- an empty store has no "available history" to
    # clamp against, so there is nothing to diff. The existing STALENESS
    # check (`is_stale`, driven off `latest_trading_date` independently)
    # governs lag in this case; `missing_days` itself must not report the
    # entire window as missing.
    today = date(2026, 7, 6)
    assert missing_days(set(), today, HOLIDAYS) == []


def test_missing_days_clamp_is_a_noop_at_full_window_depth():
    # At depth >= window (the pre-existing/steady-state case), the clamp's
    # floor (min(dates_present)) sits at or before the window's own start,
    # so intersecting changes nothing -- this is the same assertion as
    # test_missing_days_clean_window_returns_empty, restated here explicitly
    # as a "clamp is a no-op once history is deep enough" regression guard.
    today = date(2026, 7, 6)
    expected = _expected_window(today, HOLIDAYS)
    assert missing_days(set(expected), today, HOLIDAYS) == []


def test_missing_days_year_boundary_with_young_store_no_prev_year_data():
    # SECOND TRIGGER of the same defect: at a year boundary, a dataset less
    # than a year old has no previous-year data at all (there's nothing to
    # have -- the dataset didn't exist yet). Pre-clamp, every previous-year
    # expected day would be a false hole exactly like the 3-day repro above.
    # Post-clamp: dates_present's earliest date is itself in the CURRENT
    # year, so the previous-year portion of the window is clamped away.
    today = date(2026, 1, 5)  # Monday; window reaches back into Dec 2025
    expected = _expected_window(today, HOLIDAYS)
    assert expected[0].year == 2025  # confirms the window does straddle Jan 1
    current_year_only = {d for d in expected if d.year == 2026}
    assert current_year_only  # fixture sanity: some current-year days exist
    assert missing_days(current_year_only, today, HOLIDAYS) == []
