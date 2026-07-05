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
    hole = expected[0]  # a December day from the previous year
    present = set(expected)
    present.discard(hole)
    assert missing_days(present, today, HOLIDAYS) == [hole]


def test_missing_days_respects_custom_window_size():
    today = date(2026, 7, 6)
    expected5 = _expected_window(today, HOLIDAYS, window=5)
    assert len(expected5) == 5
    hole = expected5[0]
    present = set(expected5)
    present.discard(hole)
    assert missing_days(present, today, HOLIDAYS, window=5) == [hole]
    # The same hole outside a shorter window doesn't leak into a wider one's
    # absence check when the wider window's present set is fully seeded.
    expected10 = _expected_window(today, HOLIDAYS, window=10)
    assert missing_days(set(expected10), today, HOLIDAYS, window=10) == []
