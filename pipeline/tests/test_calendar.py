from datetime import date
from pathlib import Path

import pytest

from pipeline import calendar as cal
from tests import fakes


@pytest.fixture
def holidays(tmp_path: Path) -> set[date]:
    fakes.write_market_calendar(tmp_path, nse_closures=["2026-01-26", "2026-08-15"])
    return cal.load_trading_calendar(tmp_path)[0]


def test_weekend_is_not_a_trading_day(holidays: set[date]):
    assert cal.is_trading_day(date(2026, 7, 4), holidays) is False  # Saturday
    assert cal.is_trading_day(date(2026, 7, 5), holidays) is False  # Sunday


def test_holiday_is_not_a_trading_day(holidays: set[date]):
    assert cal.is_trading_day(date(2026, 1, 26), holidays) is False


def test_normal_weekday_is_a_trading_day(holidays: set[date]):
    assert cal.is_trading_day(date(2026, 7, 3), holidays) is True  # Friday


def test_previous_trading_day_skips_weekend(holidays: set[date]):
    # Monday 2026-07-06 -> previous trading day is Friday 2026-07-03
    assert cal.previous_trading_day(date(2026, 7, 6), holidays) == date(2026, 7, 3)


def test_trading_days_back_counts_trading_days_only(holidays: set[date]):
    days = cal.trading_days_back(date(2026, 7, 3), 3, holidays)
    assert days == [date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)]


def test_previous_trading_day_skips_holiday(holidays: set[date]):
    # Tue 2026-01-27: the previous weekday is Mon 2026-01-26 (a holiday) -> skip it
    # and the weekend -> Fri 2026-01-23.
    assert cal.previous_trading_day(date(2026, 1, 27), holidays) == date(2026, 1, 23)


def test_trading_days_back_when_end_is_non_trading_day(holidays: set[date]):
    # Sat 2026-07-04 is non-trading; counting starts from Fri 2026-07-03.
    days = cal.trading_days_back(date(2026, 7, 4), 2, holidays)
    assert days == [date(2026, 7, 2), date(2026, 7, 3)]


def test_trading_days_back_rejects_non_positive_n(holidays: set[date]):
    with pytest.raises(ValueError):
        cal.trading_days_back(date(2026, 7, 3), 0, holidays)


def test_special_session_overrides_weekend_and_holiday(tmp_path):
    from datetime import date

    from pipeline import calendar as cal

    muhurat = date(2026, 11, 8)  # a Sunday
    assert not cal.is_trading_day(muhurat, set())
    assert cal.is_trading_day(muhurat, set(), special_sessions={muhurat})
    assert cal.is_trading_day(muhurat, {muhurat}, special_sessions={muhurat})  # beats holiday too

    # A listed session trades even before its hours are known.
    fakes.write_market_calendar(tmp_path, nse_sessions=[{"date": "2026-11-08", "label": "muhurat"}])
    assert cal.load_trading_calendar(tmp_path) == (set(), {muhurat})


def test_the_trading_calendar_is_the_one_market_calendar_file(tmp_path: Path):
    from pipeline import market_calendar

    with pytest.raises(market_calendar.CalendarError, match="missing"):
        cal.load_trading_calendar(tmp_path)
    # The committed file: NSE's closures are the pipeline's holidays, and a
    # Muhurat day listed as a closure still trades.
    holidays, special = cal.load_trading_calendar(Path(__file__).parents[1] / "data" / "meta")
    assert date(2026, 10, 2) in holidays
    assert cal.is_trading_day(date(2025, 10, 21), holidays, special)
    assert cal.is_trading_day(date(2026, 2, 1), holidays, special)  # Budget Sunday


def test_previous_trading_day_sees_special_session():
    from datetime import date

    from pipeline import calendar as cal

    muhurat = date(2026, 11, 8)  # Sunday
    monday = date(2026, 11, 9)
    assert cal.previous_trading_day(monday, set()) == date(2026, 11, 6)
    assert cal.previous_trading_day(monday, set(), special_sessions={muhurat}) == muhurat
