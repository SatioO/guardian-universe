from datetime import date

from pipeline.freshness import is_stale

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
