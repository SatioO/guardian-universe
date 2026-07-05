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
