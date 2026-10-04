"""Trading-calendar logic. Pure; holidays are injected as a set of dates."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from pipeline import market_calendar


def load_trading_calendar(meta_dir: Path) -> tuple[set[date], set[date]]:
    """(holidays, special_sessions) of NSE, the pipeline's trading calendar,
    from the one market calendar file (market_calendar.json): its closures,
    and its sessions, which trade despite a weekend or a holiday (Muhurat)."""
    return market_calendar.load(meta_dir / market_calendar.FILENAME).trading_inputs("NSE")


def is_trading_day(
    d: date, holidays: set[date], special_sessions: set[date] | None = None
) -> bool:
    if special_sessions and d in special_sessions:
        return True
    if d.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    return d not in holidays


def previous_trading_day(
    d: date, holidays: set[date], special_sessions: set[date] | None = None
) -> date:
    """The trading day immediately before `d`. Assumes `holidays` is sparse
    (never covers every weekday indefinitely), which always holds for a real
    exchange calendar."""
    cur = d - timedelta(days=1)
    while not is_trading_day(cur, holidays, special_sessions):
        cur -= timedelta(days=1)
    return cur


def trading_days_back(
    end: date, n: int, holidays: set[date], special_sessions: set[date] | None = None
) -> list[date]:
    """The `n` trading days ending at `end`, ascending. `end` need not be a
    trading day; if it isn't, counting starts from the previous trading day."""
    if n <= 0:
        raise ValueError(f"n must be positive, got {n}")
    days: list[date] = []
    cur = (
        end
        if is_trading_day(end, holidays, special_sessions)
        else previous_trading_day(end, holidays, special_sessions)
    )
    while len(days) < n:
        days.append(cur)
        cur = previous_trading_day(cur, holidays, special_sessions)
    return sorted(days)
