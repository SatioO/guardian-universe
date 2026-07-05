"""Trading-calendar logic. Pure; holidays are injected as a set of dates."""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path


def load_holidays(path: Path) -> set[date]:
    raw: dict[str, list[str]] = json.loads(path.read_text())
    out: set[date] = set()
    for _year, days in raw.items():
        for d in days:
            out.add(date.fromisoformat(d))
    return out


def load_special_sessions(path: Path) -> set[date]:
    """Special trading sessions (e.g. Muhurat) that trade despite weekend/holiday."""
    if not path.exists():
        return set()
    raw = json.loads(path.read_text())
    return {date.fromisoformat(s["date"]) for s in raw.get("sessions", [])}


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
