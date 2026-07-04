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


def is_trading_day(d: date, holidays: set[date]) -> bool:
    if d.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    return d not in holidays


def previous_trading_day(d: date, holidays: set[date]) -> date:
    cur = d - timedelta(days=1)
    while not is_trading_day(cur, holidays):
        cur -= timedelta(days=1)
    return cur


def trading_days_back(end: date, n: int, holidays: set[date]) -> list[date]:
    """The `n` trading days ending at `end`, ascending. `end` need not be a
    trading day; if it isn't, counting starts from the previous trading day."""
    days: list[date] = []
    cur = end if is_trading_day(end, holidays) else previous_trading_day(end, holidays)
    while len(days) < n:
        days.append(cur)
        cur = previous_trading_day(cur, holidays)
    return sorted(days)
