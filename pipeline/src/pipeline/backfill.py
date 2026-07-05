"""One-time bootstrap: ingest a window of trading days via run_daily. Resumable."""
from __future__ import annotations

import time
from collections.abc import Callable
from datetime import date

from pipeline import calendar as cal
from pipeline.daily_update import RunStatus, run_daily
from pipeline.datasets import DatasetSpec
from pipeline.fetch import Fetcher


def backfill(
    spec: DatasetSpec,
    end: date,
    n: int,
    *,
    fetcher: Fetcher,
    holidays: set[date],
    special_sessions: set[date] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    delay_s: float = 1.0,
) -> list[RunStatus]:
    dates = cal.trading_days_back(end, n, holidays, special_sessions)
    results: list[RunStatus] = []
    for i, d in enumerate(dates):
        results.append(
            run_daily(
                spec, d, fetcher=fetcher, holidays=holidays, special_sessions=special_sessions
            )
        )
        if i < len(dates) - 1:
            sleep(delay_s)  # polite delay: NSE burst-blocks rapid archive requests
    return results
