import dataclasses
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config, datasets, store
from pipeline.backfill import backfill

HOLIDAYS: set[date] = set()
RAW = pd.read_csv(Path(__file__).parent / "fixtures" / "bhavcopy_normal.csv")


def datasets_spec(base):
    # abs_rowcount_range is re-read from config (not just base_dir) so that
    # tests monkeypatching config.ROWCOUNT_ABS_RANGE still take effect — the
    # spec field is otherwise frozen at datasets.py import time.
    return dataclasses.replace(
        datasets.EQUITIES, base_dir=base, abs_rowcount_range=config.ROWCOUNT_ABS_RANGE
    )


class StubFetcher:
    def __init__(self):
        self.dates: list[date] = []

    def fetch_raw(self, d: date) -> pd.DataFrame:
        self.dates.append(d)
        # Tag rows with the target date so each day is stored under its own date,
        # matching run_daily's idempotency check (has_day uses the stored TradDt).
        df = RAW.copy()
        df["TradDt"] = d.isoformat()
        return df


def _no_sleep(_s: float) -> None:
    return None


def test_backfill_ingests_n_trading_days(tmp_path: Path, monkeypatch):
    from pipeline import config
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    f = StubFetcher()
    out = backfill(datasets_spec(tmp_path), date(2026, 7, 3), 3, fetcher=f,
                   holidays=HOLIDAYS, sleep=_no_sleep)
    assert [s.status for s in out] == ["success", "success", "success"]
    # exact 3 trading days ending 2026-07-03 (Fri), ascending — pins the window,
    # not just non-decreasing order.
    assert f.dates == [date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)]


def test_backfill_is_resumable(tmp_path: Path, monkeypatch):
    from pipeline import config
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    backfill(datasets_spec(tmp_path), date(2026, 7, 3), 3, fetcher=StubFetcher(),
             holidays=HOLIDAYS, sleep=_no_sleep)
    # Second run: every day already present -> idempotent skips, no refetch.
    f2 = StubFetcher()
    out = backfill(datasets_spec(tmp_path), date(2026, 7, 3), 3, fetcher=f2,
                   holidays=HOLIDAYS, sleep=_no_sleep)
    assert [s.status for s in out] == ["skipped_idempotent"] * 3
    assert f2.dates == []
    for d in (date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)):
        assert store.has_day(tmp_path, d)  # all 3 days persisted, not just the last
