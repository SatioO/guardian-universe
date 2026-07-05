import dataclasses
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config, datasets, store
from pipeline.backfill import backfill
from pipeline.errors import NotYetPublished
from pipeline.fetch import FetchResult

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

    def fetch_raw(self, d: date) -> FetchResult:
        self.dates.append(d)
        # Tag rows with the target date so each day is stored under its own date,
        # matching run_daily's idempotency check (has_day uses the stored TradDt).
        df = RAW.copy()
        df["TradDt"] = d.isoformat()
        return FetchResult(df, "nse-udiff")


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


class _RaisingOnDateFetcher:
    """Raises `NotYetPublished` for exactly one configured date; serves a
    normal one-day frame (tagged with the requested date, like `StubFetcher`)
    for every other date."""

    def __init__(self, raise_on: date):
        self._raise_on = raise_on
        self.dates: list[date] = []

    def fetch_raw(self, d: date) -> FetchResult:
        self.dates.append(d)
        if d == self._raise_on:
            raise NotYetPublished(f"404 for {d.isoformat()}")
        df = RAW.copy()
        df["TradDt"] = d.isoformat()
        return FetchResult(df, "nse-udiff")


def test_backfill_non_final_day_404_is_failed_not_not_yet(tmp_path: Path, monkeypatch):
    """Consistency fold with the catch-up loop (G2 Task 4): every backfill day
    except the most-recent/final one is, by construction, strictly in the
    past relative to `end` -- a 404 there means NSE's archive genuinely has a
    hole, not that the day is running late. Only the final day keeps
    `not_yet` lateness semantics."""
    from pipeline import config
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    # 3 trading days ending 2026-07-03 (Fri): 2026-07-01, 07-02, 07-03 (final).
    non_final_day = date(2026, 7, 1)
    f = _RaisingOnDateFetcher(raise_on=non_final_day)
    out = backfill(datasets_spec(tmp_path), date(2026, 7, 3), 3, fetcher=f,
                   holidays=HOLIDAYS, sleep=_no_sleep)
    by_date = {s.date: s for s in out}
    assert by_date[non_final_day].status == "failed"
    assert "archive missing for past trading day" in by_date[non_final_day].message
    assert by_date[date(2026, 7, 3)].status == "success"  # final day unaffected


def test_backfill_final_day_404_is_not_yet(tmp_path: Path, monkeypatch):
    """The final/most-recent backfill day keeps `not_yet` lateness semantics
    -- a 404 there is ordinary lateness (the bhavcopy for the most recent
    requested day just isn't out yet), same as the catch-up loop's target
    day."""
    from pipeline import config
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    final_day = date(2026, 7, 3)
    f = _RaisingOnDateFetcher(raise_on=final_day)
    out = backfill(datasets_spec(tmp_path), date(2026, 7, 3), 3, fetcher=f,
                   holidays=HOLIDAYS, sleep=_no_sleep)
    by_date = {s.date: s for s in out}
    assert by_date[final_day].status == "not_yet"
