from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipeline import config
from pipeline.daily_update import RunStatus, run_daily
from pipeline.errors import NotYetPublished
from pipeline.fetch import Fetcher

HOLIDAYS = {date(2026, 8, 15)}
RAW = pd.read_csv(Path(__file__).parent / "fixtures" / "bhavcopy_normal.csv")


class StubFetcher:
    def __init__(self, df: pd.DataFrame | None = None, exc: Exception | None = None):
        self._df, self._exc = df, exc

    def fetch_raw(self, d: date) -> pd.DataFrame:
        if self._exc is not None:
            raise self._exc
        assert self._df is not None
        return self._df


def _run(target: date, fetcher: Fetcher, base: Path) -> RunStatus:
    return run_daily(target, fetcher=fetcher, holidays=HOLIDAYS, base=base)


def test_normal_day_ingests_and_persists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    st = _run(date(2026, 7, 3), StubFetcher(RAW), tmp_path)
    assert st.status == "success"
    assert st.symbol_count == 2  # RELIANCE + INFY (BE row filtered out)
    out = pd.read_parquet(base_year(tmp_path))
    assert set(out["symbol"]) == {"RELIANCE", "INFY"}


def test_holiday_skips_cleanly_without_fetching(tmp_path: Path):
    st = _run(date(2026, 8, 15), StubFetcher(exc=AssertionError("must not fetch")), tmp_path)
    assert st.status == "skipped_holiday"


def test_idempotent_rerun_is_a_noop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    _run(date(2026, 7, 3), StubFetcher(RAW), tmp_path)
    st = _run(date(2026, 7, 3), StubFetcher(exc=AssertionError("must not refetch")), tmp_path)
    assert st.status == "skipped_idempotent"


def test_not_yet_published_is_reported_not_raised(tmp_path: Path):
    st = _run(date(2026, 7, 3), StubFetcher(exc=NotYetPublished("404")), tmp_path)
    assert st.status == "not_yet"


def base_year(base: Path) -> Path:
    from pipeline import config
    return config.ohlc_path(2026, base)
