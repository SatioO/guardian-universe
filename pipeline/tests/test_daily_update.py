import dataclasses
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipeline import config, datasets, store
from pipeline.daily_update import RunStatus, run_daily
from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.fetch import Fetcher

HOLIDAYS = {date(2026, 8, 15)}
RAW = pd.read_csv(Path(__file__).parent / "fixtures" / "bhavcopy_normal.csv")


def datasets_spec(base):
    # abs_rowcount_range is re-read from config (not just base_dir) so that
    # tests monkeypatching config.ROWCOUNT_ABS_RANGE still take effect — the
    # spec field is otherwise frozen at datasets.py import time.
    return dataclasses.replace(
        datasets.EQUITIES, base_dir=base, abs_rowcount_range=config.ROWCOUNT_ABS_RANGE
    )


class StubFetcher:
    def __init__(self, df: pd.DataFrame | None = None, exc: Exception | None = None):
        self._df, self._exc = df, exc

    def fetch_raw(self, d: date) -> pd.DataFrame:
        if self._exc is not None:
            raise self._exc
        assert self._df is not None
        return self._df


def _run(target: date, fetcher: Fetcher, base: Path) -> RunStatus:
    return run_daily(datasets_spec(base), target, fetcher=fetcher, holidays=HOLIDAYS)


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


def test_unexpected_failure_is_reported_not_raised(tmp_path: Path):
    st = _run(date(2026, 7, 3), StubFetcher(exc=UnexpectedFailure("timeout")), tmp_path)
    assert st.status == "failed"


def test_schema_failure_returns_failed_without_appending(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    # This row passes the STK + EQ + F-session filters AND quarantine (valid
    # prices/volume/instrument_key) but has a NULL symbol, which the schema gate
    # (symbol non-nullable) must reject. run_daily must return "failed" WITHOUT
    # appending (fail-closed) — not raise.
    bad_symbol = pd.DataFrame([{
        "TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": "INE002A01018",
        "TckrSymb": None, "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 2990,
        "HghPric": 3010, "LwPric": 2985, "ClsPric": 3000, "PrvsClsgPric": 2980,
        "TtlTradgVol": 1000000, "TtlTrfVal": 3000000000, "TtlNbOfTxsExctd": 50000,
    }])
    st = _run(date(2026, 7, 3), StubFetcher(bad_symbol), tmp_path)
    assert st.status == "failed"
    assert not store.has_day(tmp_path, date(2026, 7, 3))  # nothing written


def base_year(base: Path) -> Path:
    from pipeline import config
    return config.ohlc_path(2026, base)


def _canon_rows(d: str, n: int) -> pd.DataFrame:
    return pd.DataFrame([{
        "date": pd.Timestamp(d), "instrument_key": f"K{i:05d}", "isin": f"K{i:05d}",
        "symbol": f"S{i}", "series": "EQ",
        "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "prevclose": 100.0,
        "volume": 1, "value": 1.0, "trades": 1, "source": "seed",
    } for i in range(n)])[config.CANON_COLUMNS]


def test_all_rows_quarantined_returns_failed(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    # Every row has open<=0 -> all quarantined -> clean empty. Must be "failed"
    # (not a success no-op) and nothing written, so the day can be retried.
    bad = pd.DataFrame([{
        "TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": f"K{i}", "TckrSymb": f"S{i}",
        "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 0, "HghPric": 10, "LwPric": 0,
        "ClsPric": 5, "PrvsClsgPric": 5, "TtlTradgVol": 1, "TtlTrfVal": 1,
        "TtlNbOfTxsExctd": 1,
    } for i in range(3)])
    st = _run(date(2026, 7, 3), StubFetcher(bad), tmp_path)
    assert st.status == "failed"
    assert not store.has_day(tmp_path, date(2026, 7, 3))


def test_malformed_raw_is_reported_not_raised(tmp_path: Path):
    # A fetched frame missing required UDiFF columns must yield "failed", not raise.
    malformed = pd.DataFrame([{"TradDt": "2026-07-03", "TckrSymb": "X"}])
    st = _run(date(2026, 7, 3), StubFetcher(malformed), tmp_path)
    assert st.status == "failed"
    assert not store.has_day(tmp_path, date(2026, 7, 3))


def test_store_error_is_reported_not_raised(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    def _boom(*_a, **_k):
        raise OSError("disk full")
    monkeypatch.setattr(store, "append_day", _boom)
    st = _run(date(2026, 7, 3), StubFetcher(RAW), tmp_path)
    assert st.status == "failed"


def test_deviation_gate_fires_from_stored_trailing_window(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    # Seed the 3 trading days before Fri 2026-07-03 each with 100 rows (mean=100).
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):
        store.append_day(_canon_rows(d, 100), tmp_path)
    # The target-day fetch yields only 2 rows -> ~98% deviation from mean 100 ->
    # the deviation gate (fed by _trailing_counts over the real stored window)
    # must fire end-to-end -> "failed", nothing written.
    st = _run(date(2026, 7, 3), StubFetcher(RAW), tmp_path)
    assert st.status == "failed"
    assert not store.has_day(tmp_path, date(2026, 7, 3))
