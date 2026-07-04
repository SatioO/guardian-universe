from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config
from pipeline.store import append_day, has_day, read_trailing_window


def _day(d: str, close: float, key: str = "INE002A01018") -> pd.DataFrame:
    return pd.DataFrame([{
        "date": pd.Timestamp(d), "instrument_key": key, "isin": key,
        "symbol": "RELIANCE", "series": "EQ",
        "open": close, "high": close, "low": close, "close": close,
        "prevclose": close, "volume": 1, "value": 1.0, "trades": 1,
        "source": "nse-udiff",
    }])[config.CANON_COLUMNS]


def test_append_creates_year_file_and_has_day(tmp_path: Path):
    assert has_day(tmp_path, date(2026, 7, 3)) is False
    append_day(_day("2026-07-03", 3000), tmp_path)
    assert config.ohlc_path(2026, tmp_path).exists()
    assert has_day(tmp_path, date(2026, 7, 3)) is True


def test_day_symbol_count(tmp_path: Path):
    from pipeline.store import day_symbol_count
    assert day_symbol_count(tmp_path, date(2026, 7, 3)) == 0
    append_day(_day("2026-07-03", 3000, "INE002A01018"), tmp_path)
    append_day(_day("2026-07-03", 1500, "INE009A01021"), tmp_path)
    assert day_symbol_count(tmp_path, date(2026, 7, 3)) == 2


def test_reappending_same_day_dedupes_keep_last(tmp_path: Path):
    append_day(_day("2026-07-03", 3000), tmp_path)
    append_day(_day("2026-07-03", 3050), tmp_path)  # corrected value, same (date,key)
    out = pd.read_parquet(config.ohlc_path(2026, tmp_path))
    assert len(out) == 1
    assert out.iloc[0]["close"] == 3050.0


def test_trailing_window_reads_across_year_boundary(tmp_path: Path):
    append_day(_day("2025-12-31", 100), tmp_path)
    append_day(_day("2026-01-01", 101), tmp_path)
    append_day(_day("2026-01-02", 102), tmp_path)
    out = read_trailing_window(tmp_path, date(2026, 1, 2), 2)
    assert sorted(out["close"]) == [101.0, 102.0]  # last 2 dates, spanning files


def test_append_day_routes_rows_spanning_two_years_in_one_call(tmp_path: Path):
    # A single append_day call whose DataFrame straddles a year boundary must
    # route each row to the correct year file (the point of groupby-by-year).
    df = pd.concat(
        [_day("2025-12-31", 100, "K1"), _day("2026-01-01", 101, "K1")],
        ignore_index=True,
    )
    append_day(df, tmp_path)
    assert config.ohlc_path(2025, tmp_path).exists()
    assert config.ohlc_path(2026, tmp_path).exists()
    y2025 = pd.read_parquet(config.ohlc_path(2025, tmp_path))
    y2026 = pd.read_parquet(config.ohlc_path(2026, tmp_path))
    assert list(y2025["close"]) == [100.0]
    assert list(y2026["close"]) == [101.0]


def test_trailing_window_when_prior_year_file_absent(tmp_path: Path):
    # end.year=2025 reads 2024 (absent) + 2025 (present); the empty prior-year
    # frame must be filtered out cleanly and the 2025 row returned.
    append_day(_day("2025-12-31", 100, "K1"), tmp_path)
    out = read_trailing_window(tmp_path, date(2025, 12, 31), 2)
    assert list(out["close"]) == [100.0]
