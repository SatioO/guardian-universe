import pandas as pd
import pytest

from pipeline import config
from pipeline.errors import UnexpectedFailure
from pipeline.validate import check_rowcount, quarantine_bad_rows


def test_rowcount_within_range_and_stable_passes():
    check_rowcount(2000, [1990, 2010, 2005])  # no raise


def test_rowcount_below_absolute_floor_fails():
    with pytest.raises(UnexpectedFailure):
        check_rowcount(1500, [1990, 2010])


def test_rowcount_deviation_over_threshold_fails():
    # count 1800 is INSIDE the abs range [1800, 3000] but deviates ~18% (400/2200)
    # from the trailing mean of 2200 -> must fail on the DEVIATION check, not abs-range.
    with pytest.raises(UnexpectedFailure):
        check_rowcount(1800, [2200, 2200, 2200])


def test_rowcount_above_absolute_ceiling_fails():
    with pytest.raises(UnexpectedFailure):
        check_rowcount(3500, [2000, 2000])


def test_rowcount_nonpositive_trailing_mean_fails():
    # trailing present but all-zero mean -> fail-closed, not a silent skip.
    with pytest.raises(UnexpectedFailure):
        check_rowcount(2000, [0, 0, 0])


def test_rowcount_empty_trailing_uses_abs_range_only():
    check_rowcount(1900, [])  # no raise (within 1800..3000)


def _row(**over) -> dict:
    base = {
        "date": pd.Timestamp("2026-07-03"),
        "instrument_key": "INE002A01018", "isin": "INE002A01018",
        "symbol": "RELIANCE", "series": "EQ",
        "open": 2990.0, "high": 3010.0, "low": 2985.0, "close": 3000.0,
        "prevclose": 2980.0, "volume": 1000, "value": 1.0, "trades": 10,
        "source": "nse-udiff",
    }
    base.update(over)
    return base


def test_quarantine_separates_bad_rows():
    df = pd.DataFrame([
        _row(symbol="GOOD"),
        _row(symbol="NEGVOL", volume=-1),
        _row(symbol="HILO", high=10.0, low=20.0),
        _row(symbol="CLOSEOOB", close=9999.0),
        _row(symbol="NOKEY", instrument_key=None),
        _row(symbol="EMPTYKEY", instrument_key=""),
    ])[config.CANON_COLUMNS]
    clean, bad = quarantine_bad_rows(df)
    assert set(clean["symbol"]) == {"GOOD"}
    assert set(bad["symbol"]) == {"NEGVOL", "HILO", "CLOSEOOB", "NOKEY", "EMPTYKEY"}


def test_quarantine_handles_empty_frame():
    empty = pd.DataFrame([_row()])[config.CANON_COLUMNS].iloc[0:0]
    clean, bad = quarantine_bad_rows(empty)
    assert len(clean) == 0
    assert len(bad) == 0
