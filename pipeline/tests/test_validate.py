import pandas as pd
import pytest

from pipeline import config
from pipeline.errors import UnexpectedFailure
from pipeline.validate import check_rowcount_by_series, quarantine_bad_rows


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


# --- check_rowcount_by_series (Task 4: per-series gate) ---------------------


def test_by_series_total_outside_abs_range_fails():
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            1000, {"EQ": 1000}, {"EQ": [1000, 1000]}, abs_range=(2000, 10000)
        )


def test_by_series_total_within_abs_range_and_stable_passes():
    check_rowcount_by_series(
        2500, {"EQ": 2500}, {"EQ": [2500, 2500, 2500]}, abs_range=(2000, 10000)
    )  # no raise


def test_by_series_uses_config_abs_range_by_default(monkeypatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (100, 200))
    check_rowcount_by_series(150, {"EQ": 150}, {"EQ": [150]})  # no raise
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(50, {"EQ": 50}, {"EQ": [150]})


def test_by_series_deviation_over_threshold_fails():
    # EQ deviates ~50% from its trailing mean of 2000 -> must fail even though
    # the total is within abs_range.
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            3000, {"EQ": 1000, "BE": 2000}, {"EQ": [2000, 2000, 2000], "BE": [2000, 2000]},
            abs_range=(2000, 10000),
        )


def test_by_series_new_series_with_no_trailing_passes():
    # A series with an empty trailing list is "new today" -- it accumulates
    # history instead of failing.
    check_rowcount_by_series(
        2100, {"EQ": 2000, "BE": 100}, {"EQ": [2000, 2000], "BE": []}, abs_range=(2000, 10000)
    )  # no raise


def test_by_series_major_series_vanished_fails():
    # BE had a trailing mean >= 50 but is absent from today's series_counts ->
    # a truncation signal, must fail even though EQ alone is fine.
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            2000, {"EQ": 2000}, {"EQ": [2000, 2000], "BE": [60, 55, 58]},
            abs_range=(2000, 10000),
        )


def test_by_series_minor_series_vanished_passes():
    # A series with trailing mean < 50 disappearing is not treated as a
    # truncation signal.
    check_rowcount_by_series(
        2000, {"EQ": 2000}, {"EQ": [2000, 2000], "BZ": [3, 4, 2]}, abs_range=(2000, 10000)
    )  # no raise


def test_migration_widened_day_against_eq_only_trailing_passes():
    # The core migration property: a widened day (~2x rows via new series,
    # EQ itself roughly stable) validated against EQ-ONLY trailing history
    # (pre-migration data) must PASS.
    trailing = {"EQ": [2000, 2010, 1995]}  # EQ-only history, no BE/BZ trailing yet
    today_series = {"EQ": 2005, "BE": 1800, "BZ": 200}  # ~4000 total, new series
    total = sum(today_series.values())
    check_rowcount_by_series(total, today_series, trailing, abs_range=(2000, 10000))  # no raise


def test_migration_truncated_eq_file_fails():
    # A truncated file: EQ count halved relative to EQ-only trailing history.
    # Even with new series present, the EQ deviation must trip the gate.
    trailing = {"EQ": [2000, 2010, 1995]}
    today_series = {"EQ": 1000, "BE": 1800, "BZ": 200}  # EQ halved
    total = sum(today_series.values())
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(total, today_series, trailing, abs_range=(2000, 10000))
