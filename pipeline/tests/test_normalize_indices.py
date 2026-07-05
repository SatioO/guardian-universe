import pandas as pd
import pytest

from pipeline import config, validate
from pipeline.errors import UnexpectedFailure
from pipeline.normalize_indices import normalize_indices
from pipeline.schema import validate_ohlc


def _raw() -> pd.DataFrame:
    return pd.DataFrame({
        "Index Name": ["Nifty 50 ", "Nifty Bank"],
        "Index Date": ["03-07-2026", "03-07-2026"],
        "Open Index Value": [24500.10, 52000.00],
        "High Index Value": [24700.55, 52500.00],
        "Low Index Value": [24450.00, 51800.00],
        "Closing Index Value": [24650.25, 52300.75],
        "Points Change": [150.15, 300.75],
        "Volume": [350000000.0, float("nan")],
        "Turnover (Rs. Cr.)": [45000.50, float("nan")],
    })


def _raw_with_close_only_row() -> pd.DataFrame:
    # Mirrors the real ind_close_all_03072026.csv shape: a normal full-OHLC
    # row, a close-only (TRI/G-Sec) row publishing literal "-" for
    # Open/High/Low AND Volume/Turnover, and a row with "-" Volume/Turnover
    # only (OHLC present) -- both patterns confirmed present in the real file
    # (22 rows OHLC "-", 23 rows Volume/Turnover "-").
    return pd.DataFrame({
        "Index Name": ["Nifty 50 ", "Nifty50 TRI", "Nifty Bank"],
        "Index Date": ["03-07-2026", "03-07-2026", "03-07-2026"],
        "Open Index Value": [24500.10, "-", 52000.00],
        "High Index Value": [24700.55, "-", 52500.00],
        "Low Index Value": [24450.00, "-", 51800.00],
        "Closing Index Value": [24650.25, 39250.40, 52300.75],
        "Points Change": [150.15, 210.30, 300.75],
        "Volume": [350000000.0, "-", "-"],
        "Turnover (Rs. Cr.)": [45000.50, "-", "-"],
    })


def test_normalize_indices_canonical():
    df = normalize_indices(_raw())
    assert list(df.columns) == config.CANON_COLUMNS
    assert df["series"].tolist() == ["INDEX", "INDEX"]
    assert df["instrument_key"].tolist() == ["IDX:NIFTY50", "IDX:NIFTYBANK"]
    assert df["symbol"].tolist() == ["Nifty 50", "Nifty Bank"]
    assert df["isin"].tolist() == ["", ""]
    assert df["trades"].tolist() == [0, 0]
    assert df["volume"].tolist() == [350000000, 0]
    assert df["prevclose"].iloc[0] == pytest.approx(24650.25 - 150.15)
    assert str(df["date"].iloc[0])[:10] == "2026-07-03"
    assert df["source"].tolist() == ["nse-indices", "nse-indices"]
    assert df["volume"].dtype == "int64" and df["trades"].dtype == "int64"


def test_normalize_indices_missing_column_fails_loud():
    with pytest.raises(UnexpectedFailure, match="missing"):
        normalize_indices(_raw().drop(columns=["Points Change"]))


def test_normalize_indices_iso_date_fails():
    # Index Date must be strict DD-MM-YYYY; an ISO-formatted date must not
    # silently parse (pd.to_datetime with format=... raises ValueError).
    raw = _raw()
    raw["Index Date"] = ["2026-07-03", "2026-07-03"]
    with pytest.raises(ValueError):
        normalize_indices(raw)


def test_negative_prevclose_index_row_is_quarantined_not_fatal():
    # Points Change > close is possible on small-base indices, driving
    # prevclose negative. Build a normalized-indices-shaped frame (one normal
    # row, one with prevclose < 0) and push it through the same
    # quarantine -> schema path run_daily uses, to prove the bad row is
    # dropped by quarantine_bad_rows BEFORE validate_ohlc ever sees it.
    raw = _raw()
    raw["Points Change"] = [150.15, 60000.0]  # row 1: Points Change > close -> prevclose < 0
    df = normalize_indices(raw)
    assert df["prevclose"].iloc[1] < 0

    clean, bad = validate.quarantine_bad_rows(df)

    assert df["symbol"].iloc[1] in set(bad["symbol"])
    assert set(clean["symbol"]) == {df["symbol"].iloc[0]}
    assert (clean["prevclose"] > 0).all()

    validate_ohlc(clean)  # must not raise: bad row already quarantined out


def test_close_only_index_rows_normalize_to_flat_bars():
    # Real NSE ind_close_all publishes literal "-" for OHLC on total-return /
    # fixed-income indices (TRI, G-Sec) that don't trade intraday, and "-" for
    # Volume/Turnover on plenty of rows besides. Both must normalize instead
    # of blowing up the whole day's ingest.
    df = normalize_indices(_raw_with_close_only_row())

    # Row 1: close-only index -- open/high/low fall back to close (flat bar);
    # volume/turnover fall back to 0.
    tri = df.iloc[1]
    assert tri["open"] == pytest.approx(39250.40)
    assert tri["high"] == pytest.approx(39250.40)
    assert tri["low"] == pytest.approx(39250.40)
    assert tri["close"] == pytest.approx(39250.40)
    assert tri["volume"] == 0
    assert tri["value"] == pytest.approx(0.0)

    # Row 2: normal OHLC, but "-" Volume/Turnover -> 0.
    bank = df.iloc[2]
    assert bank["open"] == pytest.approx(52000.00)
    assert bank["volume"] == 0
    assert bank["value"] == pytest.approx(0.0)

    # Row 0: untouched full row still parses normally.
    assert df.iloc[0]["volume"] == 350000000

    assert df["volume"].dtype == "int64"
    assert df["value"].dtype == float

    # The full gate path run_daily runs: quarantine then schema validation.
    clean, bad = validate.quarantine_bad_rows(df)
    assert len(bad) == 0, f"unexpected quarantined rows: {bad['symbol'].tolist()}"
    assert len(clean) == len(df)
    validate_ohlc(clean)  # must not raise


def test_close_only_index_strict_close_still_fails_loud():
    # Closing Index Value is never "-" in the real file for any of the 160
    # rows; if it ever were, that row is genuinely unusable (no price at
    # all) and must fail loudly rather than silently coerce to NaN/0.
    raw = _raw_with_close_only_row()
    raw["Closing Index Value"] = [24650.25, "-", 52300.75]
    with pytest.raises(ValueError, match="could not convert string to float"):
        normalize_indices(raw)
