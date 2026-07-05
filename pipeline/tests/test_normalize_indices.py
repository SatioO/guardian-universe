import pandas as pd
import pytest

from pipeline import config
from pipeline.errors import UnexpectedFailure
from pipeline.normalize_indices import normalize_indices


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
