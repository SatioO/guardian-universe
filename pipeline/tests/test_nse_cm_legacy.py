"""The legacy CM bhavcopy adapter: it has no NSE token, and says so."""
from __future__ import annotations

import pandas as pd

from pipeline.normalize import normalize_equity_bhavcopy
from pipeline.sources.nse_cm_legacy import legacy_to_udiff_shape


def _raw() -> pd.DataFrame:
    return pd.DataFrame([{
        "SYMBOL": "RELIANCE", "SERIES": "EQ", "OPEN": 2990.0, "HIGH": 3010.0,
        "LOW": 2985.0, "CLOSE": 3000.0, "PREVCLOSE": 2980.0, "TOTTRDQTY": 1000,
        "TOTTRDVAL": 3.0e6, "TIMESTAMP": "03-JUL-2022", "TOTALTRADES": 50,
        "ISIN": "INE002A01018",
    }])


def test_legacy_rows_normalize_with_a_null_nse_token():
    out = normalize_equity_bhavcopy(legacy_to_udiff_shape(_raw()), source="nse-cm-legacy")
    assert out.iloc[0]["instrument_key"] == "INE002A01018"
    assert pd.isna(out.iloc[0]["nse_token"])
    assert out.attrs["nse_token_nulled"] == 0
