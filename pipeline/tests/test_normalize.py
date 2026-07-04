from pathlib import Path

import pandas as pd

from pipeline import config
from pipeline.normalize import normalize_equity_bhavcopy

FIX = Path(__file__).parent / "fixtures"


def test_filters_to_eq_stk_and_maps_columns():
    raw = pd.read_csv(FIX / "bhavcopy_normal.csv")
    out = normalize_equity_bhavcopy(raw)
    assert list(out.columns) == config.CANON_COLUMNS
    # BE row (HDFCBANK) dropped; only RELIANCE + INFY remain
    assert set(out["symbol"]) == {"RELIANCE", "INFY"}
    r = out[out["symbol"] == "RELIANCE"].iloc[0]
    assert r["instrument_key"] == "INE002A01018"
    assert r["close"] == 3000.0
    assert r["series"] == "EQ"
    assert r["source"] == "nse-udiff"


def test_keeps_only_final_session_rows():
    raw = pd.read_csv(FIX / "bhavcopy_mixed_session.csv")
    out = normalize_equity_bhavcopy(raw)
    # Two RELIANCE rows in (I1 pre-open + F1 final); only the F1 final survives.
    assert len(out) == 1
    assert out.iloc[0]["close"] == 3000.0
    assert out.iloc[0]["volume"] == 1_000_000
