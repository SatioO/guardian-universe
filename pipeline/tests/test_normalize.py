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
    assert pd.api.types.is_datetime64_any_dtype(out["date"])


def test_keeps_only_final_session_rows():
    raw = pd.read_csv(FIX / "bhavcopy_mixed_session.csv")
    out = normalize_equity_bhavcopy(raw)
    # Two RELIANCE rows in (I1 pre-open + F1 final); only the F1 final survives.
    assert len(out) == 1
    assert out.iloc[0]["close"] == 3000.0
    assert out.iloc[0]["volume"] == 1_000_000


def test_drops_non_stk_instrument_type():
    # A non-STK instrument (e.g. a debenture) that passes the EQ + F-session
    # filters must still be dropped by the FinInstrmTp == 'STK' predicate.
    raw = pd.DataFrame([
        {"TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": "INE002A01018",
         "TckrSymb": "RELIANCE", "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 2990,
         "HghPric": 3010, "LwPric": 2985, "ClsPric": 3000, "PrvsClsgPric": 2980,
         "TtlTradgVol": 1000000, "TtlTrfVal": 3000000000, "TtlNbOfTxsExctd": 50000},
        {"TradDt": "2026-07-03", "FinInstrmTp": "DB", "ISIN": "INE000000001",
         "TckrSymb": "SOMEBOND", "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 100,
         "HghPric": 101, "LwPric": 99, "ClsPric": 100, "PrvsClsgPric": 100,
         "TtlTradgVol": 5, "TtlTrfVal": 500, "TtlNbOfTxsExctd": 2},
    ])
    out = normalize_equity_bhavcopy(raw)
    assert set(out["symbol"]) == {"RELIANCE"}  # DB row dropped despite EQ + F1


def test_colmap_keys_are_valid_udiff_columns():
    from pipeline.normalize import _COLMAP
    from pipeline.sources.nse_udiff import UDIFF_COLUMNS
    # Every raw column the normalizer maps must exist in the source's declared
    # column set — a rename/removal upstream then fails loudly here.
    assert set(_COLMAP).issubset(set(UDIFF_COLUMNS))
