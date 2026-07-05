from pathlib import Path

import pandas as pd
import pytest

from pipeline import config
from pipeline.normalize import normalize_equity_bhavcopy

FIX = Path(__file__).parent / "fixtures"


def test_filters_to_stk_and_maps_columns():
    raw = pd.read_csv(FIX / "bhavcopy_normal.csv")
    out = normalize_equity_bhavcopy(raw)
    assert list(out.columns) == config.CANON_COLUMNS
    # Spec change (G1b task 4): the SctySrs == "EQ" filter is DROPPED — all cash
    # series (STK + F-session) survive with their own `series` value, so the BE
    # row (HDFCBANK) is now KEPT alongside RELIANCE + INFY.
    assert set(out["symbol"]) == {"RELIANCE", "INFY", "HDFCBANK"}
    r = out[out["symbol"] == "RELIANCE"].iloc[0]
    assert r["instrument_key"] == "INE002A01018"
    assert r["close"] == 3000.0
    assert r["series"] == "EQ"
    assert r["source"] == "nse-udiff"
    assert pd.api.types.is_datetime64_any_dtype(out["date"])
    be = out[out["symbol"] == "HDFCBANK"].iloc[0]
    assert be["series"] == "BE"
    assert be["instrument_key"] == "INE040A01034"


def _stk_row(isin: str, symbol: str, series: str) -> dict:
    return {
        "TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": isin, "TckrSymb": symbol,
        "SctySrs": series, "SsnId": "F1", "OpnPric": 100, "HghPric": 101, "LwPric": 99,
        "ClsPric": 100, "PrvsClsgPric": 100, "TtlTradgVol": 1, "TtlTrfVal": 1,
        "TtlNbOfTxsExctd": 1,
    }


def test_multi_series_rows_all_survive_with_their_series():
    # A widened-universe day: STK + final-session rows across several cash
    # series (EQ, BE, BZ) must ALL survive normalization, each tagged with its
    # own `series` value -- this is the core Task 4 spec change.
    raw = pd.DataFrame([
        _stk_row("INE001", "A", "EQ"),
        _stk_row("INE002", "B", "BE"),
        _stk_row("INE003", "C", "BZ"),
    ])
    out = normalize_equity_bhavcopy(raw)
    assert set(out["symbol"]) == {"A", "B", "C"}
    assert dict(zip(out["symbol"], out["series"], strict=True)) == {
        "A": "EQ", "B": "BE", "C": "BZ",
    }


def test_null_isin_row_gets_nse_sentinel_key_and_survives():
    raw = pd.DataFrame([_stk_row(None, "NEWCO", "EQ")])
    out = normalize_equity_bhavcopy(raw)
    assert len(out) == 1
    assert out.iloc[0]["instrument_key"] == "NSE:NEWCO"
    assert out.iloc[0]["symbol"] == "NEWCO"


def test_empty_isin_row_gets_nse_sentinel_key_and_survives():
    raw = pd.DataFrame([_stk_row("", "NEWCO2", "EQ")])
    out = normalize_equity_bhavcopy(raw)
    assert len(out) == 1
    assert out.iloc[0]["instrument_key"] == "NSE:NEWCO2"


def test_both_isin_and_symbol_empty_yields_empty_key_for_quarantine():
    # Neither an ISIN nor a symbol to build a sentinel from: instrument_key
    # must come out empty so quarantine's key_ok check still rejects it (the
    # brief is explicit that this case must NOT auto-survive via a fake key).
    raw = pd.DataFrame([_stk_row("", "", "EQ")])
    out = normalize_equity_bhavcopy(raw)
    assert len(out) == 1
    assert out.iloc[0]["instrument_key"] == ""


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


def test_missing_required_column_raises():
    from pipeline.errors import UnexpectedFailure
    raw = pd.DataFrame([{"TradDt": "2026-07-03", "TckrSymb": "X"}])  # missing most cols
    with pytest.raises(UnexpectedFailure):
        normalize_equity_bhavcopy(raw)
