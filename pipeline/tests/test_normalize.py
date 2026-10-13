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
        "TradDt": "2026-07-03", "FinInstrmTp": "STK", "FinInstrmId": 1000,
        "ISIN": isin, "TckrSymb": symbol,
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
        {"TradDt": "2026-07-03", "FinInstrmTp": "STK", "FinInstrmId": 2885, "ISIN": "INE002A01018",
         "TckrSymb": "RELIANCE", "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 2990,
         "HghPric": 3010, "LwPric": 2985, "ClsPric": 3000, "PrvsClsgPric": 2980,
         "TtlTradgVol": 1000000, "TtlTrfVal": 3000000000, "TtlNbOfTxsExctd": 50000},
        {"TradDt": "2026-07-03", "FinInstrmTp": "DB", "FinInstrmId": 1, "ISIN": "INE000000001",
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


def test_null_series_row_survives_as_empty_string_and_passes_validate_ohlc():
    # Live-data finding (Calibration fix 2): the real 2026-07-03 NSE bhavcopy,
    # post-universe-widening, has 5 STK/F-session rows with a NULL SctySrs.
    # Pre-widening the EQ filter dropped them silently; now they survive the
    # FinInstrmTp/session filters with series=NaN and validate_ohlc (series is
    # non-nullable) rejected the row -- failing the ENTIRE day. These rows must
    # be stored with series="" (consistent with the isin="" sentinel) instead.
    from pipeline.schema import validate_ohlc

    raw = pd.DataFrame([
        _stk_row("INE001", "A", "EQ"),
        _stk_row("INE004", "NULLSERIES", float("nan")),
        {**_stk_row("INE005", "NONESERIES", "EQ"), "SctySrs": None},
    ])
    out = normalize_equity_bhavcopy(raw)
    assert len(out) == 3

    nullseries = out[out["symbol"] == "NULLSERIES"].iloc[0]
    assert nullseries["series"] == ""
    noneseries = out[out["symbol"] == "NONESERIES"].iloc[0]
    assert noneseries["series"] == ""

    # Must not raise -- this is exactly the frame shape that killed the whole
    # day in the live 2026-07-03 run before this fix.
    validate_ohlc(out)

    # Per-series gate sanity (store.day_series_counts-style groupby): the ""
    # series groups as its own normal (tiny, new-series-exempt) bucket, not as
    # NaN/dropped rows that would vanish from the groupby entirely.
    counts = {str(k): int(v) for k, v in out.groupby("series").size().items()}
    assert counts == {"EQ": 1, "": 2}


# -- nse_token: NSE's exchange-issued FinInstrmId, kept beside ISIN and series --

def test_nse_token_reaches_the_output_per_listing():
    raw = pd.read_csv(FIX / "bhavcopy_normal.csv")
    out = normalize_equity_bhavcopy(raw)
    tokens = dict(zip(out["symbol"], out["nse_token"], strict=True))
    assert tokens == {"RELIANCE": 2885, "INFY": 1594, "HDFCBANK": 1333}
    assert str(out["nse_token"].dtype) == "Int64"


def test_same_share_in_two_series_keeps_a_token_for_each():
    # A series change (EQ -> BE) issues a new token: both must survive.
    raw = pd.DataFrame([
        {**_stk_row("INE001", "ABC", "EQ"), "FinInstrmId": 111},
        {**_stk_row("INE001", "ABC", "BE"), "FinInstrmId": 222},
    ])
    out = normalize_equity_bhavcopy(raw)
    assert dict(zip(out["series"], out["nse_token"], strict=True)) == {"EQ": 111, "BE": 222}


@pytest.mark.parametrize("bad", ["abc", "1.5", 0, -7, "0", "-7", "1e3", "99999999999999999999"])
def test_malformed_token_becomes_null_with_a_warning_and_a_count(bad, caplog):
    raw = pd.DataFrame([
        {**_stk_row("INE001", "GOOD", "EQ"), "FinInstrmId": "2885"},
        {**_stk_row("INE002", "BAD", "EQ"), "FinInstrmId": bad},
    ])
    with caplog.at_level("WARNING", logger="pipeline.normalize"):
        out = normalize_equity_bhavcopy(raw)  # never raises: the day survives
    by = dict(zip(out["symbol"], out["nse_token"], strict=True))
    assert by["GOOD"] == 2885
    assert pd.isna(by["BAD"])
    assert out.attrs["nse_token_nulled"] == 1
    assert any("nse_token" in r.message and "1" in r.message for r in caplog.records)


def test_blank_or_missing_token_is_null_without_a_warning(caplog):
    raw = pd.DataFrame([
        {**_stk_row("INE001", "A", "EQ"), "FinInstrmId": None},
        {**_stk_row("INE002", "B", "EQ"), "FinInstrmId": " "},
        # A NaN-bearing column reads as float:
        {**_stk_row("INE003", "C", "EQ"), "FinInstrmId": 3003.0},
    ])
    with caplog.at_level("WARNING", logger="pipeline.normalize"):
        out = normalize_equity_bhavcopy(raw)
    by = dict(zip(out["symbol"], out["nse_token"], strict=True))
    assert pd.isna(by["A"]) and pd.isna(by["B"]) and by["C"] == 3003
    assert out.attrs["nse_token_nulled"] == 0
    assert not caplog.records


def test_token_column_missing_from_the_udiff_file_is_a_structural_failure():
    from pipeline.errors import UnexpectedFailure
    raw = pd.DataFrame([_stk_row("INE001", "A", "EQ")]).drop(columns=["FinInstrmId"])
    with pytest.raises(UnexpectedFailure, match="FinInstrmId"):
        normalize_equity_bhavcopy(raw)
