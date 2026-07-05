"""sec_bhavdata_full fallback adapter: shape-adapter units + end-to-end fallback.

The fallback contract (documented in fetch.py): fallback callables emit
frames already reshaped to the PRIMARY's raw shape (UDiFF columns), so the
existing `normalize_equity_bhavcopy` consumes them unchanged. Provenance
comes from FetchResult.source (Task 1), never from the shape-adapter."""
from __future__ import annotations

import dataclasses
from datetime import date

import pandas as pd
import pytest
import responses

from pipeline import config, datasets
from pipeline.calendar import is_trading_day
from pipeline.daily_update import run_daily
from pipeline.errors import UnexpectedFailure
from pipeline.fetch import FetchResult
from pipeline.normalize import normalize_equity_bhavcopy
from pipeline.sources.nse_secfull import (
    SECFULL_RAW_COLUMNS,
    build_secfull_url,
    secfull_to_udiff_shape,
)


def _raw_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "SYMBOL": "RELIANCE",
        "SERIES": "EQ",
        "DATE1": "03-Jul-2026",
        "PREV_CLOSE": 2980.0,
        "OPEN_PRICE": 2990.0,
        "HIGH_PRICE": 3010.0,
        "LOW_PRICE": 2985.0,
        "CLOSE_PRICE": 3000.0,
        "TTL_TRD_QNTY": 1000000,
        "TURNOVER_LACS": 30000.0,
        "NO_OF_TRADES": 50000,
    }
    row.update(overrides)
    return row


def _raw(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(list(rows) or [_raw_row()])


# -- build_secfull_url ---------------------------------------------------


def test_build_secfull_url_ddmmyyyy_format():
    url = build_secfull_url(date(2026, 7, 3))
    assert url == (
        "https://nsearchives.nseindia.com/products/content/"
        "sec_bhavdata_full_03072026.csv"
    )


def test_build_secfull_url_zero_pads_month_and_day():
    url = build_secfull_url(date(2026, 1, 5))
    assert "05012026" in url


# -- SECFULL_RAW_COLUMNS --------------------------------------------------


def test_secfull_raw_columns_is_the_documented_eleven():
    assert SECFULL_RAW_COLUMNS == [
        "SYMBOL", "SERIES", "DATE1", "PREV_CLOSE", "OPEN_PRICE", "HIGH_PRICE",
        "LOW_PRICE", "CLOSE_PRICE", "TTL_TRD_QNTY", "TURNOVER_LACS", "NO_OF_TRADES",
    ]


# -- secfull_to_udiff_shape: basic shape ----------------------------------


def test_shape_adapter_produces_udiff_raw_columns():
    out = secfull_to_udiff_shape(_raw())
    expected = {
        "TradDt", "ISIN", "TckrSymb", "SctySrs", "OpnPric", "HghPric", "LwPric",
        "ClsPric", "PrvsClsgPric", "TtlTradgVol", "TtlTrfVal", "TtlNbOfTxsExctd",
        "SsnId", "FinInstrmTp",
    }
    assert expected.issubset(set(out.columns))


def test_shape_adapter_output_feeds_normalize_equity_bhavcopy_unchanged():
    # The core integration point at the unit level: the adapter's output must
    # be directly consumable by the EXISTING UDiFF normalizer with no changes.
    out = secfull_to_udiff_shape(_raw(), isin_map={"RELIANCE": "INE002A01018"})
    canonical = normalize_equity_bhavcopy(out, source="nse-secfull")
    assert list(canonical.columns) == config.CANON_COLUMNS
    assert len(canonical) == 1
    row = canonical.iloc[0]
    assert row["symbol"] == "RELIANCE"
    assert row["instrument_key"] == "INE002A01018"
    assert row["close"] == 3000.0
    assert row["series"] == "EQ"
    assert row["source"] == "nse-secfull"


def test_shape_adapter_fixed_session_and_instrument_type_fields():
    out = secfull_to_udiff_shape(_raw())
    assert out.iloc[0]["SsnId"] == "F1"
    assert out.iloc[0]["FinInstrmTp"] == "STK"


def test_shape_adapter_series_from_file():
    out = secfull_to_udiff_shape(_raw(_raw_row(SERIES="BE")))
    assert out.iloc[0]["SctySrs"] == "BE"


# -- whitespace stripping (headers AND string values) ---------------------


def test_shape_adapter_strips_whitespace_from_column_headers():
    raw = _raw()
    raw.columns = [f" {c} " for c in raw.columns]
    out = secfull_to_udiff_shape(raw)
    assert out.iloc[0]["TckrSymb"] == "RELIANCE"


def test_shape_adapter_strips_whitespace_from_string_values():
    raw = _raw(_raw_row(SYMBOL=" RELIANCE ", SERIES=" EQ "))
    out = secfull_to_udiff_shape(raw)
    assert out.iloc[0]["TckrSymb"] == "RELIANCE"
    assert out.iloc[0]["SctySrs"] == "EQ"


def test_shape_adapter_strips_whitespace_from_date_value():
    raw = _raw(_raw_row(DATE1=" 03-Jul-2026 "))
    out = secfull_to_udiff_shape(raw)
    assert out.iloc[0]["TradDt"] == "2026-07-03"


# -- DATE1 strict %d-%b-%Y -> ISO TradDt -----------------------------------


def test_shape_adapter_parses_date1_to_iso_traddt():
    out = secfull_to_udiff_shape(_raw(_raw_row(DATE1="03-Jul-2026")))
    assert out.iloc[0]["TradDt"] == "2026-07-03"


def test_shape_adapter_date1_wrong_format_raises():
    # DATE1 must be strict %d-%b-%Y; an ISO-formatted date must not silently
    # parse (mirrors the indices normalizer's strict-date precedent).
    with pytest.raises(ValueError):
        secfull_to_udiff_shape(_raw(_raw_row(DATE1="2026-07-03")))


# -- '-' placeholders: OPEN/HIGH/LOW coerce+fill from CLOSE; CLOSE strict --


def test_shape_adapter_dash_open_high_low_fill_from_close():
    raw = _raw(_raw_row(OPEN_PRICE="-", HIGH_PRICE="-", LOW_PRICE="-", CLOSE_PRICE=3000.0))
    out = secfull_to_udiff_shape(raw)
    row = out.iloc[0]
    assert row["OpnPric"] == 3000.0
    assert row["HghPric"] == 3000.0
    assert row["LwPric"] == 3000.0


def test_shape_adapter_close_price_dash_raises():
    # CLOSE is strict: a "-" here means the row is genuinely unusable data,
    # so it must fail loud rather than silently coerce.
    raw = _raw(_raw_row(CLOSE_PRICE="-"))
    with pytest.raises((ValueError, TypeError)):
        secfull_to_udiff_shape(raw)


def test_shape_adapter_prevclose_dash_coerces_but_is_not_filled_from_close():
    # PREV_CLOSE isn't in the '-'-fill-from-close list per the brief (only
    # OPEN/HIGH/LOW get that treatment); confirm it still round-trips a
    # normal numeric value through unchanged.
    raw = _raw(_raw_row(PREV_CLOSE=2980.5))
    out = secfull_to_udiff_shape(raw)
    assert out.iloc[0]["PrvsClsgPric"] == 2980.5


# -- TTL_TRD_QNTY / NO_OF_TRADES coerce -> 0 -------------------------------


def test_shape_adapter_dash_volume_coerces_to_zero():
    raw = _raw(_raw_row(TTL_TRD_QNTY="-"))
    out = secfull_to_udiff_shape(raw)
    assert out.iloc[0]["TtlTradgVol"] == 0


def test_shape_adapter_dash_trades_coerces_to_zero():
    raw = _raw(_raw_row(NO_OF_TRADES="-"))
    out = secfull_to_udiff_shape(raw)
    assert out.iloc[0]["TtlNbOfTxsExctd"] == 0


# -- TURNOVER_LACS x 100_000 -> TtlTrfVal (lakhs -> rupees) ----------------


def test_shape_adapter_turnover_lacs_converted_to_rupees():
    raw = _raw(_raw_row(TURNOVER_LACS=30000.0))
    out = secfull_to_udiff_shape(raw)
    assert out.iloc[0]["TtlTrfVal"] == pytest.approx(30000.0 * 100_000)


def test_shape_adapter_dash_turnover_coerces_to_zero_rupees():
    raw = _raw(_raw_row(TURNOVER_LACS="-"))
    out = secfull_to_udiff_shape(raw)
    assert out.iloc[0]["TtlTrfVal"] == 0.0


# -- ISIN from isin_map, hit + miss ----------------------------------------


def test_shape_adapter_isin_map_hit():
    out = secfull_to_udiff_shape(_raw(), isin_map={"RELIANCE": "INE002A01018"})
    assert out.iloc[0]["ISIN"] == "INE002A01018"


def test_shape_adapter_isin_map_miss_yields_empty_string():
    # Empty ISIN -> the normalizer's NSE: sentinel takes over downstream;
    # the shape-adapter itself never invents a key.
    out = secfull_to_udiff_shape(_raw(), isin_map={"OTHERSYMBOL": "INE999Z99999"})
    assert out.iloc[0]["ISIN"] == ""


def test_shape_adapter_isin_map_none_yields_empty_string_for_all_rows():
    out = secfull_to_udiff_shape(_raw())
    assert out.iloc[0]["ISIN"] == ""


def test_shape_adapter_isin_map_strips_symbol_whitespace_before_lookup():
    raw = _raw(_raw_row(SYMBOL=" RELIANCE "))
    out = secfull_to_udiff_shape(raw, isin_map={"RELIANCE": "INE002A01018"})
    assert out.iloc[0]["ISIN"] == "INE002A01018"


# -- missing required raw column -> UnexpectedFailure ----------------------


def test_shape_adapter_missing_required_column_raises_unexpected_failure():
    raw = _raw().drop(columns=["TURNOVER_LACS"])
    with pytest.raises(UnexpectedFailure):
        secfull_to_udiff_shape(raw)


def test_shape_adapter_missing_column_message_names_it():
    raw = _raw().drop(columns=["NO_OF_TRADES"])
    with pytest.raises(UnexpectedFailure, match="NO_OF_TRADES"):
        secfull_to_udiff_shape(raw)


# -- multi-row sanity -------------------------------------------------------


def test_shape_adapter_multiple_rows_each_mapped_independently():
    raw = _raw(
        _raw_row(SYMBOL="RELIANCE", CLOSE_PRICE=3000.0),
        _raw_row(SYMBOL="INFY", CLOSE_PRICE=1500.0, SERIES="EQ"),
    )
    out = secfull_to_udiff_shape(raw, isin_map={"RELIANCE": "INE002A01018"})
    assert set(out["TckrSymb"]) == {"RELIANCE", "INFY"}
    infy = out[out["TckrSymb"] == "INFY"].iloc[0]
    assert infy["ISIN"] == ""  # miss -> empty, sentinel deferred to normalizer
    reliance = out[out["TckrSymb"] == "RELIANCE"].iloc[0]
    assert reliance["ISIN"] == "INE002A01018"


# ===========================================================================
# Integration: primary 500s x3 -> secfull fallback serves -> run_daily
# succeeds with source == "nse-secfull" end to end, through datasets._equities_fetcher.
# ===========================================================================

_TARGET = date(2026, 7, 3)
assert is_trading_day(_TARGET, holidays=set())  # a Friday, sanity-checked


def _secfull_csv_bytes() -> bytes:
    header = ",".join(SECFULL_RAW_COLUMNS)
    row = (
        "RELIANCE,EQ,03-Jul-2026,2980,2990,3010,2985,3000,1000000,30000,50000"
    )
    return f"{header}\n{row}\n".encode()


@responses.activate
def test_equities_fetcher_falls_back_to_secfull_and_run_daily_stamps_provenance(
    tmp_path, monkeypatch
):
    from pipeline.sources import nse_secfull, nse_udiff

    # No reference/instruments_all.parquet on disk for this test -> isin_map
    # is {} -> all rows key off the NSE: sentinel. That's fine; the point of
    # this test is provenance, not isin resolution.
    monkeypatch.setattr(config, "REFERENCE_DIR", tmp_path / "reference_missing")

    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    udiff_url = nse_udiff.build_udiff_url(_TARGET)
    for _ in range(3):  # primary retry contract: 3 attempts, all fail
        responses.add(responses.GET, udiff_url, status=503)
    responses.add(
        responses.GET,
        nse_secfull.build_secfull_url(_TARGET),
        body=_secfull_csv_bytes(),
        status=200,
        content_type="text/csv",
    )

    from pipeline import fetch as fetch_mod
    monkeypatch.setattr(fetch_mod.time, "sleep", lambda *_a, **_k: None)  # no real backoff sleep

    # abs_rowcount_range overridden: this test's synthetic 1-row CSV exercises
    # the fallback/provenance mechanism, not the (already separately tested)
    # full-market rowcount gate -- the production EQUITIES range (2000..10000)
    # would fail any single-symbol fixture regardless of source.
    spec = dataclasses.replace(
        datasets.EQUITIES, base_dir=tmp_path / "ohlc", abs_rowcount_range=(0, 10**9)
    )
    fetcher = datasets._equities_fetcher()

    status = run_daily(spec, _TARGET, fetcher=fetcher, holidays=set())

    assert status.status == "success"
    assert status.source == "nse-secfull"

    stored = pd.read_parquet(spec.base_dir / f"ohlc_{_TARGET.year}.parquet")
    assert len(stored) == 1
    assert set(stored["source"]) == {"nse-secfull"}
    assert stored.iloc[0]["symbol"] == "RELIANCE"


@responses.activate
def test_equities_fetcher_secfull_fallback_resolves_isin_from_reference(
    tmp_path, monkeypatch
):
    from pipeline.sources import nse_secfull, nse_udiff

    ref_dir = tmp_path / "reference"
    ref_dir.mkdir(parents=True)
    ref_df = pd.DataFrame({
        "instrument_key": ["INE002A01018"],
        "isin": ["INE002A01018"],
        "symbol": ["RELIANCE"],
        "series": ["EQ"],
        "status": ["active"],
        "last_seen": pd.to_datetime(["2026-07-02"]),
    })
    ref_df.to_parquet(ref_dir / "instruments_all.parquet", compression="zstd", index=False)
    monkeypatch.setattr(config, "REFERENCE_DIR", ref_dir)

    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    udiff_url = nse_udiff.build_udiff_url(_TARGET)
    for _ in range(3):
        responses.add(responses.GET, udiff_url, status=503)
    responses.add(
        responses.GET,
        nse_secfull.build_secfull_url(_TARGET),
        body=_secfull_csv_bytes(),
        status=200,
        content_type="text/csv",
    )

    from pipeline import fetch as fetch_mod
    monkeypatch.setattr(fetch_mod.time, "sleep", lambda *_a, **_k: None)

    # abs_rowcount_range overridden: see the sibling provenance test above --
    # this test's synthetic 1-row CSV is exercising ISIN resolution, not the
    # full-market rowcount gate.
    spec = dataclasses.replace(
        datasets.EQUITIES, base_dir=tmp_path / "ohlc", abs_rowcount_range=(0, 10**9)
    )
    fetcher = datasets._equities_fetcher()

    status = run_daily(spec, _TARGET, fetcher=fetcher, holidays=set())

    assert status.status == "success"
    stored = pd.read_parquet(spec.base_dir / f"ohlc_{_TARGET.year}.parquet")
    assert stored.iloc[0]["instrument_key"] == "INE002A01018"
    assert stored.iloc[0]["isin"] == "INE002A01018"


# ===========================================================================
# _load_isin_map
# ===========================================================================


def test_load_isin_map_reads_active_rows_symbol_to_isin(tmp_path, monkeypatch):
    from pipeline import datasets as ds_mod

    ref_dir = tmp_path / "reference"
    ref_dir.mkdir(parents=True)
    df = pd.DataFrame({
        "instrument_key": ["INE002A01018", "INE_OLD"],
        "isin": ["INE002A01018", "INE_OLD"],
        "symbol": ["RELIANCE", "DEADCO"],
        "series": ["EQ", "EQ"],
        "status": ["active", "inactive"],
        "last_seen": pd.to_datetime(["2026-07-02", "2026-06-01"]),
    })
    df.to_parquet(ref_dir / "instruments_all.parquet", compression="zstd", index=False)
    monkeypatch.setattr(config, "REFERENCE_DIR", ref_dir)

    isin_map = ds_mod._load_isin_map()
    assert isin_map == {"RELIANCE": "INE002A01018"}  # inactive DEADCO excluded


def test_load_isin_map_drops_empty_isins(tmp_path, monkeypatch):
    from pipeline import datasets as ds_mod

    ref_dir = tmp_path / "reference"
    ref_dir.mkdir(parents=True)
    df = pd.DataFrame({
        "instrument_key": ["NSE:NEWCO"],
        "isin": [""],
        "symbol": ["NEWCO"],
        "series": ["EQ"],
        "status": ["active"],
        "last_seen": pd.to_datetime(["2026-07-02"]),
    })
    df.to_parquet(ref_dir / "instruments_all.parquet", compression="zstd", index=False)
    monkeypatch.setattr(config, "REFERENCE_DIR", ref_dir)

    isin_map = ds_mod._load_isin_map()
    assert isin_map == {}


def test_load_isin_map_dedupes_multiple_scd2_rows_by_latest_last_seen(tmp_path, monkeypatch):
    # Reference may hold multiple SCD2 rows per symbol (e.g. a series change);
    # the join must not blow up cardinality -- dedupe to the latest by
    # last_seen when building the map.
    from pipeline import datasets as ds_mod

    ref_dir = tmp_path / "reference"
    ref_dir.mkdir(parents=True)
    df = pd.DataFrame({
        "instrument_key": ["INE001_OLD", "INE001_NEW"],
        "isin": ["INE001_OLD", "INE001_NEW"],
        "symbol": ["SAMESYM", "SAMESYM"],
        "series": ["BE", "EQ"],
        "status": ["active", "active"],
        "last_seen": pd.to_datetime(["2026-06-01", "2026-07-02"]),
    })
    df.to_parquet(ref_dir / "instruments_all.parquet", compression="zstd", index=False)
    monkeypatch.setattr(config, "REFERENCE_DIR", ref_dir)

    isin_map = ds_mod._load_isin_map()
    assert isin_map == {"SAMESYM": "INE001_NEW"}  # latest by last_seen wins


def test_load_isin_map_absent_reference_returns_empty_dict(tmp_path, monkeypatch, capsys):
    from pipeline import datasets as ds_mod

    monkeypatch.setattr(config, "REFERENCE_DIR", tmp_path / "does_not_exist")

    isin_map = ds_mod._load_isin_map()
    assert isin_map == {}
    captured = capsys.readouterr()
    assert captured.err.strip() != ""  # stderr note when reference is absent


# ===========================================================================
# datasets._equities_fetcher wiring
# ===========================================================================


def test_equities_spec_uses_equities_fetcher_factory():
    assert datasets.EQUITIES.make_fetcher is datasets._equities_fetcher


def test_equities_fetcher_returns_nse_udiff_fetcher_with_secfull_fallback():
    from pipeline.fetch import NseUdiffFetcher

    fetcher = datasets._equities_fetcher()
    assert isinstance(fetcher, NseUdiffFetcher)
    labels = [label for label, _fn in fetcher._fallbacks]
    assert labels == ["nse-secfull"]


def test_secfull_fallback_fn_returns_bare_dataframe_not_fetchresult(tmp_path, monkeypatch):
    # The Fallback type alias is Callable[[date], pd.DataFrame] (fetch.py) --
    # NseUdiffFetcher._fetch_fallbacks wraps the return value into a
    # FetchResult itself. _secfull_fallback must return a bare DataFrame.
    monkeypatch.setattr(config, "REFERENCE_DIR", tmp_path / "missing")
    from pipeline import datasets as ds_mod

    with responses.RequestsMock() as rsps:
        from pipeline.sources import nse_secfull

        rsps.add(responses.GET, "https://www.nseindia.com/", status=200)
        rsps.add(
            responses.GET,
            nse_secfull.build_secfull_url(_TARGET),
            body=_secfull_csv_bytes(),
            status=200,
            content_type="text/csv",
        )
        from pipeline import fetch as fetch_mod
        monkeypatch.setattr(fetch_mod.time, "sleep", lambda *_a, **_k: None)

        result = ds_mod._secfull_fallback(_TARGET)
    assert isinstance(result, pd.DataFrame)
    assert not isinstance(result, FetchResult)
