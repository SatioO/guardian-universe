import dataclasses
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipeline import config, datasets, store
from pipeline.daily_update import RunStatus, run_daily
from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.fetch import Fetcher, FetchResult

HOLIDAYS = {date(2026, 8, 15)}
RAW = pd.read_csv(Path(__file__).parent / "fixtures" / "bhavcopy_normal.csv")


def datasets_spec(base):
    # abs_rowcount_range is re-read from config (not just base_dir) so that
    # tests monkeypatching config.ROWCOUNT_ABS_RANGE still take effect — the
    # spec field is otherwise frozen at datasets.py import time.
    return dataclasses.replace(
        datasets.EQUITIES, base_dir=base, abs_rowcount_range=config.ROWCOUNT_ABS_RANGE
    )


class StubFetcher:
    def __init__(
        self,
        df: pd.DataFrame | None = None,
        exc: Exception | None = None,
        source: str = "nse-udiff",
    ):
        self._df, self._exc, self._source = df, exc, source

    def fetch_raw(self, d: date) -> FetchResult:
        if self._exc is not None:
            raise self._exc
        assert self._df is not None
        return FetchResult(self._df, self._source)


def _run(target: date, fetcher: Fetcher, base: Path) -> RunStatus:
    return run_daily(datasets_spec(base), target, fetcher=fetcher, holidays=HOLIDAYS)


def test_normal_day_ingests_and_persists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    st = _run(date(2026, 7, 3), StubFetcher(RAW), tmp_path)
    assert st.status == "success"
    # Spec change (G1b task 4): the BE row (HDFCBANK) now survives normalization
    # alongside RELIANCE + INFY (the SctySrs == "EQ" filter is dropped).
    assert st.symbol_count == 3
    out = pd.read_parquet(base_year(tmp_path))
    assert set(out["symbol"]) == {"RELIANCE", "INFY", "HDFCBANK"}


def test_fallback_served_day_stamps_fallback_source_everywhere(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # A day served by a fallback (not the primary) must have its provenance
    # reflect the ACTUAL source that served it -- both in RunStatus.source and
    # in the stored rows' "source" column -- never the primary's label.
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    st = _run(date(2026, 7, 3), StubFetcher(RAW, source="secondary-label"), tmp_path)
    assert st.status == "success"
    assert st.source == "secondary-label"
    out = pd.read_parquet(base_year(tmp_path))
    assert set(out["source"]) == {"secondary-label"}


def test_holiday_skips_cleanly_without_fetching(tmp_path: Path):
    st = _run(date(2026, 8, 15), StubFetcher(exc=AssertionError("must not fetch")), tmp_path)
    assert st.status == "skipped_holiday"


def test_idempotent_rerun_is_a_noop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    _run(date(2026, 7, 3), StubFetcher(RAW), tmp_path)
    st = _run(date(2026, 7, 3), StubFetcher(exc=AssertionError("must not refetch")), tmp_path)
    assert st.status == "skipped_idempotent"


# G2 Task 5: completeness-aware idempotency. `has_day` alone (">=1 row
# exists") used to lock a partial day in forever -- a short day (a mid-fetch
# truncation, a fallback that only partially served the universe, etc.) would
# never be topped up because the very presence of any row short-circuited
# every subsequent run. These tests exercise the upgraded gate: present-but-
# short (vs trailing history) -> re-fetch and merge; present-and-complete ->
# still a free, zero-fetch skip; no trailing history yet -> present always
# skips (nothing to compare against).

def test_short_stored_day_reingests_and_tops_up_to_full(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    target = date(2026, 7, 3)
    # RAW (the shared fixture) re-serves 2 EQ rows (RELIANCE, INFY) + 1 BE row
    # (HDFCBANK) -- seed EQ-only trailing history so the pre-existing
    # per-series deviation gate (unrelated to this test) doesn't also fire:
    # trailing EQ mean 2 matches RAW's own EQ count, and BE is a brand-new
    # series today with no trailing data, which the gate exempts outright.
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):
        store.append_day(_canon_rows(d, 2), tmp_path)
    # Pre-seed the target day itself, truncated to JUST the RELIANCE row (real
    # ISIN INE002A01018, matching what RAW itself will re-fetch) -- well under
    # (1 - 0.15) * 2 == 1.7 -- a partial day that must NOT be treated as done.
    # Using RAW's actual key (not a synthetic one) exercises the REAL merge:
    # append_keyed's keep="last" dedupe overwrites this one row in place while
    # INFY + HDFCBANK are newly added -- proving the top-up is a true merge,
    # not an append-alongside-orphaned-rows artifact.
    truncated = pd.DataFrame([{
        "date": pd.Timestamp(target.isoformat()), "instrument_key": "INE002A01018",
        "isin": "INE002A01018", "symbol": "RELIANCE", "series": "EQ",
        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "prevclose": 1.0,
        "volume": 1, "value": 1.0, "trades": 1, "source": "seed",
    }])[config.CANON_COLUMNS]
    store.append_day(truncated, tmp_path)
    assert store.day_symbol_count(tmp_path, target) == 1

    st = _run(target, StubFetcher(RAW), tmp_path)  # RAW re-serves the full day
    assert st.status == "success"
    assert "re-ingested short day" in st.message
    assert "stored 1" in st.message
    assert store.day_symbol_count(tmp_path, target) == 3  # merged up to full (2 EQ + 1 BE)
    out = pd.read_parquet(base_year(tmp_path))
    reliance = out[(out["date"] == pd.Timestamp(target)) & (out["symbol"] == "RELIANCE")]
    assert len(reliance) == 1
    assert reliance.iloc[0]["close"] != 1.0  # the stale seeded price was overwritten by RAW's


def test_complete_stored_day_skips_without_fetching(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    target = date(2026, 7, 3)
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):
        store.append_day(_canon_rows(d, 3), tmp_path)
    store.append_day(_canon_rows(target.isoformat(), 3), tmp_path)  # full, matches mean

    st = _run(target, StubFetcher(exc=AssertionError("must not refetch")), tmp_path)
    assert st.status == "skipped_idempotent"


def test_complete_stored_day_within_shortfall_tolerance_skips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Within the 15% shortfall tolerance (not exactly full) must still be a
    # free skip -- only a day BELOW the tolerance band re-ingests.
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    target = date(2026, 7, 3)
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):
        store.append_day(_canon_rows(d, 100), tmp_path)  # trailing mean 100
    store.append_day(_canon_rows(target.isoformat(), 90), tmp_path)  # 90 >= 85

    st = _run(target, StubFetcher(exc=AssertionError("must not refetch")), tmp_path)
    assert st.status == "skipped_idempotent"


def test_present_day_with_empty_trailing_skips_without_fetching(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Fresh store: the day is present but there is no trailing history at all
    # (no prior days ingested) -- nothing to compare against, so any stored
    # rows count as complete and the day skips exactly as it did pre-Task-5.
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    target = date(2026, 7, 3)
    store.append_day(_canon_rows(target.isoformat(), 1), tmp_path)  # just 1 row, no history

    st = _run(target, StubFetcher(exc=AssertionError("must not refetch")), tmp_path)
    assert st.status == "skipped_idempotent"


def test_short_day_reingest_still_applies_wrong_date_guard_and_quarantine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Combined property: the re-ingest path is NOT a special case that
    # bypasses the rest of run_daily's pipeline -- it re-enters the exact same
    # fetch -> normalize -> wrong-date guard -> quarantine -> schema -> append
    # sequence as a normal day. Two rows this time: RELIANCE clean, INFY fails
    # quarantine (high < low) -- proves both the wrong-date guard has already
    # passed (same target date throughout) and quarantine still fires on the
    # re-ingest path, while the clean row still tops up the stored count.
    monkeypatch.setattr(config, "META_DIR", tmp_path / "meta")
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    target = date(2026, 7, 3)
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):
        store.append_day(_canon_rows(d, 2), tmp_path)  # trailing mean 2
    store.append_day(_canon_rows(target.isoformat(), 1), tmp_path)  # short: 1 < 0.85*2

    dirty_frame = pd.DataFrame([{
        "TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": "INE002A01018",
        "TckrSymb": "RELIANCE", "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 2990,
        "HghPric": 3010, "LwPric": 2985, "ClsPric": 3000, "PrvsClsgPric": 2980,
        "TtlTradgVol": 1000000, "TtlTrfVal": 3000000000, "TtlNbOfTxsExctd": 50000,
    }, {
        "TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": "INE009A01021",
        "TckrSymb": "INFY", "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 1500,
        "HghPric": 1480, "LwPric": 1510, "ClsPric": 1490, "PrvsClsgPric": 1495,
        "TtlTradgVol": 500000, "TtlTrfVal": 750000000, "TtlNbOfTxsExctd": 20000,
    }])
    st = _run(target, StubFetcher(dirty_frame), tmp_path)
    assert st.status == "success"
    assert st.quarantined_count == 1
    assert "re-ingested short day" in st.message
    qfile = tmp_path / "meta" / "quarantine" / "ohlc_2026-07-03.parquet"
    assert qfile.exists()
    assert len(pd.read_parquet(qfile)) == 1


def test_not_yet_published_is_reported_not_raised(tmp_path: Path):
    st = _run(date(2026, 7, 3), StubFetcher(exc=NotYetPublished("404")), tmp_path)
    assert st.status == "not_yet"


def test_not_yet_published_on_target_day_maps_to_not_yet(tmp_path: Path):
    # Explicit is_target_day=True (the default) reaffirms the existing
    # semantics: a 404 on the day we're actually trying to publish today is
    # ordinary lateness, not a hole.
    st = run_daily(
        datasets_spec(tmp_path), date(2026, 7, 3),
        fetcher=StubFetcher(exc=NotYetPublished("404")), holidays=HOLIDAYS,
        is_target_day=True,
    )
    assert st.status == "not_yet"


def test_not_yet_published_on_past_day_maps_to_failed(tmp_path: Path):
    # G2 Task 4: a 404 for a day that is NOT the target (a catch-up-window
    # day strictly before today's target) is a HOLE, not lateness -- NSE
    # archives don't retroactively un-publish a day, so a 404 on a day that
    # should already exist means the archive is missing it. This must map to
    # "failed" (retryable, alertable) rather than "not_yet" (which the CLI's
    # ok-set treats as a clean, non-alerting outcome).
    past_day = date(2026, 7, 1)
    st = run_daily(
        datasets_spec(tmp_path), past_day,
        fetcher=StubFetcher(exc=NotYetPublished("404")), holidays=HOLIDAYS,
        is_target_day=False,
    )
    assert st.status == "failed"
    assert "archive missing for past trading day" in st.message
    assert str(past_day) in st.message


def test_unexpected_failure_is_reported_not_raised(tmp_path: Path):
    st = _run(date(2026, 7, 3), StubFetcher(exc=UnexpectedFailure("timeout")), tmp_path)
    assert st.status == "failed"


def test_schema_failure_returns_failed_without_appending(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    # This row passes the STK + EQ + F-session filters AND quarantine (valid
    # prices/volume/instrument_key) but has a NULL symbol, which the schema gate
    # (symbol non-nullable) must reject. run_daily must return "failed" WITHOUT
    # appending (fail-closed) — not raise.
    bad_symbol = pd.DataFrame([{
        "TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": "INE002A01018",
        "TckrSymb": None, "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 2990,
        "HghPric": 3010, "LwPric": 2985, "ClsPric": 3000, "PrvsClsgPric": 2980,
        "TtlTradgVol": 1000000, "TtlTrfVal": 3000000000, "TtlNbOfTxsExctd": 50000,
    }])
    st = _run(date(2026, 7, 3), StubFetcher(bad_symbol), tmp_path)
    assert st.status == "failed"
    assert not store.has_day(tmp_path, date(2026, 7, 3))  # nothing written


def base_year(base: Path) -> Path:
    from pipeline import config
    return config.ohlc_path(2026, base)


def _canon_rows(d: str, n: int) -> pd.DataFrame:
    return pd.DataFrame([{
        "date": pd.Timestamp(d), "instrument_key": f"K{i:05d}", "isin": f"K{i:05d}",
        "symbol": f"S{i}", "series": "EQ",
        "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "prevclose": 100.0,
        "volume": 1, "value": 1.0, "trades": 1, "source": "seed",
    } for i in range(n)])[config.CANON_COLUMNS]


def test_all_rows_quarantined_returns_failed(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(config, "META_DIR", tmp_path / "meta")
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    # Every row has open<=0 -> all quarantined -> clean empty. Must be "failed"
    # (not a success no-op) and nothing written, so the day can be retried.
    bad = pd.DataFrame([{
        "TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": f"K{i}", "TckrSymb": f"S{i}",
        "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 0, "HghPric": 10, "LwPric": 0,
        "ClsPric": 5, "PrvsClsgPric": 5, "TtlTradgVol": 1, "TtlTrfVal": 1,
        "TtlNbOfTxsExctd": 1,
    } for i in range(3)])
    st = _run(date(2026, 7, 3), StubFetcher(bad), tmp_path)
    assert st.status == "failed"
    assert not store.has_day(tmp_path, date(2026, 7, 3))
    # All-corrupt path also persists a quarantine file (evidence the write still
    # happens on the "failed" branch, not just on "success").
    assert (tmp_path / "meta" / "quarantine" / "ohlc_2026-07-03.parquet").exists()


def test_malformed_raw_is_reported_not_raised(tmp_path: Path):
    # A fetched frame missing required UDiFF columns must yield "failed", not raise.
    malformed = pd.DataFrame([{"TradDt": "2026-07-03", "TckrSymb": "X"}])
    st = _run(date(2026, 7, 3), StubFetcher(malformed), tmp_path)
    assert st.status == "failed"
    assert not store.has_day(tmp_path, date(2026, 7, 3))


def test_store_error_is_reported_not_raised(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    def _boom(*_a, **_k):
        raise OSError("disk full")
    monkeypatch.setattr(store, "append_day", _boom)
    st = _run(date(2026, 7, 3), StubFetcher(RAW), tmp_path)
    assert st.status == "failed"


def test_success_emits_delta_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    st = _run(date(2026, 7, 3), StubFetcher(RAW), tmp_path)
    assert st.status == "success"
    deltas = store.list_deltas(tmp_path)
    assert [p.name for p in deltas] == ["ohlc_2026-07-03.parquet"]


def test_deviation_gate_fires_from_stored_trailing_window(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    # Seed the 3 trading days before Fri 2026-07-03 each with 100 rows (mean=100).
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):
        store.append_day(_canon_rows(d, 100), tmp_path)
    # The target-day fetch yields only 2 rows -> ~98% deviation from mean 100 ->
    # the deviation gate (fed by _trailing_counts over the real stored window)
    # must fire end-to-end -> "failed", nothing written.
    st = _run(date(2026, 7, 3), StubFetcher(RAW), tmp_path)
    assert st.status == "failed"
    assert not store.has_day(tmp_path, date(2026, 7, 3))


def _multi_series_raw(n_eq: int, n_be: int) -> pd.DataFrame:
    rows = []
    for i in range(n_eq):
        rows.append({
            "TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": f"EQ{i:05d}",
            "TckrSymb": f"EQS{i}", "SctySrs": "EQ", "SsnId": "F1",
            "OpnPric": 100, "HghPric": 101, "LwPric": 99, "ClsPric": 100,
            "PrvsClsgPric": 100, "TtlTradgVol": 1, "TtlTrfVal": 1, "TtlNbOfTxsExctd": 1,
        })
    for i in range(n_be):
        rows.append({
            "TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": f"BE{i:05d}",
            "TckrSymb": f"BES{i}", "SctySrs": "BE", "SsnId": "F1",
            "OpnPric": 100, "HghPric": 101, "LwPric": 99, "ClsPric": 100,
            "PrvsClsgPric": 100, "TtlTradgVol": 1, "TtlTrfVal": 1, "TtlNbOfTxsExctd": 1,
        })
    return pd.DataFrame(rows)


def test_widened_universe_day_passes_against_eq_only_trailing_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # End-to-end migration property: seed EQ-only trailing history (as if
    # ingested pre-migration), then run a widened day (EQ roughly stable count
    # + a brand-new BE series) through run_daily. Must succeed even though the
    # TOTAL row count is ~2x the EQ-only trailing mean.
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (2000, 10000))
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):
        store.append_day(_canon_rows(d, 2000), tmp_path)
    raw = _multi_series_raw(n_eq=2000, n_be=1800)
    st = _run(date(2026, 7, 3), StubFetcher(raw), tmp_path)
    assert st.status == "success"
    assert st.symbol_count == 3800
    assert store.has_day(tmp_path, date(2026, 7, 3))


def test_truncated_eq_file_fails_even_with_new_series_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Migration property (failure side): EQ count halved relative to EQ-only
    # trailing history must still fail the per-series gate, even though a new
    # BE series is present and the total is within abs_range.
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (2000, 10000))
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):
        store.append_day(_canon_rows(d, 2000), tmp_path)
    raw = _multi_series_raw(n_eq=1000, n_be=1800)  # EQ halved
    st = _run(date(2026, 7, 3), StubFetcher(raw), tmp_path)
    assert st.status == "failed"
    assert not store.has_day(tmp_path, date(2026, 7, 3))


def _be_rows(d: str, n: int) -> pd.DataFrame:
    return pd.DataFrame([{
        "date": pd.Timestamp(d), "instrument_key": f"BEK{i:05d}", "isin": f"BEK{i:05d}",
        "symbol": f"BES{i}", "series": "BE",
        "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "prevclose": 100.0,
        "volume": 1, "value": 1.0, "trades": 1, "source": "seed",
    } for i in range(n)])[config.CANON_COLUMNS]


def test_vanished_major_series_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # A major series (trailing mean >= 50) absent from today's data is a
    # truncation signal and must fail the gate.
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (2000, 10000))
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):
        combined = pd.concat([_canon_rows(d, 2000), _be_rows(d, 200)], ignore_index=True)
        store.append_day(combined, tmp_path)
    raw = _multi_series_raw(n_eq=2000, n_be=0)  # BE series vanished entirely
    st = _run(date(2026, 7, 3), StubFetcher(raw), tmp_path)
    assert st.status == "failed"
    assert not store.has_day(tmp_path, date(2026, 7, 3))


def _raw_row(trad_dt: str, isin: str, symbol: str, close: float = 100) -> dict[str, object]:
    return {
        "TradDt": trad_dt, "FinInstrmTp": "STK", "ISIN": isin, "TckrSymb": symbol,
        "SctySrs": "EQ", "SsnId": "F1", "OpnPric": close, "HghPric": close + 1,
        "LwPric": close - 1, "ClsPric": close, "PrvsClsgPric": close,
        "TtlTradgVol": 1000, "TtlTrfVal": 100000, "TtlNbOfTxsExctd": 10,
    }


def test_wrong_dated_frame_fails_and_never_touches_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    target = date(2026, 7, 3)
    wrong_day = date(2026, 7, 1)  # stale republish: whole frame dated D != target
    raw = pd.DataFrame([_raw_row(wrong_day.isoformat(), "INE002A01018", "RELIANCE")])
    st = _run(target, StubFetcher(raw), tmp_path)
    assert st.status == "failed"
    assert str(target) in st.message  # target date (actual)
    assert repr(wrong_day) in st.message  # fetched date (actual), datetime.date(...) repr
    assert not store.has_day(tmp_path, target)
    assert not store.has_day(tmp_path, wrong_day)  # nothing written anywhere


def test_wrong_dated_frame_cannot_overwrite_existing_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    good_day = date(2026, 7, 1)
    target = date(2026, 7, 3)  # a later, DIFFERENT target than the pre-seeded day
    # Pre-seed a correct day D via a normal successful run.
    seed_raw = pd.DataFrame([_raw_row(good_day.isoformat(), "INE002A01018", "RELIANCE", close=100)])
    seed_st = _run(good_day, StubFetcher(seed_raw), tmp_path)
    assert seed_st.status == "success"
    before = pd.read_parquet(base_year(tmp_path))
    before_day_rows = before[before["date"] == pd.Timestamp(good_day)].reset_index(drop=True)

    # Now run target=T (T != D) with a fetcher serving a frame dated D (the
    # already-stored day) with DIFFERENT prices -- this must NOT silently
    # overwrite the pre-seeded history via append_keyed's keep="last".
    poison_raw = pd.DataFrame(
        [_raw_row(good_day.isoformat(), "INE002A01018", "RELIANCE", close=999)]
    )
    st = _run(target, StubFetcher(poison_raw), tmp_path)
    assert st.status == "failed"

    after = pd.read_parquet(base_year(tmp_path))
    after_day_rows = after[after["date"] == pd.Timestamp(good_day)].reset_index(drop=True)
    pd.testing.assert_frame_equal(before_day_rows, after_day_rows)  # byte-identical
    assert not store.has_day(tmp_path, target)


def test_mixed_dated_frame_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    target = date(2026, 7, 3)
    other_day = date(2026, 7, 2)
    raw = pd.DataFrame([
        _raw_row(target.isoformat(), "INE002A01018", "RELIANCE"),
        _raw_row(other_day.isoformat(), "INE009A01021", "INFY"),
    ])
    st = _run(target, StubFetcher(raw), tmp_path)
    assert st.status == "failed"
    assert not store.has_day(tmp_path, target)
    assert not store.has_day(tmp_path, other_day)


def test_quarantined_rows_are_persisted(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(config, "META_DIR", tmp_path / "meta")
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    # Two rows: RELIANCE is clean, INFY has high<low so it fails quarantine.
    # Exactly one bad row among >=2 exercises the partial-success path (as
    # opposed to the all-corrupt "failed" path covered separately above).
    dirty_frame = pd.DataFrame([{
        "TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": "INE002A01018",
        "TckrSymb": "RELIANCE", "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 2990,
        "HghPric": 3010, "LwPric": 2985, "ClsPric": 3000, "PrvsClsgPric": 2980,
        "TtlTradgVol": 1000000, "TtlTrfVal": 3000000000, "TtlNbOfTxsExctd": 50000,
    }, {
        "TradDt": "2026-07-03", "FinInstrmTp": "STK", "ISIN": "INE009A01021",
        "TckrSymb": "INFY", "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 1500,
        "HghPric": 1480, "LwPric": 1510, "ClsPric": 1490, "PrvsClsgPric": 1495,
        "TtlTradgVol": 500000, "TtlTrfVal": 750000000, "TtlNbOfTxsExctd": 20000,
    }])
    st = _run(date(2026, 7, 3), StubFetcher(dirty_frame), tmp_path)
    assert st.status == "success"
    assert st.quarantined_count == 1
    qfile = tmp_path / "meta" / "quarantine" / "ohlc_2026-07-03.parquet"
    assert qfile.exists()
    assert len(pd.read_parquet(qfile)) == 1
