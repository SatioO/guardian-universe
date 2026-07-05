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


# G2 Task 5 fix round 1: regime-consistent INTERSECTION completeness.
#
# THE BUG (reviewer live-reproduced): the completeness gate above compared a
# stored day's TOTAL against the trailing mean summed over ALL trailing
# series. After a universe-widening event, pre-widening EQ-only days inside
# the 7-day catch-up window compare against widened trailing means -> falsely
# "short" -> nightly re-fetch, 7 nights running in the live incident.
#
# THE FIX: completeness is now computed over the series SHARED between the
# stored day and the trailing dict (a trailing entry only counts if it is
# non-empty). A pre-widening EQ-only day compared against widened (EQ+BE)
# trailing history now has shared={EQ} -- BE never enters the comparison
# because the stored day never had it -- so the day is correctly judged
# complete on its own (EQ-only) regime instead of the union of both regimes.

def _widened_rows(d: str, n_eq: int, n_be: int) -> pd.DataFrame:
    """One trailing day's stored rows spanning BOTH EQ and BE series --
    i.e. what the store looks like AFTER the universe-widening event."""
    return pd.concat(
        [_canon_rows(d, n_eq), _be_rows(d, n_be)], ignore_index=True
    )


def _eq_rows_matching_multi_series_raw(d: str, n: int) -> pd.DataFrame:
    """EQ rows keyed EQ{i:05d} -- the SAME instrument_key scheme the
    normalizer derives from `_multi_series_raw`'s ISIN column -- so a
    truncated stored day and a subsequent full re-fetch describe the SAME
    universe (real symbols served/missing), letting append_keyed's keep="last"
    dedupe genuinely overwrite+extend rather than landing disjoint rows."""
    return pd.DataFrame([{
        "date": pd.Timestamp(d), "instrument_key": f"EQ{i:05d}", "isin": f"EQ{i:05d}",
        "symbol": f"EQS{i}", "series": "EQ",
        "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "prevclose": 100.0,
        "volume": 1, "value": 1.0, "trades": 1, "source": "seed",
    } for i in range(n)])[config.CANON_COLUMNS]


def test_pre_widening_eq_only_day_vs_widened_trailing_skips_regression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # THE REPRODUCTION (mirrors the reviewer's live repro shape): the target
    # day being gated is a COMPLETE, EQ-only day of 2384 rows (matching the
    # reviewer's reported 2384) -- but ITS OWN trailing window (the 3 trading
    # days immediately before it, per `_trailing_series_counts`, which always
    # looks strictly backward from whatever day is passed to run_daily) is
    # already widened: each trailing day carries EQ=2200 + BE=1247 (trailing
    # total 3447, matching the reviewer's reported ~3447). This is exactly the
    # shape the catch-up loop (G2 Task 4) produces for an OLDER window day
    # sitting a few trading days behind today's real target: the day being
    # re-visited is older and EQ-only-complete in its own right, while the
    # days immediately preceding IT already reflect the widened universe.
    #
    # Pre-fix (RED against current code): trailing_total_mean sums per-series
    # means over ALL trailing series = 2200 (EQ) + 1247 (BE) = 3447; the
    # stored EQ-only day's flat total (2384) is compared against that whole-
    # regime figure: 2384 >= 0.85 * 3447 (=2930.0)? No -> falsely judged
    # "short" -> re-fetch triggered (the bug: a COMPLETE day gets re-ingested
    # every night this day remains inside the catch-up window).
    #
    # Post-fix (this test's actual assertion): shared = {EQ} only (BE is
    # absent from the stored day, so its trailing entry is never counted).
    # trailing_shared_mean = 2200 (EQ alone). stored_shared_total = 2384 (the
    # stored day's own EQ count -- it has no BE rows to add). 2384 >= 0.85 *
    # 2200 (=1870.0)? Yes -> skipped_idempotent, fetcher NOT called.
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    target = date(2026, 7, 3)
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):  # widened trailing (EQ+BE)
        store.append_day(_widened_rows(d, n_eq=2200, n_be=1247), tmp_path)
    store.append_day(_canon_rows(target.isoformat(), 2384), tmp_path)  # complete, EQ-only

    st = _run(target, StubFetcher(exc=AssertionError("must not refetch")), tmp_path)
    assert st.status == "skipped_idempotent"


def test_truncated_eq_only_day_vs_widened_trailing_still_repairs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Truncated-day-still-repairs: an EQ-only day genuinely truncated to HALF
    # its own (EQ-only) trailing count, compared against widened (EQ+BE)
    # trailing history, must still re-fetch and top up -- the fix narrows the
    # comparison to the SHARED series, it must not blind the gate to a real
    # truncation within that shared series. shared={EQ}, trailing_shared_mean
    # = 2200, stored_shared_total = 1100 (half): 1100 >= 0.85*2200 (=1870)?
    # No -> re-fetch triggered (stub IS called), tops up, success.
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    target = date(2026, 7, 3)
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):  # widened trailing (EQ+BE)
        store.append_day(_widened_rows(d, n_eq=2200, n_be=1247), tmp_path)
    # EQ halved, keyed EQ00000..EQ01099 -- the FIRST HALF of the 2200 keys the
    # fresh fetch below will re-serve -- so the top-up is a real merge (those
    # 1100 rows are overwritten in place, not left as orphaned duplicates).
    store.append_day(_eq_rows_matching_multi_series_raw(target.isoformat(), 1100), tmp_path)

    raw = _multi_series_raw(n_eq=2200, n_be=1247)  # fresh fetch re-serves the FULL day
    st = _run(target, StubFetcher(raw), tmp_path)
    assert st.status == "success"
    assert "re-ingested short day" in st.message
    assert "over shared series" in st.message
    assert store.day_symbol_count(tmp_path, target) == 2200 + 1247  # topped up to full


def test_short_day_reingest_still_applies_quarantine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # The re-ingest path is NOT a special case that bypasses the rest of
    # run_daily's pipeline -- it re-enters the exact same fetch -> normalize
    # -> wrong-date guard -> quarantine -> schema -> append sequence as a
    # normal day. Two rows this time: RELIANCE clean, INFY fails quarantine
    # (high < low) -- proves quarantine still fires on the re-ingest path,
    # while the clean row still tops up the stored count.
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


def test_short_day_reingest_still_applies_wrong_date_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # Stronger variant of the property above: the re-ingest path still runs
    # the wrong-date guard, not just quarantine. A short stored day whose
    # FRESH fetch comes back wrong-dated (stale republish / fallback
    # date-stamp bug) must still be rejected as "failed" -- the completeness
    # gate deciding to re-fetch must never itself bypass the guard that
    # protects every other day.
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    target = date(2026, 7, 3)
    wrong_day = date(2026, 7, 1)
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):
        store.append_day(_canon_rows(d, 2), tmp_path)  # trailing mean 2
    store.append_day(_canon_rows(target.isoformat(), 1), tmp_path)  # short: 1 < 0.85*2

    raw = pd.DataFrame([_raw_row(wrong_day.isoformat(), "INE002A01018", "RELIANCE")])
    st = _run(target, StubFetcher(raw), tmp_path)
    assert st.status == "failed"
    assert str(target) in st.message
    assert repr(wrong_day) in st.message
    # The pre-existing short row for `target` must be untouched -- the guard
    # rejected the re-fetch before any store write, so the original (still
    # short) stored count survives exactly as it was.
    assert store.day_symbol_count(tmp_path, target) == 1


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


# G3 Task 2: run_daily accepts an optional `cache: store.ReadCache | None`
# and threads it into every internal _read_year-backed store call (has_day,
# day_series_counts, and the trailing-window loop in
# _trailing_series_counts) -- but never into write_delta, which is
# delta-file I/O with no cache parameter at all.

def test_run_daily_threads_passed_cache_into_read_year_backed_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    target = date(2026, 7, 3)
    # Seed a stored, complete day (idempotent-skip path) plus trailing
    # history so BOTH cache call-sites inside run_daily actually execute:
    # the idempotency gate's has_day/day_series_counts, AND
    # _trailing_series_counts' own has_day/day_series_counts loop.
    for d in (date(2026, 6, 29), date(2026, 6, 30), date(2026, 7, 1), date(2026, 7, 2)):
        _run(d, StubFetcher(RAW), tmp_path)
    _run(target, StubFetcher(RAW), tmp_path)

    seen_caches: list[object] = []
    real_read_year = store._read_year

    def spying_read_year(base, year, prefix="ohlc", *, columns=None, cache=None):
        seen_caches.append(cache)
        return real_read_year(base, year, prefix, columns=columns, cache=cache)

    monkeypatch.setattr(store, "_read_year", spying_read_year)

    cache = store.ReadCache()
    st = run_daily(
        datasets_spec(tmp_path), target, fetcher=StubFetcher(exc=AssertionError("must not fetch")),
        holidays=HOLIDAYS, cache=cache,
    )
    assert st.status == "skipped_idempotent"  # already-complete day: no fetch needed

    assert seen_caches, "expected at least one _read_year call during the idempotent path"
    assert all(c is cache for c in seen_caches), (
        "every _read_year call inside run_daily must receive the SAME cache "
        "instance that was passed in, not a fresh/None cache"
    )


def test_run_daily_never_passes_cache_to_write_delta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """write_delta is delta-file I/O, not year-file I/O -- it has no `cache`
    parameter at all, so run_daily must never attempt to pass one through."""
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))

    calls: list[dict] = []
    real_write_delta = store.write_delta

    def spying_write_delta(df, base, d, *, prefix="ohlc", keep=35):
        calls.append({"prefix": prefix, "keep": keep})
        return real_write_delta(df, base, d, prefix=prefix, keep=keep)

    monkeypatch.setattr(store, "write_delta", spying_write_delta)

    cache = store.ReadCache()
    st = run_daily(
        datasets_spec(tmp_path), date(2026, 7, 3), fetcher=StubFetcher(RAW),
        holidays=HOLIDAYS, cache=cache,
    )
    assert st.status == "success"
    assert len(calls) == 1  # write_delta was still called (success path emits a delta)
