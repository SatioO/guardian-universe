import dataclasses
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config, datasets, store
from pipeline.backfill import backfill
from pipeline.daily_update import RunStatus, run_daily
from pipeline.errors import NotYetPublished
from pipeline.fetch import FetchResult

HOLIDAYS: set[date] = set()
RAW = pd.read_csv(Path(__file__).parent / "fixtures" / "bhavcopy_normal.csv")


def datasets_spec(base):
    # abs_rowcount_range is re-read from config (not just base_dir) so that
    # tests monkeypatching config.ROWCOUNT_ABS_RANGE still take effect — the
    # spec field is otherwise frozen at datasets.py import time.
    return dataclasses.replace(
        datasets.EQUITIES, base_dir=base, abs_rowcount_range=config.ROWCOUNT_ABS_RANGE
    )


class StubFetcher:
    def __init__(self):
        self.dates: list[date] = []

    def fetch_raw(self, d: date) -> FetchResult:
        self.dates.append(d)
        # Tag rows with the target date so each day is stored under its own date,
        # matching run_daily's idempotency check (has_day uses the stored TradDt).
        df = RAW.copy()
        df["TradDt"] = d.isoformat()
        return FetchResult(df, "nse-udiff")


def _no_sleep(_s: float) -> None:
    return None


def test_backfill_ingests_n_trading_days(tmp_path: Path, monkeypatch):
    from pipeline import config
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    f = StubFetcher()
    out = backfill(datasets_spec(tmp_path), date(2026, 7, 3), 3, fetcher=f,
                   holidays=HOLIDAYS, sleep=_no_sleep)
    assert [s.status for s in out] == ["success", "success", "success"]
    # exact 3 trading days ending 2026-07-03 (Fri), ascending — pins the window,
    # not just non-decreasing order.
    assert f.dates == [date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)]


def test_backfill_is_resumable(tmp_path: Path, monkeypatch):
    from pipeline import config
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    backfill(datasets_spec(tmp_path), date(2026, 7, 3), 3, fetcher=StubFetcher(),
             holidays=HOLIDAYS, sleep=_no_sleep)
    # Second run: every day already present -> idempotent skips, no refetch.
    f2 = StubFetcher()
    out = backfill(datasets_spec(tmp_path), date(2026, 7, 3), 3, fetcher=f2,
                   holidays=HOLIDAYS, sleep=_no_sleep)
    assert [s.status for s in out] == ["skipped_idempotent"] * 3
    assert f2.dates == []
    for d in (date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)):
        assert store.has_day(tmp_path, d)  # all 3 days persisted, not just the last


class _RaisingOnDateFetcher:
    """Raises `NotYetPublished` for exactly one configured date; serves a
    normal one-day frame (tagged with the requested date, like `StubFetcher`)
    for every other date."""

    def __init__(self, raise_on: date):
        self._raise_on = raise_on
        self.dates: list[date] = []

    def fetch_raw(self, d: date) -> FetchResult:
        self.dates.append(d)
        if d == self._raise_on:
            raise NotYetPublished(f"404 for {d.isoformat()}")
        df = RAW.copy()
        df["TradDt"] = d.isoformat()
        return FetchResult(df, "nse-udiff")


def test_backfill_non_final_day_404_is_failed_not_not_yet(tmp_path: Path, monkeypatch):
    """Consistency fold with the catch-up loop (G2 Task 4): every backfill day
    except the most-recent/final one is, by construction, strictly in the
    past relative to `end` -- a 404 there means NSE's archive genuinely has a
    hole, not that the day is running late. Only the final day keeps
    `not_yet` lateness semantics."""
    from pipeline import config
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    # 3 trading days ending 2026-07-03 (Fri): 2026-07-01, 07-02, 07-03 (final).
    non_final_day = date(2026, 7, 1)
    f = _RaisingOnDateFetcher(raise_on=non_final_day)
    out = backfill(datasets_spec(tmp_path), date(2026, 7, 3), 3, fetcher=f,
                   holidays=HOLIDAYS, sleep=_no_sleep)
    by_date = {s.date: s for s in out}
    assert by_date[non_final_day].status == "failed"
    assert "archive missing for past trading day" in by_date[non_final_day].message
    assert by_date[date(2026, 7, 3)].status == "success"  # final day unaffected


def test_backfill_final_day_404_is_not_yet(tmp_path: Path, monkeypatch):
    """The final/most-recent backfill day keeps `not_yet` lateness semantics
    -- a 404 there is ordinary lateness (the bhavcopy for the most recent
    requested day just isn't out yet), same as the catch-up loop's target
    day."""
    from pipeline import config
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))
    final_day = date(2026, 7, 3)
    f = _RaisingOnDateFetcher(raise_on=final_day)
    out = backfill(datasets_spec(tmp_path), date(2026, 7, 3), 3, fetcher=f,
                   holidays=HOLIDAYS, sleep=_no_sleep)
    by_date = {s.date: s for s in out}
    assert by_date[final_day].status == "not_yet"


def test_backfill_reuses_one_cache_across_the_whole_run(tmp_path, monkeypatch):
    seen_caches: list[object] = []
    real_read_year = store._read_year

    def spying_read_year(base, year, prefix="ohlc", *, columns=None, cache=None):
        seen_caches.append(cache)
        return real_read_year(base, year, prefix, columns=columns, cache=cache)

    monkeypatch.setattr(store, "_read_year", spying_read_year)

    class _StubFetcher:
        def fetch_raw(self, d):
            df = pd.DataFrame({
                "TradDt": [d.isoformat()], "ISIN": ["INE1"], "TckrSymb": ["A"],
                "SctySrs": ["EQ"], "FinInstrmTp": ["STK"], "SsnId": ["F1"],
                "OpnPric": [1.0], "HghPric": [1.0], "LwPric": [1.0], "ClsPric": [1.0],
                "PrvsClsgPric": [1.0], "TtlTradgVol": [1], "TtlTrfVal": [1.0],
                "TtlNbOfTxsExctd": [1],
            })
            return FetchResult(df, "nse-udiff")

    spec = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path, abs_rowcount_range=(0, 10**9))
    backfill(
        spec, date(2026, 7, 3), 2, fetcher=_StubFetcher(), holidays=set(),
        sleep=lambda s: None,
    )

    non_none = [c for c in seen_caches if c is not None]
    assert non_none, "expected at least one cache-bearing _read_year call"
    assert len({id(c) for c in non_none}) == 1  # every call shared the SAME cache instance


def test_backfill_result_is_cache_instance_independent(tmp_path, monkeypatch):
    """NOTE on naming: this does NOT compare a cached run against an
    uncached one -- `backfill()` always builds its own internal
    `store.ReadCache()` (see `backfill.py`), so there is no uncached mode to
    invoke here; both runs below are cached, just via two independently
    constructed `ReadCache` instances (one per `tmp_path` subdirectory). What
    this actually proves: `backfill`'s result (statuses, symbol counts,
    stored bytes) does not depend on WHICH `ReadCache` instance happens to
    back a given run -- i.e. the cache is a pure, per-run implementation
    detail with no cross-run or instance-identity leakage. For a genuine
    cached-vs-uncached (cache=None) equivalence check that also exercises a
    real trailing-window threshold straddle, see
    `test_run_daily_cached_and_uncached_loops_produce_identical_results`
    below, which drives `run_daily` directly instead of going through
    `backfill`."""
    from pipeline import config
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))

    base_uncached = tmp_path / "uncached"
    base_cached = tmp_path / "cached"

    f_uncached = StubFetcher()
    out_uncached = backfill(
        datasets_spec(base_uncached), date(2026, 7, 3), 3, fetcher=f_uncached,
        holidays=HOLIDAYS, sleep=_no_sleep,
    )

    f_cached = StubFetcher()
    out_cached = backfill(
        datasets_spec(base_cached), date(2026, 7, 3), 3, fetcher=f_cached,
        holidays=HOLIDAYS, sleep=_no_sleep,
    )

    assert [s.status for s in out_uncached] == [s.status for s in out_cached]
    assert [s.symbol_count for s in out_uncached] == [s.symbol_count for s in out_cached]

    for d in (date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)):
        assert store.has_day(base_uncached, d) == store.has_day(base_cached, d)
        assert store.day_series_counts(base_uncached, d) == store.day_series_counts(base_cached, d)

    for year_file in sorted(base_uncached.glob("*.parquet")):
        cached_file = base_cached / year_file.name
        assert cached_file.exists()
        df_uncached = pd.read_parquet(year_file)
        df_cached = pd.read_parquet(cached_file)
        pd.testing.assert_frame_equal(
            df_uncached.reset_index(drop=True), df_cached.reset_index(drop=True)
        )


def _eq_rows(d: str, n: int) -> pd.DataFrame:
    """One stored day's EQ rows, keyed K00000.. -- the SAME instrument_key
    scheme `_eq_raw_frame` below derives from its ISIN column, so a
    pre-seeded/short stored day and a later re-fetch describe the SAME
    underlying universe and append_keyed's keep="last" dedupe genuinely
    merges/overwrites rather than landing disjoint rows."""
    return pd.DataFrame([{
        "date": pd.Timestamp(d), "instrument_key": f"K{i:05d}", "isin": f"K{i:05d}",
        "symbol": f"S{i}", "series": "EQ",
        "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "prevclose": 100.0,
        "volume": 1, "value": 1.0, "trades": 1, "source": "seed",
    } for i in range(n)])[config.CANON_COLUMNS]


def _eq_raw_frame(d: str, n: int) -> pd.DataFrame:
    """Raw UDiFF-shaped frame for `n` EQ rows keyed K00000.. (matches
    `_eq_rows`'s instrument_key scheme after normalization) -- what a fetcher
    would actually serve for day `d`."""
    return pd.DataFrame([{
        "TradDt": d, "FinInstrmTp": "STK", "ISIN": f"K{i:05d}", "TckrSymb": f"S{i}",
        "SctySrs": "EQ", "SsnId": "F1", "OpnPric": 100, "HghPric": 101, "LwPric": 99,
        "ClsPric": 100, "PrvsClsgPric": 100, "TtlTradgVol": 1, "TtlTrfVal": 1,
        "TtlNbOfTxsExctd": 1,
    } for i in range(n)])


class _FixedDayFetcher:
    """Serves a fixed raw frame for one specific date; asserts if asked for
    any other date (so an unexpected extra fetch fails loudly, not silently)."""

    def __init__(self, day: date, raw: pd.DataFrame):
        self._day, self._raw = day, raw

    def fetch_raw(self, d: date) -> FetchResult:
        assert d == self._day, f"fetcher asked for unexpected date {d}"
        return FetchResult(self._raw, "nse-udiff")


def test_run_daily_cached_and_uncached_loops_produce_identical_results(
    tmp_path: Path, monkeypatch
):
    """Genuine cached-vs-uncached equivalence (the crux's literal ask, not
    just the cache-instance-independence check above): drives `run_daily`
    directly, in a loop, two ways over the SAME >=4-day sequence -- (A)
    `cache=None` on every call (today's always-read-fresh default), and (B)
    one `store.ReadCache()` constructed once and threaded through every call
    (exactly how `backfill`/the catch-up window use it) -- and asserts both
    produce byte-identical stored parquet and an identical `RunStatus`
    sequence.

    This is calibrated to be a REAL threshold straddle, not just two
    parallel happy-path runs: day 4's idempotency-gate outcome depends on
    whether day 3's row count -- written by `run_daily` itself, mid-loop,
    not pre-seeded -- is visible in the trailing computation `run_daily`
    reads BEFORE deciding whether to re-fetch day 4.

    The calibration: days 1-2 (06-29, 06-30) are pre-seeded at 100 EQ rows
    each (identically, before either loop runs). Day 3 (07-01) is fetched
    through `run_daily` inside the loop with 106 EQ rows -- this write must
    land in the SAME year file trailing lookups read from (all 4 days fall
    in 2026). Day 4 (07-02) is pre-seeded (before either loop runs) at a
    short 86 EQ rows -- a previously-recorded, incomplete day. When
    `run_daily` gates day 4:
      - trailing mean WITH day 3 visible = mean(100, 100, 106) = 102;
        0.85 * 102 = 86.7 -> stored 86 < 86.7 -> judged SHORT -> re-fetch
        triggered (a genuine re-ingest, "success" with a "re-ingested short
        day" message, topped up to 106 rows).
      - trailing mean WITHOUT day 3 (the staleness failure mode this test
        would actually catch: a cache that fails to invalidate on write, or
        that serves the pre-day-3 snapshot) = mean(100, 100) = 100;
        0.85 * 100 = 85 -> stored 86 >= 85 -> wrongly judged COMPLETE ->
        wrongly skipped, no fetch, day 4 stays at 86 rows forever.
    Both loops must land on the FIRST outcome (86.7 threshold, re-fetch) for
    the assertions below to hold; a regression in `append_keyed`'s
    invalidate-on-write contract (see `store.ReadCache`'s docstring) would
    make loop B silently diverge onto the second outcome while loop A (which
    never caches anything) stays correct -- exactly the gap a raw
    "backfill vs backfill" comparison (both internally cached) cannot see.
    """
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (1, 9999))

    day1, day2, day3, day4 = (
        date(2026, 6, 29), date(2026, 6, 30), date(2026, 7, 1), date(2026, 7, 2),
    )
    assert [day1.year, day2.year, day3.year, day4.year] == [2026] * 4  # same year file

    def _run_sequence(base: Path, *, use_shared_cache: bool) -> list[RunStatus]:
        spec = datasets_spec(base)
        cache = store.ReadCache() if use_shared_cache else None
        # Pre-seed days 1, 2, and the short day 4 -- identical for both
        # loop variants, and done via direct store writes (not run_daily),
        # so it is pure fixture setup, not part of what's under test.
        store.append_day(_eq_rows(day1.isoformat(), 100), base)
        store.append_day(_eq_rows(day2.isoformat(), 100), base)
        store.append_day(_eq_rows(day4.isoformat(), 86), base)

        results = []
        # Day 3: goes through run_daily for real -- this write must be
        # visible to day 4's trailing computation below for the straddle
        # to resolve toward re-fetch, exactly as the docstring calibrates.
        results.append(run_daily(
            spec, day3,
            fetcher=_FixedDayFetcher(day3, _eq_raw_frame(day3.isoformat(), 106)),
            holidays=HOLIDAYS, is_target_day=False, cache=cache,
        ))
        # Day 4: pre-seeded short (86); the fetcher below serves the correct
        # full re-fetch (106, matching day 3 -- same K-keyed universe) so a
        # CORRECT re-ingest completes deterministically. If the cache were
        # stale, run_daily would instead skip WITHOUT calling this fetcher
        # at all -- which does not raise here (unlike the AssertionError-
        # exc StubFetcher pattern elsewhere), so the divergence surfaces via
        # the status/symbol_count/stored-bytes assertions below, not a
        # fetcher-side crash.
        results.append(run_daily(
            spec, day4,
            fetcher=_FixedDayFetcher(day4, _eq_raw_frame(day4.isoformat(), 106)),
            holidays=HOLIDAYS, is_target_day=True, cache=cache,
        ))
        return results

    base_uncached = tmp_path / "uncached"
    base_shared_cache = tmp_path / "shared_cache"

    out_uncached = _run_sequence(base_uncached, use_shared_cache=False)
    out_shared = _run_sequence(base_shared_cache, use_shared_cache=True)

    # The straddle actually resolved the way the calibration above predicts
    # -- day 4 was judged short and genuinely re-fetched in BOTH loops. If
    # this ever shows "skipped_idempotent" instead, the calibration itself
    # (not the cache) has drifted and needs re-deriving, since that would
    # make the rest of this test vacuous (nothing to distinguish cached from
    # uncached if day 4 never re-fetches in the first place).
    assert out_uncached[1].status == "success"
    assert "re-ingested short day" in out_uncached[1].message

    # The actual equivalence check: identical RunStatus sequences...
    assert out_uncached == out_shared

    # ...and byte-identical stored parquet for every year file produced.
    uncached_years = sorted(base_uncached.glob("*.parquet"))
    assert uncached_years, "expected at least one year file to have been written"
    for year_file in uncached_years:
        shared_file = base_shared_cache / year_file.name
        assert shared_file.exists()
        df_uncached = pd.read_parquet(year_file)
        df_shared = pd.read_parquet(shared_file)
        pd.testing.assert_frame_equal(
            df_uncached.reset_index(drop=True), df_shared.reset_index(drop=True)
        )

    # Explicit confirmation of the topped-up count on both sides (not just
    # "some bytes matched") -- day 4 grew from the pre-seeded 86 to the full
    # 106 in both runs.
    assert sum(store.day_series_counts(base_uncached, day4).values()) == 106
    assert sum(store.day_series_counts(base_shared_cache, day4).values()) == 106
