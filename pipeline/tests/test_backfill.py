import dataclasses
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config, datasets, store
from pipeline.backfill import backfill
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


def test_backfill_cached_result_matches_uncached_result(tmp_path, monkeypatch):
    """Correctness carry-over from T1's review: the cache invalidates on
    every append_keyed write, so within backfill's loop, day N's run_daily
    writes day N to the year file (invalidating that year in the shared
    cache), then day N+1's run_daily reads trailing counts -- which MUST
    include day N. A shared-instance assertion alone would not catch a
    staleness regression (e.g. a cache that fails to invalidate, or that
    returns a stale snapshot) -- only an end-to-end equivalence check does:
    a 3-day backfill must produce the SAME stored bytes / same per-series
    counts whether or not a cache is threaded through."""
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
