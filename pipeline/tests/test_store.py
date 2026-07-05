import os
import time
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config
from pipeline.store import (
    append_day,
    day_series_counts,
    has_day,
    read_trailing_window,
    sweep_orphan_tmp,
)


def _day(d: str, close: float, key: str = "INE002A01018") -> pd.DataFrame:
    return pd.DataFrame([{
        "date": pd.Timestamp(d), "instrument_key": key, "isin": key,
        "symbol": "RELIANCE", "series": "EQ",
        "open": close, "high": close, "low": close, "close": close,
        "prevclose": close, "volume": 1, "value": 1.0, "trades": 1,
        "source": "nse-udiff",
    }])[config.CANON_COLUMNS]


def test_append_creates_year_file_and_has_day(tmp_path: Path):
    assert has_day(tmp_path, date(2026, 7, 3)) is False
    append_day(_day("2026-07-03", 3000), tmp_path)
    assert config.ohlc_path(2026, tmp_path).exists()
    assert has_day(tmp_path, date(2026, 7, 3)) is True


def test_reappending_same_day_dedupes_keep_last(tmp_path: Path):
    append_day(_day("2026-07-03", 3000), tmp_path)
    append_day(_day("2026-07-03", 3050), tmp_path)  # corrected value, same (date,key)
    out = pd.read_parquet(config.ohlc_path(2026, tmp_path))
    assert len(out) == 1
    assert out.iloc[0]["close"] == 3050.0


def test_trailing_window_reads_across_year_boundary(tmp_path: Path):
    append_day(_day("2025-12-31", 100), tmp_path)
    append_day(_day("2026-01-01", 101), tmp_path)
    append_day(_day("2026-01-02", 102), tmp_path)
    out = read_trailing_window(tmp_path, date(2026, 1, 2), 2)
    assert sorted(out["close"]) == [101.0, 102.0]  # last 2 dates, spanning files


def test_append_day_routes_rows_spanning_two_years_in_one_call(tmp_path: Path):
    # A single append_day call whose DataFrame straddles a year boundary must
    # route each row to the correct year file (the point of groupby-by-year).
    df = pd.concat(
        [_day("2025-12-31", 100, "K1"), _day("2026-01-01", 101, "K1")],
        ignore_index=True,
    )
    append_day(df, tmp_path)
    assert config.ohlc_path(2025, tmp_path).exists()
    assert config.ohlc_path(2026, tmp_path).exists()
    y2025 = pd.read_parquet(config.ohlc_path(2025, tmp_path))
    y2026 = pd.read_parquet(config.ohlc_path(2026, tmp_path))
    assert list(y2025["close"]) == [100.0]
    assert list(y2026["close"]) == [101.0]


def test_trailing_window_when_prior_year_file_absent(tmp_path: Path):
    # end.year=2025 reads 2024 (absent) + 2025 (present); the empty prior-year
    # frame must be filtered out cleanly and the 2025 row returned.
    append_day(_day("2025-12-31", 100, "K1"), tmp_path)
    out = read_trailing_window(tmp_path, date(2025, 12, 31), 2)
    assert list(out["close"]) == [100.0]


def test_append_day_is_atomic_on_write_crash(tmp_path, monkeypatch):
    from pathlib import Path

    import pytest

    def frame(day: str) -> pd.DataFrame:
        row = {c: ["x"] for c in config.CANON_COLUMNS}
        df = pd.DataFrame(row)
        df["date"] = pd.to_datetime([day])
        df["instrument_key"] = ["INE1"]
        return df

    append_day(frame("2026-07-02"), tmp_path)
    good = config.ohlc_path(2026, tmp_path).read_bytes()

    original = pd.DataFrame.to_parquet

    def boom(self, path, *a, **kw):  # crash mid-write: leave a torn tmp file
        Path(str(path)).write_bytes(b"torn")
        raise OSError("disk full")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)
    with pytest.raises(OSError):
        append_day(frame("2026-07-03"), tmp_path)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", original)

    # The published year file is untouched by the crashed write.
    assert config.ohlc_path(2026, tmp_path).read_bytes() == good


def test_write_delta_and_prune(tmp_path):
    from datetime import date, timedelta

    import pandas as pd

    from pipeline import config, store

    def frame(day: str) -> pd.DataFrame:
        df = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
        df["date"] = pd.to_datetime([day])
        return df

    start = date(2026, 1, 1)
    for i in range(40):
        d = start + timedelta(days=i)
        p = store.write_delta(frame(d.isoformat()), tmp_path, d, keep=35)
        assert p.exists() and p.parent.name == "deltas"

    deltas = store.list_deltas(tmp_path)
    assert len(deltas) == 35                          # pruned to keep
    assert deltas[0].name == "ohlc_2026-01-06.parquet"  # oldest 5 pruned
    assert deltas[-1].name == "ohlc_2026-02-09.parquet"


def test_day_series_counts(tmp_path: Path):
    from pipeline.store import day_series_counts

    assert day_series_counts(tmp_path, date(2026, 7, 3)) == {}

    def row(key: str, series: str) -> pd.DataFrame:
        return pd.DataFrame([{
            "date": pd.Timestamp("2026-07-03"), "instrument_key": key, "isin": key,
            "symbol": key, "series": series,
            "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
            "prevclose": 1.0, "volume": 1, "value": 1.0, "trades": 1,
            "source": "nse-udiff",
        }])[config.CANON_COLUMNS]

    append_day(pd.concat([
        row("K1", "EQ"), row("K2", "EQ"), row("K3", "BE"),
    ], ignore_index=True), tmp_path)
    assert day_series_counts(tmp_path, date(2026, 7, 3)) == {"EQ": 2, "BE": 1}


def test_day_series_counts_respects_prefix(tmp_path: Path):
    from pipeline.store import day_series_counts

    def row(key: str, series: str) -> pd.DataFrame:
        return pd.DataFrame([{
            "date": pd.Timestamp("2026-07-03"), "instrument_key": key, "isin": key,
            "symbol": key, "series": series,
            "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
            "prevclose": 1.0, "volume": 1, "value": 1.0, "trades": 1,
            "source": "nse-udiff",
        }])[config.CANON_COLUMNS]

    append_day(row("NIFTY50", "INDEX"), tmp_path, prefix="indices")
    assert day_series_counts(tmp_path, date(2026, 7, 3)) == {}
    assert day_series_counts(tmp_path, date(2026, 7, 3), prefix="indices") == {"INDEX": 1}


def test_prefix_writes_independent_dataset_files(tmp_path):
    def frame(day: str, key: str) -> pd.DataFrame:
        df = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
        df["date"] = pd.to_datetime([day])
        df["instrument_key"] = [key]
        return df

    append_day(frame("2026-07-03", "INE1"), tmp_path)
    append_day(frame("2026-07-03", "NIFTY50"), tmp_path, prefix="indices")

    assert (tmp_path / "ohlc_2026.parquet").exists()
    assert (tmp_path / "indices_2026.parquet").exists()
    # No cross-contamination: each file holds only its own dataset's row.
    assert sum(day_series_counts(tmp_path, date(2026, 7, 3)).values()) == 1
    assert sum(day_series_counts(tmp_path, date(2026, 7, 3), prefix="indices").values()) == 1
    assert has_day(tmp_path, date(2026, 7, 3), prefix="indices")
    w = read_trailing_window(tmp_path, date(2026, 7, 3), 5, prefix="indices")
    assert list(w["instrument_key"]) == ["NIFTY50"]


def test_append_keyed_with_non_canonical_columns_round_trips(tmp_path: Path):
    """append_keyed is column-agnostic -- it needs only `date` + key_cols, not
    the full CANON_COLUMNS schema (G1b task 7 generalization)."""
    from pipeline.store import append_keyed

    df = pd.DataFrame([
        {"date": pd.Timestamp("2026-07-03"), "instrument_key": "K1",
         "close_prev": 1000.0, "prevclose_today": 500.0, "implied_ratio": 2.0},
    ])
    append_keyed(df, tmp_path, prefix="ca_flags")

    out_path = tmp_path / "ca_flags_2026.parquet"
    assert out_path.exists()
    out = pd.read_parquet(out_path)
    assert list(out.columns) == ["date", "instrument_key", "close_prev",
                                  "prevclose_today", "implied_ratio"]
    assert len(out) == 1
    assert out.iloc[0]["implied_ratio"] == 2.0


def test_append_keyed_dedupes_on_custom_key_cols(tmp_path: Path):
    from pipeline.store import append_keyed

    def row(ratio: float) -> pd.DataFrame:
        return pd.DataFrame([{
            "date": pd.Timestamp("2026-07-03"), "instrument_key": "K1",
            "implied_ratio": ratio,
        }])

    append_keyed(row(2.0), tmp_path, prefix="ca_flags")
    append_keyed(row(2.5), tmp_path, prefix="ca_flags")  # corrected re-run, same key

    out = pd.read_parquet(tmp_path / "ca_flags_2026.parquet")
    assert len(out) == 1
    assert out.iloc[0]["implied_ratio"] == 2.5  # keep=last


def test_append_keyed_respects_custom_key_cols_param(tmp_path: Path):
    """Passing a different key_cols tuple dedupes on those columns instead of
    the (date, instrument_key) default."""
    from pipeline.store import append_keyed

    df1 = pd.DataFrame([{"date": pd.Timestamp("2026-07-03"), "id": "A", "v": 1}])
    df2 = pd.DataFrame([{"date": pd.Timestamp("2026-07-03"), "id": "A", "v": 2}])
    append_keyed(df1, tmp_path, prefix="custom", key_cols=("date", "id"))
    append_keyed(df2, tmp_path, prefix="custom", key_cols=("date", "id"))

    out = pd.read_parquet(tmp_path / "custom_2026.parquet")
    assert len(out) == 1
    assert out.iloc[0]["v"] == 2


def test_append_day_is_thin_wrapper_over_append_keyed(tmp_path: Path):
    """append_day's existing behavior (CANON_COLUMNS empty-frame default,
    dedupe on (date, instrument_key)) must be unchanged after the
    generalization -- this pins that contract explicitly."""
    df = pd.DataFrame([{
        "date": pd.Timestamp("2026-07-03"), "instrument_key": "INE1", "isin": "INE1",
        "symbol": "AAA", "series": "EQ",
        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
        "prevclose": 1.0, "volume": 1, "value": 1.0, "trades": 1,
        "source": "nse-udiff",
    }])[config.CANON_COLUMNS]
    append_day(df, tmp_path)
    out = pd.read_parquet(config.ohlc_path(2026, tmp_path))
    assert list(out.columns) == config.CANON_COLUMNS


# -- sweep_orphan_tmp (G2 task 8 hygiene: orphaned crash-write .tmp files) --
#
# A crash mid-write (process killed, disk full, etc.) between "write to
# `*.parquet.tmp`" and "atomic replace" can leave a torn `.tmp` sibling
# behind forever -- `append_keyed`'s own atomic-write pattern always writes
# to `target.with_suffix(".parquet.tmp")` first (see the crash-atomicity
# test above), so this is exactly the file shape a real crash leaves. Left
# alone indefinitely, these accumulate as disk-space litter; a fresh
# in-flight `.tmp` (the CURRENT write, not yet replaced) must never be swept
# out from under an in-progress write.

def _touch_tmp(path: Path, *, hours_old: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"torn")
    stamp = time.time() - hours_old * 3600
    os.utime(path, (stamp, stamp))


def test_sweep_orphan_tmp_removes_stale_tmp_file(tmp_path: Path):
    stale = tmp_path / "ohlc_2026.parquet.tmp"
    _touch_tmp(stale, hours_old=25)  # older than the 24h default threshold
    count = sweep_orphan_tmp(tmp_path)
    assert count == 1
    assert not stale.exists()


def test_sweep_orphan_tmp_spares_fresh_tmp_file(tmp_path: Path):
    fresh = tmp_path / "ohlc_2026.parquet.tmp"
    _touch_tmp(fresh, hours_old=1)  # well within the 24h default threshold
    count = sweep_orphan_tmp(tmp_path)
    assert count == 0
    assert fresh.exists()


def test_sweep_orphan_tmp_respects_custom_threshold(tmp_path: Path):
    borderline = tmp_path / "ohlc_2026.parquet.tmp"
    _touch_tmp(borderline, hours_old=2)
    # 2h old, default 24h threshold -> spared.
    assert sweep_orphan_tmp(tmp_path) == 0
    assert borderline.exists()
    # Same file, 1h threshold -> now stale -> swept.
    assert sweep_orphan_tmp(tmp_path, older_than_hours=1) == 1
    assert not borderline.exists()


def test_sweep_orphan_tmp_recurses_into_deltas_subdirectory(tmp_path: Path):
    stale_delta = tmp_path / "deltas" / "ohlc_2026-07-01.parquet.tmp"
    _touch_tmp(stale_delta, hours_old=48)
    count = sweep_orphan_tmp(tmp_path)
    assert count == 1
    assert not stale_delta.exists()


def test_sweep_orphan_tmp_ignores_non_tmp_files_regardless_of_age(tmp_path: Path):
    real_file = tmp_path / "ohlc_2026.parquet"
    _touch_tmp(real_file, hours_old=999)  # ancient, but not a .tmp file
    count = sweep_orphan_tmp(tmp_path)
    assert count == 0
    assert real_file.exists()


def test_sweep_orphan_tmp_sweeps_multiple_stale_and_spares_fresh(tmp_path: Path):
    stale_a = tmp_path / "ohlc_2026.parquet.tmp"
    stale_b = tmp_path / "deltas" / "ohlc_2026-07-02.parquet.tmp"
    fresh = tmp_path / "indices_2026.parquet.tmp"
    _touch_tmp(stale_a, hours_old=30)
    _touch_tmp(stale_b, hours_old=100)
    _touch_tmp(fresh, hours_old=0.5)
    count = sweep_orphan_tmp(tmp_path)
    assert count == 2
    assert not stale_a.exists()
    assert not stale_b.exists()
    assert fresh.exists()


def test_sweep_orphan_tmp_returns_zero_when_base_does_not_exist(tmp_path: Path):
    missing = tmp_path / "never-created"
    assert sweep_orphan_tmp(missing) == 0


def test_sweep_orphan_tmp_never_raises_on_unlink_error(tmp_path: Path, monkeypatch, capsys):
    """Best-effort: an unlink failure (permissions, file vanished from under
    us in a race, a locked file on some filesystem, etc.) must never crash
    the caller -- append_day/append_keyed call this at their own entry, and
    a sweep hiccup must never block an otherwise-healthy ingest write. The
    failure is warned to stderr, not raised, and other stale files still get
    swept in the same pass."""
    stale_ok = tmp_path / "ohlc_2026.parquet.tmp"
    stale_boom = tmp_path / "indices_2026.parquet.tmp"
    _touch_tmp(stale_ok, hours_old=48)
    _touch_tmp(stale_boom, hours_old=48)

    original_unlink = Path.unlink

    def flaky_unlink(self: Path, *a: object, **kw: object) -> None:
        if self.name == "indices_2026.parquet.tmp":
            raise OSError("simulated: file busy")
        return original_unlink(self, *a, **kw)

    monkeypatch.setattr(Path, "unlink", flaky_unlink)
    count = sweep_orphan_tmp(tmp_path)  # must not raise

    assert not stale_ok.exists()       # the sweepable one still got removed
    assert stale_boom.exists()         # the flaky one is left in place, not lost
    assert count == 1                  # only the successful unlink is counted
    err = capsys.readouterr().err
    assert "indices_2026.parquet.tmp" in err  # names the file that failed


def test_append_keyed_sweeps_stale_orphan_tmp_on_entry(tmp_path: Path):
    """sweep_orphan_tmp is called at append_keyed's own entry -- a stale
    orphaned .tmp sitting in the store base from an earlier crashed write is
    cleaned up as a side effect of the very next normal append, with no
    separate maintenance step required."""
    stale = tmp_path / "ca_flags_2026.parquet.tmp"
    _touch_tmp(stale, hours_old=48)

    df = pd.DataFrame([{"date": pd.Timestamp("2026-07-03"), "instrument_key": "K1", "v": 1}])
    from pipeline.store import append_keyed
    append_keyed(df, tmp_path, prefix="ca_flags")

    assert not stale.exists()


def test_append_day_sweeps_stale_orphan_tmp_on_entry(tmp_path: Path):
    """append_day delegates to append_keyed, which performs the sweep -- this
    pins that the sweep-on-entry behavior reaches append_day callers too,
    not just direct append_keyed callers."""
    stale = tmp_path / "ohlc_2026.parquet.tmp"
    _touch_tmp(stale, hours_old=48)

    df = pd.DataFrame([{
        "date": pd.Timestamp("2026-07-03"), "instrument_key": "INE1", "isin": "INE1",
        "symbol": "AAA", "series": "EQ",
        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
        "prevclose": 1.0, "volume": 1, "value": 1.0, "trades": 1,
        "source": "nse-udiff",
    }])[config.CANON_COLUMNS]
    append_day(df, tmp_path)

    assert not stale.exists()


def test_read_cache_serves_repeated_reads_without_touching_disk(tmp_path, monkeypatch):
    from datetime import date

    import pandas as pd

    from pipeline import config, store

    df = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
    df["date"] = pd.to_datetime(["2026-07-03"])
    df["instrument_key"] = ["INE1"]
    store.append_day(df, tmp_path)

    cache = store.ReadCache()
    assert store.has_day(tmp_path, date(2026, 7, 3), cache=cache)  # primes the cache

    calls = {"n": 0}
    real_read_parquet = pd.read_parquet

    def counting_read_parquet(*a, **kw):
        calls["n"] += 1
        return real_read_parquet(*a, **kw)

    monkeypatch.setattr(pd, "read_parquet", counting_read_parquet)
    assert store.has_day(tmp_path, date(2026, 7, 3), cache=cache)
    assert store.day_series_counts(tmp_path, date(2026, 7, 3), cache=cache)
    assert calls["n"] == 0  # both served from cache, zero disk reads


def test_read_cache_is_invalidated_by_append(tmp_path):
    from datetime import date

    import pandas as pd

    from pipeline import config, store

    def frame(day: str, key: str) -> pd.DataFrame:
        df = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
        df["date"] = pd.to_datetime([day])
        df["instrument_key"] = [key]
        return df

    cache = store.ReadCache()
    store.append_day(frame("2026-07-03", "INE1"), tmp_path, cache=cache)
    # Prime the cache with the one-row state, then append a second row under cache.
    assert store.has_day(tmp_path, date(2026, 7, 3), cache=cache)
    store.append_day(frame("2026-07-03", "INE2"), tmp_path, cache=cache)
    # A read through the SAME cache after the second append must see both rows,
    # not the stale one-row snapshot -- proving invalidation, not just a cold cache.
    window = store.read_trailing_window(tmp_path, date(2026, 7, 3), 5, cache=cache)
    assert sorted(window["instrument_key"]) == ["INE1", "INE2"]


def test_read_cache_none_default_is_unchanged_behavior(tmp_path):
    # No cache argument anywhere -- must behave exactly as before this task.
    from datetime import date

    import pandas as pd

    from pipeline import config, store

    df = pd.DataFrame({c: ["x"] for c in config.CANON_COLUMNS})
    df["date"] = pd.to_datetime(["2026-07-03"])
    df["instrument_key"] = ["INE1"]
    store.append_day(df, tmp_path)
    assert store.has_day(tmp_path, date(2026, 7, 3))
    assert store.day_series_counts(tmp_path, date(2026, 7, 3))
