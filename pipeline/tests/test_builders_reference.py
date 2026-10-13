"""Tests for the reference/instruments derived-dataset builder: the
incremental daily merge (build_reference, since 2026-10-13) and the full
rebuild behind the bootstrap (rebuild_reference)."""
from __future__ import annotations

import dataclasses
import functools
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from pipeline import config, datasets, store
from pipeline.daily_update import RunStatus


def _row(
    d: str, key: str, symbol: str, series: str = "EQ", isin: str | None = None,
    nse_token: int | None = None,
) -> dict:
    return {
        "date": pd.Timestamp(d), "instrument_key": key, "isin": isin or key,
        "symbol": symbol, "series": series,
        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
        "prevclose": 1.0, "volume": 1, "value": 1.0, "trades": 1,
        "nse_token": nse_token,
        "source": "nse-udiff",
    }


def _seed_equities_store(base: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows).astype({"nse_token": "Int64"})[config.CANON_COLUMNS]
    store.append_day(df, base, prefix="ohlc")


def _source_spec(base_dir: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.EQUITIES, base_dir=base_dir)


def _target_spec(base_dir: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.REFERENCE, base_dir=base_dir)


def _weekdays_only() -> tuple[set[date], set[date]]:
    """A calendar with no holidays: every weekday trades."""
    return set(), set()


def _weekdays(start: str, n: int) -> list[str]:
    return [d.date().isoformat() for d in pd.bdate_range(start, periods=n)]


def _legacy_full_rebuild(sessions: pd.DataFrame) -> pd.DataFrame:
    """The reference build as it was until 2026-10-13 (whole price history,
    `active` = last_seen among the store's 10 latest distinct dates), kept
    here as the oracle the new builds must reproduce."""
    recent = sorted(sessions["date"].drop_duplicates(), reverse=True)[:10]
    active_dates = set(recent)
    grouped = sessions.groupby(["instrument_key", "symbol", "series"], as_index=False).agg(
        first_seen=("date", "min"), last_seen=("date", "max"), isin=("isin", "last"),
        nse_token=("nse_token", "last"),
    )
    grouped["name"] = grouped["symbol"]
    grouped["status"] = grouped["last_seen"].apply(
        lambda d: "active" if d in active_dates else "inactive"
    )
    grouped["valid_from"] = grouped["first_seen"]
    grouped["valid_to"] = grouped["last_seen"]
    grouped["date"] = grouped["last_seen"]
    cols = [
        "instrument_key", "isin", "symbol", "name", "series", "first_seen",
        "last_seen", "status", "valid_from", "valid_to", "date", "nse_token",
    ]
    return grouped[cols].sort_values(["instrument_key", "first_seen"]).reset_index(drop=True)


def _history() -> list[dict]:
    """30 weekday sessions: a steady stock, a rename, a delisting early on, a
    sentinel key, a listing inside the last week, and a sporadic bond."""
    days = _weekdays("2026-06-01", 30)
    rows: list[dict] = []
    for i, d in enumerate(days):
        rows.append(_row(d, "INE_STEADY", "STEADY"))
        rows.append(_row(d, "INE_REN", "OLDNAME" if i < 20 else "NEWNAME"))
        if i < 3:
            rows.append(_row(d, "INE_GONE", "GONE"))
        if i % 4 == 0:
            rows.append(_row(d, "NSE:WEIRD", "WEIRD", isin="NSE:WEIRD"))
        if i >= 27:
            rows.append(_row(d, "INE_NEW", "NEWCO"))
        if i in (5, 21, 26):
            rows.append(_row(d, "INE_BOND", "STEADY", series="N1"))
    return rows


def test_reference_spec_fields():
    s = datasets.REFERENCE
    assert s.key == "reference"
    assert s.file_prefix == "instruments"
    assert s.base_dir == config.REFERENCE_DIR
    assert s.source_label == "derived"
    assert s.abs_rowcount_range == (0, 10**9)
    assert s.manifest_name == "reference"
    assert s.schema_version == 1
    assert s.derived is True
    assert s.published is True
    assert datasets.DATASETS["reference"] is s
    assert datasets.DATASET_ORDER == [
        "equities", "indices", "reference", "sector_industry",
        "classification_registry", "classification_observations",
        "fundamentals", "index_constituents", "cas_eligible", "fno_freeze_limits",
        "market_calendar", "corporate_actions",
    ]
    # reference leads the published specs: it is the publish's primary.
    assert datasets.published_specs()[0] is s
    with pytest.raises(RuntimeError, match="derived dataset has no fetcher"):
        s.make_fetcher()
    assert s.normalizer(pd.DataFrame({"a": [1]})).equals(pd.DataFrame({"a": [1]}))


def test_reference_registered_in_builders():
    """builders.py stays name-free: the CLI (the allowed edge) is what binds
    source_spec=DATASETS[DATASET_ORDER[0]] and populates BUILDERS["reference"]
    at import time."""
    from pipeline import cli
    assert "reference" in cli.builders.BUILDERS
    bound = cli.builders.BUILDERS["reference"]
    assert isinstance(bound, functools.partial)
    assert bound.func is cli.builders.build_reference
    assert bound.keywords == {"source_spec": datasets.DATASETS[datasets.DATASET_ORDER[0]]}


# ── full rebuild (the bootstrap path) ───────────────────────────────────────


def test_rebuild_reference_basic_row_shape_and_date_column(tmp_path: Path):
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    _seed_equities_store(src_base, [
        _row("2026-07-01", "INE001", "AAA"),
        _row("2026-07-02", "INE001", "AAA"),
    ])

    result = builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 7, 2), source_spec=_source_spec(src_base),
        trading_calendar=_weekdays_only,
    )

    assert isinstance(result, RunStatus)
    assert result.status == "success"
    assert result.source == "derived"
    assert result.symbol_count == 1  # one distinct (instrument_key, symbol, series)

    out = pd.read_parquet(ref_base / "instruments_all.parquet")
    assert list(out.columns) == [
        "instrument_key", "isin", "symbol", "name", "series",
        "first_seen", "last_seen", "status", "valid_from", "valid_to", "date", "nse_token",
    ]
    row = out.iloc[0]
    assert row["instrument_key"] == "INE001"
    assert row["symbol"] == "AAA"
    assert row["name"] == "AAA"
    assert row["series"] == "EQ"
    assert row["first_seen"] == pd.Timestamp("2026-07-01")
    assert row["last_seen"] == pd.Timestamp("2026-07-02")
    assert row["valid_from"] == pd.Timestamp("2026-07-01")
    assert row["valid_to"] == pd.Timestamp("2026-07-02")
    assert row["date"] == pd.Timestamp("2026-07-02")  # REQUIRED for manifest latest_date
    assert row["status"] == "active"


def test_rebuild_reference_rename_window_yields_two_rows(tmp_path: Path):
    """Same instrument_key, symbol renamed on day 3 -> two SCD2 rows with
    correct valid_from/valid_to windows."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    _seed_equities_store(src_base, [
        _row("2026-07-01", "INE001", "OLDNAME"),
        _row("2026-07-02", "INE001", "OLDNAME"),
        _row("2026-07-03", "INE001", "NEWNAME"),  # renamed
    ])

    result = builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 7, 3), source_spec=_source_spec(src_base),
        trading_calendar=_weekdays_only,
    )
    assert result.symbol_count == 2

    out = pd.read_parquet(ref_base / "instruments_all.parquet")
    out = out.sort_values("first_seen").reset_index(drop=True)
    assert list(out["symbol"]) == ["OLDNAME", "NEWNAME"]
    assert out.iloc[0]["valid_from"] == pd.Timestamp("2026-07-01")
    assert out.iloc[0]["valid_to"] == pd.Timestamp("2026-07-02")
    assert out.iloc[1]["valid_from"] == pd.Timestamp("2026-07-03")
    assert out.iloc[1]["valid_to"] == pd.Timestamp("2026-07-03")


def test_rebuild_reference_active_vs_inactive_status(tmp_path: Path):
    """status: active iff last_seen is among the 10 latest trading sessions of
    the calendar, ending at the newest last_seen; else inactive."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    days = _weekdays("2026-06-01", 12)
    rows = [_row(d, "INE_ACTIVE", "ACTIVE") for d in days]
    rows.append(_row(days[0], "INE_GONE", "GONE"))  # 11 sessions before the last
    rows.append(_row(days[2], "INE_EDGE", "EDGE"))  # exactly the 10th latest session
    _seed_equities_store(src_base, rows)

    builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 6, 16), source_spec=_source_spec(src_base),
        trading_calendar=_weekdays_only,
    )

    out = pd.read_parquet(ref_base / "instruments_all.parquet").set_index("instrument_key")
    assert out.loc["INE_ACTIVE", "status"] == "active"
    assert out.loc["INE_EDGE", "status"] == "active"
    assert out.loc["INE_GONE", "status"] == "inactive"


def test_rebuild_reference_active_window_skips_calendar_holidays(tmp_path: Path):
    """A holiday is not a session: the 10-session window reaches one day
    further back across it, exactly as the old store-date rule did (a holiday
    has no rows in the store)."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    days = [d for d in _weekdays("2026-06-01", 12) if d != "2026-06-10"]  # holiday
    rows = [_row(d, "INE_ACTIVE", "ACTIVE") for d in days]
    rows.append(_row(days[1], "INE_EDGE", "EDGE"))  # the 10th latest session (06-02)
    rows.append(_row(days[0], "INE_GONE", "GONE"))  # the 11th (06-01)
    _seed_equities_store(src_base, rows)

    builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 6, 16), source_spec=_source_spec(src_base),
        trading_calendar=lambda: ({date(2026, 6, 10)}, set()),
    )

    out = pd.read_parquet(ref_base / "instruments_all.parquet").set_index("instrument_key")
    assert out.loc["INE_EDGE", "status"] == "active"
    assert out.loc["INE_GONE", "status"] == "inactive"


def test_rebuild_reference_includes_nse_sentinel_keys(tmp_path: Path):
    """Sentinel NSE:<symbol> instrument_keys (null/empty ISIN rows, task 4)
    are treated like any other instrument_key -- included in the output."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    _seed_equities_store(src_base, [
        _row("2026-07-01", "NSE:WEIRD", "WEIRD", isin="NSE:WEIRD"),
    ])

    result = builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 7, 1), source_spec=_source_spec(src_base),
        trading_calendar=_weekdays_only,
    )
    assert result.symbol_count == 1
    out = pd.read_parquet(ref_base / "instruments_all.parquet")
    assert "NSE:WEIRD" in set(out["instrument_key"])


def test_rebuild_reference_equals_the_legacy_full_build(tmp_path: Path):
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    rows = _history()
    _seed_equities_store(src_base, rows)

    builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 7, 10), source_spec=_source_spec(src_base),
        trading_calendar=_weekdays_only,
    )

    sessions = pd.DataFrame(rows).astype({"nse_token": "Int64"})[
        ["date", "instrument_key", "isin", "symbol", "series", "nse_token"]
    ]
    pd.testing.assert_frame_equal(
        pd.read_parquet(ref_base / "instruments_all.parquet"), _legacy_full_rebuild(sessions)
    )


def test_rebuild_reference_empty_store_fails_and_writes_nothing(tmp_path: Path):
    from pipeline import builders

    ref_base = tmp_path / "reference"
    result = builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 7, 1), source_spec=_source_spec(tmp_path / "ohlc"),
        trading_calendar=_weekdays_only,
    )
    assert result.status == "failed"
    assert not (ref_base / "instruments_all.parquet").exists()


def test_rebuild_reference_merges_a_prior_unless_told_not_to(tmp_path: Path):
    """keep_prior keeps versions the price files never saw (first seen after
    they end); keep_prior=False replaces the prior outright."""
    from pipeline import builders

    ref_base = tmp_path / "reference"
    later = tmp_path / "later"
    _seed_equities_store(later, [_row("2026-07-06", "INE_LATE", "LATE")])
    builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 7, 6), source_spec=_source_spec(later),
        trading_calendar=_weekdays_only,
    )

    src_base = tmp_path / "ohlc"
    _seed_equities_store(src_base, [_row("2026-07-01", "INE001", "AAA")])
    builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 7, 6), source_spec=_source_spec(src_base),
        trading_calendar=_weekdays_only,
    )
    out = pd.read_parquet(ref_base / "instruments_all.parquet")
    assert set(out["instrument_key"]) == {"INE001", "INE_LATE"}

    builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 7, 6), source_spec=_source_spec(src_base),
        keep_prior=False, trading_calendar=_weekdays_only,
    )
    out = pd.read_parquet(ref_base / "instruments_all.parquet")
    assert set(out["instrument_key"]) == {"INE001"}


# ── incremental daily merge ─────────────────────────────────────────────────


def _bootstrap_prior(tmp_path: Path, rows: list[dict]) -> Path:
    """Write a prior instrument list built (by the full rebuild) from `rows`."""
    from pipeline import builders

    base = tmp_path / "prior_store"
    _seed_equities_store(base, rows)
    ref_base = tmp_path / "reference"
    status = builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 7, 1), source_spec=_source_spec(base),
        keep_prior=False, trading_calendar=_weekdays_only,
    )
    assert status.status == "success"
    return ref_base


def test_build_reference_incremental_equals_full_rebuild(tmp_path: Path):
    """The merge requirement in miniature: a prior built from the history up
    to a cut, plus a run that fetched only the trailing window, gives exactly
    the old full rebuild over the whole history -- rows, columns, types,
    statuses, first_seen/last_seen."""
    from pipeline import builders

    rows = _history()
    days = sorted({r["date"] for r in rows})
    cut = days[-3]  # prior covers all but the last two sessions
    ref_base = _bootstrap_prior(tmp_path, [r for r in rows if r["date"] <= cut])

    window_start = days[-7]  # the run's 7-session catch-up window
    run_store = tmp_path / "ohlc"
    _seed_equities_store(run_store, [r for r in rows if r["date"] >= window_start])

    result = builders.build_reference(
        _target_spec(ref_base), days[-1].date(), source_spec=_source_spec(run_store),
        trading_calendar=_weekdays_only,
    )
    assert result.status == "success"

    sessions = pd.DataFrame(rows).astype({"nse_token": "Int64"})[
        ["date", "instrument_key", "isin", "symbol", "series", "nse_token"]
    ]
    pd.testing.assert_frame_equal(
        pd.read_parquet(ref_base / "instruments_all.parquet"), _legacy_full_rebuild(sessions)
    )


def test_build_reference_keeps_history_the_window_never_saw(tmp_path: Path):
    from pipeline import builders

    ref_base = _bootstrap_prior(tmp_path, [
        _row("2026-06-01", "INE001", "AAA"),
        _row("2026-06-02", "INE_OLD", "OLD"),
    ])
    run_store = tmp_path / "ohlc"
    _seed_equities_store(run_store, [
        _row("2026-06-29", "INE001", "AAA"),
        _row("2026-06-30", "INE001", "AAA"),
    ])

    result = builders.build_reference(
        _target_spec(ref_base), date(2026, 6, 30), source_spec=_source_spec(run_store),
        trading_calendar=_weekdays_only,
    )

    assert result.status == "success"
    out = pd.read_parquet(ref_base / "instruments_all.parquet").set_index("instrument_key")
    assert out.loc["INE001", "first_seen"] == pd.Timestamp("2026-06-01")  # from the prior
    assert out.loc["INE001", "last_seen"] == pd.Timestamp("2026-06-30")  # from the run
    assert out.loc["INE001", "status"] == "active"
    assert out.loc["INE_OLD", "last_seen"] == pd.Timestamp("2026-06-02")  # kept, untraded
    assert out.loc["INE_OLD", "status"] == "inactive"


def test_build_reference_without_a_prior_fails_closed(tmp_path: Path):
    """Cold start: never a list built from the run's sessions alone."""
    from pipeline import builders

    run_store = tmp_path / "ohlc"
    _seed_equities_store(run_store, [_row("2026-07-01", "INE001", "AAA")])
    ref_base = tmp_path / "reference"

    result = builders.build_reference(
        _target_spec(ref_base), date(2026, 7, 1), source_spec=_source_spec(run_store),
        trading_calendar=_weekdays_only,
    )

    assert result.status == "failed"
    assert "bootstrap-reference" in result.message
    assert not (ref_base / "instruments_all.parquet").exists()


@pytest.mark.parametrize(
    ("prior_bytes", "why"),
    [
        (b"not a parquet file", "unreadable"),
        (pd.DataFrame({"symbol": ["AAA"], "date": [pd.Timestamp("2026-07-01")]}), "lacks columns"),
        ("empty", "empty"),
        ("undated", "undated"),
    ],
)
def test_build_reference_refuses_a_mis_shaped_prior(tmp_path: Path, prior_bytes, why):
    from pipeline import builders

    ref_base = _bootstrap_prior(tmp_path, [_row("2026-06-29", "INE001", "AAA")])
    out_path = ref_base / "instruments_all.parquet"
    good = pd.read_parquet(out_path)
    if isinstance(prior_bytes, bytes):
        out_path.write_bytes(prior_bytes)
    elif isinstance(prior_bytes, pd.DataFrame):
        prior_bytes.to_parquet(out_path, index=False)
    elif prior_bytes == "empty":
        good.iloc[0:0].to_parquet(out_path, index=False)
    else:
        broken = good.copy()
        broken["first_seen"] = pd.NaT
        broken.to_parquet(out_path, index=False)
    before = out_path.read_bytes()

    run_store = tmp_path / "ohlc"
    _seed_equities_store(run_store, [_row("2026-06-30", "INE001", "AAA")])
    result = builders.build_reference(
        _target_spec(ref_base), date(2026, 6, 30), source_spec=_source_spec(run_store),
        trading_calendar=_weekdays_only,
    )

    assert result.status == "failed"
    assert why in result.message
    assert out_path.read_bytes() == before  # nothing written over it


def test_build_reference_refuses_a_truncated_prior(tmp_path: Path):
    """A prior missing more than the allowed share of the versions the run
    fetched is a broken file, not churn: refused, left as it was, so it
    cannot compound day after day."""
    from pipeline import builders

    ref_base = _bootstrap_prior(tmp_path, [_row("2026-06-01", "INE000", "S000")])
    before = (ref_base / "instruments_all.parquet").read_bytes()
    run_store = tmp_path / "ohlc"
    _seed_equities_store(run_store, [
        _row("2026-06-30", f"INE{i:03d}", f"S{i:03d}") for i in range(10)
    ])  # 9 of the 10 versions are unknown to the prior

    result = builders.build_reference(
        _target_spec(ref_base), date(2026, 6, 30), source_spec=_source_spec(run_store),
        trading_calendar=_weekdays_only,
    )

    assert result.status == "failed"
    assert "lacks 9 of the 10 versions" in result.message
    assert (ref_base / "instruments_all.parquet").read_bytes() == before


def test_build_reference_accepts_routine_listing_churn(tmp_path: Path):
    """New listings within the allowed share merge in normally."""
    from pipeline import builders

    known = [_row("2026-06-01", f"INE{i:03d}", f"S{i:03d}") for i in range(20)]
    ref_base = _bootstrap_prior(tmp_path, known)
    run_store = tmp_path / "ohlc"
    _seed_equities_store(run_store, [
        *[_row("2026-06-30", f"INE{i:03d}", f"S{i:03d}") for i in range(20)],
        _row("2026-06-30", "INE_IPO", "IPO"),
    ])

    result = builders.build_reference(
        _target_spec(ref_base), date(2026, 6, 30), source_spec=_source_spec(run_store),
        trading_calendar=_weekdays_only,
    )

    assert result.status == "success"
    assert result.symbol_count == 21
    assert result.message == "1 new versions"


def test_build_reference_with_no_session_fetched_keeps_the_prior(tmp_path: Path):
    """A run that fetched nothing (a holiday target, NSE late on every day)
    leaves the prior byte-for-byte as it was."""
    from pipeline import builders

    ref_base = _bootstrap_prior(tmp_path, [_row("2026-06-29", "INE001", "AAA")])
    before = (ref_base / "instruments_all.parquet").read_bytes()

    result = builders.build_reference(
        _target_spec(ref_base), date(2026, 6, 30), source_spec=_source_spec(tmp_path / "ohlc"),
        trading_calendar=_weekdays_only,
    )

    assert result.status == "skipped_idempotent"
    assert result.symbol_count == 1
    assert (ref_base / "instruments_all.parquet").read_bytes() == before


def test_build_reference_writes_atomically_tmp_and_replace(tmp_path: Path, monkeypatch):
    from pipeline import builders

    ref_base = _bootstrap_prior(tmp_path, [_row("2026-06-29", "INE001", "AAA")])
    run_store = tmp_path / "ohlc"
    _seed_equities_store(run_store, [_row("2026-06-30", "INE001", "AAA")])
    out_path = ref_base / "instruments_all.parquet"
    good = out_path.read_bytes()

    def boom(self, path, *a, **kw):
        Path(str(path)).write_bytes(b"torn")
        raise OSError("disk full")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)
    with pytest.raises(OSError):
        builders.build_reference(
            _target_spec(ref_base), date(2026, 6, 30), source_spec=_source_spec(run_store),
            trading_calendar=_weekdays_only,
        )
    monkeypatch.undo()

    assert out_path.read_bytes() == good  # untouched by the crashed write


def test_build_manifest_picks_up_reference_output(tmp_path: Path):
    from pipeline import builders, manifest

    ref_base = _bootstrap_prior(tmp_path, [_row("2026-07-01", "INE001", "AAA")])
    run_store = tmp_path / "ohlc"
    _seed_equities_store(run_store, [_row("2026-07-02", "INE001", "AAA")])
    target_spec = _target_spec(ref_base)
    builders.build_reference(
        target_spec, date(2026, 7, 2), source_spec=_source_spec(run_store),
        trading_calendar=_weekdays_only,
    )

    m = manifest.build_manifest(
        [target_spec], latest_trading_date=date(2026, 7, 2), generated_at="g",
    )
    (ds,) = m["datasets"]
    assert ds["name"] == "reference"
    assert ds["latest_date"] == "2026-07-02"
    (b,) = ds["baseline"]
    assert b["name"] == "instruments_all.parquet"


# -- nse_token: the exchange-issued id, one per (instrument_key, symbol, series) version --


def _token_of(out: pd.DataFrame, key: str, series: str) -> object:
    row = out[(out["instrument_key"] == key) & (out["series"] == series)].iloc[0]
    return row["nse_token"]


def test_rebuild_reference_carries_the_token_of_each_series_version(tmp_path: Path):
    """A share that moves EQ -> BE is issued a new token: both versions keep
    their own, which is what links the old listing to the new one."""
    import pyarrow.parquet as pq

    from pipeline import builders

    src_base = tmp_path / "ohlc"
    ref_base = tmp_path / "reference"
    _seed_equities_store(src_base, [
        _row("2026-07-01", "INE001", "AAA", series="EQ", nse_token=111),
        _row("2026-07-02", "INE001", "AAA", series="BE", nse_token=222),
        _row("2026-07-02", "INE002", "BBB", series="EQ"),  # token unknown
    ])
    builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 7, 2), source_spec=_source_spec(src_base),
        trading_calendar=_weekdays_only,
    )
    path = ref_base / "instruments_all.parquet"
    out = pd.read_parquet(path)
    assert _token_of(out, "INE001", "EQ") == 111
    assert _token_of(out, "INE001", "BE") == 222
    assert pd.isna(_token_of(out, "INE002", "EQ"))
    # A published contract: a plain nullable int64, whatever the pandas version.
    assert pq.read_schema(path).field("nse_token").type == pa.int64()


def test_build_reference_adds_tokens_to_a_prior_that_has_none(tmp_path: Path):
    """The list published before this column existed is a valid prior: versions
    the run sees gain a token, the rest stay null, nothing else changes."""
    from pipeline import builders

    ref_base = _bootstrap_prior(tmp_path, [
        _row("2026-06-29", "INE001", "AAA"), _row("2026-06-29", "INE_OLD", "OLD"),
    ])
    path = ref_base / "instruments_all.parquet"
    pd.read_parquet(path).drop(columns=["nse_token"]).to_parquet(path, index=False)

    run_store = tmp_path / "ohlc"
    _seed_equities_store(run_store, [_row("2026-06-30", "INE001", "AAA", nse_token=111)])
    result = builders.build_reference(
        _target_spec(ref_base), date(2026, 6, 30), source_spec=_source_spec(run_store),
        trading_calendar=_weekdays_only,
    )
    assert result.status == "success", result.message
    out = pd.read_parquet(path)
    assert _token_of(out, "INE001", "EQ") == 111
    assert pd.isna(_token_of(out, "INE_OLD", "EQ"))


def test_build_reference_keeps_a_recorded_token_when_a_day_has_none(tmp_path: Path):
    """A fallback source has no token: that day must not erase one already
    recorded, and a fresh token replaces a stale one (a re-issue)."""
    from pipeline import builders

    ref_base = _bootstrap_prior(tmp_path, [
        _row("2026-06-29", "INE001", "AAA", nse_token=111),
        _row("2026-06-29", "INE002", "BBB", nse_token=333),
    ])
    run_store = tmp_path / "ohlc"
    _seed_equities_store(run_store, [
        _row("2026-06-30", "INE001", "AAA"),  # fallback day: token unknown
        _row("2026-06-30", "INE002", "BBB", nse_token=444),  # re-issued
    ])
    builders.build_reference(
        _target_spec(ref_base), date(2026, 6, 30), source_spec=_source_spec(run_store),
        trading_calendar=_weekdays_only,
    )
    out = pd.read_parquet(ref_base / "instruments_all.parquet")
    assert _token_of(out, "INE001", "EQ") == 111
    assert _token_of(out, "INE002", "EQ") == 444


def test_bootstrap_rebuild_reads_price_files_that_predate_the_token(tmp_path: Path):
    """The price files left on a release have no nse_token column; the
    bootstrap rebuilds from them, with every token null."""
    from pipeline import builders

    src_base = tmp_path / "ohlc"
    _seed_equities_store(src_base, [_row("2026-07-01", "INE001", "AAA")])
    for f in src_base.glob("ohlc_*.parquet"):
        pd.read_parquet(f).drop(columns=["nse_token"]).to_parquet(f, index=False)
    ref_base = tmp_path / "reference"
    result = builders.rebuild_reference(
        _target_spec(ref_base), date(2026, 7, 1), source_spec=_source_spec(src_base),
        trading_calendar=_weekdays_only,
    )
    assert result.status == "success", result.message
    out = pd.read_parquet(ref_base / "instruments_all.parquet")
    assert out["nse_token"].isna().all() and len(out) == 1
