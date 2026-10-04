"""market_calendar: the one calendar file, its validation, the published rows,
and the builder's fail-closed policy.

The failure this dataset must never produce is a calendar that is WRONG BUT
LOOKS RIGHT to the desktop app: a session with impossible hours, a year
claimed complete that is not, or a published list that silently lost days.
The app fires price alerts by it, so every doubt keeps the prior file.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline import builders, config, datasets, market_calendar

TARGET = date(2026, 10, 5)


def _hm(seconds: int | None) -> str | None:
    return None if seconds is None else f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}"


def _find(rows, venue: str, day: str, kind: str):
    return [
        r for r in rows if r.venue == venue and r.day == date.fromisoformat(day) and r.kind == kind
    ]


def _committed() -> market_calendar.MarketCalendar:
    return market_calendar.load(config.META_DIR / market_calendar.FILENAME)


# ── the committed calendar ──────────────────────────────────────────────────


def test_the_committed_calendar_holds_the_verified_special_sessions():
    rows = _committed().rows()
    # Union Budget, Sunday 2026-02-01: standard hours with the call auction.
    (budget,) = _find(rows, "NSE", "2026-02-01", "session")
    assert (_hm(budget.pre_open_seconds), _hm(budget.open_seconds), _hm(budget.close_seconds)) == (
        "09:00",
        "09:15",
        "15:30",
    )
    # Diwali Muhurat 2025 is a listed closure on both venues; the session replaces it.
    for venue in ("NSE", "BSE"):
        (muhurat,) = _find(rows, venue, "2025-10-21", "session")
        assert (
            _hm(muhurat.pre_open_seconds),
            _hm(muhurat.open_seconds),
            _hm(muhurat.close_seconds),
        ) == (
            "13:30",
            "13:45",
            "14:45",
        )
        assert _find(rows, venue, "2025-10-21", "closed") == []
    # MCX traded 09:00-17:00 on Budget Sunday, with no call auction.
    (mcx,) = _find(rows, "MCX", "2026-02-01", "session")
    assert (mcx.pre_open_seconds, _hm(mcx.open_seconds), _hm(mcx.close_seconds)) == (
        None,
        "09:00",
        "17:00",
    )


def test_a_session_awaiting_its_hours_is_named_and_not_sent_to_the_app():
    calendar = _committed()
    # Muhurat 2026: NSE lists the day; its timings are notified later.
    assert "NSE 2026-11-08" in calendar.sessions_awaiting_hours()
    assert _find(calendar.rows(), "NSE", "2026-11-08", "session") == []
    # ...but the pipeline's own calendar trades it already.
    assert date(2026, 11, 8) in calendar.trading_inputs("NSE")[1]


def test_the_committed_calendar_carries_each_venues_closures_and_partial_days():
    rows = _committed().rows()
    for venue in ("NSE", "BSE"):
        assert len(_find(rows, venue, "2026-10-02", "closed")) == 1, venue
    assert len(_find(rows, "MCX", "2026-12-25", "closed")) == 1
    # MCX morning-session closures run 17:00 to the evening close, which
    # follows US daylight saving (23:30 within it, 23:55 outside it).
    (holi,) = _find(rows, "MCX", "2026-03-03", "session")
    (dussehra,) = _find(rows, "MCX", "2026-10-20", "session")
    assert (_hm(holi.open_seconds), _hm(holi.close_seconds)) == ("17:00", "23:55")
    assert (_hm(dussehra.open_seconds), _hm(dussehra.close_seconds)) == ("17:00", "23:30")


def test_only_verified_years_are_covered():
    rows = _committed().rows()
    covered = {(r.venue, r.day.year) for r in rows if r.kind == "covered"}
    assert covered == {("NSE", 2025), ("NSE", 2026), ("BSE", 2025), ("BSE", 2026), ("MCX", 2026)}
    # Older NSE closures are published, but their years stay unknown to the
    # app: their special sessions' hours are not verified.
    assert _find(rows, "NSE", "2024-01-22", "closed")


# ── validation ──────────────────────────────────────────────────────────────


def _doc(nse_sessions=None, **venues) -> dict:
    doc = {
        "NSE": {
            "source": "nse",
            "coveredYears": [2026],
            "closures": {"2026": ["2026-01-26", "2026-10-02"]},
            "sessions": nse_sessions or [],
        },
        "BSE": {"source": "bse", "closures": {"2026": ["2026-01-26"]}},
        "MCX": {"source": "mcx"},
    }
    doc.update(venues)
    return {"venues": doc}


def _budget(**hours) -> list[dict]:
    return [{"date": "2026-02-01", "label": "budget", **hours}]


@pytest.mark.parametrize(
    ("hours", "message"),
    [
        ({"open": "15:30", "close": "09:15"}, "closes at"),
        ({"preOpen": "09:30", "open": "09:15", "close": "15:30"}, "after the open"),
        ({"open": "9.15", "close": "15:30"}, "HH:MM"),
        ({"open": "09:75", "close": "15:30"}, "time of day"),
        ({"open": "09:15"}, "both open and close"),
    ],
)
def test_impossible_session_hours_are_refused(hours: dict, message: str):
    with pytest.raises(market_calendar.CalendarError, match=message):
        market_calendar.parse(_doc(_budget(**hours)))


def test_a_year_cannot_be_covered_without_its_closures():
    doc = _doc(BSE={"source": "bse", "coveredYears": [2027], "closures": {}})
    with pytest.raises(market_calendar.CalendarError, match="covered year 2027"):
        market_calendar.parse(doc)


def test_a_closure_filed_under_the_wrong_year_is_refused():
    doc = _doc(BSE={"source": "bse", "closures": {"2026": ["2025-12-25"]}})
    with pytest.raises(market_calendar.CalendarError, match="wrong year"):
        market_calendar.parse(doc)


def test_overlapping_sessions_on_one_day_are_refused():
    doc = _doc(
        MCX={
            "source": "mcx",
            "sessions": [
                {"date": "2026-02-01", "open": "09:00", "close": "17:00"},
                {"date": "2026-02-01", "open": "16:00", "close": "18:00"},
            ],
        }
    )
    with pytest.raises(market_calendar.CalendarError, match="overlapping"):
        market_calendar.parse(doc)


def test_two_separate_windows_on_one_day_are_kept():
    doc = _doc(
        MCX={
            "source": "mcx",
            "sessions": [
                {"date": "2026-02-01", "open": "11:30", "close": "12:30"},
                {"date": "2026-02-01", "open": "09:15", "close": "10:00"},
            ],
        }
    )
    rows = market_calendar.parse(doc).rows()
    windows = [
        (_hm(r.open_seconds), _hm(r.close_seconds))
        for r in _find(rows, "MCX", "2026-02-01", "session")
    ]
    assert windows == [("09:15", "10:00"), ("11:30", "12:30")]


def test_every_venue_must_be_present_and_known():
    doc = _doc()
    del doc["venues"]["MCX"]
    with pytest.raises(market_calendar.CalendarError, match="no entry for MCX"):
        market_calendar.parse(doc)
    with pytest.raises(market_calendar.CalendarError, match="unknown venue"):
        market_calendar.parse(_doc(CBOE={"source": "x"}))
    with pytest.raises(market_calendar.CalendarError, match="no source"):
        market_calendar.parse(_doc(MCX={}))


# ── the builder ─────────────────────────────────────────────────────────────


def _meta(tmp: Path, doc: dict | None = None) -> Path:
    meta = tmp / "meta"
    meta.mkdir(exist_ok=True)
    (meta / market_calendar.FILENAME).write_text(
        json.dumps(doc or _doc(_budget(open="09:15", close="15:30")))
    )
    return meta


def _spec(tmp: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.DATASETS["market_calendar"], base_dir=tmp / "calendar")


def _out(tmp: Path) -> Path:
    return tmp / "calendar" / "market_calendar_all.parquet"


def test_the_builder_writes_the_rows_with_the_types_the_app_reads(tmp_path: Path):
    status = builders.build_market_calendar(_spec(tmp_path), TARGET, meta_dir=_meta(tmp_path))
    assert status.status == "success", status.message
    table = pq.read_table(_out(tmp_path))
    assert table.schema.field("day").type == pa.date32()
    assert table.schema.field("open_seconds").type == pa.int32()
    assert table.schema.field("venue").type == pa.string()
    assert table.schema.field("date").type == pa.timestamp("ms")
    frame = table.to_pandas()
    # The as-of date is the run's target (the manifest reads it); `day` is the calendar date.
    assert set(frame["date"].dt.date) == {TARGET}
    session = frame[frame["kind"] == "session"].iloc[0]
    assert (session["venue"], str(session["day"]), session["open_seconds"]) == (
        "NSE",
        "2026-02-01",
        33300,
    )
    assert frame.loc[frame["kind"] == "closed", "open_seconds"].isna().all()


def test_the_builder_names_sessions_awaiting_hours(tmp_path: Path):
    doc = _doc([{"date": "2026-11-08", "label": "muhurat"}])
    status = builders.build_market_calendar(_spec(tmp_path), TARGET, meta_dir=_meta(tmp_path, doc))
    assert status.status == "success"
    assert "awaiting hours: NSE 2026-11-08" in status.message


def test_a_malformed_calendar_keeps_the_prior_file(tmp_path: Path):
    spec = _spec(tmp_path)
    assert (
        builders.build_market_calendar(spec, TARGET, meta_dir=_meta(tmp_path)).status == "success"
    )
    before = _out(tmp_path).read_bytes()
    broken = _meta(tmp_path, _doc(_budget(open="15:30", close="09:15")))
    status = builders.build_market_calendar(spec, TARGET, meta_dir=broken)
    assert status.status == "skipped_idempotent"
    assert "closes at" in status.message
    assert _out(tmp_path).read_bytes() == before


def test_a_malformed_calendar_with_nothing_to_keep_fails(tmp_path: Path):
    broken = _meta(tmp_path, _doc(_budget(open="15:30", close="09:15")))
    assert (
        builders.build_market_calendar(_spec(tmp_path), TARGET, meta_dir=broken).status == "failed"
    )
    assert not _out(tmp_path).exists()


def test_a_calendar_that_lost_days_is_held_back_loudly(tmp_path: Path):
    spec = _spec(tmp_path)
    assert (
        builders.build_market_calendar(spec, TARGET, meta_dir=_meta(tmp_path)).status == "success"
    )
    before = _out(tmp_path).read_bytes()
    shorter = _doc(_budget(open="09:15", close="15:30"))
    shorter["venues"]["NSE"]["closures"]["2026"] = ["2026-01-26"]
    status = builders.build_market_calendar(spec, TARGET, meta_dir=_meta(tmp_path, shorter))
    assert status.status == "failed"
    assert "NSE 2026-10-02 closed" in status.message
    assert _out(tmp_path).read_bytes() == before


def test_the_dataset_is_registered_for_the_daily_run():
    spec = datasets.DATASETS["market_calendar"]
    assert spec.manifest_name == "market_calendar"
    assert spec.derived and not spec.external
    assert "market_calendar" in datasets.DATASET_ORDER
    from pipeline import cli  # noqa: F401 - binds BUILDERS at import

    assert builders.BUILDERS["market_calendar"] is builders.build_market_calendar
