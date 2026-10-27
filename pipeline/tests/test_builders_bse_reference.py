"""bse_reference: BSE's bhavcopy parser and the bse_instruments_all builder.

The list ties BSE's scrip code to the ISIN, so the desktop app can read a
BSE scrip code (the exchange token brokers report) as an alias of the NSE
listing with the same ISIN. What it must never do: move its newest session
past a day it could not read (that day would never be read again), build
over a published list it cannot read, or lose a listing that stopped trading.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import responses

from pipeline import builders, cli, datasets
from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.sources import bse_cm_bhavcopy
from pipeline.sources.bse_cm_bhavcopy import BhavcopyDay, MalformedBhavcopy

# Cut from BSE's real BhavCopy_BSE_CM_0_0_0_20261026_F_0000.CSV, columns
# untouched: SULA, EXCELSOFT and BBOX, a few large caps, SPICEJET (group Z),
# PRISMX (T) and two debt-group (F) ETFs.
FIXTURE = Path(__file__).parent / "fixtures" / "bse_cm_bhavcopy_20261026.csv"
FIXTURE_DAY = date(2026, 10, 26)

# Mon 2026-10-19 .. Fri 2026-10-30, with no closures: tests pick days off it.
SESSIONS = [date(2026, 10, 19) + timedelta(days=i) for i in range(12)]
SESSIONS = [d for d in SESSIONS if d.weekday() < 5]


def _text(day: date = FIXTURE_DAY) -> str:
    return FIXTURE.read_text().replace(FIXTURE_DAY.isoformat(), day.isoformat())


def _payload(day: date = FIXTURE_DAY, text: str | None = None) -> bytes:
    return (text if text is not None else _text(day)).encode()


def _edit(text: str, symbol: str, column: str, value: str) -> str:
    lines = text.splitlines()
    header = lines[0].split(",")
    out = [lines[0]]
    for line in lines[1:]:
        cells = line.split(",")
        if cells[header.index("TckrSymb")] == symbol:
            cells[header.index(column)] = value
        out.append(",".join(cells))
    return "\n".join(out) + "\n"


def _no_closures() -> tuple[set[date], set[date]]:
    return set(), set()


# ── parser ──────────────────────────────────────────────────────────────────


def test_parse_reads_scrip_code_isin_symbol_and_group():
    got = bse_cm_bhavcopy.parse_bhavcopy(_payload(), FIXTURE_DAY)
    df = got.frame.set_index("symbol")
    assert list(got.frame.columns) == bse_cm_bhavcopy.DAY_COLUMNS
    assert df.loc["SULA", "scrip_code"] == 543711
    assert df.loc["SULA", "isin"] == "INE142Q01026"
    assert df.loc["SULA", "group"] == "B"
    assert df.loc["SULA", "name"] == "Sula Vineyards Limited"
    assert df.loc["EXCELSOFT", "scrip_code"] == 544617
    assert df.loc["EXCELSOFT", "isin"] == "INE606N01019"
    assert df.loc["BBOX", "scrip_code"] == 500463
    assert df.loc["BBOX", "isin"] == "INE676A01027"
    assert df.loc["SPICEJET", "group"] == "Z"
    assert got.frame["scrip_code"].dtype == "int64"
    assert (got.frame["date"] == pd.Timestamp(FIXTURE_DAY)).all()
    assert (got.dropped_rows, got.unreadable_isins) == (0, 0)


def test_parse_reads_bse_html_page_as_not_posted():
    """BSE serves its home page with HTTP 200 for a day it has not posted."""
    with pytest.raises(NotYetPublished):
        bse_cm_bhavcopy.parse_bhavcopy(b"<!DOCTYPE html><html>...", FIXTURE_DAY)
    with pytest.raises(NotYetPublished):
        bse_cm_bhavcopy.parse_bhavcopy(b"", FIXTURE_DAY)


def test_parse_refuses_a_file_without_a_required_column():
    text = _text().replace("SctySrs", "Series")
    with pytest.raises(MalformedBhavcopy, match="SctySrs"):
        bse_cm_bhavcopy.parse_bhavcopy(_payload(text=text), FIXTURE_DAY)


def test_parse_refuses_another_days_file():
    with pytest.raises(MalformedBhavcopy, match="dated"):
        bse_cm_bhavcopy.parse_bhavcopy(_payload(), date(2026, 10, 23))


def test_parse_refuses_a_scrip_code_listed_twice():
    text = _edit(_text(), "EXCELSOFT", "FinInstrmId", "543711")
    with pytest.raises(MalformedBhavcopy, match="543711"):
        bse_cm_bhavcopy.parse_bhavcopy(_payload(text=text), FIXTURE_DAY)


def test_parse_drops_an_unkeyable_row_and_nulls_a_bad_isin_counting_both():
    text = _edit(_text(), "ABB", "FinInstrmId", "x500002")
    text = _edit(text, "SULA", "ISIN", "NOTANISIN")
    got = bse_cm_bhavcopy.parse_bhavcopy(_payload(text=text), FIXTURE_DAY)
    df = got.frame.set_index("symbol")
    assert "ABB" not in df.index
    assert df.loc["SULA", "scrip_code"] == 543711
    assert pd.isna(df.loc["SULA", "isin"])
    assert (got.dropped_rows, got.unreadable_isins) == (1, 1)


def test_bhavcopy_url_names_the_day():
    assert bse_cm_bhavcopy.bhavcopy_url(FIXTURE_DAY) == (
        "https://www.bseindia.com/download/BhavCopy/Equity/"
        "BhavCopy_BSE_CM_0_0_0_20261026_F_0000.CSV"
    )


# ── fetch ───────────────────────────────────────────────────────────────────


@responses.activate
def test_fetch_parses_the_file_and_reads_html_as_not_posted(monkeypatch):
    monkeypatch.setattr(builders.time, "sleep", lambda _s: None)
    responses.get(bse_cm_bhavcopy.bhavcopy_url(FIXTURE_DAY), body=_payload())
    got = builders._fetch_bse_bhavcopy(FIXTURE_DAY)
    assert "SULA" in set(got.frame["symbol"])
    assert "Chrome" in responses.calls[0].request.headers["User-Agent"]

    later = date(2026, 10, 27)
    responses.get(bse_cm_bhavcopy.bhavcopy_url(later), body=b"<!DOCTYPE html><html></html>")
    with pytest.raises(NotYetPublished):
        builders._fetch_bse_bhavcopy(later)


@responses.activate
def test_fetch_retries_then_raises_on_a_refusal(monkeypatch):
    monkeypatch.setattr(builders.time, "sleep", lambda _s: None)
    responses.get(bse_cm_bhavcopy.bhavcopy_url(FIXTURE_DAY), status=403)
    with pytest.raises(UnexpectedFailure, match="HTTP 403"):
        builders._fetch_bse_bhavcopy(FIXTURE_DAY)
    assert len(responses.calls) == 3


# ── builder ─────────────────────────────────────────────────────────────────


@pytest.fixture
def spec(tmp_path: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.BSE_REFERENCE, base_dir=tmp_path / "reference")


class FakeBse:
    """BSE's bhavcopies: the fixture for every day, edited per day, with days
    that are not posted (HTML) or broken. Records the days asked for."""

    def __init__(self) -> None:
        self.asked: list[date] = []
        self.not_posted: set[date] = set()
        self.broken: set[date] = set()
        self.edits: dict[date, Callable[[str], str]] = {}

    def __call__(self, day: date) -> BhavcopyDay:
        self.asked.append(day)
        if day in self.not_posted:
            return bse_cm_bhavcopy.parse_bhavcopy(b"<!DOCTYPE html>", day)
        if day in self.broken:
            raise UnexpectedFailure("HTTP 503")
        text = _text(day)
        for edit_day, edit in self.edits.items():
            if day >= edit_day:
                text = edit(text)
        return bse_cm_bhavcopy.parse_bhavcopy(text.encode(), day)


def _build(
    spec: datasets.DatasetSpec, target: date, fake: FakeBse, **kwargs: object
) -> builders.RunStatus:
    kwargs.setdefault("seed_days", 3)
    kwargs.setdefault("min_rows", 1)
    return builders.build_bse_reference(
        spec, target, fetch_day=fake, trading_calendar=_no_closures, **kwargs  # type: ignore[arg-type]
    )


def _out(spec: datasets.DatasetSpec) -> pd.DataFrame:
    return pd.read_parquet(spec.base_dir / "bse_instruments_all.parquet")


def _row(df: pd.DataFrame, symbol: str) -> pd.Series:
    rows = df[df["symbol"] == symbol]
    assert len(rows) == 1, rows
    return rows.iloc[0]


def test_first_build_seeds_the_last_sessions_with_instruments_all_columns(spec):
    fake = FakeBse()
    st = _build(spec, date(2026, 10, 26), fake)

    assert st.status == "success", st.message
    assert fake.asked == [date(2026, 10, 22), date(2026, 10, 23), date(2026, 10, 26)]
    out = _out(spec)
    assert list(out.columns) == [
        "scrip_code", "isin", "symbol", "name", "group", "status",
        "first_seen", "last_seen", "valid_from", "valid_to", "date",
    ]
    sula = _row(out, "SULA")
    assert (sula["scrip_code"], sula["isin"], sula["group"]) == (543711, "INE142Q01026", "B")
    assert sula["first_seen"] == sula["valid_from"] == pd.Timestamp("2026-10-22")
    assert sula["last_seen"] == sula["valid_to"] == sula["date"] == pd.Timestamp("2026-10-26")
    assert sula["status"] == "active"
    assert {"EXCELSOFT", "BBOX"} <= set(out["symbol"])
    assert st.symbol_count == len(out) == 10

    schema = pq.read_schema(spec.base_dir / "bse_instruments_all.parquet")
    assert schema.field("scrip_code").type == pa.int64()
    assert schema.field("first_seen").type == pa.timestamp("ns")
    assert schema.field("isin").type == pa.string()


def test_daily_reads_only_the_sessions_after_the_newest_and_is_idempotent(spec):
    fake = FakeBse()
    _build(spec, date(2026, 10, 23), fake)
    fake.asked.clear()

    st = _build(spec, date(2026, 10, 26), fake)
    assert st.status == "success", st.message
    assert fake.asked == [date(2026, 10, 26)]
    assert _row(_out(spec), "SULA")["last_seen"] == pd.Timestamp("2026-10-26")
    assert _row(_out(spec), "SULA")["first_seen"] == pd.Timestamp("2026-10-21")

    fake.asked.clear()
    st = _build(spec, date(2026, 10, 26), fake)
    assert st.status == "skipped_idempotent"
    assert fake.asked == []


def test_daily_catches_up_every_session_the_producer_missed(spec):
    fake = FakeBse()
    _build(spec, date(2026, 10, 21), fake)
    fake.asked.clear()

    st = _build(spec, date(2026, 10, 26), fake)
    assert st.status == "success", st.message
    assert fake.asked == [date(2026, 10, 22), date(2026, 10, 23), date(2026, 10, 26)]


def test_daily_too_far_behind_fails_and_writes_nothing(spec):
    fake = FakeBse()
    _build(spec, date(2026, 10, 21), fake)
    before = (spec.base_dir / "bse_instruments_all.parquet").read_bytes()
    fake.asked.clear()

    st = _build(spec, date(2026, 10, 26), fake, max_catchup_days=2)
    assert st.status == "failed"
    assert "backfill-bse-instruments" in st.message
    assert fake.asked == []
    assert (spec.base_dir / "bse_instruments_all.parquet").read_bytes() == before


def test_target_not_posted_yet_merges_the_days_before_and_is_not_yet(spec):
    fake = FakeBse()
    _build(spec, date(2026, 10, 21), fake)
    fake.not_posted = {date(2026, 10, 26)}

    st = _build(spec, date(2026, 10, 26), fake)
    assert st.status == "not_yet"
    assert _out(spec)["last_seen"].max() == pd.Timestamp("2026-10-23")

    # Once posted, the next run reads just that day.
    fake.not_posted.clear()
    fake.asked.clear()
    st = _build(spec, date(2026, 10, 26), fake)
    assert st.status == "success", st.message
    assert fake.asked == [date(2026, 10, 26)]


def test_an_unreadable_past_day_stops_the_merge_there_and_fails(spec):
    """The newest session never moves past a hole: the next run reads from it."""
    fake = FakeBse()
    _build(spec, date(2026, 10, 21), fake)
    fake.broken = {date(2026, 10, 23)}
    fake.asked.clear()

    st = _build(spec, date(2026, 10, 26), fake)
    assert st.status == "failed"
    assert "2026-10-23" in st.message
    assert fake.asked == [date(2026, 10, 22), date(2026, 10, 23)]
    assert _out(spec)["last_seen"].max() == pd.Timestamp("2026-10-22")

    fake.broken.clear()
    fake.asked.clear()
    assert _build(spec, date(2026, 10, 26), fake).status == "success"
    assert fake.asked == [date(2026, 10, 23), date(2026, 10, 26)]


def test_a_truncated_file_counts_as_unreadable(spec):
    fake = FakeBse()
    st = _build(spec, date(2026, 10, 26), fake, min_rows=50)
    assert st.status == "failed"
    assert "floor" in st.message
    assert not (spec.base_dir / "bse_instruments_all.parquet").exists()


def test_a_group_or_isin_change_starts_a_new_version_under_the_same_scrip_code(spec):
    fake = FakeBse()
    fake.edits[date(2026, 10, 26)] = lambda t: _edit(
        _edit(t, "PRISMX", "SctySrs", "B"), "SULA", "ISIN", "INE142Q01034"
    )
    _build(spec, date(2026, 10, 26), fake)
    out = _out(spec)

    prismx = out[out["scrip_code"] == 501314].sort_values("first_seen")
    assert list(prismx["group"]) == ["T", "B"]
    assert list(prismx["last_seen"]) == [pd.Timestamp("2026-10-23"), pd.Timestamp("2026-10-26")]
    assert list(prismx["valid_to"]) == list(prismx["last_seen"])

    sula = out[out["scrip_code"] == 543711].sort_values("first_seen")
    assert list(sula["isin"]) == ["INE142Q01026", "INE142Q01034"]


def _drop(symbol: str) -> Callable[[str], str]:
    return lambda t: "\n".join(line for line in t.splitlines() if f",{symbol}," not in line) + "\n"


def test_a_listing_that_stops_trading_is_kept_active_for_ten_sessions(spec):
    fake = FakeBse()
    _build(spec, date(2026, 10, 21), fake, seed_days=1)
    fake.edits[date(2026, 10, 22)] = _drop("EXCELSOFT")

    # 10 sessions ending 2026-10-30 reach back to 2026-10-19: still active.
    _build(spec, date(2026, 10, 30), fake)
    excelsoft = _row(_out(spec), "EXCELSOFT")
    assert excelsoft["last_seen"] == pd.Timestamp("2026-10-21")
    assert excelsoft["status"] == "active"

    # Ending 2026-11-04 they start 2026-10-22: kept, and inactive.
    _build(spec, date(2026, 11, 4), fake)
    out = _out(spec)
    assert _row(out, "EXCELSOFT")["status"] == "inactive"
    assert _row(out, "EXCELSOFT")["last_seen"] == pd.Timestamp("2026-10-21")
    assert _row(out, "SULA")["status"] == "active"


def test_an_unusable_published_list_fails_and_is_left_alone(spec):
    spec.base_dir.mkdir(parents=True)
    path = spec.base_dir / "bse_instruments_all.parquet"
    pd.DataFrame({"scrip_code": [1], "symbol": ["X"]}).to_parquet(path)
    before = path.read_bytes()
    fake = FakeBse()

    st = _build(spec, date(2026, 10, 26), fake)
    assert st.status == "failed"
    assert "lacks columns" in st.message
    assert fake.asked == []
    assert path.read_bytes() == before


def test_a_truncated_published_list_is_refused(spec):
    fake = FakeBse()
    _build(spec, date(2026, 10, 23), fake)
    path = spec.base_dir / "bse_instruments_all.parquet"
    _out(spec).head(2).to_parquet(path)
    before = path.read_bytes()

    st = _build(spec, date(2026, 10, 26), fake)
    assert st.status == "failed"
    assert "truncated or broken" in st.message
    assert path.read_bytes() == before


def test_a_bse_holiday_reads_nothing(spec):
    fake = FakeBse()
    _build(spec, date(2026, 10, 23), fake)
    fake.asked.clear()

    st = _build(spec, date(2026, 10, 25), fake)  # a Sunday
    assert st.status == "skipped_holiday"
    assert fake.asked == []


def test_backfill_merges_older_days_and_skips_days_bse_did_not_post(spec):
    fake = FakeBse()
    _build(spec, date(2026, 10, 26), fake, seed_days=1)
    fake.not_posted = {date(2026, 10, 21)}

    st = _build(spec, date(2026, 10, 26), fake, days=SESSIONS[:6])
    assert st.status == "success", st.message
    assert "2026-10-21" in st.message
    sula = _row(_out(spec), "SULA")
    assert sula["first_seen"] == pd.Timestamp("2026-10-19")
    assert sula["last_seen"] == pd.Timestamp("2026-10-26")


def test_registered_as_a_daily_builder_and_published():
    assert builders.BUILDERS["bse_reference"] is builders.build_bse_reference
    assert datasets.BSE_REFERENCE in datasets.published_specs()
    assert datasets.BSE_REFERENCE.file_prefix == "bse_instruments"
    # instruments_all's glob ("instruments_*") never picks up the BSE file.
    assert not "bse_instruments_all.parquet".startswith(f"{datasets.REFERENCE.file_prefix}_")


def test_backfill_command_reads_the_last_n_bse_sessions(monkeypatch):
    seen: dict[str, object] = {}

    def fake_build(spec, target, *, days):
        seen.update(spec=spec, target=target, days=days)
        return builders.RunStatus("success", target)

    monkeypatch.setattr(cli.builders, "build_bse_reference", fake_build)
    monkeypatch.setattr(cli.builders, "bse_calendar", _no_closures)
    rc = cli.main(["backfill-bse-instruments", "--days", "3", "--date", "2026-10-26"])
    assert rc == 0
    assert seen["spec"] is datasets.BSE_REFERENCE
    assert seen["days"] == [date(2026, 10, 22), date(2026, 10, 23), date(2026, 10, 26)]
