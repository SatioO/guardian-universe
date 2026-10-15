"""special_pre_open: the security master's parse and base-price band, and the
builder's session keying, accumulation and fail-closed policy.

The failure that matters is a wrong list: an instrument the app holds back to
10:00 that actually opened at 09:15, or a listing measured against a price
that is not its base. So the parse refuses anything it does not recognise,
the base is published only when both bounds of the band prove it, and a bad
day keeps the prior file.
"""

from __future__ import annotations

import dataclasses
import gzip
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline import datasets, special_pre_open
from pipeline.errors import NotYetPublished
from pipeline.sources import nse_cm_security as sec

# Cut from the real NSE_CM_security_29092026.csv.gz: its seven special
# pre-open rows, three regular shares, two illiquid and two SME periodic
# call-auction rows (one of them deleted), and one deleted debt row.
FIXTURE = Path(__file__).parent / "fixtures" / "nse_cm_security_29092026.csv"
FILE_DAY = date(2026, 9, 29)  # a Tuesday
SESSION = date(2026, 9, 30)


def _payload() -> bytes:
    return FIXTURE.read_bytes()


def _master() -> sec.SecurityMaster:
    return sec.parse_security_master(_payload())


def _spec(tmp_path: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.SPECIAL_PRE_OPEN, base_dir=tmp_path / "special_pre_open")


def _weekday(day: date) -> bool:
    return day.weekday() < 5


def _table(spec: datasets.DatasetSpec) -> pd.DataFrame:
    return pd.read_parquet(spec.base_dir / "special_pre_open_all.parquet")


def _build(spec, target, fetch, *, trading_day=_weekday, seed_days=1):
    return special_pre_open.build_special_pre_open(
        spec,
        target,
        fetch_master=fetch,
        trading_day=trading_day,
        seed_days=seed_days,
        min_securities=1,
    )


def _empty_master(securities: int = 14) -> sec.SecurityMaster:
    return sec.SecurityMaster(
        sessions=pd.DataFrame(columns=sec.SESSION_COLUMNS), securities=securities
    )


# ── the parse ───────────────────────────────────────────────────────────────


def test_the_flagged_rows_are_the_special_pre_open_securities():
    master = _master()
    rows = {
        (r.symbol, r.series): (r.kind, None if pd.isna(r.base_price) else r.base_price)
        for r in master.sessions.itertuples()
    }
    # The listing-day UDiFF bhavcopy (30-09-2026) carries each listing's base
    # as PrvsClsgPric: ADROITIND 134, ARMEE 375, COREIN 78, ELEVATE 362,
    # POOJALOGIS 115, SWASTIKAIN 185.
    assert rows == {
        ("ADROITIND", "BE"): ("listing", 134.0),
        ("ARMEE", "EQ"): ("listing", 375.0),
        ("COREIN", "ST"): ("listing", 78.0),
        ("ELEVATE", "EQ"): ("listing", 362.0),
        ("POOJALOGIS", "ST"): ("listing", 115.0),
        ("SWASTIKAIN", "BE"): ("listing", 185.0),
        ("GSPLTRANS", "BE"): ("restructuring", None),
    }
    assert (
        master.sessions.loc[master.sessions["symbol"] == "ARMEE", "isin"].item() == "INE0TWE01016"
    )


def test_regular_periodic_auction_and_deleted_rows_are_not_special():
    master = _master()
    symbols = set(master.sessions["symbol"])
    assert not symbols & {"RELIANCE", "INFY", "TCS", "ABMINTLLTD", "ACSTECH", "AAATECH", "AAKAAR"}
    # The two deleted rows (AAATECH SM, 0MOFSL26 N1) are not listed securities.
    assert master.securities == 13


def test_a_gzipped_file_parses_the_same():
    assert sec.parse_security_master(gzip.compress(_payload())).sessions.equals(_master().sessions)


@pytest.mark.parametrize(
    "payload",
    [
        b"<html><body>Access denied</body></html>",
        b"TckrSymb,SctySrs,ISIN\nABC,EQ,INE000000000\n",
    ],
)
def test_anything_but_the_security_master_is_refused(payload):
    with pytest.raises(sec.MalformedSecurityFile):
        sec.parse_security_master(payload)


def _with_indicator(value: str, deleted: str = "N") -> bytes:
    frame = pd.read_csv(FIXTURE, dtype=str, keep_default_na=False)
    frame.loc[frame["TckrSymb"] == "TCS", ["CallAuctnInd", "DelFlg"]] = [value, deleted]
    return frame.to_csv(index=False).encode()


def test_a_call_auction_indicator_never_seen_before_is_refused():
    with pytest.raises(sec.UnknownCallAuction, match="6"):
        sec.parse_security_master(_with_indicator("6"))


def test_a_deleted_row_is_not_read():
    assert "TCS" not in set(
        sec.parse_security_master(_with_indicator("6", deleted="Y")).sessions["symbol"]
    )


# ── the base price ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("series", "price_range", "base"),
    [
        # Main board: -50% / +100%. LGEINDIA listed on 14-10-2025 at its
        # issue price of 1140.
        ("EQ", "570.00-2280.00", 1140.0),
        ("BE", "83.50-334.00", 167.0),
        # SME: -20% / +90%. HIMALAYAN listed on 05-10-2026 at 103.
        ("ST", "82.40-195.70", 103.0),
        ("SM", "43.20-102.60", 54.0),
    ],
)
def test_the_base_is_the_band_both_bounds_agree_on(series, price_range, base):
    assert sec.base_price(series, price_range) == base


@pytest.mark.parametrize(
    ("series", "price_range"),
    [
        ("EQ", "570.00-2281.00"),  # the bounds name two bases
        ("ST", "82.40-196.00"),
        ("ST", "570.00-2280.00"),  # a main-board band read on the SME series
        ("RR", "50.00-200.00"),  # no documented band for the series
        ("EQ", "-"),
        ("EQ", "abc-def"),
        ("EQ", "200.00-100.00"),
        ("EQ", "0.00-0.00"),
    ],
)
def test_no_base_unless_both_bounds_prove_it(series, price_range):
    assert sec.base_price(series, price_range) is None


# ── the builder ─────────────────────────────────────────────────────────────


def test_a_file_names_the_next_trading_session(tmp_path):
    spec = _spec(tmp_path)
    status = _build(spec, FILE_DAY, lambda _day: _master())
    assert status.status == "success", status.message
    table = _table(spec)
    assert set(table["session_date"]) == {SESSION}
    assert set(table["file_date"]) == {FILE_DAY}
    assert set(table["source"]) == {"nse-cm-security"}
    assert set(table["date"]) == {pd.Timestamp(SESSION)}
    assert "ADROITIND (listing)" in status.message


def test_a_friday_file_names_monday_and_skips_a_holiday(tmp_path):
    spec = _spec(tmp_path)
    holiday = date(2026, 10, 2)  # a Friday holiday: Thursday's file names Monday
    _build(
        spec,
        date(2026, 10, 1),
        lambda _day: _master(),
        trading_day=lambda d: _weekday(d) and d != holiday,
    )
    assert set(_table(spec)["session_date"]) == {date(2026, 10, 5)}


def test_the_newest_file_replaces_its_session_and_keeps_the_others(tmp_path):
    spec = _spec(tmp_path)
    _build(spec, FILE_DAY, lambda _day: _master())
    one = _master().sessions.iloc[:1]
    _build(spec, date(2026, 9, 30), lambda _day: sec.SecurityMaster(sessions=one, securities=14))
    # An evening correction of the same file: only ARMEE left for 01-10.
    corrected = _master().sessions[_master().sessions["symbol"] == "ARMEE"]
    status = _build(
        spec, date(2026, 9, 30), lambda _day: sec.SecurityMaster(sessions=corrected, securities=14)
    )
    assert status.status == "success"
    table = _table(spec)
    assert len(table[table["session_date"] == SESSION]) == 7
    assert list(table.loc[table["session_date"] == date(2026, 10, 1), "symbol"]) == ["ARMEE"]
    assert set(table["date"]) == {pd.Timestamp(date(2026, 10, 1))}


def test_a_session_with_no_special_pre_open_still_moves_the_coverage(tmp_path):
    spec = _spec(tmp_path)
    _build(spec, FILE_DAY, lambda _day: _master())
    status = _build(spec, date(2026, 9, 30), lambda _day: _empty_master())
    assert status.status == "success"
    assert "0 in special pre-open" in status.message
    table = _table(spec)
    assert len(table) == 7
    # "None on 01-10" is now distinguishable from "01-10 not built yet".
    assert set(table["date"]) == {pd.Timestamp(date(2026, 10, 1))}


def test_a_file_not_posted_yet_keeps_the_prior_list(tmp_path):
    spec = _spec(tmp_path)
    _build(spec, FILE_DAY, lambda _day: _master())
    before = (spec.base_dir / "special_pre_open_all.parquet").read_bytes()

    def not_yet(_day: date) -> sec.SecurityMaster:
        raise NotYetPublished("404 for 2026-09-30")

    status = _build(spec, date(2026, 9, 30), not_yet)
    assert status.status == "not_yet"
    assert (spec.base_dir / "special_pre_open_all.parquet").read_bytes() == before


@pytest.mark.parametrize(
    "fetch",
    [
        lambda _day: (_ for _ in ()).throw(sec.UnknownCallAuction("CallAuctnInd 6")),
        lambda _day: (_ for _ in ()).throw(sec.MalformedSecurityFile("missing columns")),
        lambda _day: _empty_master(securities=0),
    ],
    ids=["unknown-indicator", "malformed", "truncated"],
)
def test_a_bad_file_fails_loudly_and_keeps_the_prior_list(tmp_path, fetch):
    spec = _spec(tmp_path)
    _build(spec, FILE_DAY, lambda _day: _master())
    before = (spec.base_dir / "special_pre_open_all.parquet").read_bytes()
    status = _build(spec, date(2026, 9, 30), fetch)
    assert status.status == "failed"
    assert (spec.base_dir / "special_pre_open_all.parquet").read_bytes() == before


def test_the_real_floor_refuses_a_truncated_file(tmp_path):
    status = special_pre_open.build_special_pre_open(
        _spec(tmp_path),
        FILE_DAY,
        fetch_master=lambda _day: _master(),
        trading_day=_weekday,
        seed_days=1,
    )
    assert status.status == "failed"
    assert "truncated" in status.message


def test_a_first_build_seeds_the_days_before(tmp_path):
    spec = _spec(tmp_path)
    asked: list[date] = []

    def fetch(day: date) -> sec.SecurityMaster:
        asked.append(day)
        if day == date(2026, 9, 25):
            raise NotYetPublished("404")
        return _master() if day == FILE_DAY else _empty_master()

    status = _build(spec, FILE_DAY, fetch, seed_days=3)
    assert status.status == "success"
    assert asked == [date(2026, 9, 25), date(2026, 9, 28), FILE_DAY]
    assert "seed days without a file: 2026-09-25" in status.message
    assert set(_table(spec)["date"]) == {pd.Timestamp(SESSION)}


def test_a_first_build_still_publishes_when_today_is_not_posted(tmp_path):
    spec = _spec(tmp_path)

    def fetch(day: date) -> sec.SecurityMaster:
        if day == FILE_DAY:
            raise NotYetPublished("404")
        return _master()

    status = _build(spec, FILE_DAY, fetch, seed_days=2)
    assert status.status == "success"
    # Monday's file covers Tuesday, the run's own day, and no further.
    assert set(_table(spec)["date"]) == {pd.Timestamp(FILE_DAY)}


def test_a_holiday_run_reads_nothing(tmp_path):
    status = _build(_spec(tmp_path), FILE_DAY, lambda _day: _master(), trading_day=lambda _d: False)
    assert status.status == "skipped_holiday"


def test_the_published_types_are_pinned(tmp_path):
    spec = _spec(tmp_path)
    _build(spec, FILE_DAY, lambda _day: _master())
    schema = pq.read_schema(spec.base_dir / "special_pre_open_all.parquet")
    assert schema.field("session_date").type == pa.date32()
    assert schema.field("base_price").type == pa.float64()
    assert schema.field("symbol").type == pa.string()
    assert schema.field("date").type == pa.timestamp("ms")
    table = _table(spec)
    assert table.loc[table["symbol"] == "GSPLTRANS", "base_price"].isna().all()
