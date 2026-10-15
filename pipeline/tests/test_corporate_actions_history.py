"""corporate_actions_history: the backfill and top-up from NSE's API, the
daily book-closure refresh, the ISIN each row gets, and the fail-closed policy.

What this dataset must never do is tie an action to the wrong share: the app
joins a stock's history across a break only when an action for that ISIN
falls on the break's day. So `isin` comes from the pipeline's own instrument
list, never from NSE's printed field taken on trust, and a run that cannot
read the list, the table or a source writes nothing.

All inputs are real NSE files: API windows (2021-10, 2025-05, 2026-10 main
board, 2025-09 SME), the book-closure files of 2021-10-01 and 2026-10-14, and
the published instrument list's rows for the symbols in them.
"""

from __future__ import annotations

import dataclasses
import hashlib
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from pipeline import cli, datasets
from pipeline import corporate_actions_history as cah
from pipeline.sources import nse_ca_history as ca

FIXTURES = Path(__file__).parent / "fixtures"
TODAY = date(2026, 10, 15)
API_FILES = {
    (ca.INDEX_EQUITIES, "2021-10"): "nse_ca_api_equities_202110.json",
    (ca.INDEX_EQUITIES, "2025-05"): "nse_ca_api_equities_202505.json",
    (ca.INDEX_EQUITIES, "2026-10"): "nse_ca_api_equities_202610.json",
    (ca.INDEX_SME, "2025-09"): "nse_ca_api_sme_202509.json",
}


@pytest.fixture
def specs(tmp_path: Path) -> tuple[datasets.DatasetSpec, datasets.DatasetSpec]:
    reference = dataclasses.replace(datasets.REFERENCE, base_dir=tmp_path / "reference")
    reference.base_dir.mkdir()
    sample = pd.read_csv(
        FIXTURES / "instruments_ca_sample.csv", parse_dates=["first_seen", "last_seen"]
    )
    sample.to_parquet(reference.base_dir / "instruments_all.parquet", index=False)
    spec = dataclasses.replace(datasets.CORPORATE_ACTIONS_HISTORY, base_dir=tmp_path / "ca_history")
    return spec, reference


def _get(index: str, start: date, end: date) -> bytes:
    name = API_FILES.get((index, f"{start:%Y-%m}"))
    return (FIXTURES / name).read_bytes() if name else b"[]"


def _backfill(
    spec: datasets.DatasetSpec,
    reference: datasets.DatasetSpec,
    start: date,
    end: date,
    get: Callable[[str, date, date], bytes] = _get,
    today: date = TODAY,
    indexes: tuple[str, ...] = (ca.INDEX_EQUITIES, ca.INDEX_SME),
):
    return cah.backfill_corporate_actions_history(
        spec, universe_spec=reference, start=start, end=end, today=today, get=get,
        indexes=indexes,
    )


def _bc(name: str = "nse_bc14102026.csv", on: date = TODAY) -> Callable[[date], pd.DataFrame]:
    """The real file as `on`'s bundle; the catch-up days before it list nothing."""
    frame = ca.parse_book_closure_csv((FIXTURES / name).read_bytes())
    return lambda day: frame if day == on else frame.iloc[0:0]


def _daily(spec, reference, target: date = TODAY, **kwargs: object):
    kwargs.setdefault("fetch_bundle", _bc())
    kwargs.setdefault("trading_day", lambda day: day.weekday() < 5)
    return cah.build_corporate_actions_history(spec, target, universe_spec=reference, **kwargs)


def _digests(spec: datasets.DatasetSpec) -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(spec.base_dir.glob("*.parquet"))
    }


def _one(table: pd.DataFrame, symbol: str, ex_date: date, kind: str | None = None) -> pd.Series:
    rows = table[(table["symbol"] == symbol) & (table["ex_date"] == ex_date)]
    if kind is not None:
        rows = rows[rows["kind"] == kind]
    assert len(rows) == 1, rows
    return rows.iloc[0]


def _table(spec: datasets.DatasetSpec) -> pd.DataFrame:
    table = cah.read_table(spec)
    assert table is not None
    return table


# ── Backfill from NSE's API ─────────────────────────────────────────────────


def test_backfill_writes_one_file_per_ex_date_year_with_the_published_types(specs) -> None:
    spec, reference = specs
    status = _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    assert status.status == "success", status.message
    assert sorted(p.name for p in spec.base_dir.glob("*.parquet")) == ["ca_history_2021.parquet"]
    written = pq.read_table(spec.base_dir / "ca_history_2021.parquet")
    assert written.schema.remove_metadata() == cah.SCHEMA
    table = _table(spec)
    # 57 equity rows; ACCELYA's "Annual General Meeting/Dividend - Rs 17 Per
    # Share" is two actions.
    assert len(table) == 57 + 1
    affle = table[table["symbol"] == "AFFLE"].iloc[0]
    assert (affle["kind"], affle["fv_from"], affle["fv_to"], affle["isin"]) == (
        "split", 10.0, 2.0, "INE00WC01019",
    )
    assert set(written.column("date").to_pylist()) == {pd.Timestamp(TODAY).to_pydatetime()}
    assert set(table["first_seen"]) == {TODAY}


def test_backfill_ties_each_action_to_the_isin_that_traded_on_its_ex_date(specs) -> None:
    spec, reference = specs
    assert _backfill(spec, reference, date(2025, 5, 1), date(2025, 5, 31)).status == "success"
    table = _table(spec)
    # The demergers the app sees as breaks.
    assert _one(table, "RAYMOND", date(2025, 5, 14), "demerger")["isin"] == "INE301A01014"
    assert _one(table, "ABFRL", date(2025, 5, 22), "demerger")["isin"] == "INE647O01011"
    # A split re-issues the ISIN on its ex-date; NSE's API prints the old one.
    naukri = _one(table, "NAUKRI", date(2025, 5, 7), "split")
    assert (naukri["isin"], naukri["isin_nse"]) == ("INE663F01032", "INE663F01024")
    # NSE prints KSB's ISIN from before its 2024-07-25 split; INE999A01023 trades.
    ksb = _one(table, "KSB", date(2025, 5, 2), "dividend")
    assert (ksb["isin"], ksb["isin_nse"], ksb["dividend_ps"]) == (
        "INE999A01023", "INE999A01015", 4.0,
    )


def test_sme_rows_take_their_isin_from_the_instrument_list_only(specs) -> None:
    spec, reference = specs
    status = _backfill(
        spec, reference, date(2025, 9, 1), date(2025, 9, 30), indexes=(ca.INDEX_SME,)
    )
    assert status.status == "success", status.message
    sme = _table(spec)
    sme = sme[sme["series"] == "SM"]
    assert len(sme) == 33
    assert sme["isin_nse"].isna().all()
    assert sme["isin"].notna().all()
    assert sme.set_index("symbol").loc["ACCENTMIC", "isin"] == "INE0Q5D01013"


def test_a_backfill_that_meets_one_bad_window_writes_nothing(specs) -> None:
    spec, reference = specs

    def broken(index: str, start: date, end: date) -> bytes:
        if index == ca.INDEX_SME:
            return b"<html>Access Denied</html>"
        return _get(index, start, end)

    status = _backfill(spec, reference, date(2025, 5, 1), date(2025, 5, 31), get=broken)
    assert status.status == "failed"
    assert "not JSON" in status.message
    assert not spec.base_dir.exists() or not list(spec.base_dir.glob("*.parquet"))


def test_a_past_month_of_the_main_board_with_no_action_is_a_broken_answer(specs) -> None:
    spec, reference = specs
    status = _backfill(spec, reference, date(2022, 1, 1), date(2022, 1, 31))
    assert status.status == "failed"
    assert "no equities action in 2022-01-01..2022-01-31" in status.message
    # The current month may well be empty so far.
    ok = _backfill(spec, reference, date(2026, 11, 1), date(2026, 11, 3), today=date(2026, 11, 3))
    assert ok.status == "success", ok.message


def test_backfill_without_the_instrument_list_writes_nothing(specs) -> None:
    spec, reference = specs
    (reference.base_dir / "instruments_all.parquet").unlink()
    status = _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    assert status.status == "failed"
    assert "sync" in status.message
    assert not list(spec.base_dir.glob("*.parquet")) if spec.base_dir.exists() else True


def test_backfill_run_again_changes_no_file(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    before = _digests(spec)
    again = _backfill(
        spec, reference, date(2021, 10, 1), date(2021, 10, 31), today=date(2026, 10, 20)
    )
    assert again.status == "success"
    assert "years written: none" in again.message
    assert _digests(spec) == before


# ── Daily: the book-closure file ────────────────────────────────────────────


def test_daily_without_a_history_skips_and_names_the_backfill(specs) -> None:
    spec, reference = specs
    status = _daily(spec, reference)
    assert status.status == "skipped_idempotent"
    assert "backfill-corporate-actions" in status.message
    assert not spec.base_dir.exists() or not list(spec.base_dir.glob("*.parquet"))


def test_daily_adds_the_book_closure_rows_the_api_has_not_listed(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    status = _daily(spec, reference)
    assert status.status == "success", status.message
    assert "10 new from the book-closure file" in status.message
    new = _table(spec)
    new = new[new["source"] == ca.SOURCE_BOOK_CLOSURE].set_index("symbol")
    assert len(new) == 10
    assert set(new["first_seen"]) == {TODAY}
    # Ex-dates up to the instrument list's newest day (2026-10-14) resolve on
    # that day; later ones on its newest day too.
    assert new.loc["TCS", "isin"] == "INE467B01029"
    assert new.loc["BANSALWIRE", ["kind", "fv_from", "fv_to"]].tolist() == ["split", 5.0, 1.0]
    assert new.loc["OLIL", ["series", "kind", "ratio_new", "ratio_held"]].tolist() == [
        "SM", "bonus", 1, 2,
    ]
    assert new.loc["OLIL", "isin"] is not None


def test_daily_run_again_rewrites_nothing(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    _daily(spec, reference)
    before = _digests(spec)
    again = _daily(spec, reference, target=date(2026, 10, 16))
    assert again.status == "success"
    assert "0 new" in again.message and "years written: none" in again.message
    assert _digests(spec) == before


def test_a_top_up_replaces_book_closure_rows_with_the_apis_and_never_shrinks(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    _daily(spec, reference)
    held = len(_table(spec))
    status = _backfill(spec, reference, date(2026, 10, 1), date(2026, 10, 31))
    assert status.status == "success", status.message
    table = _table(spec)
    book = table[table["source"] == ca.SOURCE_BOOK_CLOSURE]
    # The API lists 9 of the 10 (OLIL is SME; that window has no SME rows).
    assert book["symbol"].tolist() == ["OLIL"]
    tcs = table[(table["symbol"] == "TCS") & (table["ex_date"] == date(2026, 10, 14))]
    assert tcs[["source", "purpose", "dividend_ps"]].values.tolist() == [
        [ca.SOURCE_API, "Interim Dividend - Rs 12 Per Share", 12.0]
    ]
    # 58 rows of 2021 + 10 book-closure rows held; the 9 the API lists give
    # way to its 26 rows of 2026-10, OLIL stays.
    assert (held, len(table)) == (58 + 10, 58 + 26 + 1)


def _edited(name: str, edit: Callable[[list[dict[str, object]]], list[dict[str, object]]]) -> bytes:
    import json

    return json.dumps(edit(json.loads((FIXTURES / name).read_bytes()))).encode()


def test_a_top_up_makes_the_apis_current_rows_the_windows_rows(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    before = _table(spec)

    def corrected(rows: list[dict[str, object]]) -> list[dict[str, object]]:
        # NSE corrects TIRUPATIFL's bonus and withdraws SRF's.
        out = [row for row in rows if row["symbol"] != "SRF"]
        for row in out:
            if row["symbol"] == "TIRUPATIFL" and str(row["subject"]).strip() == "Bonus 3:4":
                row["subject"] = "Bonus 4:3"
        return out

    def get(index: str, start: date, end: date) -> bytes:
        if index == ca.INDEX_EQUITIES:
            return _edited(API_FILES[(index, "2021-10")], corrected)
        return b"[]"

    status = _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31), get=get,
                       today=date(2026, 10, 20))
    assert status.status == "success", status.message
    assert "2 rows replaced or withdrawn" in status.message
    table = _table(spec)
    bonuses = table[(table["symbol"] == "TIRUPATIFL") & (table["kind"] == "bonus")]
    assert bonuses[["purpose", "ratio_new", "ratio_held"]].values.tolist() == [["Bonus 4:3", 4, 3]]
    assert "SRF" not in set(table["symbol"])
    assert len(table) == len(before) - 1
    # A row NSE did not change keeps the day it was first recorded.
    assert _one(table, "AFFLE", date(2021, 10, 7))["first_seen"] == TODAY
    assert bonuses["first_seen"].tolist() == [date(2026, 10, 20)]


def test_a_top_up_that_would_drop_a_twentieth_of_a_window_writes_nothing(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    before = _digests(spec)

    def get(index: str, start: date, end: date) -> bytes:
        if index == ca.INDEX_EQUITIES:
            return _edited(API_FILES[(index, "2021-10")], lambda rows: rows[:40])
        return b"[]"

    status = _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31), get=get)
    assert status.status == "failed"
    assert "2021-10-01..2021-10-31" in status.message and "refusing" in status.message
    assert _digests(spec) == before


def test_a_top_up_drops_past_book_closure_rows_the_api_never_listed(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    _daily(spec, reference)
    # Five days on, the API (main board and SME) has had its say on
    # 2026-10-15: OLIL's bonus, which only the book-closure file listed, goes.
    status = _backfill(spec, reference, date(2026, 10, 1), date(2026, 10, 31),
                       today=date(2026, 10, 20))
    assert status.status == "success", status.message
    table = _table(spec)
    assert ca.SOURCE_BOOK_CLOSURE not in set(table["source"])
    assert "OLIL" not in set(table["symbol"])


def test_a_main_board_top_up_leaves_sme_book_closure_rows(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    _daily(spec, reference)
    status = _backfill(spec, reference, date(2026, 10, 1), date(2026, 10, 31),
                       today=date(2026, 10, 20), indexes=(ca.INDEX_EQUITIES,))
    assert status.status == "success", status.message
    book = _table(spec)
    assert book[book["source"] == ca.SOURCE_BOOK_CLOSURE]["symbol"].tolist() == ["OLIL"]


def test_a_rerun_that_finds_no_isin_keeps_the_one_already_known(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    known = _table(spec)["isin"].tolist()
    before = _digests(spec)
    # An instrument list whose history starts later (a rebuilt one) knows no
    # 2021 version: the ISINs resolved then stay.
    path = reference.base_dir / "instruments_all.parquet"
    listed = pd.read_parquet(path)
    listed = listed[listed["last_seen"] >= pd.Timestamp("2026-01-01")]
    listed = listed.assign(first_seen=listed["first_seen"].clip(lower=pd.Timestamp("2026-01-01")))
    listed.to_parquet(path, index=False)
    status = _daily(spec, reference)
    assert status.status == "success", status.message
    table = _table(spec)
    assert table[table["source"] == ca.SOURCE_API]["isin"].tolist() == known
    assert _digests(spec)["ca_history_2021.parquet"] == before["ca_history_2021.parquet"]


def test_daily_catches_up_on_the_bundles_of_the_days_before(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    older = ca.parse_book_closure_csv((FIXTURES / "nse_bc14102026.csv").read_bytes())
    older = older[older["symbol"] == "TCS"].assign(ex_date=date(2026, 10, 19))
    bundles = {date(2026, 10, 13): older}

    def fetch(day: date) -> pd.DataFrame:
        if day == TODAY:
            return older.iloc[0:0]
        if day in bundles:
            return bundles[day]
        raise ca.MalformedCorporateActions("404")

    status = _daily(spec, reference, fetch_bundle=fetch)
    assert status.status == "success", status.message
    assert "1 new" in status.message
    assert "bundles not read: 2026-10-09, 2026-10-12, 2026-10-14" in status.message
    tcs = _one(_table(spec), "TCS", date(2026, 10, 19))
    assert (tcs["source"], tcs["first_seen"]) == (ca.SOURCE_BOOK_CLOSURE, date(2026, 10, 13))


def test_a_book_closure_rewording_of_a_held_action_is_not_added(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    _daily(spec, reference)
    held = len(_table(spec))
    raw = _bc()(TODAY)
    raw = raw[raw["symbol"] == "TCS"].assign(purpose="INT DIV - RS 12 PER SHARE")
    status = _daily(spec, reference, target=date(2026, 10, 16), fetch_bundle=lambda day: raw)
    assert "0 new" in status.message, status.message
    assert len(_table(spec)) == held


def test_daily_does_not_add_an_action_the_api_already_listed(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2026, 10, 1), date(2026, 10, 31))
    status = _daily(spec, reference)
    assert "1 new from the book-closure file" in status.message  # OLIL only


def test_two_actions_in_one_book_closure_purpose_are_two_rows(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    raw = ca.parse_book_closure_csv((FIXTURES / "nse_bc14102026.csv").read_bytes())
    raw = raw[raw["symbol"] == "TCS"].assign(purpose="DIV-RS30/SPLDIV-RS60")
    _daily(spec, reference, fetch_bundle=lambda day: raw)
    tcs = _table(spec)
    tcs = tcs[(tcs["symbol"] == "TCS") & (tcs["ex_date"] == date(2026, 10, 14))]
    assert tcs[["part", "kind", "dividend_ps"]].values.tolist() == [
        [0, "dividend", 30.0], [1, "dividend", 60.0],
    ]


@pytest.mark.parametrize(
    ("setup", "status_name", "fragment"),
    [
        ("bundle", "skipped_idempotent", "bundle: not a zip"),
        ("reference", "skipped_idempotent", "instrument list unusable"),
        ("holiday", "skipped_holiday", "non-trading day"),
        ("table", "failed", "table unreadable"),
    ],
)
def test_daily_fails_closed_keeping_every_file(specs, setup, status_name, fragment) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    kwargs: dict[str, object] = {}
    if setup == "bundle":
        kwargs["fetch_bundle"] = lambda day: ca.parse_pr_bundle(b"<html>")
    if setup == "reference":
        (reference.base_dir / "instruments_all.parquet").unlink()
    if setup == "holiday":
        kwargs["trading_day"] = lambda day: False
    if setup == "table":
        frame = pd.read_parquet(spec.base_dir / "ca_history_2021.parquet").drop(columns=["isin"])
        frame.to_parquet(spec.base_dir / "ca_history_2021.parquet", index=False)
    before = _digests(spec)
    status = _daily(spec, reference, **kwargs)
    assert (status.status, fragment in status.message) == (status_name, True), status.message
    assert _digests(spec) == before


def test_an_unrecognised_purpose_is_kept_as_other_and_named(specs) -> None:
    spec, reference = specs
    _backfill(spec, reference, date(2021, 10, 1), date(2021, 10, 31))
    raw = _bc()(TODAY)
    raw = raw[raw["symbol"] == "TCS"].assign(purpose="SPIN OFF OF UNIT")
    status = _daily(spec, reference, fetch_bundle=lambda day: raw)
    assert "purposes not recognised" in status.message and "SPIN OFF OF UNIT" in status.message
    row = _one(_table(spec), "TCS", date(2026, 10, 14))
    assert (row["kind"], row["purpose"]) == ("other", "SPIN OFF OF UNIT")


# ── ISIN resolution ─────────────────────────────────────────────────────────


@pytest.fixture
def listed() -> cah.InstrumentList:
    return cah.InstrumentList(
        pd.read_csv(FIXTURES / "instruments_ca_sample.csv", parse_dates=["first_seen", "last_seen"])
    )


@pytest.mark.parametrize(
    ("symbol", "ex_date", "isin_nse", "expected"),
    [
        # Split on 2026-09-04: the new ISIN lists that day.
        ("TCC", date(2026, 9, 4), "INE887D01016", "INE887D01024"),
        ("TCC", date(2026, 9, 3), None, "INE887D01016"),
        # NSE prints ONGC's ISIN from before 2011.
        ("ONGC", date(2026, 9, 4), "INE213A01011", "INE213A01029"),
        # Britannia's bonds trade under its symbol with their own ISINs.
        ("BRITANNIA", date(2022, 6, 20), None, "INE216A01030"),
        # No trade on the ex-date; the new ISIN listed 5 and 16 days later.
        ("DRCSYSTEMS", date(2022, 3, 16), None, "INE03RS01027"),
        ("VERTOZ", date(2025, 6, 25), None, "INE188Y01031"),
        # A share listed nowhere near its ex-date.
        ("TIPSMUSIC", date(2022, 5, 19), "INE716B01029", None),
        # A symbol the list does not know, NSE's ISIN trading that day.
        ("RENAMED", date(2026, 9, 4), "INE213A01029", "INE213A01029"),
        ("RENAMED", date(2026, 9, 4), "INE213A01011", None),
        # An ex-date after the list's newest day resolves on that day.
        ("TCS", date(2026, 10, 30), None, "INE467B01029"),
    ],
)
def test_isin_is_the_equity_that_traded_under_the_symbol_on_the_ex_date(
    listed, symbol, ex_date, isin_nse, expected
) -> None:
    assert listed.isin(symbol, ex_date, isin_nse) == expected


def test_a_tie_between_two_isins_is_settled_only_by_nse_naming_one() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["DUP", "DUP"],
            "series": ["EQ", "SM"],
            "isin": ["INE00WC01019", "INE301A01014"],
            "first_seen": pd.to_datetime(["2025-01-01", "2025-01-01"]),
            "last_seen": pd.to_datetime(["2025-12-31", "2025-12-31"]),
        }
    )
    tied = cah.InstrumentList(frame)
    assert tied.isin("DUP", date(2025, 6, 2), None) is None
    assert tied.isin("DUP", date(2025, 6, 2), "INE301A01014") == "INE301A01014"


# ── Registry and CLI ────────────────────────────────────────────────────────


def test_the_dataset_is_published_under_its_own_name_and_files() -> None:
    spec = datasets.DATASETS["corporate_actions_history"]
    assert spec in datasets.published_specs()
    assert (spec.manifest_name, spec.file_prefix, spec.schema_version) == (
        "corporate_actions_history", "ca_history", 1,
    )
    # Not under corporate_actions' directory, whose glob would pick it up.
    assert spec.base_dir != datasets.CORPORATE_ACTIONS.base_dir
    assert cli.builders.BUILDERS["corporate_actions_history"].keywords == {
        "universe_spec": datasets.REFERENCE
    }


def test_month_windows_cut_at_month_ends() -> None:
    assert cah.month_windows(date(2021, 10, 15), date(2021, 12, 3)) == [
        (date(2021, 10, 15), date(2021, 10, 31)),
        (date(2021, 11, 1), date(2021, 11, 30)),
        (date(2021, 12, 1), date(2021, 12, 3)),
    ]


def test_backfill_command_passes_its_window(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake(spec, *, universe_spec, start, end, today, get):
        seen.update(spec=spec, universe_spec=universe_spec, start=start, end=end)
        return cli.RunStatus("success", today, message="ok")

    monkeypatch.setattr(cli, "backfill_corporate_actions_history", fake)
    code = cli.main(["backfill-corporate-actions", "--from", "2025-05-01", "--to", "2025-05-31"])
    assert code == 0
    assert seen == {
        "spec": datasets.CORPORATE_ACTIONS_HISTORY,
        "universe_spec": datasets.REFERENCE,
        "start": date(2025, 5, 1),
        "end": date(2025, 5, 31),
    }
    backwards = ["backfill-corporate-actions", "--from", "2025-06-01", "--to", "2025-05-31"]
    assert cli.main(backwards) == 2


class _Response:
    def __init__(self, status_code: int, content: bytes = b"[]") -> None:
        self.status_code = status_code
        self.content = content


class _Session:
    def __init__(self, answers: list[int]) -> None:
        self.headers: dict[str, str] = {}
        self.answers = answers
        self.urls: list[str] = []

    def get(self, url: str, timeout: int) -> _Response:
        self.urls.append(url)
        if url == ca.API_REFERER:
            return _Response(200, b"<html>")
        return _Response(self.answers.pop(0))


def test_api_client_visits_the_page_first_spaces_requests_and_revisits_once_on_403() -> None:
    session = _Session([403, 200])
    slept: list[float] = []
    client = cah.ApiClient(session, spacing=3.0, sleep=slept.append)  # type: ignore[arg-type]
    assert client.get(ca.INDEX_EQUITIES, date(2021, 10, 1), date(2021, 10, 31)) == b"[]"
    api = ca.api_url(ca.INDEX_EQUITIES, date(2021, 10, 1), date(2021, 10, 31))
    assert session.urls == [ca.API_REFERER, api, ca.API_REFERER, api]
    assert slept == [3.0, 3.0]
    assert session.headers["Referer"] == ca.API_REFERER
    with pytest.raises(ca.MalformedCorporateActions, match="HTTP 500"):
        cah.ApiClient(_Session([500]), sleep=slept.append).get(  # type: ignore[arg-type]
            ca.INDEX_SME, date(2021, 10, 1), date(2021, 10, 31)
        )
