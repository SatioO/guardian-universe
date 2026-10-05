"""corporate_actions: the book-closure parser's gate, the purpose classifier,
and the builder's accumulation and fail-closed policy.

The failure this dataset must never produce is a MISSING ex-date: the app
pauses a price alert before its stock opens ex-bonus or ex-split, and an
action dropped here is an alert that fires on the re-based price. So the
table only grows, a bad day keeps the prior file, and a purpose the
classifier does not know is named, never guessed at.
"""

from __future__ import annotations

import dataclasses
import io
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline import builders, datasets
from pipeline.sources import nse_corporate_actions as ca

FIXTURE = Path(__file__).parent / "fixtures" / "nse_bc01102026.csv"
TARGET = date(2026, 10, 1)  # a Thursday; the fixture is that day's real file


def _spec(tmp_path: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.CORPORATE_ACTIONS, base_dir=tmp_path / "corporate_actions")


def _bundle(csv_bytes: bytes, name: str = "bc01102026.csv") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        bundle.writestr("readme.txt", "PR bundle")
        bundle.writestr(name, csv_bytes)
    return buffer.getvalue()


def _weekday(day: date) -> bool:
    return day.weekday() < 5


def _table(spec: datasets.DatasetSpec) -> pd.DataFrame:
    return pd.read_parquet(spec.base_dir / "corporate_actions_all.parquet")


# ── classify ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("purpose", "expected"),
    [
        # The vocabulary of 50 real bundles (Aug–Oct 2026).
        ("BONUS 1:1", [("bonus", 0.5)]),
        ("BONUS 2:5", [("bonus", 5 / 7)]),
        ("BONUS 2:1", [("bonus", 1 / 3)]),
        ("FVSPLT FRM RS 10 TO RS 2", [("split", 0.2)]),
        ("FVSPLT FRM RS 10 TO RE 1", [("split", 0.1)]),
        ("FVSPLT FRM RS 2 TO RE 1", [("split", 0.5)]),
        ("FV CONSOLDTN FRM RS 1 TO RS 10", [("consolidation", 10.0)]),
        ("RGHTS 2:21 @PRM RS 748/-", [("rights", None)]),
        ("RIGHTS 1:1 @ PRM RS 25/-", [("rights", None)]),
        ("RIGHTS- 7CCPS/ 7WRNTS:40", [("rights", None)]),
        ("DEMERGER", [("demerger", None)]),
        ("CAPITAL REDUCTION", [("capital_reduction", None)]),
        ("BONUS 1:1/FVSPLT FRM RS 10 TO RS 5", [("bonus", 0.5), ("split", 0.5)]),
    ],
)
def test_a_re_basing_purpose_is_classified_with_its_price_factor(purpose, expected):
    actions = ca.classify(purpose)
    assert [(a.kind, a.price_factor) for a in actions] == [
        (kind, pytest.approx(factor) if factor is not None else None) for kind, factor in expected
    ]


@pytest.mark.parametrize(
    "purpose",
    [
        "DIV - RE 1 PER SH",
        "INTDIV - RS 8 PER SH",
        "INTEREST PAYMENT",
        "INT PAYMENT/REDEMPTN",
        "INT PAYMNT/PART REDEMPTN",
        "BUY BACK",
        "DISTRIBUTN- RS 6.31",
        "FSTFINCL RS 115 PR SH",
        "AGM/DIV - RS 2 PER SH",
        # Seen by the first live build (2026-10-01, 20 trading days back).
        "INTDVSPDVRS 7.50 & 86.50",
        "CONVERSION INTO EQUITY",
        "INT PAYMENT/PART RDEMPTN",
        "INTT PAYMENT/REDEMPTION",
    ],
)
def test_a_purpose_that_never_re_bases_a_price_is_dropped(purpose):
    assert ca.classify(purpose) == []


def test_an_unknown_purpose_is_not_guessed_at():
    assert ca.classify("AMALGAMATION") is None


# ── parse ───────────────────────────────────────────────────────────────────


def test_the_real_file_parses_one_row_per_symbol_ex_date_and_purpose():
    rows = ca.parse_book_closure_csv(FIXTURE.read_bytes())
    mold = rows[rows["symbol"] == "MOLDTKPAC"]
    assert mold.to_dict("records") == [
        {"symbol": "MOLDTKPAC", "ex_date": date(2026, 10, 9), "purpose": "BONUS 1:1"}
    ], "four series, one row"
    # KMSUGAR: record date 2 Oct, ex-date 1 Oct — the ex-date wins.
    assert rows[rows["symbol"] == "KMSUGAR"]["ex_date"].tolist() == [date(2026, 10, 1)]


def test_a_blank_ex_date_falls_back_to_the_record_date():
    payload = (
        ",".join(ca.EXPECTED_HEADER) + "\n"
        "EQ,ABC,Abc Ltd,2026-10-12,,,,,,BONUS 1:1\n"
        "EQ,XYZ,Xyz Ltd,,,,,,,BONUS 1:1\n"
    ).encode()
    rows = ca.parse_book_closure_csv(payload)
    assert rows.to_dict("records") == [
        {"symbol": "ABC", "ex_date": date(2026, 10, 12), "purpose": "BONUS 1:1"}
    ]


@pytest.mark.parametrize(
    "payload",
    [b"", b"<html><body>Access Denied</body></html>", b"SYMBOL,PURPOSE\nABC,BONUS 1:1\n"],
)
def test_anything_but_the_book_closure_file_is_refused(payload):
    with pytest.raises(ca.MalformedBookClosure):
        ca.parse_book_closure_csv(payload)


def test_a_bundle_without_the_book_closure_file_is_refused():
    with pytest.raises(ca.MalformedBookClosure):
        ca.parse_pr_bundle(_bundle(b"x", name="pd01102026.csv"))
    with pytest.raises(ca.MalformedBookClosure):
        ca.parse_pr_bundle(b"<html>404</html>")


# ── build ───────────────────────────────────────────────────────────────────


def test_a_first_build_seeds_from_earlier_days_and_publishes_typed_columns(tmp_path):
    spec = _spec(tmp_path)
    earlier = date(2026, 9, 30)
    seen = []

    def fetch(day: date) -> pd.DataFrame:
        seen.append(day)
        if day == earlier:
            return pd.DataFrame(
                [
                    {
                        "symbol": "EARLY",
                        "ex_date": date(2026, 10, 2),
                        "purpose": "FVSPLT FRM RS 10 TO RS 2",
                    }
                ]
            )
        if day == TARGET:
            return ca.parse_pr_bundle(_bundle(FIXTURE.read_bytes()))
        raise ca.MalformedBookClosure("no bundle")

    status = builders.build_corporate_actions(
        spec, TARGET, fetch_bundle=fetch, trading_day=_weekday, seed_days=3
    )

    assert status.status == "success", status.message
    assert seen == [date(2026, 9, 29), earlier, TARGET]
    table = _table(spec)
    early = table[table["symbol"] == "EARLY"].iloc[0]
    assert (early["kind"], early["price_factor"], early["first_seen"]) == ("split", 0.2, earlier)
    mold = table[table["symbol"] == "MOLDTKPAC"].iloc[0]
    assert (mold["kind"], mold["price_factor"], mold["ex_date"]) == (
        "bonus",
        0.5,
        date(2026, 10, 9),
    )
    assert set(table["kind"]) <= set(ca.KINDS), "no dividends, interest or buy-backs"
    assert "seed days without a bundle: 2026-09-29" in status.message
    schema = pq.read_schema(spec.base_dir / "corporate_actions_all.parquet")
    assert schema.field("ex_date").type == pa.date32()
    assert schema.field("price_factor").type == pa.float64()
    assert schema.field("date").type == pa.timestamp("ms")


def test_the_table_accumulates_and_keeps_an_actions_first_sighting(tmp_path):
    spec = _spec(tmp_path)
    builders.build_corporate_actions(
        spec,
        TARGET,
        fetch_bundle=lambda _: ca.parse_pr_bundle(_bundle(FIXTURE.read_bytes())),
        trading_day=_weekday,
        seed_days=1,
    )
    first = _table(spec)
    nxt = date(2026, 10, 5)
    later = pd.DataFrame(
        [
            {
                "symbol": "MOLDTKPAC",
                "ex_date": date(2026, 10, 9),
                "purpose": "BONUS 1:1",
            },  # listed again
            {"symbol": "NEWCO", "ex_date": date(2026, 10, 15), "purpose": "BONUS 1:2"},
        ]
    )

    status = builders.build_corporate_actions(
        spec, nxt, fetch_bundle=lambda _: later, trading_day=_weekday
    )

    table = _table(spec)
    assert len(table) == len(first) + 1, status.message
    assert table[table["symbol"] == "MOLDTKPAC"]["first_seen"].tolist() == [TARGET]
    assert table[table["symbol"] == "NEWCO"]["price_factor"].tolist() == [pytest.approx(2 / 3)]
    # An action no longer listed (its ex-date passed) stays: the table never shrinks.
    assert set(first["symbol"]) <= set(table["symbol"])
    assert (table["date"] == pd.Timestamp(nxt)).all()


def test_a_bad_day_keeps_the_prior_file(tmp_path):
    spec = _spec(tmp_path)
    builders.build_corporate_actions(
        spec,
        TARGET,
        fetch_bundle=lambda _: ca.parse_pr_bundle(_bundle(FIXTURE.read_bytes())),
        trading_day=_weekday,
        seed_days=1,
    )
    before = _table(spec)

    def broken(_: date) -> pd.DataFrame:
        raise ca.MalformedBookClosure("unexpected header '<html>'")

    status = builders.build_corporate_actions(
        spec, date(2026, 10, 5), fetch_bundle=broken, trading_day=_weekday
    )

    assert status.status == "skipped_idempotent"
    pd.testing.assert_frame_equal(_table(spec), before)


def test_a_first_build_without_the_target_bundle_fails(tmp_path):
    def missing(_: date) -> pd.DataFrame:
        raise ca.MalformedBookClosure("404")

    status = builders.build_corporate_actions(
        _spec(tmp_path), TARGET, fetch_bundle=missing, trading_day=_weekday, seed_days=1
    )
    assert status.status == "failed"


def test_a_holiday_reads_nothing(tmp_path):
    def unexpected(_: date) -> pd.DataFrame:
        raise AssertionError("no fetch on a holiday")

    status = builders.build_corporate_actions(
        _spec(tmp_path), date(2026, 10, 3), fetch_bundle=unexpected, trading_day=_weekday
    )
    assert status.status == "skipped_holiday"


def test_an_unrecognised_purpose_is_named_not_guessed(tmp_path):
    spec = _spec(tmp_path)
    frame = pd.DataFrame(
        [
            {"symbol": "ABC", "ex_date": date(2026, 10, 9), "purpose": "AMALGAMATION"},
            {"symbol": "DEF", "ex_date": date(2026, 10, 9), "purpose": "BONUS 1:1"},
        ]
    )
    status = builders.build_corporate_actions(
        spec, TARGET, fetch_bundle=lambda _: frame, trading_day=_weekday, seed_days=1
    )
    assert status.status == "success"
    assert "AMALGAMATION" in status.message
    assert _table(spec)["symbol"].tolist() == ["DEF"]


def test_the_dataset_is_registered_for_publishing():
    assert datasets.by_manifest_name("corporate_actions") is datasets.CORPORATE_ACTIONS
    assert "corporate_actions" in datasets.DATASET_ORDER
