"""nse_ca_history: the parsers' gates and the purpose classifier.

The classifier's vocabulary is pinned by `nse_ca_purpose_forms.csv`: one real
purpose for each distinct wording (numbers aside) NSE used on an equity share
from 2021-10 to 2026-10, from its corporate-actions API (main board and SME)
and from 20 book-closure files across those years, with the kinds it names.
A wording that stops being recognised, or changes kind, fails here.
"""

from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import date
from pathlib import Path

import pytest

from pipeline.sources import nse_ca_history as ca

FIXTURES = Path(__file__).parent / "fixtures"


def _forms() -> list[tuple[str, str, str]]:
    with (FIXTURES / "nse_ca_purpose_forms.csv").open(newline="") as handle:
        return [(row["source"], row["purpose"], row["actions"]) for row in csv.DictReader(handle)]


def render(actions: list[ca.Component]) -> str:
    """The actions as the fixture writes them: "dividend rs 17", "bonus 3:4",
    "split fv 10>2", "rights 2:7 prm 22"; "?" marks an unrecognised part."""
    out = []
    for action in actions:
        text = action.kind
        if action.ratio_new is not None:
            text += f" {action.ratio_new}:{action.ratio_held}"
        if action.fv_from is not None:
            text += f" fv {action.fv_from:g}>{action.fv_to:g}"
        if action.premium is not None:
            text += f" prm {action.premium:g}"
        if action.dividend_ps is not None:
            text += f" rs {action.dividend_ps:g}"
        out.append(text + ("" if action.recognised else "?"))
    return "; ".join(out)


# ── classify ────────────────────────────────────────────────────────────────


def test_every_real_purpose_form_is_recognised_with_its_kinds_and_numbers() -> None:
    forms = _forms()
    assert len(forms) == 224
    wrong = [
        (purpose, expected, got)
        for _, purpose, expected in forms
        if (got := render(ca.classify(purpose))) != expected
    ]
    assert wrong == []
    assert not any("?" in actions for _, _, actions in forms)


@pytest.mark.parametrize(
    ("purpose", "expected"),
    [
        ("Bonus 3:4", [ca.Component(ca.BONUS, ratio_new=3, ratio_held=4)]),
        ("Bonus- 1:2", [ca.Component(ca.BONUS, ratio_new=1, ratio_held=2)]),
        (
            "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share",
            [ca.Component(ca.SPLIT, fv_from=10.0, fv_to=2.0)],
        ),
        ("FVSPLT FRMRS 100 TO RE 1", [ca.Component(ca.SPLIT, fv_from=100.0, fv_to=1.0)]),
        (
            "Consolidation Of Equity Shares From Re 1 Per Share To Rs 10 Per Share",
            [ca.Component(ca.CONSOLIDATION, fv_from=1.0, fv_to=10.0)],
        ),
        # Cut at 25 characters: a split, but no number to read.
        ("FV SPLT FRM RS 216.75 TO", [ca.Component(ca.SPLIT)]),
        (
            "Rights 2:7 @ Premium Rs 22/-",
            [ca.Component(ca.RIGHTS, ratio_new=2, ratio_held=7, premium=22.0)],
        ),
        (
            "RGHTS 3:20 @PRM RS 150/-",
            [ca.Component(ca.RIGHTS, ratio_new=3, ratio_held=20, premium=150.0)],
        ),
        # Not a share-for-share ratio: left unread.
        ("Rights - 7 Ccps And 7 Warrants:40", [ca.Component(ca.RIGHTS)]),
        ("Demerger", [ca.Component(ca.DEMERGER)]),
        ("Capital Reduction Pursuant To Nclt Order", [ca.Component(ca.CAPITAL_REDUCTION)]),
        # Preference shares under a scheme: their ratio is not an equity one.
        ("Scheme Of Arrangement - Bonus Ncrps 1:10", [ca.Component(ca.SCHEME)]),
        # A capital reduction worded as a face-value change is not a split.
        (
            "Capital Reduction - Reduction Of Face Value From Rs 10 To Rs 2",
            [ca.Component(ca.CAPITAL_REDUCTION)],
        ),
        ("CAPITAL REDUCTION FV FRM RS 10 TO RS 2", [ca.Component(ca.CAPITAL_REDUCTION)]),
        # Two actions with no separator but "Per Share" between them.
        (
            "Interim Dividend - Rs 8 Per Share Special Dividend - Rs 67 Per Share",
            [
                ca.Component(ca.DIVIDEND, dividend_ps=8.0),
                ca.Component(ca.DIVIDEND, dividend_ps=67.0),
            ],
        ),
        (
            "Annual General Meeting/Dividend - Rs 5 Per Share Bonus 1:1",
            [
                ca.Component(ca.OTHER),
                ca.Component(ca.DIVIDEND, dividend_ps=5.0),
                ca.Component(ca.BONUS, ratio_new=1, ratio_held=1),
            ],
        ),
        # The book-closure file's special dividend straight after an amount.
        (
            "AGM/DIV RS 3 SPLDV RS 3",
            [
                ca.Component(ca.OTHER),
                ca.Component(ca.DIVIDEND, dividend_ps=3.0),
                ca.Component(ca.DIVIDEND, dividend_ps=3.0),
            ],
        ),
        (
            "AGM/DIV-RS1SPLDIV-RS0.75",
            [
                ca.Component(ca.OTHER),
                ca.Component(ca.DIVIDEND, dividend_ps=1.0),
                ca.Component(ca.DIVIDEND, dividend_ps=0.75),
            ],
        ),
        # Amounts NSE writes with a dash after "Rs", or beside a percentage.
        ("Interim Dividend Rs - 2.10 Per Share", [ca.Component(ca.DIVIDEND, dividend_ps=2.1)]),
        (
            "Special Dividend Rs -10 Per Share/Interim Dividend Rs - 26 Per Share",
            [
                ca.Component(ca.DIVIDEND, dividend_ps=10.0),
                ca.Component(ca.DIVIDEND, dividend_ps=26.0),
            ],
        ),
        (
            "Final Dividend - Rs 2 Per Share (20%)",
            [ca.Component(ca.DIVIDEND, dividend_ps=2.0)],
        ),
        # A bonus of preference shares is not a bonus of equity.
        ("Bonus Ncrps 1:116", [ca.Component(ca.OTHER)]),
        ("CONV TO INDEX SCHEME", [ca.Component(ca.OTHER)]),
        (
            "Annual General Meeting/Dividend - Rs 17 Per Share",
            [ca.Component(ca.OTHER), ca.Component(ca.DIVIDEND, dividend_ps=17.0)],
        ),
        (
            "DIV-RS30/SPLDIV-RS60",
            [
                ca.Component(ca.DIVIDEND, dividend_ps=30.0),
                ca.Component(ca.DIVIDEND, dividend_ps=60.0),
            ],
        ),
        (
            "Dividend - Rs 26 Per Share And Special Dividend Rs 22 Per Share",
            [
                ca.Component(ca.DIVIDEND, dividend_ps=26.0),
                ca.Component(ca.DIVIDEND, dividend_ps=22.0),
            ],
        ),
        # Two amounts in one part: which is which is not stated, so neither.
        ("DVSPDV - RS 35/RS 130", [ca.Component(ca.DIVIDEND)]),
        # No currency on the amount: not read.
        ("Dividend - 1 Rs Per Share", [ca.Component(ca.DIVIDEND)]),
        # A meeting, a dividend and a special dividend, run together; the
        # amounts are not tied to them.
        (
            "AGMDIV/SPDV RE 0.25&0.25",
            [ca.Component(ca.OTHER), ca.Component(ca.DIVIDEND), ca.Component(ca.DIVIDEND)],
        ),
        (
            "INT&SPLDIV- RS 3.57&RS 2",
            [ca.Component(ca.DIVIDEND), ca.Component(ca.DIVIDEND)],
        ),
        ("INT PAYMENT/PART REDEMPTN", [ca.Component(ca.OTHER), ca.Component(ca.OTHER)]),
        ("Buy Back", [ca.Component(ca.OTHER)]),
        ("EOGM", [ca.Component(ca.OTHER)]),
    ],
)
def test_classify_reads_kinds_and_only_the_numbers_the_purpose_states(
    purpose: str, expected: list[ca.Component]
) -> None:
    assert ca.classify(purpose) == expected


def test_a_purpose_the_vocabulary_does_not_know_is_other_and_unrecognised() -> None:
    assert ca.classify("Spin Off Of Unit") == [ca.Component(ca.OTHER, recognised=False)]
    assert ca.classify("") == [ca.Component(ca.OTHER, recognised=False)]


# ── ISIN ────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("INE301A01014", "INE301A01014"),
        (" ine00wc01019 ", "INE00WC01019"),
        ("IN0000000000", None),  # NSE's placeholder (KSHITIJPOL)
        ("INE301A01015", None),  # check digit wrong
        ("460862", None),  # the SME API's 6-digit field
        (None, None),
        ("-", None),
    ],
)
def test_valid_isin(value: object, expected: str | None) -> None:
    assert ca.valid_isin(value) == expected


# ── API payload ─────────────────────────────────────────────────────────────

OCT_2021 = (date(2021, 10, 1), date(2021, 10, 31))


def _api(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_api_payload_keeps_equity_rows_with_their_fields() -> None:
    frame = ca.parse_api_payload(
        _api("nse_ca_api_equities_202110.json"), index=ca.INDEX_EQUITIES,
        start=OCT_2021[0], end=OCT_2021[1],
    )
    # 68 rows: 57 equity, 9 G-sec, 2 InvIT.
    assert len(frame) == 57
    assert set(frame["series"]) == {"EQ"}
    affle = frame[frame["symbol"] == "AFFLE"].iloc[0].to_dict()
    assert affle == {
        "symbol": "AFFLE",
        "series": "EQ",
        "company": "Affle 3i Limited",
        "isin_nse": "INE00WC01019",
        "ex_date": date(2021, 10, 7),
        "record_date": date(2021, 10, 8),
        "purpose": "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share",
        "source": ca.SOURCE_API,
    }


def test_sme_payload_never_reads_its_isin_field() -> None:
    frame = ca.parse_api_payload(
        _api("nse_ca_api_sme_202509.json"), index=ca.INDEX_SME,
        start=date(2025, 9, 1), end=date(2025, 9, 30),
    )
    assert len(frame) == 33
    assert set(frame["series"]) == {"SM"}
    assert frame["isin_nse"].isna().all()


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": "TCS", "series": "EQ", "isin": "INE467B01029", "comp": "TCS Ltd",
        "subject": "Interim Dividend - Rs 7 Per Share", "exDate": "14-Oct-2021",
        "recDate": "15-Oct-2021",
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        (b"<html><body>Access Denied</body></html>", "not JSON"),
        (json.dumps({"data": []}).encode(), "not a JSON list"),
        (json.dumps(["x"]).encode(), "not an object"),
        (json.dumps([{k: v for k, v in _row().items() if k != "isin"}]).encode(), "lacks isin"),
        (json.dumps([_row(symbol=" ")]).encode(), "without a symbol"),
        (json.dumps([_row(exDate="-")]).encode(), "no exDate"),
        (json.dumps([_row(exDate="2021-10-14")]).encode(), "not dd-Mon-yyyy"),
        (json.dumps([_row(exDate="01-Nov-2021")]).encode(), "outside the window"),
    ],
)
def test_api_payload_gate_refuses_what_nse_does_not_serve(payload: bytes, match: str) -> None:
    with pytest.raises(ca.MalformedCorporateActions, match=match):
        ca.parse_api_payload(payload, index=ca.INDEX_EQUITIES, start=OCT_2021[0], end=OCT_2021[1])


def test_api_url_names_the_index_and_window() -> None:
    assert ca.api_url(ca.INDEX_SME, date(2021, 10, 1), date(2021, 10, 31)) == (
        "https://www.nseindia.com/api/corporates-corporateActions"
        "?index=sme&from_date=01-10-2021&to_date=31-10-2021"
    )


# ── Book-closure file ───────────────────────────────────────────────────────


def test_book_closure_file_of_2026_keeps_equity_rows_once_preferring_eq() -> None:
    frame = ca.parse_book_closure_csv((FIXTURES / "nse_bc14102026.csv").read_bytes())
    # 160 rows, mostly bonds; TCS is listed under BE, BL, EQ, IQ, T0 and TL.
    assert len(frame) == 10
    tcs = frame[frame["symbol"] == "TCS"]
    assert tcs[["series", "ex_date", "purpose"]].values.tolist() == [
        ["EQ", date(2026, 10, 14), "INTDIV - RS 12 PER SH"]
    ]
    assert frame[frame["symbol"] == "OLIL"]["series"].tolist() == ["SM"]
    assert frame["isin_nse"].isna().all()
    assert set(frame["source"]) == {ca.SOURCE_BOOK_CLOSURE}


def test_book_closure_file_of_2021_reads_its_older_dates_and_padded_purposes() -> None:
    frame = ca.parse_book_closure_csv((FIXTURES / "nse_Bc011021.csv").read_bytes())
    affle = frame[frame["symbol"] == "AFFLE"].iloc[0]
    assert (affle["ex_date"], affle["record_date"], affle["purpose"]) == (
        date(2021, 10, 7), date(2021, 10, 8), "FVSPLT FRM RS 10TO RS 2",
    )
    # No ex-date and no record date in that file: the record date stands in
    # only when there is one.
    cartrade = frame[frame["symbol"] == "CARTRADE"].iloc[0]
    assert (cartrade["ex_date"], cartrade["record_date"]) == (date(2021, 9, 23), None)


def test_book_closure_gate() -> None:
    with pytest.raises(ca.MalformedCorporateActions, match="empty"):
        ca.parse_book_closure_csv(b"")
    with pytest.raises(ca.MalformedCorporateActions, match="unexpected header"):
        ca.parse_book_closure_csv(b"<html>maintenance</html>\n")
    with pytest.raises(ca.MalformedCorporateActions, match="not a zip"):
        ca.parse_pr_bundle(b"<html>")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        bundle.writestr("pr14102026.csv", "x")
    with pytest.raises(ca.MalformedCorporateActions, match="no book-closure file"):
        ca.parse_pr_bundle(buffer.getvalue())
