"""bse_fno_freeze_limits: the BSE contract-file parser's gate, the limit per
(underlying, product), and the builder's fail-closed policy.

The failure this dataset must never produce is a limit that is too HIGH: the
desktop app slices BFO orders and protective stops by it, and an order above
BSE's single-order limit is rejected (a stop, at the moment it fires). So
every doubt resolves to the smaller number, BSE's "not set" sentinel never
reads as a limit, an older file never replaces a newer one, and every bad
fetch keeps the list we had. NSE's fno_freeze_limits file is never touched.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest

from pipeline.sources import bse_eqd_contract

FIXTURE = Path(__file__).parent / "fixtures" / "bse_eqd_contract_sample.csv"

# Cut from the real BSE_EQD_CONTRACT_14102026.csv (inside
# Live_Masters_for_Equity_Derivatives.zip), columns untouched: up to two
# futures and two option rows per underlying, plus the expiring SENSEX series'
# DelFlg X row. APLAPOLLO carries BSE's 100,000,000 "not set" value.
INDEX_SYMBOLS = ["SENSEX", "BANKEX", "SENSEX50", "FOCIT"]
STOCK_SYMBOLS = ["ADANIENT", "BAJFINANCE", "RELIANCE", "APLAPOLLO"]
MEMBER = "BSE_EQD_CONTRACT_14102026.csv"


def _csv() -> str:
    return FIXTURE.read_text()


def _zip(
    text: str | None = None, *, member: str = MEMBER, extra: dict[str, str] | None = None
) -> bytes:
    """The fixture packed as BSE packs it, beside the zip's other members."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(member, _csv() if text is None else text)
        z.writestr("EQD_DP141026", "SeriesId,Precision\n1129871,2\n")
        for name, body in (extra or {}).items():
            z.writestr(name, body)
    return buf.getvalue()


def _edit(text: str, symbol: str, edit: Callable[[int, dict[str, str]], None]) -> str:
    """`text` with `edit(n, row)` applied to the n-th row of `symbol`."""
    lines = text.splitlines()
    header = lines[0].split(",")
    out = [lines[0]]
    seen = 0
    for line in lines[1:]:
        cells = line.split(",")
        row = dict(zip(header, cells, strict=False))
        if row["TckrSymb"] == symbol:
            edit(seen, row)
            seen += 1
            cells = [row[h] for h in header]
        out.append(",".join(cells))
    assert seen, symbol
    return "\n".join(out) + "\n"


def _set(text: str, symbol: str, column: str, value: str, *, product: str | None = None) -> str:
    def setter(_n: int, row: dict[str, str]) -> None:
        if product is None or row["FinInstrmNm"][1] == product:
            row[column] = value

    return _edit(text, symbol, setter)


def _without(text: str, *symbols: str) -> str:
    lines = text.splitlines()
    column = lines[0].split(",").index("TckrSymb")
    keep = [lines[0]] + [line for line in lines[1:] if line.split(",")[column] not in symbols]
    return "\n".join(keep) + "\n"


def _limits(payload: bytes) -> pd.DataFrame:
    _day, limits = bse_eqd_contract.parse_freeze_limits(payload)
    return limits.set_index(["symbol", "product"])


# ── parser ──────────────────────────────────────────────────────────────────


def test_each_underlying_carries_a_limit_per_product():
    limits = _limits(_zip())
    symbols = sorted(INDEX_SYMBOLS + STOCK_SYMBOLS)
    assert sorted(limits.index) == sorted((s, p) for s in symbols for p in ("futures", "options"))
    columns = ["kind", "underlying_id", "lot_size", "max_order_quantity", "basis"]
    assert tuple(limits.loc[("SENSEX50", "options"), columns]) == (
        "index",
        47,
        75,
        1800,
        "exchange",
    )
    assert tuple(limits.loc[("RELIANCE", "futures"), columns]) == (
        "stock",
        500325,
        500,
        10000,
        "exchange",
    )


def test_the_day_comes_from_the_csv_name():
    day, _limits_ = bse_eqd_contract.parse_freeze_limits(_zip())
    assert day.isoformat() == "2026-10-14"
    day, _limits_ = bse_eqd_contract.parse_freeze_limits(
        _zip(member="BSE_EQD_CONTRACT_02012027.csv")
    )
    assert day.isoformat() == "2027-01-02"


def test_max_trad_qty_is_the_largest_order_bse_takes_so_nothing_is_subtracted():
    # BSE notice 20230526-54: orders "having quantity more than" the limit are
    # rejected, so 500 is an order BSE takes. NSE's file is the other way round
    # (its MaxTradQty is the first quantity that freezes; that leg subtracts one).
    limits = _limits(_zip())
    assert limits.loc[("SENSEX", "futures"), "max_order_quantity"] == 500
    assert limits.loc[("BANKEX", "futures"), "max_order_quantity"] == 600


def test_futures_and_options_keep_their_own_limits():
    # Never merged by keeping the smaller across products: SENSEX options
    # would halve to 500.
    limits = _limits(_zip())["max_order_quantity"]
    pairs = {
        s: (limits[(s, "futures")], limits[(s, "options")]) for s in ["SENSEX", "BANKEX", "FOCIT"]
    }
    assert pairs == {"SENSEX": (500, 1000), "BANKEX": (600, 900), "FOCIT": (900, 1350)}


def test_a_limit_that_is_not_a_whole_number_of_lots_is_published_as_bse_gives_it():
    # ADANIENT: lot 309, limit 9,000 (29.1 lots). The app rounds down to its
    # own contract's lot; the dataset carries the exchange's number.
    limits = _limits(_zip())
    row = limits.loc[("ADANIENT", "options")]
    assert (row["lot_size"], row["max_order_quantity"], row["basis"]) == (309, 9000, "exchange")


def test_bse_s_not_set_value_stands_at_one_lot_never_as_a_limit():
    limits = _limits(_zip())
    for product in ("futures", "options"):
        row = limits.loc[("APLAPOLLO", product)]
        assert (row["max_order_quantity"], row["basis"]) == (350, "no_exchange_limit")


def test_any_value_from_ten_million_up_is_the_not_set_sentinel():
    text = _set(_csv(), "RELIANCE", "MaxTradQty", "10000000", product="F")
    limits = _limits(_zip(text))
    assert limits.loc[("RELIANCE", "futures"), "basis"] == "no_exchange_limit"
    assert limits.loc[("RELIANCE", "futures"), "max_order_quantity"] == 500
    text = _set(_csv(), "RELIANCE", "MaxTradQty", "9999999", product="F")
    assert _limits(_zip(text)).loc[("RELIANCE", "futures"), "basis"] == "exchange"


def test_a_product_listed_with_two_limits_keeps_the_smaller_wherever_it_sits():
    for n in (0, 1):

        def lower(m: int, row: dict[str, str], n: int = n) -> None:
            if row["FinInstrmNm"] == "IO" and m == n:
                row["MaxTradQty"] = "800"

        limits = _limits(_zip(_edit(_csv(), "BANKEX", lower)))
        assert limits.loc[("BANKEX", "options"), "max_order_quantity"] == 800
        assert limits.loc[("BANKEX", "futures"), "max_order_quantity"] == 600


def test_a_real_limit_beside_the_sentinel_wins():
    def one_real(n: int, row: dict[str, str]) -> None:
        if row["FinInstrmNm"] == "SO" and n == 0:
            row["MaxTradQty"] = "7000"

    row = _limits(_zip(_edit(_csv(), "APLAPOLLO", one_real))).loc[("APLAPOLLO", "options")]
    assert (row["max_order_quantity"], row["basis"]) == (7000, "exchange")


def test_two_lot_sizes_keep_the_larger():
    def bigger_lot(n: int, row: dict[str, str]) -> None:
        if row["FinInstrmNm"] == "SF" and n == 2:
            row["MinLot"] = "600"

    row = _limits(_zip(_edit(_csv(), "RELIANCE", bigger_lot))).loc[("RELIANCE", "futures")]
    assert (row["lot_size"], row["max_order_quantity"]) == (600, 10000)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("MaxTradQty", ""),
        ("MaxTradQty", "0"),
        ("MaxTradQty", "-5"),
        ("MinLot", ""),
        ("MinLot", "0"),
    ],
)
def test_a_product_with_an_unreadable_row_stands_in_at_one_lot(column: str, value: str):
    def spoil_one(n: int, row: dict[str, str]) -> None:
        if row["FinInstrmNm"] == "IO" and n == 0:
            row[column] = value

    limits = _limits(_zip(_edit(_csv(), "SENSEX", spoil_one)))
    row = limits.loc[("SENSEX", "options")]
    assert (row["max_order_quantity"], row["basis"]) == (20, "stand_in")
    # The other product reads on.
    assert tuple(limits.loc[("SENSEX", "futures"), ["max_order_quantity", "basis"]]) == (
        500,
        "exchange",
    )


def test_an_unknown_instrument_code_stands_in_every_product_of_its_underlying():
    # A row that cannot be placed in a product could belong to either.
    def recode(n: int, row: dict[str, str]) -> None:
        if n == 0:
            row["FinInstrmNm"] = "XX"

    limits = _limits(_zip(_edit(_csv(), "BANKEX", recode)))
    assert set(limits.loc[[("BANKEX", "futures"), ("BANKEX", "options")], "basis"]) == {"stand_in"}
    assert (
        limits.loc[[("BANKEX", "futures"), ("BANKEX", "options")], "max_order_quantity"] == 30
    ).all()


def test_an_underlying_whose_every_code_is_unknown_still_gets_both_rows():
    # Absent, the builder would carry its old limit forward; it must stand in.
    def recode(_n: int, row: dict[str, str]) -> None:
        row["FinInstrmNm"] = "XX"

    limits = _limits(_zip(_edit(_csv(), "BANKEX", recode)))
    rows = limits.loc[[("BANKEX", "futures"), ("BANKEX", "options")]]
    assert set(rows["basis"]) == {"stand_in"} and (rows["max_order_quantity"] == 30).all()


def test_a_limit_below_one_lot_stands_in_never_above_what_the_file_shows():
    text = _set(_csv(), "SENSEX50", "MaxTradQty", "50", product="O")  # lot 75
    row = _limits(_zip(text)).loc[("SENSEX50", "options")]
    assert (row["max_order_quantity"], row["basis"]) == (50, "stand_in")


def test_an_unreadable_or_split_underlying_id_stands_in():
    text = _set(_csv(), "FOCIT", "UndrlygFinInstrmId", "")
    limits = _limits(_zip(text))
    assert set(limits.loc[[("FOCIT", "futures"), ("FOCIT", "options")], "basis"]) == {"stand_in"}
    assert (limits.loc[[("FOCIT", "futures"), ("FOCIT", "options")], "underlying_id"] == 0).all()

    def split(n: int, row: dict[str, str]) -> None:
        if n == 0:
            row["UndrlygFinInstrmId"] = "76"

    limits = _limits(_zip(_edit(_csv(), "FOCIT", split)))
    assert set(limits.loc[[("FOCIT", "futures"), ("FOCIT", "options")], "basis"]) == {"stand_in"}


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"<html><body>Access Denied</body></html>",
        b"%PDF-1.7 a notice",
        _zip(member="EQD_CO141026.csv"),  # no standardised contract file
        _zip(extra={"BSE_EQD_CONTRACT_13102026.csv": "TckrSymb\n"}),  # two of them
        _zip(member="BSE_EQD_CONTRACT_32132026.csv"),  # no such day
        _zip("TckrSymb,FinInstrmNm,MinLot\nSENSEX,IO,20\n"),  # a column gone
        _zip(""),
    ],
)
def test_anything_but_the_contract_file_is_refused(payload: bytes):
    with pytest.raises(bse_eqd_contract.MalformedContractFile):
        bse_eqd_contract.parse_freeze_limits(payload)
