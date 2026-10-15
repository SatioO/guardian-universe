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

import dataclasses
import hashlib
import io
import zipfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import responses

from pipeline import builders, datasets, fetch
from pipeline.errors import UnexpectedFailure
from pipeline.sources import bse_eqd_contract, nse_fo_contract

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


# ── builder ─────────────────────────────────────────────────────────────────

# File D is written around 17:40 IST on D, for the next session.
FILE_DAY = date(2026, 10, 14)
TARGET = date(2026, 10, 15)
OUT_NAME = "bse_fno_freeze_limits_all.parquet"


@pytest.fixture
def spec(tmp_path: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.BSE_FNO_FREEZE_LIMITS, base_dir=tmp_path / "fno")


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fetch.time, "sleep", lambda *_a, **_k: None)


def _build(
    spec: datasets.DatasetSpec,
    text: str | bytes | None = None,
    *,
    file_day: date = FILE_DAY,
    target: date = TARGET,
    min_rows: int = 5,
    trading: bool = True,
):
    body = text if isinstance(text, bytes) else _zip(text)

    def fetch_limits(_d: date) -> tuple[date, pd.DataFrame]:
        _named_day, limits = bse_eqd_contract.parse_freeze_limits(body)
        return file_day, limits

    return builders.build_bse_fno_freeze_limits(
        spec,
        target,
        fetch_limits=fetch_limits,
        trading_day=lambda d: trading and d.weekday() < 5,
        min_rows=min_rows,
    )


def _out(spec: datasets.DatasetSpec) -> pd.DataFrame:
    return pd.read_parquet(spec.base_dir / OUT_NAME).set_index(["symbol", "product"])


def _quantity(spec: datasets.DatasetSpec, symbol: str, product: str) -> int:
    return int(_out(spec).loc[(symbol, product), "max_order_quantity"])


def _first(spec: datasets.DatasetSpec) -> None:
    """A published file from the day before (file 13 Oct, for 14 Oct)."""
    status = _build(spec, file_day=date(2026, 10, 13), target=date(2026, 10, 14))
    assert status.status == "success", status.message


def test_a_good_run_publishes_a_row_per_underlying_and_product_dated_by_its_file(
    spec: datasets.DatasetSpec,
):
    status = _build(spec)
    assert status.status == "success", status.message
    out = pd.read_parquet(spec.base_dir / OUT_NAME)
    assert list(out.columns) == [
        "symbol",
        "kind",
        "product",
        "underlying_id",
        "lot_size",
        "max_order_quantity",
        "basis",
        "date",
        "source",
    ]
    assert len(out) == 16 and not out.duplicated(["symbol", "product"]).any()
    assert (out["date"] == pd.Timestamp(FILE_DAY)).all()
    assert set(out["source"]) == {"bse-eqd-contract"}
    # The run says how many underlyings BSE shows no limit for.
    assert "1 underlying with no exchange limit" in status.message, status.message


def test_the_published_file_is_typed_for_the_client(spec: datasets.DatasetSpec):
    _build(spec)
    schema = pq.read_schema(spec.base_dir / OUT_NAME)
    for name in ("symbol", "kind", "product", "basis", "source"):
        assert schema.field(name).type == pa.string(), name
    for name in ("underlying_id", "lot_size", "max_order_quantity"):
        assert schema.field(name).type == pa.int64(), name
    assert schema.field("date").type == pa.timestamp("ms")


def test_a_holiday_does_not_restamp_the_list(spec: datasets.DatasetSpec):
    assert _build(spec, trading=False).status == "skipped_holiday"
    assert not (spec.base_dir / OUT_NAME).exists()


def test_an_older_file_never_replaces_a_newer_one(spec: datasets.DatasetSpec):
    assert _build(spec).status == "success"
    higher = _set(_csv(), "SENSEX", "MaxTradQty", "1500", product="F")
    older = _build(spec, higher, file_day=date(2026, 10, 13))
    assert older.status == "skipped_idempotent", older.message
    assert _quantity(spec, "SENSEX", "futures") == 500


def test_the_same_day_s_file_posted_again_is_read_again(spec: datasets.DatasetSpec):
    assert _build(spec).status == "success"
    corrected = _set(_csv(), "SENSEX", "MaxTradQty", "400", product="F")
    status = _build(spec, corrected)
    assert status.status == "success", status.message
    assert _quantity(spec, "SENSEX", "futures") == 400


def test_a_failed_fetch_keeps_the_list_we_had(spec: datasets.DatasetSpec):
    assert _build(spec).status == "success"
    status = _build(spec, b"<html>Access Denied</html>")
    assert status.status == "skipped_idempotent", status.message
    assert len(_out(spec)) == 16


def test_a_first_run_with_nothing_to_keep_fails(spec: datasets.DatasetSpec):
    assert _build(spec, b"<html>Access Denied</html>").status == "failed"
    assert not (spec.base_dir / OUT_NAME).exists()


def test_a_truncated_file_is_not_published(spec: datasets.DatasetSpec):
    status = _build(spec, _without(_csv(), *STOCK_SYMBOLS), min_rows=8)
    assert status.status == "failed" and "floor" in status.message


def test_an_underlying_that_left_is_carried_forward_and_the_rest_still_publish(
    spec: datasets.DatasetSpec,
):
    _first(spec)
    lowered = _set(_without(_csv(), "RELIANCE"), "BANKEX", "MaxTradQty", "450", product="O")
    status = _build(spec, lowered)
    assert status.status == "success", status.message
    assert "RELIANCE/futures" in status.message
    assert _quantity(spec, "BANKEX", "options") == 450  # the lower limit is not withheld
    assert _quantity(spec, "RELIANCE", "options") == 10000  # carried, so no row is lost
    assert len(_out(spec)) == 16


def test_an_underlying_still_listed_never_gets_its_old_limit_back(spec: datasets.DatasetSpec):
    _first(spec)

    def lowered_with_a_blank(n: int, row: dict[str, str]) -> None:
        if row["FinInstrmNm"] == "IO":
            row["MaxTradQty"] = "" if n == 0 else "800"

    status = _build(spec, _edit(_csv(), "BANKEX", lowered_with_a_blank))
    assert status.status == "success", status.message
    row = _out(spec).loc[("BANKEX", "options")]
    assert (row["max_order_quantity"], row["basis"]) == (30, "stand_in")


def test_many_stand_ins_mean_the_format_changed_and_fail(spec: datasets.DatasetSpec):
    _first(spec)
    text = _csv()
    for symbol in ["SENSEX", "BANKEX"]:
        text = _set(text, symbol, "MinLot", "")
    status = _build(spec, text)
    assert status.status == "failed", status.message
    assert _quantity(spec, "SENSEX", "options") == 1000


def test_many_rows_missing_is_a_partial_file_not_an_exit(spec: datasets.DatasetSpec):
    _first(spec)
    status = _build(spec, _without(_csv(), "RELIANCE", "BAJFINANCE"), min_rows=5)
    assert status.status == "failed", status.message
    assert len(_out(spec)) == 16


def test_limits_a_trading_day_behind_are_loud_whatever_the_cause(spec: datasets.DatasetSpec):
    # File D serves session D+1: a run on D+1 holding file D is current.
    _first(spec)
    stale = _build(spec, file_day=date(2026, 10, 13), target=date(2026, 10, 15))
    assert stale.status == "failed", stale.message
    unreachable = _build(spec, b"<html>Access Denied</html>", target=date(2026, 10, 15))
    assert unreachable.status == "failed", unreachable.message
    assert len(_out(spec)) == 16


# BSE's "not set" value: one lot, never a limit, and never a broken file
# passed off as "no limits".


def test_not_set_rows_are_not_stand_ins_so_bse_s_75_never_trip_the_churn_guard(
    spec: datasets.DatasetSpec,
):
    text = _csv()
    for symbol in ["ADANIENT", "BAJFINANCE", "RELIANCE"]:
        text = _set(text, symbol, "MaxTradQty", "100000000")
    status = _build(spec, text)  # a first run: 8 not-set rows, churn allows 3
    assert status.status == "success", status.message
    assert "4 underlyings with no exchange limit" in status.message
    assert _quantity(spec, "RELIANCE", "futures") == 500


def test_an_index_showing_the_not_set_value_fails_the_run(spec: datasets.DatasetSpec):
    _first(spec)
    status = _build(spec, _set(_csv(), "SENSEX", "MaxTradQty", "100000000", product="F"))
    assert status.status == "failed", status.message
    assert "SENSEX" in status.message
    assert _quantity(spec, "SENSEX", "futures") == 500  # nothing written


def test_an_index_showing_the_not_set_value_fails_even_a_first_run(spec: datasets.DatasetSpec):
    status = _build(spec, _set(_csv(), "SENSEX50", "MaxTradQty", "100000000", product="O"))
    assert status.status == "failed", status.message
    assert not (spec.base_dir / OUT_NAME).exists()


def test_a_jump_in_not_set_rows_past_churn_fails_the_run(spec: datasets.DatasetSpec):
    _first(spec)
    text = _csv()
    for symbol in ["ADANIENT", "BAJFINANCE"]:  # 4 rows newly not set; churn allows 3
        text = _set(text, symbol, "MaxTradQty", "100000000")
    status = _build(spec, text)
    assert status.status == "failed", status.message
    assert "no exchange limit" in status.message
    assert _quantity(spec, "ADANIENT", "options") == 9000


def test_not_set_rows_within_churn_publish(spec: datasets.DatasetSpec):
    _first(spec)
    status = _build(spec, _set(_csv(), "ADANIENT", "MaxTradQty", "100000000"))
    assert status.status == "success", status.message
    row = _out(spec).loc[("ADANIENT", "options")]
    assert (row["max_order_quantity"], row["basis"]) == (309, "no_exchange_limit")


# A consumer matches an underlying by underlying_id (ISIN is blank), so the
# file never holds two rows for one (underlying_id, product).


def _renamed(text: str, old: str, new: str) -> str:
    def rename(_n: int, row: dict[str, str]) -> None:
        row["TckrSymb"] = new

    return _edit(text, old, rename)


def test_a_renamed_underlying_never_publishes_its_old_limit_beside_the_new(
    spec: datasets.DatasetSpec,
):
    # BSE renames RELIANCE's TckrSymb, keeping UndrlygFinInstrmId 500325, and
    # lowers its limit at the same time: the old 10,000 must not be carried.
    _first(spec)
    renamed = _set(_renamed(_csv(), "RELIANCE", "RELIND"), "RELIND", "MaxTradQty", "8000")
    status = _build(spec, renamed)
    assert status.status == "success", status.message
    assert "RELIANCE/futures" in status.message and "carried" not in status.message
    out = _out(spec)
    assert "RELIANCE" not in out.index.get_level_values("symbol")
    ids = out[out["underlying_id"] == 500325]
    assert len(ids) == 2 and set(ids["max_order_quantity"]) == {8000}


def test_a_gone_row_whose_underlying_is_still_listed_is_never_carried_even_in_part(
    spec: datasets.DatasetSpec,
):
    # Only the futures of the renamed underlying are listed (its options
    # gone): the old options row has no listed twin, so it is carried.
    _first(spec)

    def futures_only(text: str) -> str:
        lines = text.splitlines()
        header = lines[0].split(",")
        symbol, code = header.index("TckrSymb"), header.index("FinInstrmNm")
        keep = [lines[0]] + [
            line
            for line in lines[1:]
            if not (line.split(",")[symbol] == "RELIANCE" and line.split(",")[code] == "SO")
        ]
        return "\n".join(keep) + "\n"

    status = _build(spec, _renamed(futures_only(_csv()), "RELIANCE", "RELIND"))
    assert status.status == "success", status.message
    out = _out(spec)
    assert ("RELIANCE", "futures") not in out.index
    assert out.loc[("RELIANCE", "options"), "max_order_quantity"] == 10000  # carried
    assert out.loc[("RELIND", "futures"), "max_order_quantity"] == 10000


def test_two_listed_underlyings_sharing_an_underlying_id_fail_the_run(spec: datasets.DatasetSpec):
    _first(spec)
    shared = _set(_csv(), "BAJFINANCE", "UndrlygFinInstrmId", "512599")  # ADANIENT's
    status = _build(spec, shared)
    assert status.status == "failed", status.message
    assert "512599" in status.message
    assert _out(spec).loc[("BAJFINANCE", "options"), "underlying_id"] == 500034  # nothing written


def test_unknown_underlying_ids_never_clash():
    # 0 is "unreadable", not an underlying: two stand-ins may both carry it.
    rows = pd.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC", "CCC"],
            "product": ["options", "options", "futures", "options"],
            "underlying_id": [0, 0, 7, 7],
        }
    )
    assert builders._bse_clash(rows) is None
    clash = pd.concat([rows, rows.iloc[[2]].assign(symbol="DDD")], ignore_index=True)
    assert "7" in (builders._bse_clash(clash) or "")
    # Nor does a gone stand-in count as listed again: it is carried as usual.
    gone = pd.DataFrame({"symbol": ["OLD"], "product": ["options"], "underlying_id": [0]})
    assert not builders._bse_replaced(gone, rows).any()


def test_a_run_never_touches_nse_s_file(tmp_path: Path):
    # Both live in data/fno; the NSE file's bytes and manifest entry stay its own.
    from pipeline import manifest

    base = tmp_path / "fno"
    nse = dataclasses.replace(datasets.FNO_FREEZE_LIMITS, base_dir=base)
    bse = dataclasses.replace(datasets.BSE_FNO_FREEZE_LIMITS, base_dir=base)
    nse_body = (Path(__file__).parent / "fixtures" / "nse_fo_contract_sample.csv.gz").read_bytes()
    nse_status = builders.build_fno_freeze_limits(
        nse,
        date(2026, 10, 1),
        fetch_limits=lambda _d: (date(2026, 10, 1), nse_fo_contract.parse_freeze_limits(nse_body)),
        trading_day=lambda _d: True,
        min_rows=5,
    )
    assert nse_status.status == "success", nse_status.message
    nse_path = base / "fno_freeze_limits_all.parquet"
    before = hashlib.sha256(nse_path.read_bytes()).hexdigest()
    assert _build(bse).status == "success"
    assert hashlib.sha256(nse_path.read_bytes()).hexdigest() == before
    m = manifest.build_manifest([nse, bse], latest_trading_date=TARGET, generated_at="t")
    files = {e["name"]: [f["name"] for f in e["baseline"]] for e in m["datasets"]}
    assert files == {
        "fno_freeze_limits": ["fno_freeze_limits_all.parquet"],
        "bse_fno_freeze_limits": [OUT_NAME],
    }


def test_both_venues_mark_rows_with_the_same_basis_words():
    assert bse_eqd_contract.BASIS_EXCHANGE == nse_fo_contract.BASIS_EXCHANGE
    assert bse_eqd_contract.BASIS_STAND_IN == nse_fo_contract.BASIS_STAND_IN


# ── fetch ───────────────────────────────────────────────────────────────────


@responses.activate
def test_the_fetch_reads_the_rolling_zip_as_a_browser_and_dates_it_by_its_csv():
    # python-requests' own User-Agent gets 403 from bseindia.com (2026-10-15).
    responses.add(responses.GET, bse_eqd_contract.URL, body=_zip(), status=200)
    day, limits = builders._fetch_bse_freeze_limits(date(2026, 10, 15))
    assert day == FILE_DAY
    assert len(limits) == 16
    (call,) = responses.calls
    assert call.request.headers["User-Agent"] == fetch._BROWSER_UA


@responses.activate
@pytest.mark.parametrize(
    ("status", "body"),
    [(403, b"<HTML>Access Denied</HTML>"), (404, b"gone"), (500, b"oops")],
)
def test_a_refused_fetch_raises_after_its_retries(status: int, body: bytes):
    responses.add(responses.GET, bse_eqd_contract.URL, body=body, status=status)
    with pytest.raises(UnexpectedFailure):
        builders._fetch_bse_freeze_limits(date(2026, 10, 15))
    assert len(responses.calls) == 3


@responses.activate
def test_a_wrong_document_served_with_200_is_refused():
    responses.add(responses.GET, bse_eqd_contract.URL, body=b"<html>maint</html>", status=200)
    with pytest.raises(bse_eqd_contract.MalformedContractFile):
        builders._fetch_bse_freeze_limits(date(2026, 10, 15))


# ── registration ────────────────────────────────────────────────────────────


def test_bse_fno_freeze_limits_spec_fields():
    s = datasets.BSE_FNO_FREEZE_LIMITS
    assert (s.key, s.file_prefix, s.manifest_name, s.schema_version) == (
        "bse_fno_freeze_limits",
        "bse_fno_freeze_limits",
        "bse_fno_freeze_limits",
        1,
    )
    assert s.source_label == bse_eqd_contract.SOURCE
    assert s.derived is True and s.external is False and s.published is True
    assert datasets.DATASETS["bse_fno_freeze_limits"] is s
    assert datasets.by_manifest_name("bse_fno_freeze_limits") is s
    assert s in datasets.all_specs()
    with pytest.raises(RuntimeError, match="derived dataset has no fetcher"):
        s.make_fetcher()
    # NSE's spec is unchanged: same file, schema v1.
    n = datasets.FNO_FREEZE_LIMITS
    assert (n.file_prefix, n.manifest_name, n.schema_version) == (
        "fno_freeze_limits",
        "fno_freeze_limits",
        1,
    )


def test_bse_fno_freeze_limits_is_built_by_the_daily_loop():
    from pipeline import cli

    built = cli.builders.BUILDERS["bse_fno_freeze_limits"]
    assert built is cli.builders.build_bse_fno_freeze_limits
    order = datasets.DATASET_ORDER
    assert order.index("bse_fno_freeze_limits") == order.index("fno_freeze_limits") + 1


def test_the_manifest_lists_the_published_file(spec: datasets.DatasetSpec):
    from pipeline import manifest, publish

    _build(spec)
    m = manifest.build_manifest([spec], latest_trading_date=TARGET, generated_at="t")
    (entry,) = m["datasets"]
    assert entry["name"] == "bse_fno_freeze_limits"
    assert entry["latest_date"] == FILE_DAY.isoformat()
    (f,) = entry["baseline"]
    assert f["name"] == OUT_NAME and f["rows"] == 16
    prefixes = frozenset(s.file_prefix for s in datasets.all_specs())
    assert publish.owns_asset(f["asset"], file_prefixes=prefixes)
