"""fno_freeze_limits: the contract-file parser's gate, the per-underlying
limit, and the builder's fail-closed policy.

The failure this dataset must never produce is a limit that is too HIGH: the
desktop app slices orders and protective stops by it, and a stop sized above
the exchange's freeze is rejected at the moment it fires. So every doubt
resolves to the smaller number (or to no number at all), an older file never
replaces a newer one, and every bad fetch keeps the list we had.
"""

from __future__ import annotations

import dataclasses
import gzip
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import responses

from pipeline import builders, datasets, fetch
from pipeline.errors import NotYetPublished
from pipeline.sources import nse_fo_contract

FIXTURE = Path(__file__).parent / "fixtures" / "nse_fo_contract_sample.csv.gz"

# The trimmed real file (NSE_FO_contract_01102026.csv.gz): two rows per
# instrument kind for six index and six stock underlyings, plus NSE's own
# test underlying (011NSETEST, never permitted to trade), columns untouched.
INDEX_SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTYFPI"]
STOCK_SYMBOLS = ["RELIANCE", "TCS", "M&M", "BAJAJ-AUTO", "HDFCBANK", "SBIN"]

# File D is posted on the evening of D for the next session.
FILE_DAY = date(2026, 10, 1)
TARGET = date(2026, 10, 1)


def _fixture() -> bytes:
    return FIXTURE.read_bytes()


def _csv(payload: bytes) -> str:
    return gzip.decompress(payload).decode()


def _gz(text: str) -> bytes:
    return gzip.compress(text.encode(), mtime=0)


def _edit(payload: bytes, symbol: str, edit: Callable[[int, dict[str, str]], None]) -> bytes:
    """`payload` with `edit(n, row)` applied to the n-th row of `symbol`."""
    lines = _csv(payload).splitlines()
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
    return _gz("\n".join(out) + "\n")


def _without(payload: bytes, *symbols: str) -> bytes:
    lines = _csv(payload).splitlines()
    column = lines[0].split(",").index("TckrSymb")
    keep = [lines[0]] + [line for line in lines[1:] if line.split(",")[column] not in symbols]
    return _gz("\n".join(keep) + "\n")


def _limits(payload: bytes) -> pd.DataFrame:
    return nse_fo_contract.parse_freeze_limits(payload).set_index("symbol")


@pytest.fixture
def spec(tmp_path: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.FNO_FREEZE_LIMITS, base_dir=tmp_path / "fno")


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fetch.time, "sleep", lambda *_a, **_k: None)


def _build(
    spec: datasets.DatasetSpec,
    payload: bytes | None = None,
    *,
    file_day: date = FILE_DAY,
    target: date = TARGET,
    min_rows: int = 10,
    trading: bool = True,
):
    body = _fixture() if payload is None else payload
    return builders.build_fno_freeze_limits(
        spec,
        target,
        fetch_limits=lambda _d: (file_day, nse_fo_contract.parse_freeze_limits(body)),
        trading_day=lambda _d: trading,
        min_rows=min_rows,
    )


def _out(spec: datasets.DatasetSpec) -> pd.DataFrame:
    return pd.read_parquet(spec.base_dir / "fno_freeze_limits_all.parquet")


# ── parser ──────────────────────────────────────────────────────────────────


def test_each_underlying_carries_its_lot_and_the_largest_order_the_exchange_takes():
    limits = _limits(_fixture()).drop(index="011NSETEST")
    assert sorted(limits.index) == sorted(INDEX_SYMBOLS + STOCK_SYMBOLS)
    assert set(limits["basis"]) == {"exchange"}
    # NSE's MaxTradQty is the first quantity that freezes: NIFTY 3,511 means
    # an order of at most 3,510 units, 54 lots of 65.
    columns = ["kind", "lot_size", "max_order_quantity"]
    assert tuple(limits.loc["NIFTY", columns]) == ("index", 65, 3510)
    assert tuple(limits.loc["RELIANCE", columns]) == ("stock", 500, 20000)
    assert limits.loc["NIFTYFPI", "kind"] == "index"
    assert limits.loc["M&M", "max_order_quantity"] == 8000
    # Every limit is a whole number of lots.
    assert ((limits["max_order_quantity"] % limits["lot_size"]) == 0).all()


def test_an_underlying_with_no_option_series_is_flagged_for_the_builder():
    # NSE's test series list futures only; so would a real underlying that
    # lost its options. The builder publishes only those it published before.
    limits = _limits(_fixture())
    assert not limits.loc["011NSETEST", "has_options"]
    assert limits.drop(index="011NSETEST")["has_options"].all()


def test_an_underlying_with_no_tradable_option_series_stands_in_at_one_lot():
    def untradable(_n: int, row: dict[str, str]) -> None:
        row["PrtdToTrad"] = "0"

    limits = _limits(_edit(_fixture(), "NIFTY", untradable))
    assert tuple(limits.loc["NIFTY", ["lot_size", "max_order_quantity", "basis"]]) == (
        65,
        65,
        "stand_in",
    )


def test_the_first_frozen_quantity_is_one_past_the_largest_order():
    # MaxTradQty exactly 54 lots: the largest order is one unit less, so 53 lots.
    def frozen_at_3510(_n: int, row: dict[str, str]) -> None:
        row["MaxTradQty"] = "3510"

    limits = _limits(_edit(_fixture(), "NIFTY", frozen_at_3510))
    assert limits.loc["NIFTY", "max_order_quantity"] == 3445


def test_an_underlying_listed_with_two_limits_keeps_the_smaller_wherever_it_sits():
    for position in (0, 3):  # the first row and the last NIFTY row

        def lower(n: int, row: dict[str, str], at: int = position) -> None:
            if n == at:
                row["MaxTradQty"] = "1.756000E+03"  # 27 lots of 65, below the other rows' 54

        assert _limits(_edit(_fixture(), "NIFTY", lower)).loc["NIFTY", "max_order_quantity"] == 1755


def test_two_lot_sizes_round_the_limit_down_to_the_larger():
    # A lot revision: one series still on 75 while the rest moved to 65.
    def old_lot(n: int, row: dict[str, str]) -> None:
        if n == 0:
            row["MinLot"] = "75"

    limits = _limits(_edit(_fixture(), "NIFTY", old_lot))
    assert limits.loc["NIFTY", "lot_size"] == 75
    assert limits.loc["NIFTY", "max_order_quantity"] == 3450  # 46 lots of 75 <= 3,510


def test_a_limit_that_is_not_a_whole_number_of_lots_rounds_down_to_one():
    def odd(_n: int, row: dict[str, str]) -> None:
        row["MaxTradQty"] = "9100"

    # 9,099 units is 40.44 lots of 225: 40 lots, 9,000 units.
    assert _limits(_edit(_fixture(), "TCS", odd)).loc["TCS", "max_order_quantity"] == 9000


@pytest.mark.parametrize("bad", ["", "NaN", "abc", "0", "-5"])
def test_an_underlying_with_an_unreadable_limit_stands_in_at_one_lot(bad: str):
    # The unreadable row may carry the lower limit: publishing the others'
    # would be a guess that can only be too high, so one lot stands in.
    def unreadable(n: int, row: dict[str, str]) -> None:
        if n == 1:
            row["MaxTradQty"] = bad

    limits = _limits(_edit(_fixture(), "SBIN", unreadable))
    assert tuple(limits.loc["SBIN", ["lot_size", "max_order_quantity", "basis"]]) == (
        750,
        750,
        "stand_in",
    )
    assert limits.loc["RELIANCE", "basis"] == "exchange"


@pytest.mark.parametrize(
    "payload",
    [
        b"%PDF-1.7 not a contract file",
        _gz("<html><body>Access Denied</body></html>"),
        _gz("FinInstrmId,TckrSymb,FinInstrmNm,MinLot,PrtdToTrad\n1,NIFTY,OPTIDX,65,1\n"),
        _gz("FinInstrmId,TckrSymb,FinInstrmNm,MinLot,MaxTradQty\n1,NIFTY,OPTIDX,65,3511\n"),
        _gz(""),
    ],
)
def test_anything_but_the_contract_file_is_refused(payload: bytes):
    with pytest.raises(nse_fo_contract.MalformedContractFile):
        nse_fo_contract.parse_freeze_limits(payload)


# ── builder ─────────────────────────────────────────────────────────────────


def test_a_good_run_publishes_one_row_per_underlying_dated_by_its_file(spec: datasets.DatasetSpec):
    status = _build(spec, target=date(2026, 10, 2), file_day=FILE_DAY)
    assert status.status == "success", status.message
    out = _out(spec)
    assert list(out.columns) == [
        "symbol",
        "kind",
        "lot_size",
        "max_order_quantity",
        "basis",
        "date",
        "source",
    ]
    assert len(out) == 12 and out["symbol"].is_unique
    assert "011NSETEST" not in set(out["symbol"])
    # The file's own day, not the run's.
    assert (out["date"] == pd.Timestamp(FILE_DAY)).all()
    assert set(out["source"]) == {nse_fo_contract.SOURCE}


def test_the_published_file_is_typed_for_the_client(spec: datasets.DatasetSpec):
    _build(spec)
    schema = pq.read_schema(spec.base_dir / "fno_freeze_limits_all.parquet")
    assert schema.field("symbol").type == pa.string()
    assert schema.field("kind").type == pa.string()
    assert schema.field("source").type == pa.string()
    assert schema.field("lot_size").type == pa.int64()
    assert schema.field("max_order_quantity").type == pa.int64()
    assert schema.field("basis").type == pa.string()
    assert schema.field("date").type == pa.timestamp("ms")


def test_a_holiday_does_not_restamp_the_list(spec: datasets.DatasetSpec):
    assert _build(spec, trading=False).status == "skipped_holiday"
    assert not (spec.base_dir / "fno_freeze_limits_all.parquet").exists()


def test_an_older_file_never_replaces_a_newer_one(spec: datasets.DatasetSpec):
    assert _build(spec, file_day=date(2026, 10, 1)).status == "success"

    def higher(_n: int, row: dict[str, str]) -> None:
        row["MaxTradQty"] = "7.021000E+03"

    # A back-dated run (`daily --date 2026-09-15`) reads that day's archived
    # file, whose limits may be higher than today's.
    back_dated = date(2026, 9, 15)
    older = _build(spec, _edit(_fixture(), "NIFTY", higher), file_day=back_dated, target=back_dated)
    assert older.status == "skipped_idempotent", older.message
    assert _out(spec).set_index("symbol").loc["NIFTY", "max_order_quantity"] == 3510


def test_the_same_day_s_file_posted_again_is_read_again(spec: datasets.DatasetSpec):
    # NSE re-posts a day's file in the evening (30 Sep's was modified at
    # 19:57 IST): the correction is published.
    assert _build(spec, file_day=date(2026, 10, 1)).status == "success"

    def corrected(_n: int, row: dict[str, str]) -> None:
        row["MaxTradQty"] = "1.801000E+03"

    status = _build(spec, _edit(_fixture(), "NIFTY", corrected), file_day=date(2026, 10, 1))
    assert status.status == "success", status.message
    assert _out(spec).set_index("symbol").loc["NIFTY", "max_order_quantity"] == 1755


def test_a_newer_file_publishes_its_changed_limits(spec: datasets.DatasetSpec):
    assert _build(spec, file_day=date(2026, 9, 30)).status == "success"

    def lowered(_n: int, row: dict[str, str]) -> None:
        row["MaxTradQty"] = "1.801000E+03"

    newer = _build(spec, _edit(_fixture(), "NIFTY", lowered), file_day=date(2026, 10, 1))
    assert newer.status == "success"
    assert _out(spec).set_index("symbol").loc["NIFTY", "max_order_quantity"] == 1755


def test_a_failed_fetch_keeps_the_list_we_had(spec: datasets.DatasetSpec):
    assert _build(spec).status == "success"
    status = _build(spec, payload=b"%PDF-1.7 a circular", file_day=date(2026, 10, 5))
    assert status.status == "skipped_idempotent", status.message
    assert len(_out(spec)) == 12


def test_no_file_for_days_is_loud_even_with_a_list_to_keep(spec: datasets.DatasetSpec):
    assert _build(spec).status == "success"

    def nothing(_d: date) -> tuple[date, pd.DataFrame]:
        raise NotYetPublished("no contract file in the 5 days to 2026-10-09")

    status = builders.build_fno_freeze_limits(
        spec, date(2026, 10, 9), fetch_limits=nothing, trading_day=lambda _d: True, min_rows=10
    )
    assert status.status == "failed", status.message
    assert len(_out(spec)) == 12


def test_a_first_run_with_nothing_to_keep_fails(spec: datasets.DatasetSpec):
    assert _build(spec, payload=b"%PDF-1.7 a circular").status == "failed"


def test_a_truncated_file_is_not_published(spec: datasets.DatasetSpec):
    status = _build(spec, payload=_without(_fixture(), *STOCK_SYMBOLS), min_rows=10)
    assert status.status == "failed" and "floor" in status.message


def test_an_underlying_that_left_is_carried_forward_and_the_rest_still_publish(
    spec: datasets.DatasetSpec,
):
    assert _build(spec, file_day=date(2026, 9, 30)).status == "success"

    def lowered(_n: int, row: dict[str, str]) -> None:
        row["MaxTradQty"] = "1.801000E+03"

    # SBIN leaves F&O at the same rollover that lowers NIFTY's limit.
    payload = _edit(_without(_fixture(), "SBIN"), "NIFTY", lowered)
    status = _build(spec, payload, file_day=date(2026, 10, 1), min_rows=5)
    assert status.status == "success", status.message
    assert "SBIN" in status.message
    out = _out(spec).set_index("symbol")
    assert out.loc["NIFTY", "max_order_quantity"] == 1755  # the lower limit is not withheld
    assert out.loc["SBIN", "max_order_quantity"] == 22500  # carried, so the row count never drops
    assert len(out) == 12


def test_an_underlying_still_listed_never_gets_its_old_limit_back(spec: datasets.DatasetSpec):
    assert _build(spec, file_day=date(2026, 9, 30)).status == "success"

    def lowered_with_a_blank(n: int, row: dict[str, str]) -> None:
        row["MaxTradQty"] = "" if n == 0 else "1.801000E+03"

    status = _build(
        spec, _edit(_fixture(), "NIFTY", lowered_with_a_blank), file_day=date(2026, 10, 1)
    )
    assert status.status == "success", status.message
    assert "NIFTY" in status.message
    row = _out(spec).set_index("symbol").loc["NIFTY"]
    assert (row["max_order_quantity"], row["basis"]) == (65, "stand_in")


def test_a_real_underlying_that_lost_its_options_stays_published(spec: datasets.DatasetSpec):
    assert _build(spec, file_day=date(2026, 9, 30)).status == "success"
    lines = _csv(_fixture()).splitlines()
    header = lines[0].split(",")
    symbol, kind = header.index("TckrSymb"), header.index("FinInstrmNm")
    futures_only = [lines[0]] + [
        line
        for line in lines[1:]
        if not (line.split(",")[symbol] == "RELIANCE" and line.split(",")[kind] == "OPTSTK")
    ]
    status = _build(spec, _gz("\n".join(futures_only) + "\n"), file_day=date(2026, 10, 1))
    assert status.status == "success", status.message
    out = _out(spec).set_index("symbol")
    assert out.loc["RELIANCE", "max_order_quantity"] == 20000
    assert "011NSETEST" not in out.index


def test_many_stand_ins_mean_the_format_changed_and_fail(spec: datasets.DatasetSpec):
    assert _build(spec, file_day=date(2026, 9, 30)).status == "success"
    payload = _fixture()
    for symbol in ["NIFTY", "BANKNIFTY", "RELIANCE", "TCS"]:

        def flagged_differently(_n: int, row: dict[str, str]) -> None:
            row["PrtdToTrad"] = "Y"

        payload = _edit(payload, symbol, flagged_differently)
    status = _build(spec, payload, file_day=date(2026, 10, 1))
    assert status.status == "failed", status.message
    assert _out(spec).set_index("symbol").loc["NIFTY", "max_order_quantity"] == 3510


def test_many_underlyings_missing_is_a_partial_file_not_an_exit(spec: datasets.DatasetSpec):
    assert _build(spec, file_day=date(2026, 9, 30)).status == "success"
    status = _build(
        spec,
        _without(_fixture(), "SBIN", "TCS", "HDFCBANK", "M&M"),
        file_day=date(2026, 10, 1),
        min_rows=5,
    )
    assert status.status == "failed", status.message
    assert len(_out(spec)) == 12


def test_limits_a_trading_day_behind_are_loud_whatever_the_cause(
    spec: datasets.DatasetSpec,
):
    # File D (posted the evening of D) serves session D+1, so a run on D+1
    # before that evening holds file D: current.
    assert _build(spec, file_day=date(2026, 9, 30)).status == "success"  # a Wednesday
    quiet = _build(
        spec, b"%PDF-1.7 a circular", file_day=date(2026, 10, 1), target=date(2026, 10, 1)
    )
    assert quiet.status == "skipped_idempotent", quiet.message
    # Friday: Thursday's file never came, so Friday trades on Wednesday's: loud.
    loud = _build(
        spec, b"%PDF-1.7 a circular", file_day=date(2026, 10, 2), target=date(2026, 10, 2)
    )
    assert loud.status == "failed", loud.message
    # The same when the newest file is only the old one again.
    stale = _build(spec, file_day=date(2026, 9, 30), target=date(2026, 10, 2))
    assert stale.status == "failed", stale.message
    assert len(_out(spec)) == 12


def test_holidays_do_not_count_toward_staleness(spec: datasets.DatasetSpec):
    assert _build(spec, file_day=date(2026, 10, 7)).status == "success"  # a Wednesday
    holidays = {date(2026, 10, 8), date(2026, 10, 9)}  # Thursday and Friday
    status = builders.build_fno_freeze_limits(
        spec,
        date(2026, 10, 12),  # Monday, before its file is posted
        fetch_limits=lambda _d: (
            date(2026, 10, 7),
            nse_fo_contract.parse_freeze_limits(_fixture()),
        ),
        trading_day=lambda d: d.weekday() < 5 and d not in holidays,
        min_rows=10,
    )
    assert status.status != "failed", status.message


# ── fetch ───────────────────────────────────────────────────────────────────


def _warm() -> None:
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)


@responses.activate
def test_a_file_not_yet_posted_is_read_from_the_last_day_that_has_one():
    _warm()
    responses.add(responses.GET, nse_fo_contract.contract_url(date(2026, 10, 1)), status=404)
    responses.add(responses.GET, nse_fo_contract.contract_url(date(2026, 9, 30)), status=404)
    responses.add(
        responses.GET, nse_fo_contract.contract_url(date(2026, 9, 29)), body=_fixture(), status=200
    )
    day, limits = builders._fetch_freeze_limits(date(2026, 10, 1))
    assert day == date(2026, 9, 29)
    assert len(limits) == 13  # the twelve and NSE's test series, flagged for the builder


@responses.activate
@pytest.mark.parametrize(
    ("status", "body"),
    [(403, b"forbidden"), (500, b"oops"), (200, b"<html>maintenance</html>")],
)
def test_a_refused_or_wrong_answer_never_falls_back_to_an_older_file(status: int, body: bytes):
    _warm()
    responses.add(
        responses.GET, nse_fo_contract.contract_url(date(2026, 10, 1)), status=status, body=body
    )
    responses.add(
        responses.GET, nse_fo_contract.contract_url(date(2026, 9, 30)), body=_fixture(), status=200
    )
    with pytest.raises(Exception) as caught:
        builders._fetch_freeze_limits(date(2026, 10, 1))
    assert not isinstance(caught.value, NotYetPublished)


@responses.activate
def test_no_file_in_the_window_says_so():
    _warm()
    from pipeline import config

    for back in range(config.FNO_CONTRACT_LOOKBACK_DAYS):
        responses.add(
            responses.GET,
            nse_fo_contract.contract_url(date.fromordinal(date(2026, 10, 9).toordinal() - back)),
            status=404,
        )
    with pytest.raises(NotYetPublished):
        builders._fetch_freeze_limits(date(2026, 10, 9))


def test_the_contract_file_is_named_for_its_day():
    assert nse_fo_contract.contract_url(date(2026, 10, 1)) == (
        "https://nsearchives.nseindia.com/content/fo/NSE_FO_contract_01102026.csv.gz"
    )


# ── registration ────────────────────────────────────────────────────────────


def test_fno_freeze_limits_spec_fields():
    s = datasets.FNO_FREEZE_LIMITS
    assert (s.key, s.file_prefix, s.manifest_name, s.schema_version) == (
        "fno_freeze_limits",
        "fno_freeze_limits",
        "fno_freeze_limits",
        1,
    )
    assert s.source_label == nse_fo_contract.SOURCE
    assert s.derived is True and s.external is False
    assert datasets.DATASETS["fno_freeze_limits"] is s
    assert datasets.by_manifest_name("fno_freeze_limits") is s
    assert s in datasets.all_specs()
    with pytest.raises(RuntimeError, match="derived dataset has no fetcher"):
        s.make_fetcher()


def test_fno_freeze_limits_is_built_by_the_daily_loop():
    from pipeline import cli

    assert cli.builders.BUILDERS["fno_freeze_limits"] is cli.builders.build_fno_freeze_limits
    assert "fno_freeze_limits" in datasets.DATASET_ORDER


def test_the_manifest_lists_the_published_file(spec: datasets.DatasetSpec):
    from pipeline import manifest, publish

    _build(spec)
    m = manifest.build_manifest([spec], latest_trading_date=TARGET, generated_at="t")
    (entry,) = m["datasets"]
    assert entry["name"] == "fno_freeze_limits"
    assert entry["latest_date"] == FILE_DAY.isoformat()
    (f,) = entry["baseline"]
    assert f["name"] == "fno_freeze_limits_all.parquet"
    assert f["rows"] == 12
    prefixes = frozenset(s.file_prefix for s in datasets.all_specs())
    assert publish.owns_asset(f["asset"], file_prefixes=prefixes)


def test_a_limit_below_one_lot_stands_in_rather_than_publishing_zero():
    def below_a_lot(_n: int, row: dict[str, str]) -> None:
        row["MaxTradQty"] = "50"  # NIFTY's lot is 65

    limits = _limits(_edit(_fixture(), "NIFTY", below_a_lot))
    # Never more than the file shows: 49 units, which the app reads as one lot.
    assert tuple(limits.loc["NIFTY", ["max_order_quantity", "basis"]]) == (49, "stand_in")


def test_a_prior_file_without_dates_reads_as_undated(spec: datasets.DatasetSpec):
    assert _build(spec).status == "success"
    path = spec.base_dir / "fno_freeze_limits_all.parquet"
    undated = pd.read_parquet(path)
    undated["date"] = pd.NaT
    undated.to_parquet(path, index=False)

    def nothing(_d: date) -> tuple[date, pd.DataFrame]:
        raise NotYetPublished("no contract file in the 7 days to 2026-10-09")

    status = builders.build_fno_freeze_limits(
        spec, date(2026, 10, 9), fetch_limits=nothing, trading_day=lambda _d: True, min_rows=10
    )
    assert status.status == "failed"
    assert "NaT" not in status.message


def _recoded(payload: bytes, symbol: str, column: str, value: str) -> bytes:
    def set_all(_n: int, row: dict[str, str]) -> None:
        row[column] = value

    return _edit(payload, symbol, set_all)


@pytest.mark.parametrize(
    ("column", "value"),
    [("MinLot", ""), ("FinInstrmNm", "IDXOPT")],
)
def test_a_listed_underlying_the_parser_cannot_read_is_never_carried(
    spec: datasets.DatasetSpec, column: str, value: str
):
    assert _build(spec, file_day=date(2026, 9, 30)).status == "success"
    payload = _recoded(
        _recoded(_fixture(), "NIFTY", "MaxTradQty", "1.801000E+03"), "NIFTY", column, value
    )
    status = _build(spec, payload, file_day=date(2026, 10, 1))
    assert status.status == "success", status.message
    row = _out(spec).set_index("symbol").loc["NIFTY"]
    assert row["basis"] == "stand_in"
    assert row["max_order_quantity"] <= 1800, row.to_dict()


def test_a_stand_in_never_exceeds_the_exchange_limit():
    # One bogus row makes the largest lot 3,600, above the 3,510 limit.
    def bogus_lot(n: int, row: dict[str, str]) -> None:
        if n == 0:
            row["MinLot"] = "3600"

    limits = _limits(_edit(_fixture(), "NIFTY", bogus_lot))
    assert limits.loc["NIFTY", "basis"] == "stand_in"
    assert limits.loc["NIFTY", "max_order_quantity"] <= 3510


def test_underlyings_carried_before_do_not_count_against_new_ones(spec: datasets.DatasetSpec):
    # One F&O exit a day, past the churn allowance in total: each day still publishes.
    start = date(2026, 9, 24)
    assert _build(spec, file_day=start, target=start).status == "success"
    leavers = ["SBIN", "TCS", "HDFCBANK", "M&M", "BAJAJ-AUTO"]
    days = [date(2026, 9, 25 + n) for n in range(5)]
    for n, day in enumerate(days):
        status = _build(
            spec, _without(_fixture(), *leavers[: n + 1]), file_day=day, target=day, min_rows=5
        )
        assert status.status == "success", (day, status.message)
    out = _out(spec)
    assert len(out) == 12
    assert set(leavers) <= set(out["symbol"])
