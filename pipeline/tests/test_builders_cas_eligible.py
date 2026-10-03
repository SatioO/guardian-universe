"""cas_eligible: the market-lot parser's gate, the stock/index decision, the
ISIN join, and the builder's fail-closed policy.

The failure this dataset must never produce is a WRONG LIST that looks right:
an error page read as "nothing is CAS-eligible", an index published as a
stock, or a list stripped of the ISINs the client joins on. Every guard here
turns "we could not get a good list" into "keep the one we had".
"""

from __future__ import annotations

import dataclasses
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
import responses

from pipeline import builders, datasets, fetch, manifest, publish
from pipeline.sources import nse_fo_mktlots

FIXTURE = Path(__file__).parent / "fixtures" / "fo_mktlots_sample.csv"

# The trimmed real file (fetched 2026-10-03): every index underlying NSE lists,
# then 15 of its 213 stocks, spellings and padding untouched.
INDEX_SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTYFPI"]
STOCK_SYMBOLS = [
    "ADANIENSOL", "ADANIGREEN", "ADANIPORTS", "ALKEM", "360ONE", "BAJAJ-AUTO", "M&M",
    "RELIANCE", "NAM-INDIA", "SBIN", "TCS", "GVT&D", "HDFCBANK", "INFY", "UJJIVANSFB",
]

TARGET = date(2026, 10, 1)  # a Thursday, not an NSE holiday
NEXT = date(2026, 10, 5)


def _fixture() -> bytes:
    return FIXTURE.read_bytes()


def _without(payload: bytes, *symbols: str) -> bytes:
    """The payload minus the rows for `symbols` (matched on the padded cell)."""
    keep = [
        line for line in payload.splitlines(keepends=True)
        if not any(f",{s.ljust(10)}," in line.decode() for s in symbols)
    ]
    assert len(keep) == len(payload.splitlines()) - len(symbols), symbols
    return b"".join(keep)


def _isin(i: int) -> str:
    return f"INE{i:03d}X01010"


def _master_rows() -> list[tuple[str, str, str, str]]:
    """(symbol, series, isin, last_seen) for every fixture stock, plus traps."""
    rows = [(s, "EQ", _isin(i), "2026-09-30") for i, s in enumerate(STOCK_SYMBOLS)]
    rows = [r for r in rows if r[0] not in {"RELIANCE", "HDFCBANK", "360ONE"}]
    rows += [
        ("RELIANCE", "EQ", "INE002A01018", "2026-09-30"),
        ("HDFCBANK", "EQ", "INE040A01034", "2026-09-30"),
        # A bond under the same symbol with its OWN ISIN, seen more recently:
        # a series-blind "latest row wins" would hand HDFCBANK this ISIN.
        ("HDFCBANK", "N1", "INE040A13013", "2026-10-01"),
        # A face-value split re-issued the ISIN: the latest EQ row is current.
        ("360ONE", "EQ", "INE466L01020", "2024-01-10"),
        ("360ONE", "EQ", "INE466L01038", "2026-09-30"),
    ]
    return rows


def _write_master(base: Path, rows: list[tuple[str, str, str, str]]) -> datasets.DatasetSpec:
    spec = dataclasses.replace(datasets.REFERENCE, base_dir=base)
    base.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=["symbol", "series", "isin", "last_seen"])
    df["last_seen"] = pd.to_datetime(df["last_seen"])
    df["instrument_key"] = df["isin"]
    df["status"] = "active"
    df.to_parquet(base / f"{spec.file_prefix}_all.parquet", index=False)
    return spec


@pytest.fixture
def spec(tmp_path: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.CAS_ELIGIBLE, base_dir=tmp_path / "cas")


@pytest.fixture
def master(tmp_path: Path) -> datasets.DatasetSpec:
    return _write_master(tmp_path / "reference", _master_rows())


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fetch.time, "sleep", lambda *_a, **_k: None)


def _build(
    spec: datasets.DatasetSpec,
    master: datasets.DatasetSpec,
    target: date = TARGET,
    payload: bytes | None = None,
    min_rows: int = 10,
):
    body = _fixture() if payload is None else payload
    return builders.build_cas_eligible(
        spec,
        target,
        universe_spec=master,
        fetch_underlyings=lambda _d: nse_fo_mktlots.parse_mktlots_csv(body),
        trading_day=lambda _d: True,
        min_rows=min_rows,
    )


def _out(spec: datasets.DatasetSpec) -> pd.DataFrame:
    return pd.read_parquet(spec.base_dir / "cas_eligible_all.parquet")


# ── parser ──────────────────────────────────────────────────────────────────


def test_parses_the_real_file_into_index_and_stock_blocks():
    df = nse_fo_mktlots.parse_mktlots_csv(_fixture())
    assert list(df.columns) == nse_fo_mktlots.UNDERLYING_COLUMNS
    by_section = df.groupby("section")["symbol"].apply(list).to_dict()
    assert by_section["index"] == INDEX_SYMBOLS
    assert by_section["stock"] == STOCK_SYMBOLS
    # The fixed-width padding is gone; NSE's punctuation survives.
    assert {"M&M", "BAJAJ-AUTO", "GVT&D", "360ONE"} <= set(df["symbol"])
    assert df.loc[df["symbol"] == "RELIANCE", "underlying"].item() == "RELIANCE INDUSTRIES LTD"


def test_symbols_are_trimmed_and_uppercased():
    payload = (
        b"UNDERLYING,SYMBOL,OCT-26,NOV-26\n"
        b"Derivatives on Individual Securities,Symbol,OCT-26,NOV-26\n"
        b"Reliance Industries Ltd ,  reliance  ,500 ,500\n"
        b"Mahindra & Mahindra,m&m\t,200,\n"
    )
    df = nse_fo_mktlots.parse_mktlots_csv(payload)
    assert df["symbol"].tolist() == ["RELIANCE", "M&M"]
    assert set(df["section"]) == {"stock"}


@pytest.mark.parametrize(
    "payload",
    [
        b"<!DOCTYPE html><html><body>Service unavailable</body></html>",
        # What the legacy archives host serves with HTTP 200 for this path.
        b"%PDF-1.7\n%\xe2\xe3\xcf\xd3\n1 0 obj\n<< /Type /Catalog >>\nendobj\n",
        b"",
        b"SYMBOL,UNDERLYING,OCT-26\n",           # columns swapped
        b"UNDERLYING,SYMBOL,LOT\nNIFTY 50,NIFTY,65\n",  # no expiry month
    ],
)
def test_anything_but_the_market_lot_file_is_rejected(payload: bytes):
    with pytest.raises(nse_fo_mktlots.MalformedMktLotsCsv):
        nse_fo_mktlots.parse_mktlots_csv(payload)


def test_tolerates_a_bom_and_crlf():
    payload = b"\xef\xbb\xbf" + _fixture().replace(b"\n", b"\r\n")
    df = nse_fo_mktlots.parse_mktlots_csv(payload)
    assert df.loc[df["section"] == "stock", "symbol"].tolist() == STOCK_SYMBOLS


def test_an_underlying_with_no_live_contract_is_dropped_and_dupes_keep_the_first():
    payload = (
        b"UNDERLYING,SYMBOL,OCT-26,NOV-26\n"
        b"Derivatives on Individual Securities,Symbol,OCT-26,NOV-26\n"
        b"Gone Ltd,GONE,,\n"
        b"Exiting Ltd,EXITING,300,\n"  # last month of contracts: still live
        b"Twice Ltd,TWICE,100,100\n"
        b"Twice Again,TWICE,999,999\n"
        b"UNDERLYING,SYMBOL,OCT-26,NOV-26\n"  # a repeated header row
    )
    df = nse_fo_mktlots.parse_mktlots_csv(payload)
    assert df["symbol"].tolist() == ["EXITING", "TWICE"]
    assert df.loc[df["symbol"] == "TWICE", "underlying"].item() == "Twice Ltd"


def _no_marker() -> bytes:
    return b"".join(
        line for line in _fixture().splitlines(keepends=True)
        if not line.startswith(b"Derivatives on Individual Securities")
    )


def test_without_the_marker_every_row_is_an_index_row():
    df = nse_fo_mktlots.parse_mktlots_csv(_no_marker())
    assert set(df["section"]) == {"index"}
    assert len(df) == len(INDEX_SYMBOLS) + len(STOCK_SYMBOLS)


# ── stock decision + ISIN join ──────────────────────────────────────────────


def test_a_healthy_run_publishes_stocks_only_with_their_isins(spec, master):
    st = _build(spec, master)
    assert st.status == "success", st.message
    assert st.symbol_count == len(STOCK_SYMBOLS)
    assert "6 index underlyings dropped" in st.message

    out = _out(spec)
    assert list(out.columns) == ["symbol", "isin", "date", "source"]
    assert sorted(out["symbol"]) == sorted(STOCK_SYMBOLS)
    assert not set(INDEX_SYMBOLS) & set(out["symbol"])
    assert out["symbol"].is_monotonic_increasing
    assert (out["date"] == pd.Timestamp(TARGET)).all()
    assert (out["source"] == "nse-fo-mktlots").all()
    isin = dict(zip(out["symbol"], out["isin"], strict=True))
    assert isin["RELIANCE"] == "INE002A01018"
    assert isin["HDFCBANK"] == "INE040A01034"  # the EQ ISIN, never the bond's
    assert isin["360ONE"] == "INE466L01038"    # the post-split ISIN
    assert out["isin"].notna().all()


def test_the_published_file_is_typed_for_the_client(spec, master):
    _build(spec, master)
    schema = pq.read_schema(spec.base_dir / "cas_eligible_all.parquet")
    assert schema.field("symbol").type == pa.string()
    assert schema.field("isin").type == pa.string()
    assert schema.field("source").type == pa.string()
    # The unit the published file has always carried: a writer change must not move it.
    assert schema.field("date").type == pa.timestamp("ms")


def test_a_be_series_row_resolves_when_there_is_no_eq_row(spec, tmp_path):
    rows = [r for r in _master_rows() if r[0] != "TCS"]
    rows.append(("TCS", "BE", "INE467B01029", "2026-09-30"))
    st = _build(spec, _write_master(tmp_path / "ref2", rows))
    assert st.status == "success"
    assert _out(spec).set_index("symbol").loc["TCS", "isin"] == "INE467B01029"


def test_a_stock_below_the_marker_that_the_master_lacks_keeps_a_null_isin(spec, tmp_path):
    # A symbol renamed today is not in the master yet; NSE's own file still
    # says it has contracts, so it stays — with its ISIN honestly unknown.
    rows = [r for r in _master_rows() if r[0] != "UJJIVANSFB"]
    st = _build(spec, _write_master(tmp_path / "ref2", rows))
    assert st.status == "success"
    assert "1 without ISIN: UJJIVANSFB" in st.message
    out = _out(spec).set_index("symbol")
    assert pd.isna(out.loc["UJJIVANSFB", "isin"])
    assert out["isin"].notna().sum() == len(STOCK_SYMBOLS) - 1
    schema = pq.read_schema(spec.base_dir / "cas_eligible_all.parquet")
    assert schema.field("isin").type == pa.string()


def test_without_the_marker_the_equity_universe_still_separates_stocks(spec, master):
    # NSE renames or drops its marker row: the universe match carries the
    # decision, and the index underlyings — in no equity series — still drop.
    st = _build(spec, master, payload=_no_marker())
    assert st.status == "success", st.message
    assert sorted(_out(spec)["symbol"]) == sorted(STOCK_SYMBOLS)


# ── fail-closed ─────────────────────────────────────────────────────────────


def test_a_failed_fetch_keeps_the_prior_file(spec, master):
    _build(spec, master)

    def _boom(_d: date) -> pd.DataFrame:
        raise RuntimeError("host unreachable")

    st = builders.build_cas_eligible(
        spec, NEXT, universe_spec=master, fetch_underlyings=_boom,
        trading_day=lambda _d: True, min_rows=10,
    )
    assert st.status == "skipped_idempotent"
    assert "fetch failed" in st.message and "retained prior" in st.message
    out = _out(spec)
    assert len(out) == len(STOCK_SYMBOLS)
    assert (out["date"] == pd.Timestamp(TARGET)).all()  # the as-of did not move


def test_with_no_prior_file_a_failure_is_loud(spec, master):
    def _boom(_d: date) -> pd.DataFrame:
        raise RuntimeError("host unreachable")

    st = builders.build_cas_eligible(
        spec, TARGET, universe_spec=master, fetch_underlyings=_boom,
        trading_day=lambda _d: True,
    )
    assert st.status == "failed"
    assert not (spec.base_dir / "cas_eligible_all.parquet").exists()


@responses.activate
def test_the_default_fetch_reads_the_real_file_shape(spec, master):
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET, nse_fo_mktlots.MKTLOTS_URL, body=_fixture(), status=200,
        content_type="text/csv",
    )
    st = builders.build_cas_eligible(
        spec, TARGET, universe_spec=master, trading_day=lambda _d: True, min_rows=10
    )
    assert st.status == "success", st.message
    assert len(_out(spec)) == len(STOCK_SYMBOLS)


@responses.activate
def test_an_html_page_served_with_200_keeps_the_prior_file(spec, master):
    _build(spec, master)
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET, nse_fo_mktlots.MKTLOTS_URL,
        body=b"<!DOCTYPE html><html><body>Access Denied</body></html>", status=200,
        content_type="text/html",
    )
    st = builders.build_cas_eligible(
        spec, NEXT, universe_spec=master, trading_day=lambda _d: True, min_rows=10
    )
    assert st.status == "skipped_idempotent"
    assert "unexpected header" in st.message and "retained prior" in st.message
    assert (_out(spec)["date"] == pd.Timestamp(TARGET)).all()


@responses.activate
def test_a_network_error_on_the_default_fetch_keeps_the_prior_file(spec, master):
    _build(spec, master)
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET, nse_fo_mktlots.MKTLOTS_URL,
        body=requests.ConnectionError("connection reset"),
    )
    st = builders.build_cas_eligible(
        spec, NEXT, universe_spec=master, trading_day=lambda _d: True, min_rows=10
    )
    assert st.status == "skipped_idempotent"
    assert len(_out(spec)) == len(STOCK_SYMBOLS)


def test_too_few_stocks_keeps_the_prior_file_and_fails_without_one(spec, master):
    # 15 stocks against a floor of 20: a truncated response, not a real list.
    first = _build(spec, master, min_rows=20)
    assert first.status == "failed"
    assert "floor 20" in first.message
    assert not (spec.base_dir / "cas_eligible_all.parquet").exists()

    _build(spec, master)  # a good list lands
    st = _build(spec, master, target=NEXT, min_rows=20)
    assert st.status == "skipped_idempotent"
    assert "floor 20" in st.message and "retained prior" in st.message
    assert (_out(spec)["date"] == pd.Timestamp(TARGET)).all()


def test_a_missing_symbol_master_never_publishes_a_list_without_isins(spec, master, tmp_path):
    _build(spec, master)
    empty = dataclasses.replace(datasets.REFERENCE, base_dir=tmp_path / "no-reference")
    st = builders.build_cas_eligible(
        spec, NEXT, universe_spec=empty,
        fetch_underlyings=lambda _d: nse_fo_mktlots.parse_mktlots_csv(_fixture()),
        trading_day=lambda _d: True, min_rows=10,
    )
    assert st.status == "skipped_idempotent"
    assert "resolved an ISIN" in st.message
    assert _out(spec)["isin"].notna().all()


def test_a_shrink_is_held_back_loudly_naming_the_stocks_that_left(spec, master):
    _build(spec, master)
    # Two stocks' last contracts expired. A shorter file would trip the
    # publish shrink-guard and block the WHOLE release, so it is never
    # written — but unlike a transient failure it repeats every run until an
    # operator accepts it, so it must not be silent.
    st = _build(spec, master, target=NEXT, payload=_without(_fixture(), "TCS", "INFY"))
    assert st.status == "failed"
    assert "shrink-guard" in st.message
    assert "INFY, TCS" in st.message
    assert "retained prior file" in st.message
    assert st.symbol_count == len(STOCK_SYMBOLS)
    out = _out(spec)
    assert len(out) == len(STOCK_SYMBOLS)
    assert (out["date"] == pd.Timestamp(TARGET)).all()


def test_a_same_size_membership_change_is_published(spec, master, tmp_path):
    _build(spec, master)
    swapped = _fixture().replace(b",TCS       ,", b",NEWSTOCK  ,")
    rows = _master_rows() + [("NEWSTOCK", "EQ", "INE999Z01010", "2026-10-02")]
    st = _build(spec, _write_master(tmp_path / "ref2", rows), target=NEXT, payload=swapped)
    assert st.status == "success", st.message
    out = _out(spec)
    assert "NEWSTOCK" in set(out["symbol"]) and "TCS" not in set(out["symbol"])
    assert (out["date"] == pd.Timestamp(NEXT)).all()


def test_a_non_trading_day_does_not_fetch_or_restamp(spec, master):
    _build(spec, master)

    def _must_not_run(_d: date) -> pd.DataFrame:
        raise AssertionError("fetched on a non-trading day")

    # The default calendar check, against the committed holidays file:
    # Saturday, and Gandhi Jayanti (an NSE weekday holiday).
    for day in (date(2026, 10, 3), date(2026, 10, 2)):
        st = builders.build_cas_eligible(
            spec, day, universe_spec=master, fetch_underlyings=_must_not_run, min_rows=10
        )
        assert st.status == "skipped_holiday", day
    assert (_out(spec)["date"] == pd.Timestamp(TARGET)).all()


# ── registration ────────────────────────────────────────────────────────────


def test_cas_eligible_spec_fields():
    s = datasets.CAS_ELIGIBLE
    assert s.key == "cas_eligible"
    assert s.file_prefix == "cas_eligible"
    assert s.manifest_name == "cas_eligible"
    assert s.schema_version == 1
    assert s.source_label == "nse-fo-mktlots"
    # Built by the daily Phase-2 loop: derived (no Phase-1 fetch, no
    # continuity check) but NOT external (it has a builder here).
    assert s.derived is True and s.external is False
    assert datasets.DATASETS["cas_eligible"] is s
    assert datasets.by_manifest_name("cas_eligible") is s  # what sync routes on
    with pytest.raises(RuntimeError, match="derived dataset has no fetcher"):
        s.make_fetcher()


def test_cas_eligible_runs_after_the_symbol_master_it_joins():
    order = datasets.DATASET_ORDER
    assert order.index("cas_eligible") > order.index("reference")
    assert datasets.CAS_ELIGIBLE in datasets.all_specs()


def test_cas_eligible_registered_in_builders_bound_to_the_reference_master():
    import functools

    from pipeline import cli

    bound = cli.builders.BUILDERS["cas_eligible"]
    assert isinstance(bound, functools.partial)
    assert bound.func is cli.builders.build_cas_eligible
    assert bound.keywords == {"universe_spec": datasets.DATASETS["reference"]}


def test_the_manifest_lists_the_published_file(spec, master):
    _build(spec, master)
    m = manifest.build_manifest([spec], latest_trading_date=TARGET, generated_at="t")
    (entry,) = m["datasets"]
    assert entry["name"] == "cas_eligible"
    assert entry["schema_version"] == 1
    assert entry["latest_date"] == TARGET.isoformat()
    (f,) = entry["baseline"]
    assert f["name"] == "cas_eligible_all.parquet"
    assert f["rows"] == len(STOCK_SYMBOLS)
    # The publish GC will only ever delete assets in a namespace this runner owns.
    prefixes = frozenset(s.file_prefix for s in datasets.all_specs())
    assert publish.owns_asset(f["asset"], file_prefixes=prefixes)
