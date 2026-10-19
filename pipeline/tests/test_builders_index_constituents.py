"""index_constituents: the parser's integrity gate and the builder's two-level
fail-closed policy.

The failure this dataset must never produce is an EMPTY BASKET. Downstream, an
index with no members is indistinguishable from a real answer — the desktop
client's drill just shows nothing — so every guard here exists to turn "we
could not fetch it" into "keep what we had", never into "it has none".
"""

from __future__ import annotations

import dataclasses
from datetime import date

import pandas as pd
import pytest

from pipeline import builders, datasets
from pipeline.sources import nse_constituents

HEADER = b"Company Name,Industry,Symbol,Series,ISIN Code\n"


def _csv(n: int = 8) -> bytes:
    rows = b"".join(
        f"Company {i},Financial Services,SYM{i},EQ,INE{i:03d}A01011\n".encode()
        for i in range(n)
    )
    return HEADER + rows


def _parse(payload: bytes):
    return nse_constituents.parse_constituents_csv(
        payload,
        index_key="IDX:NIFTYBANK",
        index_name="Nifty Bank",
        family="sectoral",
        source_file="ind_niftybanklist.csv",
    )


# ── parser ──────────────────────────────────────────────────────────────────

def test_parses_a_published_basket_into_the_normalized_frame():
    df = _parse(_csv(6))
    assert list(df.columns) == nse_constituents.CONSTITUENT_COLUMNS
    assert len(df) == 6
    assert df["index_key"].unique().tolist() == ["IDX:NIFTYBANK"]
    # NSE's family travels WITH the rows: the consumer scopes by it and cannot
    # re-derive it from the name.
    assert df["family"].unique().tolist() == ["sectoral"]
    # The ISIN is the join key and is mirrored into instrument_key.
    assert (df["instrument_key"] == df["isin"]).all()
    assert df["symbol"].iloc[0] == "SYM0"


def test_an_html_error_page_served_with_200_is_rejected_not_read_as_empty():
    # THE failure mode this gate exists for: NSE answers 200 with an error
    # document, and a lenient parser reports "0 members" — which publishes as
    # a real, empty basket.
    with pytest.raises(nse_constituents.MalformedConstituentCsv):
        _parse(b"<!DOCTYPE html><html><body>Service unavailable</body></html>")


def test_a_truncated_basket_is_rejected(monkeypatch):
    with pytest.raises(nse_constituents.MalformedConstituentCsv):
        _parse(_csv(2))


def test_tolerates_a_bom_and_crlf():
    payload = b"\xef\xbb\xbf" + _csv(6).replace(b"\n", b"\r\n")
    assert len(_parse(payload)) == 6


def test_a_quoted_comma_in_a_company_name_does_not_shift_columns():
    row = b'"Bajaj Finserv, Ltd",Financial Services,BAJAJFINSV,EQ,INE918I01026\n'
    payload = HEADER + row + _csv(6)[len(HEADER):]
    df = _parse(payload)
    assert df["company"].iloc[0] == "Bajaj Finserv, Ltd"
    assert df["symbol"].iloc[0] == "BAJAJFINSV"
    assert df["isin"].iloc[0] == "INE918I01026"


def test_no_series_or_isin_prefix_filter_at_the_producer():
    # IN9-prefixed DVR equities are real, and universe policy belongs to the
    # consumer. A producer-side INE-only filter would silently drop them.
    row = b"Jain Irrigation DVR,Capital Goods,JISLDVREQS,BE,IN9175A01010\n"
    payload = HEADER + row + _csv(6)[len(HEADER):]
    df = _parse(payload)
    assert "IN9175A01010" in set(df["isin"])
    assert "BE" in set(df["series"])


def test_duplicate_isins_keep_the_first_occurrence():
    rows = b"A,Fin,SYMA,EQ,INE001A01011\nB,Fin,SYMB,EQ,INE001A01011\n"
    dupe = HEADER + rows + _csv(6)[len(HEADER):]
    df = _parse(dupe)
    assert (df["isin"] == "INE001A01011").sum() == 1


def test_index_name_normalization_ignores_spaces_but_keeps_punctuation():
    n = nse_constituents.normalize_index_name
    assert n("Nifty Oil & Gas") == n("Nifty Oil&Gas") == "NIFTYOIL&GAS"
    # 39 live keys depend on punctuation surviving.
    assert n("Nifty Financial Services 25/50") == "NIFTYFINANCIALSERVICES25/50"


def test_url_construction_handles_the_one_index_that_is_a_full_path():
    assert nse_constituents.primary_url("ind_niftybanklist.csv").endswith(
        "/IndexConstituent/ind_niftybanklist.csv"
    )
    # Nifty EV & New Age Automotive is published under a different directory.
    ev = nse_constituents.primary_url("/Index_Statistics/ind_niftyEv_NewAgeAutomotive_list.csv")
    assert ev == "https://niftyindices.com/Index_Statistics/ind_niftyEv_NewAgeAutomotive_list.csv"
    # The mirror flattens directories.
    assert nse_constituents.mirror_url(
        "/Index_Statistics/ind_niftyEv_NewAgeAutomotive_list.csv"
    ).endswith("/content/indices/ind_niftyEv_NewAgeAutomotive_list.csv")


# ── builder ─────────────────────────────────────────────────────────────────

TARGET = date(2026, 7, 30)
# NSE closed on Friday 2026-10-02 (Gandhi Jayanti): the calendar the builder
# tests run on, so session arithmetic never depends on the committed file.
HOLIDAYS = {date(2026, 10, 2)}


def _calendar() -> tuple[set[date], set[date]]:
    return HOLIDAYS, set()


def _build(spec, target, frame, **kw):
    """One builder run with the test calendar; `frame` is a frame or a fetch."""
    fetch = frame if callable(frame) else (lambda _d: frame)
    kw.setdefault("min_rows", 10)
    return builders.build_index_constituents(
        spec, target, fetch_lists=fetch, calendar=_calendar, **kw
    )


def _frame(indices: dict[str, int]) -> pd.DataFrame:
    rows = []
    for key, n in indices.items():
        for i in range(n):
            rows.append(
                {
                    "index_key": key, "index_name": key.removeprefix("IDX:"),
                    "family": "sectoral",
                    "instrument_key": f"INE{i:04d}A0101", "symbol": f"S{i}",
                    "isin": f"INE{i:04d}A0101", "company": f"C{i}",
                    "industry": "Fin", "series": "EQ", "source_file": "x.csv",
                }
            )
    return pd.DataFrame(rows, columns=nse_constituents.CONSTITUENT_COLUMNS)


def _read(spec) -> pd.DataFrame:
    return pd.read_parquet(spec.base_dir / "index_constituents_all.parquet")


def _dates(spec) -> dict[str, date]:
    out = _read(spec)
    return {k: pd.Timestamp(d).date() for k, d in out.groupby("index_key")["date"].max().items()}


@pytest.fixture
def spec(tmp_path):
    # DatasetSpec is frozen; replace() is how the sector tests rebind base_dir.
    return dataclasses.replace(datasets.INDEX_CONSTITUENTS, base_dir=tmp_path / "constituents")


def test_a_healthy_run_writes_the_parquet_with_a_date_column(spec):
    st = _build(spec, TARGET, _frame({"IDX:A": 30, "IDX:B": 20}))
    assert st.status == "success"
    out = _read(spec)
    # REQUIRED: build_manifest reads columns=["date"]; a missing column raises
    # ArrowInvalid and aborts the publish for EVERY dataset, not just this one.
    assert "date" in out.columns
    assert out["date"].nunique() == 1
    assert len(out) == 50
    assert out["catalog_rev"].nunique() == 1


def test_a_failed_fetch_keeps_the_prior_file_rather_than_emptying_it(spec):
    _build(spec, TARGET, _frame({"IDX:A": 30}))

    def _boom(_d):
        raise RuntimeError("host unreachable")

    st = _build(spec, date(2026, 7, 31), _boom)
    assert st.status == "skipped_idempotent"
    assert "retained prior" in st.message
    assert len(_read(spec)) == 30


def test_with_no_prior_file_a_failure_is_loud(spec):
    def _boom(_d):
        raise RuntimeError("host unreachable")

    assert _build(spec, TARGET, _boom).status == "failed"


def test_an_index_missing_from_this_run_carries_its_prior_rows_and_its_own_date(spec):
    _build(spec, date(2026, 9, 29), _frame({"IDX:A": 30, "IDX:B": 20}))
    # B was unreachable this time. It must NOT vanish — a drill into B would
    # otherwise show an empty basket, which reads as a real answer. And it must
    # not claim today's date: its membership was last read on 09-29.
    st = _build(spec, date(2026, 9, 30), _frame({"IDX:A": 30}))
    assert st.status == "success"
    assert "carried (not read this run): IDX:B" in st.message
    assert _dates(spec) == {"IDX:A": date(2026, 9, 30), "IDX:B": date(2026, 9, 29)}


def test_a_genuine_reconstitution_that_shrinks_the_file_is_written(spec):
    # 2026-09-30, as NSE published it: Nifty IPO 152 -> 114, Nifty Cement
    # 16 -> 12. The whole-file guard read the smaller total as a partial run
    # and kept the pre-review list live from 10-06 to 10-19.
    _build(spec, date(2026, 9, 29), _frame({"IDX:IPO": 152, "IDX:CEMENT": 16, "IDX:N50": 50}))
    st = _build(spec, date(2026, 9, 30), _frame({"IDX:IPO": 114, "IDX:CEMENT": 12, "IDX:N50": 50}))
    assert st.status == "success", st.message
    out = _read(spec)
    sizes = out.groupby("index_key").size().to_dict()
    assert sizes == {"IDX:CEMENT": 12, "IDX:IPO": 114, "IDX:N50": 50}
    assert set(_dates(spec).values()) == {date(2026, 9, 30)}


def test_a_basket_under_half_its_prior_size_keeps_the_prior_and_is_named(spec):
    _build(spec, date(2026, 9, 29), _frame({"IDX:A": 40, "IDX:B": 30}))
    st = _build(spec, date(2026, 9, 30), _frame({"IDX:A": 12, "IDX:B": 30}))
    assert st.status == "success"
    assert "held (shrink-guard): IDX:A 40->12" in st.message
    out = _read(spec)
    assert out.groupby("index_key").size().to_dict() == {"IDX:A": 40, "IDX:B": 30}
    # The held basket is the prior one, so it keeps the prior's session.
    assert _dates(spec) == {"IDX:A": date(2026, 9, 29), "IDX:B": date(2026, 9, 30)}


def test_a_basket_under_the_member_floor_is_held_even_without_a_prior(spec):
    st = _build(spec, date(2026, 9, 29), _frame({"IDX:A": 30, "IDX:TINY": 4}))
    assert "IDX:TINY 0->4" in st.message
    assert set(_read(spec)["index_key"]) == {"IDX:A"}


def test_an_index_unread_for_more_than_three_sessions_fails_the_run_naming_it(spec):
    _build(spec, date(2026, 9, 29), _frame({"IDX:A": 30, "IDX:B": 20}))
    # 09-30, 10-01 and 10-05 (10-02 is a holiday): three sessions behind is
    # one bad stretch of rungs, still fine.
    st = _build(spec, date(2026, 10, 5), _frame({"IDX:A": 30}))
    assert st.status == "success", st.message
    # The fourth session is a list nobody is reading any more: loud.
    st = _build(spec, date(2026, 10, 6), _frame({"IDX:A": 30}))
    assert st.status == "failed"
    assert "STALE > 3 sessions: IDX:B (4 sessions)" in st.message
    # Still written with everything fresh: an alert, not a held publish.
    assert _dates(spec) == {"IDX:A": date(2026, 10, 6), "IDX:B": date(2026, 9, 29)}


def test_a_holiday_or_weekend_run_dates_the_list_with_the_previous_session(spec):
    _build(spec, date(2026, 10, 2), _frame({"IDX:A": 30}))  # NSE holiday (Fri)
    assert _dates(spec) == {"IDX:A": date(2026, 10, 1)}
    _build(spec, date(2026, 10, 4), _frame({"IDX:A": 30}))  # Sunday
    assert _dates(spec) == {"IDX:A": date(2026, 10, 1)}


def test_every_run_reads_nse_again_even_the_day_after_a_build(spec):
    # No TTL. The weekly one is half of why the 09-30 review never arrived:
    # the 09-29 build made every run until 10-06 skip the fetch.
    _build(spec, date(2026, 9, 29), _frame({"IDX:A": 30}))
    ran = {"fetched": False}

    def fetch(_d):
        ran["fetched"] = True
        return _frame({"IDX:A": 31})

    st = _build(spec, date(2026, 9, 30), fetch)
    assert ran["fetched"]
    assert st.status == "success"
    assert len(_read(spec)) == 31


def test_a_back_dated_run_never_moves_the_published_list_backwards(spec):
    _build(spec, date(2026, 9, 30), _frame({"IDX:A": 30}))
    # A real frame, not a raising fetch: a raise would be swallowed by the
    # fail-closed path and prove nothing.
    calls = []

    def fetch(d):
        calls.append(d)
        return _frame({"IDX:A": 31})

    st = _build(spec, date(2026, 9, 29), fetch)
    assert st.status == "skipped_idempotent"
    assert calls == []
    assert _dates(spec) == {"IDX:A": date(2026, 9, 30)}
    assert len(_read(spec)) == 30


def test_an_empty_or_thin_run_never_overwrites(spec):
    empty = pd.DataFrame(columns=nse_constituents.CONSTITUENT_COLUMNS)
    assert _build(spec, TARGET, empty).status == "failed"
    assert _build(spec, TARGET, _frame({"IDX:A": 5}), min_rows=100).status == "failed"


def test_the_committed_catalog_resolves_and_covers_the_thematics():
    catalog = builders._read_constituents_catalog()
    assert len(catalog) > 100
    by_name = {r["index_name"]: r for r in catalog}
    # The four filename conventions that cannot be derived — if a refresh of
    # the catalog ever "tidies" these, the fetch 404s and the index drops out.
    assert by_name["Nifty India Defence"]["csv_path"] == "ind_niftyindiadefence_list.csv"
    assert by_name["Nifty India Consumption"]["csv_path"] == "ind_niftyconsumptionlist.csv"
    assert by_name["Nifty Infrastructure"]["csv_path"] == "ind_niftyinfralist.csv"
    assert "financail" in by_name["Nifty MidSmall Financial Services"]["csv_path"]
    # The whole point of the dataset: thematics NSE never gave the client.
    for name in ["Nifty MNC", "Nifty India Manufacturing", "Nifty Chemicals",
                 "Nifty EV & New Age Automotive", "Nifty India Digital", "Nifty Mobility"]:
        assert name in by_name, name


def test_index_keys_are_resolved_from_the_indices_dataset_not_the_catalog(tmp_path):
    # Names drift and keys drift with them (IDX:NIFTYINDIAINTERNET&E-COMMERCE
    # became IDX:NIFTYINDIAINTERNET). Resolution must follow the live indices
    # data, never the catalog's advisory column.
    idx = dataclasses.replace(datasets.INDICES, base_dir=tmp_path)
    pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-07-29")] * 2,
            "instrument_key": ["IDX:NIFTYINDIAINTERNET", "IDX:NIFTYBANK"],
            "symbol": ["Nifty India Internet", "Nifty Bank"],
        }
    ).to_parquet(tmp_path / f"{idx.file_prefix}_2026.parquet", index=False)

    keys = builders._index_key_map(idx)
    assert keys["NIFTYINDIAINTERNET"] == "IDX:NIFTYINDIAINTERNET"
    assert keys["NIFTYBANK"] == "IDX:NIFTYBANK"


def test_every_catalog_row_carries_nse_s_own_family_label():
    # The family filter is what the Rotation tool scopes its list by
    # (sectoral + thematic), so an unlabelled row silently disappears from the
    # tool rather than erroring. Harvested from NSE's own page URLs
    # (/indices/equity/<family>-indices/...), never inferred from the name:
    # NSE files Nifty Energy as THEMATIC, which no name-based rule would guess.
    catalog = builders._read_constituents_catalog()
    families = {r["family"] for r in catalog}
    assert families == {"sectoral", "thematic", "strategy", "broad"}
    by_name = {r["index_name"]: r["family"] for r in catalog}
    assert by_name["Nifty Energy"] == "thematic"
    assert by_name["Nifty Bank"] == "sectoral"
    assert by_name["Nifty MNC"] == "thematic"
    assert by_name["Nifty High Beta 50"] == "strategy"
    assert by_name["Nifty 50"] == "broad"
    keep = [r for r in catalog if r["family"] in ("sectoral", "thematic")]
    assert len(keep) > 60


def test_curation_is_seed_data_with_the_owner_s_thirty_names():
    # The Rotation tool's list is DATA: exactly 30 rows flagged, each carrying
    # the owner's display name. Changing the list is a seed edit + republish,
    # never an app release — that promise is this test.
    catalog = builders._read_constituents_catalog()
    flagged = [r for r in catalog if r.get("rotation", "").strip().lower() == "yes"]
    assert len(flagged) == 30
    labels = {r["index_name"]: r["display_label"] for r in flagged}
    # The owner's names where NSE's differ.
    assert labels["Nifty India Defence"] == "Nifty Defence"
    assert labels["Nifty EV & New Age Automotive"] == "Nifty EV"
    assert labels["Nifty Healthcare Index"] == "Nifty Healthcare"
    assert labels["Nifty Metal"] == "Nifty Metals & Mining"
    assert labels["Nifty India Manufacturing"] == "Nifty Manufacturing"
    # None of the near-duplicates the owner rejected.
    names = set(labels)
    for banned in ["25/50", "MidSmall", "Ex-Bank", "Nifty500 Healthcare"]:
        assert not any(banned in n for n in names), banned


def test_display_label_falls_back_to_the_index_name():
    df = nse_constituents.parse_constituents_csv(
        _csv(6),
        index_key="IDX:NIFTYBANK",
        index_name="Nifty Bank",
        family="sectoral",
        source_file="x.csv",
    )
    assert df["display_label"].unique().tolist() == ["Nifty Bank"]
    assert not df["rotation_list"].any()


def test_the_housing_header_variant_is_accepted():
    # NSE ships two header spellings. The Housing list says "Company" where
    # every other file says "Company Name" — a real variant the gate must
    # admit, found when the 30-index curation came back 29. Still a whitelist:
    # anything outside the two known forms is rejected.
    variant = b"Company,Industry,Symbol,Series,ISIN Code\n" + _csv(6)[len(HEADER):]
    assert len(_parse(variant)) == 6
    with pytest.raises(nse_constituents.MalformedConstituentCsv):
        _parse(b"Firm,Industry,Symbol,Series,ISIN Code\n" + _csv(6)[len(HEADER):])
