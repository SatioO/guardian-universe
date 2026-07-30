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


@pytest.fixture
def spec(tmp_path):
    # DatasetSpec is frozen; replace() is how the sector tests rebind base_dir.
    return dataclasses.replace(datasets.INDEX_CONSTITUENTS, base_dir=tmp_path / "constituents")


def test_a_healthy_run_writes_the_parquet_with_a_date_column(spec):
    st = builders.build_index_constituents(
        spec, TARGET, fetch_lists=lambda _d: _frame({"IDX:A": 30, "IDX:B": 20}), min_rows=10
    )
    assert st.status == "success"
    out = pd.read_parquet(spec.base_dir / "index_constituents_all.parquet")
    # REQUIRED: build_manifest reads columns=["date"]; a missing column raises
    # ArrowInvalid and aborts the publish for EVERY dataset, not just this one.
    assert "date" in out.columns
    assert out["date"].nunique() == 1
    assert len(out) == 50


def test_a_failed_fetch_keeps_the_prior_file_rather_than_emptying_it(spec):
    builders.build_index_constituents(
        spec, TARGET, fetch_lists=lambda _d: _frame({"IDX:A": 30}), min_rows=10
    )

    def _boom(_d):
        raise RuntimeError("host unreachable")

    st = builders.build_index_constituents(
        spec, date(2026, 8, 20), fetch_lists=_boom, min_rows=10
    )
    assert st.status == "skipped_idempotent"
    assert "retained prior" in st.message
    assert len(pd.read_parquet(spec.base_dir / "index_constituents_all.parquet")) == 30


def test_with_no_prior_file_a_failure_is_loud(spec):
    def _boom(_d):
        raise RuntimeError("host unreachable")

    st = builders.build_index_constituents(spec, TARGET, fetch_lists=_boom, min_rows=10)
    assert st.status == "failed"


def test_an_index_missing_from_this_run_carries_its_prior_rows_forward(spec):
    builders.build_index_constituents(
        spec, TARGET, fetch_lists=lambda _d: _frame({"IDX:A": 30, "IDX:B": 20}), min_rows=10
    )
    # B was unreachable this time. It must NOT vanish — a drill into B would
    # otherwise show an empty basket, which reads as a real answer.
    st = builders.build_index_constituents(
        spec, date(2026, 8, 20), fetch_lists=lambda _d: _frame({"IDX:A": 30}), min_rows=10
    )
    assert st.status == "success"
    out = pd.read_parquet(spec.base_dir / "index_constituents_all.parquet")
    assert set(out["index_key"]) == {"IDX:A", "IDX:B"}


def test_a_shrink_is_held_back(spec):
    builders.build_index_constituents(
        spec, TARGET, fetch_lists=lambda _d: _frame({"IDX:A": 40}), min_rows=10
    )
    # Same index, fewer members, and nothing to carry forward: the publish
    # shrink-guard would block the shared release, so hold it here.
    st = builders.build_index_constituents(
        spec, date(2026, 8, 20), fetch_lists=lambda _d: _frame({"IDX:A": 20}), min_rows=10
    )
    assert st.status == "skipped_idempotent"
    assert "shrink-guard" in st.message


def test_an_empty_or_thin_run_never_overwrites(spec):
    empty = pd.DataFrame(columns=nse_constituents.CONSTITUENT_COLUMNS)
    assert builders.build_index_constituents(
        spec, TARGET, fetch_lists=lambda _d: empty, min_rows=10
    ).status == "failed"
    assert builders.build_index_constituents(
        spec, TARGET, fetch_lists=lambda _d: _frame({"IDX:A": 5}), min_rows=100
    ).status == "failed"


def test_the_weekly_ttl_makes_the_daily_cron_a_no_op(spec):
    builders.build_index_constituents(
        spec, TARGET, fetch_lists=lambda _d: _frame({"IDX:A": 30}), min_rows=10
    )

    def _must_not_run(_d):
        raise AssertionError("fetched inside the TTL")

    st = builders.build_index_constituents(
        spec, date(2026, 8, 1), fetch_lists=_must_not_run, ttl_days=7, min_rows=10
    )
    assert st.status == "skipped_idempotent"


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
