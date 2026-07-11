"""Tests for the sector_industry fetched-reference builder (P4).

Covers the pure CSV parse (schema, is_cyclical derivation, malformed-row skip,
ISIN keying/dedupe) and the builder's I/O policy (atomic write + date column,
weekly TTL skip, fail-closed on empty/short/failed fetch, shrink-guard hold,
and manifest pickup)."""
from __future__ import annotations

import dataclasses
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import builders, datasets, manifest
from pipeline.daily_update import RunStatus
from pipeline.sources import nse_sector

_HEADER = "Company Name,Industry,Symbol,Series,ISIN Code"

# Real ISINs; RELIANCE (Oil Gas) + TATASTEEL (Metals) + MARUTI (Auto) are
# cyclical, HDFCBANK (Financial Services) + INFY (IT) are not.
_GOOD_ROWS = [
    "Reliance Industries Ltd.,Oil Gas & Consumable Fuels,RELIANCE,EQ,INE002A01018",
    "HDFC Bank Ltd.,Financial Services,HDFCBANK,EQ,INE040A01034",
    "Tata Steel Ltd.,Metals & Mining,TATASTEEL,EQ,INE081A01020",
    "Maruti Suzuki India Ltd.,Automobile and Auto Components,MARUTI,EQ,INE585B01010",
    "Infosys Ltd.,Information Technology,INFY,EQ,INE009A01021",
]


def _csv(*rows: str) -> bytes:
    return ("\n".join([_HEADER, *rows]) + "\n").encode("utf-8")


def _good_csv() -> bytes:
    return _csv(*_GOOD_ROWS)


def _frame(*rows: str) -> pd.DataFrame:
    """The normalized frame the builder's fetch seam yields for these CSV rows
    (fetch+parse happen together in the real seam; injected here as a frame)."""
    return nse_sector.parse_sector_csv(_csv(*rows))


def _good_frame() -> pd.DataFrame:
    return _frame(*_GOOD_ROWS)


def _sector_spec(base_dir: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.SECTOR_INDUSTRY, base_dir=base_dir)


# ── parse_sector_csv ────────────────────────────────────────────────────────

def test_parse_schema_and_columns():
    df = nse_sector.parse_sector_csv(_good_csv())
    assert list(df.columns) == nse_sector.SECTOR_COLUMNS
    assert len(df) == 5
    reliance = df[df["symbol"] == "RELIANCE"].iloc[0]
    assert reliance["instrument_key"] == "INE002A01018"  # ISIN is the join key
    assert reliance["industry"] == "Oil Gas & Consumable Fuels"
    # single-column source -> sector/basic_industry are honestly NULL
    assert df["sector"].isna().all()
    assert df["basic_industry"].isna().all()
    assert df["is_cyclical"].dtype == bool


def test_is_cyclical_derivation_per_symbol():
    df = nse_sector.parse_sector_csv(_good_csv()).set_index("symbol")
    assert df.loc["RELIANCE", "is_cyclical"] is True or df.loc["RELIANCE", "is_cyclical"]
    assert bool(df.loc["TATASTEEL", "is_cyclical"]) is True
    assert bool(df.loc["MARUTI", "is_cyclical"]) is True
    assert bool(df.loc["HDFCBANK", "is_cyclical"]) is False
    assert bool(df.loc["INFY", "is_cyclical"]) is False


def test_is_cyclical_function_matches_industry_rs_set():
    for ind in [
        "Metals & Mining", "Automobile and Auto Components",
        "Oil Gas & Consumable Fuels", "Construction Materials", "Realty",
        "Power", "Capital Goods", "Chemicals", "Construction",
    ]:
        assert nse_sector.is_cyclical(ind), ind
    for ind in ["Financial Services", "Information Technology", "Healthcare",
                "Diversified", "Fast Moving Consumer Goods", "Telecommunication"]:
        assert not nse_sector.is_cyclical(ind), ind
    # case-sensitive, exactly like industry.rs::is_cyclical
    assert not nse_sector.is_cyclical("metals & mining")
    assert not nse_sector.is_cyclical("CHEMICALS")


def test_malformed_rows_skipped_no_crash():
    df = nse_sector.parse_sector_csv(_csv(
        "OnlyOneColumn",
        "Two,Cols",
        "Three,Cols,Only",
        "Missing ISIN Co,Chemicals,MISSISIN,EQ,",   # empty ISIN -> skip
        ",Chemicals,,EQ,INE999A01011",              # empty symbol -> skip
        "Good Co,Chemicals,GOODCO,EQ,INE000A01011",  # the one valid row
    ))
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "GOODCO"
    assert df.iloc[0]["instrument_key"] == "INE000A01011"
    assert bool(df.iloc[0]["is_cyclical"]) is True


def test_isin_keying_and_dedupe_keeps_first():
    df = nse_sector.parse_sector_csv(_csv(
        "Reliance Industries Ltd.,Oil Gas & Consumable Fuels,RELIANCE,EQ,INE002A01018",
        # duplicate ISIN, different symbol -> dropped (keep first)
        "Reliance Dup Ltd.,Chemicals,RELIANCE2,EQ,INE002A01018",
        "Infosys Ltd.,Information Technology,INFY,EQ,INE009A01021",
    ))
    assert len(df) == 2
    assert set(df["instrument_key"]) == {"INE002A01018", "INE009A01021"}
    # first RELIANCE row won -> industry is Oil Gas, not the dup's Chemicals
    assert df[df["instrument_key"] == "INE002A01018"].iloc[0]["industry"] \
        == "Oil Gas & Consumable Fuels"


def test_parse_handles_quoted_comma_in_company_name():
    # More robust than industry.rs' naive split: a quoted comma must not
    # mis-align the Industry/Symbol/ISIN columns.
    df = nse_sector.parse_sector_csv(_csv(
        '"Acme, Incorporated Ltd.",Chemicals,ACME,EQ,INE111A01011',
    ))
    assert len(df) == 1
    assert df.iloc[0]["industry"] == "Chemicals"
    assert df.iloc[0]["symbol"] == "ACME"
    assert df.iloc[0]["instrument_key"] == "INE111A01011"


def test_parse_empty_and_header_only_return_empty():
    assert nse_sector.parse_sector_csv(b"").empty
    assert nse_sector.parse_sector_csv(_HEADER.encode()).empty
    # empty frame still carries the schema
    assert list(nse_sector.parse_sector_csv(b"").columns) == nse_sector.SECTOR_COLUMNS


# ── build_sector_industry ───────────────────────────────────────────────────

def test_build_writes_parquet_with_date_and_schema(tmp_path: Path):
    spec = _sector_spec(tmp_path / "sector")
    result = builders.build_sector_industry(
        spec, date(2026, 7, 11), fetch_frame=lambda _t: _good_frame(), min_rows=1,
    )
    assert isinstance(result, RunStatus)
    assert result.status == "success"
    assert result.source == "nse-sector"
    assert result.symbol_count == 5

    out_path = tmp_path / "sector" / "sector_industry_all.parquet"
    assert out_path.exists()
    out = pd.read_parquet(out_path)
    assert list(out.columns) == [*nse_sector.SECTOR_COLUMNS, "date"]
    assert (out["date"] == pd.Timestamp("2026-07-11")).all()  # REQUIRED for manifest
    assert set(out["instrument_key"]) == {
        "INE002A01018", "INE040A01034", "INE081A01020",
        "INE585B01010", "INE009A01021",
    }


def test_build_ttl_skips_refetch_within_window(tmp_path: Path):
    spec = _sector_spec(tmp_path / "sector")
    calls = {"n": 0}

    def counting_fetch(_t: date) -> pd.DataFrame:
        calls["n"] += 1
        return _good_frame()

    r1 = builders.build_sector_industry(
        spec, date(2026, 7, 11), fetch_frame=counting_fetch, ttl_days=7, min_rows=1,
    )
    assert r1.status == "success" and calls["n"] == 1

    # 3 days later, still within the 7-day TTL -> no re-fetch.
    r2 = builders.build_sector_industry(
        spec, date(2026, 7, 14), fetch_frame=counting_fetch, ttl_days=7, min_rows=1,
    )
    assert r2.status == "skipped_idempotent"
    assert calls["n"] == 1  # fetch NOT called again
    assert r2.symbol_count == 5

    # 8 days after the write -> TTL expired -> re-fetch happens.
    r3 = builders.build_sector_industry(
        spec, date(2026, 7, 19), fetch_frame=counting_fetch, ttl_days=7, min_rows=1,
    )
    assert r3.status == "success" and calls["n"] == 2


def test_build_fail_closed_keeps_prior_on_fetch_error(tmp_path: Path):
    spec = _sector_spec(tmp_path / "sector")
    builders.build_sector_industry(
        spec, date(2026, 7, 1), fetch_frame=lambda _t: _good_frame(), min_rows=1,
    )
    out_path = tmp_path / "sector" / "sector_industry_all.parquet"
    good_bytes = out_path.read_bytes()

    def boom(_t: date) -> bytes:
        raise RuntimeError("network down")

    # 10 days later (past TTL) so it actually attempts a fetch, which fails.
    result = builders.build_sector_industry(
        spec, date(2026, 7, 11), fetch_frame=boom, ttl_days=7, min_rows=1,
    )
    assert result.status == "skipped_idempotent"  # ok status -> never reds the job
    assert result.symbol_count == 5
    assert "retained prior" in result.message
    assert out_path.read_bytes() == good_bytes  # prior file untouched


def test_build_empty_fetch_with_no_prior_is_failed(tmp_path: Path):
    spec = _sector_spec(tmp_path / "sector")
    result = builders.build_sector_industry(
        spec, date(2026, 7, 11), fetch_frame=lambda _t: _frame(), min_rows=1,
    )
    assert result.status == "failed"
    assert not (tmp_path / "sector" / "sector_industry_all.parquet").exists()


def test_build_never_overwrites_good_with_empty(tmp_path: Path):
    spec = _sector_spec(tmp_path / "sector")
    builders.build_sector_industry(
        spec, date(2026, 7, 1), fetch_frame=lambda _t: _good_frame(), min_rows=1,
    )
    out_path = tmp_path / "sector" / "sector_industry_all.parquet"
    good_bytes = out_path.read_bytes()

    result = builders.build_sector_industry(
        spec, date(2026, 7, 11), fetch_frame=lambda _t: _frame(),
        ttl_days=7, min_rows=1,
    )
    assert result.status == "skipped_idempotent"
    assert out_path.read_bytes() == good_bytes  # good file preserved


def test_build_min_rows_floor_rejects_truncated_fetch(tmp_path: Path):
    spec = _sector_spec(tmp_path / "sector")
    # Only 1 parsed row but floor is 3 -> suspected truncation, no prior -> failed.
    result = builders.build_sector_industry(
        spec, date(2026, 7, 11),
        fetch_frame=lambda _t: _frame(_GOOD_ROWS[0]), min_rows=3,
    )
    assert result.status == "failed"
    assert not (tmp_path / "sector" / "sector_industry_all.parquet").exists()


def test_build_shrink_guard_holds_smaller_list(tmp_path: Path):
    spec = _sector_spec(tmp_path / "sector")
    builders.build_sector_industry(
        spec, date(2026, 7, 1), fetch_frame=lambda _t: _good_frame(), min_rows=1,
    )  # 5 rows
    out_path = tmp_path / "sector" / "sector_industry_all.parquet"
    good_bytes = out_path.read_bytes()

    # A later fetch with fewer rows (2) must NOT overwrite -> respects the
    # publish shrink-guard (any per-file row decrease would block publish).
    result = builders.build_sector_industry(
        spec, date(2026, 7, 11),
        fetch_frame=lambda _t: _frame(_GOOD_ROWS[0], _GOOD_ROWS[1]),
        ttl_days=7, min_rows=1,
    )
    assert result.status == "skipped_idempotent"
    assert "shrink-guard" in result.message
    assert out_path.read_bytes() == good_bytes  # 5-row file preserved


def test_build_manifest_picks_up_sector_output(tmp_path: Path):
    spec = _sector_spec(tmp_path / "sector")
    builders.build_sector_industry(
        spec, date(2026, 7, 11), fetch_frame=lambda _t: _good_frame(), min_rows=1,
    )
    m = manifest.build_manifest(
        [spec], latest_trading_date=date(2026, 7, 11), generated_at="g",
    )
    (ds,) = m["datasets"]
    assert ds["name"] == "sector_industry"
    assert ds["latest_date"] == "2026-07-11"
    (b,) = ds["baseline"]
    assert b["name"] == "sector_industry_all.parquet"
    assert b["rows"] == 5
