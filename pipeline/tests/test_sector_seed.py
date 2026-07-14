"""Tests for the full-universe sector/industry SEED path.

Covers the pure seed parser (`parse_sector_seed`): 4-tier -> 3-column mapping,
punctuation-tolerant is_cyclical derivation, populated sector/basic_industry
(the whole point vs the Total-Market CSV's NULLs), malformed-row skip and ISIN
dedupe; plus the builder reading the committed seed (`_read_seed_frame`) as its
default source, end-to-end to a full-coverage parquet."""
from __future__ import annotations

import dataclasses
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import builders, config, datasets
from pipeline.sources import nse_sector

# instrument_key(ISIN), symbol, sector(<-NSE macro), industry(<-NSE sector),
# basic_industry(<-NSE basicIndustry). RELIANCE/TATASTEEL/MARUTI are cyclical;
# HDFCBANK/INFY are not. RELIANCE deliberately uses the comma'd API spelling
# "Oil, Gas & Consumable Fuels" to prove punctuation-tolerant cyclical matching.
_HEADER = ",".join(nse_sector.SEED_HEADER)
_GOOD_ROWS = [
    'INE002A01018,RELIANCE,Energy,"Oil, Gas & Consumable Fuels",Refineries & Marketing',
    "INE040A01034,HDFCBANK,Financial Services,Financial Services,Private Sector Bank",
    "INE081A01020,TATASTEEL,Commodities,Metals & Mining,Iron & Steel",
    "INE585B01010,MARUTI,Consumer Discretionary,Automobile and Auto Components,Passenger Cars",
    "INE009A01021,INFY,Information Technology,Information Technology,Computers - Software",
]


def _seed(*rows: str) -> bytes:
    return ("\n".join([_HEADER, *rows]) + "\n").encode("utf-8")


def _good_seed() -> bytes:
    return _seed(*_GOOD_ROWS)


def _sector_spec(base_dir: Path) -> datasets.DatasetSpec:
    return dataclasses.replace(datasets.SECTOR_INDUSTRY, base_dir=base_dir)


# ── parse_sector_seed ────────────────────────────────────────────────────────

def test_seed_schema_and_tier_mapping():
    df = nse_sector.parse_sector_seed(_good_seed())
    assert list(df.columns) == nse_sector.SECTOR_COLUMNS
    assert len(df) == 5
    rel = df[df["symbol"] == "RELIANCE"].iloc[0]
    assert rel["instrument_key"] == "INE002A01018"  # ISIN join key
    assert rel["sector"] == "Energy"                       # <- NSE macro
    assert rel["industry"] == "Oil, Gas & Consumable Fuels"  # <- NSE sector
    assert rel["basic_industry"] == "Refineries & Marketing"  # <- NSE basicIndustry
    # The whole point: unlike the Total-Market CSV, these are NOT all-NULL.
    assert df["sector"].notna().all()
    assert df["basic_industry"].notna().all()
    assert df["is_cyclical"].dtype == bool


def test_seed_is_cyclical_is_punctuation_tolerant():
    df = nse_sector.parse_sector_seed(_good_seed()).set_index("symbol")
    # "Oil, Gas & Consumable Fuels" (comma) still matches the no-comma cyclical set.
    assert bool(df.loc["RELIANCE", "is_cyclical"]) is True
    assert bool(df.loc["TATASTEEL", "is_cyclical"]) is True
    assert bool(df.loc["MARUTI", "is_cyclical"]) is True
    assert bool(df.loc["HDFCBANK", "is_cyclical"]) is False
    assert bool(df.loc["INFY", "is_cyclical"]) is False


def test_is_cyclical_seed_normalizes_case_and_punctuation():
    assert nse_sector.is_cyclical_seed("Oil, Gas & Consumable Fuels")
    assert nse_sector.is_cyclical_seed("METALS & MINING")
    assert nse_sector.is_cyclical_seed("metals  &  mining")
    assert not nse_sector.is_cyclical_seed("Financial Services")
    assert not nse_sector.is_cyclical_seed("Information Technology")


def test_seed_missing_optional_tiers_become_null_but_row_kept():
    # macro + basicIndustry empty, but industry (the required key) present -> kept.
    df = nse_sector.parse_sector_seed(_seed("INE111A01011,ACME,,Chemicals,"))
    assert len(df) == 1
    row = df.iloc[0]
    assert row["industry"] == "Chemicals"
    assert pd.isna(row["sector"])
    assert pd.isna(row["basic_industry"])
    assert bool(row["is_cyclical"]) is True


def test_seed_skips_rows_missing_required_fields():
    df = nse_sector.parse_sector_seed(_seed(
        "OnlyOne",
        "INE1,SYM1,Macro,,Basic",          # empty industry -> skip
        ",SYM2,Macro,Chemicals,Basic",     # empty ISIN -> skip
        "INE3,,Macro,Chemicals,Basic",     # empty symbol -> skip
        "INE4,GOODCO,Commodities,Chemicals,Commodity Chemicals",  # the one valid row
    ))
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "GOODCO"


def test_seed_dedupes_by_isin_keeping_first():
    df = nse_sector.parse_sector_seed(_seed(
        "INE002A01018,RELIANCE,Energy,Oil Gas & Consumable Fuels,Refineries & Marketing",
        "INE002A01018,RELIANCE2,Commodities,Chemicals,Commodity Chemicals",  # dup ISIN
        "INE009A01021,INFY,Information Technology,Information Technology,Software",
    ))
    assert len(df) == 2
    assert df[df["instrument_key"] == "INE002A01018"].iloc[0]["industry"] \
        == "Oil Gas & Consumable Fuels"


def test_seed_empty_and_header_only_return_empty_with_schema():
    assert nse_sector.parse_sector_seed(b"").empty
    assert nse_sector.parse_sector_seed(_HEADER.encode()).empty
    assert list(nse_sector.parse_sector_seed(b"").columns) == nse_sector.SECTOR_COLUMNS


# ── _read_seed_frame + build_sector_industry via the seed ────────────────────

def test_read_seed_frame_reads_configured_path(tmp_path, monkeypatch):
    seed = tmp_path / "sector_industry_seed.csv"
    seed.write_bytes(_good_seed())
    monkeypatch.setattr(config, "SECTOR_SEED_PATH", seed)
    df = builders._read_seed_frame(date(2026, 7, 14))
    assert len(df) == 5
    assert df["sector"].notna().all()


def test_read_seed_frame_missing_file_is_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SECTOR_SEED_PATH", tmp_path / "absent.csv")
    assert builders._read_seed_frame(date(2026, 7, 14)).empty


def test_build_from_seed_writes_full_coverage_parquet(tmp_path, monkeypatch):
    seed = tmp_path / "sector_industry_seed.csv"
    seed.write_bytes(_good_seed())
    monkeypatch.setattr(config, "SECTOR_SEED_PATH", seed)
    spec = _sector_spec(tmp_path / "sector")

    # No fetch_frame injected -> exercises the new default (_read_seed_frame).
    result = builders.build_sector_industry(spec, date(2026, 7, 14), min_rows=1)
    assert result.status == "success"
    assert result.symbol_count == 5

    out = pd.read_parquet(tmp_path / "sector" / "sector_industry_all.parquet")
    assert list(out.columns) == [*nse_sector.SECTOR_COLUMNS, "date"]
    # sector + basic_industry populated for the whole file (the coverage win).
    assert out["sector"].notna().all()
    assert out["basic_industry"].notna().all()
    assert (out["date"] == pd.Timestamp("2026-07-14")).all()
