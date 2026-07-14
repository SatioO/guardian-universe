"""Sector/industry reference source: the NSE Total-Market index constituents CSV.

Pure parse/normalize (no network, no I/O) so it is unit-testable in isolation --
the fetch + atomic write + TTL/fail-closed policy live in `builders.py`
(`build_sector_industry`), mirroring how `sources.nse_secfull` keeps its
shape-adapter pure while `datasets._secfull_fallback` owns the HTTP.

Source CSV header (row 0, skipped):
    `Company Name,Industry,Symbol,Series,ISIN Code`
~752 rows. Keyed to the OHLC/universe data by ISIN (`instrument_key`).

Robustness mirrors the app's `src-tauri/src/fundamentals/industry.rs`: malformed
/ short rows are silently skipped (never crash), symbols upper-cased, and the
`is_cyclical` vocabulary is matched case-sensitively against the exact CSV
industry strings. `is_cyclical` is derived from `industry`.

The CSV exposes a single classification column ("Industry"); it carries no
separate finer/coarser tier, so `sector` and `basic_industry` are emitted as
NULL (coverage honesty -- never a fabricated value; §3.1 of the P4/P5 design)
and can be populated later if a richer source lands, without a schema change.
"""
from __future__ import annotations

import csv
import io
import re

import pandas as pd

# The unblocked NSE-archives CDN CSV (same host the bhavcopy fallback fetches
# from). Reachable from CI datacenter IPs, unlike the Akamai-blocked NSE API.
SECTOR_CSV_URL = (
    "https://nsearchives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv"
)

# Business columns emitted by parse_sector_csv, in exact order. `build_sector_
# industry` appends an operational `date` (as-of) column for manifest
# compatibility (the manifest machinery keys latest_date/rows off a `date`
# column, exactly as the `reference` builder does).
SECTOR_COLUMNS: list[str] = [
    "instrument_key",
    "symbol",
    "sector",
    "industry",
    "basic_industry",
    "is_cyclical",
]

# Cyclical industry set -- copied verbatim (case-sensitive) from the app's
# `industry.rs::is_cyclical`, matched exactly against the CSV's Industry
# vocabulary. Keep in lockstep with that Rust source.
CYCLICAL_INDUSTRIES: frozenset[str] = frozenset({
    "Metals & Mining",
    "Automobile and Auto Components",
    "Oil Gas & Consumable Fuels",
    "Construction Materials",
    "Realty",
    "Power",
    "Capital Goods",
    "Chemicals",
    "Construction",
})


def is_cyclical(industry: str) -> bool:
    """True for industries considered cyclical in the NSE taxonomy.

    Case-sensitive, matched exactly to the CSV vocabulary -- mirrors
    `industry.rs::is_cyclical` so the producer and the app agree bit-for-bit."""
    return industry in CYCLICAL_INDUSTRIES


def parse_sector_csv(csv_bytes: bytes) -> pd.DataFrame:
    """Parse the NSE Total-Market CSV bytes into the normalized sector frame.

    Columns (0-indexed): 0=Company Name, 1=Industry, 2=Symbol, 3=Series,
    4=ISIN Code. Output columns: SECTOR_COLUMNS (no `date` -- the builder
    stamps that).

    Robustness (mirrors industry.rs, hardened): uses the stdlib csv reader so
    a quoted comma inside a company name never mis-aligns the columns, skips
    the header, and skips any row that is short (<5 fields) or missing a
    required field (ISIN, symbol, or industry). Dedupes on ISIN
    (`instrument_key`), keeping the first occurrence. Never raises on
    malformed input -- a garbage row is dropped, not fatal.
    """
    text = csv_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))

    rows: list[dict[str, object]] = []
    seen_isin: set[str] = set()
    for i, parts in enumerate(reader):
        if i == 0:
            continue  # header
        if len(parts) < 5:
            continue  # malformed / short row -- skip
        industry = parts[1].strip()
        symbol = parts[2].strip().upper()
        instrument_key = parts[4].strip().upper()
        if not instrument_key or not symbol or not industry:
            continue  # missing a required field -- skip
        if instrument_key in seen_isin:
            continue  # dedupe by ISIN, keep first
        seen_isin.add(instrument_key)
        rows.append({
            "instrument_key": instrument_key,
            "symbol": symbol,
            "sector": None,          # not provided by this single-column CSV
            "industry": industry,
            "basic_industry": None,  # not provided by this single-column CSV
            "is_cyclical": is_cyclical(industry),
        })

    if not rows:
        return _empty_sector_frame()

    df = pd.DataFrame(rows)[SECTOR_COLUMNS]
    # Nullable string dtype keeps the all-null sector/basic_industry columns
    # typed as strings (not float64) so the parquet schema is stable whether
    # or not any value is ever present.
    for col in ("instrument_key", "symbol", "sector", "industry", "basic_industry"):
        df[col] = df[col].astype("string")
    df["is_cyclical"] = df["is_cyclical"].astype(bool)
    return df.reset_index(drop=True)


def _empty_sector_frame() -> pd.DataFrame:
    df = pd.DataFrame({c: [] for c in SECTOR_COLUMNS})
    for col in ("instrument_key", "symbol", "sector", "industry", "basic_industry"):
        df[col] = df[col].astype("string")
    df["is_cyclical"] = df["is_cyclical"].astype(bool)
    return df


# ---------------------------------------------------------------------------
# 3. Full-universe SEED source (all 4 NSE tiers, harvested per-symbol)
# ---------------------------------------------------------------------------
#
# The Total-Market CSV above tops out at ~750 index constituents. To classify
# the WHOLE tradable universe (~2258) we harvest NSE's per-symbol
# `quote-equity` `industryInfo` (macro -> sector -> industry -> basicIndustry)
# offline (see `scripts/harvest_nse_industry.py`) and commit the result as a
# static seed CSV. This parser reads that seed into the SAME normalized frame
# `parse_sector_csv` produces -- identical schema, just fuller coverage and
# non-NULL sector/basic_industry -- so the builder, manifest, and Rust client
# are all unchanged.
#
# Tier mapping (NSE's 4 tiers -> our 3 columns, name-to-name):
#   NSE sector        -> `sector`         (e.g. "Metals & Mining"; drives is_cyclical)
#   NSE industry      -> `industry`       (e.g. "Ferrous Metals")
#   NSE basicIndustry -> `basic_industry` (e.g. "Iron & Steel")
# NSE's coarsest `macro` tier is dropped (three columns, name-to-name).

# Canonical header the harvest writes and this parser expects (is_cyclical is
# DERIVED here, never stored, so there is one source of truth for it).
SEED_HEADER: list[str] = ["instrument_key", "symbol", "sector", "industry", "basic_industry"]


def _norm_industry(s: str) -> str:
    """Normalize an industry label for cyclical matching: lower-case, drop
    punctuation (commas/hyphens), collapse whitespace. This absorbs the
    punctuation drift between NSE sources -- the Total-Market CSV emits
    "Oil Gas & Consumable Fuels" while the per-symbol API emits
    "Oil, Gas & Consumable Fuels" -- so the same stock is cyclical from either."""
    return re.sub(r"[^a-z0-9&]+", " ", s.lower()).strip()


_CYCLICAL_NORMALIZED: frozenset[str] = frozenset(_norm_industry(x) for x in CYCLICAL_INDUSTRIES)


def is_cyclical_seed(industry: str) -> bool:
    """Cyclical test for SEED-sourced rows: punctuation/case-insensitive match
    against `CYCLICAL_INDUSTRIES`. Distinct from the exact `is_cyclical` used by
    the Total-Market path (and mirrored in `industry.rs`) precisely so the
    per-symbol API's slightly different punctuation still classifies correctly."""
    return _norm_industry(industry) in _CYCLICAL_NORMALIZED


def parse_sector_seed(csv_bytes: bytes) -> pd.DataFrame:
    """Parse the committed full-universe seed CSV into the normalized frame.

    Expected header (SEED_HEADER, row 0, skipped):
        instrument_key,symbol,sector,industry,basic_industry

    Output columns: SECTOR_COLUMNS (adds the derived `is_cyclical`; no `date` --
    the builder stamps that). Robustness mirrors `parse_sector_csv`: stdlib csv
    reader (quoted-comma safe), skips the header, skips any row missing a
    required field (instrument_key, symbol, or sector), dedupes on ISIN
    keeping the first occurrence, and never raises on malformed input. `sector`
    is the required key -- it is the primary filter tier and the is_cyclical
    source; the finer `industry`/`basic_industry` cells may be empty -> NULL
    (coverage honesty: a stock filterable by sector need not have every tier)."""
    text = csv_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))

    rows: list[dict[str, object]] = []
    seen_isin: set[str] = set()
    for i, parts in enumerate(reader):
        if i == 0:
            continue  # header
        if len(parts) < 5:
            continue  # malformed / short row -- skip
        instrument_key = parts[0].strip().upper()
        symbol = parts[1].strip().upper()
        sector = parts[2].strip()
        industry = parts[3].strip()
        basic_industry = parts[4].strip()
        # `sector` (NSE sector tier) is the required key -- the primary filter
        # tier and the cyclical source. Finer tiers may be empty -> NULL.
        if not instrument_key or not symbol or not sector:
            continue
        if instrument_key in seen_isin:
            continue  # dedupe by ISIN, keep first
        seen_isin.add(instrument_key)
        rows.append({
            "instrument_key": instrument_key,
            "symbol": symbol,
            "sector": sector,
            "industry": industry or None,
            "basic_industry": basic_industry or None,
            "is_cyclical": is_cyclical_seed(sector),   # <- from SECTOR
        })

    if not rows:
        return _empty_sector_frame()

    df = pd.DataFrame(rows)[SECTOR_COLUMNS]
    for col in ("instrument_key", "symbol", "sector", "industry", "basic_industry"):
        df[col] = df[col].astype("string")
    df["is_cyclical"] = df["is_cyclical"].astype(bool)
    return df.reset_index(drop=True)
