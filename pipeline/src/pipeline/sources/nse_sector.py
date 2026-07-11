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
