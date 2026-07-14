#!/usr/bin/env python3
"""Measure how much of the NSE universe the (BSE-harvested) seed covers.

    python scripts/check_nse_coverage.py                 # checks config.SECTOR_SEED_PATH
    python scripts/check_nse_coverage.py --seed /tmp/x.csv

BSE is a near-superset of NSE by ISIN (most stocks dual-list), so a BSE-sourced
seed covers almost all NSE stocks — but a few NSE-only listings may be missing.
This reads NSE's official equity master (EQUITY_L.csv, on the reachable CDN — NOT
the Akamai API), joins it to the seed by ISIN, and reports exactly which NSE
symbols are NOT covered, so the gap is a known number rather than a guess.
Read-only.
"""
from __future__ import annotations

import argparse
import csv
import io

import requests

from pipeline import config

EQUITY_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/csv,*/*",
}
_EQ_SERIES = {"EQ", "BE"}


def _nse_universe() -> dict[str, str]:
    """{isin: symbol} for NSE EQ/BE equities from the reachable CDN master."""
    r = requests.get(EQUITY_LIST_URL, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.content.decode("utf-8-sig", errors="replace")))
    out: dict[str, str] = {}
    for row in reader:
        n = {(k or "").strip().upper(): (v or "").strip() for k, v in row.items()}
        if n.get("SERIES", "") in _EQ_SERIES:
            isin, sym = n.get("ISIN NUMBER", "").upper(), n.get("SYMBOL", "").upper()
            if isin and sym:
                out[isin] = sym
    return out


def _seed_isins(seed_path) -> set[str]:  # noqa: ANN001 - Path
    if not seed_path.exists():
        return set()
    with seed_path.open(newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        return {row[0].strip().upper() for row in reader if row}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seed", help="Seed CSV to check (default: config.SECTOR_SEED_PATH)")
    args = p.parse_args()
    seed_path = config.SECTOR_SEED_PATH if not args.seed else __import__("pathlib").Path(args.seed)

    print(f"NSE master: {EQUITY_LIST_URL}")
    nse = _nse_universe()
    seed = _seed_isins(seed_path)
    print(f"seed: {seed_path} ({len(seed)} ISINs)")

    covered = {isin: sym for isin, sym in nse.items() if isin in seed}
    missing = {isin: sym for isin, sym in nse.items() if isin not in seed}
    pct = 100.0 * len(covered) / len(nse) if nse else 0.0
    print(f"\nNSE EQ/BE universe : {len(nse)}")
    print(f"covered by seed    : {len(covered)} ({pct:.1f}%)")
    print(f"NSE-only, MISSING  : {len(missing)}")
    if missing:
        print("\nmissing NSE symbols (not in the BSE seed):")
        for sym in sorted(missing.values()):
            print(f"  {sym}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
