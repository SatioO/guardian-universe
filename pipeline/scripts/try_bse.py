#!/usr/bin/env python3
"""Probe BSE's scrip list as an alternative industry source (NSE's API is
Akamai-JS-walled and unreachable from a script).

    python scripts/try_bse.py

BSE publishes the full list of equity scrips — including an industry
classification and the ISIN — as a JSON endpoint that (unlike NSE's
quote-equity) is NOT behind Akamai's JS sensor. If this returns data, we can
build the seed from BSE, joined to the NSE universe by ISIN.

This probe: hits the list endpoint, prints the HTTP status, the row count, the
FIELD NAMES available (so we see exactly what industry tiers BSE exposes), and
RELIANCE's row (found by its ISIN INE002A01018). Read-only; writes nothing.
"""
from __future__ import annotations

import json

import requests

# BSE's public "List of Scrip Data" endpoint (equity, active). Served from
# api.bseindia.com with a simple Referer gate — no JS challenge.
BSE_LIST_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
    "?Group=&Scripcode=&industry=&segment=Equity&status=Active"
)
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": _UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}
_RELIANCE_ISIN = "INE002A01018"


def main() -> int:
    print(f"GET {BSE_LIST_URL}")
    try:
        r = requests.get(BSE_LIST_URL, headers=_HEADERS, timeout=30)
    except requests.RequestException as e:
        print(f"  ✗ request failed: {type(e).__name__}: {e}")
        return 1

    print(f"HTTP {r.status_code}, {len(r.content)} bytes")
    if r.status_code != 200:
        print("  ✗ blocked / error. Body[:400]:")
        print(r.text[:400])
        return 1

    try:
        data = r.json()
    except ValueError:
        print("  ✗ response was not JSON. Body[:400]:")
        print(r.text[:400])
        return 1

    rows = data if isinstance(data, list) else data.get("Table") or []
    print(f"  ✅ parsed {len(rows)} scrips")
    if not rows:
        print("  (empty list — check the endpoint/params)")
        return 1

    print("\nfield names on each row:")
    print(" ", list(rows[0].keys()))

    # Find RELIANCE by ISIN anywhere in the row values.
    match = next(
        (row for row in rows
         if any(_RELIANCE_ISIN in str(v).upper() for v in row.values())),
        None,
    )
    print("\nRELIANCE row (by ISIN INE002A01018):")
    print(json.dumps(match, indent=2) if match else "  not found — showing first row instead:")
    if not match:
        print(json.dumps(rows[0], indent=2))
        return 0

    # The bulk INDUSTRY is null — probe the per-scrip company-header endpoint to
    # see what industry granularity BSE actually exposes for one scrip.
    code = match.get("SCRIP_CD")
    hdr_url = (
        "https://api.bseindia.com/BseIndiaAPI/api/ComHeadernew/w"
        f"?quotetype=EQ&scripcode={code}&seriesid="
    )
    print(f"\nGET per-scrip header: {hdr_url}")
    try:
        hr = requests.get(hdr_url, headers=_HEADERS, timeout=30)
        print(f"HTTP {hr.status_code}")
        if hr.status_code == 200:
            hdr = hr.json()
            print("per-scrip header fields:")
            print(" ", list(hdr.keys()) if isinstance(hdr, dict) else type(hdr))
            print(json.dumps(hdr, indent=2)[:1200])
        else:
            print("  body[:300]:", hr.text[:300])
    except (requests.RequestException, ValueError) as e:
        print(f"  ✗ per-scrip probe failed: {type(e).__name__}: {e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
