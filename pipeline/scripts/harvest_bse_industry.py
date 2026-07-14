#!/usr/bin/env python3
"""Harvest the full-universe 4-tier industry classification from BSE.

WHY BSE (not NSE)
-----------------
NSE's per-symbol `quote-equity` API is behind Akamai Bot Manager's JavaScript
sensor — unreachable from any HTTP client (requests, curl_cffi, even a headed
browser's XHR all get 403). BSE exposes the SAME SEBI/AMFI 4-tier classification
via a plain JSON endpoint that is NOT JS-gated. BSE and NSE share the taxonomy,
so the data is equivalent to what NSE would give — just fetchable.

Tier mapping (BSE field -> our seed column; name-to-name with NSE's tiers):
    IndustryNew -> `sector`         (NSE 'sector' tier; drives is_cyclical)
    IGroup      -> `industry`       (NSE 'industry' tier)
    ISubGroup   -> `basic_industry` (NSE 'basicIndustry' tier)
BSE `Sector` (e.g. "Energy") is NSE's coarsest 'macro' tier and is dropped, exactly
as the NSE mapping drops macro. Keyed by ISIN (`instrument_key`).

WHAT IT DOES
------------
1. GET BSE's bulk equity scrip list (one call) -> (BSE code, ISIN, symbol) for
   every active equity.
2. For each, GET the per-scrip company header -> the 4 tier fields.
3. Write rows to the seed CSV (nse_sector.SEED_HEADER) — the SAME file the
   pipeline reads. Resumable (skips ISINs already written); failures logged to a
   sidecar for `--retry-failed`.
4. Prints a distinct-`sector` + cyclical summary — eyeball before committing.

USAGE
-----
    cd pipeline && source .venv/bin/activate
    python scripts/harvest_bse_industry.py --limit 25   # smoke test
    python scripts/harvest_bse_industry.py              # full run (~30-45 min)
    python scripts/harvest_bse_industry.py --retry-failed

Then review the summary, `git add seeds/sector_industry_seed.csv`, commit.
Read-only against BSE; writes only local files.
"""
from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

import requests

from pipeline import config
from pipeline.sources import nse_sector

BSE_LIST_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
    "?Group=&Scripcode=&industry=&segment=Equity&status=Active"
)
BSE_HEADER_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/ComHeadernew/w"
    "?quotetype=EQ&scripcode={code}&seriesid="
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
_TIMEOUT = 30
_MAX_RETRIES = 3


def fetch_scrips(session: requests.Session) -> list[tuple[str, str, str]]:
    """Return [(bse_code, isin, symbol), ...] for all active equity scrips."""
    resp = session.get(BSE_LIST_URL, timeout=_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    rows = data if isinstance(data, list) else data.get("Table") or []
    out: list[tuple[str, str, str]] = []
    for r in rows:
        code = str(r.get("SCRIP_CD") or "").strip()
        isin = str(r.get("ISIN_NUMBER") or "").strip().upper()
        symbol = str(r.get("scrip_id") or "").strip().upper()
        if code and isin and symbol:
            out.append((code, isin, symbol))
    return out


def fetch_tiers(session: requests.Session, code: str) -> dict[str, str] | None:
    """Return {sector, industry, basic_industry} for one BSE scrip, or None.

    Maps BSE IndustryNew/IGroup/ISubGroup -> sector/industry/basic_industry."""
    for attempt in range(_MAX_RETRIES):
        try:
            resp = session.get(BSE_HEADER_URL.format(code=code), timeout=_TIMEOUT)
            if resp.status_code == 200:
                h = resp.json()
                if isinstance(h, dict):
                    return {
                        "sector": (h.get("IndustryNew") or "").strip(),
                        "industry": (h.get("IGroup") or "").strip(),
                        "basic_industry": (h.get("ISubGroup") or "").strip(),
                    }
        except (requests.RequestException, ValueError):
            pass
        time.sleep(2 ** attempt)
    return None


def _load_done(out_path: Path) -> set[str]:
    if not out_path.exists():
        return set()
    try:
        with out_path.open(newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            return {row[0].strip().upper() for row in reader if row}
    except OSError:
        return set()


def run(args: argparse.Namespace) -> int:
    out_path = Path(args.out) if args.out else config.SECTOR_SEED_PATH
    out_path.parent.mkdir(parents=True, exist_ok=True)
    failures_path = out_path.with_suffix(".failures.txt")

    if args.fresh and not args.retry_failed:
        # Full re-harvest: discard any prior seed so EVERY scrip is re-fetched
        # (catches reclassifications of existing stocks, not just new listings).
        out_path.unlink(missing_ok=True)
        failures_path.unlink(missing_ok=True)

    session = requests.Session()
    session.headers.update(_HEADERS)
    print(f"Fetching BSE scrip list from {BSE_LIST_URL} ...")
    scrips = fetch_scrips(session)
    if not scrips:
        # BSE bulk list empty = outage/blocked, NOT "nothing to do". Fail so the
        # CI job goes red (and never commits a header-only seed).
        print("ABORT: BSE returned no scrips (outage/blocked) — fail-closed, seed untouched")
        return 1
    print(f"  {len(scrips)} active equity scrips")

    done = _load_done(out_path)
    if args.retry_failed and failures_path.exists():
        retry = {s.strip().upper() for s in failures_path.read_text().split() if s.strip()}
        scrips = [t for t in scrips if t[1] in retry]  # match by ISIN
        print(f"  --retry-failed: {len(scrips)} to re-attempt")
    elif done:
        scrips = [t for t in scrips if t[1] not in done]
        print(f"  resuming: {len(done)} already done, {len(scrips)} remaining")
    if args.limit:
        scrips = scrips[: args.limit]

    write_header = not out_path.exists() or out_path.stat().st_size == 0
    ok = 0
    failed: list[str] = []
    consecutive_fail = 0
    with out_path.open("a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(nse_sector.SEED_HEADER)
        for n, (code, isin, symbol) in enumerate(scrips, 1):
            tiers = fetch_tiers(session, code)
            # `sector` (BSE IndustryNew) is the primary tier + parser's required
            # key; a scrip with no sector classification is skipped (logged).
            if tiers is None or not tiers["sector"]:
                failed.append(isin)
                consecutive_fail += 1
                # Circuit breaker: a long unbroken run of failures means BSE is
                # down/blocking (not that these scrips are all unclassified) —
                # abort rather than grind through thousands and the 60-min CI cap.
                if consecutive_fail >= 50:
                    failures_path.write_text("\n".join(failed) + "\n")
                    print(f"\nABORT: {consecutive_fail} consecutive BSE failures "
                          "(systemic outage) — fail-closed")
                    return 1
            else:
                writer.writerow([isin, symbol, tiers["sector"],
                                 tiers["industry"], tiers["basic_industry"]])
                f.flush()
                ok += 1
                consecutive_fail = 0
            if n % 100 == 0:
                print(f"  [{n}/{len(scrips)}] ok={ok} failed={len(failed)}")
            time.sleep(args.sleep)

    if failed:
        failures_path.write_text("\n".join(failed) + "\n")
        print(f"\n{len(failed)} failures -> {failures_path} (retry with --retry-failed)")
    else:
        failures_path.unlink(missing_ok=True)  # clean run -> clear any stale sidecar

    _sort_seed(out_path)
    _summarize(out_path)
    print(f"\nSeed written to {out_path} ({ok} new rows this run).")
    return 0 if ok or not scrips else 1


def _sort_seed(out_path: Path) -> None:
    """Rewrite the seed sorted by instrument_key (ISIN) so a periodic re-harvest
    yields a deterministic diff -- only real classification changes show, never
    row-order churn."""
    if not out_path.exists():
        return
    with out_path.open(newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        rows = [r for r in reader if r]
    if header is None:
        return
    rows.sort(key=lambda r: r[0])
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def _summarize(out_path: Path) -> None:
    try:
        with out_path.open(newline="") as f:
            rows = list(csv.DictReader(f))
    except OSError:
        return
    sectors: dict[str, int] = {}
    for r in rows:
        sec = (r.get("sector") or "").strip()
        sectors[sec] = sectors.get(sec, 0) + 1
    print(f"\n=== seed summary: {len(rows)} rows, {len(sectors)} distinct sectors ===")
    for sec in sorted(sectors):
        cyc = "cyclical" if nse_sector.is_cyclical_seed(sec) else ""
        print(f"  {sectors[sec]:>5}  {sec}  {cyc}")


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--out", help="Output CSV path (default: config.SECTOR_SEED_PATH)")
    p.add_argument("--limit", type=int, default=0, help="Only harvest the first N scrips")
    p.add_argument("--sleep", type=float, default=0.3,
                   help="Seconds between per-scrip requests (default 0.3)")
    p.add_argument("--fresh", action="store_true",
                   help="Full re-harvest: discard any prior seed, re-fetch every scrip "
                        "(catches reclassifications). Use for the periodic CI refresh.")
    p.add_argument("--retry-failed", action="store_true",
                   help="Re-attempt only ISINs in the .failures.txt sidecar")
    return run(p.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
