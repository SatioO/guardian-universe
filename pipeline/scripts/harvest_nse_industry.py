#!/usr/bin/env python3
"""Harvest NSE's per-symbol 4-tier industry classification into the seed CSV.

WHY THIS EXISTS
---------------
NSE publishes **no** bulk file mapping every listed security to its industry.
The full 4-tier classification (macro -> sector -> industry -> basicIndustry)
lives only in the per-symbol `quote-equity` API, which is anti-bot gated and
unreliable from CI datacenter IPs. So we harvest it OFFLINE, once, from a
machine that can reach NSE (your laptop), and commit the result as a static
seed CSV. `build_sector_industry` then reads that seed on every pipeline run --
classifying the whole tradable universe (~2258) instead of only the ~750
Nifty-Total-Market index members the old CSV source covered.

Classification is near-static (a stock's sector rarely changes), so you only
re-run this occasionally (e.g. monthly, or after a batch of new listings) and
re-commit the refreshed seed.

WHAT IT DOES
------------
1. Downloads NSE's full equity master (EQUITY_L.csv) -> the (symbol, ISIN)
   universe.
2. For each symbol, GETs quote-equity `industryInfo` behind a warm anti-bot
   session, extracting the four tiers.
3. Writes rows to the seed CSV (SEED_HEADER), mapping tiers name-to-name:
       sector         <- NSE sector          (drives is_cyclical)
       industry       <- NSE industry
       basic_industry <- NSE basicIndustry
   NSE's coarsest `macro` tier is dropped.
4. Resumable: re-running skips symbols already in the output; failures are
   logged to a sidecar so you can retry just those.
5. Prints a coverage + distinct-`sector` summary at the end, including which
   values matched the cyclical set -- eyeball this before committing to catch
   any NSE-vs-Total-Market punctuation drift.

USAGE
-----
    # from pipeline/ , inside the project venv (rye/uv/venv):
    python scripts/harvest_nse_industry.py               # full run -> the seed CSV
    python scripts/harvest_nse_industry.py --limit 25      # smoke test on 25 symbols
    python scripts/harvest_nse_industry.py --sleep 0.6     # be gentler on NSE
    python scripts/harvest_nse_industry.py --retry-failed  # re-attempt only prior failures

Then review the summary, `git add seeds/sector_industry_seed.csv`, commit, and
run the pipeline -- sector_industry_all.parquet now covers the full universe.

This script only ever READS from NSE and WRITES local files; it changes nothing
on NSE and places no orders. It is intentionally NOT imported by the pipeline.
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
import time
from pathlib import Path

import requests

# Keep the header, seed path, and cyclical-summary logic in lockstep with the
# pipeline by importing them rather than re-declaring.
from pipeline import config
from pipeline.sources import nse_sector

EQUITY_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
QUOTE_API = "https://www.nseindia.com/api/quote-equity?symbol={symbol}"
QUOTE_PAGE = "https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
HOMEPAGE = "https://www.nseindia.com/"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_TIMEOUT = 30
_MAX_RETRIES = 4
# Series we treat as the equity universe (matches the scanner's series gate).
_EQ_SERIES = {"EQ", "BE"}


def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": HOMEPAGE,
    })
    return s


def _warm(session: requests.Session, symbol: str | None = None) -> None:
    """Prime NSE anti-bot cookies. The homepage seeds the base cookie; hitting
    the symbol's quote PAGE (not the API) seeds the per-quote cookie the API
    then checks. Best-effort -- failures are non-fatal, the GET retries."""
    try:
        session.get(HOMEPAGE, timeout=_TIMEOUT)
        if symbol:
            session.get(QUOTE_PAGE.format(symbol=symbol), timeout=_TIMEOUT)
    except requests.RequestException:
        pass


def fetch_equity_universe(session: requests.Session) -> list[tuple[str, str]]:
    """Return [(symbol, isin), ...] for the full EQ/BE universe from EQUITY_L.csv."""
    _warm(session)
    resp = session.get(EQUITY_LIST_URL, timeout=_TIMEOUT)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.content.decode("utf-8-sig", errors="replace")))
    # EQUITY_L headers carry stray spaces (" SERIES", " ISIN NUMBER"): normalize.
    out: list[tuple[str, str]] = []
    for row in reader:
        r = {(k or "").strip().upper(): (v or "").strip() for k, v in row.items()}
        series = r.get("SERIES", "")
        symbol = r.get("SYMBOL", "").upper()
        isin = r.get("ISIN NUMBER", "").upper()
        if series in _EQ_SERIES and symbol and isin:
            out.append((symbol, isin))
    return out


def fetch_industry_info(session: requests.Session, symbol: str) -> dict[str, str] | None:
    """Return the 4-tier industryInfo for `symbol`, or None on hard failure.

    Re-warms and backs off on the anti-bot 401/403/JSON-less responses NSE
    intermittently serves."""
    last = "unknown"
    for attempt in range(_MAX_RETRIES):
        try:
            resp = session.get(QUOTE_API.format(symbol=symbol), timeout=_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                info = data.get("industryInfo") or {}
                if info.get("sector") or info.get("macro") or info.get("basicIndustry"):
                    return {
                        "macro": (info.get("macro") or "").strip(),
                        "sector": (info.get("sector") or "").strip(),
                        "industry": (info.get("industry") or "").strip(),
                        "basic_industry": (info.get("basicIndustry") or "").strip(),
                    }
                last = "empty industryInfo"
            else:
                last = f"HTTP {resp.status_code}"
        except (requests.RequestException, ValueError) as e:  # ValueError = bad JSON
            last = type(e).__name__
        # Anti-bot / transient: re-warm with the symbol page and back off.
        _warm(session, symbol)
        time.sleep(2 ** attempt)
    print(f"  ! {symbol}: giving up ({last})", file=sys.stderr)
    return None


def _load_done(out_path: Path) -> set[str]:
    """instrument_keys already present in the output CSV (for resume)."""
    if not out_path.exists():
        return set()
    try:
        with out_path.open(newline="") as f:
            reader = csv.reader(f)
            next(reader, None)  # header
            return {row[0].strip().upper() for row in reader if row}
    except OSError:
        return set()


def run(args: argparse.Namespace) -> int:
    out_path = Path(args.out) if args.out else config.SECTOR_SEED_PATH
    out_path.parent.mkdir(parents=True, exist_ok=True)
    failures_path = out_path.with_suffix(".failures.txt")

    session = _new_session()
    print(f"Fetching equity universe from {EQUITY_LIST_URL} ...")
    universe = fetch_equity_universe(session)
    print(f"  {len(universe)} EQ/BE symbols")

    done = _load_done(out_path)
    if args.retry_failed and failures_path.exists():
        retry = {s.strip().upper() for s in failures_path.read_text().split() if s.strip()}
        universe = [(sym, isin) for sym, isin in universe if sym in retry]
        print(f"  --retry-failed: {len(universe)} symbols to re-attempt")
    elif done:
        universe = [(sym, isin) for sym, isin in universe if isin not in done]
        print(f"  resuming: {len(done)} already done, {len(universe)} remaining")

    if args.limit:
        universe = universe[: args.limit]

    write_header = not out_path.exists() or out_path.stat().st_size == 0
    ok = 0
    failed: list[str] = []
    with out_path.open("a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(nse_sector.SEED_HEADER)
        for n, (symbol, isin) in enumerate(universe, 1):
            info = fetch_industry_info(session, symbol)
            if info is None or not info["industry"]:
                failed.append(symbol)
            else:
                # Tier mapping (see nse_sector.parse_sector_seed): name-to-name.
                #   sector col <- NSE sector ; industry col <- NSE industry ;
                #   basic_industry col <- NSE basicIndustry. NSE macro dropped.
                writerow_cols = [
                    isin, symbol, info["sector"], info["industry"], info["basic_industry"],
                ]
                writer.writerow(writerow_cols)
                f.flush()
                ok += 1
            if n % 50 == 0:
                print(f"  [{n}/{len(universe)}] ok={ok} failed={len(failed)}")
            time.sleep(args.sleep)

    if failed:
        failures_path.write_text("\n".join(failed) + "\n")
        print(f"\n{len(failed)} failures written to {failures_path} "
              f"(re-run with --retry-failed)")

    _summarize(out_path)
    print(f"\nSeed written to {out_path} ({ok} new rows this run).")
    print("Review the summary above, then: git add "
          f"{out_path.relative_to(config.PROJECT_ROOT)} && git commit")
    return 0 if ok or not universe else 1


def _summarize(out_path: Path) -> None:
    """Print total coverage + distinct `sector` (NSE sector) values with their
    cyclical flag, so vocabulary drift is visible before committing."""
    try:
        with out_path.open(newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except OSError:
        return
    sectors: dict[str, int] = {}
    for r in rows:
        sec = r.get("sector", "").strip()
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
    p.add_argument("--limit", type=int, default=0,
                   help="Only harvest the first N symbols (smoke test)")
    p.add_argument("--sleep", type=float, default=0.4,
                   help="Seconds between symbol requests (default 0.4)")
    p.add_argument("--retry-failed", action="store_true",
                   help="Re-attempt only symbols in the .failures.txt sidecar")
    return run(p.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
