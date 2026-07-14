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
import contextlib
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


def _dump(symbol: str) -> int:
    """Diagnostic: probe ONE symbol's quote-equity API two ways and print the raw
    result, so we can see WHY it 403s before changing the main flow.

      A = the current failing shape: homepage warm + session Referer=homepage.
      B = hypothesis fix: warm the symbol's quote PAGE, then call the API with
          Referer=that page + browser Sec-Fetch-* headers.

    If B returns 200, the Referer/warm shaping was the root cause. If B also
    fails, we dump its status, response headers, session cookie names, and body
    snippet so the actual block reason (Akamai challenge, geo, cookie) is visible."""
    symbol = symbol.upper()

    # Attempt A — reproduce the current flow.
    sa = _new_session()
    sa.get(HOMEPAGE, timeout=_TIMEOUT)
    a = sa.get(QUOTE_API.format(symbol=symbol), timeout=_TIMEOUT)
    print(f"A [homepage warm, Referer=homepage]          -> HTTP {a.status_code}")

    # Attempt B — page-warm + per-symbol Referer + Sec-Fetch headers.
    sb = _new_session()
    sb.get(HOMEPAGE, timeout=_TIMEOUT)
    sb.get(QUOTE_PAGE.format(symbol=symbol), timeout=_TIMEOUT)
    api_headers = {
        "Referer": QUOTE_PAGE.format(symbol=symbol),
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }
    b = sb.get(QUOTE_API.format(symbol=symbol), headers=api_headers, timeout=_TIMEOUT)
    print(f"B [page warm, Referer=quote page, sec-fetch] -> HTTP {b.status_code}")

    # Attempt C — faithful nsepython recipe: browser PAGE-LOAD headers (text/html
    # Accept + navigate Sec-Fetch) to prime `/` and `/option-chain`, then the API
    # on the same session. The key difference from A/B: the priming GETs look like
    # real browser navigations, not a JSON XHR.
    page_headers = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    sc = requests.Session()
    sc.headers.update(page_headers)
    sc.get(HOMEPAGE, timeout=_TIMEOUT)
    sc.get(f"{HOMEPAGE}option-chain", timeout=_TIMEOUT)
    c = sc.get(QUOTE_API.format(symbol=symbol), timeout=_TIMEOUT)
    print(f"C [nsepython recipe: text/html prime + / + /option-chain] -> HTTP {c.status_code}")

    # Attempt D — curl_cffi impersonating Chrome's TLS + HTTP2 fingerprint. This
    # is the real hypothesis: Akamai fingerprints the TLS ClientHello (JA3), so
    # python-requests is flagged regardless of headers/cookies. curl_cffi mimics
    # Chrome's handshake, which is what defeats it. Optional dependency.
    d = None
    try:
        from curl_cffi import requests as cffi  # type: ignore
        sd = cffi.Session(impersonate="chrome")
        sd.get(HOMEPAGE, timeout=_TIMEOUT)
        sd.get(f"{HOMEPAGE}option-chain", timeout=_TIMEOUT)
        d = sd.get(QUOTE_API.format(symbol=symbol), timeout=_TIMEOUT)
        print(f"D [curl_cffi impersonate=chrome]             -> HTTP {d.status_code}")
    except ImportError:
        print("D [curl_cffi] -> SKIPPED (run: pip install curl_cffi, then re-run --dump)")

    candidates = [("B", b), ("C", c)] + ([("D", d)] if d is not None else [])
    for label, resp in candidates:
        if resp.status_code == 200:
            info = resp.json().get("industryInfo") or {}
            print(f"  ✅ {label} works — industryInfo: {info}")
            return 0

    worst = d if d is not None else c
    tag = "D (curl_cffi)" if d is not None else "C"
    print(f"\n--- attempt {tag} failing response (diagnose the block) ---")
    print(f"status: {worst.status_code}")
    print("response headers:")
    for k, v in worst.headers.items():
        print(f"  {k}: {v}")
    print("body[:800]:")
    print(worst.text[:800])
    return 1


def _probe(symbol: str, headed: bool = False) -> int:
    """Playwright PROOF: drive a real Chromium so Akamai's JS sensor runs and
    validates `_abck`, then fetch ONE symbol's industryInfo from the page
    context. Confirms the browser approach clears the 403 before we rewrite the
    whole harvest. `--headed` opens a visible window (most reliable if headless
    is fingerprinted)."""
    symbol = symbol.upper()
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except ImportError:
        print("Install first:  pip install playwright  &&  playwright install chromium")
        return 2

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not headed,
            # --disable-http2: Akamai often RST_STREAMs the h2 connection for
            # clients it distrusts -> Chromium reports ERR_HTTP2_PROTOCOL_ERROR.
            # Forcing HTTP/1.1 sidesteps that.
            args=["--disable-blink-features=AutomationControlled", "--disable-http2"],
        )
        ctx = browser.new_context(user_agent=_UA, locale="en-US")
        page = ctx.new_page()

        # Capture the quote-equity response the PAGE fetches on its own — that
        # request rides NSE's legitimate flow (post-sensor) and is not denied,
        # unlike a fetch we inject ourselves.
        captured: dict[str, object] = {}

        def _on_response(resp) -> None:  # noqa: ANN001 - playwright Response
            if "/api/quote-equity" in resp.url and resp.status == 200 and "data" not in captured:
                # non-JSON/blocked body -> ignore
                with contextlib.suppress(Exception):
                    captured["data"] = resp.json()

        page.on("response", _on_response)

        print(f"navigating to {QUOTE_PAGE.format(symbol=symbol)} (Akamai JS solving _abck)...")
        for attempt in range(3):
            try:
                page.goto(QUOTE_PAGE.format(symbol=symbol),
                          wait_until="domcontentloaded", timeout=60000)
                break
            except Exception as e:  # noqa: BLE001 - retry transient nav/protocol errors
                print(f"  nav attempt {attempt + 1} failed: {type(e).__name__}; retrying...")
                page.wait_for_timeout(2000)

        # Wait (up to ~20s) for the page's own quote-equity XHR to land.
        for _ in range(20):
            if "data" in captured:
                break
            page.wait_for_timeout(1000)
        browser.close()

    if "data" in captured:
        data = captured["data"]
        info = (data.get("industryInfo") if isinstance(data, dict) else None) or {}
        print("captured the page's own quote-equity response ✅")
        print(f"  industryInfo: {info}")
        return 0 if info else 1
    print("  ❌ the page never produced a 200 quote-equity response (Akamai blocked its XHR too).")
    print("     Look at the browser window: did RELIANCE's price/quote actually render,")
    print("     or an 'Access Denied' / blank page? Tell me which.")
    return 1


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
    p.add_argument("--dump", metavar="SYMBOL",
                   help="Diagnose the quote-equity API for ONE symbol (probe + raw dump)")
    p.add_argument("--probe", metavar="SYMBOL",
                   help="Playwright proof: fetch ONE symbol's industryInfo via a real browser")
    p.add_argument("--headed", action="store_true",
                   help="With --probe: open a visible browser window (most reliable)")
    args = p.parse_args()
    if args.dump:
        return _dump(args.dump)
    if args.probe:
        return _probe(args.probe, headed=args.headed)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
