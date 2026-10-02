"""CAS-eligibility source: NSE's F&O market-lot file.

NSE's Closing Auction Session (CAS, live 2026-08-03; circulars CMTR73362,
CMTR74466, CMTR75479) applies to every cash-market stock that has derivative
contracts. The one public, bulk, machine-readable list of those stocks is the
F&O market-lot file, which names every underlying with a live contract and its
lot size per expiry month.

Pure parse — no network, no I/O — so it is unit-testable in isolation. The
fetch, the ISIN join, the stock/index decision and the fail-closed policy live
in `builders.build_cas_eligible`, the same split as `nse_constituents`.

The file as NSE publishes it (fixed-width, space-padded, LF line endings):

    UNDERLYING                          ,SYMBOL    ,OCT-26     ,NOV-26     ,...
    NIFTY 50                            ,NIFTY     ,65         ,65         ,...
    ...index underlyings...
    Derivatives on Individual Securities,Symbol    ,OCT-26     ,NOV-26     ,...
    ADANI ENERGY SOLUTION LTD           ,ADANIENSOL,675        ,675        ,...
    ...stock underlyings...

The marker row is NSE's own label for where stocks begin. It is one of two
signals the builder uses; the other is the pipeline's own equity universe,
because neither alone survives every plausible change (a renamed marker, a
stock whose symbol changed today).
"""

from __future__ import annotations

import csv
import io
import re

import pandas as pd

# The archives host is the CI-safe one: GitHub Actions cannot reach
# www.nseindia.com/api/* (Akamai blocks datacenter IPs), and nsearchives is
# already proven from CI for the constituents mirror and the equity list.
#
# There is deliberately NO mirror. The obvious candidate, the legacy
# archives.nseindia.com path, answers HTTP 200 by redirecting to an unrelated
# circular PDF (checked 2026-10-03) — a fallback that "succeeds" with the
# wrong document, which only the header gate below stands between.
MKTLOTS_URL = "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv"

# Provenance stamped on every published row.
SOURCE = "nse-fo-mktlots"

# First cell of the row that opens the stock block, compared case-folded.
STOCK_SECTION_MARKER = "derivatives on individual securities"

# The gate against an HTML/PDF/error document served with HTTP 200: the first
# two header cells must be exactly these, and at least one further cell must
# be an expiry month ("OCT-26"). A whitelist, like the constituents gate —
# an error page that happened to parse as "no stocks" would publish as a real
# answer ("nothing is CAS-eligible").
EXPECTED_LEADING_HEADER: tuple[str, str] = ("UNDERLYING", "SYMBOL")
_EXPIRY_MONTH = re.compile(r"^[A-Z]{3}-\d{2}$")

SECTION_INDEX = "index"
SECTION_STOCK = "stock"

# Emitted columns, in order.
UNDERLYING_COLUMNS: list[str] = [
    "symbol",      # NSE trading symbol, trimmed + uppercased
    "underlying",  # NSE's own (truncated) underlying name, diagnostics only
    "section",     # SECTION_INDEX | SECTION_STOCK — which block of the file
]


class MalformedMktLotsCsv(ValueError):
    """The payload is not NSE's F&O market-lot file."""


def _has_live_contract(lots: list[str]) -> bool:
    """True when at least one expiry-month cell carries a positive lot size.

    A listed underlying with every month blank has no contract to trade, so
    it is not in the derivatives segment. This is also what drops the marker
    row and any repeated header row: their cells are month names, not lots.
    """
    return any(cell.isdigit() and int(cell) > 0 for cell in lots)


def parse_mktlots_csv(payload: bytes) -> pd.DataFrame:
    """Parse the market-lot file into one row per underlying with a live
    contract: `symbol`, `underlying`, `section`.

    Raises `MalformedMktLotsCsv` when the payload is not the market-lot file
    (wrong header, empty, an HTML or PDF document) — caught by the builder,
    which keeps the prior published list rather than publishing a wrong one.

    Rows before the marker are `index`, rows after it `stock`; a file with no
    marker yields only `index` rows and leaves the stock decision to the
    builder's equity-universe match. Symbols are trimmed and uppercased and
    deduped (first occurrence wins). Tolerates a UTF-8 BOM and CRLF.
    """
    text = payload.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))

    try:
        header = [cell.strip().upper() for cell in next(reader)]
    except StopIteration as e:
        raise MalformedMktLotsCsv("empty payload") from e
    except csv.Error as e:  # binary junk (a PDF served with 200) can trip the reader
        raise MalformedMktLotsCsv(f"unreadable payload: {e}") from e

    if tuple(header[:2]) != EXPECTED_LEADING_HEADER or not any(
        _EXPIRY_MONTH.match(cell) for cell in header[2:]
    ):
        # Quote what we got: when NSE changes the format, the message is the
        # whole diagnosis.
        raise MalformedMktLotsCsv(f"unexpected header {','.join(header)[:120]!r}")

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    section = SECTION_INDEX
    try:
        for rec in reader:
            cells = [cell.strip() for cell in rec]
            if len(cells) < 3 or not any(cells):
                continue
            if cells[0].casefold().startswith(STOCK_SECTION_MARKER):
                section = SECTION_STOCK
                continue
            symbol = cells[1].upper()
            if not symbol or symbol in seen or not _has_live_contract(cells[2:]):
                continue
            seen.add(symbol)
            rows.append({"symbol": symbol, "underlying": cells[0], "section": section})
    except csv.Error as e:
        raise MalformedMktLotsCsv(f"unreadable payload: {e}") from e

    return pd.DataFrame(rows, columns=UNDERLYING_COLUMNS)
