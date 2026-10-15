"""Special pre-open source: NSE's daily capital-market security master.

On an IPO's first day, a relisting's first day and a restructured stock's
ex-date, NSE opens that one security through a Special Pre-Open Session
(SPOS) of its own: order entry 09:00-09:45 with a random close from minute 35,
matching to 09:55, trading from 10:00. Every other security keeps the regular
09:00-09:15 auction (NSE's FAQ on Special Pre-Open Session, v6.1, Oct 2025).
The desktop app needs the list on the morning itself, broker-neutrally.

The list is the MII security master `NSE_CM_security_DDMMYYYY.csv.gz`. NSE
posts file D in the evening of D (19:20-19:40 IST seen), describing the next
trading session. Its `CallAuctnInd` marks the call auction each security
opens through:

* 1: the regular pre-open;
* 2: SPOS for a listing (main-board and SME IPOs, direct listings,
  relistings);
* 3: SPOS for a scheme of arrangement or restructuring (a demerger's
  ex-date, a scheme's new listing);
* 4 and 5: the periodic call auctions of illiquid and SME securities, which
  run all day and are not SPOS.

A security flagged 2 or 3 also loses normal-market eligibility
(`ElgbltyNrmlMkt` 0) and gains `ElgbltyAddtlMkt2`. Checked on 22 files from
13-10-2025 to 15-10-2026: each of the 39 rows flagged 2 or 3 opened through
SPOS on the next trading day (HINDUNILVR on 05-12-2025 per NSE/CMTR/71508,
VEDL on 30-04-2026 per NSE/CMTR/73856, TATAMOTORS's demerger on 14-10-2025)
or listed that day per EQUITY_L / SME_EQUITY_L.

**Base price.** NSE's base price for an IPO is its issue price (FAQ Q21), and
the file's `IssePric` column is empty. But `PricRg`, the session's operating
range, is NSE's dummy band around that base (FAQ Q15): -50% to +100% for a
main-board listing, -20% to +90% for an SME IPO. Both bounds therefore give
the base, and when they agree to the paisa that is the base. It matched the
listing day's `PrvsClsgPric` in the UDiFF bhavcopy for all 24 listings
checked (LGEINDIA 1140, its issue price). A range whose bounds disagree, a
series with no documented band, and every restructuring row get no base.

Pure parse, no network, no I/O. The fetch, the session date and the
fail-closed policy live in `special_pre_open.build_special_pre_open`.
"""

from __future__ import annotations

import gzip
import io
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

import pandas as pd

# The archives host, which CI can reach (see nse_fo_mktlots).
_URL = "https://nsearchives.nseindia.com/content/cm/NSE_CM_security_{day}.csv.gz"

# Provenance stamped on every published row.
SOURCE = "nse-cm-security"

# The columns the parse reads: the gate against any other document (an error
# page, a circular) served with HTTP 200. Older files lack some of the others.
REQUIRED_COLUMNS: tuple[str, ...] = (
    "TckrSymb",
    "SctySrs",
    "ISIN",
    "CallAuctnInd",
    "PricRg",
    "DelFlg",
)

# CallAuctnInd values, as the file has carried them (22 files, 2025-2026).
_KNOWN_INDICATORS = frozenset({"1", "2", "3", "4", "5"})
KIND_OF_INDICATOR = {"2": "listing", "3": "restructuring"}

# The operating range around the base price, as (low, high) multiples of it
# (FAQ Q15), per series. A listing in any other series gets no base.
_MAIN_BOARD_BAND = (Decimal("0.5"), Decimal("2"))
_SME_BAND = (Decimal("0.8"), Decimal("1.9"))
_BAND_OF_SERIES = {
    "EQ": _MAIN_BOARD_BAND,
    "BE": _MAIN_BOARD_BAND,
    "BZ": _MAIN_BOARD_BAND,
    "SM": _SME_BAND,
    "ST": _SME_BAND,
    "SZ": _SME_BAND,
}
_PAISA = Decimal("0.01")

# Emitted columns, in order.
SESSION_COLUMNS: list[str] = [
    "symbol",  # TckrSymb, trimmed and uppercased
    "series",  # SctySrs
    "isin",
    "kind",  # "listing" | "restructuring"
    "base_price",  # float, or None when the range does not prove one
]


class MalformedSecurityFile(ValueError):
    """The payload is not NSE's capital-market security master."""


class UnknownCallAuction(ValueError):
    """The file uses a CallAuctnInd value this parser has never seen."""


@dataclass(frozen=True)
class SecurityMaster:
    """One file's special pre-open rows, and how many securities it lists."""

    sessions: pd.DataFrame
    securities: int


def security_url(day: date) -> str:
    return _URL.format(day=day.strftime("%d%m%Y"))


def base_price(series: str, price_range: str) -> float | None:
    """The base price `price_range` ("LOW-HIGH") is the listing band of, or
    None when the series has no documented band or the bounds disagree."""
    band = _BAND_OF_SERIES.get(series.strip().upper())
    if band is None:
        return None
    low_text, sep, high_text = price_range.strip().partition("-")
    if not sep:
        return None
    try:
        low, high = Decimal(low_text.strip()), Decimal(high_text.strip())
    except InvalidOperation:
        return None
    if low <= 0 or high <= low:
        return None
    from_low = (low / band[0]).quantize(_PAISA)
    from_high = (high / band[1]).quantize(_PAISA)
    if from_low != from_high:
        return None
    return float(from_low)


def parse_security_master(payload: bytes) -> SecurityMaster:
    """The special pre-open rows of a security master (gzip or plain CSV)."""
    raw = gzip.decompress(payload) if payload[:2] == b"\x1f\x8b" else payload
    try:
        frame = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False)
    except (pd.errors.ParserError, UnicodeDecodeError, ValueError) as e:
        raise MalformedSecurityFile(f"not a CSV: {e}") from e
    frame.columns = [str(c).strip() for c in frame.columns]
    missing = [c for c in REQUIRED_COLUMNS if c not in frame.columns]
    if missing:
        raise MalformedSecurityFile(f"missing columns: {', '.join(missing)}")
    frame = frame[[*REQUIRED_COLUMNS]].apply(lambda col: col.str.strip())
    live = frame[frame["DelFlg"].str.upper() != "Y"]
    unknown = sorted(set(live["CallAuctnInd"]) - _KNOWN_INDICATORS)
    if unknown:
        raise UnknownCallAuction(f"CallAuctnInd values never seen before: {', '.join(unknown)}")
    flagged = live[live["CallAuctnInd"].isin(list(KIND_OF_INDICATOR))]
    rows = []
    for symbol, series, isin, indicator, price_range in zip(
        flagged["TckrSymb"],
        flagged["SctySrs"],
        flagged["ISIN"],
        flagged["CallAuctnInd"],
        flagged["PricRg"],
        strict=True,
    ):
        kind = KIND_OF_INDICATOR[indicator]
        rows.append(
            {
                "symbol": symbol.upper(),
                "series": series.upper(),
                "isin": isin.upper(),
                "kind": kind,
                "base_price": base_price(series, price_range) if kind == "listing" else None,
            }
        )
    sessions = pd.DataFrame(rows, columns=SESSION_COLUMNS)
    return SecurityMaster(sessions=sessions, securities=len(live))
