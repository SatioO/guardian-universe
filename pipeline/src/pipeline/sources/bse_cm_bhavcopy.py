"""BSE's daily cash-market bhavcopy (UDiFF): the listings BSE traded that day.

`bse_instruments_all.parquet` is built from it (builders.build_bse_reference),
the BSE twin of NSE's `instruments_all.parquet`: it ties BSE's scrip code to
the ISIN, so the desktop app can match a BSE listing (Kite and Dhan report the
scrip code as the exchange token) to the NSE listing of the same share.

One file per session, at
https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_<yyyymmdd>_F_0000.CSV
(about 880 KB, one row per scrip traded; UDiFF since at least 2024-01-02).
Columns read:
  - `FinInstrmId`: the scrip code (543711 for SULA), 6 digits;
  - `ISIN`, `TckrSymb` (the scrip id), `SctySrs` (BSE's group: A, B, T, X,
    XT, Z, M, MT, F debt, G government securities, ...), `FinInstrmNm`;
  - `TradDt`, which must be the day asked for, and `FinInstrmTp` (only `STK`
    rows are kept; the 2026-10-26 file has no other).
Checked on 2026-10-26: 5,170 rows, every one with an ISIN, no scrip code or
ISIN or symbol twice.

Fetching: python-requests' own User-Agent gets HTTP 403; a browser one gets
the file. A day BSE has not posted (a holiday, or later than ~18:00 IST on the
day) is served as BSE's HTML home page with HTTP 200, not a 404: the parse
reads anything that is not a CSV with this header as "not published".

Pure parse: no network, no I/O. The fetch and the merge onto the published
list live in `builders.build_bse_reference`.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import date

import pandas as pd

from pipeline.errors import NotYetPublished

_BASE = "https://www.bseindia.com/download/BhavCopy/Equity"

# Provenance of the published list.
SOURCE = "bse-cm-udiff"

REQUIRED_COLUMNS: tuple[str, ...] = (
    "TradDt",
    "FinInstrmTp",
    "FinInstrmId",
    "ISIN",
    "TckrSymb",
    "SctySrs",
    "FinInstrmNm",
)

# Emitted columns, in order: one row per scrip traded that day.
DAY_COLUMNS: list[str] = ["scrip_code", "isin", "symbol", "name", "group", "date"]

_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
# BSE's scrip codes are 6 digits today; anything that does not fit an int64
# comfortably is not one.
_MAX_SCRIP_CODE = 10**12


class MalformedBhavcopy(ValueError):
    """The payload is a CSV, but not a usable BSE bhavcopy for the day."""


def bhavcopy_url(day: date) -> str:
    return f"{_BASE}/BhavCopy_BSE_CM_0_0_0_{day:%Y%m%d}_F_0000.CSV"


def _text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


@dataclass(frozen=True)
class BhavcopyDay:
    """One day's scrips (DAY_COLUMNS), and what the parse could not read:
    rows with no usable scrip code or symbol (dropped, as they cannot be
    keyed) and ISINs that are not one (set to null, the row kept: its scrip
    code still names it). Both are counted in the run's status message."""

    frame: pd.DataFrame
    dropped_rows: int
    unreadable_isins: int


def parse_bhavcopy(payload: bytes, day: date) -> BhavcopyDay:
    """The scrips BSE traded on `day`.

    Raises `NotYetPublished` for anything that is not a bhavcopy CSV (BSE's
    HTML page for a day it has not posted), and `MalformedBhavcopy` for a CSV
    that is not this day's file: missing columns, another trade date, or a
    scrip code listed twice.
    """
    head = payload[:4096].lstrip()
    if not head.startswith(b"TradDt"):
        raise NotYetPublished(f"BSE bhavcopy for {day.isoformat()} not posted (no CSV)")
    try:
        raw = pd.read_csv(io.BytesIO(payload), dtype=str, keep_default_na=False)
    except (ValueError, pd.errors.ParserError) as e:
        raise MalformedBhavcopy(f"unreadable bhavcopy: {e}") from e
    missing = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
    if missing:
        raise MalformedBhavcopy(f"bhavcopy missing required columns {missing}")

    traded = set(_text(raw["TradDt"]))
    if traded != {day.isoformat()}:
        raise MalformedBhavcopy(
            f"bhavcopy asked for {day.isoformat()} is dated {sorted(traded)[:3]}"
        )

    raw = raw[_text(raw["FinInstrmTp"]).str.upper() == "STK"]
    code = pd.to_numeric(_text(raw["FinInstrmId"]), errors="coerce")
    symbol = _text(raw["TckrSymb"]).str.upper()
    keyed = (code > 0) & (code < _MAX_SCRIP_CODE) & (code % 1 == 0) & (symbol != "")
    raw, code, symbol = raw[keyed], code[keyed], symbol[keyed]

    isin = _text(raw["ISIN"]).str.upper()
    readable = isin.str.match(_ISIN)
    group = _text(raw["SctySrs"]).str.upper()
    name = _text(raw["FinInstrmNm"])
    out = pd.DataFrame(
        {
            "scrip_code": code.astype("int64"),
            "isin": isin.where(readable, None),
            "symbol": symbol,
            "name": name.where(name != "", None),
            "group": group.where(group != "", None),
            "date": pd.Timestamp(day),
        }
    )[DAY_COLUMNS]
    twice = out["scrip_code"].duplicated(keep=False)
    if twice.any():
        codes = sorted(set(out.loc[twice, "scrip_code"].tolist()))
        raise MalformedBhavcopy(f"scrip codes listed twice: {codes[:10]}")
    return BhavcopyDay(
        frame=out.reset_index(drop=True),
        dropped_rows=int((~keyed).sum()),
        unreadable_isins=int((~readable).sum()),
    )
