"""Corporate-actions source: the book-closure file in NSE's daily PR bundle.

The desktop app pauses a price alert before a split, a bonus or another
capital action re-bases its stock's price (its level was set against the old
prices). That needs the ex-dates BEFORE they happen. NSE lists them in
`bc{DDMMYYYY}.csv`, one of the files in each trading day's PR bundle
(`PR{DDMMYY}.zip`), about one to two weeks ahead:

    SERIES,SYMBOL,SECURITY,RECORD_DT,BC_STRT_DT,BC_END_DT,EX_DT,ND_STRT_DT,ND_END_DT,PURPOSE
    EQ,MOLDTKPAC,Mold-Tek Packaging Ltd,2026-10-09,,,2026-10-09,,,BONUS 1:1

Pure parse, no network, no I/O, the same split as `nse_fo_mktlots`: the fetch,
the accumulation across days and the fail-closed policy live in
`builders.build_corporate_actions`.

Only actions that re-base the price are kept (`classify`): splits and
consolidations of face value, bonuses, rights, demergers, capital reductions.
Dividends, interest, buy-backs, distributions and partly-paid calls are not:
the exchange does not re-base a price for them. The vocabulary was taken from
50 consecutive bundles (Aug–Oct 2026); a purpose the classifier does not know
is reported by the builder, never guessed at.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass
from datetime import date

import pandas as pd

# The archives host is the CI-safe one (GitHub Actions cannot reach
# www.nseindia.com/api/*); the PR bundle is published there each trading day.
PR_URL = "https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{ddmmyy}.zip"

EXPECTED_HEADER: tuple[str, ...] = (
    "SERIES",
    "SYMBOL",
    "SECURITY",
    "RECORD_DT",
    "BC_STRT_DT",
    "BC_END_DT",
    "EX_DT",
    "ND_STRT_DT",
    "ND_END_DT",
    "PURPOSE",
)

# The kinds the app acts on, in the published `kind` column.
SPLIT = "split"
CONSOLIDATION = "consolidation"
BONUS = "bonus"
RIGHTS = "rights"
DEMERGER = "demerger"
CAPITAL_REDUCTION = "capital_reduction"
KINDS = (SPLIT, CONSOLIDATION, BONUS, RIGHTS, DEMERGER, CAPITAL_REDUCTION)


class MalformedBookClosure(ValueError):
    """The payload is not NSE's book-closure file (or its bundle)."""


@dataclass(frozen=True)
class Action:
    """One price re-basing action a purpose names. `price_factor` is the
    post ÷ pre price ratio when the purpose states it (a bonus 1:1 halves the
    price: 0.5), else None."""

    kind: str
    price_factor: float | None


# A change of face value, either way: "FVSPLT FRM RS 10 TO RE 1" (a split),
# or a consolidation worded the same way with the values the other way round.
_FACE_VALUE = re.compile(
    r"\bFV.*?(?:FRM|FROM)\s*R[SE]\.?\s*([\d.]+)\s*(?:/-)?\s*TO\s*R[SE]\.?\s*([\d.]+)"
)
_CONSOLIDATION = re.compile(r"CONSOL")
_BONUS = re.compile(r"\bBONUS\s*(\d+)\s*:\s*(\d+)")
_RIGHTS = re.compile(r"\bR(?:I?GHTS)\b")
_DEMERGER = re.compile(r"\bDEMERGER\b")
_CAPITAL_REDUCTION = re.compile(r"\bCAPITAL\s+REDUCTION\b")

# Purposes that never re-base a price: dropped silently. Anything else the
# classifier does not know is reported by the builder.
# Dividends come spelt in full ("DIV"), or run together ("INTDVSPDVRS 7.50 &
# 86.50": an interim and a special dividend); conversions of warrants or
# bonds into equity add shares without re-basing the price.
_NOT_A_RE_BASING = re.compile(
    r"DIV|(?:INT|SP|SPL|FNL)\s*DV|INT\b|INTT\b|INTEREST|PAYM|REDEM|REDMP|RDEM|AGM|EGM|BUY\s*BACK"
    r"|DISTRIBUT|FSTFINCL|CALL\b|ANNUAL|CONVERSION",
)


def classify(purpose: str) -> list[Action] | None:
    """The price re-basing actions `purpose` names: an empty list for a purpose
    that never re-bases (a dividend, interest, a buy-back), None for one the
    classifier does not recognise. A purpose may name several ("BONUS 1:1/FV
    SPLIT ..."): each is one action."""
    text = " ".join(purpose.upper().split())
    actions: list[Action] = []
    for part in re.split(r"\s*/\s*(?=[A-Z])|\s+AND\s+|\s*\+\s*", text):
        if not part:
            continue
        face_value = _FACE_VALUE.search(part)
        bonus = _BONUS.search(part)
        if face_value:
            before, after = float(face_value.group(1)), float(face_value.group(2))
            if before > 0 and after > 0 and before != after:
                kind = SPLIT if after < before else CONSOLIDATION
                actions.append(Action(kind, after / before))
                continue
        if _CONSOLIDATION.search(part):
            actions.append(Action(CONSOLIDATION, None))
        elif bonus:
            new, held = int(bonus.group(1)), int(bonus.group(2))
            if new > 0 and held > 0:
                actions.append(Action(BONUS, held / (new + held)))
        elif _RIGHTS.search(part):
            actions.append(Action(RIGHTS, None))
        elif _DEMERGER.search(part):
            actions.append(Action(DEMERGER, None))
        elif _CAPITAL_REDUCTION.search(part):
            actions.append(Action(CAPITAL_REDUCTION, None))
        elif _NOT_A_RE_BASING.search(part):
            continue
        else:
            return None
    return actions


def _iso_date(cell: str) -> date | None:
    cell = cell.strip()
    if not cell:
        return None
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return pd.to_datetime(cell, format=fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def parse_book_closure_csv(payload: bytes) -> pd.DataFrame:
    """Parse `bc{DDMMYYYY}.csv` into one row per (symbol, ex_date, purpose):
    `symbol`, `ex_date`, `purpose`. The ex-date is EX_DT, or RECORD_DT when NSE
    leaves EX_DT blank (under T+1 settlement the two are the same day); a row
    with neither is dropped. A symbol listed in several series (EQ, BE, ...)
    is one row.

    Raises `MalformedBookClosure` when the payload is not that file (wrong
    header, empty, an HTML document served with 200).
    """
    text = payload.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))
    try:
        header = tuple(cell.strip().upper() for cell in next(reader))
    except StopIteration as e:
        raise MalformedBookClosure("empty payload") from e
    except csv.Error as e:
        raise MalformedBookClosure(f"unreadable payload: {e}") from e
    if header != EXPECTED_HEADER:
        raise MalformedBookClosure(f"unexpected header {','.join(header)[:160]!r}")

    seen: set[tuple[str, date, str]] = set()
    rows: list[dict[str, object]] = []
    for cells in reader:
        if len(cells) < len(EXPECTED_HEADER):
            continue
        record = dict(zip(EXPECTED_HEADER, (cell.strip() for cell in cells), strict=False))
        symbol = record["SYMBOL"].upper()
        purpose = " ".join(record["PURPOSE"].split())
        ex_date = _iso_date(record["EX_DT"]) or _iso_date(record["RECORD_DT"])
        if not symbol or not purpose or ex_date is None:
            continue
        key = (symbol, ex_date, purpose)
        if key in seen:
            continue
        seen.add(key)
        rows.append({"symbol": symbol, "ex_date": ex_date, "purpose": purpose})
    return pd.DataFrame(rows, columns=["symbol", "ex_date", "purpose"])


def parse_pr_bundle(payload: bytes) -> pd.DataFrame:
    """The book-closure rows of one PR bundle (`parse_book_closure_csv`).

    Raises `MalformedBookClosure` when the payload is not a zip, or holds no
    `bc*.csv`."""
    try:
        bundle = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile as e:
        raise MalformedBookClosure(f"not a zip: {payload[:40]!r}") from e
    names = [
        n for n in bundle.namelist() if n.lower().startswith("bc") and n.lower().endswith(".csv")
    ]
    if not names:
        listing = ", ".join(bundle.namelist())[:160]
        raise MalformedBookClosure(f"no book-closure file in bundle ({listing})")
    return parse_book_closure_csv(bundle.read(names[0]))


def pr_url(day: date) -> str:
    return PR_URL.format(ddmmyy=day.strftime("%d%m%y"))
