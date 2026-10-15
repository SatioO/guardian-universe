"""Corporate-actions history source: every NSE corporate action on an equity
share, as NSE publishes it, with its kind and the ratios its purpose states.

The trader's app serves a stock's daily history from its latest break (a bar
wholly outside the price band from the previous close). A break on the ex-date
of a demerger, a scheme, a split or a bonus is the corporate action, not a gap
in the data, and the app can join the history back across it. This module is
what tells it so.

Two sources, both NSE's own:

* The corporate-actions API behind nseindia.com's "Corporate actions" page,
  one request per window of ex-dates (residential machines only: Akamai
  blocks datacenter IPs, as it does for every `www.nseindia.com/api/*`):

      GET /api/corporates-corporateActions?index=equities&from_date=01-10-2021&to_date=31-10-2021
      [{"symbol":"AFFLE","series":"EQ","isin":"INE00WC01019","comp":"Affle 3i Limited",
        "subject":" Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share",
        "exDate":"07-Oct-2021","recDate":"08-Oct-2021",...}]

  It reaches back past 2021-10, with the purpose in full. `index=sme` lists
  NSE Emerge: there the `isin` field is a 6-digit number, never an ISIN, so it
  is not read.

* The book-closure file `bc{DDMMYYYY}.csv` in each day's PR bundle on the
  CI-safe archives host (see `nse_corporate_actions`), which lists an action
  from two days to a few weeks before its ex-date, with the purpose cut to 25
  characters and no ISIN. The daily run reads it; the API's row replaces it on
  the next residential top-up.

Pure parse, no network: the fetch, the ISIN resolution and the accumulation
live in `pipeline.corporate_actions_history`.
"""

from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd

API_URL = (
    "https://www.nseindia.com/api/corporates-corporateActions"
    "?index={index}&from_date={start}&to_date={end}"
)
# The page whose visit deposits the cookies the API checks.
API_REFERER = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
INDEX_EQUITIES = "equities"
INDEX_SME = "sme"

SOURCE_API = "nse-api"
SOURCE_BOOK_CLOSURE = "nse-book-closure"

# Equity shares, main board and Emerge. The API files a BE or BZ share under
# EQ; G-secs (GS), InvITs (IV), REITs (RR) and the book-closure file's bond
# and fund series are out of scope.
EQUITY_SERIES_ORDER = ("EQ", "BE", "BZ", "SM", "ST", "SZ")
EQUITY_SERIES = frozenset(EQUITY_SERIES_ORDER)
_SERIES_RANK = {series: rank for rank, series in enumerate(EQUITY_SERIES_ORDER)}
# The series each API index answers for (a top-up of one index leaves the
# other's rows alone).
INDEX_SERIES = {
    INDEX_EQUITIES: frozenset({"EQ", "BE", "BZ"}),
    INDEX_SME: frozenset({"SM", "ST", "SZ"}),
}

# Kinds, in the published `kind` column.
SPLIT = "split"
CONSOLIDATION = "consolidation"
BONUS = "bonus"
RIGHTS = "rights"
DEMERGER = "demerger"
SCHEME = "scheme"
CAPITAL_REDUCTION = "capital_reduction"
DIVIDEND = "dividend"
OTHER = "other"
KINDS = (
    SPLIT, CONSOLIDATION, BONUS, RIGHTS, DEMERGER, SCHEME, CAPITAL_REDUCTION, DIVIDEND, OTHER,
)

# What one API row carries (`ind`, `faceVal`, the book-closure and no-delivery
# dates and `caBroadcastDate` are there too, and not read).
API_KEYS = ("symbol", "series", "isin", "comp", "subject", "exDate", "recDate")
RAW_COLUMNS = [
    "symbol", "series", "company", "isin_nse", "ex_date", "record_date", "purpose", "source",
]

BC_HEADER: tuple[str, ...] = (
    "SERIES", "SYMBOL", "SECURITY", "RECORD_DT", "BC_STRT_DT", "BC_END_DT",
    "EX_DT", "ND_STRT_DT", "ND_END_DT", "PURPOSE",
)


class MalformedCorporateActions(ValueError):
    """The payload is not what NSE serves (an HTML page, a changed shape, a
    row outside the window asked for)."""


@dataclass(frozen=True)
class Component:
    """One action a purpose names. The numbers are only ever read from the
    purpose's own text; a field it does not state is None."""

    kind: str
    recognised: bool = True
    ratio_new: int | None = None   # bonus/rights/scheme "a:b": a new for b held
    ratio_held: int | None = None
    fv_from: float | None = None   # split/consolidation: face value before
    fv_to: float | None = None     # ... and after
    premium: float | None = None   # rights: premium per share, Rs
    dividend_ps: float | None = None  # dividend: Rs per share


# ── ISIN ────────────────────────────────────────────────────────────────────

_ISIN = re.compile(r"IN[A-Z0-9]{9}[0-9]")


def valid_isin(value: object) -> str | None:
    """`value` as an Indian ISIN when it is one (shape and check digit), else
    None. NSE prints `IN0000000000` where it has none."""
    if not isinstance(value, str):
        return None
    text = value.strip().upper()
    # NSE's placeholder IN0000000000 fails the check digit.
    if not _ISIN.fullmatch(text):
        return None
    digits = "".join(str(int(ch, 36)) for ch in text[:-1])
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch) * (2 if i % 2 == 0 else 1)
        total += n // 10 + n % 10
    return text if (10 - total % 10) % 10 == int(text[-1]) else None


# ── API ─────────────────────────────────────────────────────────────────────


def api_url(index: str, start: date, end: date) -> str:
    return API_URL.format(
        index=index, start=start.strftime("%d-%m-%Y"), end=end.strftime("%d-%m-%Y")
    )


def _api_date(cell: object, *, field: str, symbol: str) -> date | None:
    text = cell.strip() if isinstance(cell, str) else ""
    if text in ("", "-"):
        return None
    try:
        return datetime.strptime(text, "%d-%b-%Y").date()
    except ValueError as e:
        raise MalformedCorporateActions(f"{symbol}: {field} {text!r} is not dd-Mon-yyyy") from e


def parse_api_payload(payload: bytes, *, index: str, start: date, end: date) -> pd.DataFrame:
    """One API window's rows (`RAW_COLUMNS`), equity series only.

    Raises `MalformedCorporateActions` when the payload is not a JSON list of
    objects carrying `API_KEYS`, a row has no symbol or ex-date, or an ex-date
    falls outside [start, end]: any of these means NSE changed what it serves,
    and the whole run stops. On `index=sme` the `isin` field is not read."""
    try:
        rows = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise MalformedCorporateActions(f"not JSON: {payload[:60]!r}") from e
    if not isinstance(rows, list):
        raise MalformedCorporateActions(f"not a JSON list: {payload[:60]!r}")
    out: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise MalformedCorporateActions(f"row is not an object: {str(row)[:60]!r}")
        missing = [key for key in API_KEYS if key not in row]
        if missing:
            raise MalformedCorporateActions(f"row lacks {', '.join(missing)}: {str(row)[:80]!r}")
        symbol = str(row["symbol"] or "").strip().upper()
        if not symbol:
            raise MalformedCorporateActions(f"row without a symbol: {str(row)[:80]!r}")
        ex_date = _api_date(row["exDate"], field="exDate", symbol=symbol)
        if ex_date is None:
            raise MalformedCorporateActions(f"{symbol}: no exDate")
        if not start <= ex_date <= end:
            raise MalformedCorporateActions(
                f"{symbol}: exDate {ex_date} outside the window {start}..{end}"
            )
        series = str(row["series"] or "").strip().upper()
        if series not in EQUITY_SERIES:
            continue
        out.append(
            {
                "symbol": symbol,
                "series": series,
                "company": " ".join(str(row["comp"] or "").split()) or None,
                "isin_nse": row["isin"] if index == INDEX_EQUITIES else None,
                "ex_date": ex_date,
                "record_date": _api_date(row["recDate"], field="recDate", symbol=symbol),
                "purpose": " ".join(str(row["subject"] or "").split()),
                "source": SOURCE_API,
            }
        )
    return pd.DataFrame(out, columns=RAW_COLUMNS)


# ── Book-closure file ───────────────────────────────────────────────────────


def _bc_date(cell: str) -> date | None:
    text = cell.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%b-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_book_closure_csv(payload: bytes) -> pd.DataFrame:
    """`bc{DDMMYYYY}.csv` as `RAW_COLUMNS` rows, equity series only. The
    ex-date is EX_DT, or RECORD_DT when NSE leaves EX_DT blank; a row with
    neither is dropped. A share the file repeats under several series (TCS
    as BE, BL, EQ, ...) is one row, under the first of `EQUITY_SERIES_ORDER`.

    Raises `MalformedCorporateActions` on an empty payload or another header."""
    reader = csv.reader(io.StringIO(payload.decode("utf-8-sig", errors="replace")))
    try:
        header = tuple(cell.strip().upper() for cell in next(reader))
    except StopIteration as e:
        raise MalformedCorporateActions("empty book-closure file") from e
    if header != BC_HEADER:
        raise MalformedCorporateActions(f"unexpected header {','.join(header)[:160]!r}")
    seen: dict[tuple[str, date, str], int] = {}
    out: list[dict[str, object]] = []
    for cells in reader:
        if len(cells) < len(BC_HEADER):
            continue
        record = dict(zip(BC_HEADER, (cell.strip() for cell in cells), strict=False))
        series = record["SERIES"].upper()
        symbol = record["SYMBOL"].upper()
        purpose = " ".join(record["PURPOSE"].split())
        ex_date = _bc_date(record["EX_DT"]) or _bc_date(record["RECORD_DT"])
        if series not in EQUITY_SERIES or not symbol or not purpose or ex_date is None:
            continue
        row: dict[str, object] = {
            "symbol": symbol,
            "series": series,
            "company": record["SECURITY"] or None,
            "isin_nse": None,
            "ex_date": ex_date,
            "record_date": _bc_date(record["RECORD_DT"]),
            "purpose": purpose,
            "source": SOURCE_BOOK_CLOSURE,
        }
        key = (symbol, ex_date, purpose)
        if key not in seen:
            seen[key] = len(out)
            out.append(row)
        elif _SERIES_RANK[series] < _SERIES_RANK[str(out[seen[key]]["series"])]:
            out[seen[key]] = row
    return pd.DataFrame(out, columns=RAW_COLUMNS)


def parse_pr_bundle(payload: bytes) -> pd.DataFrame:
    """The book-closure rows of one PR bundle. Raises
    `MalformedCorporateActions` when it is not a zip or holds no `bc*.csv`."""
    try:
        bundle = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile as e:
        raise MalformedCorporateActions(f"not a zip: {payload[:40]!r}") from e
    names = [n for n in bundle.namelist() if re.fullmatch(r"(?i)bc\d+\.csv", n)]
    if not names:
        listing = ", ".join(bundle.namelist())[:160]
        raise MalformedCorporateActions(f"no book-closure file in bundle ({listing})")
    return parse_book_closure_csv(bundle.read(names[0]))


# ── Classifier ──────────────────────────────────────────────────────────────

# A purpose naming several actions joins them with "/", "&", "+" or "AND",
# always followed by a word that is not an amount ("Rs 690/- With 17
# Warrants", "INTDVSPDVRS 7.50 & 86.50" and "DVSPDV - RS 35/RS 130" are one
# action each).
_SEPARATOR = re.compile(
    r"(?:\s*/\s*|\s*&\s*|\s*\+\s*|\s+AND\s+)(?=[A-Z])(?!R[SE]\.?\s*-?\s*\d)"
)
# Actions run together with no separator. The book-closure file does it to
# fit 25 characters: "AGMDIV/SPDV ..." is a meeting and a dividend,
# "INT&SPLDIV- ..." an interim and a special dividend (where INT alone is bond
# interest). The API sometimes drops the "/" after "Per Share": "Interim
# Dividend - Rs 8 Per Share Special Dividend - Rs 67 Per Share" is two.
_RUN_TOGETHER = (
    (re.compile(r"\bAGM(?=DIV)"), "AGM/"),
    (re.compile(r"\bINT\s*&\s*(?=SPL?\s*DI?V)"), "INTDIV&"),
    # "AGM/DIV RS 3 SPLDV RS 3", "DIV-RS1SPLDIV-RS0.75".
    (re.compile(r"(\d)\s*(?=SPL?\s*DI?V)"), r"\1/"),
    (
        re.compile(
            r"(PER\s+SH(?:ARE)?)\s+(?=(?:SPECIAL|SPL|INTERIM|FINAL|DIVIDEND|BONUS|RIGHTS"
            r"|FACE|DEMERGER|ANNUAL)\b)"
        ),
        r"\1/",
    ),
)

# "Rs 2", "Re. 0.50", "Rs - 2.10", "Rs -10".
_RUPEES = r"R[SE]\.?\s*-?\s*([\d]+(?:\.\d+)?)"
_FACE_VALUE_CHANGE = re.compile(
    r"(?:FRM|FROM)\s*" + _RUPEES + r"\s*(?:/-)?\s*(?:PER\s+SH(?:ARE)?\s*)?\s*TO\s*" + _RUPEES
)
_SPLIT_WORDS = re.compile(r"SPLIT|SPLT|SUB-?\s*DIVISION")
_CONSOLIDATION_WORDS = re.compile(r"CONSOL")
_FACE_VALUE_WORDS = re.compile(r"\bFV|FACE\s*VALUE")
_RATIO = re.compile(r"(?<![\d.])(\d+)\s*:\s*(\d+)(?![\d.])")
_PREMIUM = re.compile(r"(?:PREMIUM|PRM)\s*(?:OF\s*)?(?:R[SE]\.?)?\s*(\d+(?:\.\d+)?)")
_AMOUNT = re.compile(_RUPEES)
_NUMBER = re.compile(r"\d+(?:\.\d+)?")
# "(20%)" restates a dividend as a share of face value: not a second amount.
_PERCENT = re.compile(r"\(?\s*\d+(?:\.\d+)?\s*%\s*\)?")

# "CONV TO INDEX SCHEME": a fund converted, not a scheme of arrangement.
_CONVERSION = re.compile(r"\bCONV(?:ERSION)?\b")
_SCHEME = re.compile(r"SCHEME|ARRANGEMENT|AMALGAMATION|\bMERGER")
_DEMERGER = re.compile(r"DE-?\s*MERGER")
_CAPITAL_REDUCTION = re.compile(r"CAPITAL\s+REDUCTION|REDUCTION\s+OF\s+(?:SHARE\s+)?CAPITAL")
# A bonus of preference shares or debentures is not a bonus of equity.
_BONUS = re.compile(r"\bBONUS\b")
_NOT_EQUITY = re.compile(r"NCRPS|PREFERENCE|PREF\b|DEBENTURE|\bNCD")
_RIGHTS = re.compile(r"\bR(?:I?GH?TS?)(?![A-Z])")
_DIVIDEND = re.compile(
    r"DIVIDEND|DIVDEND|\bDIV(?!IS)|(?:INT|SP|SPL|FNL|FIN)\s*DI?V|\bDV(?![A-Z])|DVRS|INTDV|SPDV"
)
# Purposes known to name no capital action and no dividend: meetings,
# interest and redemption of bonds, buy-backs, distributions, calls on partly
# paid shares, conversions.
_OTHER = re.compile(
    r"GENERAL\s+MEE?T|GENERAL\s+METING|\bAGM\b|\bE?O?GM\b|BUY\s*-?\s*BACK|INTEREST|\bINTT?\b"
    r"|\bINT\s+PAY|PAYM|REDEM|REDMP|RDEM|DISTRIBUT|\bCALL\b|FSTFINCL|CONVERSION|CONV\b"
)


def _ratio(part: str) -> tuple[int | None, int | None]:
    found = _RATIO.findall(part)
    if len(found) != 1:
        return None, None
    new, held = int(found[0][0]), int(found[0][1])
    return (new, held) if new > 0 and held > 0 else (None, None)


def _one_amount(part: str) -> float | None:
    """The part's amount in rupees, when it states exactly one number and
    that number is in rupees ("SPDV RE 0.25&0.25" names two: neither)."""
    text = _PERCENT.sub(" ", part)
    found = _AMOUNT.findall(text)
    numbers = _NUMBER.findall(text)
    return float(found[0]) if len(found) == 1 and len(numbers) == 1 else None


def _classify_part(part: str) -> Component:
    # Before the face-value test: a reduction can be worded as one ("Capital
    # Reduction - Reduction Of Face Value From Rs 10 To Rs 2"), and read as a
    # split it would claim a 5x re-basing.
    if _CAPITAL_REDUCTION.search(part):
        return Component(CAPITAL_REDUCTION)
    face_value = _FACE_VALUE_CHANGE.search(part)
    names_face_value = (
        _SPLIT_WORDS.search(part)
        or _CONSOLIDATION_WORDS.search(part)
        or _FACE_VALUE_WORDS.search(part)
    )
    if face_value and names_face_value:
        before, after = float(face_value.group(1)), float(face_value.group(2))
        if before > 0 and after > 0 and before != after:
            kind = SPLIT if after < before else CONSOLIDATION
            return Component(kind, fv_from=before, fv_to=after)
    if _CONSOLIDATION_WORDS.search(part):
        return Component(CONSOLIDATION)
    if _SPLIT_WORDS.search(part) and _FACE_VALUE_WORDS.search(part):
        return Component(SPLIT)
    if _DEMERGER.search(part):
        return Component(DEMERGER)
    if _CONVERSION.search(part):
        return Component(OTHER)
    if _SCHEME.search(part):
        if _NOT_EQUITY.search(part):  # preference shares: no equity ratio
            return Component(SCHEME)
        new, held = _ratio(part)
        return Component(SCHEME, ratio_new=new, ratio_held=held)
    if _BONUS.search(part):
        if _NOT_EQUITY.search(part):
            return Component(OTHER)
        new, held = _ratio(part)
        return Component(BONUS, ratio_new=new, ratio_held=held)
    if _RIGHTS.search(part):
        new, held = _ratio(part)
        premium = _PREMIUM.findall(part)
        return Component(
            RIGHTS,
            ratio_new=new,
            ratio_held=held,
            premium=float(premium[0]) if len(premium) == 1 else None,
        )
    if _DIVIDEND.search(part):
        return Component(DIVIDEND, dividend_ps=_one_amount(part))
    if _OTHER.search(part):
        return Component(OTHER)
    return Component(OTHER, recognised=False)


def classify(purpose: str) -> list[Component]:
    """The actions `purpose` names, one per part, in its order. A part the
    vocabulary does not know is `other` with `recognised=False`: the builder
    names it, so the vocabulary can be extended, and never guesses a kind."""
    text = " ".join(purpose.upper().split())
    for pattern, replacement in _RUN_TOGETHER:
        text = pattern.sub(replacement, text)
    parts = [part for part in _SEPARATOR.split(text) if part.strip(" -")]
    return [_classify_part(part) for part in parts] or [Component(OTHER, recognised=False)]
