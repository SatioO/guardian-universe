"""Freeze-limit source: BSE's daily equity-derivatives contract master.

BSE rejects a single F&O order above a per-contract quantity, its "Single
Order Quantity Limit" (notices 20230526-54, 20230621-48, 20231201-16). The
desktop app slices BFO orders and protective stops by it, broker-neutrally,
like NSE's limits in `nse_fo_contract`.

The limit is `MaxTradQty` in `BSE_EQD_CONTRACT_<ddmmyyyy>.csv`, one row per
live contract, inside the rolling zip
https://www.bseindia.com/downloads1/Live_Masters_for_Equity_Derivatives.zip
(about 2.4 MB, written around 17:40 IST; no dated archive, so the file's day
comes from the CSV's name). Columns read: `TckrSymb` (the underlying),
`FinInstrmNm` (IF, IO, SF, SO: index/stock futures/options), `MinLot`,
`MaxTradQty` and `UndrlygFinInstrmId` (BSE's code for the underlying: SENSEX
1, BANKEX 12, SENSEX50 47, FOCIT 75, a stock's scrip code). `ISIN` is blank
in every row, so a consumer matches an underlying by that code or symbol.

How it differs from NSE's file, checked on the 2026-10-14 file (40,429 rows,
217 underlyings, each with futures and options):
  - `MaxTradQty` is the largest order BSE TAKES: orders "having quantity more
    than" it are rejected (20230526-54). NSE's is the first quantity that
    freezes, hence that leg's minus one; never subtract one here.
  - Futures and options differ for some underlyings (SENSEX 500/1000, BANKEX
    600/900, FOCIT 900/1350), so a limit is per (underlying, product). Within
    a product, no underlying showed two values or two lots.
  - Limits are not whole lots (18,094 rows; ADANIENT lot 309, limit 9,000).
    The exchange's number is published; a consumer rounds down to its lot.
  - 75 stock underlyings carry 100,000,000: not a limit but "not set"
    (`NOT_SET_FROM`). Published as one lot with `basis` BASIS_NOT_SET.
  - `PrtdToTrad` is 0 on every future and most options, unlike NSE's, so it
    is not read. There are no test or dummy series.

Fetching: python-requests' default User-Agent gets HTTP 403 (a 442-byte HTML
page); a browser User-Agent gets the file (200). No Referer or cookie is
needed (curl's own User-Agent also gets 200). Measured 2026-10-15.

Pure parse — no network, no I/O — so it is unit-testable in isolation. The
fetch and the fail-closed policy live in `builders.build_bse_fno_freeze_limits`.
"""

from __future__ import annotations

import io
import re
import zipfile
from datetime import date, datetime

import pandas as pd

URL = "https://www.bseindia.com/downloads1/Live_Masters_for_Equity_Derivatives.zip"

# Provenance stamped on every published row.
SOURCE = "bse-eqd-contract"

# The standardised contract file inside the zip, named for its day.
_MEMBER = re.compile(r"^BSE_EQD_CONTRACT_(\d{8})\.csv$", re.IGNORECASE)

# The columns the parse needs: the gate against any other document (an error
# page, a notice, a changed format) served with HTTP 200.
REQUIRED_COLUMNS: tuple[str, ...] = (
    "TckrSymb",
    "FinInstrmNm",
    "MinLot",
    "MaxTradQty",
    "UndrlygFinInstrmId",
)

_KIND_OF = {"I": "index", "S": "stock"}
_PRODUCT_OF = {"F": "futures", "O": "options"}
PRODUCTS: tuple[str, ...] = ("futures", "options")

# BSE's "no limit set" value (100,000,000 on 2026-10-14). Anything from here
# up is read as not set, never as a limit.
NOT_SET_FROM = 10_000_000

# Emitted columns, in order.
LIMIT_COLUMNS: list[str] = [
    "symbol",  # BSE underlying symbol (TckrSymb), trimmed + uppercased
    "kind",  # "index" | "stock"
    "product",  # "futures" | "options"
    "underlying_id",  # UndrlygFinInstrmId, BSE's code for the underlying; 0 if unreadable
    "lot_size",  # units per lot: the largest across the product's contracts
    "max_order_quantity",  # the largest single order BSE takes, in units, as
    # the exchange gives it (not always a whole number of lots)
    "basis",  # BASIS_EXCHANGE | BASIS_STAND_IN | BASIS_NOT_SET
]

# The limit is the exchange's, read from the file.
BASIS_EXCHANGE = "exchange"
# The file lists the product but its limit cannot be trusted: one lot stands
# in (or the smaller limit the file does show).
BASIS_STAND_IN = "stand_in"
# BSE shows its "not set" value: one lot stands in, which is never too high.
BASIS_NOT_SET = "no_exchange_limit"


class MalformedContractFile(ValueError):
    """The payload is not BSE's equity-derivatives contract master."""


def _contract_csv(payload: bytes) -> tuple[date, bytes]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile as e:  # an HTML refusal or a notice served with 200
        raise MalformedContractFile(f"not a zip payload: {payload[:40]!r}") from e
    with archive:
        named = [(n, m) for n in archive.namelist() if (m := _MEMBER.match(n.rsplit("/", 1)[-1]))]
        if len(named) != 1:
            raise MalformedContractFile(
                f"expected one BSE_EQD_CONTRACT_<ddmmyyyy>.csv, found {[n for n, _ in named]}"
            )
        ((name, match),) = named
        try:
            day = datetime.strptime(match.group(1), "%d%m%Y").date()
        except ValueError as e:
            raise MalformedContractFile(f"{name} names no day") from e
        return day, archive.read(name)


def parse_freeze_limits(payload: bytes) -> tuple[date, pd.DataFrame]:
    """The file's day (from the CSV's name) and one row per (underlying,
    product) the file lists: kind, BSE's underlying code, lot size, the
    largest single order and its `basis`.

    Every doubt resolves to a smaller limit, because a limit too high is the
    one failure that hurts (an order or a stop the exchange then refuses):
      - a product listed with two limits keeps the smaller, and with two lot
        sizes takes the larger lot;
      - BSE's "not set" value (`NOT_SET_FROM` up) is never a limit: with no
        real limit beside it, the product stands at one lot (BASIS_NOT_SET);
      - a product with any row it cannot read (a lot, a limit), a limit below
        one lot, an underlying with a row whose instrument code it does not
        know (so either product), or whose underlying code is missing or
        split, stands in at one lot, or at the limit the file does show when
        that is smaller (BASIS_STAND_IN); with no readable lot, at one unit.
    Futures and options are never merged. Every symbol listed gets its rows.
    Raises `MalformedContractFile` for anything that is not the file.
    """
    day, text = _contract_csv(payload)
    if not text.strip():
        raise MalformedContractFile("empty contract file")
    try:
        df = pd.read_csv(io.BytesIO(text), usecols=lambda c: c in REQUIRED_COLUMNS, dtype=str)
    except (ValueError, pd.errors.ParserError, pd.errors.EmptyDataError) as e:
        raise MalformedContractFile(f"unreadable contract file: {e}") from e
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise MalformedContractFile(f"missing columns {missing}")

    code = df["FinInstrmNm"].fillna("").str.strip().str.upper()
    df = df.assign(
        symbol=df["TckrSymb"].fillna("").str.strip().str.upper(),
        kind=code.str[0].map(_KIND_OF).where(code.str.len() == 2),
        product=code.str[1].map(_PRODUCT_OF).where(code.str.len() == 2),
        lot=pd.to_numeric(df["MinLot"], errors="coerce"),
        limit=pd.to_numeric(df["MaxTradQty"], errors="coerce"),
        uid=pd.to_numeric(df["UndrlygFinInstrmId"], errors="coerce"),
    )
    df = df[df["symbol"] != ""]
    if df.empty:
        return day, pd.DataFrame(columns=LIMIT_COLUMNS)

    known = df["kind"].notna() & df["product"].notna()
    # Per underlying: its kind, its code, and whether any row cannot be placed.
    by_symbol = (
        df.assign(known=known)
        .groupby("symbol")
        .agg(
            kind=("kind", "first"),
            all_known=("known", "all"),
            uid=("uid", "max"),
            uids=("uid", "nunique"),
            uid_blank=("uid", lambda s: s.isna().any()),
        )
    )
    uid_ok = (by_symbol["uids"] == 1) & ~by_symbol["uid_blank"] & (by_symbol["uid"] > 0)
    by_symbol = by_symbol.assign(
        kind=by_symbol["kind"].fillna("unknown"),
        underlying_id=by_symbol["uid"].where(uid_ok, 0).astype("int64"),
        symbol_trusted=by_symbol["all_known"] & uid_ok,
    )

    # Rows that cannot be placed in a product count against both.
    unplaced = df[~known]
    placed = df[known]
    if not unplaced.empty:
        spread = [unplaced.assign(product=p) for p in PRODUCTS]
        placed = pd.concat([placed, *spread], ignore_index=True)
    placed = placed.assign(
        readable=(placed["lot"] > 0) & (placed["limit"] > 0) & placed["kind"].notna(),
        lot_known=placed["lot"].where(placed["lot"] > 0),
        real=placed["limit"].where((placed["limit"] > 0) & (placed["limit"] < NOT_SET_FROM)),
    )
    grouped = placed.groupby(["symbol", "product"], sort=True).agg(
        lot_size=("lot_known", "max"),
        largest=("real", "min"),
        all_readable=("readable", "all"),
    )
    grouped = grouped.join(by_symbol[["kind", "underlying_id", "symbol_trusted"]], on="symbol")

    lot = grouped["lot_size"].fillna(1).astype("int64")
    largest = grouped["largest"]
    readable = grouped["all_readable"] & grouped["symbol_trusted"]
    not_set = readable & largest.isna()
    trusted = readable & largest.notna() & (largest >= lot)
    # One lot stands in, and never more than a limit the file does show.
    stand_in = lot.where(largest.isna(), lot.clip(upper=largest.fillna(0).astype("int64")))
    basis = pd.Series(BASIS_STAND_IN, index=grouped.index)
    basis[trusted] = BASIS_EXCHANGE
    basis[not_set] = BASIS_NOT_SET
    quantity = stand_in.where(~trusted, largest.fillna(0).astype("int64"))
    quantity = quantity.where(~not_set, lot)
    grouped = grouped.assign(lot_size=lot, max_order_quantity=quantity, basis=basis).reset_index()
    out = grouped[LIMIT_COLUMNS].astype(
        {"underlying_id": "int64", "lot_size": "int64", "max_order_quantity": "int64"}
    )
    return day, out
