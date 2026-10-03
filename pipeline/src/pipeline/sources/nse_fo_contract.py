"""Freeze-limit source: NSE's daily F&O contract file.

The exchange refuses ("freezes") a single F&O order above a per-underlying
quantity. Clients slice larger orders, and a protective stop above the limit
is rejected at the moment it fires, so the desktop app needs the limit for
every underlying, broker-neutrally.

NSE stopped publishing its separate quantity-freeze report on 2024-04-15
(circular NSE/FAOP/61157): the old qtyfreeze path now redirects to that
circular. The limit lives in the MII contract file instead,
`NSE_FO_contract_DDMMYYYY.csv.gz`, one row per live contract with
`TckrSymb` (the underlying), `FinInstrmNm` (FUTIDX, OPTIDX, FUTSTK, OPTSTK),
`MinLot` and `MaxTradQty`.

`MaxTradQty` is the first quantity that freezes, so the largest order the
exchange takes is one less: NIFTY 3,511 is an order of at most 3,510 units,
54 lots of 65. Checked on the 2026-10-01 file: all 237 underlyings carry one
lot size and one limit, and limit - 1 is a whole number of lots for every one
(219 real underlyings and NSE's 18 test series).

Pure parse — no network, no I/O — so it is unit-testable in isolation. The
fetch and the fail-closed policy live in `builders.build_fno_freeze_limits`.
"""

from __future__ import annotations

import gzip
import io
from datetime import date

import pandas as pd

# The archives host, which CI can reach (see nse_fo_mktlots). The contract
# file is published per trading day; the builder walks back to the last day
# that has one.
_URL = "https://nsearchives.nseindia.com/content/fo/NSE_FO_contract_{day}.csv.gz"

# Provenance stamped on every published row.
SOURCE = "nse-fo-contract"

# The columns the parse needs: the gate against any other document (an error
# page, a circular PDF, a changed format) served with HTTP 200.
REQUIRED_COLUMNS: tuple[str, ...] = (
    "TckrSymb",
    "FinInstrmNm",
    "MinLot",
    "MaxTradQty",
    "PrtdToTrad",
)

_KIND_OF = {"FUTIDX": "index", "OPTIDX": "index", "FUTSTK": "stock", "OPTSTK": "stock"}

# Emitted columns, in order.
LIMIT_COLUMNS: list[str] = [
    "symbol",  # NSE underlying symbol (TckrSymb), trimmed + uppercased
    "kind",  # "index" | "stock"
    "lot_size",  # units per lot: the largest across the underlying's
    # contracts (mid lot-revision, near series keep the old lot)
    "max_order_quantity",  # the largest single order, in units: a whole number of `lot_size`.
    # A consumer rounds it down to its own contract's lot.
    "basis",  # BASIS_EXCHANGE | BASIS_STAND_IN
]

# The limit is the exchange's, read from the file.
BASIS_EXCHANGE = "exchange"
# The file lists the underlying but its limit cannot be trusted (an unreadable
# row, or option series none of which is permitted to trade): one lot stands
# in, which is never above the exchange's limit.
BASIS_STAND_IN = "stand_in"


class MalformedContractFile(ValueError):
    """The payload is not NSE's F&O contract file."""


def contract_url(day: date) -> str:
    return _URL.format(day=day.strftime("%d%m%Y"))


def parse_freeze_limits(payload: bytes) -> pd.DataFrame:
    """One row per underlying the file lists: its kind, lot size, largest
    single order and `basis`, plus `has_options` (whether it lists option
    series) for the builder.

    Every doubt resolves to a smaller limit, because a limit too high is the
    one failure that hurts (an order or a stop the exchange then refuses):
      - an underlying listed with two limits keeps the smaller, and with two
        lot sizes rounds down to the larger lot;
      - a limit that is not a whole number of lots rounds down to one;
      - an underlying with any row it cannot read (a lot, a limit, an
        instrument code it does not know), a limit below one lot, or option
        series none of which is permitted to trade (`PrtdToTrad`, which NSE
        sets on option series only; every future reads 0), stands in at one
        lot, or at the limit the file does show when that is smaller
        (`basis` = stand_in); with no readable lot at all, at one unit.
    An underlying that lists no option series at all (NSE's own test series,
    011NSETEST...181NSETEST, are futures-only) is returned with
    `has_options` False; the builder publishes it only if it published it
    before. Every symbol listed gets a row. Raises
    `MalformedContractFile` for anything that is not the file.
    """
    columns = [*LIMIT_COLUMNS, "has_options"]
    try:
        text = gzip.decompress(payload)
    except (OSError, EOFError) as e:  # not gzip at all: a PDF or HTML served with 200
        raise MalformedContractFile(f"not a gzip payload: {payload[:40]!r}") from e
    if not text.strip():
        raise MalformedContractFile("empty payload")
    try:
        df = pd.read_csv(io.BytesIO(text), usecols=lambda c: c in REQUIRED_COLUMNS, dtype=str)
    except (ValueError, pd.errors.ParserError, pd.errors.EmptyDataError) as e:
        raise MalformedContractFile(f"unreadable payload: {e}") from e
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise MalformedContractFile(f"missing columns {missing}")

    instrument = df["FinInstrmNm"].fillna("").str.strip().str.upper()
    df = df.assign(
        symbol=df["TckrSymb"].fillna("").str.strip().str.upper(),
        kind=instrument.map(_KIND_OF),
        option=instrument.str.startswith("OPT"),
        lot_size=pd.to_numeric(df["MinLot"], errors="coerce"),
        frozen_at=pd.to_numeric(df["MaxTradQty"], errors="coerce"),
        tradable=pd.to_numeric(df["PrtdToTrad"], errors="coerce") == 1,
    )
    # Every symbol the file lists gets a row, whatever its rows say: one it
    # cannot read must stand in, never read as absent (an absent one is
    # carried forward at its old limit by the builder).
    df = df[df["symbol"] != ""]
    if df.empty:
        return pd.DataFrame(columns=columns)

    df = df.assign(
        known=df["kind"].notna(),
        readable=df["kind"].notna() & (df["lot_size"] > 0) & (df["frozen_at"] > 1),
        lot_known=df["lot_size"].where(df["lot_size"] > 0),
        largest=(df["frozen_at"] - 1).where(df["frozen_at"] > 1),
        tradable_option=df["option"] & df["tradable"],
    )
    grouped = df.groupby("symbol", sort=True).agg(
        kind=("kind", "first"),
        lot_size=("lot_known", "max"),
        largest=("largest", "min"),
        all_readable=("readable", "all"),
        has_options=("option", "any"),
        any_tradable_option=("tradable_option", "any"),
    )
    lot = grouped["lot_size"].fillna(1).astype("int64")
    largest = grouped["largest"].fillna(0).astype("int64")
    exchange = (largest // lot) * lot
    trusted = (
        grouped["all_readable"]
        & (grouped["any_tradable_option"] | ~grouped["has_options"])
        & (exchange > 0)
    )
    # One lot stands in, and never more than a limit the file does show.
    stand_in = lot.where(largest <= 0, lot.clip(upper=largest))
    grouped = grouped.assign(
        kind=grouped["kind"].fillna("unknown"),
        lot_size=lot,
        max_order_quantity=exchange.where(trusted, stand_in),
        basis=trusted.map({True: BASIS_EXCHANGE, False: BASIS_STAND_IN}),
    ).reset_index()
    return grouped[columns].astype({"lot_size": "int64", "max_order_quantity": "int64"})
