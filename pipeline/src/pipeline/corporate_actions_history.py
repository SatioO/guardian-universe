"""corporate_actions_history: every NSE corporate action on an equity share
since 2021-10, one row per action, keyed to the ISIN that traded on its
ex-date.

The trader's app joins a stock's history back across a break (a bar wholly
outside the price band) when an exchange action for that ISIN falls on the
break's day. This dataset is that record. It is NOT the `corporate_actions`
dataset (#48): that one lists only the actions that re-base a price, and the
app pauses a price alert for every row in it.

Two writers, one table (`ca_history/ca_history_{YYYY}.parquet`, by ex-date
year):

* `backfill_corporate_actions_history` (residential machine, `python -m
  pipeline backfill-corporate-actions`): NSE's corporate-actions API, month by
  month, equities and SME. The record of truth, purpose in full.
* `build_corporate_actions_history` (the daily run, CI): the book-closure files
  in the PR bundles of the last few trading days. The file lists an action
  from two days to a few weeks before its ex-date (2 on 2026-10-14, up to 37
  in 2022), with a 25-character purpose. Added only where no row has that
  action yet; the API's row replaces it on the next top-up.

`isin` is never NSE's printed one taken on trust. The API prints a company's
original ISIN on about a quarter of its rows (ONGC's INE213A01011 where
INE213A01029 has traded since 2011; the pre-split ISIN on a split's own
ex-date), and nothing on SME rows. So every run resolves it from the
pipeline's own instrument list (`reference`): the ISIN of the equity version
listed under that symbol on the ex-date. NSE's value is kept as `isin_nse`.

The table only grows: rows are keyed (source, symbol, ex_date, purpose, part),
a row seen again keeps its first sighting, and a book-closure row leaves only
when an API row for the same action replaces it.
"""

from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from datetime import date, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipeline import config
from pipeline.daily_update import RunStatus
from pipeline.datasets import DatasetSpec
from pipeline.fetch import _BROWSER_UA, _fetch_with_retry
from pipeline.sources import nse_ca_history as ca
from pipeline.sources import nse_corporate_actions

# The types the client reads. `part` numbers the actions one purpose names
# ("AGM/Dividend Rs 17" is parts 0 and 1); `first_seen` is the day this
# pipeline first recorded the row; `date` the as-of the manifest reads (the
# run that last rewrote that year's file).
SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("series", pa.string()),
        ("isin", pa.string()),
        ("isin_nse", pa.string()),
        ("company", pa.string()),
        ("ex_date", pa.date32()),
        ("record_date", pa.date32()),
        ("purpose", pa.string()),
        ("part", pa.int32()),
        ("kind", pa.string()),
        ("ratio_new", pa.int64()),
        ("ratio_held", pa.int64()),
        ("fv_from", pa.float64()),
        ("fv_to", pa.float64()),
        ("premium", pa.float64()),
        ("dividend_ps", pa.float64()),
        ("source", pa.string()),
        ("first_seen", pa.date32()),
        ("date", pa.timestamp("ms")),
    ]
)
COLUMNS = [field.name for field in SCHEMA if field.name != "date"]
KEY = ["source", "symbol", "ex_date", "purpose", "part"]
_INT_COLUMNS = ("part", "ratio_new", "ratio_held")
_DATE_COLUMNS = ("ex_date", "record_date", "first_seen")
_FLOAT_COLUMNS = ("fv_from", "fv_to", "premium", "dividend_ps")
_TEXT_COLUMNS = ("symbol", "series", "isin", "isin_nse", "company", "purpose", "kind", "source")

# How long after an ex-date a share that did not trade on it may relist and
# still be the share the action produced (InstrumentList.isin). The longest
# seen 2021-2026 was VERTOZ's consolidation: 16 days.
_RELISTED_WITHIN = timedelta(days=31)

BACKFILL_HINT = (
    "run `python -m pipeline backfill-corporate-actions` from a residential machine "
    "(RUNBOOK.md 'Corporate actions history')"
)


# ── Rows ────────────────────────────────────────────────────────────────────


def expand(raw: pd.DataFrame, *, first_seen: date) -> tuple[pd.DataFrame, set[str]]:
    """One row per action each raw row's purpose names (`COLUMNS`, `isin`
    unresolved), and the purposes the classifier did not recognise (kept as
    `other`, never guessed at)."""
    rows: list[dict[str, object]] = []
    unknown: set[str] = set()
    for record in raw.to_dict("records"):
        for part, action in enumerate(ca.classify(str(record["purpose"]))):
            if not action.recognised:
                unknown.add(str(record["purpose"]))
            rows.append(
                {
                    "symbol": record["symbol"],
                    "series": record["series"],
                    "isin": None,
                    "isin_nse": ca.valid_isin(record["isin_nse"]),
                    "company": record["company"],
                    "ex_date": record["ex_date"],
                    "record_date": record["record_date"],
                    "purpose": record["purpose"],
                    "part": part,
                    "kind": action.kind,
                    "ratio_new": action.ratio_new,
                    "ratio_held": action.ratio_held,
                    "fv_from": action.fv_from,
                    "fv_to": action.fv_to,
                    "premium": action.premium,
                    "dividend_ps": action.dividend_ps,
                    "source": record["source"],
                    "first_seen": first_seen,
                }
            )
    return _typed(pd.DataFrame(rows, columns=COLUMNS)), unknown


def _typed(frame: pd.DataFrame) -> pd.DataFrame:
    """One dtype per column whatever the frame came from (dates and text as
    Python objects with None, ints nullable, floats NaN), so frames from a
    parse and from a parquet file compare, dedupe and concatenate alike."""
    out = frame[COLUMNS].copy()
    for column in _DATE_COLUMNS:
        days = pd.to_datetime(out[column])
        out[column] = pd.Series(
            [None if pd.isna(day) else day.date() for day in days], index=out.index, dtype=object
        )
    for column in _TEXT_COLUMNS:
        out[column] = pd.Series(
            [None if pd.isna(value) else str(value) for value in out[column]],
            index=out.index,
            dtype=object,
        )
    for column in _INT_COLUMNS:
        out[column] = out[column].astype("Int64")
    for column in _FLOAT_COLUMNS:
        out[column] = out[column].astype("float64")
    return out


# ── ISIN ────────────────────────────────────────────────────────────────────


class InstrumentList:
    """The equity versions of the pipeline's instrument list
    (`instruments_all.parquet`), for resolving which ISIN traded under a
    symbol on a day. A face-value split re-issues the ISIN, so a symbol has
    several versions, each with its own first_seen..last_seen; an issuer's
    bonds trade under its symbol too, which is why only equity series count."""

    def __init__(self, frame: pd.DataFrame) -> None:
        equity = frame[frame["series"].isin(ca.EQUITY_SERIES)]
        self._by_symbol: dict[str, list[tuple[date, date, str]]] = defaultdict(list)
        self._by_isin: dict[str, list[tuple[date, date]]] = defaultdict(list)
        latest = date.min
        for symbol, isin, first, last in zip(
            equity["symbol"], equity["isin"], equity["first_seen"], equity["last_seen"], strict=True
        ):
            isin = ca.valid_isin(isin)
            if isin is None or pd.isna(first) or pd.isna(last):
                continue
            span = (pd.Timestamp(first).date(), pd.Timestamp(last).date())
            self._by_symbol[str(symbol).strip().upper()].append((*span, isin))
            self._by_isin[isin].append(span)
            latest = max(latest, span[1])
        self.latest = latest

    @classmethod
    def read(cls, spec: DatasetSpec) -> InstrumentList:
        """Raises when the list is missing, unreadable or empty: ISINs are
        resolved on every run, and a run without them must not write."""
        path = spec.base_dir / f"{spec.file_prefix}_all.parquet"
        columns = ["symbol", "series", "isin", "first_seen", "last_seen"]
        frame = pd.read_parquet(path, columns=columns)
        listed = cls(frame)
        if listed.latest == date.min:
            raise ValueError(f"{path.name} lists no equity version with an ISIN")
        return listed

    def isin(self, symbol: str, ex_date: date, isin_nse: str | None) -> str | None:
        """The ISIN of the equity listed as `symbol` on `ex_date` (or, for an
        ex-date after the list's newest day, on that day). NSE's printed ISIN
        settles a tie, or stands in when the symbol is unknown that day (a
        later rename) but that ISIN itself was trading.

        A share often does not trade on the ex-date of a split or a scheme:
        the old ISIN stops before it and the new one lists a few sessions
        later (DRCSYSTEMS split 2022-03-16, its new ISIN first traded
        2022-03-21). With nothing listed under the symbol that day, the first
        listing within `_RELISTED_WITHIN` after it is the share the action
        produced. Otherwise None."""
        day = min(ex_date, self.latest)
        versions = self._by_symbol.get(symbol, ())
        found = {isin for first, last, isin in versions if first <= day <= last}
        if len(found) == 1:
            return found.pop()
        if isin_nse is not None and (
            isin_nse in found
            or (not found and any(f <= day <= last for f, last in self._by_isin.get(isin_nse, ())))
        ):
            return isin_nse
        if found:
            return None
        window = day + _RELISTED_WITHIN
        later = [(first, isin) for first, _, isin in versions if day < first <= window]
        if not later:
            return None
        relisted = {isin for first, isin in later if first == min(later)[0]}
        return relisted.pop() if len(relisted) == 1 else None


def resolve_isins(frame: pd.DataFrame, listed: InstrumentList) -> pd.DataFrame:
    """Every row's ISIN from `listed`. A row the list cannot resolve keeps
    the ISIN it already has: a list rebuilt with a shorter history (or a
    version dropped from it) must not erase what an earlier one established."""
    out = frame.copy()
    resolved = [
        listed.isin(symbol, ex_date, None if pd.isna(isin_nse) else isin_nse)
        for symbol, ex_date, isin_nse in zip(
            out["symbol"], out["ex_date"], out["isin_nse"], strict=True
        )
    ]
    out["isin"] = [
        new if new is not None else (None if pd.isna(old) else old)
        for new, old in zip(resolved, out["isin"], strict=True)
    ]
    return out


# ── Table ───────────────────────────────────────────────────────────────────


def read_table(spec: DatasetSpec) -> pd.DataFrame | None:
    """Every year file as one frame, or None when there are none. Raises on a
    file that is unreadable or lacks a column: the run must not rewrite a
    table it could not read whole."""
    paths = sorted(spec.base_dir.glob(f"{spec.file_prefix}_*.parquet"))
    if not paths:
        return None
    frames = []
    for path in paths:
        frame = pd.read_parquet(path)
        missing = [column for column in COLUMNS if column not in frame.columns]
        if missing:
            raise ValueError(f"{path.name} lacks {', '.join(missing)}")
        frames.append(frame[COLUMNS])
    return _concat([_typed(frame) for frame in frames])


def _action_keys(frame: pd.DataFrame) -> set[tuple[object, ...]]:
    """(ex_date, kind, isin-or-symbol) of each row: an action, whoever listed it."""
    keys: set[tuple[object, ...]] = set()
    for ex_date, kind, isin, symbol in zip(
        frame["ex_date"], frame["kind"], frame["isin"], frame["symbol"], strict=True
    ):
        keys.add((ex_date, kind, "symbol", symbol))
        if isinstance(isin, str):
            keys.add((ex_date, kind, "isin", isin))
    return keys


def _is_listed(frame: pd.DataFrame, keys: set[tuple[object, ...]]) -> pd.Series:
    return pd.Series(
        [
            (ex_date, kind, "symbol", symbol) in keys
            or (isinstance(isin, str) and (ex_date, kind, "isin", isin) in keys)
            for ex_date, kind, isin, symbol in zip(
                frame["ex_date"], frame["kind"], frame["isin"], frame["symbol"], strict=True
            )
        ],
        index=frame.index,
        dtype=bool,
    )


def _concat(frames: list[pd.DataFrame]) -> pd.DataFrame:
    parts = [frame for frame in frames if not frame.empty]
    if not parts:
        return frames[0].iloc[0:0]
    return _typed(pd.concat(parts, ignore_index=True))


def merge(prior: pd.DataFrame | None, fresh: pd.DataFrame) -> pd.DataFrame:
    """`prior` plus what `fresh` adds, both with resolved ISINs. A book-closure
    row seen again keeps its first sighting; a book-closure row gives way to an API
    row for the same action (same ex-date and kind, same ISIN or symbol), and
    a book-closure row for an action already in the table is not added. Two
    actions of one purpose ("DIV-RS30/SPLDIV-RS60") are two rows."""
    held = fresh.iloc[0:0] if prior is None else prior
    new = fresh.drop_duplicates(subset=KEY, keep="first")
    # A book-closure row for an action the table already holds (the same row
    # a day later, the API's wording, or the file's own reworded) is a repeat.
    # An API row is never one: a top-up has retired the window's earlier API
    # rows first (`_retire_window`).
    repeat = (new["source"] == ca.SOURCE_BOOK_CLOSURE) & _is_listed(new, _action_keys(held))
    out = _concat([held, new[~repeat]])
    api = out[out["source"] == ca.SOURCE_API]
    book = out[out["source"] == ca.SOURCE_BOOK_CLOSURE]
    out = _concat([api, book[~_is_listed(book, _action_keys(api))]])
    return out.sort_values(
        ["ex_date", "symbol", "source", "purpose", "part"], kind="mergesort"
    ).reset_index(drop=True)


def write_years(spec: DatasetSpec, table: pd.DataFrame, as_of: date) -> list[int]:
    """Write each ex-date year whose rows changed, stamped `date = as_of`;
    leave the others (and their sha256 on the release) as they are. Returns
    the years written."""
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    years = pd.Series([day.year for day in table["ex_date"]], index=table.index, dtype=int)
    content_schema = pa.schema([field for field in SCHEMA if field.name != "date"])
    written: list[int] = []
    for year in sorted(set(years)):
        rows = table[years == year].reset_index(drop=True)
        path = spec.base_dir / f"{spec.file_prefix}_{year}.parquet"
        new = pa.Table.from_pandas(rows[COLUMNS], schema=content_schema, preserve_index=False)
        if path.exists():
            old = pq.read_table(path)  # type: ignore[no-untyped-call]
            if old.schema.names == SCHEMA.names and old.drop_columns(["date"]).equals(new):
                continue
        stamped = rows[COLUMNS].assign(date=pd.Timestamp(as_of))
        tmp = path.with_suffix(".parquet.tmp")
        stamped.to_parquet(tmp, compression="zstd", index=False, schema=SCHEMA)
        tmp.replace(path)
        written.append(year)
    return written


def _summary(table: pd.DataFrame, written: list[int], unknown: set[str]) -> str:
    without = int(table["isin"].isna().sum())
    text = (
        f"{len(table)} actions; {without} without an ISIN; "
        f"years written: {', '.join(map(str, written)) or 'none'}"
    )
    if unknown:
        named = sorted(unknown)
        more = f" (+{len(named) - 10} more)" if len(named) > 10 else ""
        text += (
            "; purposes not recognised, kept as `other` (extend nse_ca_history.classify): "
            + "; ".join(named[:10])
            + more
        )
    return text


# ── Daily: the book-closure file (CI) ───────────────────────────────────────


def _fetch_bundle(day: date) -> pd.DataFrame:
    """The day's PR bundle (the CI-safe archives host), through the shared
    warm-session + retry contract; a 404 is `NotYetPublished`."""
    session = requests.Session()
    session.headers.update({"User-Agent": _BROWSER_UA})
    return _fetch_with_retry(
        session, nse_corporate_actions.pr_url(day), day, parse=ca.parse_pr_bundle
    )


def _trading_day(day: date) -> bool:
    from pipeline import builders  # the daily run's own calendar

    return builders._cas_trading_day(day)


def _kept(spec: DatasetSpec, target: date, rows: int, why: str) -> RunStatus:
    return RunStatus(
        "skipped_idempotent",
        target,
        symbol_count=rows,
        source=spec.source_label,
        message=f"{why}; retained prior files",
    )


def build_corporate_actions_history(
    spec: DatasetSpec,
    target: date,
    *,
    universe_spec: DatasetSpec,
    fetch_bundle: Callable[[date], pd.DataFrame] = _fetch_bundle,
    trading_day: Callable[[date], bool] = _trading_day,
    catchup_days: int = config.CA_HISTORY_CATCHUP_DAYS,
) -> RunStatus:
    """The daily refresh: add the actions the book-closure files of the last
    `catchup_days` trading days (ending at `target`) list that the table does
    not hold yet, and re-resolve every row's ISIN against today's instrument
    list. The file lists an action from as little as two days before its
    ex-date, so reading the days before covers a run or two that never
    happened; an earlier day's missing bundle is named, not fatal.

    Never seeds the history: with no table yet it skips, naming the backfill.
    Fail-closed, keeping every file as it is: the table unreadable (`failed`),
    the instrument list missing or unreadable, `target`'s bundle missing or
    malformed (`skipped_idempotent`, which the daily run counts as healthy;
    the next run reads this day's bundle again)."""
    try:
        prior = read_table(spec)
    except Exception as e:  # noqa: BLE001 - an unreadable table is never rewritten
        return RunStatus(
            "failed", target, source=spec.source_label, message=f"table unreadable: {e}"
        )
    if prior is None:
        return RunStatus(
            "skipped_idempotent",
            target,
            source=spec.source_label,
            message=f"no history yet; {BACKFILL_HINT}",
        )
    if not trading_day(target):
        return RunStatus(
            "skipped_holiday",
            target,
            symbol_count=len(prior),
            source=spec.source_label,
            message="non-trading day; no book-closure file",
        )
    try:
        listed = InstrumentList.read(universe_spec)
    except Exception as e:  # noqa: BLE001 - no ISINs, no write
        return _kept(spec, target, len(prior), f"instrument list unusable: {e}")
    bundles: list[tuple[date, pd.DataFrame]] = []
    missed: list[str] = []
    for day in _trading_days_to(target, catchup_days, trading_day):
        try:
            bundles.append((day, fetch_bundle(day)))
        except Exception as e:  # noqa: BLE001 - any fetch/parse failure -> fail-closed
            if day == target:
                return _kept(spec, target, len(prior), f"{target.isoformat()} bundle: {e}")
            missed.append(day.isoformat())

    table = resolve_isins(prior, listed)
    unknown: set[str] = set()
    for day, raw in bundles:  # oldest first: the first sighting is kept
        fresh, not_known = expand(raw, first_seen=day)
        unknown |= not_known
        table = merge(table, resolve_isins(fresh, listed))
    written = write_years(spec, table, target)
    added = len(table) - len(prior)
    note = f"; bundles not read: {', '.join(missed)}" if missed else ""
    return RunStatus(
        "success",
        target,
        symbol_count=len(table),
        source=spec.source_label,
        message=f"{added} new from the book-closure files{note}; "
        + _summary(table, written, unknown),
    )


def _trading_days_to(target: date, count: int, trading_day: Callable[[date], bool]) -> list[date]:
    """The `count` trading days ending at `target`, oldest first."""
    days: list[date] = []
    day = target
    while len(days) < count and (target - day).days < count * 3 + 14:
        if trading_day(day):
            days.append(day)
        day -= timedelta(days=1)
    return sorted(days)


# ── Backfill / top-up: NSE's API (residential) ──────────────────────────────


def month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """[start, end] cut at month ends."""
    windows: list[tuple[date, date]] = []
    day = start
    while day <= end:
        next_month = (day.replace(day=28) + timedelta(days=4)).replace(day=1)
        windows.append((day, min(end, next_month - timedelta(days=1))))
        day = next_month
    return windows


class ApiClient:
    """NSE's corporate-actions API from a residential machine: one session
    whose cookies come from visiting the page the API serves, requests spaced
    `spacing` seconds apart, a 401/403 answered by one fresh visit."""

    def __init__(
        self,
        session: requests.Session | None = None,
        *,
        spacing: float = config.CA_HISTORY_REQUEST_SPACING_S,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._session = session or requests.Session()
        self._session.headers.update(
            {"User-Agent": _BROWSER_UA, "Accept": "application/json", "Referer": ca.API_REFERER}
        )
        self._spacing = spacing
        self._sleep = sleep
        self._warm = False

    def _visit(self) -> None:
        self._session.get(ca.API_REFERER, timeout=30)
        self._warm = True

    def get(self, index: str, start: date, end: date) -> bytes:
        if not self._warm:
            self._visit()
        self._sleep(self._spacing)
        url = ca.api_url(index, start, end)
        response = self._session.get(url, timeout=30)
        if response.status_code in (401, 403):
            self._visit()
            self._sleep(self._spacing)
            response = self._session.get(url, timeout=30)
        if response.status_code != 200:
            raise ca.MalformedCorporateActions(f"HTTP {response.status_code} for {url}")
        return response.content


def fetch_api(
    windows: Iterable[tuple[date, date]],
    *,
    today: date,
    get: Callable[[str, date, date], bytes],
    indexes: tuple[str, ...] = (ca.INDEX_EQUITIES, ca.INDEX_SME),
) -> pd.DataFrame:
    """Every window of every index, parsed; raises on the first one that
    fails. A month of the main board that has ended with no action at all is
    a broken answer, not a quiet month (the quietest since 2021 had 27)."""
    frames: list[pd.DataFrame] = []
    for start, end in windows:
        for index in indexes:
            frame = ca.parse_api_payload(get(index, start, end), index=index, start=start, end=end)
            whole_month_past = end < today and (end - start).days >= 27
            if index == ca.INDEX_EQUITIES and frame.empty and whole_month_past:
                raise ca.MalformedCorporateActions(f"no {index} action in {start}..{end}")
            frames.append(frame)
    parts = [frame for frame in frames if not frame.empty]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=ca.RAW_COLUMNS)


def backfill_corporate_actions_history(
    spec: DatasetSpec,
    *,
    universe_spec: DatasetSpec,
    start: date,
    end: date,
    today: date,
    get: Callable[[str, date, date], bytes],
    indexes: tuple[str, ...] = (ca.INDEX_EQUITIES, ca.INDEX_SME),
) -> RunStatus:
    """Fetch NSE's API for [start, end] (equities and SME, by month) and fold
    it into the table: the first backfill, and each residential top-up.

    Within the window the API is the record: for the series its indexes
    answer for, its current rows replace the table's earlier API rows (a
    corrected purpose replaces the old one, a withdrawn action goes; a row
    NSE left as it was keeps its first_seen), and a book-closure row whose
    ex-date has passed goes unless the API lists it. A book-closure row whose
    ex-date is still ahead stays (the API lists only a day or so ahead).
    `_retire_window` refuses a window whose API rows would shrink by more
    than a twentieth: a truncated answer, not withdrawals.

    Every window must parse before anything is written. Needs the instrument
    list locally (`sync` first)."""
    try:
        listed = InstrumentList.read(universe_spec)
    except Exception as e:  # noqa: BLE001
        return RunStatus(
            "failed", today, source=spec.source_label,
            message=f"instrument list unusable ({e}); run `python -m pipeline sync` first",
        )
    try:
        prior = read_table(spec)
        raw = fetch_api(month_windows(start, end), today=today, get=get, indexes=indexes)
    except Exception as e:  # noqa: BLE001 - nothing written on any failure
        return RunStatus("failed", today, source=spec.source_label, message=str(e))

    fresh, unknown = expand(raw, first_seen=today)
    series = frozenset().union(*(ca.INDEX_SERIES[index] for index in indexes))
    retired = 0
    if prior is not None:
        try:
            prior, fresh, retired = _retire_window(
                prior, fresh, start=start, end=end, today=today, series=series
            )
        except ValueError as e:
            return RunStatus("failed", today, source=spec.source_label, message=str(e))
    resolved_prior = None if prior is None else resolve_isins(prior, listed)
    table = merge(resolved_prior, resolve_isins(fresh, listed))
    written = write_years(spec, table, today)
    return RunStatus(
        "success",
        today,
        symbol_count=len(table),
        source=spec.source_label,
        message=(
            f"{len(raw)} API rows for {start}..{end}; {retired} rows replaced or withdrawn; "
            + _summary(table, written, unknown)
        ),
    )


def _retire_window(
    prior: pd.DataFrame,
    fresh: pd.DataFrame,
    *,
    start: date,
    end: date,
    today: date,
    series: frozenset[str],
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """`prior` without the rows the fetched window now answers for, `fresh`
    with the first_seen of each row NSE left unchanged, and how many of the
    earlier API rows the window no longer lists. Raises `ValueError` when a
    month's API rows would shrink by more than a twentieth (and 3 rows)."""
    in_window = pd.Series(
        [start <= day <= end for day in prior["ex_date"]], index=prior.index, dtype=bool
    )
    scope = in_window & prior["series"].isin(series)
    old_api = prior[scope & (prior["source"] == ca.SOURCE_API)]
    for month_start, month_end in month_windows(start, end):
        held = sum(month_start <= day <= month_end for day in old_api["ex_date"])
        listed = sum(month_start <= day <= month_end for day in fresh["ex_date"])
        if held - listed > max(3, held // 20):
            raise ValueError(
                f"the API lists {listed} rows for {month_start}..{month_end} where the table "
                f"holds {held}; refusing to drop more than a twentieth (a truncated answer? "
                "re-run; RUNBOOK 'Corporate actions history')"
            )
    first_seen = dict(
        zip(old_api[KEY].itertuples(index=False, name=None), old_api["first_seen"], strict=True)
    )
    fresh = fresh.assign(
        first_seen=[
            first_seen.get(key, day)
            for key, day in zip(
                fresh[KEY].itertuples(index=False, name=None), fresh["first_seen"], strict=True
            )
        ]
    )
    fresh_keys = set(fresh[KEY].itertuples(index=False, name=None))
    retired = sum(key not in fresh_keys for key in first_seen)
    passed = pd.Series([day < today for day in prior["ex_date"]], index=prior.index, dtype=bool)
    stale_book = scope & (prior["source"] == ca.SOURCE_BOOK_CLOSURE) & passed
    kept = prior[~(scope & (prior["source"] == ca.SOURCE_API)) & ~stale_book]
    return kept.reset_index(drop=True), fresh, retired
