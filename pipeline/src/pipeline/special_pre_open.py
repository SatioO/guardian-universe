"""special_pre_open: the securities NSE opens through a Special Pre-Open
Session (SPOS), keyed by the session they open in.

The desktop app settles each instrument's opening-auction price at the end of
its own call auction. A security in SPOS (an IPO's first day, a relisting, a
restructuring's ex-date) runs 09:00-10:00 instead of 09:00-09:15, so the app
must know the list on the morning itself. NSE's security master for day D,
posted in the evening of D, flags them for the next session
(sources/nse_cm_security.py).

One table, `special_pre_open_all.parquet`, accumulated by `session_date`:

* each run reads the file for its own trading day and replaces that file's
  session wholesale, so an evening correction of the same file wins;
* other sessions' rows stay, so the table is the record of every SPOS seen;
* `date`, which the manifest reads, is the newest session any file has
  covered. A session with no SPOS security has no rows, so `date` is how a
  reader tells "none tomorrow" from "not built yet": a list whose `date` is
  before today says nothing about today.

A first build (no prior file) seeds the files of the `seed_days` trading days
before the target, best effort, the same as corporate_actions.

Fail-closed, keeping the prior file:
* the target's file not posted yet (HTTP 404) is `not_yet`, so a later rung
  of the same evening retries;
* a malformed or truncated file, or a CallAuctnInd value never seen before, is
  `failed`: the job turns red and nothing is written.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import requests

from pipeline import calendar as cal
from pipeline import config
from pipeline.daily_update import RunStatus
from pipeline.datasets import DatasetSpec
from pipeline.errors import NotYetPublished
from pipeline.fetch import _BROWSER_UA, _fetch_with_retry
from pipeline.sources import nse_cm_security
from pipeline.sources.nse_cm_security import SecurityMaster

COLUMNS: list[str] = [
    "session_date",
    "symbol",
    "series",
    "isin",
    "kind",
    "base_price",
    "file_date",
    "source",
    "date",
]
# The types the client reads.
SCHEMA = pa.schema(
    [
        ("session_date", pa.date32()),
        ("symbol", pa.string()),
        ("series", pa.string()),
        ("isin", pa.string()),
        ("kind", pa.string()),
        ("base_price", pa.float64()),
        ("file_date", pa.date32()),
        ("source", pa.string()),
        ("date", pa.timestamp("ms")),
    ]
)
_KEY = ["session_date", "symbol", "series"]

# A session can follow its file by a weekend and two holidays.
_NEXT_SESSION_HORIZON_DAYS = 14


def _trading_day(day: date) -> bool:
    """The daily run's own calendar (META_DIR/market_calendar.json)."""
    holidays, special = cal.load_trading_calendar(config.META_DIR)
    return cal.is_trading_day(day, holidays, special_sessions=special)


def _fetch_security_master(day: date) -> SecurityMaster:
    """The security master for `day`, through the shared warm-session + retry
    contract. A 404 is `NotYetPublished`; the parser is the gate against any
    other document served with 200."""
    parsed: list[SecurityMaster] = []

    def parse(payload: bytes) -> pd.DataFrame:
        master = nse_cm_security.parse_security_master(payload)
        parsed.append(master)
        return master.sessions

    with requests.Session() as session:
        session.headers.update({"User-Agent": _BROWSER_UA})
        _fetch_with_retry(session, nse_cm_security.security_url(day), day, parse=parse)
    return parsed[-1]


def next_session(day: date, trading_day: Callable[[date], bool]) -> date | None:
    """The first trading day after `day`, or None within the horizon."""
    for ahead in range(1, _NEXT_SESSION_HORIZON_DAYS + 1):
        candidate = day + timedelta(days=ahead)
        if trading_day(candidate):
            return candidate
    return None


def _trading_days_back(target: date, count: int, trading_day: Callable[[date], bool]) -> list[date]:
    """The `count` trading days ending at `target`, oldest first."""
    days: list[date] = []
    day = target
    while len(days) < count and (target - day).days < count * 3 + 14:
        if trading_day(day):
            days.append(day)
        day -= timedelta(days=1)
    return sorted(days)


def _read_prior(out_path: Path) -> pd.DataFrame | None:
    if not out_path.exists():
        return None
    return pd.read_parquet(out_path)


def _covered_through(prior: pd.DataFrame | None) -> date | None:
    if prior is None or prior.empty:
        return None
    latest = prior["date"].max()
    return None if pd.isna(latest) else pd.Timestamp(latest).date()


def build_special_pre_open(
    spec: DatasetSpec,
    target: date,
    *,
    fetch_master: Callable[[date], SecurityMaster] = _fetch_security_master,
    trading_day: Callable[[date], bool] = _trading_day,
    seed_days: int = config.SPECIAL_PRE_OPEN_SEED_DAYS,
    min_securities: int = config.SPECIAL_PRE_OPEN_MIN_SECURITIES,
) -> RunStatus:
    """Add the session after `target` to `special_pre_open_all.parquet`."""
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"
    try:
        prior = _read_prior(out_path)
    except Exception as e:  # noqa: BLE001 - an unreadable prior is never overwritten blind
        return RunStatus("failed", target, source=spec.source_label, message=f"prior file: {e}")
    if prior is not None and not set(COLUMNS) <= set(prior.columns):
        return RunStatus(
            "failed",
            target,
            source=spec.source_label,
            message="prior file lacks the special_pre_open columns",
        )
    prior_rows = 0 if prior is None else len(prior)

    if not trading_day(target):
        return RunStatus(
            "skipped_holiday",
            target,
            symbol_count=prior_rows,
            source=spec.source_label,
            message="non-trading day; no security master",
        )

    days = [target] if prior is not None else _trading_days_back(target, seed_days, trading_day)
    found: list[pd.DataFrame] = []
    sessions: list[date] = []
    seed_misses: list[str] = []
    for day in days:
        try:
            master = fetch_master(day)
            if master.securities < min_securities:
                raise nse_cm_security.MalformedSecurityFile(
                    f"{master.securities} securities (under {min_securities}: truncated?)"
                )
        except NotYetPublished as e:
            # A first build still publishes the sessions its seed days cover.
            if day == target and prior is not None:
                return RunStatus(
                    "not_yet",
                    target,
                    symbol_count=prior_rows,
                    source=spec.source_label,
                    message=(
                        f"{day.isoformat()} security master not posted yet ({e}); "
                        "kept the prior list"
                    ),
                )
            seed_misses.append(day.isoformat())
            continue
        except Exception as e:  # noqa: BLE001 - any fetch/parse failure fails closed
            if day == target:
                return RunStatus(
                    "failed",
                    target,
                    symbol_count=prior_rows,
                    source=spec.source_label,
                    message=f"{day.isoformat()} security master: {e}; kept the prior list",
                )
            seed_misses.append(day.isoformat())
            continue
        session = next_session(day, trading_day)
        if session is None:
            return RunStatus(
                "failed",
                target,
                symbol_count=prior_rows,
                source=spec.source_label,
                message=(
                    f"no trading day within {_NEXT_SESSION_HORIZON_DAYS} days "
                    f"of {day.isoformat()}"
                ),
            )
        sessions.append(session)
        found.append(
            master.sessions.assign(
                session_date=session, file_date=day, source=nse_cm_security.SOURCE
            )
        )

    if not sessions:
        return RunStatus(
            "failed", target, source=spec.source_label, message="no security master could be read"
        )

    parts: list[pd.DataFrame] = []
    if prior is not None:
        kept = prior[COLUMNS[:-1]].copy()
        kept["session_date"] = pd.to_datetime(kept["session_date"]).dt.date
        kept["file_date"] = pd.to_datetime(kept["file_date"]).dt.date
        # This run's sessions are replaced wholesale: the newest file wins.
        parts.append(kept[~kept["session_date"].isin(sessions)])
    parts.extend(frame for frame in found if not frame.empty)
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=COLUMNS[:-1])
    out = out.drop_duplicates(subset=_KEY, keep="last")
    out = out.sort_values(_KEY, kind="mergesort").reset_index(drop=True)
    covered = max(sessions)
    prior_covered = _covered_through(prior)
    if prior_covered is not None and prior_covered > covered:
        covered = prior_covered
    out["date"] = pd.Timestamp(covered)
    out = out[COLUMNS]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".parquet.tmp")
    out.to_parquet(tmp, compression="zstd", index=False, schema=SCHEMA)
    tmp.replace(out_path)

    newest = max(sessions)
    upcoming = out[out["session_date"] == newest]
    named = ", ".join(f"{r.symbol} ({r.kind})" for r in upcoming.itertuples()) or "none"
    summary = f"{newest.isoformat()}: {len(upcoming)} in special pre-open: {named}"
    if seed_misses:
        summary += f"; seed days without a file: {', '.join(seed_misses)}"
    return RunStatus(
        "success", target, symbol_count=len(out), source=spec.source_label, message=summary
    )
