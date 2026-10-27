"""Registry of derived-dataset builders, keyed by DatasetSpec.key.

This module intentionally imports daily_update.RunStatus and datasets.DatasetSpec
only for typing -- builders themselves must stay name-free (no hardcoded
dataset-key lookups); the CLI is the allowed edge that resolves specs by name
and passes them in.

Builder functions that need a *source* spec (e.g. build_reference reads the
equities store) take it as a keyword-only `source_spec` argument rather than
looking it up by name -- the CLI resolves `DATASETS[DATASET_ORDER[0]]` and
binds it via `functools.partial` when it populates BUILDERS, so this module
never hardcodes "equities" (or any other dataset key) anywhere. The bound
partial still satisfies BUILDERS' `Callable[[DatasetSpec, date], RunStatus]`
signature -- `source_spec` is filled in, leaving exactly the two positional
parameters (`spec`, `target`) the registry and `_run_builder` expect.

WARNING (carried in from the task-6/task-7 review): the `cli.py` BUILDERS
bindings (e.g. `BUILDERS["reference"]`) bind `source_spec`
to the REAL registry spec (`datasets.DATASETS[datasets.DATASET_ORDER[0]]`) at
CLI *import time* -- not at call time. Monkeypatching `datasets.DATASETS`
alone in a test does NOT redirect the `source_spec` a bound partial already
captured; the partial keeps pointing at whatever spec was live when
`pipeline.cli` was first imported. Tests that want a real builder run against
tmp dirs via the registered `BUILDERS` entries must monkeypatch
`cli.builders.BUILDERS` directly (e.g. replace the dict entry with a fresh
`functools.partial(build_x, source_spec=<tmp-scoped spec>)`), not
`datasets.DATASETS`.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import replace as _dc_replace
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipeline import calendar as cal
from pipeline import config, market_calendar
from pipeline.daily_update import RunStatus
from pipeline.datasets import DatasetSpec
from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.fetch import _BROWSER_UA, _MAX_RETRIES, _TIMEOUT, _fetch_with_retry
from pipeline.sources import (
    bse_cm_bhavcopy,
    bse_eqd_contract,
    classification_publication,
    nse_constituents,
    nse_corporate_actions,
    nse_fo_contract,
    nse_fo_mktlots,
    nse_index_announcements,
    nse_sector,
)
from pipeline.sources.classification_registry import ClassificationRegistryRecord
from pipeline.sources.nse_universe import (
    RegistryEntry,
    RegistryStatus,
    active_classification_records,
    load_registry,
    write_registry,
)

BUILDERS: dict[str, Callable[[DatasetSpec, date], RunStatus]] = {}

# A version counts as `active` when its last_seen falls among the 10 most
# recent trading sessions (of the daily run's own calendar) ending at the
# newest last_seen in the list.
_ACTIVE_WINDOW = 10

_REFERENCE_COLUMNS = ["date", "instrument_key", "isin", "symbol", "series"]
_REFERENCE_KEY = ["instrument_key", "symbol", "series"]
# What a prior list must carry to be usable. `nse_token` is NOT required: the
# list published before it existed is a valid prior, and gains the column here.
_REFERENCE_VERSION_COLUMNS = [*_REFERENCE_KEY, "first_seen", "last_seen", "isin"]
_REFERENCE_MERGE_COLUMNS = [*_REFERENCE_VERSION_COLUMNS, "nse_token"]
_REFERENCE_OUTPUT_COLUMNS = [
    "instrument_key",
    "isin",
    "symbol",
    "name",
    "series",
    "first_seen",
    "last_seen",
    "status",
    "valid_from",
    "valid_to",
    "date",
    # Appended last so no existing column moves. NSE's exchange-issued id
    # (UDiFF FinInstrmId) of this version: a series change issues a new one.
    # Null where no source file carried it (history before 2026-10-14,
    # fallback-source days).
    "nse_token",
]
_REFERENCE_BOOTSTRAP_HINT = (
    "run `python -m pipeline bootstrap-reference` (RUNBOOK.md 'Reference bootstrap')"
)

_LEGACY_V1_SECTOR_COLUMNS = frozenset(
    {
        "instrument_key",
        "symbol",
        "sector",
        "industry",
        "basic_industry",
        "is_cyclical",
        "date",
    }
)
_CURRENT_V2_SECTOR_COLUMNS = frozenset([*nse_sector.SECTOR_COLUMNS, "date"])


@dataclass(frozen=True)
class _PriorClassificationArtifact:
    records: dict[str, ClassificationRegistryRecord]
    legacy_missing_macro_sector: bool
    error: str | None = None


def _read_all_years(source_spec: DatasetSpec) -> pd.DataFrame:
    """Column-pruned read of every `{prefix}_{year}.parquet` file under the
    source spec's base_dir. Missing/empty store -> empty frame (a legitimate
    state, e.g. a run that fetched no session)."""
    base = source_spec.base_dir
    if not base.exists():
        return _with_nse_token(pd.DataFrame(columns=_REFERENCE_COLUMNS))
    frames = [
        _with_nse_token(
            pd.read_parquet(p, columns=[*_REFERENCE_COLUMNS, *_present_token_column(p)])
        )
        for p in sorted(base.glob(f"{source_spec.file_prefix}_*.parquet"))
    ]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return _with_nse_token(pd.DataFrame(columns=_REFERENCE_COLUMNS))
    return pd.concat(frames, ignore_index=True)


def _present_token_column(parquet: Path) -> list[str]:
    """`["nse_token"]` when the file has it, else `[]`: the price files left
    on a release (the bootstrap's source) predate the column."""
    names = pq.ParquetFile(parquet).schema_arrow.names  # type: ignore[no-untyped-call]
    return ["nse_token"] if "nse_token" in names else []


def _with_nse_token(frame: pd.DataFrame) -> pd.DataFrame:
    """`frame` with `nse_token` as nullable Int64: added all-null when absent."""
    out = frame.copy()
    if "nse_token" not in out.columns:
        out["nse_token"] = pd.array([pd.NA] * len(out), dtype="Int64")
    out["nse_token"] = out["nse_token"].astype("Int64")
    return out


def _reference_calendar() -> tuple[set[date], set[date]]:
    """(holidays, special_sessions): the daily run's own NSE calendar."""
    return cal.load_trading_calendar(config.META_DIR)


def _reference_versions(sessions: pd.DataFrame) -> pd.DataFrame:
    """One row per distinct `(instrument_key, symbol, series)` version in the
    stored sessions, with the first and last session it traded in."""
    return sessions.groupby(_REFERENCE_KEY, as_index=False).agg(
        first_seen=("date", "min"), last_seen=("date", "max"), isin=("isin", "last"),
        nse_token=("nse_token", "last"),
    )


def _merge_versions(prior: pd.DataFrame, fresh: pd.DataFrame) -> pd.DataFrame:
    """Fold freshly seen versions into the prior list: earliest first_seen,
    latest last_seen, and the fresh ISIN and NSE token where both have one (a
    fresh null never erases a recorded token: `last` skips nulls)."""
    both = pd.concat(
        [
            _with_nse_token(prior)[_REFERENCE_MERGE_COLUMNS],
            _with_nse_token(fresh)[_REFERENCE_MERGE_COLUMNS],
        ],
        ignore_index=True,
    )
    return both.groupby(_REFERENCE_KEY, as_index=False).agg(
        first_seen=("first_seen", "min"), last_seen=("last_seen", "max"),
        isin=("isin", "last"), nse_token=("nse_token", "last"),
    )


def _shape_reference(
    versions: pd.DataFrame, trading_calendar: Callable[[], tuple[set[date], set[date]]]
) -> pd.DataFrame:
    holidays, special = trading_calendar()
    latest = pd.Timestamp(versions["last_seen"].max()).date()
    active_dates = {
        pd.Timestamp(d) for d in cal.trading_days_back(latest, _ACTIVE_WINDOW, holidays, special)
    }
    out = versions.copy()
    out["name"] = out["symbol"]
    out["status"] = out["last_seen"].apply(
        lambda d: "active" if d in active_dates else "inactive"
    )
    out["valid_from"] = out["first_seen"]
    out["valid_to"] = out["last_seen"]
    out["date"] = out["last_seen"]
    return (
        out[_REFERENCE_OUTPUT_COLUMNS]
        .sort_values(["instrument_key", "first_seen"])
        .reset_index(drop=True)
    )


def _reference_prior(out_path: Path) -> tuple[pd.DataFrame | None, str]:
    """The prior published instrument list, or None and why it is unusable.

    Only its shape is judged here (readable, the version columns present,
    non-empty, dated). Whether it is complete is judged against what the run
    fetched, in build_reference."""
    if not out_path.exists():
        return None, f"no prior instrument list at {out_path}"
    try:
        prior = pd.read_parquet(out_path)
    except Exception as e:  # noqa: BLE001 - an unreadable prior is unusable, not a crash
        return None, f"prior instrument list unreadable: {e}"
    missing = [c for c in _REFERENCE_VERSION_COLUMNS if c not in prior.columns]
    if missing:
        return None, f"prior instrument list lacks columns {missing}"
    if prior.empty:
        return None, "prior instrument list is empty"
    if prior[["first_seen", "last_seen"]].isna().any().any():
        return None, "prior instrument list has undated rows"
    return prior, ""


def build_reference(
    spec: DatasetSpec,
    target: date,
    *,
    source_spec: DatasetSpec,
    trading_calendar: Callable[[], tuple[set[date], set[date]]] = _reference_calendar,
    max_new_share: float = config.REFERENCE_PRIOR_MAX_NEW_SHARE,
) -> RunStatus:
    """Update the `reference/instruments` SCD2 symbol master: one row per
    distinct `(instrument_key, symbol, series)` version, with the sessions it
    first and last traded in.

    Incremental since 2026-10-13. The equities store no longer outlives a run
    (no price history is published or synced), so it holds only the sessions
    this run fetched (its catch-up window). The full history lives in the
    prior published `instruments_all.parquet`, which sync brings down: this
    merges the run's sessions into it. Same columns, same rows as the old
    rebuild from the whole price history (RUNBOOK.md 'Reference bootstrap').

    Fail-closed on the prior, because a list built from the run's sessions
    alone would drop every listing that did not trade in the window and reset
    every first_seen:
      - a missing, unreadable, mis-shaped, empty or undated prior is `failed`
        and nothing is written (the bootstrap rebuilds it from price files);
      - a prior that lacks more than `max_new_share` of the versions this run
        fetched is truncated or broken, not listing churn: `failed`, nothing
        written, so a bad file never compounds day after day.
    A run that fetched no session keeps the prior untouched.

    `active` means last_seen falls among the latest `_ACTIVE_WINDOW` trading
    sessions of the run's calendar, ending at the newest last_seen in the
    list. `suspended`/`delisted` need an exchange feed and are not emitted.
    Full rewrite each run, atomically (tmp+rename).
    """
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"

    prior, why = _reference_prior(out_path)
    if prior is None:
        return RunStatus(
            "failed", target, source="derived",
            message=f"{why}; refusing to build it from this run's sessions alone -- "
            f"{_REFERENCE_BOOTSTRAP_HINT}",
        )

    sessions = _read_all_years(source_spec)
    if sessions.empty:
        return RunStatus(
            "skipped_idempotent", target, symbol_count=len(prior), source="derived",
            message="no session fetched this run; prior instrument list kept",
        )

    fresh = _reference_versions(sessions)
    known = pd.MultiIndex.from_frame(prior[_REFERENCE_KEY])
    unseen = int((~pd.MultiIndex.from_frame(fresh[_REFERENCE_KEY]).isin(known)).sum())
    if unseen > max_new_share * len(fresh):
        return RunStatus(
            "failed", target, symbol_count=len(prior), source="derived",
            message=(
                f"prior instrument list lacks {unseen} of the {len(fresh)} versions "
                f"this run fetched (over the {max_new_share:.0%} limit): truncated or "
                f"broken; refusing to merge -- {_REFERENCE_BOOTSTRAP_HINT}"
            ),
        )

    out = _shape_reference(_merge_versions(prior, fresh), trading_calendar)
    _write_atomic(out, out_path)
    return RunStatus(
        "success", target, symbol_count=len(out), source="derived",
        message=f"{unseen} new versions",
    )


def rebuild_reference(
    spec: DatasetSpec,
    target: date,
    *,
    source_spec: DatasetSpec,
    keep_prior: bool = True,
    trading_calendar: Callable[[], tuple[set[date], set[date]]] = _reference_calendar,
) -> RunStatus:
    """Rebuild the symbol master from EVERY session in the source store: the
    one-off bootstrap and rollback path (`pipeline bootstrap-reference`, which
    fills the store from the price files left on a release).

    With `keep_prior`, a well-shaped prior instrument list is merged in too,
    so versions first seen after the price files end are kept; pass False
    when the prior itself is what needs replacing. An empty source store is
    `failed`, never an empty list.
    """
    sessions = _read_all_years(source_spec)
    if sessions.empty:
        return RunStatus(
            "failed", target, source="derived",
            message=f"no stored sessions under {source_spec.base_dir}",
        )
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"

    versions = _reference_versions(sessions)
    note = (
        f"rebuilt from {sessions['date'].nunique()} sessions "
        f"({pd.Timestamp(sessions['date'].min()).date()} to "
        f"{pd.Timestamp(sessions['date'].max()).date()})"
    )
    if keep_prior:
        prior, why = _reference_prior(out_path)
        if prior is None:
            note += f"; no prior merged ({why})"
        else:
            versions = _merge_versions(prior, versions)
            note += f"; merged the prior list ({len(prior)} rows)"

    out = _shape_reference(versions, trading_calendar)
    _write_atomic(out, out_path)
    return RunStatus("success", target, symbol_count=len(out), source="derived", message=note)


# ── BSE instrument list ─────────────────────────────────────────────────────
# `bse_instruments_all.parquet`: the BSE twin of `instruments_all.parquet`,
# one row per BSE listing version with its scrip code and ISIN, so the desktop
# app can read a BSE scrip code (the exchange token Kite and Dhan report) as
# an alias of the NSE listing with the same ISIN.

# A listing version: a change of ISIN, scrip id or group starts a new row,
# as a change of series does on NSE's list. The scrip code is BSE's identity
# and stays across them.
_BSE_REFERENCE_KEY = ["scrip_code", "isin", "symbol", "group"]
_BSE_REFERENCE_VERSION_COLUMNS = [*_BSE_REFERENCE_KEY, "name", "first_seen", "last_seen"]
_BSE_REFERENCE_COLUMNS = [
    "scrip_code",
    "isin",
    "symbol",
    "name",
    "group",
    "status",
    "first_seen",
    "last_seen",
    "valid_from",
    "valid_to",
    "date",
]
# instruments_all's types (its dates are timestamp[ns]), pinned.
_BSE_REFERENCE_SCHEMA = pa.schema(
    [
        ("scrip_code", pa.int64()),
        ("isin", pa.string()),
        ("symbol", pa.string()),
        ("name", pa.string()),
        ("group", pa.string()),
        ("status", pa.string()),
        ("first_seen", pa.timestamp("ns")),
        ("last_seen", pa.timestamp("ns")),
        ("valid_from", pa.timestamp("ns")),
        ("valid_to", pa.timestamp("ns")),
        ("date", pa.timestamp("ns")),
    ]
)
_BSE_REFERENCE_BACKFILL_HINT = (
    "run `python -m pipeline backfill-bse-instruments --days N` "
    "(RUNBOOK.md 'BSE instrument list')"
)


def bse_calendar() -> tuple[set[date], set[date]]:
    """(holidays, special_sessions) of BSE, from the one market calendar."""
    return market_calendar.load(config.META_DIR / market_calendar.FILENAME).trading_inputs("BSE")


def _fetch_bse_bhavcopy(day: date) -> bse_cm_bhavcopy.BhavcopyDay:
    """BSE's bhavcopy for `day`, parsed. bseindia.com refuses python-requests'
    own User-Agent (403) and serves the file to a browser's. A 404, or BSE's
    HTML page served with 200, is `NotYetPublished`; any other status is
    retried, then raised."""
    last = ""
    with requests.Session() as session:
        session.headers.update({"User-Agent": _BROWSER_UA})
        for attempt in range(_MAX_RETRIES):
            resp = session.get(bse_cm_bhavcopy.bhavcopy_url(day), timeout=_TIMEOUT)
            if resp.status_code == 404:
                raise NotYetPublished(f"404 for BSE bhavcopy {day.isoformat()}")
            if resp.status_code == 200:
                return bse_cm_bhavcopy.parse_bhavcopy(resp.content, day)
            last = f"HTTP {resp.status_code}"
            if attempt < _MAX_RETRIES - 1:
                time.sleep(2**attempt)
    raise UnexpectedFailure(f"BSE bhavcopy {day.isoformat()}: {last}")


def _bse_reference_prior(out_path: Path) -> tuple[pd.DataFrame | None, str]:
    """The published BSE list, or None. A reason comes back only for a file
    that exists and is unusable: the run then refuses rather than seed over
    it (no file at all is a first build)."""
    if not out_path.exists():
        return None, ""
    try:
        prior = pd.read_parquet(out_path)
    except Exception as e:  # noqa: BLE001 - an unreadable prior is unusable, not a crash
        return None, f"prior BSE instrument list unreadable: {e}"
    missing = [c for c in _BSE_REFERENCE_VERSION_COLUMNS if c not in prior.columns]
    if missing:
        return None, f"prior BSE instrument list lacks columns {missing}"
    if prior.empty:
        return None, "prior BSE instrument list is empty"
    if prior[["first_seen", "last_seen"]].isna().any().any():
        return None, "prior BSE instrument list has undated rows"
    return prior, ""


def _bse_versions(frame: pd.DataFrame) -> pd.DataFrame:
    """One row per version in `frame` (day rows or versions): the earliest
    first_seen, the latest last_seen, and the name of the latest sighting.
    A null ISIN or group is part of the key, not dropped."""
    frame = frame.sort_values("last_seen", kind="mergesort")
    return frame.groupby(_BSE_REFERENCE_KEY, as_index=False, dropna=False).agg(
        name=("name", "last"), first_seen=("first_seen", "min"), last_seen=("last_seen", "max")
    )


def _shape_bse_reference(
    versions: pd.DataFrame, holidays: set[date], special: set[date]
) -> pd.DataFrame:
    """instruments_all's rules: `active` when last_seen falls among the latest
    `_ACTIVE_WINDOW` sessions ending at the newest last_seen in the list;
    valid_from/valid_to are first_seen/last_seen; `date` is last_seen."""
    latest = pd.Timestamp(versions["last_seen"].max()).date()
    active_dates = {
        pd.Timestamp(d) for d in cal.trading_days_back(latest, _ACTIVE_WINDOW, holidays, special)
    }
    out = versions.copy()
    out["status"] = out["last_seen"].apply(lambda d: "active" if d in active_dates else "inactive")
    out["valid_from"] = out["first_seen"]
    out["valid_to"] = out["last_seen"]
    out["date"] = out["last_seen"]
    out["scrip_code"] = out["scrip_code"].astype("int64")
    return (
        out[_BSE_REFERENCE_COLUMNS]
        .sort_values(["scrip_code", "first_seen", "last_seen"], kind="mergesort")
        .reset_index(drop=True)
    )


def build_bse_reference(
    spec: DatasetSpec,
    target: date,
    *,
    fetch_day: Callable[[date], bse_cm_bhavcopy.BhavcopyDay] = _fetch_bse_bhavcopy,
    trading_calendar: Callable[[], tuple[set[date], set[date]]] = bse_calendar,
    days: list[date] | None = None,
    seed_days: int = config.BSE_REFERENCE_SEED_DAYS,
    max_catchup_days: int = config.BSE_REFERENCE_MAX_CATCHUP_DAYS,
    min_rows: int = config.BSE_BHAVCOPY_MIN_ROWS,
    max_new_share: float = config.REFERENCE_PRIOR_MAX_NEW_SHARE,
) -> RunStatus:
    """Update `bse_instruments_all.parquet` from BSE's daily bhavcopies: one
    row per `(scrip_code, isin, symbol, group)` version, with the sessions it
    first and last traded in, merged onto the published list like
    `build_reference` merges NSE's sessions.

    Which days it reads (BSE's own calendar):
      - daily (`days` None): every BSE session after the list's newest
        last_seen, up to `target`, so a day the producer did not run is read
        on the next run; more than `max_catchup_days` behind fails (backfill);
      - first build (no published list): the `seed_days` sessions ending at
        `target`, so first_seen starts there;
      - `days` given (`backfill-bse-instruments`): those days, merged in
        again; a day BSE did not post is skipped and named.

    Days are read oldest first and merged only up to the first that cannot
    be read, so the list's newest last_seen never moves past a hole: the
    target day not posted yet is `not_yet`, an earlier day is `failed`, and
    either way the next run reads from there. A file under `min_rows` rows
    counts as unreadable (truncated).

    Fail-closed on the prior, as build_reference: an unusable published list
    is `failed` and nothing is written, and one that lacks more than
    `max_new_share` of the versions read is truncated, not churn.
    """
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"
    source = bse_cm_bhavcopy.SOURCE

    prior, why = _bse_reference_prior(out_path)
    if why:
        return RunStatus(
            "failed", target, source=source,
            message=f"{why}; nothing written -- {_BSE_REFERENCE_BACKFILL_HINT}",
        )
    prior_rows = 0 if prior is None else len(prior)
    holidays, special = trading_calendar()

    backfill = days is not None
    if days is None:
        if not cal.is_trading_day(target, holidays, special_sessions=special):
            return RunStatus(
                "skipped_holiday", target, symbol_count=prior_rows, source=source,
                message="not a BSE trading day",
            )
        if prior is None:
            days = cal.trading_days_back(target, seed_days, holidays, special)
        else:
            newest = pd.Timestamp(prior["last_seen"].max()).date()
            days = []
            day = target
            while day > newest:
                if len(days) == max_catchup_days:
                    return RunStatus(
                        "failed", target, symbol_count=prior_rows, source=source,
                        message=(
                            f"list is more than {max_catchup_days} BSE sessions behind "
                            f"(newest {newest.isoformat()}); nothing written -- "
                            f"{_BSE_REFERENCE_BACKFILL_HINT}"
                        ),
                    )
                days.append(day)
                day = cal.previous_trading_day(day, holidays, special)
            days.sort()
            if not days:
                return RunStatus(
                    "skipped_idempotent", target, symbol_count=prior_rows, source=source,
                    message=f"already holds {newest.isoformat()}",
                )

    frames: list[pd.DataFrame] = []
    skipped: list[str] = []
    stopped: tuple[date, str, bool] | None = None  # (day, why, not yet posted)
    dropped = unreadable = 0
    for day in sorted(days):
        try:
            got = fetch_day(day)
        except NotYetPublished as e:
            if backfill:
                skipped.append(day.isoformat())
                continue
            stopped = (day, str(e), True)
            break
        except Exception as e:  # noqa: BLE001 - any fetch/parse failure stops at this day
            stopped = (day, f"BSE bhavcopy {day.isoformat()}: {e}", False)
            break
        if len(got.frame) < min_rows:
            stopped = (
                day,
                f"BSE bhavcopy {day.isoformat()} has {len(got.frame)} rows < floor "
                f"{min_rows} (truncated?)",
                False,
            )
            break
        frames.append(got.frame)
        dropped += got.dropped_rows
        unreadable += got.unreadable_isins

    def unmerged(status: str, why: str) -> RunStatus:
        return RunStatus(
            status, target, symbol_count=prior_rows, source=source,
            message=f"{why}; nothing new merged" + ("; prior kept" if prior_rows else ""),
        )

    if not frames:
        if stopped is None:
            return unmerged("failed", f"no BSE bhavcopy posted for {', '.join(skipped)}")
        day, reason, not_posted = stopped
        return unmerged("not_yet" if not_posted and day == target else "failed", reason)

    sessions = pd.concat(frames, ignore_index=True)
    fresh = _bse_versions(sessions.assign(first_seen=sessions["date"], last_seen=sessions["date"]))
    unseen = len(fresh)
    if prior is not None:
        known = pd.MultiIndex.from_frame(prior[_BSE_REFERENCE_KEY])
        unseen = int((~pd.MultiIndex.from_frame(fresh[_BSE_REFERENCE_KEY]).isin(known)).sum())
        if unseen > max_new_share * len(fresh):
            return unmerged(
                "failed",
                f"prior BSE instrument list lacks {unseen} of the {len(fresh)} versions read "
                f"(over the {max_new_share:.0%} limit): truncated or broken -- "
                f"{_BSE_REFERENCE_BACKFILL_HINT}",
            )
        prior_versions = prior[_BSE_REFERENCE_VERSION_COLUMNS].assign(
            first_seen=pd.to_datetime(prior["first_seen"]),
            last_seen=pd.to_datetime(prior["last_seen"]),
        )
        fresh = _bse_versions(pd.concat([prior_versions, fresh], ignore_index=True))

    out = _shape_bse_reference(fresh, holidays, special)
    _write_atomic_typed(out, out_path, _BSE_REFERENCE_SCHEMA)

    read = sorted({pd.Timestamp(d).date() for d in sessions["date"].unique()})
    span = read[0].isoformat() if len(read) == 1 else f"{read[0]} to {read[-1]}"
    summary = f"{len(out)} versions, {unseen} new, from {len(read)} BSE sessions ({span})"
    if dropped:
        summary += f"; {dropped} rows without a scrip code or symbol dropped"
    if unreadable:
        summary += f"; {unreadable} unreadable ISINs nulled"
    if skipped:
        summary += f"; not posted, skipped: {', '.join(skipped)}"
    if stopped is not None:
        day, reason, not_posted = stopped
        status = "not_yet" if not_posted and day == target else "failed"
        return RunStatus(
            status, target, symbol_count=len(out), source=source,
            message=f"{summary}; stopped at {day.isoformat()}: {reason}",
        )
    return RunStatus("success", target, symbol_count=len(out), source=source, message=summary)


def _write_atomic(df: pd.DataFrame, target: Path) -> None:
    tmp = target.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, compression="zstd", index=False)
    tmp.replace(target)


def _write_atomic_typed(df: pd.DataFrame, target: Path, schema: pa.Schema) -> None:
    """`_write_atomic` with the column types pinned, for a file the desktop
    client reads by type.

    Left to pandas, the Arrow types follow the pandas version, not the code:
    pandas 3 writes text as `large_string` where 2.x wrote `string`, so a
    dependency bump alone would change a published contract. Casting through
    an explicit schema keeps the file identical across both.
    """
    tmp = target.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, compression="zstd", index=False, schema=schema)
    tmp.replace(target)


# ---------------------------------------------------------------------------
# sector_industry: a FETCHED reference (the NSE Total-Market constituents CSV),
# modelled as a derived-style builder (full-rewrite, atomic, no per-day store
# read) because the fetched-dataset path (run_daily + Fetcher) is OHLC-shaped
# and does not fit a slow-moving, weekly-refreshed snapshot. Weekly TTL,
# fail-closed on empty/short/failed fetch, and shrink-safe against the publish
# guard -- see build_sector_industry.
# ---------------------------------------------------------------------------


def _read_seed_frame(target: date) -> pd.DataFrame:
    """Read the committed full-universe seed CSV into the normalized sector
    frame -- the DEFAULT source for build_sector_industry.

    Same `date -> DataFrame` seam as `_fetch_sector_frame`, but a local file read
    (no network): the seed is harvested offline (scripts/harvest_nse_industry.py)
    because NSE has no bulk per-security classification file. Covers the whole
    tradable universe with all NSE tiers, so sector/basic_industry are populated
    (not NULL) and the industry filter reaches every symbol, not just the ~750
    Total-Market members. A missing/empty seed yields an empty frame, which the
    builder's fail-closed guard turns into a retained-prior/failed outcome --
    never a bad overwrite."""
    path = config.SECTOR_SEED_PATH
    if not path.exists():
        return nse_sector.parse_sector_seed(b"")  # empty -> builder fail-closes
    return nse_sector.parse_sector_seed(path.read_bytes())


def _fetch_sector_frame(target: date) -> pd.DataFrame:
    """Fetch + parse the NSE Total-Market CSV into the normalized sector frame,
    reusing the shared warm-session + retry contract (`fetch._fetch_with_retry`)
    exactly as the equities/indices fetchers do -- `parse=parse_sector_csv` is a
    `bytes -> DataFrame` parser of the same shape as `_unzip_to_df`/`_csv_to_df`.
    A 404/exhaustion raises (caught by the builder's fail-closed guard).

    Retained as the seam for the future incremental top-up (seed UNION a live
    fetch of brand-new index entrants); no longer the default source."""
    session = requests.Session()
    session.headers.update({"User-Agent": _BROWSER_UA})
    return _fetch_with_retry(
        session,
        nse_sector.SECTOR_CSV_URL,
        target,
        parse=nse_sector.parse_sector_csv,
    )


def _sector_prior_rows(out_path: Path) -> int | None:
    """Row count of the currently-stored sector file, or None if absent/unreadable."""
    if not out_path.exists():
        return None
    try:
        return int(len(pd.read_parquet(out_path, columns=["date"])))
    except Exception:  # noqa: BLE001 - a corrupt prior file is treated as absent
        return None


def _sector_is_fresh(out_path: Path, target: date, ttl_days: int) -> bool:
    """True when the stored file's as-of `date` is within `ttl_days` of
    `target` -- the weekly-TTL guard that keeps the daily cron from re-fetching
    a slow-moving list every run."""
    if not out_path.exists():
        return False
    try:
        col = pd.to_datetime(pd.read_parquet(out_path, columns=["date"])["date"])
    except Exception:  # noqa: BLE001 - unreadable -> not fresh, re-fetch
        return False
    if col.empty:
        return False
    as_of = col.max().date()
    return (target - as_of).days < ttl_days


def _sector_fail_closed(
    target: date, out_path: Path, prior_rows: int | None, reason: str
) -> RunStatus:
    """Fail-closed outcome: keep any prior good file rather than overwrite it
    with an empty/short/failed fetch. With a prior file present this is a clean
    `skipped_idempotent` (an ok status -- never reds the daily job); with no
    prior file at all it is a genuine `failed` (a real first-run alert)."""
    if prior_rows is not None:
        return RunStatus(
            "skipped_idempotent",
            target,
            symbol_count=prior_rows,
            source="nse-sector",
            message=f"{reason}; retained prior file",
        )
    return RunStatus(
        "failed",
        target,
        source="nse-sector",
        message=f"{reason}; no prior file to retain",
    )


def _classification_registry(df: pd.DataFrame) -> dict[str, ClassificationRegistryRecord]:
    """Map a normalized active sector frame to its ISIN-keyed publishable rows."""

    def label(value: object) -> str:
        if value is None or value is pd.NA:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        return str(value)

    return {
        str(row.instrument_key): ClassificationRegistryRecord(
            instrument_key=str(row.instrument_key),
            symbol=str(row.symbol),
            macro_sector=label(row.macro_sector),
            sector=label(row.sector),
            industry=label(row.industry),
            basic_industry=label(row.basic_industry),
        )
        for row in df[nse_sector.SECTOR_COLUMNS].itertuples(index=False)
    }


def _seed_provenance(
    records: dict[str, ClassificationRegistryRecord], target: date
) -> dict[str, classification_publication.Provenance]:
    """Record the committed seed as the source until the Screener collector lands.

    The same seam will accept real per-page Screener provenance in the baseline
    collector; this fallback never pretends that historical seed data came from
    Screener.
    """
    observed_at = pd.Timestamp(target).to_pydatetime()
    source_url = config.SECTOR_SEED_PATH.resolve().as_uri()
    return {
        instrument_key: classification_publication.Provenance(
            observed_at=observed_at,
            source_url=source_url,
            extractor_version="sector-seed-v1",
            source_fragment_hash=classification_publication.classification_fingerprint(
                {instrument_key: record}
            ),
        )
        for instrument_key, record in records.items()
    }


def _prior_classification_registry(
    out_path: Path,
) -> _PriorClassificationArtifact:
    if not out_path.exists():
        return _PriorClassificationArtifact({}, False)
    try:
        previous = pd.read_parquet(out_path)
    except Exception as error:  # noqa: BLE001 - retain a prior artifact on unreadable input
        return _PriorClassificationArtifact(
            {}, False, f"could not read prior classification artifact: {error}"
        )

    columns = frozenset(previous.columns)
    legacy_missing_macro_sector = columns == _LEGACY_V1_SECTOR_COLUMNS
    if columns not in {_LEGACY_V1_SECTOR_COLUMNS, _CURRENT_V2_SECTOR_COLUMNS}:
        return _PriorClassificationArtifact(
            {}, False, "prior classification artifact has an unrecognized schema"
        )

    records = _classification_registry(previous.reindex(columns=nse_sector.SECTOR_COLUMNS))
    if len(records) != len(previous):
        return _PriorClassificationArtifact(
            {}, False, "prior classification artifact has duplicate instrument keys"
        )
    return _PriorClassificationArtifact(records, legacy_missing_macro_sector)


def _active_registry_or_bootstrap(
    spec: DatasetSpec,
    seed_records: dict[str, ClassificationRegistryRecord],
    target: date,
) -> tuple[
    dict[str, ClassificationRegistryRecord],
    dict[str, classification_publication.Provenance],
    int,
]:
    """Use persisted active state, or bootstrap it once from the legacy seed.

    The bootstrap is only a migration bridge. Once the approved NSE/Screener
    collector writes state, its active/pending/inactive lifecycle is authoritative
    and the seed no longer controls which records are publishable.
    """
    state_path = spec.base_dir / "classification_registry_all.parquet"
    persisted = load_registry(state_path)
    if persisted:
        records = active_classification_records(persisted)
        provenance = {
            instrument_key: entry.last_provenance
            for instrument_key, entry in persisted.items()
            if instrument_key in records and entry.last_provenance is not None
        }
        expected_active_count = sum(
            entry.status is not RegistryStatus.INACTIVE for entry in persisted.values()
        )
        return records, provenance, expected_active_count
    seed_provenance = _seed_provenance(seed_records, target)
    bootstrap = {
        instrument_key: RegistryEntry(
            instrument_key=instrument_key,
            symbol=record.symbol,
            status=RegistryStatus.CLASSIFIED,
            last_known_good=record,
            last_provenance=seed_provenance[instrument_key],
        )
        for instrument_key, record in seed_records.items()
    }
    write_registry(state_path, bootstrap, updated_on=target)
    return seed_records, seed_provenance, len(seed_records)


def _sector_frame_from_registry(
    records: dict[str, ClassificationRegistryRecord], target: date
) -> pd.DataFrame:
    rows = [
        {
            "instrument_key": record.instrument_key,
            "symbol": record.symbol,
            "macro_sector": record.macro_sector or None,
            "sector": record.sector,
            "industry": record.industry or None,
            "basic_industry": record.basic_industry or None,
            "is_cyclical": nse_sector.is_cyclical_seed(record.sector),
            "cyclicality_rule_version": nse_sector.CYCLICAL_RULE_VERSION,
            "date": pd.Timestamp(target),
        }
        for _, record in sorted(records.items())
    ]
    return pd.DataFrame(rows, columns=[*nse_sector.SECTOR_COLUMNS, "date"])


def build_sector_industry(
    spec: DatasetSpec,
    target: date,
    *,
    fetch_frame: Callable[[date], pd.DataFrame] = _read_seed_frame,
    ttl_days: int = config.SECTOR_REFRESH_TTL_DAYS,
    min_rows: int = config.SECTOR_MIN_ROWS,
) -> RunStatus:
    """Read (default) or fetch + normalize + full-rewrite the `sector_industry`
    reference. The default source is the committed full-universe seed
    (`_read_seed_frame`); inject `fetch_frame=_fetch_sector_frame` for the
    legacy Total-Market fetch, or any `date -> DataFrame` seam in tests.

    Weekly TTL: if the stored file's as-of date is younger than `ttl_days`,
    skip the fetch entirely (cheap idempotent no-op on the nightly cron).

    Data-quality wall (fail-closed -- a wrong/missing sector file is worse than
    a stale-but-correct one):
      - fetch error            -> keep prior file (skipped) / failed if none
      - parsed 0 rows          -> keep prior file / failed if none
      - parsed < `min_rows`    -> suspected truncation: keep prior / failed
      - parsed < prior rows    -> respect the publish shrink-guard (any per-file
                                  row decrease blocks the shared publish): hold
                                  the smaller list back; a manual refresh
                                  (delete the file) accepts a genuine shrink.

    On a healthy fetch the file is written atomically (tmp+rename, zstd) with an
    appended `date` (as-of) column so the manifest machinery can read
    latest_date/rows off it exactly like every other dataset."""
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"
    prior_rows = _sector_prior_rows(out_path)

    # The weekly TTL exists ONLY to avoid re-FETCHING the Total-Market CSV over
    # the network every day. The default seed source is a free local read, so the
    # TTL must NOT apply to it -- otherwise a synced, fresh-dated prior parquet
    # skips picking up a refreshed committed seed (the seed would never build).
    # Only the legacy fetch path keeps the TTL; the seed path relies on the
    # content-guard below to avoid churn.
    if fetch_frame is not _read_seed_frame and _sector_is_fresh(out_path, target, ttl_days):
        return RunStatus(
            "skipped_idempotent",
            target,
            symbol_count=prior_rows or 0,
            source="nse-sector",
            message=f"within {ttl_days}-day TTL; not re-fetched",
        )

    try:
        df = fetch_frame(target)
    except Exception as e:  # noqa: BLE001 - any fetch/parse failure -> fail-closed
        return _sector_fail_closed(target, out_path, prior_rows, f"fetch failed: {e}")

    if df.empty:
        return _sector_fail_closed(target, out_path, prior_rows, "parsed 0 valid rows")
    if len(df) < min_rows:
        return _sector_fail_closed(
            target,
            out_path,
            prior_rows,
            f"parsed {len(df)} rows < floor {min_rows} (suspected truncation)",
        )
    # NO parse-vs-artifact shrink guard here, ON PURPOSE. One lived here and
    # broke the pipeline for a week: it compared the parsed frame against the
    # PUBLISHED artifact's row count, and the registry cutover made the
    # artifact a ratcheting superset (registry actives merged over retained
    # prior rows). The first post-cutover publish grew it 4681 -> 4814, after
    # which the unchanged 4681-row seed read as a 2.76% "shrink" and every run
    # hard-failed before any registry logic executed. Nothing had shrunk;
    # nothing was delisted; the comparison itself was between two different
    # universes -- the one this function's own comment below says coverage
    # must never be measured against.
    #
    # The protections that made that guard feel necessary all still stand,
    # each measuring like against like:
    #   - the empty/min_rows walls above catch a truncated parse;
    #   - the registry, not the parse, controls what is publishable
    #     (_active_registry_or_bootstrap), so a thin parse cannot drop records;
    #   - the union guard below refuses any publish that would shrink the
    #     artifact (published-vs-published);
    #   - decide_publication gates coverage against the expected active count.

    # Content-guard (seed path only): skip the write (and thus the daily
    # re-publish) when the classification is byte-identical to the current file,
    # ignoring the as-of `date`. Lets the seed path rebuild whenever the seed
    # changes without churning the release every day when it doesn't. The legacy
    # fetch path keeps its original weekly-TTL write behavior.
    seed_registry = _classification_registry(df)
    current_registry, provenance, expected_active_count = _active_registry_or_bootstrap(
        spec, seed_registry, target
    )
    if not current_registry:
        return _sector_fail_closed(
            target, out_path, prior_rows, "active registry has no classified records"
        )
    previous_artifact = _prior_classification_registry(out_path)
    if previous_artifact.error is not None:
        return _sector_fail_closed(target, out_path, prior_rows, previous_artifact.error)
    previous_registry = previous_artifact.records
    # The previous legacy artifact includes non-EQ/BE securities. Retain those
    # untouched compatibility rows while replacing the active EQ/BE subset from
    # the authoritative registry. Coverage below is measured only against the
    # active NSE snapshot, never against this historical wider universe.
    publish_registry = {
        instrument_key: record
        for instrument_key, record in previous_registry.items()
        if instrument_key not in current_registry
    }
    publish_registry.update(current_registry)
    if prior_rows is not None and len(publish_registry) < prior_rows:
        return _sector_fail_closed(
            target,
            out_path,
            prior_rows,
            "classification cutover would shrink the prior compatibility artifact",
        )
    out = _sector_frame_from_registry(publish_registry, target)
    publication = classification_publication.decide_publication(
        publish_registry,
        previous_registry,
        provenance,
        observed_active_count=len(current_registry),
        expected_active_count=expected_active_count,
        legacy_missing_macro_sector=previous_artifact.legacy_missing_macro_sector,
    )
    if not publication.publish:
        status = "skipped_idempotent" if publication.reason == "fingerprint unchanged" else "failed"
        return RunStatus(
            status,
            target,
            symbol_count=prior_rows or len(out),
            source="nse-sector",
            message=publication.reason,
        )

    _write_atomic(out, out_path)
    classification_publication.append_observations(
        spec.base_dir / "classification_observations_all.parquet",
        publication.observations,
    )
    return RunStatus("success", target, symbol_count=len(out), source="nse-sector")


def _sector_content_unchanged(out_path: Path, df: pd.DataFrame) -> bool:
    """True when `df`'s classification (SECTOR_COLUMNS, ignoring the as-of `date`)
    matches the current file. Biased to False (rebuild) on any missing/unreadable
    prior or shape mismatch, so a real change is never missed."""
    if not out_path.exists():
        return False
    try:
        prev = pd.read_parquet(out_path)
    except Exception:  # noqa: BLE001 - unreadable prior -> rebuild
        return False
    cols = nse_sector.SECTOR_COLUMNS
    if not set(cols).issubset(prev.columns):
        return False

    def _canon(frame: pd.DataFrame) -> str:
        f = frame[cols].sort_values("instrument_key").reset_index(drop=True)
        return f.astype("string").fillna("").to_csv(index=False)

    return _canon(df) == _canon(prev)


# ── index constituents ──────────────────────────────────────────────────────

def _read_constituents_catalog() -> list[dict[str, str]]:
    """The committed index -> published-CSV catalog.

    Not computed: NSE's per-index filenames follow no rule (Defence keeps
    "india", Consumption drops it, Infrastructure abbreviates, MidSmall
    Financial Services carries NSE's own typo). See the seeds README.
    """
    path = config.CONSTITUENTS_CATALOG_PATH
    if not path.exists():
        return []
    import csv as _csv

    with path.open(newline="") as f:
        return [row for row in _csv.DictReader(f) if row.get("csv_path")]


def _index_key_map(indices_spec: DatasetSpec) -> dict[str, str]:
    """Map normalized index NAME -> current `IDX:` key, read from the indices
    dataset's most recent session.

    Resolved per run, never frozen into the catalog, because index names drift
    and the key drifts with them: `IDX:NIFTYINDIAINTERNET&E-COMMERCE` became
    `IDX:NIFTYINDIAINTERNET` on 2025-05-05. A stale key does not error — it
    publishes a basket under a key nothing joins to, and the consumer's drill
    silently empties, which reads as "this index has no members" rather than
    as a fault.
    """
    files = sorted(indices_spec.base_dir.glob(f"{indices_spec.file_prefix}_*.parquet"))
    if not files:
        return {}
    df = pd.read_parquet(files[-1], columns=["date", "instrument_key", "symbol"])
    if df.empty:
        return {}
    latest = df[df["date"] == df["date"].max()]
    return {
        nse_constituents.normalize_index_name(str(name)): str(key)
        for name, key in zip(latest["symbol"], latest["instrument_key"], strict=False)
    }


def _fetch_one_constituent_list(
    session: requests.Session, row: dict[str, str], index_key: str, target: date
) -> pd.DataFrame:
    """Fetch one index's basket, primary host first then the archives mirror.

    Order matters and is the opposite of a "prefer the CDN" instinct: the
    mirror is missing 23 of 134 lists (Chemicals, Housing, Capital Goods,
    Consumer Services, ...), so leading with it would drop those indices
    entirely. The mirror is the fallback for when the primary is unreachable —
    notably from CI, where Akamai blocks datacenter IPs on NSE's API host.
    """
    csv_path = row["csv_path"]
    name = row["index_name"]
    source_file = csv_path.rsplit("/", 1)[-1]

    def _parse(payload: bytes) -> pd.DataFrame:
        return nse_constituents.parse_constituents_csv(
            payload,
            index_key=index_key,
            index_name=name,
            family=row.get("family", "unknown"),
            rotation_list=row.get("rotation", "").strip().lower() == "yes",
            display_label=row.get("display_label", ""),
            source_file=source_file,
        )

    urls = [nse_constituents.primary_url(csv_path)]
    if row.get("on_mirror", "").strip().lower() == "yes":
        urls.append(nse_constituents.mirror_url(csv_path))

    last: Exception | None = None
    for url in urls:
        try:
            return _fetch_with_retry(session, url, target, parse=_parse)
        except Exception as e:  # noqa: BLE001 - try the next host, then give up
            last = e
    raise RuntimeError(f"{name}: all hosts failed ({last})")


def _catalog_rev() -> str:
    """Stable content hash of the committed catalog seed (12 hex chars)."""
    import hashlib

    path = config.CONSTITUENTS_CATALOG_PATH
    if not path.exists():
        return "no-catalog"
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


def _constituents_prior(out_path: Path) -> pd.DataFrame | None:
    if not out_path.exists():
        return None
    try:
        return pd.read_parquet(out_path)
    except Exception:  # noqa: BLE001 - a corrupt prior file is treated as absent
        return None


def _constituents_calendar() -> tuple[set[date], set[date]]:
    """(holidays, special sessions) of NSE, from the run's own market calendar
    (META_DIR/market_calendar.json), as `_cas_trading_day` reads it."""
    return cal.load_trading_calendar(config.META_DIR)


def _session_on_or_before(d: date, holidays: set[date], special: set[date]) -> date:
    if cal.is_trading_day(d, holidays, special):
        return d
    return cal.previous_trading_day(d, holidays, special)


def _sessions_after(last: date, session: date, holidays: set[date], special: set[date]) -> int:
    """How many trading sessions fall in (last, session]."""
    n = 0
    cur = session
    while cur > last:
        if cal.is_trading_day(cur, holidays, special):
            n += 1
        cur -= timedelta(days=1)
    return n


# ── index constituents: NSE's announced effective dates ────────────────────

_ANN = nse_index_announcements
# Appended after `catalog_rev` (schema v2): per index, the effective date and
# press release of the latest announced change in force at the list's session.
_CONSTITUENT_DATE_COLUMNS: list[str] = ["effective_from", "change_ref"]
_OVERRIDE_ACTIONS = frozenset({"include", "exclude", "cancel", "ignore"})
# Series an index member trades in, in preference order: NSE's lists carry
# SME (SM, ST), REIT (RR) and InvIT (IV) members beside the equity series.
_CONSTITUENT_SERIES: tuple[str, ...] = ("EQ", "BE", "BZ", "SM", "ST", "RR", "IV")


def _fetch_index_announcements(
    session_day: date,
    *,
    grace_days: int = config.CONSTITUENTS_ANNOUNCEMENT_GRACE_DAYS,
    lookback_days: int = config.CONSTITUENTS_ANNOUNCEMENT_LOOKBACK_DAYS,
) -> nse_index_announcements.Announcements:
    """Read the press-release index and the few releases that can matter to
    `session_day`, through the shared retry and backoff. A fetch failure is
    transient and reported as `unavailable`; a PDF that is not a release, or
    a release no layout reads, is `unparsed`."""
    http = requests.Session()
    http.headers.update({"User-Agent": _BROWSER_UA})
    try:
        page = _fetch_with_retry(
            http, _ANN.PRESS_RELEASE_INDEX_URL, session_day,
            parse=lambda b: b.decode("utf-8", errors="replace"),
        )
    except Exception as e:  # noqa: BLE001 - transient: the run goes on CSV-only
        return _ANN.Announcements(unavailable=[f"press-release index: {e}"])
    releases = _ANN.select_releases(
        _ANN.parse_press_release_index(page), session_day,
        grace_days=grace_days, lookback_days=lookback_days,
    )
    parsed: list[nse_index_announcements.ParsedRelease] = []
    unavailable: list[str] = []
    malformed: list[str] = []
    for rel in releases:
        try:
            text = _fetch_with_retry(http, rel.url, session_day, parse=_ANN.pdf_text)
        except _ANN.MalformedAnnouncement:
            malformed.append(rel.ref)
            continue
        except Exception as e:  # noqa: BLE001 - transient; the next rung retries
            unavailable.append(f"{rel.ref}: {e}")
            continue
        try:
            parsed.append(_ANN.parse_release_text(text, rel))
        except Exception:  # noqa: BLE001 - a text no layout survives is unparsed, loudly
            malformed.append(rel.ref)
    out = _ANN.collect(parsed)
    out.unparsed.extend(malformed)
    out.unavailable.extend(unavailable)
    return out


def _read_index_change_overrides(path: Path) -> list[dict[str, str]]:
    """The committed overrides seed; rows with an unknown action are ignored
    (the seed's own test pins every committed row's action)."""
    if not path.exists():
        return []
    import csv as _csv

    with path.open(newline="") as f:
        rows = [
            {k: (v or "").strip() for k, v in row.items() if k is not None}
            for row in _csv.DictReader(f)
        ]
    return [r for r in rows if r.get("press_release") and r.get("action") in _OVERRIDE_ACTIONS]


def _with_overrides(
    ann: nse_index_announcements.Announcements, overrides: list[dict[str, str]]
) -> tuple[list[nse_index_announcements.Change], list[str]]:
    """The announced changes after a human's overrides, and the releases still
    unparsed (any override row for a release marks it read)."""
    norm = nse_constituents.normalize_index_name
    changes = list(ann.changes)
    for o in overrides:
        if o["action"] != "cancel":
            continue
        changes = [
            c for c in changes
            if not (
                c.ref == o["press_release"]
                and (not o.get("index_name") or norm(c.index_name) == norm(o["index_name"]))
                and (not o.get("symbol") or c.symbol == o["symbol"].upper())
            )
        ]
    for i, o in enumerate(overrides):
        if o["action"] not in (_ANN.INCLUDE, _ANN.EXCLUDE):
            continue
        try:
            eff = date.fromisoformat(o.get("effective_from", ""))
        except ValueError:
            continue
        # announced_on = date.max: a human's reading beats NSE's parsed text
        # for the same effective date.
        changes.append(_ANN.Change(
            o["press_release"], 100_000 + i, date.max, eff, o.get("index_name", ""),
            o["action"], o.get("symbol", "").upper(), o.get("note", ""),
        ))
    for rev in ann.revisions:
        changes = [
            _dc_replace(c, effective_from=rev.new)
            if (c.symbol == rev.symbol and c.action == rev.action and c.effective_from == rev.old
                and (rev.index_name is None or norm(c.index_name) == norm(rev.index_name)))
            else c
            for c in changes
        ]
    read = {o["press_release"] for o in overrides}
    return changes, [r for r in ann.unparsed if r not in read]


def _index_lookup(df: pd.DataFrame) -> dict[str, str]:
    """normalized index name -> index_key, for the indices in the list. NSE's
    releases drop a trailing "Index" the catalog keeps ("Nifty Healthcare"
    vs "Nifty Healthcare Index"), so both spellings resolve."""
    norm = nse_constituents.normalize_index_name
    out: dict[str, str] = {}
    for name, key in zip(df["index_name"], df["index_key"], strict=True):
        n = norm(str(name))
        out.setdefault(n, str(key))
        out.setdefault(n.removesuffix("INDEX"), str(key))
    return out


@dataclass
class _LedgerReport:
    applied: list[str]
    held: list[str]
    unresolved: list[str]
    lagging: list[str]
    uncatalogued: int


def _apply_announcements(
    df: pd.DataFrame,
    prior: pd.DataFrame | None,
    changes: list[nse_index_announcements.Change],
    session: date,
    master: dict[str, tuple[str, str]],
    *,
    grace_days: int,
    lookback_days: int,
    lag_sessions: int,
    holidays: set[date],
    special: set[date],
) -> tuple[pd.DataFrame, dict[str, tuple[date, str]], _LedgerReport]:
    """Make each announced change decide membership at session S.

    Per (index, stock), the last change effective on or before S gives the
    state (a later release wins a tie, so a mid-review correction overrides
    the original). With only future changes, the stock is in the state before
    the first one. Where NSE's list disagrees, the list is corrected: a change
    NSE's CSV has not caught up with is applied; one it shows before its
    effective date is held back. The baseline may be a fetched list or a
    carried one, so a list read before the effective date still changes on it.

    Returns the corrected list, per index_key the (effective_from, change_ref)
    of the latest change in force at S, and the counts for the status.
    """
    norm = nse_constituents.normalize_index_name
    lookup = _index_lookup(df)
    floor = session - timedelta(days=grace_days)
    in_window: dict[tuple[str, str], list[nse_index_announcements.Change]] = {}
    uncatalogued = 0
    for c in changes:
        if c.effective_from < floor:
            continue
        if c.announced_on != date.max and c.effective_from > c.announced_on + timedelta(
            days=lookback_days
        ):
            continue  # an implausible date: NSE announces weeks ahead, not years
        n = norm(c.index_name)
        key = lookup.get(n) or lookup.get(n.removesuffix("INDEX"))
        if key is None:
            uncatalogued += 1
            continue
        stock = c.symbol or f"company:{_ANN.company_key(c.company)}"
        in_window.setdefault((key, stock), []).append(c)

    # Rows to borrow a stock's details from: this index's prior rows first,
    # then the stock in any index.
    pool = df if prior is None else pd.concat([df, prior.reindex(columns=df.columns)])
    def _records(frame: pd.DataFrame) -> list[dict[str, object]]:
        return [{str(k): v for k, v in r.items()} for r in frame.to_dict("records")]

    by_symbol: dict[str, dict[str, object]] = {}
    for rec in _records(pool):
        by_symbol.setdefault(str(rec["symbol"]), rec)
    templates: dict[str, dict[str, object]] = {}
    for rec in _records(df):
        templates.setdefault(str(rec["index_key"]), rec)
    prior_rows: dict[tuple[str, str], dict[str, object]] = {}
    for rec in ([] if prior is None else _records(prior)):
        prior_rows.setdefault((str(rec["index_key"]), str(rec["symbol"])), rec)
    company_symbols: dict[tuple[str, str], str] = {}
    for key, sym, comp in zip(pool["index_key"], pool["symbol"], pool["company"], strict=True):
        company_symbols.setdefault((str(key), _ANN.company_key(str(comp))), str(sym))

    report = _LedgerReport([], [], [], [], uncatalogued)
    in_force: dict[str, tuple[date, str, date]] = {}
    drop: set[tuple[str, str]] = set()
    add: list[dict[str, object]] = []
    members = {(str(k), str(s)) for k, s in zip(df["index_key"], df["symbol"], strict=True)}
    for (key, stock), cs in sorted(in_window.items()):
        cs.sort(key=lambda c: (c.effective_from, c.announced_on, c.ref, c.seq))
        past = [c for c in cs if c.effective_from <= session]
        for c in past:
            best = in_force.get(key)
            if best is None or (c.effective_from, c.announced_on) >= (best[0], best[2]):
                in_force[key] = (c.effective_from, c.ref, c.announced_on)
        decisive = past[-1] if past else cs[0]
        want = (decisive.action == _ANN.INCLUDE) if past else (decisive.action != _ANN.INCLUDE)
        symbol = stock
        if stock.startswith("company:"):
            symbol = company_symbols.get((key, stock.removeprefix("company:")), "")
            if not symbol:
                if want:
                    report.unresolved.append(f"{key} {decisive.company}")
                continue
        present = (key, symbol) in members
        if want == present:
            continue
        label = f"{key} {'+' if want else '-'}{symbol} ({decisive.ref})"
        (report.applied if past else report.held).append(label)
        behind = _sessions_after(decisive.effective_from, session, holidays, special)
        if past and behind > lag_sessions:
            report.lagging.append(label)
        if not want:
            drop.add((key, symbol))
            continue
        tmpl = templates[key]
        row: dict[str, object] = {c: tmpl[c] for c in df.columns}
        if (key, symbol) in prior_rows:
            src = prior_rows[(key, symbol)]
            row.update({c: src[c] for c in ("instrument_key", "isin", "company", "industry",
                                             "series", "source_file")})
        elif symbol in by_symbol:
            src = by_symbol[symbol]
            row.update({c: src[c] for c in ("instrument_key", "isin", "company", "industry",
                                             "series")})
            row["source_file"] = f"{decisive.ref}.pdf"
        elif symbol in master:
            isin, series = master[symbol]
            row.update(instrument_key=isin, isin=isin, series=series, industry="",
                       company=decisive.company, source_file=f"{decisive.ref}.pdf")
        else:
            report.unresolved.append(f"{key} {symbol}")
            (report.applied if past else report.held).remove(label)
            if label in report.lagging:
                report.lagging.remove(label)
            continue
        row["symbol"] = symbol
        add.append(row)

    if drop:
        keep = [
            (str(k), str(s)) not in drop
            for k, s in zip(df["index_key"], df["symbol"], strict=True)
        ]
        df = df[keep]
    if add:
        df = pd.concat([df, pd.DataFrame(add, columns=df.columns)], ignore_index=True)
    return df, {k: (v[0], v[1]) for k, v in in_force.items()}, report


def _effective_columns(
    out: pd.DataFrame, prior: pd.DataFrame | None, in_force: dict[str, tuple[date, str]]
) -> pd.DataFrame:
    """Per index, `effective_from` / `change_ref`: the change in force this
    run, else the prior file's values (so the last review is remembered once
    its release leaves the lookback), else null."""
    remembered: dict[str, tuple[pd.Timestamp, object]] = {}
    if prior is not None and set(_CONSTITUENT_DATE_COLUMNS) <= set(prior.columns):
        for key, eff, ref in zip(prior["index_key"], prior["effective_from"], prior["change_ref"],
                                 strict=True):
            if pd.notna(eff):
                remembered.setdefault(str(key), (pd.Timestamp(eff), ref))
    eff_col: list[object] = []
    ref_col: list[object] = []
    for key in out["index_key"]:
        k = str(key)
        if k in in_force:
            eff_col.append(pd.Timestamp(in_force[k][0]))
            ref_col.append(in_force[k][1])
        elif k in remembered:
            eff_col.append(remembered[k][0])
            ref_col.append(remembered[k][1])
        else:
            eff_col.append(pd.NaT)
            ref_col.append(None)
    out = out.copy()
    out["effective_from"] = pd.to_datetime(pd.Series(eff_col, index=out.index, dtype="object"))
    out["change_ref"] = pd.Series(ref_col, index=out.index, dtype="object").astype("string")
    return out


def _stale_indices(
    frame: pd.DataFrame, session: date, holidays: set[date], special: set[date], limit: int
) -> list[str]:
    """Indices whose NSE list was last read more than `limit` sessions ago."""
    out = []
    for key, d in frame.groupby("index_key")["date"].max().items():
        behind = _sessions_after(pd.Timestamp(d).date(), session, holidays, special)
        if behind > limit:
            out.append(f"{key} ({behind} sessions)")
    return sorted(out)


def _some(items: list[str], limit: int = 25) -> str:
    """A status list that stays readable: a review applies hundreds."""
    shown = ", ".join(items[:limit])
    more = f", ... (+{len(items) - limit} more)" if len(items) > limit else ""
    return f"{len(items)}: {shown}{more}"


def build_index_constituents(
    spec: DatasetSpec,
    target: date,
    *,
    fetch_lists: Callable[[date], pd.DataFrame] | None = None,
    calendar: Callable[[], tuple[set[date], set[date]]] | None = None,
    min_rows: int = config.CONSTITUENTS_MIN_ROWS,
    shrink_floor: float = config.CONSTITUENTS_SHRINK_FLOOR,
    stale_sessions: int = config.CONSTITUENTS_STALE_SESSIONS,
    fetch_announcements: Callable[[date], nse_index_announcements.Announcements] | None = None,
    symbol_master: Callable[[], dict[str, tuple[str, str]]] | None = None,
    overrides_path: Path | None = None,
    grace_days: int = config.CONSTITUENTS_ANNOUNCEMENT_GRACE_DAYS,
    lookback_days: int = config.CONSTITUENTS_ANNOUNCEMENT_LOOKBACK_DAYS,
) -> RunStatus:
    """Fetch every catalogued index's published basket and full-rewrite
    `index_constituents_all.parquet`.

    `date` is per index: the trading session whose membership the rows give.
    An index read from NSE this run is dated S, the last NSE session on or
    before `target` (a holiday run never stamps a non-session). An index
    carried forward keeps the session it was last read, so its age is real.

    NSE's lists are read on every run. There is no TTL: a weekly one put the
    list a week behind each review.

    Fail-closed, at TWO levels, because the two failures are different:
      - PER INDEX: a failed fetch or malformed payload, or a basket under
        `MIN_MEMBERS` or under `shrink_floor` of that index's prior count,
        keeps that index's prior rows and names it. An index whose drill
        silently empties looks like a real (empty) answer to a user.
      - PER RUN: zero rows or a total under `min_rows` holds the whole write
        back. There is no whole-file shrink guard: a genuine reconstitution
        makes the total smaller (2026-09-30: 10,376 -> 10,333 rows), and that
        guard kept the pre-review list live for three weeks.

    Never silently stale: an index not read for more than `stale_sessions`
    sessions makes the run `failed`, naming it. The file is still written
    with everything fresh.

    NSE's announced effective dates (`_apply_announcements`): NSE's CSV says
    who is in an index, its press releases say when. Inside the grace window
    an announced change decides membership at S, applied when the CSV lags
    and held back when the CSV shows it early. `effective_from` /
    `change_ref` name the latest change in force per index. A membership
    release no layout reads makes the run `failed`, naming it, until a row in
    `seeds/index_change_overrides.csv` says how to read it.
    """
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"
    holidays, special = (calendar or _constituents_calendar)()
    session = _session_on_or_before(target, holidays, special)
    prior = _constituents_prior(out_path)
    prior_rows = None if prior is None else len(prior)

    # A back-dated `daily --date` must not restamp today's lists with an
    # older session, nor move the published file backwards.
    if prior is not None and not prior.empty:
        published = pd.to_datetime(prior["date"]).max().date()
        if published > session:
            return RunStatus(
                "skipped_idempotent", target, symbol_count=prior_rows or 0,
                source=spec.source_label,
                message=f"published list is as of {published}, after {session}; kept",
            )

    fetch_problem: str | None = None
    try:
        df = (fetch_lists or _fetch_all_constituent_lists)(target)
    except Exception as e:  # noqa: BLE001 - any failure -> fail-closed
        fetch_problem = f"fetch failed: {e}"
    else:
        if df.empty:
            fetch_problem = "parsed 0 valid rows"
        elif len(df) < min_rows:
            fetch_problem = f"parsed {len(df)} rows < floor {min_rows} (suspected partial run)"
    if fetch_problem is not None:
        if prior is None or prior.empty:
            return _constituents_fail_closed(spec, target, out_path, prior_rows, fetch_problem)
        # Nothing usable was read, so every index is carried. NSE's announced
        # changes still apply to the carried list below: a list read before an
        # effective date changes on that date, even through an outage.
        df = pd.DataFrame(columns=nse_constituents.CONSTITUENT_COLUMNS)
    df = df.assign(date=pd.Timestamp(session))

    # Per-index shrink guard, then carry forward every index this run could
    # not use, so one unreachable host never deletes a basket that was fine
    # yesterday. Carried rows keep their own `date`.
    prior_counts = (
        {} if prior is None else prior.groupby("index_key").size().to_dict()
    )
    held: list[str] = []
    for key, n in df.groupby("index_key").size().items():
        before = prior_counts.get(key)
        if n < nse_constituents.MIN_MEMBERS or (before is not None and n < shrink_floor * before):
            held.append(f"{key} {before or 0}->{n}")
    held_keys = {h.split(" ", 1)[0] for h in held}
    df = df[~df["index_key"].isin(held_keys)]
    carried_keys: list[str] = []
    if prior is not None:
        carry = prior[~prior["index_key"].isin(set(df["index_key"]))]
        if not carry.empty:
            carried_keys = sorted(set(carry["index_key"]) - held_keys)
            df = pd.concat([df, carry.reindex(columns=df.columns)], ignore_index=True)

    try:
        ann = (fetch_announcements or _fetch_index_announcements)(session)
    except Exception as e:  # noqa: BLE001 - transient: the run goes on CSV-only
        ann = nse_index_announcements.Announcements(unavailable=[f"announcements: {e}"])
    changes, unparsed = _with_overrides(
        ann, _read_index_change_overrides(overrides_path or config.CONSTITUENTS_OVERRIDES_PATH)
    )
    df, in_force, ledger = _apply_announcements(
        df, prior, changes, session, (symbol_master or _constituents_symbol_master)(),
        grace_days=grace_days, lookback_days=lookback_days, lag_sessions=stale_sessions,
        holidays=holidays, special=special,
    )

    if fetch_problem is not None and not (ledger.applied or ledger.held):
        # Nothing read and nothing announced to apply: keep the file as it is.
        # Still loud once it is stale or a release went unread.
        assert prior is not None
        stale = _stale_indices(prior, session, holidays, special, stale_sessions)
        why = [f"{fetch_problem}; retained prior file"]
        if stale:
            why.append(f"STALE > {stale_sessions} sessions: {', '.join(stale)}")
        if unparsed:
            why.append(f"UNPARSED releases: {', '.join(sorted(unparsed))}")
        return RunStatus(
            "failed" if stale or unparsed else "skipped_idempotent", target,
            symbol_count=prior_rows or 0, source=spec.source_label, message="; ".join(why),
        )

    dupes = df.duplicated(subset=["index_key", "instrument_key"]).sum()
    if dupes:
        return _constituents_fail_closed(
            spec, target, out_path, prior_rows,
            f"{dupes} duplicate (index_key, instrument_key) rows",
        )

    # REQUIRED for the manifest: build_manifest reads columns=["date"], so a
    # missing column raises ArrowInvalid and aborts the publish for EVERY
    # dataset, not just this one.
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    # One value for the whole file: which seed built it (provenance).
    out["catalog_rev"] = _catalog_rev()
    out = out[[*nse_constituents.CONSTITUENT_COLUMNS, "date", "catalog_rev"]]
    out = _effective_columns(out, prior, in_force)
    _write_atomic(out, out_path)

    stale = _stale_indices(out, session, holidays, special, stale_sessions)

    parts = [f"{out['index_key'].nunique()} indices as of {session}"]
    if fetch_problem is not None:
        parts.append(f"NSE's lists not read ({fetch_problem}); every index carried")
    if held:
        parts.append(f"held (shrink-guard): {', '.join(sorted(held))}")
    if carried_keys:
        parts.append(f"carried (not read this run): {', '.join(carried_keys)}")
    if stale:
        parts.append(f"STALE > {stale_sessions} sessions: {', '.join(stale)}")
    if ledger.applied:
        parts.append(f"announced, applied ahead of NSE's list: {_some(ledger.applied)}")
    if ledger.held:
        parts.append(f"announced, held until effective: {_some(ledger.held)}")
    if ledger.lagging:
        parts.append(f"NSE's list lags its release: {_some(ledger.lagging)}")
    if ledger.unresolved:
        parts.append(f"announced, no ISIN to apply: {_some(ledger.unresolved)}")
    if ann.unavailable:
        parts.append(f"announcements unavailable: {'; '.join(ann.unavailable)}")
    if unparsed:
        parts.append(
            f"UNPARSED releases (add a row to seeds/index_change_overrides.csv): "
            f"{', '.join(sorted(unparsed))}"
        )
    parts.append(f"{len(ann.read)} releases read")
    # "success", not "ok": data-daily's secondary-status allowlist is
    # success|skipped_holiday|skipped_idempotent|not_yet, and an invented word
    # here failed the publish gate on a build that had actually WORKED.
    # `failed` on staleness reds the job (an alert) without blocking the
    # publish of the file just written.
    return RunStatus(
        "failed" if stale or unparsed else "success",
        target,
        symbol_count=len(out),
        source=spec.source_label,
        message="; ".join(parts),
    )


def _constituents_symbol_master() -> dict[str, tuple[str, str]]:
    from pipeline import datasets as _datasets

    return _symbol_master(_datasets.DATASETS["reference"], _CONSTITUENT_SERIES)


def _constituents_fail_closed(
    spec: DatasetSpec, target: date, out_path: Path, prior_rows: int | None, why: str
) -> RunStatus:
    """Keep the prior file when there is one; fail loudly when there is not."""
    if out_path.exists() and prior_rows:
        return RunStatus(
            "skipped_idempotent", target, symbol_count=prior_rows, source=spec.source_label,
            message=f"{why}; retained prior file",
        )
    return RunStatus("failed", target, source=spec.source_label, message=why)


def _fetch_all_constituent_lists(target: date) -> pd.DataFrame:
    """Fetch every catalogued basket. Per-index failures are skipped (the
    caller carries prior rows forward for them); the run fails only if the
    catalog is missing or nothing at all resolved."""
    catalog = _read_constituents_catalog()
    if not catalog:
        return pd.DataFrame(columns=nse_constituents.CONSTITUENT_COLUMNS)

    from pipeline import datasets as _datasets

    keys = _index_key_map(_datasets.INDICES)
    session = requests.Session()
    session.headers.update({"User-Agent": _BROWSER_UA})

    frames: list[pd.DataFrame] = []
    for row in catalog:
        key = keys.get(nse_constituents.normalize_index_name(row["index_name"]))
        if not key:
            # Unresolvable name -> skip. Emitting under the catalog's advisory
            # key would publish rows nothing can join to.
            continue
        try:
            frames.append(_fetch_one_constituent_list(session, row, key, target))
        except Exception:  # noqa: BLE001 - per-index skip; prior rows carry forward
            continue

    if not frames:
        return pd.DataFrame(columns=nse_constituents.CONSTITUENT_COLUMNS)
    return pd.concat(frames, ignore_index=True)


# ── CAS-eligible stocks ─────────────────────────────────────────────────────

_CAS_COLUMNS: list[str] = ["symbol", "isin", "date", "source"]
# The types the client reads (see _write_atomic_typed): text as `string`, the
# as-of date as a timestamp. An unresolved ISIN is a null string.
_CAS_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("isin", pa.string()),
        ("date", pa.timestamp("ms")),
        ("source", pa.string()),
    ]
)

# Cash series an F&O stock can sit in, in preference order. Derivatives need
# the rolling (EQ) segment; BE/BZ only cover a stock the master last saw
# there. Never every series: an issuer's bonds/NCDs trade under its symbol
# with their OWN ISINs (series N1-N9, NA-NE, ...) — the trap _load_isin_map in
# datasets.py documents — so a series-blind join can hand a stock a bond's ISIN.
_CAS_EQUITY_SERIES: tuple[str, ...] = ("EQ", "BE", "BZ")


def _cas_symbol_isins(universe_spec: DatasetSpec) -> dict[str, str]:
    """symbol -> current ISIN; see `_symbol_master`."""
    return {sym: isin for sym, (isin, _series) in _symbol_master(universe_spec).items()}


def _symbol_master(
    universe_spec: DatasetSpec, series: tuple[str, ...] = _CAS_EQUITY_SERIES
) -> dict[str, tuple[str, str]]:
    """symbol -> (current ISIN, series), from the pipeline's own symbol master
    (the `reference` dataset's `instruments_all.parquet`).

    The master is SCD2 and ISINs move under a symbol (a face-value split
    re-issues the ISIN: 360ONE has two EQ rows), and symbols get reused, so the
    LATEST `last_seen` row wins, series preference breaking ties. A missing or
    unreadable master is an empty map — the builder's ISIN-coverage gate turns
    that into a retained prior file, never a published list without ISINs.
    """
    path = universe_spec.base_dir / f"{universe_spec.file_prefix}_all.parquet"
    if not path.exists():
        return {}
    try:
        df = pd.read_parquet(path, columns=["symbol", "series", "isin", "last_seen"])
    except Exception:  # noqa: BLE001 - unreadable master -> empty; coverage gate fails closed
        return {}
    df = df[df["series"].isin(series)]
    df = df.assign(
        symbol=df["symbol"].astype(str).str.strip().str.upper(),
        isin=df["isin"].fillna("").astype(str).str.strip().str.upper(),
        _rank=df["series"].map({s: i for i, s in enumerate(series)}),
    )
    df = df[df["isin"].str.len() == 12]
    df = df.sort_values(["last_seen", "_rank"], ascending=[False, True], kind="mergesort")
    df = df.drop_duplicates(subset=["symbol"], keep="first")
    return {
        s: (i, str(se)) for s, i, se in zip(df["symbol"], df["isin"], df["series"], strict=True)
    }


def _cas_trading_day(target: date) -> bool:
    """The daily run's own calendar (META_DIR/market_calendar.json).

    The published `date` is "the trading date this list is as of", and the
    Phase-2 builders still run on a holiday (the primary's `skipped_holiday`
    counts as healthy), so without this an NSE weekday holiday would stamp a
    non-trading date on the file.
    """
    holidays, special = cal.load_trading_calendar(config.META_DIR)
    return cal.is_trading_day(target, holidays, special_sessions=special)


def _fetch_mktlots_frame(target: date) -> pd.DataFrame:
    """Fetch + parse NSE's F&O market-lot file through the shared warm-session
    + retry contract, exactly as the sector and constituents fetches do. The
    parser is the header gate: an HTML/PDF document served with 200 raises
    inside the fetch, so it reaches the builder as a fetch failure."""
    session = requests.Session()
    session.headers.update({"User-Agent": _BROWSER_UA})
    return _fetch_with_retry(
        session,
        nse_fo_mktlots.MKTLOTS_URL,
        target,
        parse=nse_fo_mktlots.parse_mktlots_csv,
    )


def build_cas_eligible(
    spec: DatasetSpec,
    target: date,
    *,
    universe_spec: DatasetSpec,
    fetch_underlyings: Callable[[date], pd.DataFrame] = _fetch_mktlots_frame,
    trading_day: Callable[[date], bool] = _cas_trading_day,
    min_rows: int = config.CAS_MIN_ROWS,
    min_isin_coverage: float = config.CAS_MIN_ISIN_COVERAGE,
) -> RunStatus:
    """Full-rewrite `cas_eligible_all.parquet`: every stock NSE's Closing
    Auction Session covers (a cash stock with derivative contracts), as of
    `target`. Columns: symbol, isin, date, source.

    STOCK OR INDEX. The market-lot file lists index underlyings (NIFTY,
    BANKNIFTY, and new ones such as NIFTYFPI) above NSE's "Derivatives on
    Individual Securities" marker and stocks below it. A row is a stock when
    it sits below the marker OR its symbol is in the pipeline's own equity
    universe. Either signal alone has a failure the other covers: a renamed
    marker would make every row an index, and a symbol renamed today is not
    in the master yet. An index symbol is in neither, which is what drops it —
    no hard-coded index list. A below-marker stock the master does not know
    keeps a null ISIN (counted in the status message).

    Every daily run, no TTL: one small file, and NSE revises it in the IST
    evening, after the earlier retry rungs.

    Fail-closed, keeping the prior file:
      - fetch error / HTML or PDF served with 200 (the parser's header gate)
      - fewer than `min_rows` stocks (a truncated or partial response)
      - fewer than `min_isin_coverage` of rows resolve an ISIN (symbol master
        missing or broken; publishing would strip the client's join key)
    These are transient, so with a prior file they are `skipped_idempotent`
    (retained, quiet) and only a first run with nothing to keep is `failed` —
    the constituents policy.

    A SHRINK is held back too, but LOUDLY. The publish shrink-guard fails the
    whole shared release on any per-file row decrease, so a shorter list must
    never be written; yet a genuine F&O exit is the normal way this list
    shrinks and repeats every run until accepted, so it reports `failed`
    (prior retained) and names the symbols — a silent hold would freeze the
    list's date with nobody told. Accepting it is the operator step in the
    RUNBOOK's cas_eligible section.
    """
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"
    prior = _constituents_prior(out_path)
    prior_rows = None if prior is None else len(prior)

    if not trading_day(target):
        return RunStatus(
            "skipped_holiday",
            target,
            symbol_count=prior_rows or 0,
            source=spec.source_label,
            message="non-trading day; list not refreshed",
        )

    try:
        underlyings = fetch_underlyings(target)
    except Exception as e:  # noqa: BLE001 - any fetch/parse failure -> fail-closed
        return _constituents_fail_closed(spec, target, out_path, prior_rows, f"fetch failed: {e}")

    isins = _cas_symbol_isins(universe_spec)
    is_stock = (underlyings["section"] == nse_fo_mktlots.SECTION_STOCK) | underlyings[
        "symbol"
    ].isin(isins.keys())
    stocks = underlyings[is_stock]
    dropped_indices = int((~is_stock).sum())

    if stocks.empty:
        return _constituents_fail_closed(
            spec, target, out_path, prior_rows, "parsed 0 stock underlyings"
        )
    if len(stocks) < min_rows:
        return _constituents_fail_closed(
            spec, target, out_path, prior_rows,
            f"parsed {len(stocks)} stock underlyings < floor {min_rows} (suspected truncation)",
        )

    resolved = stocks["symbol"].map(isins)
    with_isin = int(resolved.notna().sum())
    if with_isin < min_isin_coverage * len(stocks):
        return _constituents_fail_closed(
            spec, target, out_path, prior_rows,
            f"only {with_isin} of {len(stocks)} stocks resolved an ISIN from "
            f"{universe_spec.file_prefix}_all.parquet (floor {min_isin_coverage:.0%}); "
            "symbol master missing or stale",
        )

    out = (
        pd.DataFrame(
            {
                "symbol": stocks["symbol"].to_numpy(),
                "isin": resolved.astype(object).where(resolved.notna(), None).to_numpy(),
            }
        )
        .sort_values("symbol", kind="mergesort")
        .reset_index(drop=True)
    )
    # REQUIRED for the manifest: build_manifest reads columns=["date"].
    out["date"] = pd.Timestamp(target)
    out["source"] = nse_fo_mktlots.SOURCE
    out = out[_CAS_COLUMNS]

    if prior is not None and prior_rows is not None and len(out) < prior_rows:
        gone: list[str] = (
            sorted(set(prior["symbol"].astype(str)) - set(out["symbol"]))
            if "symbol" in prior.columns
            else []
        )
        shown = ", ".join(gone[:15]) + (f" (+{len(gone) - 15} more)" if len(gone) > 15 else "")
        return RunStatus(
            "failed",
            target,
            symbol_count=prior_rows,
            source=spec.source_label,
            message=(
                f"{len(out)} stocks < prior {prior_rows} (shrink-guard; no longer listed: "
                f"{shown or 'n/a'}); retained prior file -- accept per RUNBOOK cas_eligible"
            ),
        )

    _write_atomic_typed(out, out_path, _CAS_SCHEMA)
    missing = sorted(out.loc[out["isin"].isna(), "symbol"])
    note = f"; {len(missing)} without ISIN: {', '.join(missing[:15])}" if missing else ""
    # "success", not "ok": data-daily's secondary-status allowlist is
    # success|skipped_holiday|skipped_idempotent|not_yet.
    return RunStatus(
        "success",
        target,
        symbol_count=len(out),
        source=spec.source_label,
        message=f"{len(out)} stocks; {dropped_indices} index underlyings dropped{note}",
    )


# ── Market calendar ─────────────────────────────────────────────────────────

# The types the client reads (see _write_atomic_typed). `day` is the calendar
# date; `date` is the as-of the manifest reads, in the pipeline's ms unit.
_MARKET_CALENDAR_SCHEMA = pa.schema(
    [
        ("venue", pa.string()),
        ("day", pa.date32()),
        ("kind", pa.string()),
        ("pre_open_seconds", pa.int32()),
        ("open_seconds", pa.int32()),
        ("close_seconds", pa.int32()),
        ("label", pa.string()),
        ("source", pa.string()),
        ("date", pa.timestamp("ms")),
    ]
)


def build_market_calendar(
    spec: DatasetSpec, target: date, *, meta_dir: Path = config.META_DIR
) -> RunStatus:
    """Full-rewrite `market_calendar_all.parquet` from the one market calendar
    file, META_DIR/market_calendar.json: per venue, closures, days with their
    own hours, and the years verified complete (rows and rules:
    market_calendar.py).

    Every daily run, holiday or not: nothing is fetched, and a meta commit
    should publish on the next run.

    Fail-closed, keeping the prior file: malformed or self-contradicting meta
    is `skipped_idempotent` with a prior file and `failed` without one. A
    calendar that LOST rows is held back loudly (`failed`, prior retained, the
    lost rows named): the publish shrink-guard would block the whole release,
    and a dropped closure or session silently changes when the app's alerts
    fire. Removing a wrong entry is the operator step in the RUNBOOK's
    market_calendar section.
    """
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"
    prior = _constituents_prior(out_path)
    prior_rows = None if prior is None else len(prior)

    try:
        calendar = market_calendar.load(meta_dir / market_calendar.FILENAME)
        rows = calendar.rows()
    except market_calendar.CalendarError as e:
        return _constituents_fail_closed(
            spec, target, out_path, prior_rows, f"calendar invalid: {e}"
        )

    out = pd.DataFrame(
        {
            "venue": [r.venue for r in rows],
            "day": [r.day for r in rows],
            "kind": [r.kind for r in rows],
            "pre_open_seconds": pd.array([r.pre_open_seconds for r in rows], dtype="Int32"),
            "open_seconds": pd.array([r.open_seconds for r in rows], dtype="Int32"),
            "close_seconds": pd.array([r.close_seconds for r in rows], dtype="Int32"),
            "label": [r.label for r in rows],
            "source": [r.source for r in rows],
        }
    )
    # REQUIRED for the manifest: build_manifest reads columns=["date"].
    out["date"] = pd.Timestamp(target)

    if prior is not None and prior_rows is not None and len(out) < prior_rows:
        def keys(frame: pd.DataFrame) -> set[str]:
            return {
                f"{v} {pd.Timestamp(d).date().isoformat()} {k}"
                for v, d, k in zip(frame["venue"], frame["day"], frame["kind"], strict=True)
            }

        comparable = {"venue", "day", "kind"} <= set(prior.columns)
        gone = sorted(keys(prior) - keys(out)) if comparable else []
        shown = ", ".join(gone[:15]) + (f" (+{len(gone) - 15} more)" if len(gone) > 15 else "")
        return RunStatus(
            "failed",
            target,
            symbol_count=prior_rows,
            source=spec.source_label,
            message=(
                f"{len(out)} rows < prior {prior_rows} (shrink-guard; no longer listed: "
                f"{shown or 'n/a'}); retained prior file -- accept per RUNBOOK market_calendar"
            ),
        )

    _write_atomic_typed(out, out_path, _MARKET_CALENDAR_SCHEMA)
    covered = ", ".join(sorted(f"{r.venue} {r.day.year}" for r in rows if r.kind == "covered"))
    closed = sum(r.kind == "closed" for r in rows)
    sessions = sum(r.kind == "session" for r in rows)
    pending = calendar.sessions_awaiting_hours()
    # Named on every run until the hours are added: the app treats such a day
    # as closed, so a Muhurat session left without hours is missed by alerts.
    awaiting = f"; awaiting hours: {', '.join(pending)}" if pending else ""
    return RunStatus(
        "success",
        target,
        symbol_count=len(out),
        source=spec.source_label,
        message=f"{closed} closures, {sessions} sessions; covered: {covered or 'none'}{awaiting}",
    )


# ── F&O freeze limits ───────────────────────────────────────────────────────

_FNO_LIMIT_COLUMNS: list[str] = [*nse_fo_contract.LIMIT_COLUMNS, "date", "source"]
# Every venue's parser marks its rows with the same basis words.
_BASIS_EXCHANGE = nse_fo_contract.BASIS_EXCHANGE
_BASIS_STAND_IN = nse_fo_contract.BASIS_STAND_IN
# The types the client reads (see _write_atomic_typed). `date` keeps the
# millisecond unit every published file in this pipeline carries.
_FNO_LIMIT_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("kind", pa.string()),
        ("lot_size", pa.int64()),
        ("max_order_quantity", pa.int64()),
        ("basis", pa.string()),
        ("date", pa.timestamp("ms")),
        ("source", pa.string()),
    ]
)


def _fetch_freeze_limits(
    target: date, *, lookback_days: int = config.FNO_CONTRACT_LOOKBACK_DAYS
) -> tuple[date, pd.DataFrame]:
    """The latest contract file on or before `target`, and its day.

    NSE posts file D in the evening of D (18:30-20:00 IST seen), listing the
    contracts of the next session, so a run before that reads D-1; the walk
    back also covers holidays and long weekends. Only a 404 moves to an older
    day: a refusal, a server error or a wrong document served with 200 fails
    the fetch, so it is never mistaken for an older right file.
    """
    with requests.Session() as session:
        session.headers.update({"User-Agent": _BROWSER_UA})
        for back in range(lookback_days):
            day = target - timedelta(days=back)
            try:
                frame = _fetch_with_retry(
                    session,
                    nse_fo_contract.contract_url(day),
                    day,
                    parse=nse_fo_contract.parse_freeze_limits,
                )
            except NotYetPublished:
                continue
            return day, frame
    raise NotYetPublished(
        f"no contract file in the {lookback_days} days to {target.isoformat()}"
    )


def _prior_file_day(prior: pd.DataFrame | None) -> date | None:
    if prior is None or prior.empty or "date" not in prior.columns:
        return None
    latest = prior["date"].max()
    if pd.isna(latest):
        return None
    try:
        return pd.Timestamp(latest).date()
    except (TypeError, ValueError):
        return None


def _trading_days_between(
    after: date, before: date, trading_day: Callable[[date], bool]
) -> int:
    """Trading days strictly after `after` and strictly before `before`."""
    count = 0
    day = after + timedelta(days=1)
    while day < before:
        if trading_day(day):
            count += 1
        day += timedelta(days=1)
    return count


def _allowed(prior_rows: int) -> int:
    """How many underlyings may be carried or stood in before a run reads as
    a broken or partial file rather than routine F&O churn."""
    share = int(prior_rows * config.FNO_FREEZE_LIMITS_MAX_CHURN_SHARE)
    return max(config.FNO_FREEZE_LIMITS_MAX_CHURN, share)


def _nothing_replaced(gone: pd.DataFrame, _listed: pd.DataFrame) -> pd.Series:
    return pd.Series(False, index=gone.index)


def _no_clash(_out: pd.DataFrame) -> str | None:
    return None


@dataclass(frozen=True)
class _FreezeLeg:
    """What one venue's freeze-limit file does its own way. The guards that
    keep a limit from ever reading too high are `_build_freeze_limits`'s,
    shared by every venue."""

    key: tuple[str, ...]  # a published row's identity, e.g. ("symbol",)
    columns: list[str]  # published, in order (`date` and `source` included)
    schema: pa.Schema  # the types the client reads
    source: str  # provenance stamped on every row
    # Which parsed rows publish, given the prior file (if any).
    admit: Callable[[pd.DataFrame, pd.DataFrame | None], pd.DataFrame]
    # An anomaly only this venue can see (limits, prior, churn allowance):
    # the reason to refuse the run, or None.
    refuse: Callable[[pd.DataFrame, pd.DataFrame | None, int], str | None]
    # The run's summary head, e.g. "12 underlyings (6 indices)".
    describe: Callable[[pd.DataFrame], str]
    # Which rows absent from the file the file still lists under another key
    # (gone rows, listed rows): those are never carried forward.
    replaced: Callable[[pd.DataFrame, pd.DataFrame], pd.Series] = _nothing_replaced
    # A reason the file about to be written is inconsistent, or None.
    clash: Callable[[pd.DataFrame], str | None] = _no_clash


def _key_labels(frame: pd.DataFrame, key: tuple[str, ...]) -> list[str]:
    """Each row's key as one readable label ("NIFTY", "SENSEX/options")."""
    if frame.empty:
        return []
    return list(frame[list(key)].astype(str).agg("/".join, axis=1))


def _build_freeze_limits(
    spec: DatasetSpec,
    target: date,
    leg: _FreezeLeg,
    *,
    fetch_limits: Callable[[date], tuple[date, pd.DataFrame]],
    trading_day: Callable[[date], bool],
    min_rows: int,
) -> RunStatus:
    """Full-rewrite `<file_prefix>_all.parquet` from one venue's contract
    file, under the guards every venue's limits share.

    A limit too high is the failure that matters (the app sizes orders and
    protective stops by it), so:
      - an OLDER file never replaces a newer one (a back-dated run keeps the
        published file); the same day's file read again is published again,
        so an evening correction lands;
      - a row the file still lists never gets an old limit back: one whose
        limit cannot be trusted stands in at one lot (parser), and only one
        ABSENT from the file is carried forward, so a routine F&O exit never
        shrinks the file and never withholds a lowered limit elsewhere;
      - more stand-ins or carried rows than routine churn allows means a
        broken or partial file: nothing is written, `failed`; so does any
        anomaly the venue's own `refuse` names;
      - limits a trading day or more behind are `failed`, loudly, whatever
        the cause (no file, a wrong document, a format change): file D
        serves session D+1, so a run on D+1 holding file D is current.
    Otherwise fail-closed like cas_eligible: a fetch error or wrong document,
    or fewer than `min_rows` underlyings, keeps the prior file
    (`skipped_idempotent`; `failed` on a first run with nothing to keep).
    """
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"
    prior = _constituents_prior(out_path)
    prior_rows = None if prior is None else len(prior)
    prior_day = _prior_file_day(prior)
    key = list(leg.key)

    if not trading_day(target):
        return RunStatus(
            "skipped_holiday",
            target,
            symbol_count=prior_rows or 0,
            source=spec.source_label,
            message="non-trading day; limits not refreshed",
        )

    def behind(day: date | None) -> int:
        return 0 if day is None else _trading_days_between(day, target, trading_day)

    def kept(why: str) -> RunStatus:
        """The prior file stays: quiet while it is current, loud once stale."""
        missed = behind(prior_day)
        if prior_day is not None and missed >= 1:
            return RunStatus(
                "failed",
                target,
                symbol_count=prior_rows or 0,
                source=spec.source_label,
                message=(
                    f"{why}; published limits are from {prior_day.isoformat()}, "
                    f"{missed} trading days behind (prior kept)"
                ),
            )
        return _constituents_fail_closed(spec, target, out_path, prior_rows, why)

    def refused(why: str) -> RunStatus:
        """An anomaly to look at (a format change, a partial file): loud now."""
        return RunStatus(
            "failed",
            target,
            symbol_count=prior_rows or 0,
            source=spec.source_label,
            message=f"{why}; nothing written" + ("; prior kept" if prior_rows else ""),
        )

    try:
        file_day, limits = fetch_limits(target)
    except NotYetPublished as e:
        return RunStatus(
            "failed",
            target,
            symbol_count=prior_rows or 0,
            source=spec.source_label,
            message=f"{e}; limits going stale"
            + (f" (published file is from {prior_day.isoformat()}, kept)" if prior_day else ""),
        )
    except Exception as e:  # noqa: BLE001 - any fetch/parse failure -> fail-closed
        return kept(f"fetch failed: {e}")

    if prior_day is not None and file_day < prior_day:
        return kept(
            f"contract file of {file_day.isoformat()} is older than the published "
            f"{prior_day.isoformat()}"
        )

    limits = leg.admit(limits, prior)
    underlyings = limits["symbol"].nunique()
    if limits.empty:
        return kept("parsed 0 underlyings")
    if underlyings < min_rows:
        return kept(f"parsed {underlyings} underlyings < floor {min_rows} (suspected truncation)")

    churn = _allowed(prior_rows or 0)
    stand_ins = sorted(_key_labels(limits[limits["basis"] == _BASIS_STAND_IN], leg.key))
    if len(stand_ins) > churn:
        return refused(
            f"{len(stand_ins)} underlyings without a trustworthy limit (over {churn}; "
            f"format change?): {', '.join(stand_ins[:15])}"
        )
    anomaly = leg.refuse(limits, prior, churn)
    if anomaly:
        return refused(anomaly)

    out = limits.copy()
    # REQUIRED for the manifest: build_manifest reads columns=["date"].
    out["date"] = pd.Timestamp(file_day)
    out["source"] = leg.source
    out = out[leg.columns]

    carried: list[str] = []
    replaced: list[str] = []
    carriable = set(leg.columns) - {"basis", "source"}
    if prior is not None and carriable <= set(prior.columns):
        listed = pd.MultiIndex.from_frame(out[key])
        gone = prior[~pd.MultiIndex.from_frame(prior[key]).isin(listed)].copy()
        if not gone.empty:
            # Listed again under another key: its old limit must not sit
            # beside the new one.
            superseded = leg.replaced(gone, out)
            replaced = sorted(_key_labels(gone[superseded], leg.key))
            gone = gone[~superseded]
        if not gone.empty:
            carried = sorted(_key_labels(gone, leg.key))
            # Only the ones the last file listed are new: rows carried before
            # keep their older date, and must not count again on every run.
            new_dates = pd.to_datetime(gone["date"], errors="coerce").dt.date
            leaving = sorted(_key_labels(gone[new_dates == prior_day], leg.key))
            if len(leaving) > churn:
                return refused(
                    f"{len(leaving)} underlyings missing from the file (over {churn}; "
                    f"partial file?): {', '.join(leaving[:15])}"
                )
            if "basis" not in gone.columns:
                gone["basis"] = _BASIS_EXCHANGE
            if "source" not in gone.columns:
                gone["source"] = leg.source
            out = pd.concat([out, gone[leg.columns]], ignore_index=True)

    out = out.sort_values(key, kind="mergesort").reset_index(drop=True)
    inconsistent = leg.clash(out)
    if inconsistent:
        return refused(inconsistent)
    _write_atomic_typed(out, out_path, leg.schema)

    notes = ""
    if stand_ins:
        notes += f"; {len(stand_ins)} stand in at one lot: {', '.join(stand_ins[:15])}"
    if replaced:
        notes += f"; {len(replaced)} listed again under another symbol: {', '.join(replaced[:15])}"
    if carried:
        more = f" (+{len(carried) - 15} more)" if len(carried) > 15 else ""
        named = ", ".join(carried[:15]) + more
        notes += f"; {len(carried)} no longer listed, carried forward: {named}"
    summary = f"{leg.describe(limits)} from the {file_day.isoformat()} contract file{notes}"
    missed = behind(file_day)
    if missed >= 1:
        return RunStatus(
            "failed",
            target,
            symbol_count=len(out),
            source=spec.source_label,
            message=f"{summary}; that file is {missed} trading days behind",
        )
    return RunStatus(
        "success", target, symbol_count=len(out), source=spec.source_label, message=summary
    )


def _nse_admit(limits: pd.DataFrame, prior: pd.DataFrame | None) -> pd.DataFrame:
    """NSE's test series (futures-only) never enter; a real underlying that
    lost its options keeps its row."""
    prior_symbols: set[str] = set()
    if prior is not None and "symbol" in prior.columns:
        prior_symbols = set(prior["symbol"].astype(str))
    admitted = limits[limits["has_options"] | limits["symbol"].isin(prior_symbols)]
    return admitted.drop(columns=["has_options"])


_NSE_FREEZE_LEG = _FreezeLeg(
    key=("symbol",),
    columns=_FNO_LIMIT_COLUMNS,
    schema=_FNO_LIMIT_SCHEMA,
    source=nse_fo_contract.SOURCE,
    admit=_nse_admit,
    refuse=lambda _limits, _prior, _churn: None,
    describe=lambda limits: (
        f"{len(limits)} underlyings ({int((limits['kind'] == 'index').sum())} indices)"
    ),
)


def build_fno_freeze_limits(
    spec: DatasetSpec,
    target: date,
    *,
    fetch_limits: Callable[[date], tuple[date, pd.DataFrame]] = _fetch_freeze_limits,
    trading_day: Callable[[date], bool] = _cas_trading_day,
    min_rows: int = config.FNO_FREEZE_LIMITS_MIN_ROWS,
) -> RunStatus:
    """Full-rewrite `fno_freeze_limits_all.parquet`: per NSE F&O underlying,
    its kind (index or stock), lot size, the largest single order NSE takes
    (`max_order_quantity`, a whole number of lots) and its `basis`. `date` is
    the day of the contract file the limits come from (NSE's file D, for
    session D+1).

    The guards are `_build_freeze_limits`'s. NSE's own rule: an underlying
    with no option series is published only if it was before (a real one
    that lost its options), never NSE's test series.
    """
    return _build_freeze_limits(
        spec,
        target,
        _NSE_FREEZE_LEG,
        fetch_limits=fetch_limits,
        trading_day=trading_day,
        min_rows=min_rows,
    )


# BSE's limits, per (underlying, product), in their own file: NSE's file and
# its readers stay as they are.
_BSE_FNO_LIMIT_COLUMNS: list[str] = [*bse_eqd_contract.LIMIT_COLUMNS, "date", "source"]
_BSE_FNO_LIMIT_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("kind", pa.string()),
        ("product", pa.string()),
        ("underlying_id", pa.int64()),
        ("lot_size", pa.int64()),
        ("max_order_quantity", pa.int64()),
        ("basis", pa.string()),
        ("date", pa.timestamp("ms")),
        ("source", pa.string()),
    ]
)


def _fetch_bse_freeze_limits(_target: date) -> tuple[date, pd.DataFrame]:
    """BSE's rolling contract master and the day it is for (named in the
    zip, not by the run: there is no dated archive to walk back through).

    bseindia.com refuses python-requests' own User-Agent (403) and serves the
    file to a browser's; nothing else is needed. Anything but HTTP 200 is
    retried, then raised: the builder keeps the published file.
    """
    last = ""
    with requests.Session() as session:
        session.headers.update({"User-Agent": _BROWSER_UA})
        for attempt in range(_MAX_RETRIES):
            resp = session.get(bse_eqd_contract.URL, timeout=_TIMEOUT)
            if resp.status_code == 200:
                return bse_eqd_contract.parse_freeze_limits(resp.content)
            last = f"HTTP {resp.status_code}"
            if attempt < _MAX_RETRIES - 1:
                time.sleep(2**attempt)
    raise UnexpectedFailure(f"BSE contract master: {last}")


def _not_set(frame: pd.DataFrame) -> pd.DataFrame:
    if "basis" not in frame.columns:
        return frame.iloc[0:0]
    return frame[frame["basis"] == bse_eqd_contract.BASIS_NOT_SET]


def _bse_refuse(limits: pd.DataFrame, prior: pd.DataFrame | None, churn: int) -> str | None:
    """BSE's "not set" value must never pass off a broken file as "no limits":
      - an index always has a limit, so one showing the value fails the run;
      - more products newly showing it than routine churn fails the run."""
    unset = _not_set(limits)
    indices = sorted(_key_labels(unset[unset["kind"] == "index"], ("symbol", "product")))
    if indices:
        return f"an index shows BSE's no-limit value: {', '.join(indices)}"
    if prior is None or not {"symbol", "product"} <= set(prior.columns):
        return None
    before = set(_key_labels(_not_set(prior), ("symbol", "product")))
    newly = sorted(set(_key_labels(unset, ("symbol", "product"))) - before)
    if len(newly) > churn:
        return (
            f"{len(newly)} products newly show no exchange limit (over {churn}; "
            f"broken file?): {', '.join(newly[:15])}"
        )
    return None


def _bse_describe(limits: pd.DataFrame) -> str:
    unset = _not_set(limits)["symbol"].nunique()
    indices = limits.loc[limits["kind"] == "index", "symbol"].nunique()
    noun = "underlying" if unset == 1 else "underlyings"
    return (
        f"{len(limits)} limits for {limits['symbol'].nunique()} underlyings "
        f"({indices} indices; {unset} {noun} with no exchange limit, at one lot)"
    )


# A consumer matches a BSE underlying by underlying_id (ISIN is blank in BSE's
# file), so the published file holds one row per (underlying_id, product).
_BSE_UNDERLYING = ["underlying_id", "product"]


def _bse_replaced(gone: pd.DataFrame, listed: pd.DataFrame) -> pd.Series:
    """Rows absent from the file whose underlying the file still lists (BSE
    renamed its TckrSymb and kept UndrlygFinInstrmId): their old limit is
    never carried beside the new one. 0 is "unreadable", never a match."""
    known = listed[listed["underlying_id"] > 0]
    still_listed = pd.MultiIndex.from_frame(gone[_BSE_UNDERLYING]).isin(
        pd.MultiIndex.from_frame(known[_BSE_UNDERLYING])
    )
    return pd.Series(still_listed, index=gone.index) & (gone["underlying_id"] > 0)


def _bse_clash(out: pd.DataFrame) -> str | None:
    """Two rows for one (underlying_id, product) would let a consumer pick
    either limit: the run fails instead."""
    known = out[out["underlying_id"] > 0]
    twins = known[known.duplicated(_BSE_UNDERLYING, keep=False)]
    if twins.empty:
        return None
    named = sorted(
        f"{row.symbol}/{row.product} ({row.underlying_id})" for row in twins.itertuples()
    )
    return f"{len(twins)} rows share an underlying_id and product: {', '.join(named[:15])}"


_BSE_FREEZE_LEG = _FreezeLeg(
    key=("symbol", "product"),
    columns=_BSE_FNO_LIMIT_COLUMNS,
    schema=_BSE_FNO_LIMIT_SCHEMA,
    source=bse_eqd_contract.SOURCE,
    admit=lambda limits, _prior: limits,  # BSE lists no test series
    refuse=_bse_refuse,
    describe=_bse_describe,
    replaced=_bse_replaced,
    clash=_bse_clash,
)


def build_bse_fno_freeze_limits(
    spec: DatasetSpec,
    target: date,
    *,
    fetch_limits: Callable[[date], tuple[date, pd.DataFrame]] = _fetch_bse_freeze_limits,
    trading_day: Callable[[date], bool] = _cas_trading_day,
    min_rows: int = config.BSE_FNO_FREEZE_LIMITS_MIN_ROWS,
) -> RunStatus:
    """Full-rewrite `bse_fno_freeze_limits_all.parquet`: per BSE F&O
    underlying and product (futures, options), its kind, BSE's underlying
    code, lot size, the largest single order BSE takes (`max_order_quantity`,
    as the exchange gives it) and its `basis`. `date` is the day the contract
    file names (file D, written around 17:40 IST, for session D+1).

    The guards are `_build_freeze_limits`'s, NSE's too. BSE's own: its "not
    set" value is published as one lot (`basis` no_exchange_limit), not
    counted as a stand-in, and fails the run on an index or when more
    products newly show it than churn allows.
    """
    return _build_freeze_limits(
        spec,
        target,
        _BSE_FREEZE_LEG,
        fetch_limits=fetch_limits,
        trading_day=trading_day,
        min_rows=min_rows,
    )


# ── Corporate actions ───────────────────────────────────────────────────────

# The types the client reads (see _write_atomic_typed). `price_factor` is the
# post ÷ pre price ratio when the purpose states it; `first_seen` the trading
# day NSE first listed the action; `date` the as-of the manifest reads.
_CORPORATE_ACTIONS_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("ex_date", pa.date32()),
        ("kind", pa.string()),
        ("price_factor", pa.float64()),
        ("purpose", pa.string()),
        ("first_seen", pa.date32()),
        ("date", pa.timestamp("ms")),
    ]
)
_CORPORATE_ACTION_KEY = ["symbol", "ex_date", "kind"]
_CORPORATE_ACTION_COLUMNS = ["symbol", "ex_date", "kind", "price_factor", "purpose", "first_seen"]


def _fetch_pr_bundle(day: date) -> pd.DataFrame:
    """The book-closure rows of NSE's PR bundle for `day`, through the shared
    warm-session + retry contract. The parser is the gate: an HTML page served
    with 200 raises, so it reaches the builder as a fetch failure; a 404 is
    `NotYetPublished`."""
    session = requests.Session()
    session.headers.update({"User-Agent": _BROWSER_UA})
    return _fetch_with_retry(
        session,
        nse_corporate_actions.pr_url(day),
        day,
        parse=nse_corporate_actions.parse_pr_bundle,
    )


def _trading_days_back(target: date, count: int, trading_day: Callable[[date], bool]) -> list[date]:
    """The `count` trading days ending at `target`, oldest first."""
    days: list[date] = []
    day = target
    while len(days) < count and (target - day).days < count * 3 + 14:
        if trading_day(day):
            days.append(day)
        day -= timedelta(days=1)
    return sorted(days)


def build_corporate_actions(
    spec: DatasetSpec,
    target: date,
    *,
    fetch_bundle: Callable[[date], pd.DataFrame] = _fetch_pr_bundle,
    trading_day: Callable[[date], bool] = _cas_trading_day,
    seed_days: int = config.CORPORATE_ACTIONS_SEED_DAYS,
) -> RunStatus:
    """Accumulate `corporate_actions_all.parquet`: every capital action that
    re-bases a stock's price (split, consolidation, bonus, rights, demerger,
    capital reduction), keyed (symbol, ex_date, kind), from the book-closure
    file in NSE's PR bundle for `target` (sources/nse_corporate_actions.py).

    NSE lists an action one to two weeks before its ex-date, and the app must
    know it before the stock opens ex: the table is the union of every day's
    file, never a snapshot. A first build (no prior file) also reads the
    `seed_days` trading days before `target`, best effort, so actions listed
    before it are there too. An action seen again keeps its first row.

    Fail-closed, keeping the prior file: the target day's bundle missing or
    malformed is `skipped_idempotent` with a prior file and `failed` without
    one. A purpose the classifier does not recognise is never guessed at: the
    run succeeds and names it, so the vocabulary can be extended (RUNBOOK
    corporate_actions).
    """
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"
    prior = _constituents_prior(out_path)
    if prior is not None and not set(_CORPORATE_ACTION_COLUMNS) <= set(prior.columns):
        return _constituents_fail_closed(
            spec, target, out_path, len(prior), "prior file lacks the corporate_actions columns"
        )
    prior_rows = None if prior is None else len(prior)

    if not trading_day(target):
        return RunStatus(
            "skipped_holiday",
            target,
            symbol_count=prior_rows or 0,
            source=spec.source_label,
            message="non-trading day; no book-closure file",
        )

    days = [target] if prior is not None else _trading_days_back(target, seed_days, trading_day)
    listed: list[pd.DataFrame] = []
    seed_misses: list[str] = []
    for day in days:
        try:
            frame = fetch_bundle(day)
        except Exception as e:  # noqa: BLE001 - any fetch/parse failure -> fail-closed
            if day == target:
                return _constituents_fail_closed(
                    spec, target, out_path, prior_rows, f"{day.isoformat()} bundle: {e}"
                )
            seed_misses.append(day.isoformat())
            continue
        listed.append(frame.assign(first_seen=day))

    rows: list[dict[str, object]] = []
    unknown: set[str] = set()
    for frame in listed:
        for symbol, ex_date, purpose, first_seen in zip(
            frame["symbol"], frame["ex_date"], frame["purpose"], frame["first_seen"], strict=True
        ):
            actions = nse_corporate_actions.classify(purpose)
            if actions is None:
                unknown.add(purpose)
                continue
            for action in actions:
                rows.append(
                    {
                        "symbol": symbol,
                        "ex_date": pd.Timestamp(ex_date).date(),
                        "kind": action.kind,
                        "price_factor": action.price_factor,
                        "purpose": purpose,
                        "first_seen": pd.Timestamp(first_seen).date(),
                    }
                )
    found = pd.DataFrame(rows, columns=_CORPORATE_ACTION_COLUMNS)

    parts = [found]
    if prior is not None:
        kept = prior[_CORPORATE_ACTION_COLUMNS].copy()
        kept["ex_date"] = pd.to_datetime(kept["ex_date"]).dt.date
        kept["first_seen"] = pd.to_datetime(kept["first_seen"]).dt.date
        parts.insert(0, kept)
    out = pd.concat([p for p in parts if not p.empty] or [found], ignore_index=True)
    # The first sighting wins: prior rows come first, then oldest days first.
    out = out.drop_duplicates(subset=_CORPORATE_ACTION_KEY, keep="first")
    out = out.sort_values(["ex_date", "symbol", "kind"], kind="mergesort").reset_index(drop=True)
    # REQUIRED for the manifest: build_manifest reads columns=["date"].
    out["date"] = pd.Timestamp(target)
    _write_atomic_typed(out, out_path, _CORPORATE_ACTIONS_SCHEMA)

    upcoming = out[pd.to_datetime(out["ex_date"]).dt.date >= target]
    added = len(out) - (prior_rows or 0)
    summary = (
        f"{len(out)} actions ({added} new, "
        f"{len(upcoming)} with an ex-date from {target.isoformat()})"
    )
    if seed_misses:
        summary += f"; seed days without a bundle: {', '.join(seed_misses)}"
    if unknown:
        named = sorted(unknown)
        more = f" (+{len(named) - 10} more)" if len(named) > 10 else ""
        shown = "; ".join(named[:10]) + more
        summary += f"; purposes not recognised (extend nse_corporate_actions.classify): {shown}"
    return RunStatus(
        "success", target, symbol_count=len(out), source=spec.source_label, message=summary
    )

