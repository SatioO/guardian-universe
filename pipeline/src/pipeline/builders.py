"""Registry of derived-dataset builders, keyed by DatasetSpec.key.

Populated by later G1b tasks (6: reference/instruments, 7: ca_flags). This
module intentionally imports daily_update.RunStatus and datasets.DatasetSpec
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
bindings (`BUILDERS["reference"]`, `BUILDERS["ca_flags"]`) bind `source_spec`
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

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from pipeline import config, store
from pipeline.daily_update import RunStatus
from pipeline.datasets import DatasetSpec
from pipeline.fetch import _BROWSER_UA, _fetch_with_retry
from pipeline.sources import classification_publication, nse_constituents, nse_sector
from pipeline.sources.classification_registry import ClassificationRegistryRecord
from pipeline.sources.nse_universe import (
    RegistryEntry,
    RegistryStatus,
    active_classification_records,
    load_registry,
    write_registry,
)

BUILDERS: dict[str, Callable[[DatasetSpec, date], RunStatus]] = {}

# 10 most-recent DISTINCT trading dates present in the source store. v1 keeps
# this holiday-free and calendar-free (builders have no `holidays` input,
# unlike run_daily): "trading day" here means "a date that actually appears
# in the store", which by construction only ever contains trading days.
_ACTIVE_WINDOW = 10

_REFERENCE_COLUMNS = ["date", "instrument_key", "isin", "symbol", "series"]

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
    state, e.g. the very first backfill day)."""
    base = source_spec.base_dir
    if not base.exists():
        return pd.DataFrame(columns=_REFERENCE_COLUMNS)
    frames = [
        pd.read_parquet(p, columns=_REFERENCE_COLUMNS)
        for p in sorted(base.glob(f"{source_spec.file_prefix}_*.parquet"))
    ]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=_REFERENCE_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def build_reference(spec: DatasetSpec, target: date, *, source_spec: DatasetSpec) -> RunStatus:
    """Build the `reference/instruments` SCD2 symbol master from the source
    (equities) store's own presence -- one row per distinct
    `(instrument_key, symbol, series)` version.

    v1 status subset: only `active`/`inactive` are ever emitted. `active`
    means `last_seen` falls among the store's own 10 most recent distinct
    dates (no holiday calendar dependency -- the store's date values ARE
    trading days by construction). `suspended`/`delisted` need an external
    exchange feed and are deferred to a later phase.

    Full rewrite each run: `instruments_all.parquet` is atomically replaced
    (tmp+rename) under `spec.base_dir`, never appended to -- idempotent at
    the scale of a few thousand rows.
    """
    df = _read_all_years(source_spec)

    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"

    if df.empty:
        out = _empty_reference_frame()
        _write_atomic(out, out_path)
        return RunStatus("success", target, symbol_count=0, source="derived")

    recent_dates = sorted(df["date"].drop_duplicates(), reverse=True)[:_ACTIVE_WINDOW]
    active_dates = set(recent_dates)

    grouped = df.groupby(["instrument_key", "symbol", "series"], as_index=False).agg(
        first_seen=("date", "min"), last_seen=("date", "max"), isin=("isin", "last")
    )
    grouped["name"] = grouped["symbol"]
    grouped["status"] = grouped["last_seen"].apply(
        lambda d: "active" if d in active_dates else "inactive"
    )
    grouped["valid_from"] = grouped["first_seen"]
    grouped["valid_to"] = grouped["last_seen"]
    grouped["date"] = grouped["last_seen"]

    out = (
        grouped[
            [
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
            ]
        ]
        .sort_values(["instrument_key", "first_seen"])
        .reset_index(drop=True)
    )

    _write_atomic(out, out_path)

    return RunStatus("success", target, symbol_count=len(out), source="derived")


def _empty_reference_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
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
        ]
    )


def _write_atomic(df: pd.DataFrame, target: Path) -> None:
    tmp = target.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, compression="zstd", index=False)
    tmp.replace(target)


_CA_FLAGS_JOIN_COLUMNS = ["date", "instrument_key", "close", "prevclose"]
_CA_FLAGS_OUTPUT_COLUMNS = [
    "date",
    "instrument_key",
    "close_prev",
    "prevclose_today",
    "implied_ratio",
]


def build_ca_flags(spec: DatasetSpec, target: date, *, source_spec: DatasetSpec) -> RunStatus:
    """Corporate-action ex-date detector: flag instruments whose today's
    prevclose implies a discontinuity vs the previous trading day's close --
    a split, bonus, or other ex-date event, not ordinary price movement.

    "Previous trading day" = the max date present in the source store that is
    strictly less than `target` (store dates ARE trading days by
    construction -- no holiday calendar dependency, same v1 posture as the
    reference builder). Only instrument_keys present on BOTH days are joined;
    a key with no prior day (new listing, or the very first backfill day
    overall) is simply never flagged. Zero flags, or no previous day at all,
    are both a clean `success` with `symbol_count=0` -- not a failure.

    Appends (never overwrites) via `store.append_keyed`, deduped on
    (date, instrument_key) -- idempotent re-runs for the same target date
    replace that date's flags rather than duplicating them.

    Known limitation (dual-key join, until reference-remap linking lands in
    P4a): an instrument that switches its `instrument_key` between days (e.g.
    the `NSE:{symbol}` sentinel resolving to its real ISIN once one appears)
    is absent from the same-key join on the day of the switch, so a
    corporate action coinciding with that switch is silently missed that day.
    """
    df = _read_all_years_for_ca_flags(source_spec)

    if df.empty:
        return RunStatus("success", target, symbol_count=0, source="derived")

    target_ts = pd.Timestamp(target)
    prior_dates = df.loc[df["date"] < target_ts, "date"]
    if prior_dates.empty:
        return RunStatus("success", target, symbol_count=0, source="derived")
    prev_day = prior_dates.max()

    today = df[df["date"] == target_ts][["instrument_key", "prevclose"]]
    prev = df[df["date"] == prev_day][["instrument_key", "close"]]
    if today.empty or prev.empty:
        return RunStatus("success", target, symbol_count=0, source="derived")

    joined = today.merge(prev, on="instrument_key", how="inner", suffixes=("_today", "_prev"))
    if joined.empty:
        return RunStatus("success", target, symbol_count=0, source="derived")

    joined["implied_ratio"] = joined["close"] / joined["prevclose"]
    deviation = (joined["prevclose"] / joined["close"] - 1).abs()
    flagged = joined[deviation > config.CA_DISCONTINUITY_THRESHOLD]

    if flagged.empty:
        return RunStatus("success", target, symbol_count=0, source="derived")

    out = pd.DataFrame(
        {
            "date": target_ts,
            "instrument_key": flagged["instrument_key"].to_numpy(),
            "close_prev": flagged["close"].to_numpy(),
            "prevclose_today": flagged["prevclose"].to_numpy(),
            "implied_ratio": flagged["implied_ratio"].to_numpy(),
        }
    )[_CA_FLAGS_OUTPUT_COLUMNS]

    store.append_keyed(out, spec.base_dir, prefix=spec.file_prefix)

    return RunStatus("success", target, symbol_count=len(out), source="derived")


def _read_all_years_for_ca_flags(source_spec: DatasetSpec) -> pd.DataFrame:
    """Column-pruned read of the source store restricted to the columns
    build_ca_flags needs. Missing/empty store -> empty frame (a legitimate
    state, e.g. the very first backfill day)."""
    base = source_spec.base_dir
    if not base.exists():
        return pd.DataFrame(columns=_CA_FLAGS_JOIN_COLUMNS)
    frames = [
        pd.read_parquet(p, columns=_CA_FLAGS_JOIN_COLUMNS)
        for p in sorted(base.glob(f"{source_spec.file_prefix}_*.parquet"))
    ]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=_CA_FLAGS_JOIN_COLUMNS)
    return pd.concat(frames, ignore_index=True)


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
    if prior_rows is not None and len(df) < prior_rows:
        if len(df) < prior_rows * 0.99:
            return RunStatus(
                "failed",
                target,
                symbol_count=prior_rows,
                source="nse-sector",
                message=(
                    f"parsed {len(df)} rows < 99% of prior {prior_rows}; "
                    "coverage guard rejected publication"
                ),
            )
        return _sector_fail_closed(
            target,
            out_path,
            prior_rows,
            f"parsed {len(df)} rows < prior {prior_rows} (shrink-guard)",
        )

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


def _constituents_prior(out_path: Path) -> pd.DataFrame | None:
    if not out_path.exists():
        return None
    try:
        return pd.read_parquet(out_path)
    except Exception:  # noqa: BLE001 - a corrupt prior file is treated as absent
        return None


def build_index_constituents(
    spec: DatasetSpec,
    target: date,
    *,
    fetch_lists: Callable[[date], pd.DataFrame] | None = None,
    ttl_days: int = config.CONSTITUENTS_REFRESH_TTL_DAYS,
    min_rows: int = config.CONSTITUENTS_MIN_ROWS,
) -> RunStatus:
    """Fetch every catalogued index's published basket and full-rewrite
    `index_constituents_all.parquet`.

    Weekly TTL: index rebalances are semi-annual (March/September) with ad-hoc
    corporate-action changes between, so re-fetching ~134 CSVs daily would poll
    a near-static resource for nothing. Within the TTL this is a no-op.

    Fail-closed, at TWO levels, because the two failures are different:
      - PER INDEX: a failed fetch or malformed payload keeps that index's prior
        rows and is counted, never published as an empty basket. An index whose
        drill silently empties looks like a real (empty) answer to a user.
      - PER RUN: zero rows, a total under `min_rows`, or a shrink against the
        stored file holds the whole write back — the same wall
        build_sector_industry puts up, and the publish shrink-guard besides.
    """
    spec.base_dir.mkdir(parents=True, exist_ok=True)
    out_path = spec.base_dir / f"{spec.file_prefix}_all.parquet"
    prior = _constituents_prior(out_path)
    prior_rows = None if prior is None else len(prior)

    # The TTL exists to avoid re-fetching ~134 CSVs daily; it must NOT pin the
    # CATALOG's metadata. Curation (rotation_list, display_label) is seed data
    # that reaches the parquet only through a rebuild, so an edited seed beats
    # the TTL — otherwise a curation change would sit invisible for a week.
    seed_newer = (
        config.CONSTITUENTS_CATALOG_PATH.exists()
        and out_path.exists()
        and config.CONSTITUENTS_CATALOG_PATH.stat().st_mtime > out_path.stat().st_mtime
    )
    if _sector_is_fresh(out_path, target, ttl_days) and not seed_newer:
        return RunStatus(
            "skipped_idempotent",
            target,
            symbol_count=prior_rows or 0,
            source=spec.source_label,
            message=f"within {ttl_days}-day TTL; not re-fetched",
        )

    try:
        df = (fetch_lists or _fetch_all_constituent_lists)(target)
    except Exception as e:  # noqa: BLE001 - any failure -> fail-closed
        return _constituents_fail_closed(spec, target, out_path, prior_rows, f"fetch failed: {e}")

    if df.empty:
        return _constituents_fail_closed(spec, target, out_path, prior_rows, "parsed 0 valid rows")
    if len(df) < min_rows:
        return _constituents_fail_closed(
            spec, target, out_path, prior_rows,
            f"parsed {len(df)} rows < floor {min_rows} (suspected partial run)",
        )

    # Carry forward any index the run could not reach, so one unreachable host
    # never deletes a basket that was fine yesterday.
    if prior is not None:
        missing = set(prior["index_key"]) - set(df["index_key"])
        if missing:
            carried = prior[prior["index_key"].isin(missing)]
            df = pd.concat([df, carried], ignore_index=True)

    if prior_rows is not None and len(df) < prior_rows:
        return _constituents_fail_closed(
            spec, target, out_path, prior_rows,
            f"parsed {len(df)} rows < prior {prior_rows} (shrink-guard)",
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
    out["date"] = pd.Timestamp(target)
    out = out[[*nse_constituents.CONSTITUENT_COLUMNS, "date"]]
    _write_atomic(out, out_path)
    return RunStatus(
        "ok",
        target,
        symbol_count=len(out),
        source=spec.source_label,
        message=f"{out['index_key'].nunique()} indices",
    )


def _constituents_fail_closed(
    spec: DatasetSpec, target: date, out_path: Path, prior_rows: int | None, why: str
) -> RunStatus:
    """Keep the prior file when there is one; fail loudly when there is not."""
    if out_path.exists() and prior_rows:
        return RunStatus(
            "skipped", target, symbol_count=prior_rows, source=spec.source_label,
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
