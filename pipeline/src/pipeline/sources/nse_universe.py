"""Incremental Active NSE Universe reconciliation for the Classification Registry."""

from __future__ import annotations

import csv
import io
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, cast

import pandas as pd

from pipeline.sources.classification_publication import Provenance
from pipeline.sources.classification_registry import (
    ClassificationRegistryRecord,
    apply_observation,
)
from pipeline.sources.screener_classification import ClassificationObservation
from pipeline.sources.screener_collector import CollectedClassification, CollectionDeferred


class RegistryStatus(StrEnum):
    CLASSIFIED = "classified"
    PENDING = "pending"
    INACTIVE = "inactive"


@dataclass(frozen=True)
class ActiveNseEquity:
    instrument_key: str
    symbol: str


@dataclass(frozen=True)
class RegistryEntry:
    instrument_key: str
    symbol: str
    status: RegistryStatus
    last_known_good: ClassificationRegistryRecord | None
    last_provenance: Provenance | None = None
    baseline_pending: bool = False
    last_audit_on: date | None = None
    consecutive_absences: int = 0
    retry_on: date | None = None
    retry_attempts: int = 0
    deferred_to_full_audit: bool = False

    @classmethod
    def classified(
        cls, instrument_key: str, symbol: str, observation: ClassificationObservation
    ) -> RegistryEntry:
        return cls(
            instrument_key=instrument_key,
            symbol=symbol,
            status=RegistryStatus.CLASSIFIED,
            last_known_good=apply_observation(
                None,
                instrument_key=instrument_key,
                symbol=symbol,
                observation=observation,
            ),
        )

    @classmethod
    def pending(
        cls,
        instrument_key: str,
        symbol: str,
        *,
        retry_on: date | None = None,
        baseline_pending: bool = False,
    ) -> RegistryEntry:
        return cls(
            instrument_key=instrument_key,
            symbol=symbol,
            status=RegistryStatus.PENDING,
            last_known_good=None,
            retry_on=retry_on,
            baseline_pending=baseline_pending,
        )


@dataclass(frozen=True)
class IncrementalCollectionResult:
    records: dict[str, RegistryEntry]
    valid_snapshot: bool
    candidates: tuple[str, ...]
    attempted: tuple[str, ...] = ()


_RETRY_DELAYS = (1, 3, 7)
_EQUITY_SERIES = frozenset({"EQ", "BE"})
_BOOTSTRAP_SNAPSHOT_SIZE = 20
_MIN_SNAPSHOT_OVERLAP = 0.95


_REGISTRY_COLUMNS = [
    "instrument_key",
    "symbol",
    "status",
    "macro_sector",
    "sector",
    "industry",
    "basic_industry",
    "observed_at",
    "source_url",
    "extractor_version",
    "source_fragment_hash",
    "baseline_pending",
    "last_audit_on",
    "consecutive_absences",
    "retry_on",
    "retry_attempts",
    "deferred_to_full_audit",
    "date",
]


def parse_active_nse_equities(csv_bytes: bytes) -> tuple[ActiveNseEquity, ...]:
    """Parse the official NSE EQUITY_L snapshot into the active equity universe."""
    reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig", errors="replace")))
    equities: list[ActiveNseEquity] = []
    for row in reader:
        normalized = {
            (key or "").strip().upper(): (value or "").strip() for key, value in row.items()
        }
        if normalized.get("SERIES") not in _EQUITY_SERIES:
            continue
        instrument_key = normalized.get("ISIN NUMBER", "").upper()
        symbol = normalized.get("SYMBOL", "").upper()
        if instrument_key and symbol:
            equities.append(ActiveNseEquity(instrument_key, symbol))
    return tuple(equities)


def write_registry(path: Path, records: Mapping[str, RegistryEntry], *, updated_on: date) -> None:
    """Atomically persist all registry state needed by the next daily run."""
    rows: list[dict[str, object]] = []
    for instrument_key, entry in sorted(records.items()):
        classification = entry.last_known_good
        rows.append(
            {
                "instrument_key": instrument_key,
                "symbol": entry.symbol,
                "status": entry.status.value,
                "macro_sector": classification.macro_sector if classification else None,
                "sector": classification.sector if classification else None,
                "industry": classification.industry if classification else None,
                "basic_industry": classification.basic_industry if classification else None,
                "observed_at": provenance.observed_at
                if (provenance := entry.last_provenance)
                else None,
                "source_url": provenance.source_url if provenance else None,
                "extractor_version": provenance.extractor_version if provenance else None,
                "source_fragment_hash": provenance.source_fragment_hash if provenance else None,
                "baseline_pending": entry.baseline_pending,
                "last_audit_on": entry.last_audit_on.isoformat() if entry.last_audit_on else None,
                "consecutive_absences": entry.consecutive_absences,
                "retry_on": entry.retry_on.isoformat() if entry.retry_on else None,
                "retry_attempts": entry.retry_attempts,
                "deferred_to_full_audit": entry.deferred_to_full_audit,
                "date": pd.Timestamp(updated_on),
            }
        )
    frame = pd.DataFrame(rows, columns=_REGISTRY_COLUMNS)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    frame.to_parquet(temporary_path, compression="zstd", index=False)
    temporary_path.replace(path)


def load_registry(path: Path) -> dict[str, RegistryEntry]:
    """Load persisted registry state; an absent state is a valid first run."""
    if not path.exists():
        return {}
    frame = pd.read_parquet(path)
    if "baseline_pending" not in frame:
        frame["baseline_pending"] = False
    frame = frame.reindex(columns=_REGISTRY_COLUMNS)
    records: dict[str, RegistryEntry] = {}
    for row in frame.itertuples(index=False):

        def label(value: object) -> str:
            if value is None or value is pd.NA:
                return ""
            if isinstance(value, float) and pd.isna(value):
                return ""
            return str(value)

        classification_values = (
            row.macro_sector,
            row.sector,
            row.industry,
            row.basic_industry,
        )
        classification = (
            None
            if all(pd.isna(value) for value in classification_values)
            else ClassificationRegistryRecord(
                instrument_key=str(row.instrument_key),
                symbol=str(row.symbol),
                macro_sector=label(row.macro_sector),
                sector=label(row.sector),
                industry=label(row.industry),
                basic_industry=label(row.basic_industry),
            )
        )
        retry_on = None if pd.isna(row.retry_on) else pd.Timestamp(cast(Any, row.retry_on)).date()
        last_audit_on = (
            None
            if pd.isna(row.last_audit_on)
            else pd.Timestamp(cast(Any, row.last_audit_on)).date()
        )
        provenance = (
            None
            if pd.isna(row.observed_at)
            else Provenance(
                observed_at=pd.Timestamp(cast(Any, row.observed_at)).to_pydatetime(),
                source_url=str(row.source_url),
                extractor_version=str(row.extractor_version),
                source_fragment_hash=str(row.source_fragment_hash),
            )
        )
        instrument_key = str(row.instrument_key)
        records[instrument_key] = RegistryEntry(
            instrument_key=instrument_key,
            symbol=str(row.symbol),
            status=RegistryStatus(str(row.status)),
            last_known_good=classification,
            last_provenance=provenance,
            baseline_pending=bool(row.baseline_pending),
            last_audit_on=last_audit_on,
            consecutive_absences=int(cast(Any, row.consecutive_absences)),
            retry_on=retry_on,
            retry_attempts=int(cast(Any, row.retry_attempts)),
            deferred_to_full_audit=bool(row.deferred_to_full_audit),
        )
    return records


def registry_needs_baseline_migration(path: Path) -> bool:
    """Whether an older registry must be explicitly migrated by baseline mode."""
    if not path.exists():
        return False
    return "baseline_pending" not in pd.read_parquet(path).columns


def active_classification_records(
    records: Mapping[str, RegistryEntry],
) -> dict[str, ClassificationRegistryRecord]:
    """Return active Last-Known-Good rows, with the current NSE symbol applied."""
    return {
        instrument_key: replace(entry.last_known_good, symbol=entry.symbol)
        for instrument_key, entry in records.items()
        if entry.status is not RegistryStatus.INACTIVE and entry.last_known_good is not None
    }


def run_incremental_collection(
    records: Mapping[str, RegistryEntry],
    snapshot: Iterable[ActiveNseEquity],
    *,
    today: date,
    collect: Callable[
        [str], ClassificationObservation | CollectedClassification | CollectionDeferred | None
    ],
    candidate_limit: int | None = None,
    force_pending: bool = False,
    force_all_active: bool = False,
    baseline_batch: bool = False,
) -> IncrementalCollectionResult:
    """Reconcile one validated NSE snapshot and collect only eligible symbols.

    ``force_pending`` is reserved for an operator-approved baseline batch. It
    makes pending rows eligible without changing retry state outside the
    bounded candidate slice. ``force_all_active`` is the quarterly audit mode.
    """
    equities = tuple(snapshot)
    if not _valid_snapshot(equities, records):
        return IncrementalCollectionResult(dict(records), False, ())

    reconciled = dict(records)
    active = {equity.instrument_key: equity for equity in equities}
    candidates: list[str] = []

    for instrument_key, equity in active.items():
        entry = reconciled.get(instrument_key)
        if entry is None:
            entry = RegistryEntry.pending(
                instrument_key,
                equity.symbol,
                baseline_pending=baseline_batch,
            )
            candidates.append(instrument_key)
        else:
            reactivated = entry.status is RegistryStatus.INACTIVE
            renamed = entry.symbol != equity.symbol
            due_retry = entry.status is RegistryStatus.PENDING and (
                force_pending
                or (
                    not entry.baseline_pending
                    and not entry.deferred_to_full_audit
                    and (entry.retry_on is None or entry.retry_on <= today)
                )
            )
            status = RegistryStatus.CLASSIFIED if entry.last_known_good else RegistryStatus.PENDING
            entry = replace(
                entry,
                symbol=equity.symbol,
                status=status,
                consecutive_absences=0,
            )
            due_audit = force_all_active and (
                entry.last_audit_on is None or entry.last_audit_on < _quarter_start(today)
            )
            if reactivated or due_audit or renamed or due_retry:
                candidates.append(instrument_key)
        reconciled[instrument_key] = entry

    for instrument_key, entry in tuple(reconciled.items()):
        if instrument_key in active:
            continue
        absences = entry.consecutive_absences + 1
        reconciled[instrument_key] = replace(
            entry,
            consecutive_absences=absences,
            status=RegistryStatus.INACTIVE if absences >= 2 else entry.status,
        )

    if candidate_limit is not None:
        candidates = candidates[:candidate_limit]

    attempted: list[str] = []
    for instrument_key in candidates:
        entry = reconciled[instrument_key]
        attempted.append(instrument_key)
        collected = collect(entry.symbol)
        if isinstance(collected, CollectionDeferred):
            break
        observation: ClassificationObservation | None
        provenance: Provenance | None
        if isinstance(collected, CollectedClassification):
            observation = collected.observation
            provenance = collected.provenance
        else:
            observation = collected
            provenance = entry.last_provenance
        last_known_good = apply_observation(
            entry.last_known_good,
            instrument_key=instrument_key,
            symbol=entry.symbol,
            observation=observation,
        )
        if observation is not None:
            reconciled[instrument_key] = replace(
                entry,
                status=RegistryStatus.CLASSIFIED,
                last_known_good=last_known_good,
                last_provenance=provenance,
                baseline_pending=False,
                last_audit_on=today if force_all_active else entry.last_audit_on,
                retry_on=None,
                retry_attempts=0,
            )
            continue
        attempts = entry.retry_attempts + 1
        reconciled[instrument_key] = replace(
            entry,
            status=RegistryStatus.PENDING,
            last_known_good=last_known_good,
            baseline_pending=False,
            last_audit_on=today if force_all_active else entry.last_audit_on,
            retry_attempts=attempts,
            retry_on=_retry_on(today, attempts),
            deferred_to_full_audit=attempts > len(_RETRY_DELAYS),
        )

    return IncrementalCollectionResult(
        reconciled,
        True,
        tuple(candidates),
        tuple(attempted),
    )


def _valid_snapshot(
    snapshot: tuple[ActiveNseEquity, ...], records: Mapping[str, RegistryEntry]
) -> bool:
    if not snapshot:
        return False
    keys = [equity.instrument_key for equity in snapshot]
    if len(keys) != len(set(keys)):
        return False
    if any(not equity.instrument_key or not equity.symbol for equity in snapshot):
        return False
    prior_active = {
        instrument_key
        for instrument_key, entry in records.items()
        if entry.status is not RegistryStatus.INACTIVE
    }
    if not prior_active:
        return True
    overlap = len(set(keys) & prior_active)
    if len(prior_active) < _BOOTSTRAP_SNAPSHOT_SIZE:
        return overlap > 0
    return (
        len(snapshot) >= len(prior_active) * _MIN_SNAPSHOT_OVERLAP
        and overlap >= len(prior_active) * _MIN_SNAPSHOT_OVERLAP
    )


def _retry_on(today: date, attempts: int) -> date | None:
    if attempts > len(_RETRY_DELAYS):
        return None
    return today + timedelta(days=_RETRY_DELAYS[attempts - 1])


def _quarter_start(today: date) -> date:
    month = 3 * ((today.month - 1) // 3) + 1
    return date(today.year, month, 1)
