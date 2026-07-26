"""Incremental Active NSE Universe reconciliation for the Classification Registry."""
from __future__ import annotations

import csv
import io
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date, timedelta
from enum import StrEnum

from pipeline.sources.classification_registry import (
    ClassificationRegistryRecord,
    apply_observation,
)
from pipeline.sources.screener_classification import ClassificationObservation


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
        cls, instrument_key: str, symbol: str, *, retry_on: date | None = None
    ) -> RegistryEntry:
        return cls(
            instrument_key=instrument_key,
            symbol=symbol,
            status=RegistryStatus.PENDING,
            last_known_good=None,
            retry_on=retry_on,
        )


@dataclass(frozen=True)
class IncrementalCollectionResult:
    records: dict[str, RegistryEntry]
    valid_snapshot: bool
    candidates: tuple[str, ...]


_RETRY_DELAYS = (1, 3, 7)
_EQUITY_SERIES = frozenset({"EQ", "BE"})
_BOOTSTRAP_SNAPSHOT_SIZE = 20
_MIN_SNAPSHOT_OVERLAP = 0.95


def parse_active_nse_equities(csv_bytes: bytes) -> tuple[ActiveNseEquity, ...]:
    """Parse the official NSE EQUITY_L snapshot into the active equity universe."""
    reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig", errors="replace")))
    equities: list[ActiveNseEquity] = []
    for row in reader:
        normalized = {
            (key or "").strip().upper(): (value or "").strip()
            for key, value in row.items()
        }
        if normalized.get("SERIES") not in _EQUITY_SERIES:
            continue
        instrument_key = normalized.get("ISIN NUMBER", "").upper()
        symbol = normalized.get("SYMBOL", "").upper()
        if instrument_key and symbol:
            equities.append(ActiveNseEquity(instrument_key, symbol))
    return tuple(equities)


def run_incremental_collection(
    records: Mapping[str, RegistryEntry],
    snapshot: Iterable[ActiveNseEquity],
    *,
    today: date,
    collect: Callable[[str], ClassificationObservation | None],
) -> IncrementalCollectionResult:
    """Reconcile one validated NSE snapshot and collect only eligible symbols."""
    equities = tuple(snapshot)
    if not _valid_snapshot(equities, records):
        return IncrementalCollectionResult(dict(records), False, ())

    reconciled = dict(records)
    active = {equity.instrument_key: equity for equity in equities}
    candidates: list[str] = []

    for instrument_key, equity in active.items():
        entry = reconciled.get(instrument_key)
        if entry is None:
            entry = RegistryEntry.pending(instrument_key, equity.symbol)
            candidates.append(instrument_key)
        else:
            renamed = entry.symbol != equity.symbol
            due_retry = (
                entry.status is RegistryStatus.PENDING
                and not entry.deferred_to_full_audit
                and (entry.retry_on is None or entry.retry_on <= today)
            )
            status = RegistryStatus.CLASSIFIED if entry.last_known_good else RegistryStatus.PENDING
            entry = replace(
                entry,
                symbol=equity.symbol,
                status=status,
                consecutive_absences=0,
            )
            if renamed or due_retry:
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

    for instrument_key in candidates:
        entry = reconciled[instrument_key]
        observation = collect(entry.symbol)
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
                retry_on=None,
                retry_attempts=0,
            )
            continue
        attempts = entry.retry_attempts + 1
        reconciled[instrument_key] = replace(
            entry,
            status=RegistryStatus.PENDING,
            last_known_good=last_known_good,
            retry_attempts=attempts,
            retry_on=_retry_on(today, attempts),
            deferred_to_full_audit=attempts > len(_RETRY_DELAYS),
        )

    return IncrementalCollectionResult(reconciled, True, tuple(candidates))


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
