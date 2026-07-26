"""Bounded, resumable baseline runner for the approved Classification Registry."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

from pipeline.sources.classification_publication import (
    PublishedClassificationObservation,
    append_observations,
)
from pipeline.sources.nse_universe import (
    IncrementalCollectionResult,
    RegistryStatus,
    load_registry,
    parse_active_nse_equities,
    run_incremental_collection,
    write_registry,
)
from pipeline.sources.screener_collector import (
    CollectedClassification,
    CollectionDeferred,
    ScreenerClassificationCollector,
)


@dataclass(frozen=True)
class BaselineRun:
    result: IncrementalCollectionResult
    deferred_reason: str | None


def run_approved_baseline(
    snapshot_csv: bytes,
    registry_path: Path,
    collector: ScreenerClassificationCollector,
    *,
    today: date,
    batch_size: int = 25,
    manual_batch: bool = False,
    full_audit: bool = False,
) -> BaselineRun:
    """Collect a bounded NSE EQ/BE baseline batch until the collector defers.

    A manual batch makes pending records eligible for this invocation only. A
    quarterly audit makes every active record eligible. Neither mode resets
    retry state for symbols outside the bounded batch.
    """
    if batch_size < 1 or batch_size > 25:
        raise ValueError("baseline batch_size must be between 1 and 25")
    deferred_reason: str | None = None
    observations: list[PublishedClassificationObservation] = []

    records = load_registry(registry_path)
    initial_baseline = not records
    if manual_batch:
        # This is the sole, operator-approved migration path for a v1
        # registry. A daily run never guesses whether an untouched pending row
        # is baseline backlog or a freshly listed equity.
        records = {
            instrument_key: (
                replace(entry, baseline_pending=True)
                if (
                    entry.status is RegistryStatus.PENDING
                    and entry.last_known_good is None
                    and entry.retry_attempts == 0
                    and entry.retry_on is None
                )
                else entry
            )
            for instrument_key, entry in records.items()
        }

    def collect(symbol: str):
        nonlocal deferred_reason
        if deferred_reason is not None:
            return None
        try:
            collected = collector.collect(symbol)
        except CollectionDeferred as error:
            deferred_reason = str(error)
            return error
        if isinstance(collected, CollectedClassification):
            observation = collected.observation
            observations.append(
                PublishedClassificationObservation(
                    instrument_key="",
                    symbol=symbol,
                    macro_sector=observation.macro_sector,
                    sector=observation.sector,
                    industry=observation.industry,
                    basic_industry=observation.basic_industry,
                    provenance=collected.provenance,
                )
            )
        return collected

    result = run_incremental_collection(
        records,
        parse_active_nse_equities(snapshot_csv),
        today=today,
        collect=collect,
        candidate_limit=batch_size,
        force_pending=manual_batch,
        force_all_active=full_audit,
        baseline_batch=manual_batch or initial_baseline,
    )
    if result.valid_snapshot:
        write_registry(registry_path, result.records, updated_on=today)
        by_symbol = {
            entry.symbol: instrument_key for instrument_key, entry in result.records.items()
        }
        append_observations(
            registry_path.parent / "classification_observations_all.parquet",
            tuple(
                PublishedClassificationObservation(
                    instrument_key=by_symbol[observation.symbol],
                    symbol=observation.symbol,
                    macro_sector=observation.macro_sector,
                    sector=observation.sector,
                    industry=observation.industry,
                    basic_industry=observation.basic_industry,
                    provenance=observation.provenance,
                )
                for observation in observations
            ),
        )
    return BaselineRun(result, deferred_reason)
