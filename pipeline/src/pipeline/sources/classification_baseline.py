"""Bounded, resumable baseline runner for the approved Classification Registry."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pipeline.sources.nse_universe import (
    IncrementalCollectionResult,
    load_registry,
    parse_active_nse_equities,
    run_incremental_collection,
    write_registry,
)
from pipeline.sources.screener_collector import (
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
) -> BaselineRun:
    """Collect a bounded NSE EQ/BE baseline batch until the collector defers.

    A manual batch makes pending records eligible for this invocation only. It
    never resets retry state for symbols outside the bounded batch.
    """
    if batch_size < 1 or batch_size > 25:
        raise ValueError("baseline batch_size must be between 1 and 25")
    deferred_reason: str | None = None

    records = load_registry(registry_path)

    def collect(symbol: str):
        nonlocal deferred_reason
        if deferred_reason is not None:
            return None
        try:
            return collector.collect(symbol)
        except CollectionDeferred as error:
            deferred_reason = str(error)
            return error

    result = run_incremental_collection(
        records,
        parse_active_nse_equities(snapshot_csv),
        today=today,
        collect=collect,
        candidate_limit=batch_size,
        force_pending=manual_batch,
    )
    if result.valid_snapshot:
        write_registry(registry_path, result.records, updated_on=today)
    return BaselineRun(result, deferred_reason)
