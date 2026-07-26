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
) -> BaselineRun:
    """Collect active NSE EQ/BE symbols sequentially until the collector defers.

    All symbols after a cap/block are returned as pending by the reconciler; no
    further Screener requests are sent during this invocation.
    """
    deferred_reason: str | None = None


    def collect(symbol: str):
        nonlocal deferred_reason
        if deferred_reason is not None:
            return None
        try:
            return collector.collect(symbol)
        except CollectionDeferred as error:
            deferred_reason = str(error)
            return None


    result = run_incremental_collection(
        load_registry(registry_path),
        parse_active_nse_equities(snapshot_csv),
        today=today,
        collect=collect,
    )
    if result.valid_snapshot:
        write_registry(registry_path, result.records, updated_on=today)
    return BaselineRun(result, deferred_reason)
