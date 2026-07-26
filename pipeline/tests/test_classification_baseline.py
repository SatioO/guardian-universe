from __future__ import annotations

from datetime import date, datetime

from pipeline.sources.classification_baseline import run_approved_baseline
from pipeline.sources.classification_publication import Provenance
from pipeline.sources.screener_classification import ClassificationObservation
from pipeline.sources.screener_collector import (
    CollectedClassification,
    CollectionDeferred,
)


class _Collector:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def collect(self, symbol: str):
        self.calls.append(symbol)
        if symbol == "BLOCKED":
            raise CollectionDeferred("rate limit")
        return CollectedClassification(
            ClassificationObservation("Energy", "Oil, Gas", "Petroleum", "Refining"),
            Provenance(datetime(2026, 7, 26, 10), "https://source", "v1", "hash"),
        )


def test_baseline_stops_requests_and_persists_pending_after_deferral(tmp_path):
    collector = _Collector()
    result = run_approved_baseline(
        (
            b"SYMBOL,SERIES,ISIN NUMBER\nFIRST,EQ,INE000A00001\n"
            b"BLOCKED,EQ,INE000A00002\nLATER,EQ,INE000A00003\n"
        ),
        tmp_path / "classification_registry_all.parquet",
        collector, today=date(2026, 7, 26),
    )

    assert collector.calls == ["FIRST", "BLOCKED"]
    assert result.deferred_reason == "rate limit"
    assert result.result.records["INE000A00001"].last_provenance is not None
    assert result.result.records["INE000A00003"].last_known_good is None
