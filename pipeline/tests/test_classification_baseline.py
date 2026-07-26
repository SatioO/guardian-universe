from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from pipeline.sources.classification_baseline import run_approved_baseline
from pipeline.sources.classification_publication import Provenance
from pipeline.sources.nse_universe import RegistryEntry, load_registry, write_registry
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
        collector,
        today=date(2026, 7, 26),
    )

    assert collector.calls == ["FIRST", "BLOCKED"]
    assert result.deferred_reason == "rate limit"
    assert result.result.records["INE000A00001"].last_provenance is not None
    later = result.result.records["INE000A00003"]
    assert later.last_known_good is None
    assert later.retry_attempts == 0
    assert later.retry_on is None


def test_manual_batch_only_processes_the_bounded_pending_slice(tmp_path):
    registry_path = tmp_path / "classification_registry_all.parquet"
    retry_on = date(2026, 8, 2)
    write_registry(
        registry_path,
        {
            "INE000A00001": RegistryEntry.pending("INE000A00001", "FIRST", retry_on=retry_on),
            "INE000A00002": RegistryEntry.pending("INE000A00002", "SECOND", retry_on=retry_on),
        },
        updated_on=date(2026, 7, 26),
    )
    collector = _Collector()

    result = run_approved_baseline(
        b"SYMBOL,SERIES,ISIN NUMBER\nFIRST,EQ,INE000A00001\nSECOND,EQ,INE000A00002\n",
        registry_path,
        collector,
        today=date(2026, 7, 26),
        batch_size=1,
        manual_batch=True,
    )

    assert collector.calls == ["FIRST"]
    assert result.result.candidates == ("INE000A00001",)
    assert load_registry(registry_path)["INE000A00002"].retry_on == retry_on


def test_manual_baseline_explicitly_migrates_legacy_unattempted_backlog(tmp_path):
    registry_path = tmp_path / "classification_registry_all.parquet"
    write_registry(
        registry_path,
        {
            "INE000A00001": RegistryEntry.pending("INE000A00001", "FIRST"),
            "INE000A00002": RegistryEntry.pending("INE000A00002", "LATER"),
        },
        updated_on=date(2026, 7, 26),
    )
    legacy = pd.read_parquet(registry_path).drop(columns="baseline_pending")
    legacy.to_parquet(registry_path, compression="zstd", index=False)

    run_approved_baseline(
        b"SYMBOL,SERIES,ISIN NUMBER\nFIRST,EQ,INE000A00001\nLATER,EQ,INE000A00002\n",
        registry_path,
        _Collector(),
        today=date(2026, 7, 26),
        batch_size=1,
        manual_batch=True,
    )

    assert load_registry(registry_path)["INE000A00002"].baseline_pending is True
