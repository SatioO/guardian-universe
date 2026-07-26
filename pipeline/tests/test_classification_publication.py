from __future__ import annotations

from datetime import datetime

from pipeline.sources.classification_publication import (
    Provenance,
    append_observations,
    decide_publication,
)
from pipeline.sources.classification_registry import ClassificationRegistryRecord


def _record(
    instrument_key: str = "INE002A01018",
    symbol: str = "RELIANCE",
    sector: str = "Oil, Gas & Consumable Fuels",
) -> ClassificationRegistryRecord:
    return ClassificationRegistryRecord(
        instrument_key=instrument_key,
        symbol=symbol,
        macro_sector="Energy",
        sector=sector,
        industry="Petroleum Products",
        basic_industry="Refineries & Marketing",
    )


def _provenance() -> Provenance:
    return Provenance(
        observed_at=datetime(2026, 7, 24, 10, 0),
        source_url="https://www.screener.in/company/RELIANCE/consolidated/",
        extractor_version="screener-peer-titles-v1",
        source_fragment_hash="abc123",
    )


def _registry(count: int = 10) -> dict[str, ClassificationRegistryRecord]:
    return {
        f"INE{i:08d}A": _record(
            f"INE{i:08d}A",
            f"STOCK{i}",
            ("Energy", "Materials", "Financial Services")[i % 3],
        )
        for i in range(count)
    }


def test_identical_active_classifications_skip_publication():
    decision = decide_publication({"INE002A01018": _record()}, {"INE002A01018": _record()}, {})

    assert decision.publish is False
    assert decision.reason == "fingerprint unchanged"


def test_changed_classification_publishes_with_auditable_observation():
    previous = _registry()
    current = {
        **previous,
        "INE00000000A": _record("INE00000000A", "STOCK0", "Metals & Mining"),
    }

    decision = decide_publication(current, previous, {"INE00000000A": _provenance()})

    assert decision.publish is True
    assert decision.observations[0].symbol == "STOCK0"
    assert decision.observations[0].sector == "Metals & Mining"
    assert decision.observations[0].provenance.extractor_version == "screener-peer-titles-v1"


def test_symbol_rename_updates_the_artifact_without_counting_as_a_taxonomy_shift():
    previous = _registry()
    current = {
        **previous,
        "INE00000000A": _record("INE00000000A", "RENAMED", "Energy"),
    }

    decision = decide_publication(current, previous, {"INE00000000A": _provenance()})

    assert decision.publish is True
    assert decision.observations[0].symbol == "RENAMED"


def test_quality_gate_retains_prior_when_coverage_or_taxonomy_changes_are_suspicious():
    previous = {
        "INE002A01018": _record(),
        "INE009A01021": _record(),
    }
    decision = decide_publication({"INE002A01018": _record("Metals & Mining")}, previous, {})

    assert decision.publish is False
    assert "coverage" in decision.reason


def test_suspicious_taxonomy_or_diversity_changes_are_rejected():
    previous = _registry()
    changed = {
        instrument_key: _record(instrument_key, record.symbol, "Energy")
        for instrument_key, record in previous.items()
    }

    decision = decide_publication(changed, previous, {})

    assert decision.publish is False
    assert "sector-diversity" in decision.reason


def test_approved_observations_are_retained_as_a_jsonl_audit_trail(tmp_path):
    observation = decide_publication(
        {"INE002A01018": _record()}, {}, {"INE002A01018": _provenance()}
    ).observations
    audit_path = tmp_path / "sector" / "classification_observations.jsonl"

    append_observations(audit_path, observation)

    row = audit_path.read_text().strip()
    assert (
        "\"source_url\":\"https://www.screener.in/company/RELIANCE/consolidated/\""
        in row
    )
    assert "\"observed_at\":\"2026-07-24T10:00:00\"" in row
