"""ISIN-keyed state transitions for Classification Registry records."""
from __future__ import annotations

from dataclasses import dataclass

from pipeline.sources.screener_classification import ClassificationObservation


@dataclass(frozen=True)
class ClassificationRegistryRecord:
    """The publishable, last-known-good taxonomy for one ISIN."""

    instrument_key: str
    symbol: str
    macro_sector: str
    sector: str
    industry: str
    basic_industry: str


def apply_observation(
    existing: ClassificationRegistryRecord | None,
    *,
    instrument_key: str,
    symbol: str,
    observation: ClassificationObservation | None,
) -> ClassificationRegistryRecord | None:
    """Apply a complete observation or retain the existing Last Known-Good row."""
    if observation is None:
        return existing
    return ClassificationRegistryRecord(
        instrument_key=instrument_key,
        symbol=symbol,
        macro_sector=observation.macro_sector,
        sector=observation.sector,
        industry=observation.industry,
        basic_industry=observation.basic_industry,
    )
