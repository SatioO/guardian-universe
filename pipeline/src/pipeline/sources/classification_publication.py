"""Fail-closed publication decisions for active Classification Registry rows."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from pipeline.sources.classification_registry import ClassificationRegistryRecord

_MIN_COVERAGE_RATIO = 0.99
_MAX_CHANGED_RATIO = 0.10
_MIN_SECTOR_DIVERSITY_RATIO = 0.50


@dataclass(frozen=True)
class Provenance:
    observed_at: datetime
    source_url: str
    extractor_version: str
    source_fragment_hash: str


@dataclass(frozen=True)
class PublishedClassificationObservation:
    """An immutable record of a classification that was approved to publish."""

    instrument_key: str
    symbol: str
    macro_sector: str
    sector: str
    industry: str
    basic_industry: str
    provenance: Provenance


@dataclass(frozen=True)
class PublicationDecision:
    publish: bool
    reason: str
    fingerprint: str
    observations: tuple[PublishedClassificationObservation, ...]


def decide_publication(
    current: Mapping[str, ClassificationRegistryRecord],
    previous: Mapping[str, ClassificationRegistryRecord],
    provenance: Mapping[str, Provenance],
    *,
    observed_active_count: int | None = None,
    expected_active_count: int | None = None,
    legacy_missing_macro_sector: bool = False,
) -> PublicationDecision:
    """Return whether a new active classification artifact may be published.

    ``legacy_missing_macro_sector`` is reserved for the one-time migration of
    the known v1 artifact, whose otherwise-compatible rows predate the
    ``macro_sector`` field. It does not relax coverage, provenance, diversity,
    or non-macro taxonomy-change checks.
    """
    fingerprint = classification_fingerprint(current)
    if fingerprint == classification_fingerprint(previous):
        return PublicationDecision(False, "fingerprint unchanged", fingerprint, ())
    if not current:
        return PublicationDecision(
            False, "empty active registry rejected publication", fingerprint, ()
        )
    if (
        observed_active_count is not None
        and expected_active_count is not None
        and observed_active_count < expected_active_count * _MIN_COVERAGE_RATIO
    ):
        return PublicationDecision(False, "coverage guard rejected publication", fingerprint, ())
    if previous and len(current) < len(previous) * _MIN_COVERAGE_RATIO:
        return PublicationDecision(False, "coverage guard rejected publication", fingerprint, ())

    changed = tuple(
        instrument_key
        for instrument_key in sorted(set(current) | set(previous))
        if current.get(instrument_key) != previous.get(instrument_key)
    )
    changed_existing = tuple(
        instrument_key
        for instrument_key in changed
        if (
            instrument_key in current
            and instrument_key in previous
            and _taxonomy_fields(
                current[instrument_key],
                include_macro_sector=not legacy_missing_macro_sector,
            )
            != _taxonomy_fields(
                previous[instrument_key],
                include_macro_sector=not legacy_missing_macro_sector,
            )
        )
    )
    if (
        previous
        and _sector_diversity(current) < _sector_diversity(previous) * _MIN_SECTOR_DIVERSITY_RATIO
    ):
        return PublicationDecision(
            False, "sector-diversity guard rejected publication", fingerprint, ()
        )
    if previous and len(changed_existing) > len(previous) * _MAX_CHANGED_RATIO:
        return PublicationDecision(
            False, "taxonomy-change guard rejected publication", fingerprint, ()
        )
    missing_provenance = [
        instrument_key
        for instrument_key in changed
        if instrument_key in current and instrument_key not in provenance
    ]
    if missing_provenance:
        return PublicationDecision(
            False, "missing provenance rejected publication", fingerprint, ()
        )

    observations = tuple(
        PublishedClassificationObservation(
            instrument_key=instrument_key,
            symbol=current[instrument_key].symbol,
            macro_sector=current[instrument_key].macro_sector,
            sector=current[instrument_key].sector,
            industry=current[instrument_key].industry,
            basic_industry=current[instrument_key].basic_industry,
            provenance=provenance[instrument_key],
        )
        for instrument_key in changed
        if instrument_key in current
    )
    return PublicationDecision(True, "classification changed", fingerprint, observations)


def append_observations(
    audit_path: Path, observations: tuple[PublishedClassificationObservation, ...]
) -> None:
    """Atomically append immutable observations to the Parquet audit dataset.

    Rewriting to a sibling temporary file prevents a half-written ledger if the
    process is interrupted while recording the new batch. Collection writes its
    real Screener observations here after registry persistence; publication may
    append the subset that changed the scanner artifact, and deduplication
    makes that repeat harmless.
    """
    if not observations:
        return
    rows = pd.DataFrame(
        [
            {
                "instrument_key": observation.instrument_key,
                "symbol": observation.symbol,
                "macro_sector": observation.macro_sector,
                "sector": observation.sector,
                "industry": observation.industry,
                "basic_industry": observation.basic_industry,
                "observed_at": pd.Timestamp(observation.provenance.observed_at),
                "source_url": observation.provenance.source_url,
                "extractor_version": observation.provenance.extractor_version,
                "source_fragment_hash": observation.provenance.source_fragment_hash,
                "date": pd.Timestamp(observation.provenance.observed_at).normalize(),
            }
            for observation in observations
        ]
    )
    prior = pd.read_parquet(audit_path) if audit_path.exists() else rows.iloc[0:0]
    out = pd.concat([prior, rows], ignore_index=True).drop_duplicates(
        subset=[
            "instrument_key",
            "observed_at",
            "source_url",
            "extractor_version",
            "source_fragment_hash",
        ],
        keep="first",
    )
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = audit_path.with_suffix(f"{audit_path.suffix}.tmp")
    out.to_parquet(temporary_path, compression="zstd", index=False)
    temporary_path.replace(audit_path)


def classification_fingerprint(records: Mapping[str, ClassificationRegistryRecord]) -> str:
    """Hash only active published classification fields in a stable ISIN order."""
    rows = [
        [
            record.instrument_key,
            record.symbol,
            record.macro_sector,
            record.sector,
            record.industry,
            record.basic_industry,
        ]
        for _, record in sorted(records.items())
    ]
    return hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()


def _sector_diversity(records: Mapping[str, ClassificationRegistryRecord]) -> int:
    return len({record.sector for record in records.values()})


def _taxonomy_fields(
    record: ClassificationRegistryRecord, *, include_macro_sector: bool = True
) -> tuple[str, ...]:
    """The four tiers whose changes are a classification shift, not a rename."""
    fields = (
        record.sector,
        record.industry,
        record.basic_industry,
    )
    return (record.macro_sector, *fields) if include_macro_sector else fields
