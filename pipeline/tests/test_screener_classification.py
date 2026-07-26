from __future__ import annotations

from pipeline.sources.classification_registry import (
    ClassificationRegistryRecord,
    apply_observation,
)
from pipeline.sources.screener_classification import (
    ClassificationObservation,
    parse_screener_classification,
)


def test_complete_screener_hierarchy_becomes_a_classification_observation():
    page = """
    <section id="peers">
      <a title="Broad Sector" href="/company/RELIANCE/consolidated/">Energy</a>
      <a title="Sector" href="/company/RELIANCE/consolidated/">Oil, Gas &amp; Consumable Fuels</a>
      <a title="Broad Industry" href="/company/RELIANCE/consolidated/">Petroleum Products</a>
      <a title="Industry" href="/company/RELIANCE/consolidated/">Refineries &amp; Marketing</a>
    </section>
    """

    observation = parse_screener_classification(page)

    assert observation == ClassificationObservation(
        macro_sector="Energy",
        sector="Oil, Gas & Consumable Fuels",
        industry="Petroleum Products",
        basic_industry="Refineries & Marketing",
    )


def test_partial_screener_hierarchy_is_not_a_classification_observation():
    page = """
    <a title="Broad Sector">Energy</a>
    <a title="Sector">Oil, Gas &amp; Consumable Fuels</a>
    <a title="Broad Industry">Petroleum Products</a>
    """

    assert parse_screener_classification(page) is None


def test_conflicting_screener_hierarchy_is_not_a_classification_observation():
    page = """
    <a title="Broad Sector">Energy</a>
    <a title="Sector">Oil, Gas &amp; Consumable Fuels</a>
    <a title="Sector">Financial Services</a>
    <a title="Broad Industry">Petroleum Products</a>
    <a title="Industry">Refineries &amp; Marketing</a>
    """

    assert parse_screener_classification(page) is None


def test_failed_observation_preserves_last_known_good_registry_record():
    existing = ClassificationRegistryRecord(
        instrument_key="INE002A01018",
        symbol="RELIANCE",
        macro_sector="Energy",
        sector="Oil, Gas & Consumable Fuels",
        industry="Petroleum Products",
        basic_industry="Refineries & Marketing",
    )

    assert apply_observation(
        existing,
        instrument_key="INE002A01018",
        symbol="RELIANCE",
        observation=None,
    ) == existing
