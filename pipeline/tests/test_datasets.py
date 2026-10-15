import functools

import pytest

from pipeline import config, datasets
from pipeline.fetch import NseIndicesFetcher
from pipeline.normalize import normalize_equity_bhavcopy
from pipeline.normalize_indices import normalize_indices


def test_equities_spec_fields():
    s = datasets.EQUITIES
    assert s.key == "equities" and s.file_prefix == "ohlc"
    assert s.base_dir == config.OHLC_DIR and s.source_label == "nse-udiff"
    assert s.abs_rowcount_range == config.ROWCOUNT_ABS_RANGE
    # schema_version bumps 1 -> 2 (G1b task 4): EQ-only filter dropped and
    # NSE: sentinel keys introduced -- a client-visible ohlc dataset change.
    assert s.manifest_name == "ohlc" and s.schema_version == 2
    assert datasets.DATASETS["equities"] is s
    # Still the fetched primary (DATASET_ORDER[0]) whose status gates the
    # publish, but never published itself since 2026-10-13.
    assert s.published is False
    assert datasets.DATASET_ORDER == [
        "equities", "indices", "reference", "sector_industry",
        "classification_registry", "classification_observations",
        "fundamentals", "index_constituents", "cas_eligible", "fno_freeze_limits",
        "bse_fno_freeze_limits", "market_calendar", "corporate_actions",
    ]


def test_equities_normalizer_source_bound_via_partial():
    # M-1 carry-in: per-row source can never diverge from source_label -- the
    # normalizer is bound with source="nse-udiff" at spec-construction time.
    s = datasets.EQUITIES
    assert isinstance(s.normalizer, functools.partial)
    assert s.normalizer.func is normalize_equity_bhavcopy
    assert s.normalizer.keywords == {"source": "nse-udiff"}


def test_indices_spec_fields():
    s = datasets.INDICES
    assert s.key == "indices" and s.file_prefix == "indices"
    assert s.base_dir == config.INDICES_DIR and s.source_label == "nse-indices"
    assert s.abs_rowcount_range == config.INDICES_ROWCOUNT_ABS_RANGE
    assert s.manifest_name == "indices" and s.schema_version == 1
    assert isinstance(s.normalizer, functools.partial)
    assert s.normalizer.func is normalize_indices
    assert s.normalizer.keywords == {"source": "nse-indices"}
    assert s.make_fetcher is NseIndicesFetcher
    assert datasets.DATASETS["indices"] is s
    assert s.published is False


def test_sector_industry_schema_version_tracks_four_tier_contract():
    s = datasets.SECTOR_INDUSTRY
    assert s.manifest_name == "sector_industry" and s.schema_version == 2


def test_by_manifest_name():
    assert datasets.by_manifest_name("ohlc") is datasets.EQUITIES
    assert datasets.by_manifest_name("indices") is datasets.INDICES
    assert datasets.by_manifest_name("nope") is None


def test_manifest_names_are_unique():
    names = [s.manifest_name for s in datasets.DATASETS.values()]
    assert len(set(names)) == len(names)


def test_all_specs_follows_dataset_order():
    assert datasets.all_specs() == [
        datasets.EQUITIES, datasets.INDICES, datasets.REFERENCE,
        datasets.SECTOR_INDUSTRY, datasets.CLASSIFICATION_REGISTRY,
        datasets.CLASSIFICATION_OBSERVATIONS, datasets.FUNDAMENTALS,
        datasets.INDEX_CONSTITUENTS, datasets.CAS_ELIGIBLE, datasets.FNO_FREEZE_LIMITS,
        datasets.BSE_FNO_FREEZE_LIMITS, datasets.MARKET_CALENDAR, datasets.CORPORATE_ACTIONS,
    ]


def test_published_specs_are_every_kept_list_and_no_price_dataset():
    """The manifest carries the reference lists only: reference first (the
    publish's primary), no price dataset, nothing retired."""
    specs = datasets.published_specs()
    prices = (datasets.EQUITIES, datasets.INDICES)
    assert specs == [s for s in datasets.all_specs() if s not in prices]
    assert specs[0] is datasets.REFERENCE
    assert [s.manifest_name for s in specs] == [
        "reference", "sector_industry", "classification_registry",
        "classification_observations", "fundamentals", "index_constituents",
        "cas_eligible", "fno_freeze_limits", "bse_fno_freeze_limits", "market_calendar",
        "corporate_actions",
    ]
    assert not {s.manifest_name for s in specs} & datasets.RETIRED_MANIFEST_NAMES


def test_retired_manifest_names_are_the_price_datasets_and_ca_flags():
    assert {"ohlc", "indices", "ca_flags"} == datasets.RETIRED_MANIFEST_NAMES
    assert "ca_flags" not in datasets.DATASETS
    # No published spec's file prefix claims a retired dataset's assets, so
    # publish's GC can never delete them (publish.owns_asset).
    from pipeline.publish import owns_asset

    prefixes = frozenset(s.file_prefix for s in datasets.published_specs())
    for asset in (
        "ohlc_2026.3a0c9899.parquet", "delta_ohlc_2026-10-12.b8aea44a.parquet",
        "indices_2026.0964d75d.parquet", "delta_indices_2026-10-12.e9a9239a.parquet",
        "ca_flags_2026.57a24e6a.parquet",
    ):
        assert not owns_asset(asset, file_prefixes=prefixes), asset


def test_sector_industry_spec_fields():
    s = datasets.SECTOR_INDUSTRY
    assert s.key == "sector_industry"
    assert s.file_prefix == "sector_industry"
    assert s.base_dir == config.SECTOR_DIR
    assert s.source_label == "nse-sector"
    assert s.manifest_name == "sector_industry"
    assert s.schema_version == 2
    assert s.derived is True  # kept out of the OHLC fetch loop + continuity check
    assert datasets.DATASETS["sector_industry"] is s
    # fetched inside the builder, never via the run_daily Fetcher path
    with pytest.raises(RuntimeError, match="derived dataset has no fetcher"):
        s.make_fetcher()


def test_sector_industry_registered_in_builders():
    from pipeline import cli
    assert cli.builders.BUILDERS["sector_industry"] is cli.builders.build_sector_industry


def test_classification_state_artifacts_are_external_publishable_datasets():
    for spec in (
        datasets.CLASSIFICATION_REGISTRY,
        datasets.CLASSIFICATION_OBSERVATIONS,
    ):
        assert spec.base_dir == config.SECTOR_DIR
        assert spec.derived is True
        assert spec.external is True
        assert datasets.DATASETS[spec.key] is spec


def test_fundamentals_spec_fields():
    s = datasets.FUNDAMENTALS
    assert s.key == "fundamentals"
    # file_prefix must glob-match the Rust producer's fundamentals_all.parquet
    # (the instruments_all/sector_industry_all convention).
    assert s.file_prefix == "fundamentals"
    assert s.base_dir == config.FUNDAMENTALS_DIR
    assert s.source_label == "bse-xbrl"
    assert s.manifest_name == "fundamentals"
    assert s.schema_version == 1
    assert s.derived is True   # never fetched; out of the continuity check
    assert s.external is True  # produced by the Rust producer, not a builder
    assert datasets.DATASETS["fundamentals"] is s
    with pytest.raises(RuntimeError, match="derived dataset has no fetcher"):
        s.make_fetcher()


def test_fundamentals_has_no_builder_and_is_external():
    # The daily Phase-2 loop must SKIP external specs: a derived spec without
    # a BUILDERS entry would otherwise surface as a failed secondary-dataset
    # status and red the data-daily job every night.
    from pipeline import cli
    assert "fundamentals" not in cli.builders.BUILDERS
    assert datasets.FUNDAMENTALS.external is True


def test_derived_defaults_false_for_existing_specs():
    assert datasets.EQUITIES.derived is False
    assert datasets.INDICES.derived is False


def test_external_defaults_false_for_existing_specs():
    for key in ("equities", "indices", "reference", "sector_industry"):
        assert datasets.DATASETS[key].external is False, key


def test_dataset_spec_accepts_derived_true(tmp_path):
    def _raiser() -> object:
        raise RuntimeError("derived dataset has no fetcher")

    spec = datasets.DatasetSpec(
        key="fake_derived", file_prefix="fake", base_dir=tmp_path / "fake",
        source_label="derived", normalizer=lambda df: df, make_fetcher=_raiser,
        abs_rowcount_range=(0, 10**9), manifest_name="fake", schema_version=1,
        derived=True,
    )
    assert spec.derived is True
    with pytest.raises(RuntimeError, match="derived dataset has no fetcher"):
        spec.make_fetcher()
