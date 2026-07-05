import functools

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
    assert datasets.DATASET_ORDER == ["equities", "indices"]


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


def test_by_manifest_name():
    assert datasets.by_manifest_name("ohlc") is datasets.EQUITIES
    assert datasets.by_manifest_name("indices") is datasets.INDICES
    assert datasets.by_manifest_name("nope") is None


def test_manifest_names_are_unique():
    names = [s.manifest_name for s in datasets.DATASETS.values()]
    assert len(set(names)) == len(names)


def test_all_specs_follows_dataset_order():
    assert datasets.all_specs() == [datasets.EQUITIES, datasets.INDICES]
