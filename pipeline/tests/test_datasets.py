from pipeline import config, datasets


def test_equities_spec_fields():
    s = datasets.EQUITIES
    assert s.key == "equities" and s.file_prefix == "ohlc"
    assert s.base_dir == config.OHLC_DIR and s.source_label == "nse-udiff"
    assert s.abs_rowcount_range == config.ROWCOUNT_ABS_RANGE
    assert s.manifest_name == "ohlc" and s.schema_version == 1
    assert datasets.DATASETS["equities"] is s
    assert datasets.DATASET_ORDER == ["equities"]


def test_by_manifest_name():
    assert datasets.by_manifest_name("ohlc") is datasets.EQUITIES
    assert datasets.by_manifest_name("nope") is None


def test_manifest_names_are_unique():
    names = [s.manifest_name for s in datasets.DATASETS.values()]
    assert len(set(names)) == len(names)
