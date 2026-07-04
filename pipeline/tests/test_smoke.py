from pipeline import config


def test_canon_columns_shape_and_order():
    assert config.CANON_COLUMNS[0] == "date"
    assert config.CANON_COLUMNS[1] == "instrument_key"
    assert config.CANON_COLUMNS[-1] == "source"
    assert len(config.CANON_COLUMNS) == 14


def test_schema_version_is_one():
    assert config.SCHEMA_VERSION == 1
