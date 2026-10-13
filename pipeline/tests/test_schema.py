from datetime import date

import pandas as pd
import pytest
from pandera.errors import SchemaError

from pipeline import config
from pipeline.schema import validate_ohlc


def _good_row() -> dict:
    return {
        "date": date(2026, 7, 3),
        "instrument_key": "INE002A01018",
        "isin": "INE002A01018",
        "symbol": "RELIANCE",
        "series": "EQ",
        "open": 2990.0,
        "high": 3010.0,
        "low": 2985.0,
        "close": 3000.0,
        "prevclose": 2980.0,
        "volume": 1_000_000,
        "value": 3.0e9,
        "trades": 50_000,
        "nse_token": 2885,
        "source": "nse-udiff",
    }


def test_valid_frame_passes():
    df = pd.DataFrame([_good_row()])[config.CANON_COLUMNS]
    out = validate_ohlc(df)
    assert len(out) == 1


def test_negative_volume_is_rejected():
    row = _good_row()
    row["volume"] = -5
    df = pd.DataFrame([row])[config.CANON_COLUMNS]
    with pytest.raises(SchemaError):
        validate_ohlc(df)


def test_missing_instrument_key_is_rejected():
    row = _good_row()
    row["instrument_key"] = None
    df = pd.DataFrame([row])[config.CANON_COLUMNS]
    with pytest.raises(SchemaError):
        validate_ohlc(df)


def test_null_nse_token_passes():
    row = _good_row()
    row["nse_token"] = None
    df = pd.DataFrame([row])[config.CANON_COLUMNS]
    assert len(validate_ohlc(df)) == 1


@pytest.mark.parametrize("bad", [0, -1])
def test_non_positive_nse_token_is_rejected(bad):
    row = _good_row()
    row["nse_token"] = bad
    df = pd.DataFrame([row])[config.CANON_COLUMNS]
    with pytest.raises(SchemaError):
        validate_ohlc(df)
