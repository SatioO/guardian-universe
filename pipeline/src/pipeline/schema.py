"""pandera data contract for the canonical OHLC frame.

The SAME schema object is used in unit tests and the runtime validation gate,
so tests and production enforce identical guarantees."""
from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column

_non_neg = Check.ge(0)

OHLC_SCHEMA = pa.DataFrameSchema(
    {
        "date": Column("datetime64[ns]", coerce=True, nullable=False),
        "instrument_key": Column(str, nullable=False),
        "isin": Column(str, nullable=True),
        "symbol": Column(str, nullable=False),
        "series": Column(str, nullable=False),
        "open": Column(float, _non_neg, nullable=False, coerce=True),
        "high": Column(float, _non_neg, nullable=False, coerce=True),
        "low": Column(float, _non_neg, nullable=False, coerce=True),
        "close": Column(float, _non_neg, nullable=False, coerce=True),
        "prevclose": Column(float, _non_neg, nullable=False, coerce=True),
        "volume": Column("int64", _non_neg, nullable=False, coerce=True),
        "value": Column(float, _non_neg, nullable=False, coerce=True),
        "trades": Column("int64", _non_neg, nullable=False, coerce=True),
        "source": Column(str, nullable=False),
    },
    strict=True,
    ordered=False,
)


def validate_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    return OHLC_SCHEMA.validate(df, lazy=False)
