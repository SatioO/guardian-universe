"""DatasetSpec registry: identity for each ingested dataset (equities today, more
later). Shared code (run_daily/backfill) threads a spec instead of hardcoding
a dataset name."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipeline import config
from pipeline.fetch import Fetcher, NseUdiffFetcher
from pipeline.normalize import normalize_equity_bhavcopy


@dataclass(frozen=True)
class DatasetSpec:
    key: str                    # registry key: "equities"
    file_prefix: str            # {prefix}_{YYYY}.parquet
    base_dir: Path
    source_label: str           # provenance recorded in RunStatus.source
    normalizer: Callable[[pd.DataFrame], pd.DataFrame]
    make_fetcher: Callable[[], Fetcher]
    abs_rowcount_range: tuple[int, int]
    manifest_name: str          # dataset name in manifest.json
    schema_version: int


EQUITIES = DatasetSpec(
    key="equities", file_prefix="ohlc", base_dir=config.OHLC_DIR,
    source_label="nse-udiff", normalizer=normalize_equity_bhavcopy,
    make_fetcher=NseUdiffFetcher, abs_rowcount_range=config.ROWCOUNT_ABS_RANGE,
    manifest_name="ohlc", schema_version=1,
)

DATASETS: dict[str, DatasetSpec] = {"equities": EQUITIES}
DATASET_ORDER: list[str] = ["equities"]

# publish.py resolves specs by manifest_name (by_manifest_name); manifest_name
# must be unique across the registry or that resolution would be ambiguous.
if len({s.manifest_name for s in DATASETS.values()}) != len(DATASETS):
    raise ValueError("DATASETS registry has duplicate manifest_name values")


def by_manifest_name(name: str) -> DatasetSpec | None:
    for spec in DATASETS.values():
        if spec.manifest_name == name:
            return spec
    return None


def all_specs() -> list[DatasetSpec]:
    return [DATASETS[k] for k in DATASET_ORDER]
