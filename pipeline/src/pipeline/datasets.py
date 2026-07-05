"""DatasetSpec registry: identity for each ingested dataset (equities, indices
today, more later). Shared code (run_daily/backfill) threads a spec instead of
hardcoding a dataset name.

Reserved manifest dataset names for future phases: corporate_actions, breadth,
fundamentals, reference, ca_flags. Client adjustment enum (P4b): raw | split |
total_return."""
from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipeline import config
from pipeline.fetch import Fetcher, NseIndicesFetcher, NseUdiffFetcher
from pipeline.normalize import normalize_equity_bhavcopy
from pipeline.normalize_indices import normalize_indices


@dataclass(frozen=True)
class DatasetSpec:
    key: str                    # registry key: "equities"
    file_prefix: str            # {prefix}_{YYYY}.parquet
    base_dir: Path
    source_label: str           # provenance recorded in RunStatus.source
    normalizer: Callable[[pd.DataFrame], pd.DataFrame]
    make_fetcher: Callable[[], Fetcher]
    # NOTE: bound at spec-construction (import) time — this reads config.* when
    # the module-level DatasetSpec instances below are built, so editing the
    # underlying config value at runtime requires a fresh process to take effect.
    abs_rowcount_range: tuple[int, int]
    manifest_name: str          # dataset name in manifest.json
    schema_version: int
    # Derived datasets are built from other datasets already in the store
    # (via builders.BUILDERS) rather than fetched from an external source.
    # normalizer/make_fetcher are never invoked by the CLI for these specs —
    # additive field, default False preserves all existing constructions.
    derived: bool = False


EQUITIES = DatasetSpec(
    key="equities", file_prefix="ohlc", base_dir=config.OHLC_DIR,
    source_label="nse-udiff",
    # M-1 carry-in: bind source via partial at spec-construction time so the
    # per-row "source" column can never diverge from source_label.
    normalizer=functools.partial(normalize_equity_bhavcopy, source="nse-udiff"),
    make_fetcher=NseUdiffFetcher, abs_rowcount_range=config.ROWCOUNT_ABS_RANGE,
    # schema_version bumps 1 -> 2 (G1b task 4): the EQ-only filter is dropped
    # (all cash series now stored with their own `series` value) and null/empty
    # ISIN rows get an "NSE:"+symbol sentinel key -- a client-visible change to
    # the ohlc dataset's multi-series semantics.
    manifest_name="ohlc", schema_version=2,
)

INDICES = DatasetSpec(
    key="indices", file_prefix="indices", base_dir=config.INDICES_DIR,
    source_label="nse-indices",
    normalizer=functools.partial(normalize_indices, source="nse-indices"),
    make_fetcher=NseIndicesFetcher, abs_rowcount_range=config.INDICES_ROWCOUNT_ABS_RANGE,
    manifest_name="indices", schema_version=1,
)

DATASETS: dict[str, DatasetSpec] = {"equities": EQUITIES, "indices": INDICES}
DATASET_ORDER: list[str] = ["equities", "indices"]

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
