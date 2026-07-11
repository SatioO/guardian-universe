"""DatasetSpec registry: identity for each ingested dataset (equities, indices
today, more later). Shared code (run_daily/backfill) threads a spec instead of
hardcoding a dataset name.

Reserved manifest dataset names for future phases: corporate_actions, breadth,
fundamentals, reference, ca_flags. Client adjustment enum (P4b): raw | split |
total_return."""
from __future__ import annotations

import functools
import io
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from pipeline import config
from pipeline.fetch import (
    _BROWSER_UA,
    Fetcher,
    NseIndicesFetcher,
    NseUdiffFetcher,
    _fetch_with_retry,
)
from pipeline.normalize import normalize_equity_bhavcopy
from pipeline.normalize_indices import normalize_indices
from pipeline.sources.nse_secfull import build_secfull_url, secfull_to_udiff_shape


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


def _load_isin_map() -> dict[str, str]:
    """symbol -> ISIN, read from the reference/instruments store.

    secfull has no ISIN column of its own, so the fallback keys its rows via
    this reference-derived map. The reference dataset is SCD2 (multiple rows
    per symbol over time, e.g. a series change); this dedupes to the latest
    row per symbol by `last_seen` before building the map, so the join never
    blows up cardinality. Absent reference (not yet built, or this is an
    early-adopter deployment) -> {} + a stderr note; sentinel "NSE:"+symbol
    keys take over downstream and self-heal once reference lands.
    """
    path = config.REFERENCE_DIR / "instruments_all.parquet"
    if not path.exists():
        print(f"_load_isin_map: {path} not found -- isin_map will be empty", file=sys.stderr)
        return {}

    df = pd.read_parquet(path)
    df = df[df["status"] == "active"]
    df = df.sort_values("last_seen").drop_duplicates(subset="symbol", keep="last")
    df = df[df["isin"].fillna("") != ""]
    return dict(zip(df["symbol"], df["isin"], strict=True))


def _secfull_fallback(d: date) -> pd.DataFrame:
    """Fallback fetch fn for the equities NseUdiffFetcher: fetches the
    sec_bhavdata_full CSV (same warm-session/retry/404 contract as the
    primary, via fetch._fetch_with_retry) and reshapes it to the UDiFF raw
    column shape. Returns a bare DataFrame -- NseUdiffFetcher._fetch_fallbacks
    wraps it into a FetchResult itself (the Fallback contract)."""
    session = requests.Session()
    session.headers.update({"User-Agent": _BROWSER_UA})
    url = build_secfull_url(d)
    raw = _fetch_with_retry(session, url, d, parse=_secfull_csv_to_df)
    isin_map = _load_isin_map()
    return secfull_to_udiff_shape(raw, isin_map=isin_map)


def _secfull_csv_to_df(csv_bytes: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(csv_bytes))


def _equities_fetcher() -> Fetcher:
    """Factory (not a bare class default) so the equities Fetcher always
    wires the sec_bhavdata_full fallback -- EQUITIES.make_fetcher calls this
    with no arguments, per the DatasetSpec contract."""
    return NseUdiffFetcher(fallbacks=[("nse-secfull", _secfull_fallback)])


EQUITIES = DatasetSpec(
    key="equities", file_prefix="ohlc", base_dir=config.OHLC_DIR,
    source_label="nse-udiff",
    # M-1 carry-in: bind source via partial at spec-construction time so the
    # per-row "source" column can never diverge from source_label.
    normalizer=functools.partial(normalize_equity_bhavcopy, source="nse-udiff"),
    make_fetcher=_equities_fetcher, abs_rowcount_range=config.ROWCOUNT_ABS_RANGE,
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

def _no_fetcher() -> Fetcher:
    """Derived datasets are built from the store, not fetched -- this must
    never be invoked; the CLI's phase-1 fetch loop always skips `derived`
    specs before it can call make_fetcher()."""
    raise RuntimeError("derived dataset has no fetcher")


REFERENCE = DatasetSpec(
    key="reference", file_prefix="instruments", base_dir=config.REFERENCE_DIR,
    source_label="derived",
    normalizer=lambda df: df,  # identity: builders.build_reference shapes rows itself
    make_fetcher=_no_fetcher,
    abs_rowcount_range=(0, 10**9),
    manifest_name="reference", schema_version=1,
    derived=True,
)

CA_FLAGS = DatasetSpec(
    key="ca_flags", file_prefix="ca_flags", base_dir=config.CA_FLAGS_DIR,
    source_label="derived",
    normalizer=lambda df: df,  # identity: builders.build_ca_flags shapes rows itself
    make_fetcher=_no_fetcher,
    abs_rowcount_range=(0, 10**9),
    manifest_name="ca_flags", schema_version=1,
    derived=True,
)

SECTOR_INDUSTRY = DatasetSpec(
    key="sector_industry", file_prefix="sector_industry", base_dir=config.SECTOR_DIR,
    source_label="nse-sector",
    normalizer=lambda df: df,  # identity: builders.build_sector_industry shapes rows itself
    make_fetcher=_no_fetcher,  # fetched inside the builder, not via the run_daily Fetcher path
    abs_rowcount_range=(0, 10**9),
    manifest_name="sector_industry", schema_version=1,
    # `derived` here means "not run through the Phase-1 fetch loop / run_daily":
    # the CSV is fetched INSIDE the builder (build_sector_industry). This keeps
    # it out of the OHLC-shaped fetched path and out of the daily continuity
    # freshness check (it refreshes weekly, not per trading day), while still
    # running each daily `all` cycle via the Phase-2 BUILDERS loop.
    derived=True,
)

DATASETS: dict[str, DatasetSpec] = {
    "equities": EQUITIES, "indices": INDICES, "reference": REFERENCE,
    "ca_flags": CA_FLAGS, "sector_industry": SECTOR_INDUSTRY,
}
DATASET_ORDER: list[str] = [
    "equities", "indices", "reference", "ca_flags", "sector_industry",
]

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
