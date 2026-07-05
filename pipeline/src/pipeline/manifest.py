"""Build the versioned, checksummed manifest + status dicts. Pure (no network)."""
from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from pipeline import config, store

if TYPE_CHECKING:
    from pipeline.datasets import DatasetSpec

_CHUNK = 1 << 20


def file_digest(path: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def asset_name(logical: str, sha256: str) -> str:
    """Content-addressed release asset name: sha8 spliced before the extension.

    Assets named this way are immutable by construction — new content gets a
    new name, so nothing on the release is ever clobbered except manifest.json."""
    stem, _, ext = logical.rpartition(".")
    return f"{stem}.{sha256[:8]}.{ext}"


def parquet_rows(path: Path) -> int:
    # Column-pruned read: cheap at this scale and avoids a pyarrow-stubs
    # dependency for mypy --strict.
    return int(len(pd.read_parquet(path, columns=["date"])))


def build_manifest(
    specs: list[DatasetSpec], *, latest_trading_date: date, generated_at: str
) -> dict[str, Any]:
    """Build the v2 manifest: one entry per dataset spec with a baseline (full
    year-partitioned files) and a rolling deltas window.

    Deltas are a best-effort catch-up window, not a complete per-day record —
    a crash between store append and delta-write can leave a permanent gap
    for that day while the baseline stays complete; the manifest simply lists
    whatever `store.list_deltas` returns."""
    out_datasets: list[dict[str, Any]] = []
    for spec in specs:
        baseline: list[dict[str, Any]] = []
        latest: date | None = None
        for p in sorted(spec.base_dir.glob(f"{spec.file_prefix}_*.parquet")):
            sha, size = file_digest(p)
            baseline.append({"name": p.name, "asset": asset_name(p.name, sha),
                             "sha256": sha, "bytes": size, "rows": parquet_rows(p)})
            col = pd.to_datetime(pd.read_parquet(p, columns=["date"])["date"])
            if not col.empty:
                d = col.max().date()
                latest = d if latest is None or d > latest else latest
        if not baseline or latest is None:
            continue
        deltas: list[dict[str, Any]] = []
        for p in store.list_deltas(spec.base_dir, prefix=spec.file_prefix)[-30:]:
            sha, size = file_digest(p)
            deltas.append({"date": p.stem.removeprefix(f"{spec.file_prefix}_"),
                           "name": p.name, "asset": "delta_" + asset_name(p.name, sha),
                           "sha256": sha, "bytes": size})
        out_datasets.append({"name": spec.manifest_name, "schema_version": spec.schema_version,
                             "latest_date": latest.isoformat(), "baseline": baseline,
                             "deltas": deltas})
    return {"manifest_version": config.MANIFEST_VERSION,
            "min_client_version": config.MIN_CLIENT_VERSION,
            "generated_at": generated_at,
            "latest_trading_date": latest_trading_date.isoformat(),
            "datasets": out_datasets}


def dataset_files(ds: dict[str, Any]) -> list[dict[str, Any]]:
    """v1/v2 reader compat: G0 manifests use 'files', v2 uses 'baseline'.

    The ONE place v1/v2 reader compat lives; sync/publish/fakes all use it."""
    files: list[dict[str, Any]] = ds.get("baseline", ds.get("files", []))
    return files


def status_to_dict(status_obj: Any) -> dict[str, Any]:
    return {
        "status": status_obj.status,
        "date": status_obj.date.isoformat(),
        "symbol_count": status_obj.symbol_count,
        "quarantined_count": status_obj.quarantined_count,
        "source": status_obj.source,
        "message": status_obj.message,
    }


def write_json(obj: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def write_status(status_obj: Any, meta_dir: Path) -> Path:
    path = meta_dir / "last_run_status.json"
    write_json(status_to_dict(status_obj), path)
    return path
