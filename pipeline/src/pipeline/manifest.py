"""Build the versioned, checksummed manifest + status dicts. Pure (no network)."""
from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

_CHUNK = 1 << 20


def file_digest(path: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def build_manifest(
    ohlc_dir: Path,
    *,
    schema_version: int,
    latest_trading_date: date,
    generated_at: str,
) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for p in sorted(ohlc_dir.glob("ohlc_*.parquet")):
        sha, size = file_digest(p)
        files.append({"name": p.name, "sha256": sha, "bytes": size})
    return {
        "schema_version": schema_version,
        "generated_at": generated_at,
        "latest_trading_date": latest_trading_date.isoformat(),
        "datasets": [{"name": "ohlc", "files": files}],
    }


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
