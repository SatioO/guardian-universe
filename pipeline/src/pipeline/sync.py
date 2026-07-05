"""Fail-closed sync: pull the published dataset and verify every checksum.

The P0-1 fix: any failure other than "release does not exist" aborts the run.
A transient download failure must NEVER leave an empty store that a later
publish would present as the new truth."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.errors import UnexpectedFailure
from pipeline.manifest import file_digest, write_json
from pipeline.release import ReleaseClient

SYNCED_STATE = "synced_manifest.json"


def sync_store(
    client: ReleaseClient, *, ohlc_dir: Path, meta_dir: Path, work_dir: Path
) -> dict[str, Any] | None:
    ohlc_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    if not client.exists():
        write_json({"generated_at": None}, meta_dir / SYNCED_STATE)
        return None

    client.download(["manifest.json"], work_dir)
    manifest: dict[str, Any] = json.loads((work_dir / "manifest.json").read_text())
    for ds in manifest.get("datasets", []):
        for entry in ds["files"]:
            asset = entry.get("asset", entry["name"])
            client.download([asset], work_dir)
            got = work_dir / asset
            sha, _ = file_digest(got)
            if sha != entry["sha256"]:
                raise UnexpectedFailure(
                    f"sync checksum mismatch for {asset}: "
                    f"got {sha}, manifest says {entry['sha256']}"
                )
            got.replace(ohlc_dir / entry["name"])
    write_json(manifest, meta_dir / SYNCED_STATE)
    return manifest
