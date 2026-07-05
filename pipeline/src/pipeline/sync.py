"""Fail-closed sync: pull the published dataset and verify every checksum.

The P0-1 fix: any failure other than "release does not exist" aborts the run.
A transient download failure must NEVER leave an empty store that a later
publish would present as the new truth.

Two-phase materialization: phase 1 downloads and sha-verifies every ohlc file
into `work_dir`; only after every file has verified does phase 2 replace each
verified file into `ohlc_dir` under its logical name. This guarantees that a
failure partway through (download error, checksum mismatch, or a malformed
manifest) never leaves `ohlc_dir` in a hybrid old/new state -- either every
file lands, or none do.

Only the "ohlc" dataset in the manifest is materialized into `ohlc_dir`; any
other dataset (e.g. a "reference" instruments dataset) is ignored by sync."""
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
    try:
        manifest: dict[str, Any] = json.loads((work_dir / "manifest.json").read_text())

        # Phase 1: download + sha-verify every ohlc file into work_dir.
        # Nothing touches ohlc_dir until every file has verified.
        verified: list[tuple[Path, str]] = []
        for ds in manifest.get("datasets", []):
            if ds.get("name") != "ohlc":
                continue
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
                verified.append((got, entry["name"]))
    except (json.JSONDecodeError, KeyError) as e:
        raise UnexpectedFailure(f"malformed manifest: {e}") from e

    # Phase 2: every file verified -- now materialize all of them.
    for got, logical_name in verified:
        got.replace(ohlc_dir / logical_name)

    write_json(manifest, meta_dir / SYNCED_STATE)
    return manifest
