"""Fail-closed sync: pull the published dataset and verify every checksum.

The P0-1 fix: any failure other than "release does not exist" aborts the run.
A transient download failure must NEVER leave an empty store that a later
publish would present as the new truth.

Two-phase materialization: phase 1 downloads and sha-verifies every file into
`work_dir`; only after every file has verified does phase 2 replace each
verified file into its dataset's `base_dir` under its logical name. This
guarantees that a failure partway through (download error, checksum mismatch,
or a malformed manifest) never leaves a dataset's store in a hybrid old/new
state -- either every file lands, or none do. This atomicity guarantee is
scoped to ALL datasets in a single `sync_store` call together (whole-manifest
atomicity), not per-dataset: one call either materializes every dataset's
verified files or materializes none of them.

Each manifest dataset is routed to its registered DatasetSpec via
`datasets.by_manifest_name`; a dataset the registry doesn't recognize is
skipped with a stderr note rather than failing the sync -- a future producer
version may publish datasets this code predates (forward-compat). Only
baselines are materialized: deltas are a client concern and the producer
re-derives them."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from pipeline import datasets
from pipeline.errors import UnexpectedFailure
from pipeline.manifest import dataset_files, file_digest, write_json
from pipeline.release import ReleaseClient

SYNCED_STATE = "synced_manifest.json"


def sync_store(
    client: ReleaseClient, *, meta_dir: Path, work_dir: Path
) -> dict[str, Any] | None:
    meta_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    if not client.exists():
        write_json({"generated_at": None}, meta_dir / SYNCED_STATE)
        return None

    client.download(["manifest.json"], work_dir)
    try:
        manifest: dict[str, Any] = json.loads((work_dir / "manifest.json").read_text())

        # Phase 1: download + sha-verify every file into work_dir.
        # Nothing touches a dataset's base_dir until every file has verified.
        verified: list[tuple[Path, Path, str]] = []
        for ds in manifest.get("datasets", []):
            spec = datasets.by_manifest_name(str(ds.get("name", "")))
            if spec is None:
                print(f"sync: skipping unknown dataset {ds.get('name')!r}", file=sys.stderr)
                continue
            for entry in dataset_files(ds):
                asset = entry.get("asset", entry["name"])
                client.download([asset], work_dir)
                got = work_dir / asset
                sha, _ = file_digest(got)
                if sha != entry["sha256"]:
                    raise UnexpectedFailure(
                        f"sync checksum mismatch for {asset}: "
                        f"got {sha}, manifest says {entry['sha256']}"
                    )
                verified.append((got, spec.base_dir, entry["name"]))
    except (json.JSONDecodeError, KeyError) as e:
        raise UnexpectedFailure(f"malformed manifest: {e}") from e

    # Phase 2: every file verified -- now materialize all of them.
    for got, base_dir, logical_name in verified:
        base_dir.mkdir(parents=True, exist_ok=True)
        got.replace(base_dir / logical_name)

    write_json(manifest, meta_dir / SYNCED_STATE)
    return manifest
