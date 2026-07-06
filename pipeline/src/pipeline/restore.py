"""Disaster-recovery restore: materialize any release/snapshot tag's baseline
datasets under an arbitrary target directory tree.

Deliberately independent of sync.py's dataset-registry routing -- a restore
target is an arbitrary directory (a scratch dir for a drill, or the real
data/ tree for an actual recovery), not necessarily today's live DATASETS
registry, so this re-implements the two-phase verify-then-materialize
discipline rather than importing sync.py internals. Restores baselines only;
deltas are a live-client catch-up mechanism, not a DR concern.

Two-phase discipline (mirrors sync.py's own crash-safety contract): phase 1
downloads and sha-verifies every baseline file into work_dir; only after
EVERY file has verified does phase 2 replace each verified file into
target_root. A checksum mismatch on any single asset raises UnexpectedFailure
before phase 2 ever begins, so target_root is never created/touched at all --
a torn restore (some datasets materialized, others not) would be a DR
disaster in its own right, so this guarantees either every verified file
lands or none do."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.errors import UnexpectedFailure
from pipeline.manifest import dataset_files, file_digest
from pipeline.release import ReleaseClient


def restore_from_tag(
    client: ReleaseClient, *, target_root: Path, work_dir: Path
) -> dict[str, Any]:
    """Download+verify `client`'s manifest.json, then two-phase restore every
    dataset's baseline files under `target_root / dataset_name / logical_name`.

    Phase 1 (verify all): every baseline asset across every dataset is
    downloaded into `work_dir` and sha256-checked against the manifest before
    anything is written to `target_root`. Phase 2 (materialize all) only
    begins once every file has verified; each verified file is moved
    (`Path.replace`) into place. Deltas are never downloaded or restored --
    same posture as sync.py: a restore rebuilds from baselines only, deltas
    are a live-client catch-up mechanism.

    Returns the parsed manifest dict so the caller (the CLI) can report on
    dataset names, latest dates, and byte/row counts without re-parsing."""
    work_dir.mkdir(parents=True, exist_ok=True)
    client.download(["manifest.json"], work_dir)
    manifest: dict[str, Any] = json.loads((work_dir / "manifest.json").read_text())

    # Phase 1: download + sha-verify every baseline file into work_dir.
    # Nothing touches target_root until every file has verified.
    verified: list[tuple[Path, str, str]] = []  # (downloaded_path, dataset_name, logical_name)
    for ds in manifest.get("datasets", []):
        for entry in dataset_files(ds):
            asset = entry.get("asset", entry["name"])
            client.download([asset], work_dir)
            got = work_dir / asset
            sha, _ = file_digest(got)
            if sha != entry["sha256"]:
                raise UnexpectedFailure(
                    f"restore checksum mismatch for {asset}: got {sha}, "
                    f"manifest says {entry['sha256']}"
                )
            verified.append((got, str(ds["name"]), entry["name"]))

    # Phase 2: every file verified -- now materialize all of them.
    for got, dataset_name, logical_name in verified:
        dest_dir = target_root / dataset_name
        dest_dir.mkdir(parents=True, exist_ok=True)
        got.replace(dest_dir / logical_name)

    return manifest
