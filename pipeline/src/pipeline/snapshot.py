"""Monthly immutable snapshot releases: disaster-recovery mechanism (G3 task 4).

A snapshot is a point-in-time, NEVER-CLOBBERED copy of whatever `data-latest`
references at the moment `create_snapshot` runs, tagged `data-snapshot-YYYYMM`.
Unlike `sync.py`, this module copies bytes VERBATIM regardless of dataset
registry membership -- it walks `manifest["datasets"]` directly (baseline +
deltas, across every dataset the manifest references) rather than routing
through `datasets.by_manifest_name`, because a snapshot's job is "reproduce
exactly what was live", not "materialize into today's registered dataset
specs". A dataset the current codebase no longer recognizes is still
snapshotted faithfully; `sync_store` would instead skip it with a stderr note.

Immutability contract: `create_snapshot` REFUSES to recreate an existing
month's tag (raises `UnexpectedFailure`) rather than silently overwriting it --
a snapshot, once created, must never change. The monthly cadence means two
runs should never target the same `YYYYMM` in practice, but a bug/manual
re-run/clock skew must fail loud here, not corrupt an already-trusted archival
copy.

`prune_snapshots` enforces the keep-6 retention: lexical sort on the
`YYYYMM`-suffixed tag is chronological order (zero-padded, fixed-width), so
`sorted()` is sufficient -- no date parsing needed. Only tags starting with
`SNAPSHOT_TAG_PREFIX` are ever considered; `data-latest` (and anything else)
is untouched by construction, since the filter excludes it before any delete
logic runs."""
from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from pipeline.errors import UnexpectedFailure
from pipeline.manifest import dataset_files, file_digest
from pipeline.release import ReleaseClient

SNAPSHOT_TAG_PREFIX = "data-snapshot-"


def tag_for(now: datetime) -> str:
    return f"{SNAPSHOT_TAG_PREFIX}{now:%Y%m}"


def create_snapshot(
    source_client: ReleaseClient,
    dest_client_factory: Callable[[str], ReleaseClient],
    *,
    work_dir: Path,
    now: datetime,
) -> str:
    """Copy `source_client`'s current manifest + every asset it references
    into a brand-new, immutable `data-snapshot-YYYYMM` release.

    Downloads `manifest.json` from `source_client`, then for every dataset
    entry verifies+downloads `dataset_files(ds) + ds.get("deltas", [])` by
    `asset` name (the same sha-verify pattern `sync.py`/`publish.py` use, NOT
    `sync.py`'s dataset-registry routing -- a snapshot copies whatever the
    manifest references regardless of whether today's registry still
    recognizes that dataset name). Raises `UnexpectedFailure` if the
    destination tag already exists (immutable: never re-create/clobber a
    month) -- otherwise creates the destination release and uploads every
    downloaded asset plus the manifest itself. Returns the tag created."""
    work_dir.mkdir(parents=True, exist_ok=True)
    source_client.download(["manifest.json"], work_dir)
    manifest_path = work_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())

    downloaded: list[Path] = [manifest_path]
    for ds in manifest["datasets"]:
        for entry in [*dataset_files(ds), *ds.get("deltas", [])]:
            asset = entry.get("asset", entry["name"])
            source_client.download([asset], work_dir)
            got = work_dir / asset
            sha, _ = file_digest(got)
            if sha != entry["sha256"]:
                raise UnexpectedFailure(
                    f"snapshot checksum mismatch for {asset}: "
                    f"got {sha}, manifest says {entry['sha256']}"
                )
            downloaded.append(got)

    tag = tag_for(now)
    dest = dest_client_factory(tag)
    if dest.exists():
        raise UnexpectedFailure(f"snapshot tag {tag} already exists -- refusing to recreate")

    dest.create(latest=False)  # a snapshot must never steal data-latest's "Latest" badge
    for path in downloaded:
        dest.upload(path)
    return tag


def prune_snapshots(
    client_factory: Callable[[str], ReleaseClient],
    list_client: ReleaseClient,
    *,
    keep: int = 6,
) -> list[str]:
    """Delete every `data-snapshot-*` tag except the newest `keep` (lexical
    sort on the fixed-width `YYYYMM` suffix is chronological order). Any tag
    not starting with `SNAPSHOT_TAG_PREFIX` (e.g. `data-latest`) is never
    considered here, so it is never at risk of deletion. Returns the list of
    deleted tags.

    `list_client.delete_release(tag)` is what actually performs each delete
    (a repo-level operation, correct regardless of which tag `list_client`
    itself was constructed against -- see `GhReleaseClient.delete_release`,
    which acts on whichever tag is passed and ignores its own
    constructor-bound tag). `client_factory(tag)` is still consulted per tag
    first, purely as a defensive existence check: a tag that's already gone
    (e.g. a concurrent/manual prune) is skipped rather than raising, so this
    function stays idempotent under a race instead of failing loud on a
    delete-of-something-already-deleted."""
    all_tags = list_client.list_releases()
    snapshot_tags = sorted(t for t in all_tags if t.startswith(SNAPSHOT_TAG_PREFIX))
    to_delete = snapshot_tags[:-keep] if keep > 0 else snapshot_tags
    deleted: list[str] = []
    for tag in to_delete:
        if not client_factory(tag).exists():
            continue
        list_client.delete_release(tag)
        deleted.append(tag)
    return deleted
