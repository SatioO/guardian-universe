"""One-off rebuild of the `reference` symbol master from price files on a
release: `python -m pipeline bootstrap-reference --tag <tag>`.

Since 2026-10-13 the daily run merges each run's sessions into the PRIOR
published instrument list (builders.build_reference) and refuses to run
without one, because no price history survives a run any more. This is the
way back when that prior is missing or broken, and the rollback path: the
equities price files published until then are still on `data-latest` (no
longer in its manifest, and outside GC's namespace) and inside every
`data-snapshot-*` release taken before.

Which files: a release whose manifest.json still lists the equities dataset
(a snapshot) is read through that manifest, every file sha256-verified. A
release whose manifest no longer lists it (`data-latest` after the
retirement) is read from its asset list: per year, the newest
`{prefix}_{YYYY}.{sha8}.parquet`, verified against the sha8 in its name.

Nothing is uploaded: this writes the local `instruments_all.parquet` only. The
operator (or data-daily's `bootstrap_reference_from` dispatch input) then runs
`daily` and `publish` as usual. See RUNBOOK.md 'Reference bootstrap'."""

from __future__ import annotations

import dataclasses
import json
import re
from datetime import date
from pathlib import Path
from typing import Any

from pipeline import builders
from pipeline.daily_update import RunStatus
from pipeline.datasets import DatasetSpec
from pipeline.errors import UnexpectedFailure
from pipeline.manifest import dataset_files, file_digest
from pipeline.release import ReleaseClient


@dataclasses.dataclass(frozen=True)
class _PriceFile:
    name: str  # logical: {prefix}_{YYYY}.parquet
    asset: str
    sha256: str  # a full digest from a manifest, or just the 8-char name prefix


def _from_manifest(manifest: dict[str, Any], source_spec: DatasetSpec) -> list[_PriceFile]:
    for ds in manifest.get("datasets", []):
        if ds.get("name") == source_spec.manifest_name:
            return [
                _PriceFile(e["name"], e.get("asset", e["name"]), e["sha256"])
                for e in dataset_files(ds)
            ]
    return []


def _from_assets(client: ReleaseClient, source_spec: DatasetSpec) -> list[_PriceFile]:
    prefix = re.escape(source_spec.file_prefix)
    pattern = re.compile(rf"^{prefix}_(\d{{4}})\.([0-9a-f]{{8}})\.parquet$")
    newest: dict[str, tuple[str, str, str]] = {}  # year -> (created_at, asset, sha8)
    for a in client.list_assets():
        m = pattern.match(a.name)
        if m is None:
            continue
        year, sha8 = m.group(1), m.group(2)
        if year not in newest or a.created_at > newest[year][0]:
            newest[year] = (a.created_at, a.name, sha8)
    return [
        _PriceFile(f"{source_spec.file_prefix}_{year}.parquet", asset, sha8)
        for year, (_, asset, sha8) in sorted(newest.items())
    ]


def price_files(
    client: ReleaseClient, work_dir: Path, source_spec: DatasetSpec
) -> list[_PriceFile]:
    """The price files to rebuild from, chosen as the module docstring says."""
    work_dir.mkdir(parents=True, exist_ok=True)
    names = {a.name for a in client.list_assets()}
    files: list[_PriceFile] = []
    if "manifest.json" in names:
        client.download(["manifest.json"], work_dir)
        manifest: dict[str, Any] = json.loads((work_dir / "manifest.json").read_text())
        files = _from_manifest(manifest, source_spec)
    return files or _from_assets(client, source_spec)


def bootstrap_reference(
    client: ReleaseClient,
    *,
    reference_spec: DatasetSpec,
    source_spec: DatasetSpec,
    work_dir: Path,
    target: date,
    keep_prior: bool = True,
) -> RunStatus:
    """Download and verify the price files, then rebuild `reference` from
    them (builders.rebuild_reference) into `reference_spec.base_dir`.

    Fail-closed: no price files, a failed download or a checksum mismatch
    raises before anything is written."""
    files = price_files(client, work_dir / "_index", source_spec)
    if not files:
        raise UnexpectedFailure(
            f"no {source_spec.file_prefix}_* price files on this release to rebuild from"
        )
    store_dir = work_dir / source_spec.file_prefix
    store_dir.mkdir(parents=True, exist_ok=True)
    for f in files:
        client.download([f.asset], work_dir)
        got = work_dir / f.asset
        sha, _ = file_digest(got)
        if len(f.sha256) < 8 or not sha.startswith(f.sha256):
            raise UnexpectedFailure(f"checksum mismatch for {f.asset}: got {sha}")
        got.replace(store_dir / f.name)
    source = dataclasses.replace(source_spec, base_dir=store_dir)
    return builders.rebuild_reference(
        reference_spec, target, source_spec=source, keep_prior=keep_prior
    )
