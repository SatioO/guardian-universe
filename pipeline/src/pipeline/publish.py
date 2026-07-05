"""Publish v2: content-addressed data assets, manifest flipped last, guarded.

Invariant delivered to clients: ANY manifest readable from the release
references only complete, sha-verifiable, still-present assets. Data assets
are immutable (content-addressed, never clobbered); `manifest.json` is the
single mutable pointer and is uploaded strictly last."""
from __future__ import annotations

import dataclasses
import json
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline import datasets
from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.manifest import build_manifest, dataset_files, file_digest, write_json
from pipeline.release import ReleaseClient
from pipeline.sync import SYNCED_STATE

PROTECTED_ASSETS = frozenset({"manifest.json", "last_run_status.json"})
GC_GRACE = timedelta(days=7)


def latest_trading_date(ohlc_dir: Path) -> date:
    latest = date.min
    for p in sorted(ohlc_dir.glob("ohlc_*.parquet")):
        col = pd.to_datetime(pd.read_parquet(p, columns=["date"])["date"])
        if col.empty:
            continue
        latest = max(latest, col.max().date())
    if latest == date.min:
        raise UnexpectedFailure("refusing to publish: store has no dated rows")
    return latest


def check_cas(live: dict[str, Any] | None, synced: dict[str, Any]) -> None:
    live_gen = live.get("generated_at") if live else None
    if live_gen != synced.get("generated_at"):
        raise UnexpectedFailure(
            f"live release changed since sync (live={live_gen!r}, "
            f"synced={synced.get('generated_at')!r}); re-run the pipeline"
        )


def check_no_shrink(new: dict[str, Any], live: dict[str, Any] | None) -> None:
    if live is None:
        return
    new_files = {f["name"]: f for f in dataset_files(new["datasets"][0])}
    for lf in dataset_files(live["datasets"][0]):
        nf = new_files.get(lf["name"])
        if nf is None:
            raise UnexpectedFailure(
                f"shrink-guard: {lf['name']} is on the live release but missing locally"
            )
        if "rows" in lf and nf["rows"] < lf["rows"]:
            raise UnexpectedFailure(
                f"shrink-guard: {lf['name']} rows {nf['rows']} < live {lf['rows']}"
            )
    if new["latest_trading_date"] < live["latest_trading_date"]:
        raise UnexpectedFailure("shrink-guard: latest_trading_date would regress")


def _read_live_manifest(
    client: ReleaseClient, work: Path, *, listed: set[str]
) -> dict[str, Any] | None:
    # Distinguish "manifest genuinely absent" (fresh release: safe to treat
    # as None) from "manifest is listed as present but failed to download"
    # (transient read failure: must NOT be treated as None, or a never-synced
    # runner with synced generated_at=None would sail through check_cas and
    # check_no_shrink and clobber a live, populated release).
    if "manifest.json" not in listed:
        return None
    try:
        client.download(["manifest.json"], work)
    except ReleaseError as e:
        raise UnexpectedFailure(
            f"manifest.json exists on the release but could not be read: {e}"
        ) from e
    loaded: dict[str, Any] = json.loads((work / "manifest.json").read_text())
    return loaded


def _verify(client: ReleaseClient, new_manifest: dict[str, Any], work: Path) -> None:
    """Post-flip verification: confirm the live manifest and its smallest
    referenced asset match what we just published.

    Posture — detect, do not restore: verification failure after the flip
    does NOT roll back. The run fails loudly (alert), the previous
    manifest's assets are still present (GC runs only after a successful
    verify), and the remediation is to re-run sync -> daily -> publish.
    Detection happens here; restoration happens via re-run.
    """
    client.download(["manifest.json"], work)
    live = json.loads((work / "manifest.json").read_text())
    if live != new_manifest:
        raise UnexpectedFailure(
            "post-publish verification failed: live manifest is not the one just published"
        )
    files = [e for ds in new_manifest["datasets"] for e in dataset_files(ds)]
    smallest = min(files, key=lambda e: int(e["bytes"]))
    client.download([smallest["asset"]], work)
    sha, _ = file_digest(work / smallest["asset"])
    if sha != smallest["sha256"]:
        raise UnexpectedFailure(
            f"post-publish verification failed: {smallest['asset']} sha mismatch"
        )


def _gc(client: ReleaseClient, new_manifest: dict[str, Any], now: datetime) -> None:
    """Best-effort garbage collection of unreferenced, aged-out assets.

    GC must NEVER fail a publish whose manifest flip already succeeded: the
    entire body (including the initial listing) is guarded against
    ReleaseError. Any failure here is logged to stderr and swallowed; `_gc`
    always returns normally so step 12 (updating the synced-state baseline)
    still runs.
    """
    try:
        referenced = {
            e["asset"]
            for ds in new_manifest["datasets"]
            for e in [*dataset_files(ds), *ds.get("deltas", [])]
        }
        for a in client.list_assets():
            if a.name in referenced or a.name in PROTECTED_ASSETS:
                continue
            try:
                created = datetime.fromisoformat(a.created_at.replace("Z", "+00:00"))
            except ValueError:
                # Malformed created_at: treat as "too young to GC" rather
                # than let a bad timestamp fail an otherwise-successful,
                # already-flipped publish.
                print(f"gc: skipping {a.name} (unparseable created_at {a.created_at!r})",
                      file=sys.stderr)
                continue
            if now - created < GC_GRACE:
                continue
            try:
                client.delete_asset(a.name)
            except ReleaseError as e:  # GC must never fail a good publish
                print(f"gc: could not delete {a.name}: {e}", file=sys.stderr)
    except ReleaseError as e:  # e.g. list_assets() itself failed transiently
        print(f"gc: skipped ({e})", file=sys.stderr)


def publish_dataset(
    *,
    ohlc_dir: Path,
    meta_dir: Path,
    stage_dir: Path,
    client: ReleaseClient,
    schema_version: int,
    generated_at: str,
    now: datetime,
) -> None:
    data_files = sorted(ohlc_dir.glob("ohlc_*.parquet"))
    if not data_files:
        raise UnexpectedFailure("refusing to publish: no data files (empty store)")
    del schema_version  # superseded by per-dataset spec.schema_version (Task 8 removes this param)
    # publish_dataset takes a single ohlc_dir today (pre-Task-8 signature); point
    # the equities spec at it so build_manifest reads from the caller's store
    # rather than the fixed config.OHLC_DIR. Task 8 threads real per-dataset dirs.
    specs = [
        dataclasses.replace(spec, base_dir=ohlc_dir) if spec.key == "equities" else spec
        for spec in datasets.all_specs()
    ]
    new_manifest = build_manifest(
        specs, latest_trading_date=latest_trading_date(ohlc_dir), generated_at=generated_at,
    )

    if not client.exists():
        client.create()

    stage_dir.mkdir(parents=True, exist_ok=True)
    existing = {a.name for a in client.list_assets()}
    live = _read_live_manifest(client, stage_dir / "_live", listed=existing)

    synced_path = meta_dir / SYNCED_STATE
    if not synced_path.exists():
        raise UnexpectedFailure("no synced state found: run sync before publish")
    synced: dict[str, Any] = json.loads(synced_path.read_text())

    check_cas(live, synced)
    check_no_shrink(new_manifest, live)

    # Upload new content-addressed data assets (immutable: no clobber).
    by_name = {p.name: p for p in data_files}
    for ds in new_manifest["datasets"]:
        for entry in dataset_files(ds):
            if entry["asset"] in existing:
                continue
            staged = stage_dir / entry["asset"]
            shutil.copyfile(by_name[entry["name"]], staged)
            client.upload(staged)

    status_path = meta_dir / "last_run_status.json"
    if status_path.exists():
        client.upload(status_path, clobber=True)

    manifest_path = meta_dir / "manifest.json"
    write_json(new_manifest, manifest_path)
    client.upload(manifest_path, clobber=True)  # THE FLIP — strictly last

    _verify(client, new_manifest, stage_dir / "_verify")
    _gc(client, new_manifest, now)
    write_json(new_manifest, synced_path)  # our publish is now the synced baseline
