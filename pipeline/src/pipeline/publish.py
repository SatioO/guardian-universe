"""Publish v2: content-addressed data assets, manifest flipped last, guarded.

Invariant delivered to clients: ANY manifest readable from the release
references only complete, sha-verifiable, still-present assets. Data assets
are immutable (content-addressed, never clobbered); `manifest.json` is the
single mutable pointer and is uploaded strictly last."""
from __future__ import annotations

import json
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from pipeline.errors import ReleaseError, UnexpectedFailure
from pipeline.manifest import build_manifest, dataset_files, file_digest, write_json
from pipeline.release import ReleaseClient
from pipeline.sync import SYNCED_STATE

if TYPE_CHECKING:
    from pipeline.datasets import DatasetSpec

# Mutable singletons uploaded with clobber=True, never content-addressed and
# never GC'd. `fundamentals_state.json` is the Rust producer's incremental
# state (uploaded by fundamentals-daily.yml AFTER a successful publish so
# state can never claim work the published parquet doesn't have).
PROTECTED_ASSETS = frozenset(
    {"manifest.json", "last_run_status.json", "fundamentals_state.json"}
)
GC_GRACE = timedelta(days=7)


def latest_trading_date(spec: DatasetSpec) -> date:
    latest = date.min
    for p in sorted(spec.base_dir.glob(f"{spec.file_prefix}_*.parquet")):
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


def check_no_shrink(
    new: dict[str, Any], live: dict[str, Any] | None, *, allow_shrink: bool = False
) -> None:
    """Guard against a publish that would drop data from the live release.

    `allow_shrink` (operator opt-in, default off) downgrades the per-file
    row-COUNT-shrink check to a stderr warning -- for a DELIBERATE correction
    that legitimately reduces a dataset (e.g. rebuilding ca_flags after fixing
    a bug that had inflated it with false rows). It never relaxes the two
    almost-always-accidental checks: a file/dataset that vanishes locally, and
    a `latest_trading_date` regression, both stay hard errors regardless.
    """
    if live is None:
        return
    new_by_name = {ds["name"]: ds for ds in new["datasets"]}
    for live_ds in live["datasets"]:
        live_files = dataset_files(live_ds)
        new_ds = new_by_name.get(live_ds["name"])
        if new_ds is None:
            if live_files:
                raise UnexpectedFailure(
                    f"shrink-guard: dataset {live_ds['name']!r} is on the live "
                    "release but missing locally"
                )
            continue
        new_files = {f["name"]: f for f in dataset_files(new_ds)}
        for lf in live_files:
            nf = new_files.get(lf["name"])
            if nf is None:
                raise UnexpectedFailure(
                    f"shrink-guard: {lf['name']} is on the live release but missing locally"
                )
            if "rows" in lf and nf["rows"] < lf["rows"]:
                msg = f"shrink-guard: {lf['name']} rows {nf['rows']} < live {lf['rows']}"
                if allow_shrink:
                    print(f"WARNING (--allow-shrink): {msg}", file=sys.stderr)
                    continue
                raise UnexpectedFailure(msg)
    if new["latest_trading_date"] < live["latest_trading_date"]:
        raise UnexpectedFailure("shrink-guard: latest_trading_date would regress")


def carry_forward_deltas(
    new: dict[str, Any], live: dict[str, Any] | None, *, existing: set[str]
) -> None:
    """Preserve a dataset's live delta window when THIS runner has none.

    Publishes can come from more than one ephemeral runner now (data-daily
    and fundamentals-daily share the release). A runner that never ran
    `daily` has no local delta files, and rebuilding the manifest from local
    state alone would silently erase the live manifest's delta entries for
    every dataset — degrading clients to baseline re-downloads until the next
    data-daily publish. Carry the live entries forward instead, but only
    those whose assets still exist on the release (a GC'd/missing asset must
    never be re-referenced). A runner WITH local deltas (data-daily) is left
    untouched — its own freshly-built list wins, exactly as before."""
    if live is None:
        return
    live_by_name = {ds["name"]: ds for ds in live.get("datasets", [])}
    for ds in new["datasets"]:
        if ds.get("deltas"):
            continue
        live_ds = live_by_name.get(ds["name"])
        if live_ds is None:
            continue
        carried = [d for d in live_ds.get("deltas", []) if d.get("asset") in existing]
        if carried:
            ds["deltas"] = carried


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
    files = [
        e
        for ds in new_manifest["datasets"]
        for e in [*dataset_files(ds), *ds.get("deltas", [])]
    ]
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
    specs: list[DatasetSpec],
    meta_dir: Path,
    stage_dir: Path,
    client: ReleaseClient,
    generated_at: str,
    now: datetime,
    allow_shrink: bool = False,
) -> None:
    # Empty-store guard: only the PRIMARY dataset (specs[0], equities) must
    # have baseline files. Other specs may be legitimately empty (they are
    # simply omitted from the manifest by build_manifest).
    primary = specs[0]
    if not sorted(primary.base_dir.glob(f"{primary.file_prefix}_*.parquet")):
        raise UnexpectedFailure("refusing to publish: no data files (empty store)")

    new_manifest = build_manifest(
        specs, latest_trading_date=latest_trading_date(primary), generated_at=generated_at,
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
    check_no_shrink(new_manifest, live, allow_shrink=allow_shrink)
    carry_forward_deltas(new_manifest, live, existing=existing)

    # Upload new content-addressed data assets (immutable: no clobber).
    # Reconstruct real per-spec source paths: baseline files live at
    # spec.base_dir/entry["name"]; deltas live at spec.base_dir/"deltas"/entry["name"].
    # Resolve against the specs passed in (not the global registry) so a
    # caller-supplied spec with an overridden base_dir (e.g. tests pointing at
    # a tmp_path store) is honoured; in production `specs == datasets.all_specs()`
    # so this is equivalent to `datasets.by_manifest_name`.
    by_manifest_name = {spec.manifest_name: spec for spec in specs}
    worklist: list[tuple[Path, str]] = []
    for ds in new_manifest["datasets"]:
        spec = by_manifest_name.get(ds["name"])
        assert spec is not None  # manifest was built from these specs
        for entry in dataset_files(ds):
            worklist.append((spec.base_dir / entry["name"], entry["asset"]))
        for entry in ds.get("deltas", []):
            worklist.append((spec.base_dir / "deltas" / entry["name"], entry["asset"]))
    for src, asset in worklist:
        if asset in existing:
            continue
        staged = stage_dir / asset
        shutil.copyfile(src, staged)
        client.upload(staged)

    # Quarantine extras: diagnostic-only, per spec, current latest_trading_date
    # day only. Not referenced by the manifest, so they self-GC after grace.
    for spec in specs:
        qfile = (meta_dir / "quarantine"
                 / f"{spec.file_prefix}_{new_manifest['latest_trading_date']}.parquet")
        if qfile.exists():
            client.upload(qfile, clobber=True)

    status_path = meta_dir / "last_run_status.json"
    if status_path.exists():
        client.upload(status_path, clobber=True)

    manifest_path = meta_dir / "manifest.json"
    write_json(new_manifest, manifest_path)
    client.upload(manifest_path, clobber=True)  # THE FLIP — strictly last

    _verify(client, new_manifest, stage_dir / "_verify")
    _gc(client, new_manifest, now)
    write_json(new_manifest, synced_path)  # our publish is now the synced baseline
