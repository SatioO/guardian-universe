"""Publish v2: content-addressed data assets, manifest flipped last, guarded.

Invariant delivered to clients: ANY manifest readable from the release
references only complete, sha-verifiable, still-present assets. Data assets
are immutable (content-addressed, never clobbered); `manifest.json` is the
single mutable pointer and is uploaded strictly last.

SHARED-RELEASE OWNERSHIP. `manifest.json` is a WHOLE-RELEASE document, but
several producers write it: data-daily, fundamentals-daily, and sibling repos
publishing into the same tag. Every runner rebuilds the manifest from its own
local store, scoped to its own dataset registry, so a dataset it does not
produce can never appear in the manifest it builds -- it is not "dropped", it
is never constructible. Ownership is therefore registry membership: a runner
verifies and rewrites the datasets ITS specs describe, and carries every other
producer's entries through verbatim (`carry_forward_foreign_datasets`) so a
publish never unreferences data it merely doesn't know about.

Ownership binds twice, because publish both writes the manifest and deletes
assets, and BOTH once assumed registry == release:

* dataset names (`owned`) scope what `check_no_shrink` will fail on and what
  gets carried forward -- a dataset missing locally is a fault only if we
  produce it;
* asset names (`owns_asset` / `file_prefixes`) scope what `_gc` may delete --
  an asset unreferenced by our manifest is dead only if we could have written
  it, since a sibling producer may index its own assets somewhere we never
  read.

`sync.py` and `snapshot.py` already treat the manifest as bigger than the
registry; publish was the last reader that didn't."""

from __future__ import annotations

import copy
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
    {
        "manifest.json",
        "last_run_status.json",
        "classification_collection_status.json",
        "fundamentals_state.json",
    }
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
    new: dict[str, Any],
    live: dict[str, Any] | None,
    *,
    owned: set[str] | None = None,
    allow_shrink: bool = False,
) -> None:
    """Guard against a publish that would drop data from the live release.

    `owned` is the set of manifest dataset names THIS runner produces (its
    registry -- see the module docstring). A live dataset outside `owned`
    belongs to another producer on the shared release and was never going to
    be rebuildable from this runner's store, so its absence from `new` is not
    a shrink: `carry_forward_foreign_datasets` owns that dataset's fate, not
    this guard. Datasets INSIDE `owned` are unchanged -- one that vanishes
    locally is exactly the accident this guard exists to catch. `owned=None`
    (the default) treats every live dataset as owned, i.e. the behaviour that
    predates the shared release; callers wanting the pure check keep it.

    `allow_shrink` (operator opt-in, default off) downgrades the per-file
    row-COUNT-shrink check to a stderr warning -- for a DELIBERATE correction
    that legitimately reduces a dataset (e.g. rebuilding ca_flags after fixing
    a bug that had inflated it with false rows). It never relaxes the two
    almost-always-accidental checks: an owned file/dataset that vanishes
    locally, and a `latest_trading_date` regression, both stay hard errors
    regardless.
    """
    if live is None:
        return
    new_by_name = {ds["name"]: ds for ds in new["datasets"]}
    for live_ds in live["datasets"]:
        live_files = dataset_files(live_ds)
        new_ds = new_by_name.get(live_ds["name"])
        if new_ds is None:
            if live_files and (owned is None or live_ds["name"] in owned):
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


def _carryable(entry: Any, *, existing: set[str]) -> bool:
    """Is this foreign manifest entry safe for us to re-reference?

    It must supply every key the rest of publish indexes WITHOUT guarding --
    `asset` (`_gc`'s reference set, `_verify`'s download), `sha256`
    (`_verify`'s comparison) and an int-able `bytes` (`_verify`'s
    smallest-asset `min`) -- and its asset must still be on the release, so a
    deleted or retired one is never resurrected.

    Checking `bytes` here rather than trusting it matters because `_verify`
    runs AFTER the flip: an entry that blows up there costs a red run on a
    release that is actually fine, and every re-run fails identically. Refusing
    to carry it is strictly cheaper."""
    if not isinstance(entry, dict) or not {"asset", "sha256", "bytes"} <= entry.keys():
        return False
    try:
        int(entry["bytes"])
    except (TypeError, ValueError):
        return False
    asset: Any = entry["asset"]
    return isinstance(asset, str) and asset in existing


def carry_forward_foreign_datasets(
    new: dict[str, Any],
    live: dict[str, Any] | None,
    *,
    owned: set[str],
    existing: set[str],
) -> None:
    """Preserve live datasets THIS runner does not produce (shared release).

    The dataset-granularity generalisation of `carry_forward_deltas`. That one
    exists because a runner without local deltas would erase the live delta
    entries of a dataset it DOES produce; this one exists because a runner
    whose registry has no spec for a dataset at all cannot even construct its
    manifest entry -- `build_manifest` only ever emits `owned` names. Flipping
    such a manifest in would unreference every asset of the other producer's
    dataset and hide it from clients the instant we published. (It used to
    doom those assets too, once `GC_GRACE` expired; `_gc` is now namespace-
    scoped, so the two halves of the ownership rule fail independently rather
    than compounding into deletion.)

    Rules, mirroring `carry_forward_deltas` exactly:

    * Ownership is registry membership: a live name in `owned` is ours and is
      left to `build_manifest` / `check_no_shrink`; anything else is foreign.
    * A foreign dataset is copied VERBATIM (deep copy: schema_version,
      latest_date, baseline/files, deltas, and any key a future producer
      version adds). We do not own its shape, so we never rewrite it -- we
      reproduce exactly what its producer published or we carry nothing.
    * It is carried only if EVERY entry is `_carryable`: still on the release,
      and shaped so the rest of publish can index it unguarded. A deleted
      asset is never re-referenced, so a producer that retires a dataset
      (drops the entry, or deletes its assets) is never overruled and nothing
      is permanently pinned.

    All-or-nothing is deliberate: a partially-present foreign dataset is
    dropped whole rather than rewritten into a shape its producer never
    published. Dropping it hides that dataset from clients, but does NOT
    destroy it: the assets stay outside our GC's namespace (`owns_asset`), so
    the producer can repair the entry and republish. The stderr note is the
    only way anyone finds out, so it must be read, not just logged.

    Every carried dataset is echoed to stderr on EVERY publish. A silently
    abandoned producer would otherwise freeze its `latest_date` on the release
    forever with nothing to notice it."""
    if live is None:
        return
    for live_ds in live.get("datasets", []):
        name = str(live_ds.get("name", ""))
        if name in owned:
            continue
        # Nothing below may assume the entry is well-formed. This is the ONE
        # place another repo's JSON reaches our publish path, and an exception
        # here fails the run before the flip -- i.e. exactly the stall this
        # whole mechanism exists to end, just with a different producer at
        # fault. A foreign entry we cannot read is dropped, never raised on.
        parts = [dataset_files(live_ds), live_ds.get("deltas", [])]
        if not all(isinstance(part, list) for part in parts):
            print(
                f"publish: NOT carrying foreign dataset {name!r} forward -- "
                "its baseline/deltas are not lists (malformed entry)",
                file=sys.stderr,
            )
            continue
        entries = [e for part in parts for e in part]
        unusable = [e for e in entries if not _carryable(e, existing=existing)]
        if unusable:
            first = unusable[0].get("name") if isinstance(unusable[0], dict) else unusable[0]
            print(
                f"publish: NOT carrying foreign dataset {name!r} forward -- "
                f"{len(unusable)} of {len(entries)} entries are unusable or no "
                f"longer on the release (first: {first!r})",
                file=sys.stderr,
            )
            continue
        new["datasets"].append(copy.deepcopy(live_ds))
        print(
            f"publish: carrying foreign dataset {name!r} forward verbatim "
            f"({len(entries)} assets, latest_date={live_ds.get('latest_date')}) -- "
            "not produced by this runner",
            file=sys.stderr,
        )


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
        e for ds in new_manifest["datasets"] for e in [*dataset_files(ds), *ds.get("deltas", [])]
    ]
    smallest = min(files, key=lambda e: int(e["bytes"]))
    client.download([smallest["asset"]], work)
    sha, _ = file_digest(work / smallest["asset"])
    if sha != smallest["sha256"]:
        raise UnexpectedFailure(
            f"post-publish verification failed: {smallest['asset']} sha mismatch"
        )


def owns_asset(name: str, *, file_prefixes: frozenset[str]) -> bool:
    """Could THIS runner have uploaded the release asset `name`?

    Every asset `publish_dataset` uploads is named off a spec's `file_prefix`:
    `{prefix}_*.{sha8}.parquet` for a baseline entry (`manifest.asset_name`),
    the same under a `delta_` prefix for a delta, and `{prefix}_{date}.parquet`
    for a quarantine extra. The only other things it writes are the mutable
    singletons in `PROTECTED_ASSETS`. So the prefix set of the specs being
    published is exactly the namespace this runner can produce -- the
    asset-level counterpart of the dataset-level ownership in
    `carry_forward_foreign_datasets`, and the answer to "is this mine to
    delete?" on a release several producers write into."""
    return any(name.removeprefix("delta_").startswith(f"{p}_") for p in file_prefixes)


def _gc(
    client: ReleaseClient,
    new_manifest: dict[str, Any],
    now: datetime,
    *,
    file_prefixes: frozenset[str],
) -> None:
    """Best-effort garbage collection of unreferenced, aged-out assets.

    SCOPED TO THIS RUNNER'S OWN ASSETS (`owns_asset`). Two conditions must both
    hold before an asset is deleted, and each catches what the other misses:

    * Unreferenced by the manifest we just flipped in. That set spans every
      dataset the manifest carries, another producer's included (see
      `carry_forward_foreign_datasets`), so GC can never collect a foreign
      dataset we are still pointing clients at.
    * Inside our namespace. `manifest.json` is not the only index on a shared
      release -- a sibling producer may track datasets of its own in its own
      index (and does: `producer_manifest.json`), whose assets OUR manifest
      never references and never will. Unreferenced-by-us is therefore NOT
      evidence that an asset is dead; only unreferenced AND ours is. Without
      this check GC deletes another producer's live baselines the moment they
      age past `GC_GRACE` -- the shared-release assumption that broke publish,
      one layer down and destructive rather than merely fatal.

    The trade is deliberate and one-directional: a dataset RETIRED from
    `datasets.DATASETS` leaves its assets behind forever, because nothing then
    claims their prefix. Leaking another producer's bytes is recoverable;
    deleting them is not. Retiring a dataset means sweeping its assets by hand.

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
        foreign: list[str] = []
        for a in client.list_assets():
            if a.name in referenced or a.name in PROTECTED_ASSETS:
                continue
            if not owns_asset(a.name, file_prefixes=file_prefixes):
                foreign.append(a.name)
                continue
            try:
                created = datetime.fromisoformat(a.created_at.replace("Z", "+00:00"))
            except ValueError:
                # Malformed created_at: treat as "too young to GC" rather
                # than let a bad timestamp fail an otherwise-successful,
                # already-flipped publish.
                print(
                    f"gc: skipping {a.name} (unparseable created_at {a.created_at!r})",
                    file=sys.stderr,
                )
                continue
            if now - created < GC_GRACE:
                continue
            try:
                client.delete_asset(a.name)
            except ReleaseError as e:  # GC must never fail a good publish
                print(f"gc: could not delete {a.name}: {e}", file=sys.stderr)
        if foreign:
            # One bounded line, not one per asset: the COUNT is the signal.
            # Steady state is another producer's superseded versions ageing
            # out; a number that climbs run after run is a producer that
            # stopped cleaning up after itself (or was decommissioned and left
            # its data behind), and those bytes are now ours to sweep by hand.
            print(
                f"gc: leaving {len(foreign)} unreferenced asset(s) outside this "
                f"runner's namespace alone (e.g. {foreign[0]})",
                file=sys.stderr,
            )
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
        specs,
        latest_trading_date=latest_trading_date(primary),
        generated_at=generated_at,
    )

    # Resolve dataset names against the specs passed in (not the global
    # registry) so a caller-supplied spec with an overridden base_dir (e.g.
    # tests pointing at a tmp_path store) is honoured; in production
    # `specs == datasets.all_specs()` so this is equivalent to
    # `datasets.by_manifest_name`. Its key set is also this runner's OWNERSHIP
    # set on the shared release: every other dataset the live manifest carries
    # belongs to a different producer (see carry_forward_foreign_datasets).
    by_manifest_name = {spec.manifest_name: spec for spec in specs}
    owned = set(by_manifest_name)
    # The same ownership question one level down, in the flat asset namespace
    # rather than the dataset one: which release assets could WE have written?
    # `_gc` needs it because "unreferenced by our manifest" does not mean
    # "dead" on a release someone else also publishes into (see owns_asset).
    file_prefixes = frozenset(spec.file_prefix for spec in specs)

    if not client.exists():
        client.create()

    stage_dir.mkdir(parents=True, exist_ok=True)
    existing = {a.name for a in client.list_assets()}
    live = _read_live_manifest(client, stage_dir / "_live", listed=existing)

    synced_path = meta_dir / SYNCED_STATE
    if not synced_path.exists():
        raise UnexpectedFailure("no synced state found: run sync before publish")
    synced: dict[str, Any] = json.loads(synced_path.read_text())

    # Guards first (they judge what this runner produced), then the carries
    # (they restore what it doesn't).
    check_cas(live, synced)
    check_no_shrink(new_manifest, live, owned=owned, allow_shrink=allow_shrink)
    carry_forward_deltas(new_manifest, live, existing=existing)
    carry_forward_foreign_datasets(new_manifest, live, owned=owned, existing=existing)

    # Upload new content-addressed data assets (immutable: no clobber).
    # Reconstruct real per-spec source paths: baseline files live at
    # spec.base_dir/entry["name"]; deltas live at spec.base_dir/"deltas"/entry["name"].
    worklist: list[tuple[Path, str]] = []
    for ds in new_manifest["datasets"]:
        spec = by_manifest_name.get(ds["name"])
        if spec is None:
            # Foreign (carried forward): no spec, so no local path to resolve
            # -- and nothing to upload either, since it was carried only
            # because every asset it references is already on the release.
            continue
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
        qfile = (
            meta_dir
            / "quarantine"
            / f"{spec.file_prefix}_{new_manifest['latest_trading_date']}.parquet"
        )
        if qfile.exists():
            client.upload(qfile, clobber=True)

    for status_name in ("last_run_status.json", "classification_collection_status.json"):
        status_path = meta_dir / status_name
        if status_path.exists():
            client.upload(status_path, clobber=True)

    manifest_path = meta_dir / "manifest.json"
    write_json(new_manifest, manifest_path)
    client.upload(manifest_path, clobber=True)  # THE FLIP — strictly last

    _verify(client, new_manifest, stage_dir / "_verify")
    _gc(client, new_manifest, now, file_prefixes=file_prefixes)
    write_json(new_manifest, synced_path)  # our publish is now the synced baseline
