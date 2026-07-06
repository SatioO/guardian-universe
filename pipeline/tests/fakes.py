"""Deterministic in-memory ReleaseClient + release-consistency invariant.

`fail_after=N` makes the (N+1)-th and every later client operation raise —
the chaos-test harness for torn-publish scenarios."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pipeline.errors import ReleaseError
from pipeline.manifest import dataset_files
from pipeline.release import AssetInfo


class FakeReleaseClient:
    def __init__(self, *, exists: bool = False, now_iso: str = "2026-07-05T12:00:00Z") -> None:
        self.assets: dict[str, bytes] = {}
        self.created_at: dict[str, str] = {}
        self.now_iso = now_iso
        self._exists = exists
        self.fail_after: int | None = None
        self.ops = 0
        self.created_latest: bool | None = None

    def _tick(self) -> None:
        self.ops += 1
        if self.fail_after is not None and self.ops > self.fail_after:
            raise ReleaseError(f"injected failure at op {self.ops}")

    def exists(self) -> bool:
        self._tick()
        return self._exists

    def create(self, *, latest: bool = True) -> None:
        self._tick()
        self._exists = True
        self.created_latest = latest

    def list_assets(self) -> list[AssetInfo]:
        self._tick()
        return [AssetInfo(name=n, created_at=self.created_at[n]) for n in sorted(self.assets)]

    def download(self, names: list[str], dest: Path) -> None:
        self._tick()
        dest.mkdir(parents=True, exist_ok=True)
        for name in names:
            if name not in self.assets:
                raise ReleaseError(f"asset not found: {name}")
            (dest / name).write_bytes(self.assets[name])

    def upload(self, path: Path, *, clobber: bool = False) -> None:
        self._tick()
        if path.name in self.assets and not clobber:
            raise ReleaseError(f"asset exists: {path.name}")
        self.assets[path.name] = path.read_bytes()
        self.created_at[path.name] = self.now_iso

    def delete_asset(self, name: str) -> None:
        self._tick()
        if name not in self.assets:
            raise ReleaseError(f"asset not found: {name}")
        del self.assets[name]
        del self.created_at[name]

    # -- test helpers (not part of the ReleaseClient protocol) --
    def seed(self, name: str, data: bytes, created_at: str | None = None) -> None:
        self.assets[name] = data
        self.created_at[name] = created_at or self.now_iso
        self._exists = True


class FakeReleaseRepo:
    """Deterministic in-memory multi-tag release repo: models the ONE
    underlying GitHub repo that `snapshot.py`'s `create_snapshot`/
    `prune_snapshots` operate over, as a `dict[str, FakeReleaseClient]` keyed
    by tag.

    This is purely ADDITIVE alongside `FakeReleaseClient` -- it does not
    modify that class at all, so every existing single-tag `FakeReleaseClient`
    usage across the rest of the suite is untouched. `FakeReleaseClient`
    itself still only ever models ONE tag; `FakeReleaseRepo` is what lets
    tests model several tags (e.g. `data-latest` plus several
    `data-snapshot-YYYYMM` tags) sharing one fake "repo" namespace."""

    def __init__(self) -> None:
        self._clients: dict[str, FakeReleaseClient] = {}

    def client_for(self, tag: str) -> FakeReleaseClient:
        """Factory: same signature shape as the real
        `dest_client_factory: Callable[[str], ReleaseClient]` the CLI/snapshot
        code is built against. Returns the SAME `FakeReleaseClient` instance
        on every call for a given tag (lazily created on first access) --
        callers that build a client for a tag and later fetch "the client for
        that tag" again see the identical object/state, matching how the real
        `GhReleaseClient(repo=..., tag=t)` addresses the same underlying
        release across multiple constructions."""
        if tag not in self._clients:
            self._clients[tag] = FakeReleaseClient()
        return self._clients[tag]

    def list_releases(self) -> list[str]:
        return list(self._clients)

    def delete_release(self, tag: str) -> None:
        del self._clients[tag]

    def tags(self) -> list[str]:
        """Test helper (not part of the ReleaseClient protocol): every tag
        currently present in this repo, for assertions."""
        return list(self._clients)

    def as_list_client(self) -> FakeReleaseRepo:
        """Returns self: `list_releases`/`delete_release` above already give
        this object the shape callers need for the "list client" role (e.g.
        `snapshot.prune_snapshots`'s `list_client: ReleaseClient` parameter) --
        this accessor just names that role explicitly at call sites instead of
        passing the repo object bare."""
        return self


def assert_release_consistent(fake: FakeReleaseClient) -> None:
    """The G0 invariant: whatever manifest is live, every file it references
    (baseline/files, v1 or v2, plus any v2 deltas) exists on the release with
    a matching sha256."""
    if "manifest.json" not in fake.assets:
        return
    manifest_obj = json.loads(fake.assets["manifest.json"])
    for ds in manifest_obj["datasets"]:
        for entry in [*dataset_files(ds), *ds.get("deltas", [])]:
            asset = entry.get("asset", entry["name"])
            assert asset in fake.assets, f"manifest references missing asset {asset}"
            sha = hashlib.sha256(fake.assets[asset]).hexdigest()
            assert sha == entry["sha256"], f"sha mismatch for {asset}"
