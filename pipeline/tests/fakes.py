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

    def _tick(self) -> None:
        self.ops += 1
        if self.fail_after is not None and self.ops > self.fail_after:
            raise ReleaseError(f"injected failure at op {self.ops}")

    def exists(self) -> bool:
        self._tick()
        return self._exists

    def create(self) -> None:
        self._tick()
        self._exists = True

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
