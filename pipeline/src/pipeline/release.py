"""GitHub Release access layer: the single seam for `gh` CLI interaction.

`ReleaseClient` is the injectable protocol; production uses `GhReleaseClient`
(subprocess `gh`), tests use `tests.fakes.FakeReleaseClient`. Keeping every
`gh` invocation here means publish/sync logic stays pure and offline-testable.

CONTRACT (G2 task 8 hygiene): `GhReleaseClient.exists()` distinguishes "the
release genuinely does not exist" from "gh itself failed for some other
reason" by matching the literal substring `"HTTP 404"` in `gh`'s stderr --
this is `gh api`'s actual, observed stderr format for a not-found response
(e.g. `"gh: Not Found (HTTP 404)"`). This match is intentionally STRICT: a
bare `"404"` substring is too loose (it would also match unrelated messages
that merely contain the digits "404" -- a rate-limit retry-after value, a
request id, etc. -- silently misreporting a real failure as "release
absent"). There is no looser fallback guard; any stderr not containing
`"HTTP 404"` is treated as an unexpected failure and raises `ReleaseError`."""
from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from pipeline.errors import ReleaseError

CaptureRunner = Callable[[list[str]], tuple[int, str, str]]


def subprocess_capture(cmd: list[str]) -> tuple[int, str, str]:
    p = subprocess.run(cmd, check=False, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


@dataclass(frozen=True)
class AssetInfo:
    name: str
    created_at: str  # ISO-8601 as returned by the GitHub API


class ReleaseClient(Protocol):
    def exists(self) -> bool: ...
    def create(self) -> None: ...
    def list_assets(self) -> list[AssetInfo]: ...
    def download(self, names: list[str], dest: Path) -> None: ...
    def upload(self, path: Path, *, clobber: bool = False) -> None: ...
    def delete_asset(self, name: str) -> None: ...


class GhReleaseClient:
    def __init__(self, *, repo: str, tag: str, runner: CaptureRunner = subprocess_capture) -> None:
        self._repo = repo
        self._tag = tag
        self._run = runner

    def exists(self) -> bool:
        rc, _, err = self._run(["gh", "api", f"repos/{self._repo}/releases/tags/{self._tag}"])
        if rc == 0:
            return True
        # Strict match (G2 task 8 hygiene): gh CLI's stderr contract for a
        # genuine not-found response wraps the status code as "HTTP 404"
        # (e.g. "gh: Not Found (HTTP 404)") -- that exact substring is the
        # ONLY thing this treats as "the release does not exist". A bare
        # "404" substring match is too loose: it would also match on
        # unrelated messages that merely happen to contain the digits "404"
        # (a rate-limit retry-after value, a request id, etc.), silently
        # misreporting a real error as "release absent" instead of raising.
        # Deliberately no fallback/looser guard -- "HTTP 404" is the
        # contract; anything else is an unexpected failure and must raise.
        if "HTTP 404" in err:
            return False
        raise ReleaseError(f"cannot determine release state: {err.strip()}")

    def create(self) -> None:
        rc, _, err = self._run([
            "gh", "release", "create", self._tag, "--repo", self._repo,
            "--title", self._tag, "--notes", "automated data release",
        ])
        if rc != 0:
            raise ReleaseError(f"release create failed: {err.strip()}")

    def list_assets(self) -> list[AssetInfo]:
        rc, out, err = self._run([
            "gh", "api", f"repos/{self._repo}/releases/tags/{self._tag}",
            "--jq", "[.assets[] | {name, created_at}]",
        ])
        if rc != 0:
            raise ReleaseError(f"asset listing failed: {err.strip()}")
        parsed: list[dict[str, str]] = json.loads(out)
        return [AssetInfo(name=a["name"], created_at=a["created_at"]) for a in parsed]

    def download(self, names: list[str], dest: Path) -> None:
        dest.mkdir(parents=True, exist_ok=True)
        for name in names:
            rc, _, err = self._run([
                "gh", "release", "download", self._tag, "--repo", self._repo,
                "--pattern", name, "--dir", str(dest), "--clobber",
            ])
            if rc != 0 or not (dest / name).exists():
                raise ReleaseError(f"download failed for {name}: {err.strip()}")

    def upload(self, path: Path, *, clobber: bool = False) -> None:
        cmd = ["gh", "release", "upload", self._tag, str(path), "--repo", self._repo]
        if clobber:
            cmd.append("--clobber")
        rc, _, err = self._run(cmd)
        if rc != 0:
            raise ReleaseError(f"upload failed for {path.name}: {err.strip()}")

    def delete_asset(self, name: str) -> None:
        rc, _, err = self._run([
            "gh", "release", "delete-asset", self._tag, name, "--repo", self._repo, "--yes",
        ])
        if rc != 0:
            raise ReleaseError(f"asset delete failed for {name}: {err.strip()}")
