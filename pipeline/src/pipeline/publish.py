"""Publish artifacts as assets on a rolling GitHub Release via the `gh` CLI.

The command runner is injected so the upload sequence is unit-tested offline;
production uses `subprocess_runner`."""
from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path

from pipeline.errors import UnexpectedFailure

Runner = Callable[[list[str]], int]


def subprocess_runner(cmd: list[str]) -> int:
    return subprocess.run(cmd, check=False).returncode


def publish_release(
    data_files: list[Path],
    manifest_path: Path,
    *,
    tag: str,
    repo: str,
    runner: Runner,
) -> None:
    # Idempotent create: fails (non-zero) if the release already exists — ignore it.
    runner(["gh", "release", "create", tag, "--repo", repo, "--title", tag,
            "--notes", "automated data release"])
    # Upload DATA files first, then the manifest LAST (approximate atomicity:
    # clients that poll the manifest only see it after the data it references).
    for f in [*data_files, manifest_path]:
        rc = runner(["gh", "release", "upload", tag, str(f), "--clobber", "--repo", repo])
        if rc != 0:
            raise UnexpectedFailure(f"gh release upload failed ({rc}) for {f.name}")
