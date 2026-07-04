"""Publish artifacts as assets on a rolling GitHub Release via the `gh` CLI.

The command runner is injected so the upload sequence is unit-tested offline;
production uses `subprocess_runner`."""
from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence
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
    extra_files: Sequence[Path] = (),
) -> None:
    if not data_files:
        raise UnexpectedFailure("refusing to publish: no data files (empty store)")
    runner(["gh", "release", "create", tag, "--repo", repo, "--title", tag,
            "--notes", "automated data release"])
    for f in [*data_files, *extra_files, manifest_path]:
        rc = runner(["gh", "release", "upload", tag, str(f), "--clobber", "--repo", repo])
        if rc != 0:
            raise UnexpectedFailure(f"gh release upload failed ({rc}) for {f.name}")
