from pathlib import Path

import pytest

from pipeline.errors import ReleaseError
from pipeline.release import AssetInfo, GhReleaseClient
from tests.fakes import FakeReleaseClient


class RecordingRunner:
    """Records commands; returns scripted (rc, stdout, stderr) per call."""

    def __init__(self, results: list[tuple[int, str, str]]) -> None:
        self.calls: list[list[str]] = []
        self._results = list(results)

    def __call__(self, cmd: list[str]) -> tuple[int, str, str]:
        self.calls.append(cmd)
        return self._results.pop(0)


def test_exists_true_on_rc0():
    r = RecordingRunner([(0, "{}", "")])
    assert GhReleaseClient(repo="o/r", tag="t", runner=r).exists() is True
    assert r.calls[0][:2] == ["gh", "api"] and "releases/tags/t" in r.calls[0][2]


def test_exists_false_on_404():
    r = RecordingRunner([(1, "", "gh: Not Found (HTTP 404)")])
    assert GhReleaseClient(repo="o/r", tag="t", runner=r).exists() is False


def test_exists_raises_on_other_error():
    r = RecordingRunner([(1, "", "network unreachable")])
    with pytest.raises(ReleaseError):
        GhReleaseClient(repo="o/r", tag="t", runner=r).exists()


def test_list_assets_parses_names_and_dates():
    out = '[{"name": "a.parquet", "created_at": "2026-07-01T00:00:00Z"}]'
    r = RecordingRunner([(0, out, "")])
    assets = GhReleaseClient(repo="o/r", tag="t", runner=r).list_assets()
    assert assets == [AssetInfo(name="a.parquet", created_at="2026-07-01T00:00:00Z")]


def test_download_raises_when_file_absent_after_rc0(tmp_path: Path):
    # gh returns 0 for a pattern matching nothing -> must still be an error.
    r = RecordingRunner([(0, "", "")])
    with pytest.raises(ReleaseError):
        GhReleaseClient(repo="o/r", tag="t", runner=r).download(["missing.parquet"], tmp_path)


def test_upload_appends_clobber_only_when_asked(tmp_path: Path):
    f = tmp_path / "m.json"
    f.write_text("{}")
    r = RecordingRunner([(0, "", ""), (0, "", "")])
    c = GhReleaseClient(repo="o/r", tag="t", runner=r)
    c.upload(f)
    c.upload(f, clobber=True)
    assert "--clobber" not in r.calls[0]
    assert "--clobber" in r.calls[1]


def test_fake_roundtrip_and_failure_injection(tmp_path: Path):
    fake = FakeReleaseClient()
    fake.create()
    f = tmp_path / "x.bin"
    f.write_bytes(b"abc")
    fake.upload(f)
    fake.download(["x.bin"], tmp_path / "out")
    assert (tmp_path / "out" / "x.bin").read_bytes() == b"abc"
    fake.fail_after = fake.ops  # next op fails
    with pytest.raises(ReleaseError):
        fake.list_assets()


def test_fake_upload_without_clobber_rejects_existing(tmp_path: Path):
    fake = FakeReleaseClient(exists=True)
    f = tmp_path / "x.bin"
    f.write_bytes(b"abc")
    fake.upload(f)
    with pytest.raises(ReleaseError):
        fake.upload(f)
