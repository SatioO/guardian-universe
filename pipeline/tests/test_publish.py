from pathlib import Path

import pytest

from pipeline import publish
from pipeline.errors import UnexpectedFailure


class FakeRunner:
    def __init__(self, fail_on: str | None = None):
        self.calls: list[list[str]] = []
        self._fail_on = fail_on

    def __call__(self, cmd: list[str]) -> int:
        self.calls.append(cmd)
        if self._fail_on is not None and any(self._fail_on in a for a in cmd):
            return 1
        return 0


def test_publish_uploads_data_files_before_manifest(tmp_path: Path):
    a = tmp_path / "ohlc_2026.parquet"
    a.write_text("x")
    m = tmp_path / "manifest.json"
    m.write_text("{}")
    r = FakeRunner()
    publish.publish_release([a], m, tag="data-latest", repo="o/r", runner=r)
    uploads = [c for c in r.calls if "upload" in c]
    assert len(uploads) == 2
    # data file uploaded before the manifest (approximate atomicity)
    assert str(a) in uploads[0] and str(m) in uploads[1]
    assert "--clobber" in uploads[0]


def test_publish_ignores_release_create_failure(tmp_path: Path):
    # A pre-existing release makes `gh release create` fail; upload must still proceed.
    a = tmp_path / "ohlc_2026.parquet"
    a.write_text("x")
    m = tmp_path / "manifest.json"
    m.write_text("{}")
    r = FakeRunner(fail_on="create")
    publish.publish_release([a], m, tag="data-latest", repo="o/r", runner=r)  # no raise
    assert any("upload" in c for c in r.calls)


def test_publish_raises_when_an_upload_fails(tmp_path: Path):
    a = tmp_path / "ohlc_2026.parquet"
    a.write_text("x")
    m = tmp_path / "manifest.json"
    m.write_text("{}")
    r = FakeRunner(fail_on="ohlc_2026")  # the data upload fails
    with pytest.raises(UnexpectedFailure):
        publish.publish_release([a], m, tag="data-latest", repo="o/r", runner=r)


def test_publish_release_refuses_empty_data(tmp_path: Path):
    m = tmp_path / "manifest.json"
    m.write_text("{}")
    with pytest.raises(UnexpectedFailure):
        publish.publish_release([], m, tag="data-latest", repo="o/r", runner=FakeRunner())
