import io
import zipfile
from datetime import date

import pandas as pd
import pytest
import requests
import responses

from pipeline import fetch
from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.fetch import NseUdiffFetcher
from pipeline.sources import nse_udiff


def _zip_bytes(csv_text: str, name: str = "BhavCopy.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, csv_text)
    return buf.getvalue()


CSV = "TradDt,TckrSymb,ClsPric\n2026-07-03,RELIANCE,3000\n"


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch):
    """Backoff sleeps are real wall-clock time; neutralize them for fast tests."""
    monkeypatch.setattr(fetch.time, "sleep", lambda *_a, **_k: None)


@responses.activate
def test_fetch_raw_warms_session_then_downloads_and_unzips():
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET,
        nse_udiff.build_udiff_url(date(2026, 7, 3)),
        body=_zip_bytes(CSV),
        status=200,
        content_type="application/zip",
    )
    res = NseUdiffFetcher().fetch_raw(date(2026, 7, 3))
    df = res.frame
    assert list(df.columns) == ["TradDt", "TckrSymb", "ClsPric"]
    assert df.iloc[0]["TckrSymb"] == "RELIANCE"
    assert responses.calls[0].request.url.startswith("https://www.nseindia.com/")
    assert responses.calls[1].request.url == nse_udiff.build_udiff_url(date(2026, 7, 3))


@responses.activate
def test_fetch_raw_primary_success_reports_primary_label():
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET,
        nse_udiff.build_udiff_url(date(2026, 7, 3)),
        body=_zip_bytes(CSV),
        status=200,
        content_type="application/zip",
    )
    res = NseUdiffFetcher().fetch_raw(date(2026, 7, 3))
    assert res.source == "nse-udiff"


@responses.activate
def test_fetch_raw_404_raises_not_yet_published():
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET,
        nse_udiff.build_udiff_url(date(2026, 7, 3)),
        status=404,
    )
    with pytest.raises(NotYetPublished):
        NseUdiffFetcher(fallbacks=()).fetch_raw(date(2026, 7, 3))


@responses.activate
def test_fetch_raw_falls_back_when_primary_fails():
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET,
        nse_udiff.build_udiff_url(date(2026, 7, 3)),
        status=503,
    )
    fallback_df = pd.DataFrame({"TradDt": ["2026-07-03"], "TckrSymb": ["INFY"], "ClsPric": [1500]})

    def fallback(_d: date) -> pd.DataFrame:
        return fallback_df

    res = NseUdiffFetcher(fallbacks=(("secondary-label", fallback),)).fetch_raw(date(2026, 7, 3))
    assert res.frame.iloc[0]["TckrSymb"] == "INFY"


@responses.activate
def test_fetch_raw_fallback_success_reports_fallback_label():
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET,
        nse_udiff.build_udiff_url(date(2026, 7, 3)),
        status=503,
    )
    fallback_df = pd.DataFrame({"TradDt": ["2026-07-03"], "TckrSymb": ["INFY"], "ClsPric": [1500]})

    def fallback(_d: date) -> pd.DataFrame:
        return fallback_df

    res = NseUdiffFetcher(fallbacks=(("secondary-label", fallback),)).fetch_raw(date(2026, 7, 3))
    assert res.source == "secondary-label"


@responses.activate
def test_fetch_raw_all_sources_exhausted_raises_unexpected():
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET,
        nse_udiff.build_udiff_url(date(2026, 7, 3)),
        status=503,
    )

    def bad_fallback(_d: date) -> pd.DataFrame:
        raise RuntimeError("jugaad down")

    with pytest.raises(UnexpectedFailure):
        NseUdiffFetcher(fallbacks=(("bad-fallback", bad_fallback),)).fetch_raw(date(2026, 7, 3))


@responses.activate
def test_warmup_failure_still_allows_archive_fetch():
    # A transient warm-up failure must NOT abort — the archive GET still runs.
    responses.add(responses.GET, "https://www.nseindia.com/", body=requests.ConnectionError("dns"))
    responses.add(
        responses.GET,
        nse_udiff.build_udiff_url(date(2026, 7, 3)),
        body=_zip_bytes(CSV),
        status=200,
        content_type="application/zip",
    )
    df = NseUdiffFetcher().fetch_raw(date(2026, 7, 3)).frame
    assert df.iloc[0]["TckrSymb"] == "RELIANCE"
