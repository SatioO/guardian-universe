from datetime import date

import pandas as pd
import pytest
import responses

from pipeline import fetch
from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.fetch import NseIndicesFetcher
from pipeline.sources.nse_indices import INDICES_RAW_COLUMNS, build_indices_url

CSV = (
    b'"Index Name","Index Date","Open Index Value","High Index Value",'
    b'"Low Index Value","Closing Index Value","Points Change","Volume",'
    b'"Turnover (Rs. Cr.)"\n'
    b'"Nifty 50","03-07-2026","24500.10","24700.55","24450.00","24650.25",'
    b'"150.15","350000000","45000.50"\n'
    b'"Nifty Bank","03-07-2026","52000.00","52500.00","51800.00","52300.75",'
    b'"300.75","120000000","23000.10"\n'
)


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch):
    """Backoff sleeps are real wall-clock time; neutralize them for fast tests."""
    monkeypatch.setattr(fetch.time, "sleep", lambda *_a, **_k: None)


def test_url_builder_encodes_ddmmyyyy():
    assert build_indices_url(date(2026, 7, 3)) == (
        "https://nsearchives.nseindia.com/content/indices/ind_close_all_03072026.csv"
    )


def test_raw_columns_cover_the_csv_header():
    header = CSV.decode().splitlines()[0]
    for col in INDICES_RAW_COLUMNS:
        assert f'"{col}"' in header


@responses.activate
def test_fetch_parses_200_csv():
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET,
        build_indices_url(date(2026, 7, 3)),
        body=CSV,
        status=200,
        content_type="text/csv",
    )
    res = NseIndicesFetcher().fetch_raw(date(2026, 7, 3))
    df = res.frame
    assert len(df) == 2
    assert list(df.columns) == INDICES_RAW_COLUMNS
    assert df.iloc[0]["Index Name"] == "Nifty 50"


@responses.activate
def test_fetch_raw_primary_success_reports_primary_label():
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET,
        build_indices_url(date(2026, 7, 3)),
        body=CSV,
        status=200,
        content_type="text/csv",
    )
    res = NseIndicesFetcher().fetch_raw(date(2026, 7, 3))
    assert res.source == "nse-indices"


@responses.activate
def test_fetch_raw_fallback_success_reports_fallback_label():
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET,
        build_indices_url(date(2026, 7, 3)),
        status=503,
    )
    fallback_df = pd.DataFrame({"Index Name": ["Nifty 50"]})

    def fallback(_d: date) -> pd.DataFrame:
        return fallback_df

    res = NseIndicesFetcher(fallbacks=(("secondary-label", fallback),)).fetch_raw(date(2026, 7, 3))
    assert res.source == "secondary-label"
    assert res.frame.iloc[0]["Index Name"] == "Nifty 50"


@responses.activate
def test_404_raises_not_yet_published():
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET,
        build_indices_url(date(2026, 7, 3)),
        status=404,
    )
    with pytest.raises(NotYetPublished):
        NseIndicesFetcher(fallbacks=()).fetch_raw(date(2026, 7, 3))


@responses.activate
def test_repeated_500_exhausts_to_unexpected_failure():
    responses.add(responses.GET, "https://www.nseindia.com/", status=200)
    responses.add(
        responses.GET,
        build_indices_url(date(2026, 7, 3)),
        status=500,
    )
    with pytest.raises(UnexpectedFailure):
        NseIndicesFetcher(fallbacks=()).fetch_raw(date(2026, 7, 3))
