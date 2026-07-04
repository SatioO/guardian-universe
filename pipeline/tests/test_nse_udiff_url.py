from datetime import date

from pipeline.sources import nse_udiff


def test_build_udiff_url_uses_four_digit_year_format():
    # Oct-2025 UDiFF rename: BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv.zip
    url = nse_udiff.build_udiff_url(date(2026, 7, 3))
    assert url.endswith("BhavCopy_NSE_CM_0_0_0_20260703_F_0000.csv.zip")
    assert url.startswith("https://")


def test_build_udiff_url_zero_pads_month_and_day():
    url = nse_udiff.build_udiff_url(date(2026, 1, 5))
    assert "20260105" in url
