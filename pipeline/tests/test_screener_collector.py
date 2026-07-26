from __future__ import annotations

import json
from datetime import datetime

import pytest

from pipeline.sources.screener_collector import (
    CollectionDeferred,
    ScreenerClassificationCollector,
)

_PAGE = """
<a title="Broad Sector">Energy</a>
<a title="Sector">Oil, Gas &amp; Consumable Fuels</a>
<a title="Broad Industry">Petroleum Products</a>
<a title="Industry">Refineries &amp; Marketing</a>
"""


class _Response:
    def __init__(self, status_code: int, text: str = _PAGE) -> None:
        self.status_code = status_code
        self.text = text
        self.content = text.encode()


class _Session:
    def __init__(self, response: _Response) -> None:
        self.response = response
        self.urls: list[str] = []

    def get(self, url: str, *, timeout: int) -> _Response:
        self.urls.append(url)
        return self.response


def _approval_path(tmp_path):
    path = tmp_path / "approval.json"
    path.write_text(
        json.dumps(
            {
                "approved_at": "2026-07-26T00:00:00+05:30",
                "scope": "Screener company pages for the active NSE EQ/BE Classification Registry",
                "constraints": [
                    "sequential requests",
                    "rate limited",
                    "stop and defer on rate-limit or sustained-block responses",
                ],
            }
        )
    )
    return path


def test_collector_records_real_page_provenance(tmp_path):
    approval_path = _approval_path(tmp_path)
    session = _Session(_Response(200))
    collector = ScreenerClassificationCollector(
        session, approval_path=approval_path, now=lambda: datetime(2026, 7, 26, 10)
    )

    collected = collector.collect("RELIANCE")

    assert collected is not None
    assert collected.provenance.source_url.endswith("/company/RELIANCE/consolidated/")
    assert collected.provenance.observed_at == datetime(2026, 7, 26, 10)
    assert len(collected.provenance.source_fragment_hash) == 64


def test_collector_stops_on_rate_limit(tmp_path):
    approval_path = _approval_path(tmp_path)
    collector = ScreenerClassificationCollector(
        _Session(_Response(429)), approval_path=approval_path
    )


    with pytest.raises(CollectionDeferred, match="rate limit"):
        collector.collect("RELIANCE")


def test_collector_enforces_request_cap_and_minimum_pacing(tmp_path):
    approval_path = _approval_path(tmp_path)
    clock = iter([0.0, 0.2, 1.0])
    pauses: list[float] = []
    collector = ScreenerClassificationCollector(
        _Session(_Response(200)),
        approval_path=approval_path,
        max_requests=2,
        min_interval_seconds=1.0,
        monotonic=lambda: next(clock),
        sleep=pauses.append,
    )

    collector.collect("RELIANCE")
    collector.collect("INFY")

    assert pauses == [0.8]
    with pytest.raises(CollectionDeferred, match="cap"):
        collector.collect("TCS")
