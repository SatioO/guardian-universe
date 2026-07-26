"""Approval-gated, sequential Screener classification collection."""
from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests

from pipeline import config
from pipeline.sources.classification_publication import Provenance
from pipeline.sources.screener_classification import (
    ClassificationObservation,
    parse_screener_classification,
)

_EXTRACTOR_VERSION = "screener-peer-titles-v1"
_APPROVAL_SCOPE = "Screener company pages for the active NSE EQ/BE Classification Registry"
_REQUIRED_CONSTRAINTS = frozenset({
    "sequential requests",
    "rate limited",
    "stop and defer on rate-limit or sustained-block responses",
})
_APPROVAL_PATH = config.PROJECT_ROOT / "seeds" / "classification_access_approval.json"


class CollectionDeferred(RuntimeError):
    """The run must stop and defer the remaining symbols."""


@dataclass(frozen=True)
class CollectedClassification:
    observation: ClassificationObservation
    provenance: Provenance


class ScreenerClassificationCollector:
    """Approval-gated, capped, sequential Screener page collector."""

    def __init__(
        self,
        session: requests.Session,
        *,
        approval_path: Path = _APPROVAL_PATH,
        now: Callable[[], datetime] = datetime.now,
        timeout_seconds: int = 30,
        max_requests: int = 25,
        min_interval_seconds: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        _require_approval(approval_path)
        if max_requests < 1 or min_interval_seconds < 0:
            raise ValueError("collector cap and interval must be non-negative")
        self._session = session
        self._now = now
        self._timeout_seconds = timeout_seconds
        self._max_requests = max_requests
        self._min_interval_seconds = min_interval_seconds
        self._monotonic = monotonic
        self._sleep = sleep
        self._request_count = 0
        self._last_request_at: float | None = None

    def collect(self, symbol: str) -> CollectedClassification | None:
        if self._request_count >= self._max_requests:
            raise CollectionDeferred("collector request cap reached")
        if self._last_request_at is not None:
            remaining = self._min_interval_seconds - (
                self._monotonic() - self._last_request_at
            )
            if remaining > 0:
                self._sleep(remaining)
        normalized = symbol.strip().upper()
        url = f"https://www.screener.in/company/{quote(normalized)}/consolidated/"
        try:
            response = self._session.get(url, timeout=self._timeout_seconds)
        except requests.RequestException as error:
            raise CollectionDeferred("source request failed") from error
        self._request_count += 1
        self._last_request_at = self._monotonic()
        if response.status_code in (403, 429):
            raise CollectionDeferred(f"rate limit or block response ({response.status_code})")
        if response.status_code >= 500:
            raise CollectionDeferred(f"sustained-source response ({response.status_code})")
        if response.status_code != 200:
            return None
        observation = parse_screener_classification(response.text)
        if observation is None:
            return None
        return CollectedClassification(
            observation=observation,
            provenance=Provenance(
                observed_at=self._now(),
                source_url=url,
                extractor_version=_EXTRACTOR_VERSION,
                source_fragment_hash=hashlib.sha256(response.content).hexdigest(),
            ),
        )


def _require_approval(path: Path) -> None:
    try:
        approval = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise CollectionDeferred("classification access approval is missing") from error
    constraints = set(approval.get("constraints", []))
    if (
        not approval.get("approved_at")
        or approval.get("scope") != _APPROVAL_SCOPE
        or not _REQUIRED_CONSTRAINTS.issubset(constraints)
    ):
        raise CollectionDeferred("classification access approval is missing")
