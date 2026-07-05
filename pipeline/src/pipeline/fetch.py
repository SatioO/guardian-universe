"""Fetch adapter: the injectable I/O seam for acquiring a day's raw bhavcopy.

Source order: NSE UDiFF (primary) -> injected fallbacks (e.g. jugaad-data).
HTTP + retry + session-warming live here so business logic stays pure."""
from __future__ import annotations

import contextlib
import io
import time
import zipfile
from collections.abc import Callable, Sequence
from datetime import date
from typing import Protocol

import pandas as pd
import requests

from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.sources import nse_indices, nse_udiff

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_MAX_RETRIES = 3
_TIMEOUT = 30

Fallback = Callable[[date], pd.DataFrame]
Parser = Callable[[bytes], pd.DataFrame]


class Fetcher(Protocol):
    def fetch_raw(self, d: date) -> pd.DataFrame: ...


def _fetch_with_retry(
    session: requests.Session, url: str, d: date, *, parse: Parser
) -> pd.DataFrame:
    """Shared warm-session + retry contract used by every NSE fetcher.

    Best-effort warm-up: NSE deposits anti-bot cookies on the session here.
    A transient warm-up failure is non-fatal — proceed to the GET, which has
    its own retry loop and ultimately the caller's fallback chain."""
    with contextlib.suppress(requests.RequestException):
        session.get("https://www.nseindia.com/", timeout=_TIMEOUT)
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        resp = session.get(url, timeout=_TIMEOUT)
        if resp.status_code == 404:
            raise NotYetPublished(f"404 for {d.isoformat()}")
        if resp.status_code == 200:
            return parse(resp.content)
        last_exc = RuntimeError(f"HTTP {resp.status_code}")
        if attempt < _MAX_RETRIES - 1:
            time.sleep(2**attempt)  # 1s, then 2s (no sleep after the final attempt)
    raise UnexpectedFailure(f"primary exhausted for {d.isoformat()}: {last_exc}")


class NseUdiffFetcher:
    """Primary NSE fetcher with warm-session + retry, then injected fallbacks."""

    def __init__(
        self,
        session: requests.Session | None = None,
        fallbacks: Sequence[Fallback] = (),
    ) -> None:
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": _BROWSER_UA})
        self._fallbacks = tuple(fallbacks)

    def fetch_raw(self, d: date) -> pd.DataFrame:
        try:
            return self._fetch_primary(d)
        except NotYetPublished:
            raise
        except Exception:  # noqa: BLE001 - deliberate: any primary failure -> fallbacks
            return self._fetch_fallbacks(d)

    def _fetch_primary(self, d: date) -> pd.DataFrame:
        url = nse_udiff.build_udiff_url(d)
        return _fetch_with_retry(self._session, url, d, parse=_unzip_to_df)

    def _fetch_fallbacks(self, d: date) -> pd.DataFrame:
        for fb in self._fallbacks:
            try:
                return fb(d)
            except Exception:  # noqa: BLE001 - try the next source
                continue
        raise UnexpectedFailure(f"all sources exhausted for {d.isoformat()}")


class NseIndicesFetcher:
    """NSE indices-close fetcher: same warm-session + retry contract, plain CSV."""

    def __init__(
        self,
        session: requests.Session | None = None,
        fallbacks: Sequence[Fallback] = (),
    ) -> None:
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": _BROWSER_UA})
        self._fallbacks = tuple(fallbacks)

    def fetch_raw(self, d: date) -> pd.DataFrame:
        try:
            return self._fetch_primary(d)
        except NotYetPublished:
            raise
        except Exception:  # noqa: BLE001 - deliberate: any primary failure -> fallbacks
            return self._fetch_fallbacks(d)

    def _fetch_primary(self, d: date) -> pd.DataFrame:
        url = nse_indices.build_indices_url(d)
        return _fetch_with_retry(self._session, url, d, parse=_csv_to_df)

    def _fetch_fallbacks(self, d: date) -> pd.DataFrame:
        for fb in self._fallbacks:
            try:
                return fb(d)
            except Exception:  # noqa: BLE001 - try the next source
                continue
        raise UnexpectedFailure(f"all sources exhausted for {d.isoformat()}")


def _unzip_to_df(zip_bytes: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        if not names:
            raise UnexpectedFailure("downloaded archive is empty")
        with zf.open(names[0]) as fh:
            return pd.read_csv(fh)


def _csv_to_df(csv_bytes: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(csv_bytes))
