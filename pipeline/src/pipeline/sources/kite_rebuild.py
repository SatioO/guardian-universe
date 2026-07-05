"""Kite Connect manual day-rebuilder -- the LAST-RESORT recovery path when
both NSE sources (UDiFF primary + sec_bhavdata_full fallback, see fetch.py)
are down for a day, or a data hole predates what either NSE archive still
serves. Credential-gated (requires a freshly-generated Kite access token) and
human-triggered only via `pipeline rebuild-day` -- see cli.py's
`cmd_rebuild_day` docstring and RUNBOOK.md's "Kite manual day-rebuild"
section.

NEVER wire this into cron or the automatic fallback chain
(`NseUdiffFetcher.fallbacks`): Kite tokens expire daily and require a manual
browser-based login flow, so an unattended job would simply fail every day
once the token goes stale. This module exists purely for a human operator to
invoke on demand.

Shape contract: `day_frame` emits one row per successfully-rebuilt symbol in
the SAME UDiFF-raw column shape as the primary NSE fetcher and the
sec_bhavdata_full fallback (see fetch.py's fallback contract docstring), so
the existing `normalize_equity_bhavcopy` consumes it unchanged. Two fields
are degraded from the true UDiFF values because the Kite historical-candle
API does not expose them for a single day:

- `PrvsClsgPric` (previous close): the day-candle endpoint has no explicit
  previous-close field, and fetching a second (d-1) candle per symbol would
  double the already-slow per-symbol HTTP volume. Degrades to the day's own
  OPEN price -- a documented approximation, not the true prior session close.
- `TtlTrfVal` (turnover/value): not present in the day-candle payload at all.
  Defaults to 0.0 -- a documented gap, not a real turnover figure.

Both degradations are intentional and acceptable for a recovery path whose
job is to get *some* OHLCV data into the store for a hole, not to
byte-for-byte reproduce the official bhavcopy.

Broker-agnostic registration: this module implements `rebuild.RebuildSource`
(`id = "kite"`) and self-registers via `KiteDayRebuilder.from_env()` at
import time -- `cli.py` never mentions "kite" by name (see rebuild.py). Every
Kite-specific detail (URLs, auth header shape, the KITE_API_KEY/
KITE_ACCESS_TOKEN env var names, rate limiting) lives entirely inside this
class; a second broker would add a sibling module and one `register()` call,
with zero edits to cli.py or rebuild.py.
"""
from __future__ import annotations

import csv
import io
import os
import time
from collections.abc import Callable, Mapping
from datetime import date

import pandas as pd
import requests

from pipeline import rebuild
from pipeline.errors import UnexpectedFailure

_BASE = "https://api.kite.trade"
_TIMEOUT = 30

_ENV_API_KEY = "KITE_API_KEY"
_ENV_ACCESS_TOKEN = "KITE_ACCESS_TOKEN"

# Raw columns this module depends on from the Kite NSE instruments CSV.
_REQUIRED_INSTRUMENT_COLUMNS = {"instrument_token", "tradingsymbol", "segment"}


def _auth_headers(api_key: str, access_token: str) -> dict[str, str]:
    return {
        "X-Kite-Version": "3",
        "Authorization": f"token {api_key}:{access_token}",
    }


class KiteDayRebuilder:
    """Fetches one day's OHLCV for a given symbol universe directly from Kite
    Connect's historical-candle API -- one HTTP call per symbol, rate-limited.

    Never used in the automatic fallback chain (see module docstring) --
    construct and call this only from the `rebuild-day` CLI command (directly,
    or indirectly via the `rebuild` registry / `from_env()`).

    `api_key`/`access_token` may be empty strings -- the instance is still
    constructible (needed so `from_env()` can build one at import time with
    no credentials yet set), but any HTTP call will simply fail
    authentication. `available()` is the gate that decides whether this
    instance actually has usable credentials right now; it re-reads the
    environment live on every call rather than trusting whatever was
    (possibly empty) at construction time, so a credential set AFTER import
    (e.g. exported into the environment right before running `rebuild-day`,
    or set via `monkeypatch.setenv` in a test) is always honoured.
    """

    id = "kite"

    def __init__(
        self,
        api_key: str,
        access_token: str,
        session: requests.Session | None = None,
        *,
        sleep: Callable[[float], None] = time.sleep,
        rate_delay_s: float = 0.35,
    ) -> None:
        self._api_key = api_key
        self._access_token = access_token
        self._session = session or requests.Session()
        self._sleep = sleep
        self._rate_delay_s = rate_delay_s
        # Per-symbol HTTP/lookup failures collected across the last day_frame()
        # call -- not fatal, reported by the CLI as a failure count.
        self.failures: list[str] = []

    @classmethod
    def from_env(cls) -> KiteDayRebuilder:
        """Construct lazily from the environment -- safe to call at import
        time even when KITE_API_KEY/KITE_ACCESS_TOKEN aren't set yet (reads
        whatever is present now, defaulting to "" rather than raising).
        `available()`/`instruments()`/`day_frame()` all re-check the live
        environment rather than trusting these possibly-stale/empty values,
        so credentials exported (or monkeypatched, in tests) after this
        instance was constructed are still picked up correctly."""
        return cls(
            os.environ.get(_ENV_API_KEY, ""),
            os.environ.get(_ENV_ACCESS_TOKEN, ""),
        )

    def available(self) -> bool:
        """True iff both KITE_API_KEY and KITE_ACCESS_TOKEN are currently set
        in the environment. Always reads the LIVE environment (not whatever
        was passed to __init__/from_env at construction time) -- the
        registered module-level singleton is built once at import, so this
        must reflect credentials that may be exported later in the same
        process (or monkeypatched in tests) for `resolve()` to behave
        correctly no matter when the check happens."""
        return bool(os.environ.get(_ENV_API_KEY)) and bool(os.environ.get(_ENV_ACCESS_TOKEN))

    def _live_credentials(self) -> tuple[str, str]:
        """Resolve the credentials to actually use for an HTTP call: prefer
        whatever was explicitly passed to __init__ (the direct-construction
        path used by unit tests with a faked session), falling back to a
        live environment read (the from_env()/registry path) -- so a
        from_env()-built instance always uses fresh env credentials rather
        than whatever (possibly empty) snapshot existed at import time."""
        api_key = self._api_key or os.environ.get(_ENV_API_KEY, "")
        access_token = self._access_token or os.environ.get(_ENV_ACCESS_TOKEN, "")
        return api_key, access_token

    def instruments(self) -> dict[str, str]:
        """GET the Kite NSE instruments master (CSV) and map
        tradingsymbol -> instrument_token for NSE-segment (cash equity) rows.
        """
        api_key, access_token = self._live_credentials()
        resp = self._session.get(
            f"{_BASE}/instruments/NSE",
            headers=_auth_headers(api_key, access_token),
            timeout=_TIMEOUT,
        )
        reader = csv.DictReader(io.StringIO(resp.text))
        fieldnames = set(reader.fieldnames or [])
        missing = _REQUIRED_INSTRUMENT_COLUMNS - fieldnames
        if missing:
            raise UnexpectedFailure(
                f"Kite instruments CSV missing required columns: {sorted(missing)}"
            )
        mapping: dict[str, str] = {}
        for row in reader:
            if row["segment"] != "NSE":
                continue
            mapping[row["tradingsymbol"]] = row["instrument_token"]
        return mapping

    def day_frame(self, d: date, universe: Mapping[str, tuple[str, str]]) -> pd.DataFrame:
        """Rebuild one day's OHLCV for `universe` (symbol -> (isin, series)).

        For each symbol with a known Kite instrument token, fetches the
        single day-candle and emits one UDiFF-raw-shaped row. Per-symbol
        failures (missing token, HTTP error, empty candle payload) are
        collected into `self.failures` and skipped -- never fatal, so a
        handful of bad symbols doesn't sink the whole recovery run. Reset at
        the start of every call (one failures list per rebuild, not
        cumulative across calls).
        """
        self.failures = []
        token_map = self.instruments()

        symbols = list(universe)
        rows: list[dict[str, object]] = []
        for i, symbol in enumerate(symbols):
            if i > 0:
                self._sleep(self._rate_delay_s)

            isin, series = universe[symbol]
            token = token_map.get(symbol)
            if token is None:
                self.failures.append(f"{symbol}: no Kite instrument token found")
                continue

            try:
                candle = self._fetch_candle(token, d)
            except Exception as e:  # noqa: BLE001 - any per-symbol HTTP failure is non-fatal
                self.failures.append(f"{symbol}: {e}")
                continue

            if candle is None:
                self.failures.append(f"{symbol}: empty candle data for {d.isoformat()}")
                continue

            _ts, o, h, low, c, v = candle
            rows.append({
                "TradDt": d.isoformat(),
                "ISIN": isin,
                "TckrSymb": symbol,
                "SctySrs": series,
                "OpnPric": float(o),
                "HghPric": float(h),
                "LwPric": float(low),
                "ClsPric": float(c),
                # Degradation (documented in the module docstring): no true
                # previous-close is available from a single day-candle call --
                # the day's own open stands in for it.
                "PrvsClsgPric": float(o),
                "TtlTradgVol": int(v),
                # Degradation (documented in the module docstring): turnover
                # is not present in the day-candle payload.
                "TtlTrfVal": 0.0,
                "TtlNbOfTxsExctd": 0,
                "SsnId": "F1",
                "FinInstrmTp": "STK",
            })

        columns = [
            "TradDt", "ISIN", "TckrSymb", "SctySrs", "OpnPric", "HghPric", "LwPric",
            "ClsPric", "PrvsClsgPric", "TtlTradgVol", "TtlTrfVal", "TtlNbOfTxsExctd",
            "SsnId", "FinInstrmTp",
        ]
        return pd.DataFrame(rows, columns=columns)

    def _fetch_candle(
        self, token: str, d: date
    ) -> tuple[str, float, float, float, float, int] | None:
        """GET the single day-candle for `token` on `d`. Returns None when the
        API responds with no candle rows (day not available for this
        instrument -- e.g. it didn't trade, or is newly listed)."""
        api_key, access_token = self._live_credentials()
        stamp = d.isoformat()
        url = f"{_BASE}/instruments/historical/{token}/day?from={stamp}&to={stamp}"
        resp = self._session.get(
            url,
            headers=_auth_headers(api_key, access_token),
            timeout=_TIMEOUT,
        )
        body = resp.json()
        candles = body["data"]["candles"]
        if not candles:
            return None
        ts, o, h, low, c, v = candles[0]
        return (ts, o, h, low, c, v)


# Broker-agnostic registration (see rebuild.py + this module's docstring):
# self-register at import time. from_env() never requires credentials to be
# present yet -- available() is the gate `resolve()` uses before any use.
rebuild.register(KiteDayRebuilder.from_env())
