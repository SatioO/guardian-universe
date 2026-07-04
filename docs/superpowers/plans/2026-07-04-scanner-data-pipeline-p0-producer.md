# Scanner Data Pipeline — P0 Producer Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the local core of the EOD data producer — fetch one trading day's NSE equity bhavcopy, normalize/validate it, and append it to a year-partitioned Parquet store — fully TDD-tested with a green CI gate.

**Architecture:** A self-contained Python package under `pipeline/`, structured hexagonally: pure functions (`calendar`, `normalize`, `validate`, `schema`) with all I/O (`fetch_bhavcopy`, `store`) behind injectable seams so every scenario is tested deterministically with canned fixtures and mocked HTTP (no live network). `daily_update.run_daily()` wires them into a fail-closed, idempotent, calendar-aware one-day pipeline. Scheduling, CDN distribution, observability, and the Rust client come in later plans (P1–P3).

**Tech Stack:** Python 3.11+, pandas, pyarrow (Parquet), pandera (data contract), requests (+ `responses` for mocking), freezegun (time), pytest + pytest-cov, ruff, mypy.

## Global Constraints

- **Python 3.11+** (uses `datetime.date`, `zoneinfo`; no external tz dep).
- **Universe (P0):** NSE **equities only** — `FinInstrmTp == 'STK'` AND `SctySrs == 'EQ'`. Indices + BSE are later plans.
- **Session filter:** keep **F-session only** (`SsnId in {'F1','F2'}`); drop I1/I2 pre-open/interim rows. Silent-corruption trap — never skip.
- **Identity:** canonical PK is `(date, instrument_key)`; for equities `instrument_key == ISIN`. `symbol` is a mutable display column; never dedupe on it.
- **Canonical columns (exact order):** `date, instrument_key, isin, symbol, series, open, high, low, close, prevclose, volume, value, trades, source`.
- **Storage:** Parquet partitioned by calendar year (`data/ohlc/ohlc_{YYYY}.parquet`), zstd; dedupe `subset=['date','instrument_key'], keep='last'`.
- **Schema version:** `SCHEMA_VERSION = 1` (constant in `config.py`).
- **Validation thresholds:** row count absolute range `1800..2200`; deviation `> 0.15` vs trailing-10-day mean ⇒ FAIL.
- **Fail-closed:** never write/emit a partial day as complete. Typed errors distinguish `NotYetPublished` (expected) from `UnexpectedFailure` (loud).
- **No live network in tests:** all HTTP via `responses`; all dates via `freezegun`/injection.
- **Quality gate:** `ruff check`, `mypy --strict`, `pytest` must all pass before every commit.

---

### Task 0: Project scaffold + green baseline

**Files:**
- Create: `pipeline/pyproject.toml`
- Create: `pipeline/src/pipeline/__init__.py`
- Create: `pipeline/src/pipeline/config.py`
- Create: `pipeline/tests/__init__.py`
- Create: `pipeline/tests/test_smoke.py`
- Create: `pipeline/ruff.toml`
- Create: `pipeline/README.md`

**Interfaces:**
- Consumes: nothing.
- Produces: importable package `pipeline`; `pipeline.config.SCHEMA_VERSION: int`, `pipeline.config.CANON_COLUMNS: list[str]`, `pipeline.config.ROWCOUNT_ABS_RANGE: tuple[int,int]`, `pipeline.config.ROWCOUNT_DEVIATION: float`, `pipeline.config.DATA_DIR: Path` (helper `ohlc_path(year:int)->Path`).

- [ ] **Step 1: Create `pipeline/pyproject.toml`**

```toml
[project]
name = "traderview-pipeline"
version = "0.1.0"
description = "EOD data producer for the traderview scanner"
requires-python = ">=3.11"
dependencies = [
    "pandas>=2.2,<2.3",
    "pyarrow>=16",
    "pandera>=0.20",
    "requests>=2.32",
]

[project.optional-dependencies]
dev = [
    "pytest>=8",
    "pytest-cov>=5",
    "responses>=0.25",
    "freezegun>=1.5",
    "mypy>=1.10",
    "ruff>=0.6",
    "pandas-stubs",
    "types-requests",
]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
addopts = "-ra -q"
testpaths = ["tests"]

[tool.mypy]
python_version = "3.11"
strict = true
mypy_path = "src"
packages = ["pipeline"]
```

- [ ] **Step 2: Create `pipeline/ruff.toml`**

```toml
line-length = 100
target-version = "py311"

[lint]
select = ["E", "F", "I", "UP", "B", "SIM"]
```

- [ ] **Step 3: Create `pipeline/src/pipeline/__init__.py`**

```python
"""traderview EOD data producer."""
```

- [ ] **Step 4: Create `pipeline/src/pipeline/config.py`**

```python
"""Project-wide constants and path helpers."""
from __future__ import annotations

from pathlib import Path

SCHEMA_VERSION = 1

# Canonical long-format columns, in exact order.
CANON_COLUMNS: list[str] = [
    "date",
    "instrument_key",
    "isin",
    "symbol",
    "series",
    "open",
    "high",
    "low",
    "close",
    "prevclose",
    "volume",
    "value",
    "trades",
    "source",
]

# Validation thresholds.
ROWCOUNT_ABS_RANGE: tuple[int, int] = (1800, 2200)
ROWCOUNT_DEVIATION: float = 0.15

# Project root = the pipeline/ directory (two parents up from this file's src/pipeline/).
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
DATA_DIR: Path = PROJECT_ROOT / "data"
OHLC_DIR: Path = DATA_DIR / "ohlc"
META_DIR: Path = DATA_DIR / "meta"


def ohlc_path(year: int, base: Path | None = None) -> Path:
    """Path to the year-partitioned OHLC parquet file."""
    root = (base if base is not None else OHLC_DIR)
    return root / f"ohlc_{year}.parquet"
```

- [ ] **Step 5: Create `pipeline/tests/__init__.py` (empty) and `pipeline/tests/test_smoke.py`**

```python
from pipeline import config


def test_canon_columns_shape_and_order():
    assert config.CANON_COLUMNS[0] == "date"
    assert config.CANON_COLUMNS[1] == "instrument_key"
    assert config.CANON_COLUMNS[-1] == "source"
    assert len(config.CANON_COLUMNS) == 14


def test_schema_version_is_one():
    assert config.SCHEMA_VERSION == 1
```

- [ ] **Step 6: Install and run the baseline**

Run:
```bash
cd pipeline && pip install -e ".[dev]"
pytest -v && ruff check . && mypy
```
Expected: 2 tests PASS, ruff clean, mypy `Success`.

- [ ] **Step 7: Create `pipeline/README.md`**

```markdown
# traderview pipeline

EOD data producer for the scanner. See
`docs/superpowers/specs/2026-07-04-scanner-data-pipeline-design.md`.

Dev: `pip install -e ".[dev]"` then `pytest`, `ruff check .`, `mypy`.
```

- [ ] **Step 8: Commit**

```bash
git add pipeline
git commit -m "chore(pipeline): scaffold producer package with green baseline"
```

---

### Task 1: Trading calendar + holidays

**Files:**
- Create: `pipeline/data/meta/holidays.json`
- Create: `pipeline/src/pipeline/calendar.py`
- Test: `pipeline/tests/test_calendar.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `load_holidays(path: Path) -> set[date]`
  - `is_trading_day(d: date, holidays: set[date]) -> bool`
  - `previous_trading_day(d: date, holidays: set[date]) -> date`
  - `trading_days_back(end: date, n: int, holidays: set[date]) -> list[date]` (returns `n` trading days ending at/including `end` if `end` is a trading day, ascending order)

- [ ] **Step 1: Create `pipeline/data/meta/holidays.json`** (2026 NSE trading holidays — checked in, updated yearly)

```json
{
  "2026": [
    "2026-01-26",
    "2026-03-04",
    "2026-03-21",
    "2026-04-01",
    "2026-04-03",
    "2026-04-14",
    "2026-05-01",
    "2026-08-15",
    "2026-10-02",
    "2026-11-09",
    "2026-12-25"
  ]
}
```

> NOTE: This is a starter list; verify against NSE's published 2026 calendar before go-live and re-check yearly (tracked in RUNBOOK, added in P2).

- [ ] **Step 2: Write the failing test — `pipeline/tests/test_calendar.py`**

```python
import json
from datetime import date
from pathlib import Path

import pytest

from pipeline import calendar as cal


@pytest.fixture
def holidays(tmp_path: Path) -> set[date]:
    p = tmp_path / "holidays.json"
    p.write_text(json.dumps({"2026": ["2026-01-26", "2026-08-15"]}))
    return cal.load_holidays(p)


def test_weekend_is_not_a_trading_day(holidays: set[date]):
    assert cal.is_trading_day(date(2026, 7, 4), holidays) is False  # Saturday
    assert cal.is_trading_day(date(2026, 7, 5), holidays) is False  # Sunday


def test_holiday_is_not_a_trading_day(holidays: set[date]):
    assert cal.is_trading_day(date(2026, 1, 26), holidays) is False


def test_normal_weekday_is_a_trading_day(holidays: set[date]):
    assert cal.is_trading_day(date(2026, 7, 3), holidays) is True  # Friday


def test_previous_trading_day_skips_weekend(holidays: set[date]):
    # Monday 2026-07-06 -> previous trading day is Friday 2026-07-03
    assert cal.previous_trading_day(date(2026, 7, 6), holidays) == date(2026, 7, 3)


def test_trading_days_back_counts_trading_days_only(holidays: set[date]):
    days = cal.trading_days_back(date(2026, 7, 3), 3, holidays)
    assert days == [date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)]
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_calendar.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.calendar'`.

- [ ] **Step 4: Write minimal implementation — `pipeline/src/pipeline/calendar.py`**

```python
"""Trading-calendar logic. Pure; holidays are injected as a set of dates."""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path


def load_holidays(path: Path) -> set[date]:
    raw: dict[str, list[str]] = json.loads(path.read_text())
    out: set[date] = set()
    for _year, days in raw.items():
        for d in days:
            out.add(date.fromisoformat(d))
    return out


def is_trading_day(d: date, holidays: set[date]) -> bool:
    if d.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    return d not in holidays


def previous_trading_day(d: date, holidays: set[date]) -> date:
    cur = d - timedelta(days=1)
    while not is_trading_day(cur, holidays):
        cur -= timedelta(days=1)
    return cur


def trading_days_back(end: date, n: int, holidays: set[date]) -> list[date]:
    """The `n` trading days ending at `end`, ascending. `end` need not be a
    trading day; if it isn't, counting starts from the previous trading day."""
    days: list[date] = []
    cur = end if is_trading_day(end, holidays) else previous_trading_day(end, holidays)
    while len(days) < n:
        days.append(cur)
        cur = previous_trading_day(cur, holidays)
    return sorted(days)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_calendar.py -v && mypy && ruff check .`
Expected: 5 PASS, mypy Success, ruff clean.

- [ ] **Step 6: Commit**

```bash
git add pipeline/src/pipeline/calendar.py pipeline/tests/test_calendar.py pipeline/data/meta/holidays.json
git commit -m "feat(pipeline): trading calendar with injected holidays"
```

---

### Task 2: Typed errors + UDiFF URL builder

**Files:**
- Create: `pipeline/src/pipeline/errors.py`
- Create: `pipeline/src/pipeline/sources/__init__.py`
- Create: `pipeline/src/pipeline/sources/nse_udiff.py`
- Test: `pipeline/tests/test_nse_udiff_url.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `errors.PipelineError`, `errors.NotYetPublished`, `errors.UnexpectedFailure`
  - `sources.nse_udiff.build_udiff_url(d: date) -> str`
  - `sources.nse_udiff.UDIFF_COLUMNS: list[str]` (raw column names we depend on)

- [ ] **Step 1: Create `pipeline/src/pipeline/errors.py`**

```python
"""Typed pipeline errors so callers can branch on expected vs unexpected."""
from __future__ import annotations


class PipelineError(Exception):
    """Base for all pipeline errors."""


class NotYetPublished(PipelineError):
    """The bhavcopy for this date is not available yet (expected window)."""


class UnexpectedFailure(PipelineError):
    """An unexpected failure (format break, blocked, exhausted fallbacks)."""
```

- [ ] **Step 2: Write the failing test — `pipeline/tests/test_nse_udiff_url.py`**

```python
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_nse_udiff_url.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.sources'`.

- [ ] **Step 4: Write minimal implementation**

Create `pipeline/src/pipeline/sources/__init__.py`:
```python
"""Data-source adapters. Each isolates one upstream's URL/format quirks."""
```

Create `pipeline/src/pipeline/sources/nse_udiff.py`:
```python
"""NSE UDiFF CM bhavcopy adapter — the ONLY place the URL/filename pattern lives.

NSE changed this pattern in the 2024 UDiFF migration and again in Oct 2025
(four-digit year). If it changes again, this is the one file to edit."""
from __future__ import annotations

from datetime import date

_BASE = "https://nsearchives.nseindia.com/content/cm"

# Raw UDiFF columns we depend on downstream (subset of the full file).
UDIFF_COLUMNS: list[str] = [
    "TradDt",       # trade date
    "FinInstrmTp",  # 'STK' for cash equity
    "ISIN",
    "TckrSymb",     # symbol
    "SctySrs",      # series (EQ/BE/...)
    "SsnId",        # session (F1/F2 final; I1/I2 pre-open/interim)
    "OpnPric",
    "HghPric",
    "LwPric",
    "ClsPric",
    "PrvsClsgPric",
    "TtlTradgVol",
    "TtlTrfVal",
    "TtlNbOfTxsExctd",
]


def build_udiff_url(d: date) -> str:
    stamp = d.strftime("%Y%m%d")
    return f"{_BASE}/BhavCopy_NSE_CM_0_0_0_{stamp}_F_0000.csv.zip"
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_nse_udiff_url.py -v && mypy && ruff check .`
Expected: 2 PASS, mypy Success, ruff clean.

- [ ] **Step 6: Commit**

```bash
git add pipeline/src/pipeline/errors.py pipeline/src/pipeline/sources pipeline/tests/test_nse_udiff_url.py
git commit -m "feat(pipeline): typed errors + isolated NSE UDiFF URL builder"
```

---

### Task 3: Fetch adapter (session warming, retry, fallback)

**Files:**
- Modify: `pipeline/src/pipeline/sources/nse_udiff.py`
- Create: `pipeline/src/pipeline/fetch.py`
- Test: `pipeline/tests/test_fetch.py`

**Interfaces:**
- Consumes: `build_udiff_url`, `UDIFF_COLUMNS`, `errors.*`.
- Produces:
  - `fetch.Fetcher` (Protocol): `fetch_raw(d: date) -> pandas.DataFrame`
  - `fetch.NseUdiffFetcher(session=None, fallbacks=())` implementing `Fetcher`; warms an NSE session, GETs the zip with retry (3×, exponential backoff), unzips → DataFrame; on failure walks `fallbacks: Sequence[Callable[[date], pandas.DataFrame]]`; raises `NotYetPublished` on 404, `UnexpectedFailure` when all sources exhausted.

- [ ] **Step 1: Write the failing test — `pipeline/tests/test_fetch.py`**

```python
import io
import zipfile
from datetime import date

import pandas as pd
import pytest
import responses

from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.fetch import NseUdiffFetcher
from pipeline.sources import nse_udiff


def _zip_bytes(csv_text: str, name: str = "BhavCopy.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, csv_text)
    return buf.getvalue()


CSV = "TradDt,TckrSymb,ClsPric\n2026-07-03,RELIANCE,3000\n"


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
    df = NseUdiffFetcher().fetch_raw(date(2026, 7, 3))
    assert list(df.columns) == ["TradDt", "TckrSymb", "ClsPric"]
    assert df.iloc[0]["TckrSymb"] == "RELIANCE"


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

    df = NseUdiffFetcher(fallbacks=(fallback,)).fetch_raw(date(2026, 7, 3))
    assert df.iloc[0]["TckrSymb"] == "INFY"


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
        NseUdiffFetcher(fallbacks=(bad_fallback,)).fetch_raw(date(2026, 7, 3))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_fetch.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.fetch'`.

- [ ] **Step 3: Write minimal implementation — `pipeline/src/pipeline/fetch.py`**

```python
"""Fetch adapter: the injectable I/O seam for acquiring a day's raw bhavcopy.

Source order: NSE UDiFF (primary) -> injected fallbacks (e.g. jugaad-data).
HTTP + retry + session-warming live here so business logic stays pure."""
from __future__ import annotations

import io
import time
import zipfile
from collections.abc import Callable, Sequence
from datetime import date
from typing import Protocol

import pandas as pd
import requests

from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.sources import nse_udiff

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_MAX_RETRIES = 3
_TIMEOUT = 30

Fallback = Callable[[date], pd.DataFrame]


class Fetcher(Protocol):
    def fetch_raw(self, d: date) -> pd.DataFrame: ...


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
        # Warm the session (NSE rejects cold archive requests).
        self._session.get("https://www.nseindia.com/", timeout=_TIMEOUT)
        url = nse_udiff.build_udiff_url(d)
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            resp = self._session.get(url, timeout=_TIMEOUT)
            if resp.status_code == 404:
                raise NotYetPublished(f"bhavcopy 404 for {d.isoformat()}")
            if resp.status_code == 200:
                return _unzip_to_df(resp.content)
            last_exc = RuntimeError(f"HTTP {resp.status_code}")
            time.sleep(2**attempt)  # 1s, 2s, 4s
        raise UnexpectedFailure(f"primary exhausted for {d.isoformat()}: {last_exc}")

    def _fetch_fallbacks(self, d: date) -> pd.DataFrame:
        for fb in self._fallbacks:
            try:
                return fb(d)
            except Exception:  # noqa: BLE001 - try the next source
                continue
        raise UnexpectedFailure(f"all sources exhausted for {d.isoformat()}")


def _unzip_to_df(zip_bytes: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as fh:
            return pd.read_csv(fh)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_fetch.py -v && mypy && ruff check .`
Expected: 4 PASS, mypy Success, ruff clean.

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/fetch.py pipeline/tests/test_fetch.py
git commit -m "feat(pipeline): NSE fetch adapter with warm session, retry, fallbacks"
```

---

### Task 4: Canonical schema (pandera data contract)

**Files:**
- Create: `pipeline/src/pipeline/schema.py`
- Test: `pipeline/tests/test_schema.py`

**Interfaces:**
- Consumes: `config.CANON_COLUMNS`.
- Produces:
  - `schema.OHLC_SCHEMA` (pandera `DataFrameSchema`)
  - `schema.validate_ohlc(df: pandas.DataFrame) -> pandas.DataFrame` (returns validated df; raises `pandera.errors.SchemaError` on violation). Used in BOTH tests and the runtime gate.

- [ ] **Step 1: Write the failing test — `pipeline/tests/test_schema.py`**

```python
from datetime import date

import pandas as pd
import pandera as pa
import pytest

from pipeline import config
from pipeline.schema import validate_ohlc


def _good_row() -> dict:
    return {
        "date": date(2026, 7, 3),
        "instrument_key": "INE002A01018",
        "isin": "INE002A01018",
        "symbol": "RELIANCE",
        "series": "EQ",
        "open": 2990.0,
        "high": 3010.0,
        "low": 2985.0,
        "close": 3000.0,
        "prevclose": 2980.0,
        "volume": 1_000_000,
        "value": 3.0e9,
        "trades": 50_000,
        "source": "nse-udiff",
    }


def test_valid_frame_passes():
    df = pd.DataFrame([_good_row()])[config.CANON_COLUMNS]
    out = validate_ohlc(df)
    assert len(out) == 1


def test_negative_volume_is_rejected():
    row = _good_row()
    row["volume"] = -5
    df = pd.DataFrame([row])[config.CANON_COLUMNS]
    with pytest.raises(pa.errors.SchemaError):
        validate_ohlc(df)


def test_missing_instrument_key_is_rejected():
    row = _good_row()
    row["instrument_key"] = None
    df = pd.DataFrame([row])[config.CANON_COLUMNS]
    with pytest.raises(pa.errors.SchemaError):
        validate_ohlc(df)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_schema.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.schema'`.

- [ ] **Step 3: Write minimal implementation — `pipeline/src/pipeline/schema.py`**

```python
"""pandera data contract for the canonical OHLC frame.

The SAME schema object is used in unit tests and the runtime validation gate,
so tests and production enforce identical guarantees."""
from __future__ import annotations

import pandas as pd
import pandera as pa
from pandera import Check, Column

_non_neg = Check.ge(0)

OHLC_SCHEMA = pa.DataFrameSchema(
    {
        "date": Column("datetime64[ns]", coerce=True, nullable=False),
        "instrument_key": Column(str, nullable=False),
        "isin": Column(str, nullable=True),
        "symbol": Column(str, nullable=False),
        "series": Column(str, nullable=False),
        "open": Column(float, _non_neg, nullable=False, coerce=True),
        "high": Column(float, _non_neg, nullable=False, coerce=True),
        "low": Column(float, _non_neg, nullable=False, coerce=True),
        "close": Column(float, _non_neg, nullable=False, coerce=True),
        "prevclose": Column(float, _non_neg, nullable=False, coerce=True),
        "volume": Column("int64", _non_neg, nullable=False, coerce=True),
        "value": Column(float, _non_neg, nullable=False, coerce=True),
        "trades": Column("int64", _non_neg, nullable=False, coerce=True),
        "source": Column(str, nullable=False),
    },
    strict=True,
    ordered=False,
)


def validate_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    return OHLC_SCHEMA.validate(df, lazy=False)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_schema.py -v && mypy && ruff check .`
Expected: 3 PASS, mypy Success, ruff clean.

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/schema.py pipeline/tests/test_schema.py
git commit -m "feat(pipeline): pandera OHLC data contract (shared test+runtime)"
```

---

### Task 5: Normalize raw bhavcopy → canonical frame

**Files:**
- Create: `pipeline/src/pipeline/normalize.py`
- Create: `pipeline/tests/fixtures/__init__.py`
- Create: `pipeline/tests/fixtures/bhavcopy_normal.csv`
- Create: `pipeline/tests/fixtures/bhavcopy_mixed_session.csv`
- Test: `pipeline/tests/test_normalize.py`

**Interfaces:**
- Consumes: `config.CANON_COLUMNS`, `sources.nse_udiff.UDIFF_COLUMNS`.
- Produces: `normalize.normalize_equity_bhavcopy(raw: pandas.DataFrame, source: str = "nse-udiff") -> pandas.DataFrame` — filters `FinInstrmTp=='STK'`, `SctySrs=='EQ'`, `SsnId in {'F1','F2'}`; maps UDiFF columns to `CANON_COLUMNS`; sets `instrument_key = ISIN`; parses `date`.

- [ ] **Step 1: Create fixtures**

`pipeline/tests/fixtures/__init__.py` (empty).

`pipeline/tests/fixtures/bhavcopy_normal.csv`:
```csv
TradDt,FinInstrmTp,ISIN,TckrSymb,SctySrs,SsnId,OpnPric,HghPric,LwPric,ClsPric,PrvsClsgPric,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd
2026-07-03,STK,INE002A01018,RELIANCE,EQ,F1,2990,3010,2985,3000,2980,1000000,3000000000,50000
2026-07-03,STK,INE009A01021,INFY,EQ,F1,1490,1510,1485,1500,1480,800000,1200000000,40000
2026-07-03,STK,INE040A01034,HDFCBANK,BE,F1,1600,1620,1590,1610,1595,500000,805000000,30000
```

`pipeline/tests/fixtures/bhavcopy_mixed_session.csv`:
```csv
TradDt,FinInstrmTp,ISIN,TckrSymb,SctySrs,SsnId,OpnPric,HghPric,LwPric,ClsPric,PrvsClsgPric,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd
2026-07-03,STK,INE002A01018,RELIANCE,EQ,I1,2990,3010,2985,2999,2980,10,29990,5
2026-07-03,STK,INE002A01018,RELIANCE,EQ,F1,2990,3010,2985,3000,2980,1000000,3000000000,50000
```

- [ ] **Step 2: Write the failing test — `pipeline/tests/test_normalize.py`**

```python
from pathlib import Path

import pandas as pd

from pipeline import config
from pipeline.normalize import normalize_equity_bhavcopy

FIX = Path(__file__).parent / "fixtures"


def test_filters_to_eq_stk_and_maps_columns():
    raw = pd.read_csv(FIX / "bhavcopy_normal.csv")
    out = normalize_equity_bhavcopy(raw)
    assert list(out.columns) == config.CANON_COLUMNS
    # BE row (HDFCBANK) dropped; only RELIANCE + INFY remain
    assert set(out["symbol"]) == {"RELIANCE", "INFY"}
    r = out[out["symbol"] == "RELIANCE"].iloc[0]
    assert r["instrument_key"] == "INE002A01018"
    assert r["close"] == 3000.0
    assert r["series"] == "EQ"
    assert r["source"] == "nse-udiff"


def test_keeps_only_final_session_rows():
    raw = pd.read_csv(FIX / "bhavcopy_mixed_session.csv")
    out = normalize_equity_bhavcopy(raw)
    # Two RELIANCE rows in (I1 pre-open + F1 final); only the F1 final survives.
    assert len(out) == 1
    assert out.iloc[0]["close"] == 3000.0
    assert out.iloc[0]["volume"] == 1_000_000
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_normalize.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.normalize'`.

- [ ] **Step 4: Write minimal implementation — `pipeline/src/pipeline/normalize.py`**

```python
"""Raw UDiFF bhavcopy -> canonical long OHLC frame. Pure."""
from __future__ import annotations

import pandas as pd

from pipeline import config

_FINAL_SESSIONS = {"F1", "F2"}

# UDiFF raw column -> canonical column.
_COLMAP = {
    "TradDt": "date",
    "ISIN": "isin",
    "TckrSymb": "symbol",
    "SctySrs": "series",
    "OpnPric": "open",
    "HghPric": "high",
    "LwPric": "low",
    "ClsPric": "close",
    "PrvsClsgPric": "prevclose",
    "TtlTradgVol": "volume",
    "TtlTrfVal": "value",
    "TtlNbOfTxsExctd": "trades",
}


def normalize_equity_bhavcopy(raw: pd.DataFrame, source: str = "nse-udiff") -> pd.DataFrame:
    df = raw[
        (raw["FinInstrmTp"] == "STK")
        & (raw["SctySrs"] == "EQ")
        & (raw["SsnId"].isin(_FINAL_SESSIONS))
    ].copy()

    df = df.rename(columns=_COLMAP)
    df["date"] = pd.to_datetime(df["date"])
    df["instrument_key"] = df["isin"]
    df["source"] = source

    df["volume"] = df["volume"].astype("int64")
    df["trades"] = df["trades"].astype("int64")
    for col in ("open", "high", "low", "close", "prevclose", "value"):
        df[col] = df[col].astype(float)

    return df[config.CANON_COLUMNS].reset_index(drop=True)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_normalize.py -v && mypy && ruff check .`
Expected: 2 PASS, mypy Success, ruff clean.

- [ ] **Step 6: Commit**

```bash
git add pipeline/src/pipeline/normalize.py pipeline/tests/test_normalize.py pipeline/tests/fixtures
git commit -m "feat(pipeline): normalize UDiFF bhavcopy to canonical frame (EQ+F-session)"
```

---

### Task 6: Validation — row-count gate + per-row quarantine

**Files:**
- Create: `pipeline/src/pipeline/validate.py`
- Test: `pipeline/tests/test_validate.py`

**Interfaces:**
- Consumes: `config.ROWCOUNT_ABS_RANGE`, `config.ROWCOUNT_DEVIATION`, `errors.UnexpectedFailure`.
- Produces:
  - `validate.check_rowcount(count: int, trailing: list[int]) -> None` — raises `UnexpectedFailure` if `count` outside abs range OR deviates > threshold from mean(trailing). Empty `trailing` ⇒ abs-range check only.
  - `validate.quarantine_bad_rows(df: pandas.DataFrame) -> tuple[pandas.DataFrame, pandas.DataFrame]` — returns `(clean, quarantined)`; a row is bad if any of open/high/low/close/prevclose ≤ 0, volume < 0, `high < low`, `close` outside `[low, high]`, or `instrument_key` missing.

- [ ] **Step 1: Write the failing test — `pipeline/tests/test_validate.py`**

```python
import pandas as pd
import pytest

from pipeline import config
from pipeline.errors import UnexpectedFailure
from pipeline.validate import check_rowcount, quarantine_bad_rows


def test_rowcount_within_range_and_stable_passes():
    check_rowcount(2000, [1990, 2010, 2005])  # no raise


def test_rowcount_below_absolute_floor_fails():
    with pytest.raises(UnexpectedFailure):
        check_rowcount(1500, [1990, 2010])


def test_rowcount_deviation_over_threshold_fails():
    # mean(trailing)=2000; 2000*0.15=300; 1600 deviates by 400 -> fail
    with pytest.raises(UnexpectedFailure):
        check_rowcount(1600, [2000, 2000, 2000])


def test_rowcount_empty_trailing_uses_abs_range_only():
    check_rowcount(1900, [])  # no raise (within 1800..2200)


def _row(**over) -> dict:
    base = {
        "date": pd.Timestamp("2026-07-03"),
        "instrument_key": "INE002A01018", "isin": "INE002A01018",
        "symbol": "RELIANCE", "series": "EQ",
        "open": 2990.0, "high": 3010.0, "low": 2985.0, "close": 3000.0,
        "prevclose": 2980.0, "volume": 1000, "value": 1.0, "trades": 10,
        "source": "nse-udiff",
    }
    base.update(over)
    return base


def test_quarantine_separates_bad_rows():
    df = pd.DataFrame([
        _row(symbol="GOOD"),
        _row(symbol="NEGVOL", volume=-1),
        _row(symbol="HILO", high=10.0, low=20.0),
        _row(symbol="CLOSEOOB", close=9999.0),
    ])[config.CANON_COLUMNS]
    clean, bad = quarantine_bad_rows(df)
    assert set(clean["symbol"]) == {"GOOD"}
    assert set(bad["symbol"]) == {"NEGVOL", "HILO", "CLOSEOOB"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_validate.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.validate'`.

- [ ] **Step 3: Write minimal implementation — `pipeline/src/pipeline/validate.py`**

```python
"""Validation gates: row-count sanity + per-row quarantine. Pure."""
from __future__ import annotations

import pandas as pd

from pipeline import config
from pipeline.errors import UnexpectedFailure


def check_rowcount(count: int, trailing: list[int]) -> None:
    lo, hi = config.ROWCOUNT_ABS_RANGE
    if not (lo <= count <= hi):
        raise UnexpectedFailure(f"row count {count} outside absolute range {lo}..{hi}")
    if trailing:
        mean = sum(trailing) / len(trailing)
        if mean > 0 and abs(count - mean) / mean > config.ROWCOUNT_DEVIATION:
            raise UnexpectedFailure(
                f"row count {count} deviates >{config.ROWCOUNT_DEVIATION:.0%} "
                f"from trailing mean {mean:.0f}"
            )


def quarantine_bad_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    price_cols = ["open", "high", "low", "close", "prevclose"]
    positive = (df[price_cols] > 0).all(axis=1)
    vol_ok = df["volume"] >= 0
    hilo_ok = df["high"] >= df["low"]
    close_ok = (df["close"] >= df["low"]) & (df["close"] <= df["high"])
    key_ok = df["instrument_key"].notna() & (df["instrument_key"].astype(str) != "")

    good_mask = positive & vol_ok & hilo_ok & close_ok & key_ok
    clean = df[good_mask].reset_index(drop=True)
    bad = df[~good_mask].reset_index(drop=True)
    return clean, bad
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_validate.py -v && mypy && ruff check .`
Expected: 5 PASS, mypy Success, ruff clean.

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/validate.py pipeline/tests/test_validate.py
git commit -m "feat(pipeline): row-count gate + per-row quarantine"
```

---

### Task 7: Parquet store — append/dedupe by year, idempotency, trailing window

**Files:**
- Create: `pipeline/src/pipeline/store.py`
- Test: `pipeline/tests/test_store.py`

**Interfaces:**
- Consumes: `config.ohlc_path`, `config.CANON_COLUMNS`.
- Produces:
  - `store.append_day(df: pandas.DataFrame, base: Path) -> None` — writes/updates `base/ohlc_{year}.parquet` (zstd), dedupe `['date','instrument_key'] keep='last'`, sorted. Year derived from each row's `date`.
  - `store.has_day(base: Path, d: date) -> bool` — idempotency check (is `d` already present?).
  - `store.day_symbol_count(base: Path, d: date) -> int` — number of rows stored for date `d` (0 if absent). Used by the row-count deviation gate.
  - `store.read_trailing_window(base: Path, end: date, n_rows_per_key: int) -> pandas.DataFrame` — loads the last `n_rows_per_key` dates per instrument up to `end`, reading across year files.

- [ ] **Step 1: Write the failing test — `pipeline/tests/test_store.py`**

```python
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config
from pipeline.store import append_day, has_day, read_trailing_window


def _day(d: str, close: float, key: str = "INE002A01018") -> pd.DataFrame:
    return pd.DataFrame([{
        "date": pd.Timestamp(d), "instrument_key": key, "isin": key,
        "symbol": "RELIANCE", "series": "EQ",
        "open": close, "high": close, "low": close, "close": close,
        "prevclose": close, "volume": 1, "value": 1.0, "trades": 1,
        "source": "nse-udiff",
    }])[config.CANON_COLUMNS]


def test_append_creates_year_file_and_has_day(tmp_path: Path):
    assert has_day(tmp_path, date(2026, 7, 3)) is False
    append_day(_day("2026-07-03", 3000), tmp_path)
    assert config.ohlc_path(2026, tmp_path).exists()
    assert has_day(tmp_path, date(2026, 7, 3)) is True


def test_day_symbol_count(tmp_path: Path):
    from pipeline.store import day_symbol_count
    assert day_symbol_count(tmp_path, date(2026, 7, 3)) == 0
    append_day(_day("2026-07-03", 3000, "INE002A01018"), tmp_path)
    append_day(_day("2026-07-03", 1500, "INE009A01021"), tmp_path)
    assert day_symbol_count(tmp_path, date(2026, 7, 3)) == 2


def test_reappending_same_day_dedupes_keep_last(tmp_path: Path):
    append_day(_day("2026-07-03", 3000), tmp_path)
    append_day(_day("2026-07-03", 3050), tmp_path)  # corrected value, same (date,key)
    out = pd.read_parquet(config.ohlc_path(2026, tmp_path))
    assert len(out) == 1
    assert out.iloc[0]["close"] == 3050.0


def test_trailing_window_reads_across_year_boundary(tmp_path: Path):
    append_day(_day("2025-12-31", 100), tmp_path)
    append_day(_day("2026-01-01", 101), tmp_path)
    append_day(_day("2026-01-02", 102), tmp_path)
    out = read_trailing_window(tmp_path, date(2026, 1, 2), 2)
    assert sorted(out["close"]) == [101.0, 102.0]  # last 2 dates, spanning files
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_store.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.store'`.

- [ ] **Step 3: Write minimal implementation — `pipeline/src/pipeline/store.py`**

```python
"""Year-partitioned Parquet store with append+dedupe and trailing-window reads."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import config


def _read_year(base: Path, year: int) -> pd.DataFrame:
    p = config.ohlc_path(year, base)
    if p.exists():
        return pd.read_parquet(p)
    return pd.DataFrame(columns=config.CANON_COLUMNS)


def append_day(df: pd.DataFrame, base: Path) -> None:
    base.mkdir(parents=True, exist_ok=True)
    for year, chunk in df.groupby(df["date"].dt.year):
        existing = _read_year(base, int(year))
        combined = pd.concat([existing, chunk], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["date", "instrument_key"], keep="last"
        )
        combined = combined.sort_values(["date", "instrument_key"]).reset_index(drop=True)
        combined.to_parquet(
            config.ohlc_path(int(year), base), compression="zstd", index=False
        )


def has_day(base: Path, d: date) -> bool:
    df = _read_year(base, d.year)
    if df.empty:
        return False
    return bool((df["date"] == pd.Timestamp(d)).any())


def day_symbol_count(base: Path, d: date) -> int:
    df = _read_year(base, d.year)
    if df.empty:
        return 0
    return int((df["date"] == pd.Timestamp(d)).sum())


def read_trailing_window(base: Path, end: date, n_rows_per_key: int) -> pd.DataFrame:
    frames = [_read_year(base, y) for y in (end.year - 1, end.year)]
    df = pd.concat(frames, ignore_index=True)
    df = df[df["date"] <= pd.Timestamp(end)]
    if df.empty:
        return df
    df = df.sort_values(["instrument_key", "date"])
    return (
        df.groupby("instrument_key", group_keys=False)
        .tail(n_rows_per_key)
        .reset_index(drop=True)
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_store.py -v && mypy && ruff check .`
Expected: 3 PASS, mypy Success, ruff clean.

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/store.py pipeline/tests/test_store.py
git commit -m "feat(pipeline): year-partitioned parquet store (append/dedupe/window)"
```

---

### Task 8: Daily-update orchestration (wire it all, injectable, scenario tests)

**Files:**
- Create: `pipeline/src/pipeline/daily_update.py`
- Test: `pipeline/tests/test_daily_update.py`

**Interfaces:**
- Consumes: `calendar.*`, `fetch.Fetcher`, `normalize.normalize_equity_bhavcopy`, `validate.*`, `schema.validate_ohlc`, `store.*`, `errors.*`.
- Produces:
  - `daily_update.RunStatus` dataclass: `status: str` (`"success" | "skipped_holiday" | "skipped_idempotent" | "not_yet" | "failed"`), `date: date`, `symbol_count: int`, `quarantined_count: int`, `source: str`, `message: str`.
  - `daily_update.run_daily(target: date, *, fetcher: Fetcher, holidays: set[date], base: Path) -> RunStatus` — the full one-day pipeline (gate → idempotency → fetch → normalize → rowcount → quarantine → schema → append). Fail-closed; never partially writes.

- [ ] **Step 1: Write the failing test — `pipeline/tests/test_daily_update.py`**

```python
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipeline.daily_update import RunStatus, run_daily
from pipeline.errors import NotYetPublished
from pipeline.fetch import Fetcher

HOLIDAYS = {date(2026, 8, 15)}
RAW = pd.read_csv(Path(__file__).parent / "fixtures" / "bhavcopy_normal.csv")


class StubFetcher:
    def __init__(self, df: pd.DataFrame | None = None, exc: Exception | None = None):
        self._df, self._exc = df, exc

    def fetch_raw(self, d: date) -> pd.DataFrame:
        if self._exc is not None:
            raise self._exc
        assert self._df is not None
        return self._df


def _run(target: date, fetcher: Fetcher, base: Path) -> RunStatus:
    return run_daily(target, fetcher=fetcher, holidays=HOLIDAYS, base=base)


def test_normal_day_ingests_and_persists(tmp_path: Path):
    st = _run(date(2026, 7, 3), StubFetcher(RAW), tmp_path)
    assert st.status == "success"
    assert st.symbol_count == 2  # RELIANCE + INFY (BE row filtered out)
    out = pd.read_parquet(base_year(tmp_path))
    assert set(out["symbol"]) == {"RELIANCE", "INFY"}


def test_holiday_skips_cleanly_without_fetching(tmp_path: Path):
    st = _run(date(2026, 8, 15), StubFetcher(exc=AssertionError("must not fetch")), tmp_path)
    assert st.status == "skipped_holiday"


def test_idempotent_rerun_is_a_noop(tmp_path: Path):
    _run(date(2026, 7, 3), StubFetcher(RAW), tmp_path)
    st = _run(date(2026, 7, 3), StubFetcher(exc=AssertionError("must not refetch")), tmp_path)
    assert st.status == "skipped_idempotent"


def test_not_yet_published_is_reported_not_raised(tmp_path: Path):
    st = _run(date(2026, 7, 3), StubFetcher(exc=NotYetPublished("404")), tmp_path)
    assert st.status == "not_yet"


def base_year(base: Path) -> Path:
    from pipeline import config
    return config.ohlc_path(2026, base)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_daily_update.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipeline.daily_update'`.

- [ ] **Step 3: Write minimal implementation — `pipeline/src/pipeline/daily_update.py`**

```python
"""One-day pipeline orchestration: gate -> idempotency -> fetch -> normalize ->
validate -> quarantine -> schema -> append. Fail-closed and idempotent."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pipeline import calendar as cal
from pipeline import store, validate
from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.fetch import Fetcher
from pipeline.normalize import normalize_equity_bhavcopy
from pipeline.schema import validate_ohlc

_TRAILING_DAYS = 10


@dataclass(frozen=True)
class RunStatus:
    status: str
    date: date
    symbol_count: int = 0
    quarantined_count: int = 0
    source: str = ""
    message: str = ""


def run_daily(
    target: date,
    *,
    fetcher: Fetcher,
    holidays: set[date],
    base: Path,
) -> RunStatus:
    if not cal.is_trading_day(target, holidays):
        return RunStatus("skipped_holiday", target, message="non-trading day")

    if store.has_day(base, target):
        return RunStatus("skipped_idempotent", target, message="already present")

    try:
        raw = fetcher.fetch_raw(target)
    except NotYetPublished as e:
        return RunStatus("not_yet", target, message=str(e))
    except UnexpectedFailure as e:
        return RunStatus("failed", target, message=str(e))

    df = normalize_equity_bhavcopy(raw)

    trailing = _trailing_counts(base, target, holidays)
    try:
        validate.check_rowcount(len(df), trailing)
    except UnexpectedFailure as e:
        return RunStatus("failed", target, message=str(e))

    clean, bad = validate.quarantine_bad_rows(df)
    clean = validate_ohlc(clean)  # runtime contract gate (same schema as tests)

    store.append_day(clean, base)
    return RunStatus(
        status="success",
        date=target,
        symbol_count=len(clean),
        quarantined_count=len(bad),
        source="nse-udiff",
    )


def _trailing_counts(base: Path, target: date, holidays: set[date]) -> list[int]:
    counts: list[int] = []
    prev = cal.previous_trading_day(target, holidays)
    for d in cal.trading_days_back(prev, _TRAILING_DAYS, holidays):
        if store.has_day(base, d):
            counts.append(store.day_symbol_count(base, d))
    return counts
```

> NOTE: `_trailing_counts` feeds the deviation gate; it returns `[]` on a cold store (backfill/first run), so the abs-range check applies alone.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_daily_update.py -v && mypy && ruff check .`
Expected: 4 PASS, mypy Success, ruff clean.

- [ ] **Step 5: Run the FULL suite**

Run: `pytest -v --cov=pipeline && mypy && ruff check .`
Expected: all tests PASS across every module; mypy Success; ruff clean.

- [ ] **Step 6: Commit**

```bash
git add pipeline/src/pipeline/daily_update.py pipeline/tests/test_daily_update.py
git commit -m "feat(pipeline): daily-update orchestration (gate/idempotent/fail-closed)"
```

---

### Task 9: CI workflow — quality gate on every push/PR

**Files:**
- Create: `.github/workflows/pipeline-ci.yml` (repo root, NOT under `pipeline/`)

**Interfaces:**
- Consumes: `pipeline/pyproject.toml` (dev extras).
- Produces: a required status check running ruff + mypy + pytest on the `pipeline/` package.

- [ ] **Step 1: Create `.github/workflows/pipeline-ci.yml`**

```yaml
name: pipeline-ci

on:
  push:
    paths: ["pipeline/**", ".github/workflows/pipeline-ci.yml"]
  pull_request:
    paths: ["pipeline/**", ".github/workflows/pipeline-ci.yml"]

permissions:
  contents: read

concurrency:
  group: pipeline-ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  test:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    defaults:
      run:
        working-directory: pipeline
    steps:
      - uses: actions/checkout@v4  # pin to SHA before go-live (P2)
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
      - name: Install
        run: pip install -e ".[dev]"
      - name: Lint
        run: ruff check .
      - name: Types
        run: mypy
      - name: Tests
        run: pytest -v --cov=pipeline --cov-report=term-missing
```

- [ ] **Step 2: Verify locally (act-free) that the same commands pass**

Run:
```bash
cd pipeline && ruff check . && mypy && pytest -v --cov=pipeline
```
Expected: all green (this mirrors exactly what CI runs).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/pipeline-ci.yml
git commit -m "ci(pipeline): ruff + mypy + pytest gate on pipeline changes"
```

---

## What P0 delivers (Definition of Done)

- `pipeline/` package: given a raw NSE UDiFF bhavcopy for a trading day, it filters to EQ+F-session, normalizes to the canonical frame, gates row-count, quarantines bad rows, enforces the pandera contract, and appends to a year-partitioned Parquet store — **idempotently and fail-closed**.
- Every §10-spec scenario applicable to P0 is covered by a deterministic test (normal, holiday, malformed→rowcount, mixed-session filter, quarantine, year-boundary, idempotent rerun, not-yet-published, fallback chain).
- CI (`ruff` + `mypy --strict` + `pytest`) gates every change.

## Deferred to later plans (explicitly NOT in P0)

- **P1:** `backfill.py` (300 trading days, resumable), indices adapter (separate bhavcopy), two-cron GitHub Actions schedule, `manifest.json` + `last_run_status.json`, CDN publish (Releases + jsDelivr) with atomic pointer flip.
- **P2:** auto-issue-on-failure, freshness dead-man's-switch (`data-monitor.yml`), `$GITHUB_STEP_SUMMARY`, structured logging, SHA-pinned actions, Dependabot, RUNBOOK, `justfile`.
- **P3:** Rust `scanner_data` client module (hexagonal), sync loop + self-heal, local cache, `getCandles()`.
- **P4/P5:** corporate-action adjustment; first derived dataset (market breadth).

## Spec-coverage self-review

- Sources/fetch (§5.1–5.2): Tasks 2–3 (URL isolation, warm session, retry, typed errors, fallback). Indices + Kite backup deferred to P1 (noted). ✔ (partial-by-design)
- EQ + F-session filter (Global Constraints, §5.1): Task 5. ✔
- Trading calendar (§2 non-goals aside; §5.1 gate): Task 1 + Task 8 gate. ✔
- Storage schema, ISIN/instrument_key, parquet-by-year, dedupe (§6.1): Tasks 4, 5, 7. ✔
- Validation: row-count deviation + quarantine (§5.1, §9): Task 6 + Task 8. ✔
- Idempotency / fail-closed (§5.1): Task 7 (`has_day`) + Task 8. ✔
- Data contract shared test+runtime (§10): Task 4 `validate_ohlc`, used in Task 8. ✔
- Tested scenarios (§10 matrix, P0 subset): Task 8 + module tests. ✔
- CI gate (§11): Task 9 (full hardening — SHA pins, Dependabot — in P2, noted). ✔ (partial-by-design)
