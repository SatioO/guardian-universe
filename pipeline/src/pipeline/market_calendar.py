"""The one market calendar: `pipeline/data/meta/market_calendar.json`.

Everything that needs exchange days reads this file through `load`: the
producer's own trading days (`calendar.load_trading_calendar`, used by the CLI
and builders) and the `market_calendar` dataset the desktop app syncs
(`MarketCalendar.rows`, written by builders.build_market_calendar). It is
curated by hand from exchange circulars, never scraped.

Per venue (NSE, BSE, MCX; the app's NFO shares NSE's):

  closures      dates the venue is shut, by year
  sessions      days that trade other hours than the regular ones: a special
                session on a weekend or a closure (Union Budget, Diwali
                Muhurat) or a partial day (an MCX morning- or evening-session
                closure). Hours are exchange-local HH:MM, with an optional
                preOpen (call-auction start). A session without hours is a day
                the exchange is known to have traded whose hours are not yet
                verified: a trading day for the producer, not yet sent to the
                app, which needs the hours.
  coveredYears  years whose closures are verified complete, with every
                special session listed (some may still await their hours,
                which exchanges announce shortly before: the app treats such
                a day as closed until they are added, and the builder names
                it on every run). The app reads any other year as unknown,
                never as ordinary.

The published rows: `closed` per closure, `session` per session with hours (a
day may hold several windows; a session replaces that day's closure), and
`covered` per covered year (`day` = 1 January).

Pure apart from reading the file; anything malformed raises CalendarError.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

FILENAME = "market_calendar.json"

VENUES: tuple[str, ...] = ("NSE", "BSE", "MCX")

DAY_SECONDS = 24 * 60 * 60


class CalendarError(ValueError):
    """The market calendar file is missing, malformed or contradicts itself."""


@dataclass(frozen=True)
class Session:
    day: date
    label: str
    source: str
    # None when the exchange is known to have traded but its hours are not
    # verified yet (then all three are None).
    pre_open_seconds: int | None
    open_seconds: int | None
    close_seconds: int | None

    @property
    def has_hours(self) -> bool:
        return self.open_seconds is not None


@dataclass(frozen=True)
class Venue:
    id: str
    source: str
    closures: frozenset[date]
    sessions: tuple[Session, ...]
    covered_years: frozenset[int]


@dataclass(frozen=True)
class Row:
    """One published row of the dataset."""

    venue: str
    day: date
    kind: str  # "closed" | "session" | "covered"
    pre_open_seconds: int | None
    open_seconds: int | None
    close_seconds: int | None
    label: str
    source: str


@dataclass(frozen=True)
class MarketCalendar:
    venues: dict[str, Venue]

    def trading_inputs(self, venue: str = "NSE") -> tuple[set[date], set[date]]:
        """(closures, sessions) for `calendar.is_trading_day`: a session day
        trades even when it is a weekend or a listed closure."""
        v = self.venues[venue]
        return set(v.closures), {s.day for s in v.sessions}

    def sessions_awaiting_hours(self) -> list[str]:
        """'NSE 2026-11-08' for each listed session in a covered year whose
        hours are not added yet: the app treats it as closed until they are.
        Sessions in years the calendar does not cover are history the app never
        reads as complete, so they are not named."""
        return [
            f"{v.id} {s.day.isoformat()}"
            for v in self.venues.values()
            for s in v.sessions
            if not s.has_hours and s.day.year in v.covered_years
        ]

    def rows(self) -> list[Row]:
        """Every published row, sorted by venue, day, kind, open."""
        rows: list[Row] = []
        for v in self.venues.values():
            session_days = {s.day for s in v.sessions if s.has_hours}
            for year in sorted(v.covered_years):
                rows.append(
                    Row(
                        v.id,
                        date(year, 1, 1),
                        "covered",
                        None,
                        None,
                        None,
                        f"closures and session hours verified for {year}",
                        v.source,
                    )
                )
            for day in sorted(v.closures - session_days):
                rows.append(Row(v.id, day, "closed", None, None, None, "closed", v.source))
            for s in v.sessions:
                if s.has_hours:
                    rows.append(
                        Row(
                            v.id,
                            s.day,
                            "session",
                            s.pre_open_seconds,
                            s.open_seconds,
                            s.close_seconds,
                            s.label,
                            s.source,
                        )
                    )
        kind_order = {"covered": 0, "closed": 1, "session": 2}
        return sorted(rows, key=lambda r: (r.venue, r.day, kind_order[r.kind], r.open_seconds or 0))


def _day(raw: object, where: str) -> date:
    try:
        return date.fromisoformat(str(raw))
    except ValueError as e:
        raise CalendarError(f"{where}: {raw!r} is not a YYYY-MM-DD date") from e


def _seconds(raw: object, where: str) -> int:
    text = str(raw)
    hours, sep, minutes = text.partition(":")
    if not sep or not hours.isdigit() or not minutes.isdigit() or len(minutes) != 2:
        raise CalendarError(f"{where}: {text!r} is not HH:MM")
    seconds = int(hours) * 3600 + int(minutes) * 60
    if int(minutes) >= 60 or seconds > DAY_SECONDS:
        raise CalendarError(f"{where}: {text!r} is not a time of day")
    return seconds


def _session(raw: dict, venue_source: str, where: str) -> Session:
    day = _day(raw.get("date"), where)
    label = str(raw.get("label", "")).strip() or "session"
    source = str(raw.get("source", "")).strip() or venue_source
    timed = [k for k in ("preOpen", "open", "close") if k in raw]
    if not timed:
        return Session(day, label, source, None, None, None)
    if "open" not in raw or "close" not in raw:
        raise CalendarError(f"{where}: a session with hours needs both open and close")
    pre_open = _seconds(raw["preOpen"], f"{where} preOpen") if "preOpen" in raw else None
    open_ = _seconds(raw["open"], f"{where} open")
    close = _seconds(raw["close"], f"{where} close")
    if not open_ < close:
        raise CalendarError(f"{where}: opens at {raw['open']} but closes at {raw['close']}")
    if pre_open is not None and pre_open > open_:
        raise CalendarError(f"{where}: pre-open {raw['preOpen']} is after the open {raw['open']}")
    return Session(day, label, source, pre_open, open_, close)


def _venue(venue_id: str, raw: dict) -> Venue:
    where = f"{FILENAME} {venue_id}"
    source = str(raw.get("source", "")).strip()
    if not source:
        raise CalendarError(f"{where}: no source")
    closures: set[date] = set()
    years_listed: set[int] = set()
    for year_text, days in raw.get("closures", {}).items():
        if not str(year_text).isdigit():
            raise CalendarError(f"{where}: closure year {year_text!r} is not a year")
        year = int(year_text)
        years_listed.add(year)
        for d in days:
            day = _day(d, f"{where} closures {year}")
            if day.year != year:
                raise CalendarError(
                    f"{where} closures {year}: {day.isoformat()} is filed under the wrong year"
                )
            closures.add(day)
    sessions = tuple(
        sorted(
            (
                _session(s, source, f"{where} sessions[{i}]")
                for i, s in enumerate(raw.get("sessions", []))
            ),
            key=lambda s: (s.day, s.pre_open_seconds or s.open_seconds or 0),
        )
    )
    for earlier, later in zip(sessions, sessions[1:], strict=False):
        if earlier.day != later.day:
            continue
        if not (earlier.has_hours and later.has_hours):
            raise CalendarError(
                f"{where} {later.day.isoformat()}: a day with several sessions needs hours on each"
            )
        if (later.pre_open_seconds or later.open_seconds or 0) < (earlier.close_seconds or 0):
            raise CalendarError(f"{where} {later.day.isoformat()}: overlapping sessions")
    covered = raw.get("coveredYears", [])
    for year in covered:
        if not isinstance(year, int) or year not in years_listed:
            raise CalendarError(f"{where}: covered year {year!r} has no closure list")
    return Venue(venue_id, source, frozenset(closures), sessions, frozenset(covered))


def parse(doc: dict) -> MarketCalendar:
    """Validate and type the calendar document."""
    venues = doc.get("venues")
    if not isinstance(venues, dict):
        raise CalendarError(f"{FILENAME}: no venues")
    unknown = sorted(k for k in venues if k not in VENUES)
    if unknown:
        raise CalendarError(f"{FILENAME}: unknown venue {unknown[0]!r}")
    missing = [v for v in VENUES if v not in venues]
    if missing:
        raise CalendarError(f"{FILENAME}: no entry for {missing[0]}")
    return MarketCalendar({v: _venue(v, venues[v]) for v in VENUES})


def load(path: Path) -> MarketCalendar:
    """Read and validate the calendar file at `path`."""
    try:
        doc = json.loads(path.read_text())
    except FileNotFoundError as e:
        raise CalendarError(f"{path.name} is missing") from e
    except json.JSONDecodeError as e:
        raise CalendarError(f"{path.name} is not valid JSON: {e}") from e
    return parse(doc)
