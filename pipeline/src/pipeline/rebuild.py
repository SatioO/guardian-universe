"""Broker-neutral manual day-rebuild registry -- the seam that keeps
`rebuild-day` from ever hardcoding a broker name in shared/dispatch code (the
project's core registry principle: adding a broker means "implement the
interface + register", zero edits to call sites).

Each broker-specific rebuild source (e.g. `sources.kite_rebuild.KiteDayRebuilder`)
implements `RebuildSource` and self-registers at import time. `cli.py`'s
`cmd_rebuild_day` never mentions a broker by name -- it resolves a source via
`resolve()` (by an operator-supplied `--via <id>` or, absent that, the first
available registered source) and calls the resulting object's `day_frame`.

The registry mirrors the backend `HistoricalDataProvider` registry pattern
(see docs/broker-integration-guide.md in the frontend repo for the canonical
description of this pattern) -- open id strings resolved at runtime, not a
closed type union hand-edited per broker."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Protocol

import pandas as pd


class RebuildSource(Protocol):
    """A pluggable manual day-rebuild source (one per broker).

    `available()` must be a cheap, side-effect-free check of whatever that
    source needs to operate (typically: are its credential env vars present)
    -- it must NEVER raise, and must never require network access itself.
    `day_frame` returns one row per successfully-rebuilt symbol in the
    PRIMARY-RAW UDiFF shape (the same fallback contract used by the
    sec_bhavdata_full fallback, see fetch.py's docstring), so the existing
    `normalize_equity_bhavcopy` consumes it unchanged regardless of which
    broker served it.
    """

    id: str

    def available(self) -> bool: ...

    def day_frame(self, d: date, universe: Mapping[str, tuple[str, str]]) -> pd.DataFrame: ...


REBUILDERS: dict[str, RebuildSource] = {}


def register(source: RebuildSource) -> None:
    """Register a rebuild source under its own `id`. Re-registering the same
    id replaces the previous entry (idempotent-safe for repeated imports /
    test monkeypatching that restores REBUILDERS from a fresh dict)."""
    REBUILDERS[source.id] = source


def resolve(preferred: str | None) -> RebuildSource:
    """Resolve a rebuild source to use.

    - `preferred` given: that id must be registered AND available (its own
      credentials present) -- otherwise a clear, actionable error.
    - `preferred` is None: the first available source, in registration order
      (dict insertion order) -- otherwise a clear error listing every
      registered id and whether each is available.

    Never returns a source that isn't currently `available()` -- resolve is
    the one gate that decides "can we actually rebuild right now", so a
    caller never has to separately check availability after this returns.
    """
    if preferred is not None:
        source = REBUILDERS.get(preferred)
        if source is None:
            known = sorted(REBUILDERS)
            raise ValueError(
                f"no rebuild source registered with id {preferred!r} "
                f"(known: {known})"
            )
        if not source.available():
            raise ValueError(
                f"rebuild source {preferred!r} is registered but not available "
                "(its required credentials are missing -- see RUNBOOK.md)"
            )
        return source

    for source in REBUILDERS.values():
        if source.available():
            return source

    if not REBUILDERS:
        raise ValueError("no rebuild sources registered")

    raise ValueError(
        "no rebuild source is available (all registered sources are missing "
        f"credentials): {sorted(REBUILDERS)} -- see RUNBOOK.md for how to "
        "configure one"
    )
