"""Broker-neutral manual day-rebuild registry (`pipeline.rebuild`): pure unit
tests using fake `RebuildSource` instances -- no broker (Kite or otherwise)
mentioned anywhere in this file, proving the registry itself is fully
broker-agnostic."""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pipeline import rebuild


class _FakeSource:
    def __init__(self, *, id: str, available: bool) -> None:  # noqa: A002
        self.id = id
        self._available = available
        self.day_frame_calls = 0

    def available(self) -> bool:
        return self._available

    def day_frame(self, d: date, universe) -> pd.DataFrame:  # noqa: ANN001
        self.day_frame_calls += 1
        return pd.DataFrame()


@pytest.fixture(autouse=True)
def _clean_registry(monkeypatch):
    """Every test gets an empty registry -- the real module-level REBUILDERS
    (populated by importing pipeline.sources.kite_rebuild elsewhere in the
    test session) must never leak into these pure registry tests."""
    monkeypatch.setattr(rebuild, "REBUILDERS", {})


def test_register_adds_source_under_its_own_id():
    source = _FakeSource(id="alpha", available=True)
    rebuild.register(source)
    assert {"alpha": source} == rebuild.REBUILDERS


def test_register_replaces_existing_entry_with_same_id():
    first = _FakeSource(id="alpha", available=True)
    second = _FakeSource(id="alpha", available=False)
    rebuild.register(first)
    rebuild.register(second)
    assert rebuild.REBUILDERS["alpha"] is second


def test_resolve_preferred_id_returns_that_source_when_available():
    alpha = _FakeSource(id="alpha", available=True)
    beta = _FakeSource(id="beta", available=True)
    rebuild.register(alpha)
    rebuild.register(beta)

    resolved = rebuild.resolve("beta")
    assert resolved is beta


def test_resolve_preferred_id_unknown_raises_clear_error():
    rebuild.register(_FakeSource(id="alpha", available=True))
    with pytest.raises(ValueError, match="unknown-id"):
        rebuild.resolve("unknown-id")


def test_resolve_preferred_id_unavailable_raises_clear_error():
    rebuild.register(_FakeSource(id="alpha", available=False))
    with pytest.raises(ValueError, match="alpha"):
        rebuild.resolve("alpha")


def test_resolve_preferred_id_unavailable_does_not_fall_back_to_another_available_source():
    rebuild.register(_FakeSource(id="alpha", available=False))
    beta = _FakeSource(id="beta", available=True)
    rebuild.register(beta)
    # Explicitly requesting "alpha" must fail even though "beta" is ready --
    # an explicit --via is a hard requirement, not a soft preference.
    with pytest.raises(ValueError):
        rebuild.resolve("alpha")


def test_resolve_none_returns_first_available_in_registration_order():
    alpha = _FakeSource(id="alpha", available=False)
    beta = _FakeSource(id="beta", available=True)
    gamma = _FakeSource(id="gamma", available=True)
    rebuild.register(alpha)
    rebuild.register(beta)
    rebuild.register(gamma)

    resolved = rebuild.resolve(None)
    assert resolved is beta  # first AVAILABLE one, not first registered


def test_resolve_none_skips_unavailable_sources_before_the_first_available_one():
    alpha = _FakeSource(id="alpha", available=False)
    beta = _FakeSource(id="beta", available=False)
    gamma = _FakeSource(id="gamma", available=True)
    rebuild.register(alpha)
    rebuild.register(beta)
    rebuild.register(gamma)

    resolved = rebuild.resolve(None)
    assert resolved is gamma


def test_resolve_none_with_empty_registry_raises_clear_error():
    with pytest.raises(ValueError, match="no rebuild sources registered"):
        rebuild.resolve(None)


def test_resolve_none_with_no_available_sources_raises_listing_all_ids():
    rebuild.register(_FakeSource(id="alpha", available=False))
    rebuild.register(_FakeSource(id="beta", available=False))
    with pytest.raises(ValueError) as exc_info:
        rebuild.resolve(None)
    message = str(exc_info.value)
    assert "alpha" in message
    assert "beta" in message


def test_resolve_never_calls_day_frame_itself():
    # resolve() is purely a selection step -- it must never invoke day_frame
    # on the caller's behalf (that's the caller's job, with the actual
    # target date and universe).
    source = _FakeSource(id="alpha", available=True)
    rebuild.register(source)
    rebuild.resolve(None)
    assert source.day_frame_calls == 0


def test_rebuild_source_protocol_is_structural_not_nominal():
    # Any object with id/available()/day_frame() satisfies RebuildSource --
    # no inheritance from a base class required (Protocol duck-typing).
    class DuckTyped:
        id = "duck"

        def available(self) -> bool:
            return True

        def day_frame(self, d, universe):  # noqa: ANN001
            return pd.DataFrame()

    duck = DuckTyped()
    rebuild.register(duck)
    assert rebuild.resolve("duck") is duck
