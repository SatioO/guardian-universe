"""Kite manual day-rebuilder: last-resort recovery when both NSE sources are
down or a hole predates the archives. NEVER wired into cron/fallback chains --
credential-gated, human-triggered only.

All HTTP is faked (a canned `requests.Session`-like object recording calls
and returning canned responses) -- no live network.

The CLI (`rebuild-day`) section of this file exercises Kite's registration
into the broker-neutral `rebuild` registry (see pipeline/rebuild.py and
test_rebuild_registry.py for the registry's own broker-agnostic unit tests)
alongside genuinely broker-agnostic CLI dispatch tests using fake
non-Kite `RebuildSource` instances, proving `cmd_rebuild_day` never
hardcodes "kite" anywhere in its own logic."""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pipeline.errors import UnexpectedFailure
from pipeline.sources.kite_rebuild import KiteDayRebuilder

_TARGET = date(2026, 7, 3)

_INSTRUMENTS_CSV = (
    "instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,strike,"
    "tick_size,lot_size,instrument_type,segment,exchange\n"
    "128031234,500325,RELIANCE,RELIANCE INDUSTRIES,0,,0,0.05,1,EQ,NSE,NSE\n"
    "408065,1594,INFY,INFOSYS,0,,0,0.05,1,EQ,NSE,NSE\n"
    "738561,2885,TCS,TATA CONSULTANCY SERVICES,0,,0,0.05,1,EQ,NSE,NSE\n"
    # A non-NSE-segment row (e.g. an NFO future) must be excluded.
    "999999,999,RELIANCE26JULFUT,RELIANCE FUT,0,2026-07-30,0,0.05,250,FUT,NFO,NFO\n"
)


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, text: str = "", json_body: object = None):
        self.status_code = status_code
        self.text = text
        self._json_body = json_body

    def json(self) -> object:
        return self._json_body


class _FakeSession:
    """Records every GET call; returns canned responses keyed by exact URL,
    or a callable keyed by URL prefix for dynamic per-token candle responses."""

    def __init__(self, *, instruments_response: _FakeResponse | None = None):
        self.calls: list[dict[str, object]] = []
        self._instruments_response = instruments_response or _FakeResponse(
            text=_INSTRUMENTS_CSV
        )
        self._candle_responses: dict[str, _FakeResponse] = {}
        self._candle_error_urls: set[str] = set()

    def set_candle(self, token: str, response: _FakeResponse) -> None:
        self._candle_responses[token] = response

    def set_candle_error(self, token: str) -> None:
        self._candle_error_urls.add(token)

    def get(self, url: str, *, headers: dict[str, str] | None = None, timeout: int = 30):
        self.calls.append({"url": url, "headers": headers, "timeout": timeout})
        if url == "https://api.kite.trade/instruments/NSE":
            return self._instruments_response
        # /instruments/historical/{token}/day?from=...&to=...
        for token in self._candle_error_urls:
            if f"/instruments/historical/{token}/day" in url:
                raise ConnectionError(f"simulated network failure for token {token}")
        for token, resp in self._candle_responses.items():
            if f"/instruments/historical/{token}/day" in url:
                return resp
        raise AssertionError(f"unexpected URL in fake session: {url}")


def _candle_json(*, ts: str = "2026-07-03T00:00:00+0530", o=100.0, h=105.0, low=99.0,
                  c=103.0, v=10000) -> _FakeResponse:
    return _FakeResponse(json_body={"data": {"candles": [[ts, o, h, low, c, v]]}})


# ===========================================================================
# instruments()
# ===========================================================================


def test_instruments_parses_csv_and_maps_tradingsymbol_to_token():
    session = _FakeSession()
    rebuilder = KiteDayRebuilder("key", "token", session=session)
    result = rebuilder.instruments()
    assert result["RELIANCE"] == "128031234"
    assert result["INFY"] == "408065"
    assert result["TCS"] == "738561"


def test_instruments_excludes_non_nse_segment_rows():
    session = _FakeSession()
    rebuilder = KiteDayRebuilder("key", "token", session=session)
    result = rebuilder.instruments()
    assert "RELIANCE26JULFUT" not in result


def test_instruments_sends_correct_auth_headers():
    session = _FakeSession()
    rebuilder = KiteDayRebuilder("mykey", "mytoken", session=session)
    rebuilder.instruments()
    call = session.calls[0]
    assert call["url"] == "https://api.kite.trade/instruments/NSE"
    headers = call["headers"]
    assert headers["X-Kite-Version"] == "3"
    assert headers["Authorization"] == "token mykey:mytoken"


# ===========================================================================
# day_frame()
# ===========================================================================


def _universe(**overrides: tuple[str, str]) -> dict[str, tuple[str, str]]:
    base = {
        "RELIANCE": ("INE002A01018", "EQ"),
        "INFY": ("INE009A01021", "EQ"),
        "TCS": ("INE467B01029", "EQ"),
    }
    base.update(overrides)
    return base


def _rebuilder_with_instruments(**session_kwargs: object) -> tuple[KiteDayRebuilder, _FakeSession]:
    session = _FakeSession(**session_kwargs)  # type: ignore[arg-type]
    rebuilder = KiteDayRebuilder("key", "token", session=session, sleep=lambda _s: None)
    return rebuilder, session


def test_day_frame_shape_matches_udiff_raw_columns():
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json())
    session.set_candle("408065", _candle_json())
    session.set_candle("738561", _candle_json())

    df = rebuilder.day_frame(_TARGET, _universe())

    expected = {
        "TradDt", "ISIN", "TckrSymb", "SctySrs", "OpnPric", "HghPric", "LwPric",
        "ClsPric", "PrvsClsgPric", "TtlTradgVol", "TtlTrfVal", "TtlNbOfTxsExctd",
        "SsnId", "FinInstrmTp",
    }
    assert expected.issubset(set(df.columns))
    assert len(df) == 3


def test_day_frame_traddt_matches_requested_date_iso():
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json())
    df = rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "EQ")})
    assert (df["TradDt"] == "2026-07-03").all()


def test_day_frame_fixed_session_and_instrument_type_fields():
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json())
    df = rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "EQ")})
    row = df.iloc[0]
    assert row["SsnId"] == "F1"
    assert row["FinInstrmTp"] == "STK"


def test_day_frame_series_from_universe():
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json())
    df = rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "BE")})
    assert df.iloc[0]["SctySrs"] == "BE"


def test_day_frame_isin_from_universe():
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json())
    df = rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "EQ")})
    assert df.iloc[0]["ISIN"] == "INE002A01018"


def test_day_frame_ohlcv_from_candle():
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle(
        "128031234", _candle_json(o=2990.0, h=3010.0, low=2985.0, c=3000.0, v=1000000)
    )
    df = rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "EQ")})
    row = df.iloc[0]
    assert row["OpnPric"] == 2990.0
    assert row["HghPric"] == 3010.0
    assert row["LwPric"] == 2985.0
    assert row["ClsPric"] == 3000.0
    assert row["TtlTradgVol"] == 1000000


def test_day_frame_prevclose_degrades_to_open():
    # Documented degradation: the Kite day-candle API has no explicit
    # previous-close field, so PrvsClsgPric is set to the day's open with a
    # docstring note (a real prevclose would require fetching d-1's candle
    # too, which the rebuilder deliberately does not do -- one candle call
    # per symbol only).
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json(o=2990.0))
    df = rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "EQ")})
    assert df.iloc[0]["PrvsClsgPric"] == 2990.0


def test_day_frame_turnover_defaults_to_zero_with_degradation_note():
    # Documented degradation: TtlTrfVal (turnover) is unavailable from the
    # Kite day-candle payload -- defaults to 0.0.
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json())
    df = rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "EQ")})
    assert df.iloc[0]["TtlTrfVal"] == 0.0


def test_day_frame_trade_count_defaults_to_zero():
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json())
    df = rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "EQ")})
    assert df.iloc[0]["TtlNbOfTxsExctd"] == 0


def test_day_frame_rate_limit_sleep_called_n_minus_1_times():
    sleeps: list[float] = []
    session = _FakeSession()
    for token in ("128031234", "408065", "738561"):
        session.set_candle(token, _candle_json())
    rebuilder = KiteDayRebuilder(
        "key", "token", session=session, sleep=sleeps.append, rate_delay_s=0.35
    )
    rebuilder.day_frame(_TARGET, _universe())
    assert len(sleeps) == 2  # N=3 symbols -> N-1 sleeps between calls
    assert all(s == 0.35 for s in sleeps)


def test_day_frame_per_symbol_failure_tolerance():
    # 2 of 3 succeed -> 2 rows + 1 collected failure, not fatal.
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json())
    session.set_candle_error("408065")
    session.set_candle("738561", _candle_json())

    df = rebuilder.day_frame(_TARGET, _universe())

    assert len(df) == 2
    assert set(df["TckrSymb"]) == {"RELIANCE", "TCS"}
    assert len(rebuilder.failures) == 1
    assert "INFY" in rebuilder.failures[0]


def test_day_frame_symbol_missing_from_token_map_counted_as_failure():
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json())
    universe = {
        "RELIANCE": ("INE002A01018", "EQ"),
        "UNKNOWNSYM": ("INE999Z99999", "EQ"),  # not in the Kite token map
    }
    df = rebuilder.day_frame(_TARGET, universe)
    assert len(df) == 1
    assert len(rebuilder.failures) == 1
    assert "UNKNOWNSYM" in rebuilder.failures[0]


def test_day_frame_empty_candle_data_is_a_collected_failure():
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _FakeResponse(json_body={"data": {"candles": []}}))
    df = rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "EQ")})
    assert len(df) == 0
    assert len(rebuilder.failures) == 1


def test_day_frame_all_symbols_fail_yields_empty_frame_not_exception():
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle_error("128031234")
    df = rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "EQ")})
    assert len(df) == 0
    assert len(rebuilder.failures) == 1


def test_day_frame_sends_correct_historical_url_and_auth_headers():
    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json())
    rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "EQ")})
    candle_call = next(c for c in session.calls if "/instruments/historical/" in str(c["url"]))
    assert candle_call["url"] == (
        "https://api.kite.trade/instruments/historical/128031234/day"
        "?from=2026-07-03&to=2026-07-03"
    )
    headers = candle_call["headers"]
    assert headers["X-Kite-Version"] == "3"
    assert headers["Authorization"] == "token key:token"


def test_day_frame_output_feeds_normalize_equity_bhavcopy_unchanged():
    from pipeline.normalize import normalize_equity_bhavcopy

    rebuilder, session = _rebuilder_with_instruments()
    session.set_candle("128031234", _candle_json(o=2990.0, h=3010.0, low=2985.0, c=3000.0))
    df = rebuilder.day_frame(_TARGET, {"RELIANCE": ("INE002A01018", "EQ")})
    canonical = normalize_equity_bhavcopy(df, source="kite-rebuild")
    assert len(canonical) == 1
    row = canonical.iloc[0]
    assert row["symbol"] == "RELIANCE"
    assert row["instrument_key"] == "INE002A01018"
    assert row["close"] == 3000.0
    assert row["source"] == "kite-rebuild"


def test_default_sleep_parameter_is_the_real_time_sleep():
    # Per the constructor contract, `sleep` defaults to the real `time.sleep`
    # (production behavior with no override) -- default arguments bind once
    # at class-definition time, so this is an identity check rather than a
    # behavioral interception (which every other test in this file does via
    # an explicit `sleep=` override, the documented way to control it).
    import time as time_mod

    rebuilder = KiteDayRebuilder("key", "token", session=_FakeSession())
    assert rebuilder._sleep is time_mod.sleep


def test_kite_day_rebuilder_default_session_is_requests_session():
    import requests

    rebuilder = KiteDayRebuilder("key", "token")
    assert isinstance(rebuilder._session, requests.Session)


def test_instruments_missing_column_raises_unexpected_failure():
    bad_csv = "tradingsymbol,segment\nRELIANCE,NSE\n"  # missing instrument_token
    session = _FakeSession(instruments_response=_FakeResponse(text=bad_csv))
    rebuilder = KiteDayRebuilder("key", "token", session=session)
    with pytest.raises(UnexpectedFailure):
        rebuilder.instruments()


# ===========================================================================
# Broker-agnostic registration: RebuildSource conformance, available(),
# from_env(), and self-registration into the `rebuild` registry.
# ===========================================================================


def test_kite_day_rebuilder_has_id_kite():
    assert KiteDayRebuilder("key", "token").id == "kite"


def test_available_false_when_both_env_vars_missing(monkeypatch):
    monkeypatch.delenv("KITE_API_KEY", raising=False)
    monkeypatch.delenv("KITE_ACCESS_TOKEN", raising=False)
    rebuilder = KiteDayRebuilder("", "")
    assert rebuilder.available() is False


def test_available_false_when_one_env_var_missing(monkeypatch):
    monkeypatch.setenv("KITE_API_KEY", "somekey")
    monkeypatch.delenv("KITE_ACCESS_TOKEN", raising=False)
    rebuilder = KiteDayRebuilder("", "")
    assert rebuilder.available() is False


def test_available_true_when_both_env_vars_present(monkeypatch):
    monkeypatch.setenv("KITE_API_KEY", "key")
    monkeypatch.setenv("KITE_ACCESS_TOKEN", "token")
    rebuilder = KiteDayRebuilder("", "")
    assert rebuilder.available() is True


def test_available_reflects_live_environment_not_construction_time_snapshot(monkeypatch):
    # available() must re-read the environment on every call -- a from_env()
    # instance built (registered) before credentials were exported must still
    # correctly report available() once they ARE exported later in the same
    # process (this is exactly what happens across a test session: the
    # module registers once at first import, long before any test sets env).
    monkeypatch.delenv("KITE_API_KEY", raising=False)
    monkeypatch.delenv("KITE_ACCESS_TOKEN", raising=False)
    rebuilder = KiteDayRebuilder.from_env()
    assert rebuilder.available() is False

    monkeypatch.setenv("KITE_API_KEY", "key")
    monkeypatch.setenv("KITE_ACCESS_TOKEN", "token")
    assert rebuilder.available() is True  # same instance, no re-construction


def test_from_env_never_raises_with_missing_credentials(monkeypatch):
    monkeypatch.delenv("KITE_API_KEY", raising=False)
    monkeypatch.delenv("KITE_ACCESS_TOKEN", raising=False)
    rebuilder = KiteDayRebuilder.from_env()  # must not raise
    assert rebuilder.available() is False


def test_from_env_picks_up_live_credentials_for_actual_calls(monkeypatch):
    monkeypatch.setenv("KITE_API_KEY", "envkey")
    monkeypatch.setenv("KITE_ACCESS_TOKEN", "envtoken")
    session = _FakeSession()
    rebuilder = KiteDayRebuilder.from_env()
    rebuilder._session = session  # swap in the fake after construction
    rebuilder.instruments()
    call = session.calls[0]
    assert call["headers"]["Authorization"] == "token envkey:envtoken"


def test_kite_module_self_registers_under_id_kite():
    from pipeline import rebuild
    from pipeline.sources import kite_rebuild as kr_mod

    assert "kite" in rebuild.REBUILDERS
    assert isinstance(rebuild.REBUILDERS["kite"], kr_mod.KiteDayRebuilder)


def test_resolve_kite_by_id_when_available(monkeypatch):
    from pipeline import rebuild

    monkeypatch.setenv("KITE_API_KEY", "key")
    monkeypatch.setenv("KITE_ACCESS_TOKEN", "token")
    resolved = rebuild.resolve("kite")
    assert resolved.id == "kite"


def test_resolve_raises_when_kite_requested_but_unavailable(monkeypatch):
    from pipeline import rebuild

    monkeypatch.delenv("KITE_API_KEY", raising=False)
    monkeypatch.delenv("KITE_ACCESS_TOKEN", raising=False)
    with pytest.raises(ValueError, match="kite"):
        rebuild.resolve("kite")


# ===========================================================================
# CLI: rebuild-day
# ===========================================================================


def test_parser_accepts_rebuild_day_with_date():
    from pipeline import cli

    args = cli.build_parser().parse_args(["rebuild-day", "--date", "2026-07-03"])
    assert args.cmd == "rebuild-day"
    assert args.date == "2026-07-03"
    assert args.via is None  # optional, defaults to "first available"


def test_parser_accepts_rebuild_day_via_flag():
    from pipeline import cli

    args = cli.build_parser().parse_args(
        ["rebuild-day", "--date", "2026-07-03", "--via", "kite"]
    )
    assert args.via == "kite"


def test_parser_rejects_unknown_via_id():
    from pipeline import cli

    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            ["rebuild-day", "--date", "2026-07-03", "--via", "not-a-real-broker"]
        )


_UDIFF_COLUMNS = [
    "TradDt", "ISIN", "TckrSymb", "SctySrs", "OpnPric", "HghPric", "LwPric",
    "ClsPric", "PrvsClsgPric", "TtlTradgVol", "TtlTrfVal", "TtlNbOfTxsExctd",
    "SsnId", "FinInstrmTp",
]


class _FakeRebuildSource:
    """A minimal RebuildSource stand-in for CLI-level tests -- these must
    never depend on Kite specifics (env vars, HTTP), only on the
    `rebuild.RebuildSource` Protocol surface (id/available/day_frame) plus
    the conventional (not Protocol-required) `failures` list the CLI reads
    defensively via getattr."""

    def __init__(
        self, *, id: str = "fake", available: bool = True,  # noqa: A002
        rows: list[dict[str, object]] | None = None,
        failures: list[str] | None = None,
        record_universe_into: list[dict[str, tuple[str, str]]] | None = None,
    ) -> None:
        self.id = id
        self._available = available
        self._rows = rows or []
        self.failures = failures or []
        self._record_universe_into = record_universe_into

    def available(self) -> bool:
        return self._available

    def day_frame(self, d, universe):  # noqa: ANN001
        if self._record_universe_into is not None:
            self._record_universe_into.append(dict(universe))
        return pd.DataFrame(self._rows, columns=_UDIFF_COLUMNS)


def _seed_reference(ref_dir, rows: list[dict[str, object]]) -> None:
    ref_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(
        ref_dir / "instruments_all.parquet", compression="zstd", index=False
    )


def _scope_config_to_tmp(monkeypatch, config_mod, cli_mod, datasets_mod, tmp_path, *,
                          ref_rows: list[dict[str, object]],
                          abs_rowcount_range: tuple[int, int] = (0, 10**9)) -> None:
    """Shared setup for rebuild-day CLI tests: scopes REFERENCE_DIR, META_DIR
    (with an empty holidays.json), and the equities registry entry to tmp_path
    -- so a test can NEVER accidentally read/write the real repo's
    data/meta/holidays.json or data/ohlc store (Global Constraint 6)."""
    import dataclasses

    ref_dir = tmp_path / "reference"
    _seed_reference(ref_dir, ref_rows)
    monkeypatch.setattr(config_mod, "REFERENCE_DIR", ref_dir)

    meta_dir = tmp_path / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "holidays.json").write_text("{}")
    monkeypatch.setattr(config_mod, "META_DIR", meta_dir)

    equities_spec = dataclasses.replace(
        datasets_mod.EQUITIES, base_dir=tmp_path / "ohlc",
        abs_rowcount_range=abs_rowcount_range,
    )
    monkeypatch.setattr(cli_mod.datasets, "DATASETS", {"equities": equities_spec})
    monkeypatch.setattr(cli_mod.datasets, "DATASET_ORDER", ["equities"])
    return equities_spec  # noqa: RET504 -- convenience for callers wanting base_dir


_DEFAULT_REF_ROWS = [
    {
        "instrument_key": "INE002A01018", "isin": "INE002A01018",
        "symbol": "RELIANCE", "series": "EQ", "status": "active",
        "last_seen": pd.Timestamp("2026-07-02"),
    },
]


def test_rebuild_day_no_available_source_exits_2(monkeypatch, tmp_path):
    """Zero registered sources have usable credentials -- a clear, actionable
    error naming every registered id, not a Kite-specific message (the
    registry itself may hold brokers other than Kite in the future)."""
    from pipeline import cli, config, datasets

    _scope_config_to_tmp(monkeypatch, config, cli, datasets, tmp_path,
                         ref_rows=_DEFAULT_REF_ROWS)
    monkeypatch.setattr(cli.rebuild, "REBUILDERS",
                        {"fake": _FakeRebuildSource(id="fake", available=False)})

    rc = cli.main(["rebuild-day", "--date", "2026-07-03"])
    assert rc == 2


def test_rebuild_day_missing_reference_exits_2(monkeypatch, tmp_path):
    from pipeline import cli, config

    monkeypatch.setattr(cli.rebuild, "REBUILDERS",
                        {"fake": _FakeRebuildSource(id="fake", available=True)})
    monkeypatch.setattr(config, "REFERENCE_DIR", tmp_path / "does_not_exist")
    rc = cli.main(["rebuild-day", "--date", "2026-07-03"])
    assert rc == 2


def test_rebuild_day_end_to_end_lands_rows_with_derived_source_label(
    monkeypatch, tmp_path
):
    """Provenance is derived from the resolved source's own `id` (never a
    hardcoded broker name) -- a fake source with id="acme" must land rows
    tagged "acme-rebuild", proving the label isn't Kite-specific."""
    from pipeline import cli, config, datasets

    equities_spec = _scope_config_to_tmp(
        monkeypatch, config, cli, datasets, tmp_path, ref_rows=_DEFAULT_REF_ROWS,
    )

    fake_source = _FakeRebuildSource(id="acme", available=True, rows=[{
        "TradDt": "2026-07-03", "ISIN": "INE002A01018", "TckrSymb": "RELIANCE",
        "SctySrs": "EQ", "OpnPric": 2990.0, "HghPric": 3010.0, "LwPric": 2985.0,
        "ClsPric": 3000.0, "PrvsClsgPric": 2990.0, "TtlTradgVol": 1000000,
        "TtlTrfVal": 0.0, "TtlNbOfTxsExctd": 0, "SsnId": "F1", "FinInstrmTp": "STK",
    }])
    monkeypatch.setattr(cli.rebuild, "REBUILDERS", {"acme": fake_source})

    # BUILDERS must never fire for rebuild-day (single-spec run, no phase 2).
    builder_calls: list[str] = []

    def _boom_builder(spec, target):  # noqa: ANN001, ARG001
        builder_calls.append(spec.key)
        raise AssertionError("derived builders must never run for rebuild-day")

    monkeypatch.setattr(cli.builders, "BUILDERS", {"reference": _boom_builder})

    rc = cli.main(["rebuild-day", "--date", "2026-07-03"])

    assert rc == 0
    assert builder_calls == []
    stored = pd.read_parquet(equities_spec.base_dir / "ohlc_2026.parquet")
    assert len(stored) == 1
    assert set(stored["source"]) == {"acme-rebuild"}
    assert stored.iloc[0]["symbol"] == "RELIANCE"


def test_rebuild_day_dispatches_with_zero_kite_specific_code(monkeypatch, tmp_path):
    """Proves `--via` dispatch never touches Kite: the real `kite` entry is
    REMOVED from the registry (only a fake broker remains), and the resolved
    source is still used successfully end-to-end -- if cmd_rebuild_day (or
    anything it calls in the dispatch path) hardcoded "kite" anywhere, this
    would fail with a KeyError/ValueError instead of succeeding."""
    from pipeline import cli, config, datasets

    equities_spec = _scope_config_to_tmp(
        monkeypatch, config, cli, datasets, tmp_path, ref_rows=_DEFAULT_REF_ROWS,
    )

    fake_source = _FakeRebuildSource(id="acme", available=True, rows=[{
        "TradDt": "2026-07-03", "ISIN": "INE002A01018", "TckrSymb": "RELIANCE",
        "SctySrs": "EQ", "OpnPric": 2990.0, "HghPric": 3010.0, "LwPric": 2985.0,
        "ClsPric": 3000.0, "PrvsClsgPric": 2990.0, "TtlTradgVol": 1000000,
        "TtlTrfVal": 0.0, "TtlNbOfTxsExctd": 0, "SsnId": "F1", "FinInstrmTp": "STK",
    }])
    # Note: no "kite" key at all -- the registry holds ONLY the fake broker.
    monkeypatch.setattr(cli.rebuild, "REBUILDERS", {"acme": fake_source})
    monkeypatch.setattr(cli.builders, "BUILDERS", {})

    rc = cli.main(["rebuild-day", "--date", "2026-07-03", "--via", "acme"])

    assert rc == 0
    stored = pd.read_parquet(equities_spec.base_dir / "ohlc_2026.parquet")
    assert set(stored["source"]) == {"acme-rebuild"}


def test_rebuild_day_via_unavailable_preferred_source_exits_2(monkeypatch, tmp_path):
    from pipeline import cli, config, datasets

    _scope_config_to_tmp(monkeypatch, config, cli, datasets, tmp_path,
                         ref_rows=_DEFAULT_REF_ROWS)
    monkeypatch.setattr(cli.rebuild, "REBUILDERS", {
        "acme": _FakeRebuildSource(id="acme", available=False),
        "other": _FakeRebuildSource(id="other", available=True),
    })

    rc = cli.main(["rebuild-day", "--date", "2026-07-03", "--via", "acme"])
    assert rc == 2  # "other" being available doesn't help -- acme was explicitly requested


def test_rebuild_day_excludes_index_series_from_universe(monkeypatch, tmp_path):
    """The reference-derived universe must filter series != 'INDEX' -- index
    rows have no tradable broker instrument token and must never be attempted."""
    from pipeline import cli, config, datasets

    _scope_config_to_tmp(monkeypatch, config, cli, datasets, tmp_path, ref_rows=[
        {
            "instrument_key": "NIFTY 50", "isin": "", "symbol": "NIFTY 50",
            "series": "INDEX", "status": "active",
            "last_seen": pd.Timestamp("2026-07-02"),
        },
    ])

    universes: list[dict[str, tuple[str, str]]] = []
    fake_source = _FakeRebuildSource(available=True, record_universe_into=universes)
    monkeypatch.setattr(cli.rebuild, "REBUILDERS", {"fake": fake_source})

    cli.main(["rebuild-day", "--date", "2026-07-03"])

    assert universes  # the CLI actually called day_frame
    assert "NIFTY 50" not in universes[0]


def test_rebuild_day_excludes_inactive_status_from_universe(monkeypatch, tmp_path):
    from pipeline import cli, config, datasets

    _scope_config_to_tmp(monkeypatch, config, cli, datasets, tmp_path, ref_rows=[
        {
            "instrument_key": "INE_OLD", "isin": "INE_OLD", "symbol": "DEADCO",
            "series": "EQ", "status": "inactive",
            "last_seen": pd.Timestamp("2026-01-01"),
        },
    ])

    universes: list[dict[str, tuple[str, str]]] = []
    fake_source = _FakeRebuildSource(available=True, record_universe_into=universes)
    monkeypatch.setattr(cli.rebuild, "REBUILDERS", {"fake": fake_source})

    cli.main(["rebuild-day", "--date", "2026-07-03"])

    assert universes
    assert "DEADCO" not in universes[0]


def test_rebuild_day_prints_run_status_and_failure_count(monkeypatch, tmp_path, capsys):
    from pipeline import cli, config, datasets

    _scope_config_to_tmp(monkeypatch, config, cli, datasets, tmp_path,
                         ref_rows=_DEFAULT_REF_ROWS)

    fake_source = _FakeRebuildSource(
        available=True, failures=["RELIANCE: simulated failure"],
    )
    monkeypatch.setattr(cli.rebuild, "REBUILDERS", {"fake": fake_source})

    cli.main(["rebuild-day", "--date", "2026-07-03"])
    captured = capsys.readouterr()
    assert "failure" in (captured.out + captured.err).lower()


def test_rebuild_day_exits_per_run_status_failed(monkeypatch, tmp_path):
    """A partial rebuild below the abs floor legitimately fails (documented):
    the frame goes through the SAME rowcount gates as any daily run."""
    from pipeline import cli, config, datasets

    # Real production abs_rowcount_range (2000, 10000) -- a single-row rebuild
    # frame must legitimately fail the rowcount floor.
    _scope_config_to_tmp(
        monkeypatch, config, cli, datasets, tmp_path, ref_rows=_DEFAULT_REF_ROWS,
        abs_rowcount_range=datasets.EQUITIES.abs_rowcount_range,
    )

    fake_source = _FakeRebuildSource(available=True, rows=[{
        "TradDt": "2026-07-03", "ISIN": "INE002A01018", "TckrSymb": "RELIANCE",
        "SctySrs": "EQ", "OpnPric": 3000.0, "HghPric": 3010.0, "LwPric": 2990.0,
        "ClsPric": 3000.0, "PrvsClsgPric": 3000.0, "TtlTradgVol": 1000,
        "TtlTrfVal": 0.0, "TtlNbOfTxsExctd": 0, "SsnId": "F1", "FinInstrmTp": "STK",
    }])
    monkeypatch.setattr(cli.rebuild, "REBUILDERS", {"fake": fake_source})

    rc = cli.main(["rebuild-day", "--date", "2026-07-03"])
    assert rc == 1  # rowcount floor legitimately fails a 1-row "full market" frame
