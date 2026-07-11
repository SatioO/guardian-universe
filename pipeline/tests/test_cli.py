import dataclasses
import hashlib
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import calendar as cal
from pipeline import cli, config, datasets
from pipeline.errors import NotYetPublished, UnexpectedFailure
from pipeline.fetch import FetchResult
from pipeline.manifest import asset_name
from tests.fakes import FakeReleaseClient


def _write_parquet(p: Path, n: int) -> None:
    df = pd.DataFrame({c: [0] * n for c in config.CANON_COLUMNS})
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, compression="zstd", index=False)


def test_parser_reads_backfill_days():
    args = cli.build_parser().parse_args(["backfill", "--days", "42"])
    assert args.cmd == "backfill" and args.days == 42


def test_parser_backfill_dataset_choices():
    args = cli.build_parser().parse_args(["backfill", "--days", "1", "--dataset", "equities"])
    assert args.dataset == "equities"
    assert cli.build_parser().parse_args(["backfill", "--days", "1"]).dataset == "all"
    import pytest
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["backfill", "--days", "1", "--dataset", "bogus"])


def test_main_backfill_runs_all_registered_specs(monkeypatch, tmp_path):
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    seen = []

    def fake_backfill(spec, end, n, **kw):
        seen.append(spec.key)
        return [RunStatus("success", date(2026, 7, 3), source=spec.source_label)]

    monkeypatch.setattr(cli.backfill_mod, "backfill", fake_backfill)
    assert cli.main(["backfill", "--days", "1"]) == 0
    assert seen == ["equities", "indices"]  # DATASET_ORDER as of G1b task 3


def test_main_backfill_skips_derived_specs(monkeypatch, tmp_path):
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path)
    seen = []

    def fake_backfill(spec, end, n, **kw):
        seen.append(spec.key)
        return [RunStatus("success", date(2026, 7, 3), source=spec.source_label)]

    monkeypatch.setattr(cli.backfill_mod, "backfill", fake_backfill)
    assert cli.main(["backfill", "--days", "1"]) == 0
    assert seen == ["equities", "indices"]  # reference (derived) never backfills


def test_parser_reads_daily_date():
    args = cli.build_parser().parse_args(["daily", "--date", "2026-07-03"])
    assert args.cmd == "daily" and args.date == "2026-07-03"


def test_main_publish_returns_1_on_failure(monkeypatch):
    def _boom(**_kw):
        from pipeline.errors import UnexpectedFailure
        raise UnexpectedFailure("no data")
    monkeypatch.setattr(cli, "publish_dataset", _boom)
    assert cli.main(["publish"]) == 1


def test_main_publish_returns_0_on_success(monkeypatch):
    calls = {}
    monkeypatch.setattr(cli, "publish_dataset", lambda **kw: calls.update(kw))
    assert cli.main(["publish"]) == 0
    assert calls["specs"] == datasets.all_specs()


def test_parser_has_sync():
    args = cli.build_parser().parse_args(["sync"])
    assert args.cmd == "sync"


def test_main_sync_returns_1_on_failure(monkeypatch):
    from pipeline.errors import ReleaseError

    def _boom(*a, **kw):
        raise ReleaseError("network down")

    monkeypatch.setattr(cli, "sync_store", _boom)
    assert cli.main(["sync"]) == 1


def test_main_sync_returns_0_on_success(monkeypatch):
    monkeypatch.setattr(cli, "sync_store", lambda *a, **kw: None)
    assert cli.main(["sync"]) == 0


def test_parser_has_check_freshness():
    assert cli.build_parser().parse_args(["check-freshness"]).cmd == "check-freshness"


def test_cmd_check_freshness_reads_manifest_and_reports_fresh(tmp_path: Path):
    import json
    (tmp_path / "manifest.json").write_text(json.dumps({
        "latest_trading_date": "2026-07-03", "datasets": [],
    }))
    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(),
        today=date(2026, 7, 6), runner=lambda _cmd: 0, work_dir=tmp_path,
        client=FakeReleaseClient(),
    )
    assert rc == 0  # Fri 2026-07-03 published, today Mon -> fresh, no datasets to check


def test_cmd_check_freshness_flags_missing_release(tmp_path: Path):
    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(),
        today=date(2026, 7, 6), runner=lambda _cmd: 1, work_dir=tmp_path,
        client=FakeReleaseClient(),
    )
    assert rc == 1  # download failed -> treated as stale/missing


# -- check-freshness calendar hygiene (G2 task 8: holidays-refresh nag) --

def test_cmd_check_freshness_fails_when_holidays_need_refresh(tmp_path: Path, capsys):
    import json
    # today = Dec 1 2026 (Tuesday, a trading day); last completed trading
    # day = Mon 2026-11-30 -- manifest is otherwise perfectly fresh, and
    # "datasets": [] means every real fetched spec hits the never-published
    # grace warning (same shape as the pre-existing fresh/missing-release
    # tests above), so the ONLY thing that can fail this run is the
    # calendar-hygiene check. holidays=set() has no 2027 entry -> due.
    (tmp_path / "manifest.json").write_text(json.dumps({
        "latest_trading_date": "2026-11-30", "datasets": [],
    }))
    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(),
        today=date(2026, 12, 1), runner=lambda _cmd: 0, work_dir=tmp_path,
        client=FakeReleaseClient(),
    )
    assert rc == 1
    out = capsys.readouterr().out
    assert "holidays" in out.lower()
    assert "2027" in out  # names the year that's missing, not just "stale"


def test_cmd_check_freshness_passes_when_holidays_already_refreshed(tmp_path: Path):
    import json
    (tmp_path / "manifest.json").write_text(json.dumps({
        "latest_trading_date": "2026-11-30", "datasets": [],
    }))
    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays={date(2027, 1, 26)},
        today=date(2026, 12, 1), runner=lambda _cmd: 0, work_dir=tmp_path,
        client=FakeReleaseClient(),
    )
    assert rc == 0  # 2027 already present -> no nag, despite today >= Dec 1


def test_cmd_check_freshness_ignores_holiday_refresh_before_dec_1(tmp_path: Path):
    import json
    # Nov 30 2026 (Monday, a trading day); last completed trading day is the
    # prior Friday 2026-11-27. Still no 2027 holiday present, but it's one
    # day before the Dec-1 boundary -- must not fail on calendar hygiene yet.
    (tmp_path / "manifest.json").write_text(json.dumps({
        "latest_trading_date": "2026-11-27", "datasets": [],
    }))
    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(),
        today=date(2026, 11, 30), runner=lambda _cmd: 0, work_dir=tmp_path,
        client=FakeReleaseClient(),
    )
    assert rc == 0


def test_parser_daily_dataset_choices():
    args = cli.build_parser().parse_args(["daily", "--dataset", "equities"])
    assert args.dataset == "equities"
    assert cli.build_parser().parse_args(["daily"]).dataset == "all"
    import pytest
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["daily", "--dataset", "bogus"])


def test_main_daily_runs_all_registered_specs(monkeypatch, tmp_path):
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    seen = []

    def fake_run_daily(spec, target, **kw):
        seen.append(spec.key)
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    def fake_builder(spec, target):
        return RunStatus("success", target, symbol_count=0, source="derived")

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    # This uses the real (unpatched) registry, so DATASET_ORDER includes the
    # real "reference" derived spec -- with the primary healthy, main()
    # reaches the Phase 2 derived-builder loop for real. Fake the builder
    # too, else it would call the real build_reference() bound at cli.py
    # import time and write to the real config.REFERENCE_DIR.
    monkeypatch.setattr(cli.builders, "BUILDERS", {"reference": fake_builder})
    assert cli.main(["daily", "--date", "2026-07-03"]) == 0
    # G2 Task 4: run_daily is now called once per catch-up-window day per
    # spec (not once per spec) -- dedupe to assert WHICH specs were run, in
    # first-seen order, same intent as before the catch-up loop existed.
    assert list(dict.fromkeys(seen)) == ["equities", "indices"]  # DATASET_ORDER (G1b task 3)
    assert (tmp_path / "last_run_status.json").exists()


def _fake_registry(monkeypatch, tmp_path, *, extra_derived=True):
    """Install a fake registry: equities (primary), indices (secondary
    fetched), and optionally a derived spec 'reference' whose make_fetcher
    raises if ever called -- proves the fetch loop skips it.

    Every spec's base_dir is scoped under tmp_path (via dataclasses.replace
    for the real EQUITIES/INDICES specs, and directly for the fake derived
    spec) so no test can ever write into the real pipeline/data/ tree, even
    if run_daily/backfill/builders stop being faked in the future."""

    from pipeline import datasets

    def _raiser():
        raise RuntimeError("derived dataset has no fetcher -- must never be called")

    equities = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "ohlc")
    indices = dataclasses.replace(datasets.INDICES, base_dir=tmp_path / "indices")
    derived_spec = datasets.DatasetSpec(
        key="reference", file_prefix="reference", base_dir=tmp_path / "reference",
        source_label="derived", normalizer=lambda df: df, make_fetcher=_raiser,
        abs_rowcount_range=(0, 10**9), manifest_name="reference", schema_version=1,
        derived=True,
    )
    registry = {"equities": equities, "indices": indices}
    order = ["equities", "indices"]
    if extra_derived:
        registry["reference"] = derived_spec
        order = [*order, "reference"]
    monkeypatch.setattr(cli.datasets, "DATASETS", registry)
    monkeypatch.setattr(cli.datasets, "DATASET_ORDER", order)
    return derived_spec


def test_daily_fetch_loop_skips_derived_specs(monkeypatch, tmp_path):
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path)
    seen = []

    def fake_run_daily(spec, target, **kw):
        seen.append(spec.key)
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    # BUILDERS stays empty -> the derived spec still gets a (failed, "missing
    # builder") secondary status, but its make_fetcher must never be invoked.
    monkeypatch.setattr(cli.builders, "BUILDERS", {})
    cli.main(["daily", "--date", "2026-07-03"])
    # G2 Task 4: run_daily is now called once per catch-up-window day per
    # spec -- dedupe to assert WHICH specs were run (reference/derived must
    # never appear), same intent as before the catch-up loop existed.
    assert list(dict.fromkeys(seen)) == ["equities", "indices"]  # reference (derived) never fetched


def test_daily_phase2_skips_external_specs(monkeypatch, tmp_path):
    # An external spec (fundamentals: produced by the Rust producer, no
    # BUILDERS entry) must never run through the Phase-2 builder loop -- a
    # derived-but-external spec would otherwise get a spurious `failed`
    # secondary status every night (data-daily reds on those).
    import json
    from datetime import date

    from pipeline import config, datasets
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))

    equities = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "ohlc")
    external_spec = dataclasses.replace(
        datasets.FUNDAMENTALS, base_dir=tmp_path / "fundamentals"
    )
    monkeypatch.setattr(
        cli.datasets, "DATASETS", {"equities": equities, "fundamentals": external_spec}
    )
    monkeypatch.setattr(cli.datasets, "DATASET_ORDER", ["equities", "fundamentals"])

    def fake_run_daily(spec, target, **kw):
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    monkeypatch.setattr(cli.builders, "BUILDERS", {})  # no builder exists -- by design
    assert cli.main(["daily", "--date", "2026-07-03"]) == 0
    # No secondary status was written for the external spec: the loop skipped
    # it entirely instead of recording a "no builder registered" failure.
    assert not (tmp_path / "last_run_status_fundamentals.json").exists()


def test_daily_writes_primary_and_secondary_status_files(monkeypatch, tmp_path):
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path, extra_derived=False)

    def fake_run_daily(spec, target, **kw):
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    rc = cli.main(["daily", "--date", "2026-07-03"])
    assert rc == 0
    assert (tmp_path / "last_run_status.json").exists()
    assert (tmp_path / "last_run_status_indices.json").exists()
    assert not (tmp_path / "last_run_status_equities.json").exists()


def test_daily_dataset_indices_writes_only_secondary_status_file(monkeypatch, tmp_path):
    """This FIXES the T3 clobber gap: a lone `--dataset indices` run must not
    touch last_run_status.json (the primary/publish-gate file)."""
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path, extra_derived=False)

    def fake_run_daily(spec, target, **kw):
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    rc = cli.main(["daily", "--date", "2026-07-03", "--dataset", "indices"])
    assert rc == 0
    assert (tmp_path / "last_run_status_indices.json").exists()
    assert not (tmp_path / "last_run_status.json").exists()


def test_daily_secondary_failure_exits_0_when_primary_succeeds(monkeypatch, tmp_path):
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path, extra_derived=False)

    def fake_run_daily(spec, target, **kw):
        if spec.key == "equities":
            return RunStatus("success", date(2026, 7, 3), source=spec.source_label)
        return RunStatus("failed", date(2026, 7, 3), message="boom")

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    rc = cli.main(["daily", "--date", "2026-07-03"])
    assert rc == 0
    assert (tmp_path / "last_run_status_indices.json").exists()


def test_daily_primary_failure_exits_1(monkeypatch, tmp_path):
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path, extra_derived=False)

    def fake_run_daily(spec, target, **kw):
        if spec.key == "equities":
            return RunStatus("failed", date(2026, 7, 3), message="boom")
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    rc = cli.main(["daily", "--date", "2026-07-03"])
    assert rc == 1


def test_daily_dataset_derived_key_errors_out(monkeypatch, tmp_path, capsys):
    import json

    from pipeline import config

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path)  # registers "reference" as derived
    # argparse choices must accept "reference" (registered key) but main()
    # rejects running it alone.
    rc = cli.main(["daily", "--date", "2026-07-03", "--dataset", "reference"])
    assert rc == 2
    captured = capsys.readouterr()
    assert "derived datasets build automatically" in captured.out + captured.err


def test_daily_derived_builder_runs_only_when_all_and_primary_ok(monkeypatch, tmp_path):
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path)
    calls = []

    def fake_run_daily(spec, target, **kw):
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    def fake_builder(spec, target):
        calls.append(spec.key)
        return RunStatus("success", target, symbol_count=5, source="derived")

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    monkeypatch.setattr(cli.builders, "BUILDERS", {"reference": fake_builder})
    rc = cli.main(["daily", "--date", "2026-07-03"])
    assert rc == 0
    assert calls == ["reference"]
    assert (tmp_path / "last_run_status_reference.json").exists()


def test_daily_derived_builder_skipped_when_primary_fails(monkeypatch, tmp_path):
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path)
    calls = []

    def fake_run_daily(spec, target, **kw):
        if spec.key == "equities":
            return RunStatus("failed", date(2026, 7, 3), message="boom")
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    def fake_builder(spec, target):
        calls.append(spec.key)
        return RunStatus("success", target, symbol_count=5, source="derived")

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    monkeypatch.setattr(cli.builders, "BUILDERS", {"reference": fake_builder})
    rc = cli.main(["daily", "--date", "2026-07-03"])
    assert rc == 1
    assert calls == []  # derived builder never runs when primary failed


def test_daily_derived_builder_exception_maps_to_failed_status(monkeypatch, tmp_path):
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path)

    def fake_run_daily(spec, target, **kw):
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    def boom_builder(spec, target):
        raise ValueError("builder exploded")

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    monkeypatch.setattr(cli.builders, "BUILDERS", {"reference": boom_builder})
    rc = cli.main(["daily", "--date", "2026-07-03"])
    # secondary (derived) failure never fails the run when primary succeeded
    assert rc == 0
    import json as _json
    status = _json.loads((tmp_path / "last_run_status_reference.json").read_text())
    assert status["status"] == "failed"


def test_daily_derived_missing_builder_entry_is_failed_status(monkeypatch, tmp_path):
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path)

    def fake_run_daily(spec, target, **kw):
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    monkeypatch.setattr(cli.builders, "BUILDERS", {})  # no entry for "reference"
    rc = cli.main(["daily", "--date", "2026-07-03"])
    assert rc == 0
    status = json.loads((tmp_path / "last_run_status_reference.json").read_text())
    assert status["status"] == "failed"


def test_daily_derived_builder_runs_when_primary_idempotent_skip(monkeypatch, tmp_path):
    """A `skipped_idempotent` primary status is in the "ok" set -- the
    derived builder must still fire (T5 prerequisite carried into T6)."""
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path)
    calls = []

    def fake_run_daily(spec, target, **kw):
        if spec.key == "equities":
            return RunStatus("skipped_idempotent", date(2026, 7, 3), message="already present")
        return RunStatus("success", date(2026, 7, 3), source=spec.source_label)

    def fake_builder(spec, target):
        calls.append(spec.key)
        return RunStatus("success", target, symbol_count=5, source="derived")

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    monkeypatch.setattr(cli.builders, "BUILDERS", {"reference": fake_builder})
    rc = cli.main(["daily", "--date", "2026-07-03"])
    assert rc == 0
    assert calls == ["reference"]
    status = json.loads((tmp_path / "last_run_status_reference.json").read_text())
    assert status["status"] == "success"


def test_daily_lone_secondary_failure_exits_1(monkeypatch, tmp_path):
    """`--dataset indices` alone, with a failing indices run, must exit 1 --
    its own status drives the exit code since the primary never ran (T5
    prerequisite carried into T6)."""
    import json
    from datetime import date

    from pipeline import config
    from pipeline.daily_update import RunStatus

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    _fake_registry(monkeypatch, tmp_path, extra_derived=False)

    def fake_run_daily(spec, target, **kw):
        return RunStatus("failed", date(2026, 7, 3), message="boom")

    monkeypatch.setattr(cli, "run_daily", fake_run_daily)
    rc = cli.main(["daily", "--date", "2026-07-03", "--dataset", "indices"])
    assert rc == 1
    assert (tmp_path / "last_run_status_indices.json").exists()
    assert not (tmp_path / "last_run_status.json").exists()


# --- G2 Task 4: catch-up loop -----------------------------------------------
#
# These tests exercise the REAL `run_daily` (never faked) against a real
# tmp-scoped equities store, driven by a `RecordingFetcher` stub that logs
# every date it was asked to fetch and serves either a valid one-day frame or
# a raised exception per date. This is the only way to prove the CLI's window
# loop (`cal.trading_days_back` -> per-day `run_daily`) actually behaves as
# specified, as opposed to the `fake_run_daily(spec, target, **kw)` used by
# every other CLI test above (which bypasses the window loop entirely).

_CATCHUP_HOLIDAYS: set[date] = {date(2026, 8, 15)}


def _one_day_raw(d: date) -> pd.DataFrame:
    """A minimal, valid UDiFF-shaped raw frame for exactly one date -- enough
    to pass the wrong-date guard, quarantine, and schema gates."""
    return pd.DataFrame([{
        "TradDt": d.isoformat(), "FinInstrmTp": "STK", "ISIN": "INE002A01018",
        "TckrSymb": "RELIANCE", "SctySrs": "EQ", "SsnId": "F1",
        "OpnPric": 100, "HghPric": 101, "LwPric": 99, "ClsPric": 100,
        "PrvsClsgPric": 100, "TtlTradgVol": 1000, "TtlTrfVal": 100000,
        "TtlNbOfTxsExctd": 10,
    }])


def _one_day_indices_raw(d: date) -> pd.DataFrame:
    """A minimal, valid NSE-indices-shaped raw frame for exactly one date --
    mirrors `tests/test_normalize_indices.py`'s `_raw()` shape (same required
    columns/dtypes), just for a single index row, parameterized by date so
    `RecordingFetcher(raw_fn=_one_day_indices_raw)` can serve the indices
    (secondary dataset) spec's own catch-up window."""
    return pd.DataFrame([{
        "Index Name": "Nifty 50", "Index Date": d.strftime("%d-%m-%Y"),
        "Open Index Value": 24500.10, "High Index Value": 24700.55,
        "Low Index Value": 24450.00, "Closing Index Value": 24650.25,
        "Points Change": 150.15, "Volume": 350000000.0,
        "Turnover (Rs. Cr.)": 45000.50,
    }])


class RecordingFetcher:
    """Logs every requested date; serves `_one_day_raw(d)` by default, or
    raises `exceptions[d]` when that date has an override. A single instance
    is reused across an entire window's days (proving the fetcher-reuse
    contract) by having `make_fetcher` be a closure returning this same
    object every time it's invoked, while a separate counter proves
    `make_fetcher` itself is called exactly once per spec per CLI run.

    `raw_fn`/`source` are optional overrides (default: the pre-existing
    UDiFF-shaped `_one_day_raw`/"nse-udiff") so the same stub can also serve
    a non-equities spec (e.g. indices) whose normalizer expects a different
    raw column shape -- see `_one_day_indices_raw`."""

    def __init__(
        self, exceptions: dict[date, Exception] | None = None, *,
        raw_fn: Callable[[date], pd.DataFrame] = _one_day_raw,
        source: str = "nse-udiff",
    ):
        self.requested: list[date] = []
        self._exceptions = exceptions or {}
        self._raw_fn = raw_fn
        self._source = source

    def fetch_raw(self, d: date) -> FetchResult:
        self.requested.append(d)
        if d in self._exceptions:
            raise self._exceptions[d]
        return FetchResult(self._raw_fn(d), self._source)


def _catchup_registry(monkeypatch, tmp_path: Path, fetcher: RecordingFetcher):
    """A registry of exactly one fetched spec ('equities'), tmp-scoped, real
    normalizer, a permissive rowcount range (single-symbol frames are far
    below the real (2000, 10000) floor), and `make_fetcher` a counting
    closure over the single shared `fetcher` instance (fetcher-reuse
    contract: cli.py must call `spec.make_fetcher()` once per spec, not once
    per window-day)."""
    make_fetcher_calls = {"n": 0}

    def _make_fetcher():
        make_fetcher_calls["n"] += 1
        return fetcher

    equities = dataclasses.replace(
        datasets.EQUITIES, base_dir=tmp_path / "ohlc",
        abs_rowcount_range=(0, 10**9), make_fetcher=_make_fetcher,
    )
    monkeypatch.setattr(cli.datasets, "DATASETS", {"equities": equities})
    monkeypatch.setattr(cli.datasets, "DATASET_ORDER", ["equities"])
    return make_fetcher_calls


def test_catchup_window_fetches_only_the_missing_middle_day(monkeypatch, tmp_path):
    """A 3-day window where the middle day is missing from the store: the CLI
    must fetch+append exactly that missing day, treat the other two (already
    present) days as idempotent skips, still write the target's status, and
    exit 0."""
    import json

    from pipeline import config, store

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    fetcher = RecordingFetcher()
    make_fetcher_calls = _catchup_registry(monkeypatch, tmp_path, fetcher)

    equities = cli.datasets.DATASETS["equities"]
    target = date(2026, 7, 6)  # a Monday -- deliberately not a Friday
    window = cal.trading_days_back(target, config.CATCHUP_WINDOW_DAYS, set())
    assert window[-1] == target  # sanity: target is the last (ascending) element
    missing_day = window[len(window) // 2]

    # Pre-seed every window day EXCEPT missing_day, each via a real run_daily
    # (so has_day/idempotency sees genuine stored rows, not a hand-poked file).
    from pipeline.daily_update import run_daily
    for d in window:
        if d == missing_day:
            continue
        seed_fetcher = RecordingFetcher()
        st = run_daily(equities, d, fetcher=seed_fetcher, holidays=set())
        assert st.status == "success"
    fetcher.requested.clear()  # only care about requests made by the CLI run itself

    rc = cli.main(["daily", "--date", target.isoformat()])
    assert rc == 0
    assert fetcher.requested == [missing_day]  # exactly the missing day was fetched
    assert make_fetcher_calls["n"] == 1  # ONE make_fetcher() call, reused for the window
    assert store.has_day(equities.base_dir, missing_day)
    assert (tmp_path / "last_run_status.json").exists()
    status = json.loads((tmp_path / "last_run_status.json").read_text())
    # Target itself was pre-seeded above (part of "every window day except
    # missing_day") -- so its own status from THIS run is the ordinary
    # idempotent-rerun outcome, exactly as it would be without any hole in
    # the window at all. This is still the TARGET day's status driving the
    # status file/exit code (contract item 3), just not "success" specifically.
    assert status["status"] == "skipped_idempotent"
    assert status["date"] == target.isoformat()  # status file reflects the TARGET day


def test_daily_catchup_window_shares_one_cache_across_the_window(monkeypatch, tmp_path):
    """G3 Task 2: the CLI's per-spec catch-up-window loop constructs exactly
    ONE `store.ReadCache()` per spec per `daily` invocation and reuses it
    across every day in that spec's window -- not a fresh cache per day.
    Reuses the EXACT registry/fetcher/monkeypatch setup from
    `test_catchup_window_fetches_only_the_missing_middle_day` above, adding
    only a spy on `store._read_year` (same wrapper pattern as
    `test_backfill.py`'s `test_backfill_reuses_one_cache_across_the_whole_run`)
    to record every `cache` argument passed through."""
    import json

    from pipeline import config, store

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    fetcher = RecordingFetcher()
    _catchup_registry(monkeypatch, tmp_path, fetcher)

    seen_caches: list[object] = []
    real_read_year = store._read_year

    def spying_read_year(base, year, prefix="ohlc", *, columns=None, cache=None):
        seen_caches.append(cache)
        return real_read_year(base, year, prefix, columns=columns, cache=cache)

    monkeypatch.setattr(store, "_read_year", spying_read_year)

    target = date(2026, 7, 6)  # a Monday -- deliberately not a Friday
    rc = cli.main(["daily", "--date", target.isoformat()])
    assert rc == 0

    non_none = [c for c in seen_caches if c is not None]
    assert non_none, "expected at least one cache-bearing _read_year call"
    assert len({id(c) for c in non_none}) == 1  # every call shared the SAME cache instance


def test_catchup_past_day_404_exits_0_and_writes_window_failures_marker(
    monkeypatch, tmp_path
):
    """G2 final-review fix (C1): a past (non-target) day in the window that
    404s is still reported as 'failed' (a hole, not lateness) and the hole
    stays a hole -- but it must NO LONGER force the run's exit code to 1 when
    the TARGET day itself succeeds. A permanent past-day archive hole was
    previously indistinguishable, at the gate level, from the TARGET day
    itself being unhealthy -- which made data-daily.yml's un-guarded 'Ingest'
    step fail the whole job and skip 'Decide'/'Publish' even though the
    target was fine. The alert signal survives instead as a persisted
    `data/meta/window_failures.json` marker (checked by a dedicated
    after-publish workflow step), decoupled from the publish gate itself."""
    import json

    from pipeline import config, store

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))

    target = date(2026, 7, 6)
    window = cal.trading_days_back(target, config.CATCHUP_WINDOW_DAYS, set())
    past_day = window[0]  # the oldest day in the window, strictly before target
    assert past_day != target

    fetcher = RecordingFetcher(exceptions={past_day: NotYetPublished("404")})
    _catchup_registry(monkeypatch, tmp_path, fetcher)
    equities = cli.datasets.DATASETS["equities"]

    rc = cli.main(["daily", "--date", target.isoformat()])
    assert rc == 0  # healthy target always publishes -- window failure is alert-only
    assert store.has_day(equities.base_dir, target)  # target day still ingested
    assert not store.has_day(equities.base_dir, past_day)  # the hole stays a hole
    status = json.loads((tmp_path / "last_run_status.json").read_text())
    assert status["status"] == "success"  # status FILE carries the TARGET day's status
    assert status["date"] == target.isoformat()

    marker_path = tmp_path / "window_failures.json"
    assert marker_path.exists()
    marker = json.loads(marker_path.read_text())
    failures = marker["failures"]
    assert len(failures) == 1
    assert failures[0]["dataset"] == "equities"
    assert failures[0]["date"] == past_day.isoformat()
    assert "archive missing" in failures[0]["message"]


def test_daily_clean_run_writes_no_window_failures_marker(monkeypatch, tmp_path):
    """A clean run (no window-day failures at all) must not write
    `window_failures.json` -- the marker is a signal-when-present file, not an
    always-written status file (contrast with last_run_status.json)."""
    import json

    from pipeline import config

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))

    target = date(2026, 7, 6)
    fetcher = RecordingFetcher()  # no exceptions -- every window day succeeds
    _catchup_registry(monkeypatch, tmp_path, fetcher)

    rc = cli.main(["daily", "--date", target.isoformat()])
    assert rc == 0
    assert not (tmp_path / "window_failures.json").exists()


def test_daily_clean_run_removes_stale_window_failures_marker(monkeypatch, tmp_path):
    """A clean run must not inherit yesterday's marker: a stale
    `window_failures.json` pre-seeded from a PRIOR run (with past failures)
    must be removed at the start of a run that has no failures of its own --
    otherwise a resolved hole would keep re-alerting forever via a leftover
    file nothing ever cleans up."""
    import json

    from pipeline import config

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    stale_marker = tmp_path / "window_failures.json"
    stale_marker.write_text(json.dumps({
        "failures": [{"dataset": "equities", "date": "2026-06-20", "message": "stale"}]
    }))

    target = date(2026, 7, 6)
    fetcher = RecordingFetcher()  # clean run this time -- no exceptions
    _catchup_registry(monkeypatch, tmp_path, fetcher)

    rc = cli.main(["daily", "--date", target.isoformat()])
    assert rc == 0
    assert not stale_marker.exists()  # must not inherit the previous run's marker


def test_daily_primary_target_failure_exits_1_even_with_window_failure_marker(
    monkeypatch, tmp_path
):
    """Primary TARGET failure still exits 1 exactly as today -- decoupling
    window-failure alerting from the publish gate must never weaken the ONE
    signal that legitimately should still block publish: the target day
    itself being unhealthy. This holds even when a window_failures.json
    marker also gets written for an unrelated past-day hole in the same run."""
    import json

    from pipeline import config

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))

    target = date(2026, 7, 6)
    window = cal.trading_days_back(target, config.CATCHUP_WINDOW_DAYS, set())
    past_day = window[0]
    assert past_day != target

    # target itself 404s (UnexpectedFailure, not NotYetPublished, to force a
    # hard "failed" on the target day regardless of is_target_day handling)
    from pipeline.errors import UnexpectedFailure
    fetcher = RecordingFetcher(exceptions={
        past_day: NotYetPublished("404"),
        target: UnexpectedFailure("primary target source exploded"),
    })
    _catchup_registry(monkeypatch, tmp_path, fetcher)

    rc = cli.main(["daily", "--date", target.isoformat()])
    assert rc == 1  # primary TARGET failure still exits 1, unchanged
    status = json.loads((tmp_path / "last_run_status.json").read_text())
    assert status["status"] == "failed"
    assert status["date"] == target.isoformat()
    # the past-day hole is still recorded as an alert-only side channel
    marker = json.loads((tmp_path / "window_failures.json").read_text())
    assert any(f["date"] == past_day.isoformat() for f in marker["failures"])


def _two_spec_catchup_registry(
    monkeypatch, tmp_path: Path,
    primary_fetcher: "RecordingFetcher", secondary_fetcher: "RecordingFetcher",
):
    """Real two-spec registry (equities=primary, indices=secondary) for
    exercising the window/catch-up loop against BOTH specs at once, tmp-scoped
    exactly like `_catchup_registry` but with independent fetchers per spec so
    a secondary-only window failure can be produced without touching the
    primary's own fetch results."""
    def _make_primary():
        return primary_fetcher

    def _make_secondary():
        return secondary_fetcher

    equities = dataclasses.replace(
        datasets.EQUITIES, base_dir=tmp_path / "ohlc",
        abs_rowcount_range=(0, 10**9), make_fetcher=_make_primary,
    )
    indices = dataclasses.replace(
        datasets.INDICES, base_dir=tmp_path / "indices",
        abs_rowcount_range=(0, 10**9), make_fetcher=_make_secondary,
    )
    monkeypatch.setattr(cli.datasets, "DATASETS", {"equities": equities, "indices": indices})
    monkeypatch.setattr(cli.datasets, "DATASET_ORDER", ["equities", "indices"])


def test_daily_secondary_window_failure_exits_0_and_writes_marker(monkeypatch, tmp_path):
    """A window (non-target-day) failure in the SECONDARY dataset's own
    catch-up loop -- not just the primary's -- must also (a) never affect the
    exit code when the primary target is healthy, and (b) still be persisted
    to window_failures.json so it isn't silently dropped. Window failures are
    alert-worthy for BOTH primary and secondary datasets per the fix spec."""
    import json

    from pipeline import config

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))

    target = date(2026, 7, 6)
    window = cal.trading_days_back(target, config.CATCHUP_WINDOW_DAYS, set())
    past_day = window[0]
    assert past_day != target

    primary_fetcher = RecordingFetcher()  # equities: fully clean
    secondary_fetcher = RecordingFetcher(
        exceptions={past_day: NotYetPublished("404")},
        raw_fn=_one_day_indices_raw, source="nse-indices",
    )
    _two_spec_catchup_registry(monkeypatch, tmp_path, primary_fetcher, secondary_fetcher)

    rc = cli.main(["daily", "--date", target.isoformat()])
    assert rc == 0  # secondary window failure never blocks publish when primary is healthy
    marker = json.loads((tmp_path / "window_failures.json").read_text())
    failures = marker["failures"]
    assert len(failures) == 1
    assert failures[0]["dataset"] == "indices"
    assert failures[0]["date"] == past_day.isoformat()


def test_catchup_past_day_failure_is_printed(monkeypatch, tmp_path, capsys):
    """The non-target-day failure must be printed clearly, not just silently
    folded into the exit code."""
    import json

    from pipeline import config

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))

    target = date(2026, 7, 6)
    window = cal.trading_days_back(target, config.CATCHUP_WINDOW_DAYS, set())
    past_day = window[0]

    fetcher = RecordingFetcher(exceptions={past_day: NotYetPublished("404")})
    _catchup_registry(monkeypatch, tmp_path, fetcher)

    cli.main(["daily", "--date", target.isoformat()])
    captured = capsys.readouterr()
    out = captured.out + captured.err
    assert past_day.isoformat() in out
    assert "failed" in out


def test_catchup_target_day_404_is_not_yet_not_failed(monkeypatch, tmp_path):
    """A 404 on the TARGET day itself is ordinary lateness (unchanged
    semantics) -- `not_yet`, exit 0 -- never 'failed'."""
    import json

    from pipeline import config

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))

    target = date(2026, 7, 6)
    fetcher = RecordingFetcher(exceptions={target: NotYetPublished("404")})
    _catchup_registry(monkeypatch, tmp_path, fetcher)

    rc = cli.main(["daily", "--date", target.isoformat()])
    assert rc == 0
    status = json.loads((tmp_path / "last_run_status.json").read_text())
    assert status["status"] == "not_yet"


def test_catchup_window_skips_holiday_inside_window(monkeypatch, tmp_path):
    """The window must be computed via the real trading calendar -- a holiday
    that falls inside the naive 7-calendar-day span must never be requested
    (trading_days_back itself is already unit-tested; this is the one
    integration assertion that the CLI actually threads holidays through)."""
    import json

    from pipeline import config

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    holiday = date(2026, 7, 2)  # a Thursday inside the window ending 2026-07-06
    (tmp_path / "holidays.json").write_text(json.dumps({"2026": [holiday.isoformat()]}))

    target = date(2026, 7, 6)
    fetcher = RecordingFetcher()
    _catchup_registry(monkeypatch, tmp_path, fetcher)

    rc = cli.main(["daily", "--date", target.isoformat()])
    assert rc == 0
    assert holiday not in fetcher.requested


def test_daily_holiday_target_reports_that_days_skip_not_window_fallback(
    monkeypatch, tmp_path
):
    """Regression guard for the `if not cal.is_trading_day(target, ...):
    window = [target]` branch in cli.py's daily loop. `target` here (Sunday
    2026-07-05, empty holidays) is NOT a trading day, so `trading_days_back`
    would silently substitute the PREVIOUS trading day (Friday 2026-07-03) as
    the window's last element if the guard were absent/deleted -- the status
    FILE would then wrongly report Friday's outcome (a fetch attempt, not a
    holiday-skip) under a run the operator asked for on Sunday, and the
    RecordingFetcher would see a real network request that must never happen
    for a non-trading day.

    Asserts, precisely because this is what the guard branch (and only that
    branch) guarantees:
      1. the primary status FILE's `date` is the TARGET (2026-07-05), not
         Friday's date -- proving the window was NOT silently substituted;
      2. `status == "skipped_holiday"` -- the pre-existing single-day
         behavior for a non-trading-day target, preserved;
      3. exit code 0;
      4. `fetcher.requested == []` -- zero network calls, i.e. the window
         mechanism never ran at all for a non-trading-day target.
    """
    import json

    from pipeline import config

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))  # empty holidays

    target = date(2026, 7, 5)  # a Sunday -- not a trading day, no holiday entry needed
    fetcher = RecordingFetcher()
    _catchup_registry(monkeypatch, tmp_path, fetcher)

    rc = cli.main(["daily", "--date", target.isoformat()])

    assert rc == 0
    assert fetcher.requested == []  # no network: the guard short-circuits before any fetch
    status = json.loads((tmp_path / "last_run_status.json").read_text())
    assert status["status"] == "skipped_holiday"
    assert status["date"] == target.isoformat()  # target's OWN date, not Friday's


# --- G2 Task 6: weekly source cross-check -----------------------------------
#
# `cmd_cross_check` is driven by two injected raw-fetch callables (both
# returning UDiFF-raw-shape frames -- the secondary side's stub stands in for
# the real production wiring's secfull-fetch + shape-adapt-with-isin_map
# composition), so these tests never touch the network and never construct a
# real NseUdiffFetcher/secfull session.

def _cross_check_raw(rows: list[tuple[str, str, float]], d: date) -> pd.DataFrame:
    """A minimal, valid UDiFF-raw-shaped multi-symbol frame for date `d`:
    rows are (isin, symbol, close) triples."""
    return pd.DataFrame([
        {
            "TradDt": d.isoformat(), "FinInstrmTp": "STK", "ISIN": isin,
            "TckrSymb": symbol, "SctySrs": "EQ", "SsnId": "F1",
            "OpnPric": close, "HghPric": close, "LwPric": close, "ClsPric": close,
            "PrvsClsgPric": close, "TtlTradgVol": 1000, "TtlTrfVal": 100000,
            "TtlNbOfTxsExctd": 10,
        }
        for isin, symbol, close in rows
    ])


_CROSS_CHECK_ROWS: list[tuple[str, str, float]] = [
    ("INE001A01001", "AAA", 100.0),
    ("INE002A01002", "BBB", 200.0),
    ("INE003A01003", "CCC", 300.0),
]


def test_parser_has_cross_check():
    args = cli.build_parser().parse_args(["cross-check"])
    assert args.cmd == "cross-check"
    assert args.date is None


def test_parser_cross_check_reads_date():
    args = cli.build_parser().parse_args(["cross-check", "--date", "2026-07-03"])
    assert args.date == "2026-07-03"


def test_cmd_cross_check_sources_agree_exits_0(capsys):
    target = date(2026, 7, 3)

    def fetch_primary(d: date) -> pd.DataFrame:
        return _cross_check_raw(_CROSS_CHECK_ROWS, d)

    def fetch_secondary(d: date) -> pd.DataFrame:
        return _cross_check_raw(_CROSS_CHECK_ROWS, d)

    rc = cli.cmd_cross_check(
        target, fetch_primary_raw=fetch_primary, fetch_secondary_raw=fetch_secondary,
    )
    assert rc == 0
    out = capsys.readouterr().out
    # explicit success marker, distinguishing "compared and agreed" from a
    # divergence purely by prefix text, not by inferring it from exit code
    # or channel alone
    assert "cross-check: OK compared=3 mismatched=0" in out


def test_cmd_cross_check_divergence_exits_1_and_prints_table(capsys):
    target = date(2026, 7, 3)
    diverging_rows = [
        ("INE001A01001", "AAA", 100.0),
        ("INE002A01002", "BBB", 400.0),  # 100% divergence vs primary's 200.0
        ("INE003A01003", "CCC", 300.0),
    ]

    def fetch_primary(d: date) -> pd.DataFrame:
        return _cross_check_raw(_CROSS_CHECK_ROWS, d)

    def fetch_secondary(d: date) -> pd.DataFrame:
        return _cross_check_raw(diverging_rows, d)

    rc = cli.cmd_cross_check(
        target, fetch_primary_raw=fetch_primary, fetch_secondary_raw=fetch_secondary,
    )
    assert rc == 1
    out = capsys.readouterr().out
    assert "INE002A01002" in out  # the diverging key appears in the printed table
    assert "200" in out and "400" in out
    # explicit divergence marker, distinguishing this from an OK/CANNOT-RUN
    # outcome purely by prefix text
    assert "cross-check: DIVERGENCE compared=3 mismatched=1" in out


def test_cmd_cross_check_one_source_down_exits_1_with_clear_message(capsys):
    target = date(2026, 7, 3)

    def fetch_primary(d: date) -> pd.DataFrame:
        return _cross_check_raw(_CROSS_CHECK_ROWS, d)

    def fetch_secondary(d: date) -> pd.DataFrame:
        raise RuntimeError("secfull endpoint unreachable")

    rc = cli.cmd_cross_check(
        target, fetch_primary_raw=fetch_primary, fetch_secondary_raw=fetch_secondary,
    )
    assert rc == 1
    err = capsys.readouterr().err
    # explicit can't-run marker, distinguishing this from a DIVERGENCE outcome
    # by more than channel (stderr) + wording alone
    assert "cross-check: CANNOT-RUN —" in err
    assert "secfull endpoint unreachable" in err


def test_cmd_cross_check_primary_source_down_exits_1_with_clear_message(capsys):
    target = date(2026, 7, 3)

    def fetch_primary(d: date) -> pd.DataFrame:
        raise RuntimeError("udiff endpoint unreachable")

    def fetch_secondary(d: date) -> pd.DataFrame:
        return _cross_check_raw(_CROSS_CHECK_ROWS, d)

    rc = cli.cmd_cross_check(
        target, fetch_primary_raw=fetch_primary, fetch_secondary_raw=fetch_secondary,
    )
    assert rc == 1
    err = capsys.readouterr().err
    # explicit can't-run marker, distinguishing this from a DIVERGENCE outcome
    # by more than channel (stderr) + wording alone
    assert "cross-check: CANNOT-RUN —" in err
    assert "udiff endpoint unreachable" in err


def test_main_cross_check_defaults_date_to_previous_trading_day(monkeypatch, tmp_path):
    """`main(["cross-check"])` with no --date must resolve to the previous
    trading day (via the real calendar), not today."""
    import json

    from pipeline import config

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    seen_dates = []

    def fake_cmd_cross_check(target, **kw):
        seen_dates.append(target)
        return 0

    monkeypatch.setattr(cli, "cmd_cross_check", fake_cmd_cross_check)
    monkeypatch.setattr(cli, "_today_for_cli", lambda: date(2026, 7, 6))  # a Monday
    rc = cli.main(["cross-check"])
    assert rc == 0
    assert seen_dates == [date(2026, 7, 3)]  # previous trading day (Friday)


def test_main_cross_check_explicit_date_bypasses_default(monkeypatch, tmp_path):
    import json

    from pipeline import config

    monkeypatch.setattr(config, "META_DIR", tmp_path)
    (tmp_path / "holidays.json").write_text(json.dumps({}))
    seen_dates = []

    def fake_cmd_cross_check(target, **kw):
        seen_dates.append(target)
        return 0

    monkeypatch.setattr(cli, "cmd_cross_check", fake_cmd_cross_check)
    rc = cli.main(["cross-check", "--date", "2026-07-01"])
    assert rc == 0
    assert seen_dates == [date(2026, 7, 1)]


# --- G2 Task 6 fast-follow: wiring-layer isolation regression -----------------
#
# Every test above drives `cmd_cross_check` directly with injected stub
# callables -- that proves the COMPARISON logic is correct, but never
# exercises `_cross_check_fetch_primary_raw`/`_cross_check_fetch_secondary_raw`
# themselves, which is where the feature's actual load-bearing property lives:
# the primary fetch must be constructed with NO fallback chain
# (`fallbacks=()`). If a fallback were ever wired in there, a real primary
# outage would silently fall through to the secondary -- and cross-check would
# then be comparing the secondary against itself, reporting `mismatched=0`
# ("sources agree!") while never having touched the primary at all. That
# false-agreement failure mode would be invisible to every stub-based test
# above, since the stubs bypass the wiring entirely. These two tests close
# that gap by exercising the REAL production functions.

def test_cross_check_primary_wiring_has_no_fallbacks(monkeypatch):
    """`_cross_check_fetch_primary_raw` must construct `NseUdiffFetcher` with
    `fallbacks=()` -- an empty tuple, not merely "falsy" or omitted-with-some-
    default-that-happens-to-be-empty. Proven by monkeypatching
    `cli.NseUdiffFetcher` (the name as imported/used by cli.py) with a
    recording wrapper that captures its constructor kwargs, then invoking the
    REAL `_cross_check_fetch_primary_raw`. The network is short-circuited by
    having the wrapper's `fetch_raw` raise immediately after construction --
    construction already happened by then, so the kwargs are already
    captured; what `fetch_raw` does past that point is irrelevant to this
    test and is never reached for real (no network, no real HTTP session)."""
    captured_kwargs: dict[str, object] = {}

    class _RecordingFetcher:
        def __init__(self, *args: object, **kwargs: object) -> None:
            captured_kwargs.update(kwargs)

        def fetch_raw(self, d: date) -> FetchResult:
            raise UnexpectedFailure("short-circuited: construction already recorded")

    monkeypatch.setattr(cli, "NseUdiffFetcher", _RecordingFetcher)

    import pytest
    with pytest.raises(UnexpectedFailure):
        cli._cross_check_fetch_primary_raw(date(2026, 7, 3))

    assert "fallbacks" in captured_kwargs
    assert captured_kwargs["fallbacks"] == ()


def test_cross_check_secondary_wiring_has_no_fallback_machinery(monkeypatch):
    """`_cross_check_fetch_secondary_raw` has no fallback path to disable in
    the first place: unlike the primary side, it never constructs a
    `Fetcher`-protocol object at all (no `NseUdiffFetcher`/`Fallback` tuples
    anywhere in it) -- it calls the shared `_fetch_with_retry(session, url,
    d, *, parse=...)` FUNCTION directly, which has no `fallbacks` parameter
    in its signature. There is no fallback machinery to bypass, so isolation
    holds by construction rather than by an explicit empty-tuple argument.

    Proven by monkeypatching `cli._fetch_with_retry` (the name as imported/
    used by cli.py) with a recorder, invoking the REAL
    `_cross_check_fetch_secondary_raw`, and asserting the call it actually
    received carries only the plain fetch arguments -- no `fallbacks`
    keyword, because the function it calls doesn't accept one."""
    captured_kwargs: dict[str, object] = {}

    def _recording_fetch_with_retry(session: object, url: str, d: date, *,
                                     parse: object) -> pd.DataFrame:
        captured_kwargs["session"] = session
        captured_kwargs["url"] = url
        captured_kwargs["d"] = d
        captured_kwargs["parse"] = parse
        raise UnexpectedFailure("short-circuited: call already recorded")

    monkeypatch.setattr(cli, "_fetch_with_retry", _recording_fetch_with_retry)

    import pytest
    with pytest.raises(UnexpectedFailure):
        cli._cross_check_fetch_secondary_raw(date(2026, 7, 3))

    # The call cli actually made carries exactly the plain fetch arguments --
    # no `fallbacks` (or any other fallback-shaped) keyword crossed this
    # boundary, because `_fetch_with_retry`'s signature has nowhere to put
    # one. Isolation is structural here, not an empty-tuple flag to assert.
    assert set(captured_kwargs) == {"session", "url", "d", "parse"}
    assert "fallbacks" not in captured_kwargs


# -- check-freshness continuity (G2 task 7) --
#
# SCOPE AMENDMENT (controller adjudication, supersedes the plan's
# "primary dataset only" wording): continuity covers EVERY FETCHED dataset
# (equities AND indices), not just the primary. A T4 review proved a hole
# confined to a SECONDARY dataset's catch-up window is otherwise invisible
# to every existing layer (the primary's own staleness/continuity is clean,
# so nothing else would ever surface it). Derived datasets (reference,
# ca_flags) are never continuity-checked -- they rebuild from the store, so
# a hole there is a symptom of an underlying fetched-dataset hole, not an
# independent fact to alert on.

_TODAY = date(2026, 7, 6)  # Monday; last completed trading day = Fri 2026-07-03


def _continuity_registry(monkeypatch, tmp_path: Path) -> None:
    """Routes the registry to just equities + indices (both fetched, tmp
    base_dirs) -- continuity must never touch `derived` specs, so a lone
    derived entry is deliberately NOT included here (nothing to skip in this
    fixture; test_continuity_skips_derived_datasets below proves the skip
    explicitly with one present)."""
    equities = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "ohlc")
    indices = dataclasses.replace(datasets.INDICES, base_dir=tmp_path / "indices")
    monkeypatch.setattr(cli.datasets, "DATASETS", {"equities": equities, "indices": indices})
    monkeypatch.setattr(cli.datasets, "DATASET_ORDER", ["equities", "indices"])


def _window_dates(today: date = _TODAY, holidays: set[date] | None = None,
                   window: int = 10) -> list[date]:
    holidays = holidays or set()
    return cal.trading_days_back(
        cal.previous_trading_day(today, holidays), window, holidays,
    )


def _baseline_parquet_bytes(dates: list[date]) -> bytes:
    """A minimal baseline parquet: only the `date` column matters to the
    continuity reader (column-pruned read), but a real baseline carries the
    full CANON_COLUMNS shape -- match that shape so this fixture stays
    representative of a real published asset."""
    import io as _io
    n = len(dates)
    df = pd.DataFrame({
        "date": pd.to_datetime(dates),
        **{c: [0] * n for c in config.CANON_COLUMNS if c != "date"},
    })
    buf = _io.BytesIO()
    df.to_parquet(buf, compression="zstd", index=False)
    return buf.getvalue()


def _seed_dataset_baseline(
    fake: FakeReleaseClient, manifest_datasets: list[dict], *,
    manifest_name: str, file_prefix: str, year: int, dates: list[date],
) -> None:
    """Seeds one dataset's manifest entry (v2 shape, `baseline` key) plus the
    matching content-addressed parquet asset into `fake`, and appends the
    entry dict to `manifest_datasets` (mutated in place, mirroring how
    `build_manifest` accumulates `out_datasets`)."""
    data = _baseline_parquet_bytes(dates)
    sha = hashlib.sha256(data).hexdigest()
    name = f"{file_prefix}_{year}.parquet"
    asset = asset_name(name, sha)
    fake.seed(asset, data)
    manifest_datasets.append({
        "name": manifest_name, "schema_version": 1,
        "latest_date": max(dates).isoformat(),
        "baseline": [{"name": name, "asset": asset, "sha256": sha,
                      "bytes": len(data), "rows": len(dates)}],
        "deltas": [],
    })


def _seed_manifest_and_client(
    fake: FakeReleaseClient, manifest_datasets: list[dict], *,
    work_dir: Path,
    latest_trading_date: date = date(2026, 7, 3),
) -> None:
    """Seeds the manifest into `fake` (documents what a real `gh release
    download` would fetch) AND writes it directly onto `work_dir` -- the
    fake `runner=lambda _cmd: 0` used throughout these tests is a pure rc
    stub (matching the pre-existing check-freshness tests' convention) that
    does not itself perform a download, so the test must place the file
    where the real `gh` invocation would have, exactly like the pre-existing
    `test_cmd_check_freshness_reads_manifest_and_reports_fresh` does."""
    manifest = {
        "manifest_version": 2, "generated_at": "2026-07-04T00:00:00Z",
        "latest_trading_date": latest_trading_date.isoformat(),
        "datasets": manifest_datasets,
    }
    import json as _json
    payload = _json.dumps(manifest).encode()
    fake.seed("manifest.json", payload)
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "manifest.json").write_bytes(payload)


def test_continuity_clean_both_datasets_exits_0(monkeypatch, tmp_path):
    _continuity_registry(monkeypatch, tmp_path)
    window = _window_dates()
    fake = FakeReleaseClient(exists=True)
    manifest_datasets: list[dict] = []
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="ohlc",
                            file_prefix="ohlc", year=2026, dates=window)
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="indices",
                            file_prefix="indices", year=2026, dates=window)
    _seed_manifest_and_client(fake, manifest_datasets, work_dir=tmp_path / "work")

    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(), today=_TODAY,
        runner=lambda _cmd: 0, work_dir=tmp_path / "work", client=fake,
    )
    assert rc == 0


def test_continuity_hole_in_equities_baseline_exits_1_naming_day_and_dataset(
    monkeypatch, tmp_path, capsys,
):
    _continuity_registry(monkeypatch, tmp_path)
    window = _window_dates()
    hole = window[len(window) // 2]
    equities_dates = [d for d in window if d != hole]
    fake = FakeReleaseClient(exists=True)
    manifest_datasets: list[dict] = []
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="ohlc",
                            file_prefix="ohlc", year=2026, dates=equities_dates)
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="indices",
                            file_prefix="indices", year=2026, dates=window)
    _seed_manifest_and_client(fake, manifest_datasets, work_dir=tmp_path / "work")

    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(), today=_TODAY,
        runner=lambda _cmd: 0, work_dir=tmp_path / "work", client=fake,
    )
    assert rc == 1
    out = capsys.readouterr().out
    assert hole.isoformat() in out
    assert "ohlc" in out  # names the dataset with the hole


def test_continuity_hole_in_indices_baseline_only_exits_1(monkeypatch, tmp_path, capsys):
    """THE T4 GAP CLOSED: a hole confined entirely to the SECONDARY
    (indices) dataset must alert even though the primary (equities) baseline
    is perfectly clean -- this is the exact scenario the plan's original
    "primary dataset only" wording would have missed, and is the reason for
    the scope amendment."""
    _continuity_registry(monkeypatch, tmp_path)
    window = _window_dates()
    hole = window[2]
    indices_dates = [d for d in window if d != hole]
    fake = FakeReleaseClient(exists=True)
    manifest_datasets: list[dict] = []
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="ohlc",
                            file_prefix="ohlc", year=2026, dates=window)
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="indices",
                            file_prefix="indices", year=2026, dates=indices_dates)
    _seed_manifest_and_client(fake, manifest_datasets, work_dir=tmp_path / "work")

    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(), today=_TODAY,
        runner=lambda _cmd: 0, work_dir=tmp_path / "work", client=fake,
    )
    assert rc == 1
    out = capsys.readouterr().out
    assert hole.isoformat() in out
    assert "indices" in out


def test_continuity_dataset_absent_from_manifest_warns_and_exits_0(
    monkeypatch, tmp_path, capsys,
):
    """Grace rule (pre-first-publish state): a fetched dataset that has
    never appeared in the manifest yet is reported as a WARNING, not a
    hard failure -- a dataset that has never published isn't "stale", and a
    hard exit-1 here would false-alarm from the day indices was registered
    in the code until its very first successful publish. Once the dataset
    DOES appear in the manifest, holes are enforced normally (see the
    hole-in-indices test above)."""
    _continuity_registry(monkeypatch, tmp_path)
    window = _window_dates()
    fake = FakeReleaseClient(exists=True)
    manifest_datasets: list[dict] = []
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="ohlc",
                            file_prefix="ohlc", year=2026, dates=window)
    # indices deliberately NOT added to manifest_datasets -- absent entirely.
    _seed_manifest_and_client(fake, manifest_datasets, work_dir=tmp_path / "work")

    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(), today=_TODAY,
        runner=lambda _cmd: 0, work_dir=tmp_path / "work", client=fake,
    )
    assert rc == 0
    err = capsys.readouterr().err
    assert "indices" in err  # warning names the never-published dataset


def test_continuity_skips_derived_datasets(monkeypatch, tmp_path):
    """A derived spec (reference/ca_flags) has no fetcher and is never
    continuity-checked -- even if it's registered and absent from the
    manifest, it must NOT trigger the absent-dataset warning path (that path
    is scoped to FETCHED specs only) and must NOT be able to fail the
    check."""
    equities = dataclasses.replace(datasets.EQUITIES, base_dir=tmp_path / "ohlc")
    derived = datasets.DatasetSpec(
        key="reference", file_prefix="reference", base_dir=tmp_path / "reference",
        source_label="derived", normalizer=lambda df: df,
        make_fetcher=lambda: (_ for _ in ()).throw(
            RuntimeError("derived dataset has no fetcher -- must never be called")
        ),
        abs_rowcount_range=(0, 10**9), manifest_name="reference", schema_version=1,
        derived=True,
    )
    monkeypatch.setattr(cli.datasets, "DATASETS", {"equities": equities, "reference": derived})
    monkeypatch.setattr(cli.datasets, "DATASET_ORDER", ["equities", "reference"])

    window = _window_dates()
    fake = FakeReleaseClient(exists=True)
    manifest_datasets: list[dict] = []
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="ohlc",
                            file_prefix="ohlc", year=2026, dates=window)
    # "reference" absent from the manifest entirely -- must be a non-event.
    _seed_manifest_and_client(fake, manifest_datasets, work_dir=tmp_path / "work")

    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(), today=_TODAY,
        runner=lambda _cmd: 0, work_dir=tmp_path / "work", client=fake,
    )
    assert rc == 0


def test_continuity_year_boundary_window_downloads_previous_year_asset(
    monkeypatch, tmp_path,
):
    """When the trailing window crosses Jan 1, the continuity check must
    download BOTH the current-year and previous-year baseline assets (same
    year-selection logic as `store.read_trailing_window`) -- a baseline
    seeded ONLY under the correct two-year split must still read as fully
    clean, proving both years were actually fetched and combined rather
    than just the latest one."""
    monkeypatch.setattr(cli.datasets, "DATASETS", {"equities": dataclasses.replace(
        datasets.EQUITIES, base_dir=tmp_path / "ohlc")})
    monkeypatch.setattr(cli.datasets, "DATASET_ORDER", ["equities"])

    today = date(2026, 1, 5)  # window reaches back into December 2025
    window = _window_dates(today=today)
    assert window[0].year == 2025 and window[-1].year == 2026
    by_year: dict[int, list[date]] = {}
    for d in window:
        by_year.setdefault(d.year, []).append(d)

    fake = FakeReleaseClient(exists=True)
    manifest_datasets: list[dict] = []
    data_by_year = []
    for year, dates_in_year in by_year.items():
        data = _baseline_parquet_bytes(dates_in_year)
        sha = hashlib.sha256(data).hexdigest()
        name = f"ohlc_{year}.parquet"
        asset = asset_name(name, sha)
        fake.seed(asset, data)
        data_by_year.append({"name": name, "asset": asset, "sha256": sha,
                              "bytes": len(data), "rows": len(dates_in_year)})
    manifest_datasets.append({
        "name": "ohlc", "schema_version": 1, "latest_date": max(window).isoformat(),
        "baseline": data_by_year, "deltas": [],
    })
    _seed_manifest_and_client(fake, manifest_datasets, work_dir=tmp_path / "work",
                               latest_trading_date=date(2026, 1, 2))

    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(), today=today,
        runner=lambda _cmd: 0, work_dir=tmp_path / "work", client=fake,
    )
    assert rc == 0


# -- continuity clamp to available history (Critical fix) --
#
# THE BUG: the continuity window (fixed at 10 trading days) was diffed
# against `dates_present` with NO floor at the dataset's own earliest stored
# date. A live store with only a few days of history (e.g. 3 days) reported
# every expected day before its own first day as a false "hole" -- 7 false
# alarms/morning, exit 1, until depth reaches the window (or a backfill).
# THE FIX: `freshness.missing_days` clamps `expected` to days
# `>= min(dates_present)`; `_check_dataset_continuity` prints an
# informational (not alarming) note when the clamp actually truncated the
# window, so the reduced-coverage state stays visible rather than silently
# looking identical to a full 10-day-verified pass.

def test_continuity_clamps_to_available_history_reproduces_real_3day_store(
    monkeypatch, tmp_path, capsys,
):
    """THE REPRODUCTION: mirrors the real live store's shape at the time
    this bug was found -- a 3-day-old equities baseline ({2026-07-01..03}),
    today = 2026-07-06, full manifest. Pre-fix: 7 false holes predating the
    store, exit 1, every morning until depth reaches 10. Post-fix: exit 0,
    an informational 'verified over 3 of 10 days' note printed, NO holes."""
    _continuity_registry(monkeypatch, tmp_path)
    real_store_dates = [date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)]
    window = _window_dates()  # indices stays fully seeded -- isolates the equities repro
    fake = FakeReleaseClient(exists=True)
    manifest_datasets: list[dict] = []
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="ohlc",
                            file_prefix="ohlc", year=2026, dates=real_store_dates)
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="indices",
                            file_prefix="indices", year=2026, dates=window)
    _seed_manifest_and_client(fake, manifest_datasets, work_dir=tmp_path / "work",
                               latest_trading_date=date(2026, 7, 3))

    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(), today=_TODAY,
        runner=lambda _cmd: 0, work_dir=tmp_path / "work", client=fake,
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "check-freshness: dataset" not in out  # no hole line for equities
    assert "continuity: ohlc verified over 3 of 10 days" in out
    assert "2026-07-01" in out  # names the earliest available date


def test_continuity_hole_within_available_depth_still_exits_1_after_clamp(
    monkeypatch, tmp_path, capsys,
):
    """The clamp must never hide a REAL hole inside the dataset's own
    available depth -- only days predating the earliest stored date are
    excused. A young store missing a day strictly AFTER its own earliest
    date is a genuine hole and must still fail the check by name."""
    _continuity_registry(monkeypatch, tmp_path)
    window = _window_dates()
    # 4-day-deep store: earliest + latest present, one real day between them
    # missing (a hole squarely within the store's own depth, not before it).
    d_early, d_mid, d_late = window[-4], window[-2], window[-1]
    equities_dates = [d for d in [d_early, d_mid, d_late] if d != d_mid]
    fake = FakeReleaseClient(exists=True)
    manifest_datasets: list[dict] = []
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="ohlc",
                            file_prefix="ohlc", year=2026, dates=equities_dates)
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="indices",
                            file_prefix="indices", year=2026, dates=window)
    _seed_manifest_and_client(fake, manifest_datasets, work_dir=tmp_path / "work",
                               latest_trading_date=d_late)

    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(), today=_TODAY,
        runner=lambda _cmd: 0, work_dir=tmp_path / "work", client=fake,
    )
    assert rc == 1
    out = capsys.readouterr().out
    assert d_mid.isoformat() in out
    assert "ohlc" in out
    # Days strictly before d_early must NOT appear -- clamp still applies.
    for d in window:
        if d < d_early:
            assert d.isoformat() not in out


def test_continuity_empty_in_window_dates_exits_0_staleness_governs(
    monkeypatch, tmp_path, capsys,
):
    """A baseline asset that resolves to ZERO dates at all (nothing
    verifiable) must not be treated as "everything is missing" -- an empty
    `dates_present` clamps to an empty expected set (there is no earliest
    date to floor against), so `missing_days` returns `[]`. Continuity's job
    ends there; lag is independently governed by the pre-existing STALENESS
    check (`is_stale`, off `latest_trading_date`), which stays exit-0 as
    long as the manifest's own latest date is current.

    Seeded by hand (not via `_seed_dataset_baseline`, which derives
    `latest_date` via `max(dates)` and cannot express a zero-row baseline) --
    an empty-but-present baseline parquet, with `latest_date` supplied
    independently since it is a distinct manifest field, not derived from
    the (empty) baseline content in this fixture."""
    _continuity_registry(monkeypatch, tmp_path)
    window = _window_dates()
    fake = FakeReleaseClient(exists=True)
    manifest_datasets: list[dict] = []
    data = _baseline_parquet_bytes([])  # zero-row baseline, valid parquet shape
    sha = hashlib.sha256(data).hexdigest()
    name = "ohlc_2026.parquet"
    asset = asset_name(name, sha)
    fake.seed(asset, data)
    manifest_datasets.append({
        "name": "ohlc", "schema_version": 1,
        "latest_date": date(2026, 7, 3).isoformat(),
        "baseline": [{"name": name, "asset": asset, "sha256": sha,
                      "bytes": len(data), "rows": 0}],
        "deltas": [],
    })
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="indices",
                            file_prefix="indices", year=2026, dates=window)
    _seed_manifest_and_client(fake, manifest_datasets, work_dir=tmp_path / "work")

    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(), today=_TODAY,
        runner=lambda _cmd: 0, work_dir=tmp_path / "work", client=fake,
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "check-freshness: dataset 'ohlc' missing" not in out


def test_continuity_year_boundary_young_store_no_prev_year_asset_exits_0(
    monkeypatch, tmp_path,
):
    """SECOND TRIGGER of the same defect: at a year boundary, a dataset
    less than a year old has no previous-year baseline asset at all -- it's
    not that the asset is missing by accident, the dataset simply didn't
    exist yet. Pre-fix, every previous-year expected day would be a false
    hole. Post-fix: the clamp floors at the earliest PRESENT date (itself in
    the current year), so the previous-year portion of the window is
    excused, exactly like the plain year-boundary test above but with the
    previous-year asset entirely ABSENT from the manifest (not just empty)."""
    monkeypatch.setattr(cli.datasets, "DATASETS", {"equities": dataclasses.replace(
        datasets.EQUITIES, base_dir=tmp_path / "ohlc")})
    monkeypatch.setattr(cli.datasets, "DATASET_ORDER", ["equities"])

    today = date(2026, 1, 5)  # window reaches back into December 2025
    window = _window_dates(today=today)
    assert window[0].year == 2025 and window[-1].year == 2026
    current_year_dates = [d for d in window if d.year == 2026]
    assert current_year_dates

    fake = FakeReleaseClient(exists=True)
    manifest_datasets: list[dict] = []
    # ONLY the 2026 baseline is seeded -- no 2025 entry at all (young store).
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="ohlc",
                            file_prefix="ohlc", year=2026, dates=current_year_dates)
    _seed_manifest_and_client(fake, manifest_datasets, work_dir=tmp_path / "work",
                               latest_trading_date=max(current_year_dates))

    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(), today=today,
        runner=lambda _cmd: 0, work_dir=tmp_path / "work", client=fake,
    )
    assert rc == 0


def test_continuity_release_error_on_asset_download_exits_1_clean_message(
    monkeypatch, tmp_path, capsys,
):
    """Minor (from review): the continuity download loop must not crash with
    a raw traceback when the manifest names an asset the release doesn't
    actually have -- a manifest-listed-but-missing asset is a REAL alarm
    (release/manifest inconsistency), not a crash. Wrapped in try/except
    ReleaseError -> a clean 'cross-dataset' error message on stderr, exit 1."""
    _continuity_registry(monkeypatch, tmp_path)
    window = _window_dates()
    fake = FakeReleaseClient(exists=True)
    manifest_datasets: list[dict] = []
    _seed_dataset_baseline(fake, manifest_datasets, manifest_name="ohlc",
                            file_prefix="ohlc", year=2026, dates=window)
    _seed_manifest_and_client(fake, manifest_datasets, work_dir=tmp_path / "work")
    # Sabotage: remove the just-seeded asset bytes so client.download raises
    # ReleaseError("asset not found: ...") -- manifest still names it.
    fake.assets.clear()

    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(), today=_TODAY,
        runner=lambda _cmd: 0, work_dir=tmp_path / "work", client=fake,
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "cross-dataset" in err
    assert "Traceback" not in err


# --- G3 Task 4: monthly snapshot CLI subcommand ------------------------------

def test_parser_has_snapshot():
    args = cli.build_parser().parse_args(["snapshot"])
    assert args.cmd == "snapshot"


def test_main_snapshot_calls_create_then_prune_and_exits_0(monkeypatch):
    calls: list[str] = []

    def fake_create_snapshot(*a, **kw):
        calls.append("create")
        return "data-snapshot-202607"

    def fake_prune_snapshots(*a, **kw):
        calls.append("prune")
        return ["data-snapshot-202601"]

    monkeypatch.setattr(cli, "create_snapshot", fake_create_snapshot)
    monkeypatch.setattr(cli, "prune_snapshots", fake_prune_snapshots)
    rc = cli.main(["snapshot"])
    assert rc == 0
    assert calls == ["create", "prune"]  # create runs before prune, never the reverse


def test_main_snapshot_returns_1_on_release_error(monkeypatch):
    from pipeline.errors import ReleaseError

    def _boom(*a, **kw):
        raise ReleaseError("network down")

    monkeypatch.setattr(cli, "create_snapshot", _boom)
    assert cli.main(["snapshot"]) == 1


def test_main_snapshot_returns_1_on_unexpected_failure(monkeypatch):
    def _boom(*a, **kw):
        raise UnexpectedFailure("snapshot tag already exists")

    monkeypatch.setattr(cli, "create_snapshot", _boom)
    assert cli.main(["snapshot"]) == 1


def test_main_snapshot_prune_failure_also_exits_1(monkeypatch):
    from pipeline.errors import ReleaseError

    monkeypatch.setattr(cli, "create_snapshot", lambda *a, **kw: "data-snapshot-202607")

    def _boom(*a, **kw):
        raise ReleaseError("delete failed")

    monkeypatch.setattr(cli, "prune_snapshots", _boom)
    assert cli.main(["snapshot"]) == 1


def test_main_snapshot_prints_created_and_pruned(monkeypatch, capsys):
    monkeypatch.setattr(cli, "create_snapshot", lambda *a, **kw: "data-snapshot-202607")
    monkeypatch.setattr(cli, "prune_snapshots", lambda *a, **kw: ["data-snapshot-202601"])
    rc = cli.main(["snapshot"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "data-snapshot-202607" in out
    assert "data-snapshot-202601" in out


# --- G3 Task 5: restore-from-snapshot CLI subcommand -------------------------

def test_parser_has_restore_from_snapshot():
    args = cli.build_parser().parse_args(["restore-from-snapshot", "--tag", "data-snapshot-202607"])
    assert args.cmd == "restore-from-snapshot"
    assert args.tag == "data-snapshot-202607"
    assert args.target is None  # unset -> resolved to the scratch default at dispatch time


def test_parser_restore_from_snapshot_accepts_explicit_target():
    args = cli.build_parser().parse_args([
        "restore-from-snapshot", "--tag", "data-snapshot-202607", "--target", "/explicit/path",
    ])
    assert args.target == "/explicit/path"


def test_main_restore_from_snapshot_no_target_defaults_to_scratch_drill_dir(
    monkeypatch, tmp_path,
):
    """No --target -> resolves under config.DATA_DIR / "_restore_drill" / tag --
    NEVER the live data/ tree -- this is the safety rail that makes a drill
    safe to run against a real snapshot tag by default."""
    monkeypatch.setattr(cli.config, "DATA_DIR", tmp_path)
    seen: dict[str, Path] = {}

    def fake_restore_from_tag(_client, *, target_root, work_dir):  # noqa: ARG001
        seen["target_root"] = target_root
        return {"datasets": []}

    monkeypatch.setattr(cli, "restore_from_tag", fake_restore_from_tag)
    rc = cli.main(["restore-from-snapshot", "--tag", "data-snapshot-202607"])
    assert rc == 0
    assert seen["target_root"] == tmp_path / "_restore_drill" / "data-snapshot-202607"


def test_main_restore_from_snapshot_explicit_target_used_verbatim(monkeypatch, tmp_path):
    monkeypatch.setattr(cli.config, "DATA_DIR", tmp_path)
    seen: dict[str, Path] = {}

    def fake_restore_from_tag(_client, *, target_root, work_dir):  # noqa: ARG001
        seen["target_root"] = target_root
        return {"datasets": []}

    monkeypatch.setattr(cli, "restore_from_tag", fake_restore_from_tag)
    explicit = tmp_path / "explicit" / "path"
    rc = cli.main([
        "restore-from-snapshot", "--tag", "data-snapshot-202607", "--target", str(explicit),
    ])
    assert rc == 0
    assert seen["target_root"] == explicit


def test_main_restore_from_snapshot_returns_1_on_unexpected_failure(monkeypatch, tmp_path):
    monkeypatch.setattr(cli.config, "DATA_DIR", tmp_path)

    def _boom(_client, *, target_root, work_dir):  # noqa: ARG001
        raise UnexpectedFailure("restore checksum mismatch for x: got a, manifest says b")

    monkeypatch.setattr(cli, "restore_from_tag", _boom)
    assert cli.main(["restore-from-snapshot", "--tag", "data-snapshot-202607"]) == 1


def test_main_restore_from_snapshot_returns_1_on_release_error(monkeypatch, tmp_path):
    from pipeline.errors import ReleaseError

    monkeypatch.setattr(cli.config, "DATA_DIR", tmp_path)

    def _boom(_client, *, target_root, work_dir):  # noqa: ARG001
        raise ReleaseError("network down")

    monkeypatch.setattr(cli, "restore_from_tag", _boom)
    assert cli.main(["restore-from-snapshot", "--tag", "data-snapshot-202607"]) == 1


def test_main_restore_from_snapshot_prints_dataset_summary(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(cli.config, "DATA_DIR", tmp_path)

    def fake_restore_from_tag(_client, *, target_root, work_dir):  # noqa: ARG001
        return {
            "datasets": [
                {"name": "ohlc", "latest_date": "2026-07-03",
                 "baseline": [{"name": "ohlc_2026.parquet", "bytes": 8, "rows": 1}]},
            ],
        }

    monkeypatch.setattr(cli, "restore_from_tag", fake_restore_from_tag)
    rc = cli.main(["restore-from-snapshot", "--tag", "data-snapshot-202607"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "ohlc" in out
    assert "2026-07-03" in out


def test_main_restore_from_snapshot_phase2_oserror_gives_clean_message(
    monkeypatch, tmp_path, capsys,
):
    """A phase-2-only failure (materialize-time I/O: disk full, permissions,
    an OS race mid-loop) is NOT covered by restore_from_tag's two-phase
    "target_root untouched" guarantee -- that guarantee only covers phase-1
    (verification) failures. The raw OSError propagates out of
    restore_from_tag uncaught; the CLI must present it as a clean,
    actionable message (never a raw traceback) naming the remediation:
    delete target_root and retry (the restore is idempotent).

    Runs the REAL restore_from_tag (not a monkeypatched stand-in) against a
    two-file baseline manifest, with Path.replace patched to succeed on the
    1st call and raise OSError on the 2nd -- so the failure is provably
    phase-2 (the 1st file DID materialize into target_root before the
    failure), not phase-1."""
    import json

    monkeypatch.setattr(cli.config, "DATA_DIR", tmp_path)

    data_a, data_b = b"OHLCDATA-A", b"OHLCDATA-B"
    sha_a = hashlib.sha256(data_a).hexdigest()
    sha_b = hashlib.sha256(data_b).hexdigest()
    manifest_obj = {
        "manifest_version": 2, "generated_at": "g", "latest_trading_date": "2026-07-03",
        "datasets": [
            {"name": "ohlc", "schema_version": 2, "latest_date": "2026-07-03",
             "baseline": [{"name": "ohlc_2026.parquet",
                           "asset": asset_name("ohlc_2026.parquet", sha_a),
                           "sha256": sha_a, "bytes": len(data_a), "rows": 1}]},
            {"name": "indices", "schema_version": 2, "latest_date": "2026-07-03",
             "baseline": [{"name": "indices_2026.parquet",
                           "asset": asset_name("indices_2026.parquet", sha_b),
                           "sha256": sha_b, "bytes": len(data_b), "rows": 1}]},
        ],
    }
    client = FakeReleaseClient(exists=True)
    client.seed("manifest.json", json.dumps(manifest_obj).encode())
    client.seed(asset_name("ohlc_2026.parquet", sha_a), data_a)
    client.seed(asset_name("indices_2026.parquet", sha_b), data_b)
    monkeypatch.setattr(cli, "GhReleaseClient", lambda **_kw: client)

    real_replace = Path.replace
    calls = {"n": 0}

    def _flaky_replace(self: Path, target: Path) -> Path:
        calls["n"] += 1
        if calls["n"] == 2:
            raise OSError("[Errno 28] No space left on device")
        return real_replace(self, target)

    monkeypatch.setattr(Path, "replace", _flaky_replace)

    rc = cli.main(["restore-from-snapshot", "--tag", "data-snapshot-202607"])

    assert rc == 1
    err = capsys.readouterr().err
    expected_target = tmp_path / "_restore_drill" / "data-snapshot-202607"
    assert (
        f"restore-from-snapshot failed (materialize error): "
        f"[Errno 28] No space left on device — delete {expected_target} and retry"
    ) in err
    assert "Traceback" not in err
    # Provably phase-2, not phase-1: the 1st file DID land before the 2nd call raised.
    assert calls["n"] == 2
    landed = list((expected_target / "ohlc").glob("*.parquet")) + \
        list((expected_target / "indices").glob("*.parquet"))
    assert len(landed) == 1
