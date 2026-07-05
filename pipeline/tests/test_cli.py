import dataclasses
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import calendar as cal
from pipeline import cli, config, datasets
from pipeline.errors import NotYetPublished
from pipeline.fetch import FetchResult


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
    (tmp_path / "manifest.json").write_text(json.dumps({"latest_trading_date": "2026-07-03"}))
    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(),
        today=date(2026, 7, 6), runner=lambda _cmd: 0, work_dir=tmp_path,
    )
    assert rc == 0  # Fri 2026-07-03 published, today Mon -> fresh


def test_cmd_check_freshness_flags_missing_release(tmp_path: Path):
    rc = cli.cmd_check_freshness(
        repo="o/r", tag="data-latest", holidays=set(),
        today=date(2026, 7, 6), runner=lambda _cmd: 1, work_dir=tmp_path,
    )
    assert rc == 1  # download failed -> treated as stale/missing


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


class RecordingFetcher:
    """Logs every requested date; serves `_one_day_raw(d)` by default, or
    raises `exceptions[d]` when that date has an override. A single instance
    is reused across an entire window's days (proving the fetcher-reuse
    contract) by having `make_fetcher` be a closure returning this same
    object every time it's invoked, while a separate counter proves
    `make_fetcher` itself is called exactly once per spec per CLI run."""

    def __init__(self, exceptions: dict[date, Exception] | None = None):
        self.requested: list[date] = []
        self._exceptions = exceptions or {}

    def fetch_raw(self, d: date) -> FetchResult:
        self.requested.append(d)
        if d in self._exceptions:
            raise self._exceptions[d]
        return FetchResult(_one_day_raw(d), "nse-udiff")


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


def test_catchup_past_day_404_fails_and_forces_exit_1_while_target_still_ingests(
    monkeypatch, tmp_path
):
    """A past (non-target) day in the window that 404s must be reported as
    'failed' (a hole, not lateness) and force the run's exit code to 1 for
    the primary spec -- even though the target day itself succeeds and its
    status file still shows success. This is the 'repaired-hole failure must
    never silently pass' requirement."""
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
    assert rc == 1  # a repaired-hole failure must never silently pass
    assert store.has_day(equities.base_dir, target)  # target day still ingested
    assert not store.has_day(equities.base_dir, past_day)  # the hole stays a hole
    status = json.loads((tmp_path / "last_run_status.json").read_text())
    assert status["status"] == "success"  # status FILE carries the TARGET day's status
    assert status["date"] == target.isoformat()


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


def test_cmd_cross_check_sources_agree_exits_0():
    target = date(2026, 7, 3)

    def fetch_primary(d: date) -> pd.DataFrame:
        return _cross_check_raw(_CROSS_CHECK_ROWS, d)

    def fetch_secondary(d: date) -> pd.DataFrame:
        return _cross_check_raw(_CROSS_CHECK_ROWS, d)

    rc = cli.cmd_cross_check(
        target, fetch_primary_raw=fetch_primary, fetch_secondary_raw=fetch_secondary,
    )
    assert rc == 0


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
    assert "cross-check" in err
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
    assert "cross-check" in err
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
