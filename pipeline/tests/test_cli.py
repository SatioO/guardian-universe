import dataclasses
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline import cli, config, datasets


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
    assert seen == ["equities", "indices"]  # DATASET_ORDER as of G1b task 3
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
    assert seen == ["equities", "indices"]  # reference (derived) never fetched


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
