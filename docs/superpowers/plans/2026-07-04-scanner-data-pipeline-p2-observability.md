# Scanner Data Pipeline — P2 Observability — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the now-autonomous daily pipeline *observable* — emit a machine-readable last-run status, detect staleness with a dead-man's-switch, and auto-open a GitHub issue on any failure — so a silent break (NSE format change, token expiry, a missed day) is noticed within hours, not weeks.

**Architecture:** Two-layer safety. (1) The daily run emits `last_run_status.json` (published alongside the data) — a debuggable trail. (2) A separate scheduled `data-monitor.yml` computes the expected latest trading day from the calendar and compares it to the published manifest — a freshness dead-man's-switch that catches even a *silently non-running* daily job. Both the daily job (on failure) and the monitor (on staleness) open/append a deduped GitHub issue. Detection logic is pure and unit-tested; the gh/issue plumbing is validated by a live dispatch. Silence is success.

**Tech Stack:** Python 3.11+, `gh` CLI (issues + release download), GitHub Actions, pytest + ruff + mypy --strict, `filterwarnings=["error"]`.

## Global Constraints

- **Python 3.11+**; tooling via the venv (`.venv/bin/pytest -v`, `ruff check .`, `mypy` from `pipeline/`). Warning-free (`filterwarnings=["error"]`); mypy `--strict` clean.
- **Reuse:** `manifest.status_to_dict`/`write_json`, `publish.publish_release`/`Runner`/`subprocess_runner`, `calendar.previous_trading_day`/`load_holidays`, `daily_update.RunStatus`, `config.{OHLC_DIR,META_DIR,GITHUB_REPO,RELEASE_TAG}`, `errors.UnexpectedFailure`. No new error types.
- **Detection is pure + tested; plumbing (gh) is injected in tests** — NO real `gh`/network in tests. Workflows validated by a live dispatch, not unit tests.
- **Staleness rule:** the published `latest_trading_date` is STALE when it is strictly before `previous_trading_day(today)` — i.e. the most recent *completed* trading day has not been published.
- **Alert channel:** a GitHub issue labeled `pipeline-failure`, deduped (one open issue; append a comment if it already exists). No external services.
- **Least privilege:** `data-monitor.yml` → `contents: read`, `issues: write`. `data-daily.yml` gains `issues: write` (keeps `contents: write`).

---

### Task 1: Emit + publish `last_run_status.json`

**Files:**
- Modify: `pipeline/src/pipeline/manifest.py` (add `write_status`)
- Modify: `pipeline/src/pipeline/publish.py` (add `extra_files` to `publish_release`)
- Modify: `pipeline/src/pipeline/cli.py` (daily writes status; `cmd_publish` uploads it)
- Modify: `pipeline/tests/test_manifest.py`, `pipeline/tests/test_publish.py`

**Interfaces:**
- Produces:
  - `manifest.write_status(status_obj, meta_dir: Path) -> Path` — writes `status_to_dict(status_obj)` to `meta_dir/last_run_status.json`; returns the path.
  - `publish.publish_release(data_files, manifest_path, *, tag, repo, runner, extra_files: Sequence[Path] = ())` — uploads `data_files` + `extra_files`, THEN the manifest last. Empty-`data_files` guard unchanged.
  - `cli.cmd_publish` includes `meta_dir/last_run_status.json` in `extra_files` when it exists; the `daily` branch calls `manifest.write_status(st, config.META_DIR)`.

- [ ] **Step 1: Write the failing tests**

Append to `pipeline/tests/test_manifest.py`:
```python
def test_write_status_writes_last_run_status(tmp_path: Path):
    from pipeline.daily_update import RunStatus
    p = manifest.write_status(
        RunStatus("success", date(2026, 7, 3), symbol_count=2406, source="nse-udiff"),
        tmp_path,
    )
    assert p == tmp_path / "last_run_status.json"
    import json
    assert json.loads(p.read_text())["symbol_count"] == 2406
```
Append to `pipeline/tests/test_publish.py`:
```python
def test_publish_uploads_extra_files_before_manifest(tmp_path: Path):
    a = tmp_path / "ohlc_2026.parquet"; a.write_text("x")
    s = tmp_path / "last_run_status.json"; s.write_text("{}")
    m = tmp_path / "manifest.json"; m.write_text("{}")
    r = FakeRunner()
    publish.publish_release([a], m, tag="t", repo="o/r", runner=r, extra_files=[s])
    uploads = [c for c in r.calls if "upload" in c]
    assert len(uploads) == 3
    assert str(m) in uploads[-1]                       # manifest still LAST
    assert any(str(s) in u for u in uploads[:-1])      # status uploaded before it
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_manifest.py tests/test_publish.py -v`
Expected: the 2 new tests FAIL (`write_status` missing; `publish_release` has no `extra_files`).

- [ ] **Step 3a: `manifest.py`** — add:
```python
def write_status(status_obj: Any, meta_dir: Path) -> Path:
    path = meta_dir / "last_run_status.json"
    write_json(status_to_dict(status_obj), path)
    return path
```

- [ ] **Step 3b: `publish.py`** — change `publish_release`'s signature and upload loop. Add `from collections.abc import Sequence` if not present. New signature + body:
```python
def publish_release(
    data_files: list[Path],
    manifest_path: Path,
    *,
    tag: str,
    repo: str,
    runner: Runner,
    extra_files: Sequence[Path] = (),
) -> None:
    if not data_files:
        raise UnexpectedFailure("refusing to publish: no data files (empty store)")
    runner(["gh", "release", "create", tag, "--repo", repo, "--title", tag,
            "--notes", "automated data release"])
    for f in [*data_files, *extra_files, manifest_path]:
        rc = runner(["gh", "release", "upload", tag, str(f), "--clobber", "--repo", repo])
        if rc != 0:
            raise UnexpectedFailure(f"gh release upload failed ({rc}) for {f.name}")
```

- [ ] **Step 3c: `cli.py`** — in the `daily` branch, after computing `st` and before `return`, add:
```python
        manifest.write_status(st, config.META_DIR)
```
And in `cmd_publish`, before calling `publish_release`, compute extras and pass them:
```python
    status_path = meta_dir / "last_run_status.json"
    extra = [status_path] if status_path.exists() else []
    publish.publish_release(data_files, manifest_path, tag=tag, repo=repo,
                            runner=runner, extra_files=extra)
```
(Replace the existing `publish.publish_release(...)` call in `cmd_publish` with the above.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest -q && .venv/bin/ruff check . && .venv/bin/mypy`
Expected: full suite green (ZERO warnings), ruff clean, mypy Success. (Existing publish tests still pass — `extra_files` defaults to `()`.)

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/manifest.py pipeline/src/pipeline/publish.py pipeline/src/pipeline/cli.py pipeline/tests/test_manifest.py pipeline/tests/test_publish.py
git commit -m "feat(pipeline): emit and publish last_run_status.json"
```

---

### Task 2: Freshness detection + `check-freshness` command

**Files:**
- Create: `pipeline/src/pipeline/freshness.py`
- Modify: `pipeline/src/pipeline/cli.py` (add `check-freshness` subcommand + `cmd_check_freshness`)
- Create: `pipeline/tests/test_freshness.py`
- Modify: `pipeline/tests/test_cli.py`

**Interfaces:**
- Produces:
  - `freshness.is_stale(latest_trading_date: date, today: date, holidays: set[date]) -> bool` — True when `latest_trading_date < calendar.previous_trading_day(today, holidays)`.
  - `cli.cmd_check_freshness(*, repo: str, tag: str, holidays: set[date], today: date, runner: publish.Runner, work_dir: Path) -> int` — downloads `manifest.json` from the release into `work_dir`; returns 1 if the download failed OR the data is stale, else 0.
  - `build_parser` gains a `check-freshness` subcommand; `main` dispatches it (returns its exit code).

- [ ] **Step 1: Write the failing tests** — `pipeline/tests/test_freshness.py`:
```python
from datetime import date

from pipeline.freshness import is_stale

HOLIDAYS: set[date] = set()


def test_fresh_when_latest_is_the_last_completed_trading_day():
    # today Mon 2026-07-06; last completed trading day is Fri 2026-07-03.
    assert is_stale(date(2026, 7, 3), date(2026, 7, 6), HOLIDAYS) is False


def test_stale_when_latest_is_behind_the_last_completed_trading_day():
    # today Mon 2026-07-06; latest only Thu 2026-07-02 -> Friday was missed.
    assert is_stale(date(2026, 7, 2), date(2026, 7, 6), HOLIDAYS) is True


def test_not_stale_over_a_weekend():
    # today Sun 2026-07-05; last completed trading day is Fri 2026-07-03.
    assert is_stale(date(2026, 7, 3), date(2026, 7, 5), HOLIDAYS) is False
```
Append to `pipeline/tests/test_cli.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_freshness.py tests/test_cli.py -v`
Expected: FAIL — `pipeline.freshness` missing; `cmd_check_freshness`/`check-freshness` missing.

- [ ] **Step 3a: `pipeline/src/pipeline/freshness.py`**:
```python
"""Freshness detection for the published dataset. Pure."""
from __future__ import annotations

from datetime import date

from pipeline import calendar as cal


def is_stale(latest_trading_date: date, today: date, holidays: set[date]) -> bool:
    # Stale when the most recent COMPLETED trading day is not yet published.
    return latest_trading_date < cal.previous_trading_day(today, holidays)
```

- [ ] **Step 3b: `cli.py`** — add `import json` and `import tempfile` to the top imports (if absent) and `from pipeline import freshness`. In `build_parser`, after the `sync` subparser: `sub.add_parser("check-freshness")`. Add the command:
```python
def cmd_check_freshness(
    *,
    repo: str,
    tag: str,
    holidays: set[date],
    today: date,
    runner: publish.Runner,
    work_dir: Path,
) -> int:
    work_dir.mkdir(parents=True, exist_ok=True)
    rc = runner(["gh", "release", "download", tag, "--repo", repo,
                 "--pattern", "manifest.json", "--dir", str(work_dir), "--clobber"])
    manifest_path = work_dir / "manifest.json"
    if rc != 0 or not manifest_path.exists():
        return 1  # no release / download failed -> stale
    latest = date.fromisoformat(json.loads(manifest_path.read_text())["latest_trading_date"])
    return 1 if freshness.is_stale(latest, today, holidays) else 0
```
In `main`, before the publish fallthrough:
```python
    if args.cmd == "check-freshness":
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        with tempfile.TemporaryDirectory() as tmp:
            return cmd_check_freshness(
                repo=config.GITHUB_REPO, tag=config.RELEASE_TAG, holidays=holidays,
                today=datetime.now(UTC).date(), runner=publish.subprocess_runner,
                work_dir=Path(tmp),
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest -q && .venv/bin/ruff check . && .venv/bin/mypy`
Expected: full suite green (ZERO warnings), ruff clean, mypy Success.

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/freshness.py pipeline/src/pipeline/cli.py pipeline/tests/test_freshness.py pipeline/tests/test_cli.py
git commit -m "feat(pipeline): freshness dead-man's-switch detection + check-freshness command"
```

---

### Task 3: Monitor workflow, failure alerting, Dependabot

**Files:**
- Create: `.github/workflows/data-monitor.yml`
- Modify: `.github/workflows/data-daily.yml` (add `issues: write` + a failure-alert step)
- Create: `.github/dependabot.yml`

**Interfaces:**
- Consumes: `pipeline check-freshness`.
- Produces: a daily freshness monitor that opens a deduped issue when stale; a failure-alert step on the daily job; weekly dependency update PRs.

- [ ] **Step 1: Create `.github/workflows/data-monitor.yml`**

```yaml
name: data-monitor

on:
  schedule:
    - cron: "0 2 * * *"   # 07:30 IST — after both evening ingest windows
  workflow_dispatch:

permissions:
  contents: read
  issues: write

jobs:
  freshness:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    defaults:
      run:
        working-directory: pipeline
    env:
      GH_TOKEN: ${{ github.token }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
      - run: pip install -e ".[dev]"
      - name: Check dataset freshness
        id: fresh
        run: python -m pipeline check-freshness
      - name: Open/append staleness issue
        if: failure() && steps.fresh.conclusion == 'failure'
        working-directory: ${{ github.workspace }}
        run: |
          gh label create pipeline-failure --color B60205 --force >/dev/null 2>&1 || true
          body="Dataset is STALE as of $(date -u +%F). Monitor run: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}"
          n=$(gh issue list --label pipeline-failure --state open --json number -q '.[0].number')
          if [ -z "$n" ]; then
            gh issue create --title "Data pipeline: dataset stale" --label pipeline-failure --body "$body"
          else
            gh issue comment "$n" --body "$body"
          fi
```

- [ ] **Step 2: Modify `.github/workflows/data-daily.yml`** — change `permissions:` to include issues, and append a failure-alert step.
Replace:
```yaml
permissions:
  contents: write   # gh needs release read+write
```
with:
```yaml
permissions:
  contents: write   # gh needs release read+write
  issues: write     # failure alerting
```
Append as the LAST step in the `ingest-and-publish` job:
```yaml
      - name: Alert on failure
        if: failure()
        working-directory: ${{ github.workspace }}
        run: |
          gh label create pipeline-failure --color B60205 --force >/dev/null 2>&1 || true
          body="data-daily FAILED on $(date -u +%F). Run: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}"
          n=$(gh issue list --label pipeline-failure --state open --json number -q '.[0].number')
          if [ -z "$n" ]; then
            gh issue create --title "Data pipeline: data-daily failed" --label pipeline-failure --body "$body"
          else
            gh issue comment "$n" --body "$body"
          fi
```

- [ ] **Step 3: Create `.github/dependabot.yml`**

```yaml
version: 2
updates:
  - package-ecosystem: github-actions
    directory: "/"
    schedule:
      interval: weekly
  - package-ecosystem: pip
    directory: "/pipeline"
    schedule:
      interval: weekly
```

- [ ] **Step 4: Validate YAML + suite still green**

Run (from repo root):
```bash
for f in .github/workflows/data-monitor.yml .github/workflows/data-daily.yml .github/dependabot.yml; do
  pipeline/.venv/bin/python -c "import yaml,sys; yaml.safe_load(open('$f')); print('ok: $f')"
done
cd pipeline && .venv/bin/pytest -q
```
Expected: three `ok:` lines; full suite green.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/data-monitor.yml .github/workflows/data-daily.yml .github/dependabot.yml
git commit -m "ci(pipeline): freshness monitor, failure alerting, dependabot"
```

---

## Post-merge live validation (controller, after merge)

- Dispatch the monitor: `gh workflow run data-monitor.yml -R SatioO/guardian-universe` → it should PASS (the dataset is fresh: `latest_trading_date` is a recent trading day) and open no issue.
- (Optional) Confirm the failure-alert path by observing that a real future failure opens a `pipeline-failure` issue — do NOT force a failure just to test; the logic is unit-adjacent and the happy path is validated by the monitor run.

## What P2 delivers (Definition of Done)

- Every daily run emits `last_run_status.json`, published beside the data (debuggable trail; closes the P1b sync-visibility gap).
- `pipeline check-freshness` detects a stale/missing dataset (pure `is_stale` + a thin gh-download wrapper).
- `data-monitor.yml` runs daily and opens a deduped GitHub issue if the data is stale — a dead-man's-switch that fires even if `data-daily` silently never ran.
- `data-daily.yml` opens a deduped issue on any failure.
- Dependabot keeps actions + pip deps current. Full suite green, zero warnings, ruff + mypy --strict clean.

## Deferred (not in P2)

- **SHA-pinning actions** (Dependabot will now surface updates; pin in a follow-up once Dependabot is active).
- **P1c** — NSE indices. **P3** — the Rust client that consumes the release into the app.
- Slack/email alerting (GitHub issues suffice for a solo maintainer).

## Spec-coverage self-review

- Auto-open GitHub issue on failure (spec §12): Task 3 (`data-daily` alert step). ✔
- Freshness dead-man's-switch (spec §12): Task 2 (`check-freshness`) + Task 3 (`data-monitor.yml`). ✔
- `last_run_status.json` machine-readable trail (spec §5.1, §12): Task 1. ✔
- Dependabot (spec §11 hardening): Task 3. ✔
- SHA-pinning (spec §11): **deferred** — surfaced by Dependabot, pinned in a follow-up (noted). ✔ (partial-by-design)
