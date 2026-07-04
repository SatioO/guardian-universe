# Scanner Data Pipeline — P1b Daily Automation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automate the daily cycle in GitHub Actions — sync the current baseline from the `data-latest` release, ingest today's trading day (appending to real history so the deviation gate has a baseline), and republish — on a two-cron schedule, while hardening the publish/CLI paths flagged in the P1a review.

**Architecture:** Two small additions on top of P1a. A `pipeline sync` command downloads the current release's parquet(s) into the local store (via the injectable command runner) so a fresh CI runner appends *today* to accumulated history. A `data-daily.yml` workflow runs `sync → daily → publish` on two crons with least-privilege `contents: write`. Plus three P1a-review hardening fixes: refuse empty publishes, clean exit code on publish failure, and construct the fetcher/holidays only where needed. The daily workflow's viability against NSE's datacenter-IP blocking is an EMPIRICAL question resolved by a manual dispatch at the end — not assumed.

**Tech Stack:** Python 3.11+, `gh` CLI (release download/upload), GitHub Actions, pytest + ruff + mypy --strict, `filterwarnings=["error"]`.

## Global Constraints

- **Python 3.11+**; tooling via the venv: `.venv/bin/pytest -v`, `.venv/bin/ruff check .`, `.venv/bin/mypy` (from `pipeline/`).
- **Warning-free** (`filterwarnings=["error"]`); mypy `--strict` clean (narrow `# type: ignore[code]` with a comment only for genuine stub gaps).
- **Reuse existing:** `publish.Runner`/`publish.subprocess_runner`/`publish.publish_release`, `errors.UnexpectedFailure`, `config.{OHLC_DIR,META_DIR,GITHUB_REPO,RELEASE_TAG}`, `cli.cmd_publish`/`build_parser`/`main`, `daily_update.run_daily`, `backfill.backfill`, `fetch.NseUdiffFetcher`, `calendar.load_holidays`. Do NOT add new error types.
- **Injected runner in tests** — NO real `gh`/network in tests. The `data-daily.yml` workflow is verified only by a live dispatch (documented), not by a unit test.
- **Workflow:** two crons `0 14 * * 1-5` and `0 16 * * 1-5` (19:30 & 21:30 IST) + `workflow_dispatch`; `permissions: contents: write`; `concurrency` group; `timeout-minutes`; `defaults.run.working-directory: pipeline`; `gh` authed via `GH_TOKEN: ${{ github.token }}`.

---

### Task 1: Harden publish + CLI (P1a review fixes)

**Files:**
- Modify: `pipeline/src/pipeline/publish.py` (empty-publish guard)
- Modify: `pipeline/src/pipeline/cli.py` (publish try/except → exit 1; construct fetcher/holidays only in daily/backfill branches)
- Modify: `pipeline/tests/test_publish.py` (empty-guard test)
- Modify: `pipeline/tests/test_cli.py` (main-level dispatch tests)

**Interfaces:**
- Consumes: existing signatures.
- Produces: `publish_release` now raises `UnexpectedFailure` when `data_files` is empty; `main(["publish"])` returns 1 (not a traceback) on `UnexpectedFailure`; `main` no longer builds `NseUdiffFetcher`/holidays on the `publish` path.

- [ ] **Step 1: Write the failing tests**

Append to `pipeline/tests/test_publish.py`:
```python
def test_publish_release_refuses_empty_data(tmp_path: Path):
    m = tmp_path / "manifest.json"
    m.write_text("{}")
    with pytest.raises(UnexpectedFailure):
        publish.publish_release([], m, tag="data-latest", repo="o/r", runner=FakeRunner())
```
Append to `pipeline/tests/test_cli.py`:
```python
def test_main_publish_returns_1_on_failure(monkeypatch):
    def _boom(**_kw):
        from pipeline.errors import UnexpectedFailure
        raise UnexpectedFailure("no data")
    monkeypatch.setattr(cli, "cmd_publish", _boom)
    assert cli.main(["publish"]) == 1


def test_main_publish_returns_0_on_success(monkeypatch):
    calls = {}
    monkeypatch.setattr(cli, "cmd_publish", lambda **kw: calls.update(kw))
    assert cli.main(["publish"]) == 0
    assert calls["repo"] == config.GITHUB_REPO and calls["tag"] == config.RELEASE_TAG
```
(Add `from pipeline import cli` / `config` imports at the top of `test_cli.py` if not already present.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_publish.py tests/test_cli.py -v`
Expected: the 3 new tests FAIL (empty publish currently uploads only the manifest; `main` publish path has no try/except and constructs real adapters).

- [ ] **Step 3a: Modify `pipeline/src/pipeline/publish.py`** — add the guard as the first line of `publish_release`:
```python
    if not data_files:
        raise UnexpectedFailure("refusing to publish: no data files (empty store)")
```

- [ ] **Step 3b: Modify `pipeline/src/pipeline/cli.py`** — add `import sys` at the top, and change `main` so fetcher/holidays are built only where used and the publish path is guarded:
```python
def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "daily":
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        fetcher = NseUdiffFetcher()
        target = date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()
        st = run_daily(target, fetcher=fetcher, holidays=holidays, base=config.OHLC_DIR)
        print(manifest.status_to_dict(st))
        return 0 if st.status in ("success", "skipped_holiday", "skipped_idempotent",
                                  "not_yet") else 1
    if args.cmd == "backfill":
        holidays = cal.load_holidays(config.META_DIR / "holidays.json")
        fetcher = NseUdiffFetcher()
        results = backfill_mod.backfill(
            datetime.now(UTC).date(), args.days,
            fetcher=fetcher, holidays=holidays, base=config.OHLC_DIR,
        )
        return 0 if all(
            r.status in ("success", "skipped_holiday", "skipped_idempotent", "not_yet")
            for r in results
        ) else 1
    # publish
    try:
        cmd_publish(
            ohlc_dir=config.OHLC_DIR, meta_dir=config.META_DIR,
            repo=config.GITHUB_REPO, tag=config.RELEASE_TAG,
            runner=publish.subprocess_runner,
            generated_at=datetime.now(UTC).isoformat(),
        )
    except UnexpectedFailure as e:
        print(f"publish failed: {e}", file=sys.stderr)
        return 1
    return 0
```
Add the import at the top of `cli.py`: `from pipeline.errors import UnexpectedFailure`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest -q && .venv/bin/ruff check . && .venv/bin/mypy`
Expected: full suite green (ZERO warnings), ruff clean, mypy Success. (The existing `test_cmd_publish_writes_manifest_and_uploads` still passes — `cmd_publish` is unchanged.)

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/publish.py pipeline/src/pipeline/cli.py pipeline/tests/test_publish.py pipeline/tests/test_cli.py
git commit -m "fix(pipeline): refuse empty publish, clean exit on publish failure, lazy adapters"
```

---

### Task 2: `pipeline sync` — download the baseline from the release

**Files:**
- Modify: `pipeline/src/pipeline/cli.py` (add `sync` subcommand + `cmd_sync`)
- Modify: `pipeline/tests/test_cli.py` (sync tests)

**Interfaces:**
- Consumes: `publish.Runner`/`subprocess_runner`, `config.{OHLC_DIR,GITHUB_REPO,RELEASE_TAG}`.
- Produces: `cli.cmd_sync(*, ohlc_dir: Path, repo: str, tag: str, runner: publish.Runner) -> int` — creates `ohlc_dir`, runs `gh release download <tag> --repo <repo> --pattern 'ohlc_*.parquet' --dir <ohlc_dir> --clobber`, and RETURNS the runner's exit code WITHOUT raising (a missing release on the first run is tolerated). `build_parser` gains a `sync` subcommand; `main` dispatches it and always returns 0.

- [ ] **Step 1: Write the failing tests** — append to `pipeline/tests/test_cli.py`:
```python
def test_parser_has_sync():
    args = cli.build_parser().parse_args(["sync"])
    assert args.cmd == "sync"


def test_cmd_sync_downloads_ohlc_pattern(tmp_path: Path):
    calls: list[list[str]] = []
    rc = cli.cmd_sync(ohlc_dir=tmp_path / "ohlc", repo="o/r", tag="data-latest",
                      runner=lambda cmd: (calls.append(cmd), 0)[1])
    assert rc == 0
    cmd = calls[0]
    assert cmd[:4] == ["gh", "release", "download", "data-latest"]
    assert "ohlc_*.parquet" in cmd and "--clobber" in cmd


def test_cmd_sync_tolerates_missing_release(tmp_path: Path):
    # First run: no release yet -> gh returns non-zero -> cmd_sync must NOT raise.
    rc = cli.cmd_sync(ohlc_dir=tmp_path / "ohlc", repo="o/r", tag="data-latest",
                      runner=lambda _cmd: 1)
    assert rc == 1  # returned, not raised
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_cli.py -v`
Expected: the 3 new tests FAIL — `cmd_sync` and the `sync` subcommand don't exist.

- [ ] **Step 3: Modify `pipeline/src/pipeline/cli.py`**

In `build_parser`, after the `publish` subparser line, add:
```python
    sub.add_parser("sync")
```
Add the `cmd_sync` function (near `cmd_publish`):
```python
def cmd_sync(*, ohlc_dir: Path, repo: str, tag: str, runner: publish.Runner) -> int:
    # Download the current published parquet(s) so a fresh runner appends TODAY to
    # accumulated history. A missing release (first ever run) is tolerated: the
    # non-zero exit is returned, not raised — daily+publish will then create it.
    ohlc_dir.mkdir(parents=True, exist_ok=True)
    return runner([
        "gh", "release", "download", tag, "--repo", repo,
        "--pattern", "ohlc_*.parquet", "--dir", str(ohlc_dir), "--clobber",
    ])
```
In `main`, add a branch before the publish fallthrough:
```python
    if args.cmd == "sync":
        cmd_sync(ohlc_dir=config.OHLC_DIR, repo=config.GITHUB_REPO,
                 tag=config.RELEASE_TAG, runner=publish.subprocess_runner)
        return 0
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest -q && .venv/bin/ruff check . && .venv/bin/mypy`
Expected: full suite green (ZERO warnings), ruff clean, mypy Success.

- [ ] **Step 5: Commit**

```bash
git add pipeline/src/pipeline/cli.py pipeline/tests/test_cli.py
git commit -m "feat(pipeline): sync command to pull the release baseline before daily"
```

---

### Task 3: `data-daily.yml` workflow

**Files:**
- Create: `.github/workflows/data-daily.yml` (repo root)

**Interfaces:**
- Consumes: the `pipeline` CLI (`sync`, `daily`, `publish`).
- Produces: a scheduled + dispatchable workflow that ingests + publishes daily.

- [ ] **Step 1: Create `.github/workflows/data-daily.yml`**

```yaml
name: data-daily

on:
  schedule:
    - cron: "0 14 * * 1-5"   # 19:30 IST — shortly after NSE bhavcopy publishes
    - cron: "0 16 * * 1-5"   # 21:30 IST — late catch if the file published late
  workflow_dispatch:

permissions:
  contents: write   # gh needs release read+write

concurrency:
  group: data-daily
  cancel-in-progress: false   # never interrupt a publish mid-upload

jobs:
  ingest-and-publish:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    defaults:
      run:
        working-directory: pipeline
    env:
      GH_TOKEN: ${{ github.token }}
    steps:
      - uses: actions/checkout@v4   # pin to SHA in P2
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
      - name: Install
        run: pip install -e ".[dev]"
      - name: Sync baseline from the data-latest release
        run: python -m pipeline sync
      - name: Ingest today's trading day
        run: python -m pipeline daily
      - name: Publish the updated dataset
        run: python -m pipeline publish
```

- [ ] **Step 2: Validate the YAML locally**

Run (from repo root): `python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/data-daily.yml')); print('yaml ok')"`
Expected: `yaml ok`. (Confirms the workflow parses; behavior is validated by the live dispatch below.)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/data-daily.yml
git commit -m "ci(pipeline): daily ingest+publish workflow (two crons, sync->daily->publish)"
```

---

## Post-merge live validation (the empirical experiment — NOT a task, run by the controller after merge)

After merging to `main`, manually dispatch the workflow and observe whether NSE serves GitHub Actions IPs:
```bash
gh workflow run data-daily.yml -R SatioO/guardian-universe
gh run watch <id> -R SatioO/guardian-universe
```
- **If it succeeds** (real rows ingested + the `data-latest` release updated) → CI-based ingestion is viable; P1b is done.
- **If NSE blocks the Actions IP** (fetch fails / `not_yet` with no data despite it being a published trading day) → the daily fetch needs a mitigation (fallback library, an egress proxy, or a self-hosted/residential runner). Record the observed behavior and open a follow-up; the local `backfill`/`daily`/`publish` loop remains fully functional regardless.

## What P1b delivers (Definition of Done)

- `python -m pipeline sync` pulls the current release parquet(s) into the local store (first-run tolerant).
- A `data-daily.yml` workflow runs `sync → daily → publish` on two crons, least-privilege, idempotent (already-ingested day → no-op; second cron re-publishes identical bytes).
- Publish refuses an empty store and exits cleanly (code 1) on failure rather than dumping a traceback; the CLI no longer constructs broker adapters on the publish path.
- Full suite green, zero warnings, ruff + mypy --strict clean.
- The CI-fetch-from-Actions question is answered empirically by a live dispatch.

## Deferred (not in P1b)

- **P1c** — NSE indices dataset.
- **P2** — auto-issue-on-failure, freshness dead-man's-switch, SHA-pinned actions, Dependabot, `last_run_status.json` emission + publish.
- Distribution consistency-window hardening (immutable per-date assets) — revisit if a real consumer needs stronger atomicity than verify-then-use.

## Spec-coverage self-review

- Two-cron daily schedule (spec §5.1): Task 3. ✔
- Store persistence between ephemeral CI runs (implied by §5.1 "acquire prior state"): Task 2 (`sync` from release). ✔
- Idempotent daily (spec §5.1): reuses P0 `has_day`; second-cron no-op. ✔
- P1a deferred review items (empty-dir guard, publish clean-exit, lazy adapters): Task 1. ✔
- NSE-blocks-CI-IP risk (spec §9 row 4): surfaced as an explicit post-merge experiment, not assumed. ✔
- `last_run_status.json`, SHA-pinning, monitoring: **deferred to P2** (noted). ✔ (partial-by-design)
