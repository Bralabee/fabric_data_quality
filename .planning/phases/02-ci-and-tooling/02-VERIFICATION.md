---
phase: 02-ci-and-tooling
verified: 2026-03-08T21:30:00Z
status: passed
score: 4/4 must-haves verified
re_verification: false
---

# Phase 2: CI and Tooling Verification Report

**Phase Goal:** All development tooling uses a single source of truth (pyproject.toml) and CI runs the correct Python versions with ruff
**Verified:** 2026-03-08T21:30:00Z
**Status:** passed
**Re-verification:** No -- initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | CI matrix tests Python 3.10, 3.11, 3.12, and 3.13 | VERIFIED | `.github/workflows/ci.yml` line 35: `python-version: ["3.10", "3.11", "3.12", "3.13"]` |
| 2 | CI uses ruff for linting and formatting (no flake8, black, or isort references remain) | VERIFIED | ci.yml uses `ruff check --output-format=github .` (line 24) and `ruff format --check .` (line 27). grep confirms no flake8/black/isort in `.github/workflows/` |
| 3 | requirements-dev.txt is removed; all dev dependencies are in pyproject.toml [dev] extras | VERIFIED | `requirements-dev.txt` does not exist. `pyproject.toml` lines 48-55 define `[project.optional-dependencies].dev`. CI installs via `pip install -e ".[dev]"` (line 57). grep confirms no references in Makefile, CONTRIBUTING.md, or docs/ |
| 4 | pre-commit hooks run ruff lint and ruff format | VERIFIED | `.pre-commit-config.yaml` uses `astral-sh/ruff-pre-commit` v0.15.5 with hooks `ruff-check` (with `--fix`) before `ruff-format`. No black/isort/flake8/pydocstyle/safety hooks present |

**Score:** 4/4 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `.github/workflows/ci.yml` | CI workflow with ruff linting and 3.10-3.13 matrix | VERIFIED | 118 lines, contains `ruff check`, matrix with 4 Python versions, `pip install -e ".[dev]"`, lint/test/build/security jobs |
| `requirements-dev.txt` | MUST NOT EXIST (deleted) | VERIFIED | File does not exist |
| `.pre-commit-config.yaml` | Pre-commit config with ruff hooks replacing black/isort/flake8 | VERIFIED | 45 lines, uses `astral-sh/ruff-pre-commit`, correct hook order, mypy/bandit/pre-commit-hooks preserved |
| `pyproject.toml` | Source of truth for dev deps and ruff config | VERIFIED | `[project.optional-dependencies].dev` defined (lines 48-55), `[tool.ruff]` fully configured (lines 77-103) |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `.github/workflows/ci.yml` | `pyproject.toml` | `pip install -e ".[dev]"` | WIRED | Line 57: `pip install -e ".[dev]"` reads `[project.optional-dependencies].dev` |
| `.pre-commit-config.yaml` | `pyproject.toml [tool.ruff]` | ruff hooks read lint/format config | WIRED | `astral-sh/ruff-pre-commit` present; ruff automatically reads `[tool.ruff]` from pyproject.toml |
| `Makefile` | `pyproject.toml` | ruff commands in lint/format targets | WIRED | Makefile lint uses `ruff check .`, format uses `ruff format .` and `ruff check --fix .` |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| PKG-04 | 02-01-PLAN | Align CI matrix with pyproject.toml (Python 3.10-3.13, ruff instead of flake8/black/isort) | SATISFIED | CI matrix exactly matches pyproject.toml classifiers; ruff replaces all legacy linters |
| PKG-05 | 02-01-PLAN | Unify dev tooling on pyproject.toml [dev] extras (remove requirements-dev.txt conflicts) | SATISFIED | requirements-dev.txt deleted; all dev installs use `pip install -e ".[dev]"` |
| PKG-06 | 02-02-PLAN | Update pre-commit config to use ruff instead of flake8/black/isort | SATISFIED | Pre-commit config uses ruff-pre-commit with ruff-check and ruff-format; all legacy hooks removed |

No orphaned requirements found -- REQUIREMENTS.md maps PKG-04, PKG-05, PKG-06 to Phase 2, and all three are claimed by plans.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `.github/COPILOT.md` | 168-214 | Stale references to `black>=23.0.0`, `flake8>=6.0.0`, "Format with black" | Info | COPILOT.md is a GitHub Copilot instruction file with outdated tooling references. Not an operational file -- does not affect CI or pre-commit behavior. Can be updated in a future housekeeping pass. |

### Human Verification Required

### 1. CI Workflow Execution

**Test:** Push a commit to a branch and observe CI run
**Expected:** lint job runs ruff check and ruff format --check; test job runs on 3.10, 3.11, 3.12, 3.13; build job produces dist/; security job runs bandit
**Why human:** CI workflow correctness can only be fully confirmed by running it on GitHub Actions

### 2. Pre-commit Hook Execution

**Test:** Run `pre-commit run --all-files` locally
**Expected:** ruff-check and ruff-format hooks execute (may report existing lint issues in code, which is expected and not a blocker). No errors about missing hooks or invalid configuration.
**Why human:** Hook execution depends on local environment and ruff version resolution

### Gaps Summary

No gaps found. All four success criteria are met:

1. CI matrix correctly tests Python 3.10, 3.11, 3.12, and 3.13 (verified in ci.yml)
2. CI uses ruff exclusively for linting/formatting with no legacy tool references in operational files
3. requirements-dev.txt is deleted with no remaining references in Makefile, docs, or CI
4. Pre-commit hooks use ruff-check and ruff-format in correct order from astral-sh/ruff-pre-commit

The only minor observation is stale legacy tool references in `.github/COPILOT.md`, which is informational only and does not affect any operational tooling.

---

_Verified: 2026-03-08T21:30:00Z_
_Verifier: Claude (gsd-verifier)_
