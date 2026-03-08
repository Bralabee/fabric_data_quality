---
phase: 02-ci-and-tooling
plan: 01
subsystem: infra
tags: [ruff, ci, github-actions, linting, python-matrix]

requires:
  - phase: 01-repo-cleanup
    provides: Clean pyproject.toml with ruff config and [dev] extras
provides:
  - CI workflow with ruff linting replacing flake8/black/isort
  - Python 3.10-3.13 test matrix aligned to pyproject.toml classifiers
  - Single source of truth for dev dependencies (pyproject.toml [dev] extras)
affects: [02-ci-and-tooling, 04-test-coverage]

tech-stack:
  added: [ruff (CI lint), bandit (CI security)]
  patterns: [pip install -e ".[dev]" for all dev installs, ruff for all lint/format]

key-files:
  created: []
  modified:
    - .github/workflows/ci.yml
    - Makefile
    - CONTRIBUTING.md
    - docs/DEVELOPMENT.md
    - docs/SETUP_COMPLETE.md
    - requirements.txt

key-decisions:
  - "Use ruff for all linting and formatting in CI and Makefile (replacing flake8, black, isort, pylint)"
  - "Remove safety check from CI security job (per research recommendation)"
  - "Upload codecov on Python 3.12 (was 3.10)"

patterns-established:
  - "All dev dependency installs use pip install -e '.[dev]' -- never requirements-dev.txt"
  - "CI lint uses ruff check --output-format=github and ruff format --check"

requirements-completed: [PKG-04, PKG-05]

duration: 2min
completed: 2026-03-08
---

# Phase 2 Plan 1: CI Workflow and Dev Tooling Unification Summary

**Ruff-based CI workflow with Python 3.10-3.13 matrix, unified dev dependencies via pyproject.toml [dev] extras**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-08T21:08:24Z
- **Completed:** 2026-03-08T21:10:13Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Rewrote CI workflow to use ruff for linting and formatting (replacing flake8, black, isort)
- Updated test matrix from Python 3.8-3.11 to 3.10-3.13 matching pyproject.toml classifiers
- Deleted requirements-dev.txt and updated all references across Makefile, docs, and CONTRIBUTING.md
- Unified all dev dependency installs to use `pip install -e ".[dev]"`

## Task Commits

Each task was committed atomically:

1. **Task 1: Rewrite CI workflow for ruff and Python 3.10-3.13** - `0536164` (feat)
2. **Task 2: Delete requirements-dev.txt** - `280b40f` (chore)

## Files Created/Modified
- `.github/workflows/ci.yml` - Rewritten CI workflow with ruff linting, 3.10-3.13 matrix, pip install -e ".[dev]"
- `Makefile` - Updated lint/format targets to use ruff, removed requirements-dev.txt from install-dev
- `CONTRIBUTING.md` - Updated setup instructions to use pip install -e ".[dev]"
- `docs/DEVELOPMENT.md` - Updated install instructions and Python version requirements
- `docs/SETUP_COMPLETE.md` - Updated dependency file references
- `requirements.txt` - Updated comment to reference pip install -e ".[dev]"

## Decisions Made
- Used ruff for all linting and formatting in CI and Makefile, replacing flake8, black, isort, and pylint
- Removed safety check from CI security job (per phase 2 research recommendation)
- Changed codecov upload trigger from Python 3.10 to 3.12

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Updated stale references to requirements-dev.txt in Makefile, docs, and CONTRIBUTING.md**
- **Found during:** Task 2 (Delete requirements-dev.txt)
- **Issue:** Multiple files (Makefile, CONTRIBUTING.md, docs/DEVELOPMENT.md, docs/SETUP_COMPLETE.md, requirements.txt) still referenced requirements-dev.txt
- **Fix:** Updated all references to use `pip install -e ".[dev]"` instead
- **Files modified:** Makefile, CONTRIBUTING.md, docs/DEVELOPMENT.md, docs/SETUP_COMPLETE.md, requirements.txt
- **Verification:** grep confirmed no remaining references in key files
- **Committed in:** 280b40f (Task 2 commit)

**2. [Rule 2 - Missing Critical] Updated Makefile lint/format targets from flake8/black/isort to ruff**
- **Found during:** Task 2 (while updating Makefile for requirements-dev.txt removal)
- **Issue:** Makefile lint target used flake8 and pylint; format target used black and isort -- inconsistent with CI workflow now using ruff
- **Fix:** Replaced all lint/format targets with ruff equivalents
- **Files modified:** Makefile
- **Verification:** Makefile lint/format commands now use ruff check and ruff format
- **Committed in:** 280b40f (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (2 missing critical)
**Impact on plan:** Both auto-fixes necessary for consistency -- leaving stale references would cause developer confusion and broken workflows. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- CI workflow ready for use; will validate on next push to main/develop
- Pre-commit hook migration (plan 02-02) can proceed independently
- Makefile now aligned with CI tooling choices

---
*Phase: 02-ci-and-tooling*
*Completed: 2026-03-08*
