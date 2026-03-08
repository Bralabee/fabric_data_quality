---
phase: 02-ci-and-tooling
plan: 02
subsystem: infra
tags: [ruff, pre-commit, linting, formatting]

# Dependency graph
requires:
  - phase: 01-repo-cleanup
    provides: pyproject.toml with [tool.ruff] configuration
provides:
  - Pre-commit config using ruff-check and ruff-format from astral-sh/ruff-pre-commit
  - Removal of black, isort, flake8, pydocstyle, and safety hooks
affects: [03-bug-fixes, 04-test-coverage]

# Tech tracking
tech-stack:
  added: [ruff-pre-commit v0.15.5]
  patterns: [unified-linter-formatter via ruff]

key-files:
  created: []
  modified: [.pre-commit-config.yaml]

key-decisions:
  - "Removed safety hook entirely rather than replacing (freemium model, references requirements*.txt which may not exist)"

patterns-established:
  - "ruff-check before ruff-format: hook order matters because --fix can produce code needing formatting"

requirements-completed: [PKG-06]

# Metrics
duration: 1min
completed: 2026-03-08
---

# Phase 2 Plan 2: Pre-commit Ruff Migration Summary

**Replaced black/isort/flake8/pydocstyle/safety hooks with astral-sh/ruff-pre-commit (ruff-check + ruff-format)**

## Performance

- **Duration:** 1 min
- **Started:** 2026-03-08T21:08:24Z
- **Completed:** 2026-03-08T21:09:04Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Replaced 5 legacy linting/formatting hooks (black, isort, flake8, pydocstyle, safety) with 2 ruff hooks
- Maintained correct hook ordering: ruff-check (with --fix) before ruff-format
- Preserved mypy, bandit, and pre-commit-hooks repos unchanged

## Task Commits

Each task was committed atomically:

1. **Task 1: Replace legacy linting hooks with ruff in pre-commit config** - `0536164` (feat)

## Files Created/Modified
- `.pre-commit-config.yaml` - Replaced legacy linting hooks with ruff-pre-commit; removed black, isort, flake8, pydocstyle, safety

## Decisions Made
- Removed safety hook entirely rather than replacing with an alternative (safety has gone freemium and references requirements*.txt files)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Pre-commit hooks fully migrated to ruff; ready for Phase 3 (Bug Fixes) and Phase 4 (Test Coverage)
- Developers should run `pre-commit install` to activate updated hooks locally

---
*Phase: 02-ci-and-tooling*
*Completed: 2026-03-08*
