---
phase: 01-repo-cleanup
plan: 01
subsystem: packaging
tags: [pyproject.toml, setuptools, pep517, pep660, editable-install]

# Dependency graph
requires: []
provides:
  - "Single authoritative packaging config via pyproject.toml"
  - "Package discovery config for flat-layout repos"
  - "Editable install works without setup.py"
affects: [02-ci-tooling, packaging, deployment]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "[tool.setuptools.packages.find] with explicit include for flat-layout repos"
    - "python -m build --wheel as standard build command (PEP 517)"

key-files:
  created: []
  modified:
    - "pyproject.toml (added package discovery config)"
    - "docs/FABRIC_INTEGRATION.md (build command updated)"
    - "docs/FILE_STRUCTURE.md (removed setup.py entry)"
    - "docs/PROJECT_SUMMARY.md (removed setup.py entry)"
    - "README.md (removed setup.py from structure listing)"
    - ".planning/codebase/STRUCTURE.md (removed setup.py references)"
    - ".planning/codebase/STACK.md (removed setup.py from build tools)"

key-decisions:
  - "Added [tool.setuptools.packages.find] include=['dq_framework*'] to fix flat-layout auto-discovery error"

patterns-established:
  - "pyproject.toml is the single source of truth for packaging, build, and tool config"

requirements-completed: [PKG-01]

# Metrics
duration: 3min
completed: 2026-03-08
---

# Phase 1 Plan 1: Remove setup.py Summary

**Deleted legacy setup.py (v1.2.0), fixed flat-layout package discovery in pyproject.toml, and updated all documentation to use PEP 517 build commands**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-08T20:03:42Z
- **Completed:** 2026-03-08T20:07:16Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments
- Removed setup.py which had conflicting version (1.2.0 vs pyproject.toml 2.0.0) and outdated GX <1.0 dependencies
- Fixed flat-layout package discovery by adding `[tool.setuptools.packages.find]` with `include = ["dq_framework*"]` to pyproject.toml
- Verified editable install succeeds: `pip install -e .` works, `import dq_framework` returns v2.0.0
- All 213 tests pass with no regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: Remove setup.py and verify editable install** - `c454cc2` (feat)
2. **Task 2: Update documentation references** - No new commit needed; documentation was already updated by prior plan 01-02 execution (`f44db92`)

## Files Created/Modified
- `setup.py` - DELETED (was legacy v1.2.0 packaging config with outdated deps)
- `pyproject.toml` - Added `[tool.setuptools.packages.find]` section to fix flat-layout discovery
- `docs/FABRIC_INTEGRATION.md` - Build command changed to `python -m build --wheel`
- `docs/FILE_STRUCTURE.md` - Removed setup.py entry from file listing
- `docs/PROJECT_SUMMARY.md` - Removed setup.py entry from additional files listing
- `README.md` - Removed setup.py from project structure listing
- `.planning/codebase/STRUCTURE.md` - Removed setup.py from directory layout and build description
- `.planning/codebase/STACK.md` - Removed setup.py from build tools listing

## Decisions Made
- Added `[tool.setuptools.packages.find]` with `include = ["dq_framework*"]` to pyproject.toml because the flat-layout auto-discovery found multiple top-level directories (webapp, config, dq_framework, config_templates, sample_source_data) and refused to build. This restricts the installable package to only `dq_framework`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed flat-layout package discovery error**
- **Found during:** Task 1 (Remove setup.py and verify editable install)
- **Issue:** `pip install -e .` failed with "Multiple top-level packages discovered in a flat-layout" because setuptools found webapp, config, dq_framework, config_templates, sample_source_data as potential packages
- **Fix:** Added `[tool.setuptools.packages.find]` section to pyproject.toml with `include = ["dq_framework*"]`
- **Files modified:** pyproject.toml
- **Verification:** `pip install -e .` succeeds, `python -c "import dq_framework"` returns v2.0.0
- **Committed in:** c454cc2 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking issue)
**Impact on plan:** Essential fix -- without package discovery config, editable installs fail entirely. No scope creep.

## Issues Encountered
- Task 2 documentation changes were already applied by a prior execution of plan 01-02 (`f44db92`). The edits were confirmed as no-ops and no separate commit was needed.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Packaging is clean: pyproject.toml is the single source of truth
- All documentation references updated to PEP 517 commands
- Ready for Phase 2 (CI and Tooling) -- pyproject.toml already has ruff, pytest, mypy, coverage configs

---
*Phase: 01-repo-cleanup*
*Completed: 2026-03-08*
