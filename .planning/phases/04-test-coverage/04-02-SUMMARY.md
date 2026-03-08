---
phase: 04-test-coverage
plan: 02
subsystem: testing
tags: [pytest, coverage, mocking, mocker, pyarrow, batch-profiling]

requires:
  - phase: 03-bug-fixes
    provides: "Stable loader.py and batch_profiler.py without import/runtime errors"
provides:
  - "91%+ test coverage for loader.py (CSV, parquet, Excel, JSON, error paths)"
  - "98%+ test coverage for batch_profiler.py (single file, parallel, filtering)"
  - "Pytest-style test patterns with mocker fixtures for remaining test files"
affects: [05-storage-abstraction, 08-schema-evolution]

tech-stack:
  added: []
  patterns: [pytest-classes-with-mocker, FileSystemHandler-mock-helper]

key-files:
  created: []
  modified:
    - tests/test_loader.py
    - tests/test_batch_profiler.py

key-decisions:
  - "Mock pa.Table via module-level attribute swap instead of mocker.patch (pyarrow C extension is immutable)"
  - "Mock ProcessPoolExecutor and as_completed to avoid spawning real processes in tests"

patterns-established:
  - "_stub_fs helper: reusable FileSystemHandler mock setup for loader tests"
  - "_mock_profiler_pipeline helper: reusable DataLoader+DataProfiler+FSH setup for batch_profiler tests"

requirements-completed: [TEST-02, TEST-03]

duration: 3min
completed: 2026-03-08
---

# Phase 4 Plan 2: Loader & Batch Profiler Test Coverage Summary

**Pytest-style test suites for loader.py (91.67%) and batch_profiler.py (98.15%) covering all file formats, error paths, and parallel execution**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-08T22:32:39Z
- **Completed:** 2026-03-08T22:35:33Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Raised loader.py coverage from 51% to 91.67% with 12 pytest tests covering CSV encoding fallback, PyArrow sampling/fallback, Excel, JSON, ABFSS, unsupported formats, and file-not-found
- Raised batch_profiler.py coverage from 51% to 98.15% with 7 pytest tests covering process_single_file (success, error, thresholds) and run_parallel_profiling (success, no files, nonexistent dir, unsupported filtering)
- Eliminated all unittest.TestCase usage and sys.path.insert hacks from both test files

## Task Commits

Each task was committed atomically:

1. **Task 1: Rewrite test_loader.py with pytest style and full format coverage** - `083e933` (test)
2. **Task 2: Rewrite test_batch_profiler.py with pytest style and parallel coverage** - `99e3634` (test)

## Files Created/Modified
- `tests/test_loader.py` - 12 pytest tests covering all DataLoader code paths (CSV, parquet, Excel, JSON, errors, ABFSS)
- `tests/test_batch_profiler.py` - 7 pytest tests covering BatchProfiler single-file and parallel execution paths

## Decisions Made
- Used module-level attribute swap for `pa.Table.from_batches` mock because pyarrow's C extension types are immutable and reject `mocker.patch`
- Mocked `ProcessPoolExecutor` and `as_completed` rather than spawning real processes, ensuring fast deterministic tests

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed pa.Table immutable type mock approach**
- **Found during:** Task 1 (PyArrow sampling test)
- **Issue:** `mocker.patch("dq_framework.loader.pa.Table.from_batches")` fails because pyarrow.lib.Table is a C extension type that rejects setattr
- **Fix:** Replaced with direct module attribute swap: `_loader_mod.pa = mock_pa` with try/finally cleanup
- **Files modified:** tests/test_loader.py
- **Verification:** All 12 tests pass
- **Committed in:** 083e933

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Necessary workaround for pyarrow C extension immutability. No scope creep.

## Issues Encountered
None beyond the pyarrow mocking deviation documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Both loader and batch_profiler modules have comprehensive test coverage (91%+ and 98%+)
- Established reusable mock helper patterns for future test files
- Ready for Phase 4 Plan 3 (remaining module coverage) or Phase 5 (storage abstraction)

---
*Phase: 04-test-coverage*
*Completed: 2026-03-08*
