---
phase: 03-bug-fixes
plan: 01
subsystem: validation
tags: [pyspark, chunked-validation, aggregation, row_number, window-function]

requires:
  - phase: 02-ci-and-tooling
    provides: Ruff linting and CI pipeline
provides:
  - Fixed chunked Spark validation with consecutive row boundaries
  - Per-expectation average aggregation with per-chunk breakdown
affects: [fabric-connector, chunked-processing, validation-results]

tech-stack:
  added: []
  patterns: [pyspark-window-functions, per-expectation-averaging]

key-files:
  created: []
  modified:
    - dq_framework/fabric_connector.py
    - tests/test_fabric_connector.py

key-decisions:
  - "row_number().over(Window.orderBy(lit(1))) preserves row order; non-deterministic across runs is acceptable for memory optimization"
  - "evaluated_checks reports per-chunk count since all chunks run the same expectation suite"
  - "Failed expectations deduplicated by (expectation_type, column) tuple"

patterns-established:
  - "Chunked aggregation: mean success_rate, not summed totals"
  - "Per-chunk breakdown in 'chunks' key for debugging"

requirements-completed: [BUG-01, BUG-02]

duration: 5min
completed: 2026-03-08
---

# Plan 03-01: Chunked Spark Validation Fixes Summary

**row_number() replaces monotonically_increasing_id for correct chunk boundaries; aggregation uses per-expectation averages with per-chunk breakdown**

## Performance

- **Duration:** 5 min
- **Completed:** 2026-03-08
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Replaced monotonically_increasing_id with row_number().over(Window.orderBy(lit(1))) for consecutive 1-based chunk boundaries (BUG-01)
- Rewrote _aggregate_chunk_results to compute mean success_rate across chunks instead of inflated sums (BUG-02)
- Added 'chunks' key with per-chunk breakdown and deduplication of failed_expectations
- 10 new tests covering chunked validation, aggregation, threshold comparison, error handling

## Task Commits

1. **Task 1+2: Tests and implementation** - `d672566` (fix)

## Files Created/Modified
- `dq_framework/fabric_connector.py` - Fixed _validate_spark_chunked and _aggregate_chunk_results methods
- `tests/test_fabric_connector.py` - Added TestChunkedValidation, TestAggregateChunkResults, TestAllExports

## Decisions Made
- Used Window.orderBy(lit(1)) to preserve existing row order without requiring a specific sort column
- evaluated_checks equals per-chunk count (not sum) since all chunks run the same suite
- Deduplicate failed_expectations by (expectation_type, column) tuple, capped at MAX_FAILURE_DISPLAY

## Deviations from Plan
- Tests and implementation committed together (single commit) instead of TDD red/green cycle due to rate limit recovery

## Issues Encountered
- pyspark not installed in test env required mocking sys.modules for Window/row_number imports
- MagicMock __getitem__ needed explicit __ge__/__le__ support for Spark column comparison operators

## Next Phase Readiness
- Chunked validation now produces correct, reliable results
- handle_failure and _save_results_to_lakehouse compatible with new result shape

---
*Phase: 03-bug-fixes*
*Completed: 2026-03-08*
