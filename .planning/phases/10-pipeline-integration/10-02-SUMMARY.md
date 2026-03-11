---
phase: 10-pipeline-integration
plan: 02
subsystem: pipeline
tags: [fabric-connector, pipeline-wiring, schema-tracking, validation-history, alerting]

requires:
  - phase: 10-pipeline-integration
    plan: 01
    provides: Config contracts, constants, public API exports
  - phase: 06-alert-infrastructure
    provides: AlertConfig, AlertDispatcher, AlertFormatter
  - phase: 08-schema-evolution
    provides: SchemaTracker with check_and_alert
  - phase: 09-validation-history
    provides: ValidationHistory with record and apply_retention
provides:
  - FabricDataQualityRunner with full 4-stage pipeline (schema -> validate -> history -> alert)
  - Fire-and-forget error handling for each pipeline stage
  - Deprecated _send_alert with AlertDispatcher delegation
affects: [10-03-e2e-tests]

tech-stack:
  added: []
  patterns: [fire-and-forget-pipeline-stages, lazy-component-initialization]

key-files:
  created: []
  modified:
    - dq_framework/fabric_connector.py
    - tests/test_fabric_connector.py

key-decisions:
  - "getattr fallback for ChannelConfig.name since field does not exist on dataclass"
  - "Fire-and-forget pattern: each pipeline stage wrapped in try/except, failures logged not propagated"
  - "Schema check runs before validation, history and alerting run after"

duration: 5min
completed: 2026-03-10
---

# Phase 10 Plan 02: Pipeline Wiring Summary

**4-stage pipeline (schema check -> validate -> history -> alert) wired into FabricDataQualityRunner with fire-and-forget error isolation per stage**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-10T18:30:04Z
- **Completed:** 2026-03-10T18:35:32Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- FabricDataQualityRunner lazily initializes AlertDispatcher, SchemaTracker, and ValidationHistory from config sections
- validate_spark_dataframe executes full 4-stage pipeline: schema check -> validate -> record history -> alert on failure
- Each pipeline stage is fire-and-forget (exception in one does not block others)
- _send_alert deprecated with DeprecationWarning, delegates to AlertDispatcher when available
- Helper methods _build_schema_from_df and _determine_severity added
- Results dict augmented with schema_check and history_recorded metadata keys
- 52 tests pass in test_fabric_connector.py (20 new pipeline tests)
- 90 tests pass across full regression (config_loader + fabric_connector + integration_pipeline)

## Task Commits

Each task was committed atomically:

1. **Task 1 RED: Failing tests for lazy init and helpers** - `a923f0a` (test)
2. **Task 1 GREEN: Lazy init, helpers, deprecation** - `0dac09d` (feat)
3. **Task 2 GREEN: Pipeline stages in validate_spark_dataframe** - `a7b100c` (feat)

## Files Created/Modified

- `dq_framework/fabric_connector.py` - Added imports for alerting/schema/history, lazy init in __init__, _build_schema_from_df, _determine_severity, pipeline stages in validate_spark_dataframe, _send_alert deprecation
- `tests/test_fabric_connector.py` - Added TestLazyComponentInitialization (8 tests), TestBuildSchemaFromDf (1 test), TestDetermineSeverity (2 tests), TestPipelineStages (9 tests)

## Decisions Made

- ChannelConfig has no `name` field; use getattr fallback with `{type}_{idx}` naming
- Fire-and-forget pattern: each pipeline stage wrapped in try/except, failures logged not propagated
- Schema check runs before validation; history and alerting run after store.write

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] ChannelConfig.name attribute missing**
- **Found during:** Task 1
- **Issue:** ChannelConfig dataclass has no `name` field; code referenced `ch_cfg.name`
- **Fix:** Used `getattr(ch_cfg, "name", None) or f"{ch_cfg.type}_{idx}"` fallback
- **Files modified:** dq_framework/fabric_connector.py
- **Commit:** 0dac09d

**2. [Rule 1 - Bug] AlertConfig.from_dict nests settings incorrectly**
- **Found during:** Task 1
- **Issue:** Putting `webhook_url` inside `settings` dict in test config caused double-nesting
- **Fix:** Fixed test config to put `webhook_url` at channel level (from_dict extracts it into settings automatically)
- **Files modified:** tests/test_fabric_connector.py
- **Commit:** 0dac09d

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Pipeline wiring complete; Plan 03 (E2E tests) can test full end-to-end flows
- All pipeline stages are testable via mocks (fire-and-forget pattern)

## Self-Check: PASSED

All 2 files verified present. All 3 commits verified in git log.
