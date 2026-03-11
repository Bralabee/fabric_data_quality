# Roadmap: dq_framework Health Audit & Production Hardening

## Overview

This roadmap takes the dq_framework from a working-but-rough validation library to production-grade quality. The first four phases clean up the repo, modernize tooling, fix known bugs, and raise test coverage -- establishing a solid baseline. The next five phases build the three new capabilities (storage abstraction, alerting, schema evolution, validation history) as independent modules that consume validation output. The final phase wires everything into the FabricDataQualityRunner pipeline. Each phase delivers a coherent, verifiable capability.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Repo Cleanup** - Remove legacy files, build artifacts, and fix .gitignore (completed 2026-03-08)
- [x] **Phase 2: CI and Tooling** - Align CI matrix and dev tooling on ruff/pyproject.toml (completed 2026-03-08)
- [x] **Phase 3: Bug Fixes** - Fix all known bugs in validation, ingestion, and public API (completed 2026-03-08)
- [x] **Phase 4: Test Coverage** - Raise module coverage to 60%+ and add characterization tests (completed 2026-03-08)
- [x] **Phase 5: Storage Abstraction** - Build ResultStore with local and Fabric backends (completed 2026-03-08)
- [x] **Phase 6: Alert Infrastructure** - Build alert formatting, config, failure handling, and retry (completed 2026-03-09)
- [x] **Phase 7: Alert Channels** - Implement Teams and email channels with severity routing (completed 2026-03-09)
- [x] **Phase 8: Schema Evolution** - Detect, classify, and track schema changes over time (completed 2026-03-10)
- [x] **Phase 9: Validation History** - Record results and provide trend analysis queries (completed 2026-03-10)
- [x] **Phase 10: Pipeline Integration** - Wire all components into FabricRunner and update public API (completed 2026-03-10)

## Phase Details

### Phase 1: Repo Cleanup
**Goal**: The repository has a single, authoritative packaging configuration with no legacy files or committed artifacts
**Depends on**: Nothing (first phase)
**Requirements**: PKG-01, PKG-02, PKG-03
**Success Criteria** (what must be TRUE):
  1. setup.py does not exist in the repository
  2. No build artifacts (build/, dist/, htmlcov/, .coverage, pipeline.log, egg-info) exist in the repository
  3. .gitignore prevents re-committing all known artifact patterns
  4. `pip install -e .` succeeds using only pyproject.toml
**Plans**: 2 plans

Plans:
- [ ] 01-01-PLAN.md -- Remove setup.py and update documentation references
- [x] 01-02-PLAN.md -- Clean build artifacts and verify .gitignore coverage

### Phase 2: CI and Tooling
**Goal**: All development tooling uses a single source of truth (pyproject.toml) and CI runs the correct Python versions with ruff
**Depends on**: Phase 1
**Requirements**: PKG-04, PKG-05, PKG-06
**Success Criteria** (what must be TRUE):
  1. CI matrix tests Python 3.10, 3.11, 3.12, and 3.13
  2. CI uses ruff for linting and formatting (no flake8, black, or isort references remain)
  3. requirements-dev.txt is removed; all dev dependencies are in pyproject.toml [dev] extras
  4. pre-commit hooks run ruff lint and ruff format
**Plans**: 2 plans

Plans:
- [ ] 02-01-PLAN.md -- Rewrite CI workflow for ruff/3.10-3.13 and delete requirements-dev.txt
- [ ] 02-02-PLAN.md -- Replace legacy pre-commit hooks with ruff

### Phase 3: Bug Fixes
**Goal**: All known validation, ingestion, and public API bugs are resolved
**Depends on**: Phase 2
**Requirements**: BUG-01, BUG-02, BUG-03, BUG-04, BUG-05
**Success Criteria** (what must be TRUE):
  1. Chunked Spark validation produces consistent row-level results without monotonically_increasing_id misuse
  2. Aggregated chunk results report correct counts (no inflation from counting across all chunks)
  3. DataIngester has no unused engine parameter; check_data.py is removed or relocated
  4. __init__.py __all__ contains only public API symbols (no _is_fabric_runtime)
**Plans**: 2 plans

Plans:
- [x] 03-01-PLAN.md -- Fix chunked Spark validation (row_number) and aggregation (per-expectation averages)
- [x] 03-02-PLAN.md -- Remove DataIngester.engine, delete check_data.py, fix __all__ exports

### Phase 4: Test Coverage
**Goal**: Every module meets the 60% coverage minimum and threshold behavior is documented with characterization tests
**Depends on**: Phase 3
**Requirements**: TEST-01, TEST-02, TEST-03, TEST-04
**Success Criteria** (what must be TRUE):
  1. fabric_connector.py test coverage is 60% or higher (up from 18%)
  2. loader.py test coverage is 60% or higher (up from 51%)
  3. batch_profiler.py test coverage is 60% or higher (up from 51%)
  4. Characterization tests exist that document current severity-based threshold behavior and pass
  5. Reusable Spark/Fabric mock fixtures exist for use in later integration testing phases
**Plans**: 3 plans

Plans:
- [ ] 04-01-PLAN.md -- Build Spark/Fabric mock fixtures and raise fabric_connector.py coverage to 60%+
- [x] 04-02-PLAN.md -- Rewrite loader.py and batch_profiler.py tests to pytest style and raise coverage to 60%+
- [ ] 04-03-PLAN.md -- Add characterization tests for severity-based threshold logic

### Phase 5: Storage Abstraction
**Goal**: A unified storage interface supports both local and Fabric backends for result persistence
**Depends on**: Phase 1
**Requirements**: STOR-01, STOR-02, STOR-03
**Success Criteria** (what must be TRUE):
  1. ResultStore abstract class exists with read/write/list/delete operations
  2. JSONFileStore backend works for local development (files in a configurable directory)
  3. LakehouseStore backend replaces inline _save_results_to_lakehouse code
  4. Storage backend is selected automatically based on runtime detection (local vs Fabric)
**Plans**: 2 plans

Plans:
- [ ] 05-01-PLAN.md -- Create ResultStore ABC with JSONFileStore and LakehouseStore backends plus tests
- [ ] 05-02-PLAN.md -- Refactor FabricDataQualityRunner to use ResultStore and update public API

### Phase 6: Alert Infrastructure
**Goal**: A shared alert formatting and delivery framework exists with failure handling and YAML configuration
**Depends on**: Phase 1
**Requirements**: ALRT-03, ALRT-05, ALRT-06, ALRT-07
**Success Criteria** (what must be TRUE):
  1. Jinja2 templates render alert messages with validation summary, failed expectations, and severity
  2. YAML config supports an alerts: section with channel configuration and routing rules
  3. Alert delivery failures are handled with explicit policies (warn/raise/fallback), not silently ignored
  4. Circuit breaker stops retrying after N consecutive failures and recovers after a cooldown period
**Plans**: 2 plans

Plans:
- [ ] 06-01-PLAN.md -- Create alerting subpackage with Jinja2 formatter, alert config parsing, and default templates
- [ ] 06-02-PLAN.md -- Implement circuit breaker state machine and AlertDispatcher with failure policies

### Phase 7: Alert Channels
**Goal**: Validation results trigger alerts via Teams and email with severity-based routing
**Depends on**: Phase 6
**Requirements**: ALRT-01, ALRT-02, ALRT-04
**Success Criteria** (what must be TRUE):
  1. Teams webhook sends Adaptive Card formatted messages via Power Automate Workflows URL
  2. Email channel sends HTML-formatted messages with summary tables via SMTP
  3. Critical-severity failures trigger immediate alerts; low-severity results are batched or suppressed
  4. Both channels render correctly with validation result data (not raw JSON)
**Plans**: 2 plans

Plans:
- [ ] 07-01-PLAN.md -- Implement TeamsChannel and EmailChannel with Adaptive Card template and channel factory
- [ ] 07-02-PLAN.md -- Implement SeverityRouter and integrate severity-based routing into AlertDispatcher

### Phase 8: Schema Evolution
**Goal**: Schema changes are automatically detected, classified, tracked over time, and surfaced as alerts
**Depends on**: Phase 5, Phase 7
**Requirements**: SCHM-01, SCHM-02, SCHM-03, SCHM-04, SCHM-05, SCHM-06
**Success Criteria** (what must be TRUE):
  1. Schema baselines (column names, types, nullability) are persisted as JSON snapshots via ResultStore
  2. Schema changes are detected by comparing current schema against stored baseline using deepdiff
  3. Changes are classified as breaking (removal, type change) or non-breaking (addition, nullability)
  4. Schema evolution history records all diffs with timestamps for audit purposes
  5. Breaking schema changes trigger critical alerts through the alerting system
**Plans**: 2 plans

Plans:
- [x] 08-01-PLAN.md -- SchemaTracker core: baseline CRUD, deepdiff detection, change classification, baseline from profiler
- [ ] 08-02-PLAN.md -- Schema history tracking, alert wiring for breaking changes, public API export

### Phase 9: Validation History
**Goal**: Validation results are stored in structured format with trend analysis and retention policies
**Depends on**: Phase 5
**Requirements**: HIST-01, HIST-02, HIST-03, HIST-04, HIST-05, HIST-06
**Success Criteria** (what must be TRUE):
  1. Validation results are stored in structured format (Parquet in Fabric, SQLite locally) via ResultStore
  2. Result records include timestamp, suite_name, success, success_rate, severity_stats, and duration
  3. get_trend(dataset, days) returns a pandas DataFrame showing quality metrics over time
  4. get_failure_history(dataset) returns failed expectations with frequency and recency data
  5. compare_periods(dataset, period_a, period_b) identifies quality changes between time ranges
  6. Retention policy automatically cleans up records older than configured retention_days
**Plans**: 2 plans

Plans:
- [x] 09-01-PLAN.md -- ValidationHistory class with dual-backend storage (SQLite/Parquet) and record() method
- [ ] 09-02-PLAN.md -- Query APIs (get_trend, get_failure_history, compare_periods) and retention policy

### Phase 10: Pipeline Integration
**Goal**: All new components are wired into FabricRunner and the library's public API is updated
**Depends on**: Phase 4, Phase 7, Phase 8, Phase 9
**Requirements**: INTG-01, INTG-02, INTG-03, INTG-04, INTG-05, INTG-06
**Success Criteria** (what must be TRUE):
  1. FabricDataQualityRunner pipeline executes: schema check -> validate -> record history -> alert
  2. ConfigLoader validates new YAML sections (alerting, history, schema_tracking) with helpful error messages
  3. constants.py has default values for all new configuration; __init__.py exports AlertManager, SchemaTracker, ValidationHistory
  4. dq_framework dependency versions are compatible with AIMS Data Platform (GX version resolved)
  5. End-to-end integration tests cover the full pipeline with all new components using mock fixtures
**Plans**: 3 plans

Plans:
- [x] 10-01-PLAN.md -- Constants, public API exports, and ConfigLoader optional section validation
- [x] 10-02-PLAN.md -- Wire SchemaTracker, ValidationHistory, AlertDispatcher into FabricDataQualityRunner pipeline
- [x] 10-03-PLAN.md -- End-to-end integration tests and final regression verification

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8 -> 9 -> 10
Note: Phases 5-7 and 8-9 have independent dependency chains. Parallelization is possible but not required.

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Repo Cleanup | 1/2 | In progress | - |
| 2. CI and Tooling | 0/2 | Complete    | 2026-03-08 |
| 3. Bug Fixes | 2/2 | Complete    | 2026-03-08 |
| 4. Test Coverage | 3/3 | Complete   | 2026-03-08 |
| 5. Storage Abstraction | 0/2 | Not started | - |
| 6. Alert Infrastructure | 2/2 | Complete   | 2026-03-09 |
| 7. Alert Channels | 2/2 | Complete   | 2026-03-09 |
| 8. Schema Evolution | 2/2 | Complete   | 2026-03-10 |
| 9. Validation History | 2/2 | Complete   | 2026-03-10 |
| 10. Pipeline Integration | 3/3 | Complete    | 2026-03-10 |
