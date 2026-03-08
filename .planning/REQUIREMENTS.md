# Requirements: dq_framework Health Audit & Production Hardening

**Defined:** 2026-03-08
**Core Value:** Reliable, configuration-driven data quality validation that works identically in local development and Microsoft Fabric production environments.

## v2 Requirements

Requirements for production hardening milestone. Each maps to roadmap phases.

### Packaging

- [x] **PKG-01**: Remove legacy setup.py entirely (pyproject.toml v2.0.0 is canonical)
- [x] **PKG-02**: Remove committed build artifacts (build/, dist/, htmlcov/, .coverage, pipeline.log, egg-info)
- [x] **PKG-03**: Update .gitignore to prevent re-committing build artifacts
- [x] **PKG-04**: Align CI matrix with pyproject.toml (Python 3.10-3.13, ruff instead of flake8/black/isort)
- [x] **PKG-05**: Unify dev tooling on pyproject.toml [dev] extras (remove requirements-dev.txt conflicts)
- [x] **PKG-06**: Update pre-commit config to use ruff instead of flake8/black/isort

### Bug Fixes

- [ ] **BUG-01**: Fix chunked Spark validation bug (monotonically_increasing_id misuse causes missed/inconsistent rows)
- [ ] **BUG-02**: Fix aggregated chunk results miscounting (inflated statistics from counting expectations across all chunks)
- [ ] **BUG-03**: Remove unused DataIngester.engine parameter (dead code misleading users)
- [ ] **BUG-04**: Remove or relocate stale check_data.py script with hardcoded paths
- [ ] **BUG-05**: Fix _is_fabric_runtime private function exposed in __init__.py __all__

### Test Coverage

- [x] **TEST-01**: Raise fabric_connector.py test coverage from 18% to 60%+ with Spark/Fabric mock fixtures
- [x] **TEST-02**: Raise loader.py test coverage from 51% to 60%+ covering all file formats and PyArrow path
- [x] **TEST-03**: Raise batch_profiler.py test coverage from 51% to 60%+ covering parallel processing
- [x] **TEST-04**: Add characterization tests for severity-based threshold logic (document current behavior before modifications)

### Alerting

- [ ] **ALRT-01**: Implement Microsoft Teams webhook alerting using Power Automate Workflows with Adaptive Card format
- [ ] **ALRT-02**: Implement email (SMTP) alerting with HTML-formatted messages including summary tables and failed expectations
- [ ] **ALRT-03**: Implement alert message formatting layer shared by Teams and email channels (jinja2 templates)
- [ ] **ALRT-04**: Implement severity-based alert routing (critical alerts immediately, low-severity batched/suppressed)
- [ ] **ALRT-05**: Add YAML-driven alert configuration (alerts: section in existing config YAML)
- [ ] **ALRT-06**: Fix existing _send_alert return value handling (caller ignores failures — implement failure policies)
- [ ] **ALRT-07**: Implement alert retry with circuit breaker (stop retrying after N consecutive failures)

### Schema Evolution

- [ ] **SCHM-01**: Implement schema baseline storage (persist column names, types, nullability as JSON snapshots)
- [ ] **SCHM-02**: Implement schema change detection (compare current schema vs stored baseline using deepdiff)
- [ ] **SCHM-03**: Classify schema changes as breaking (removal, type change) vs non-breaking (addition, nullability change)
- [ ] **SCHM-04**: Implement schema evolution history (track diffs over time with timestamps)
- [ ] **SCHM-05**: Auto-generate schema baseline from DataProfiler.profile() output
- [ ] **SCHM-06**: Wire schema change detection into alerting system (breaking changes trigger critical alerts)

### Validation History

- [ ] **HIST-01**: Implement structured result storage replacing scattered JSON files (Parquet in Fabric, SQLite locally)
- [ ] **HIST-02**: Define validation result record schema (timestamp, suite_name, success, success_rate, severity_stats, duration)
- [ ] **HIST-03**: Implement trend query API: get_trend(dataset, days) returning pandas DataFrames
- [ ] **HIST-04**: Implement failure history query: get_failure_history(dataset) returning failed expectations over time
- [ ] **HIST-05**: Implement period comparison: compare_periods(dataset, period_a, period_b)
- [ ] **HIST-06**: Implement configurable retention policy (retention_days with automatic cleanup)

### Storage Foundation

- [ ] **STOR-01**: Create ResultStore abstraction with JSONFileStore (local) and LakehouseStore (Fabric) backends
- [ ] **STOR-02**: Refactor existing _save_results_to_lakehouse into ResultStore backend
- [ ] **STOR-03**: Support both local (SQLite/JSON) and Fabric (Parquet/Lakehouse) storage modes

### Integration

- [ ] **INTG-01**: Wire all new components into FabricDataQualityRunner pipeline (schema check -> validate -> record history -> alert)
- [ ] **INTG-02**: Update ConfigLoader to validate new YAML config sections (alerting, history, schema_tracking)
- [ ] **INTG-03**: Update constants.py with new default values for alerting, history, and schema tracking
- [ ] **INTG-04**: Update __init__.py exports with new public classes (AlertManager, SchemaTracker, ValidationHistory)
- [ ] **INTG-05**: Align dq_framework with AIMS Data Platform dependency expectations
- [ ] **INTG-06**: End-to-end integration tests covering full pipeline with all new components

## v3 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Enhanced Alerting

- **ALRT-V3-01**: Slack integration (if org adopts Slack alongside Teams)
- **ALRT-V3-02**: Generic webhook support for custom integrations
- **ALRT-V3-03**: Daily digest emails summarizing all validation results

### Advanced Analytics

- **ANAL-V3-01**: Dashboard UI for validation trends (Power BI integration or standalone)
- **ANAL-V3-02**: Anomaly detection on validation metrics (auto-detect quality degradation)
- **ANAL-V3-03**: Cross-dataset correlation analysis

### Platform

- **PLAT-V3-01**: Native Spark validation (without pandas conversion) when GX supports it
- **PLAT-V3-02**: Real-time streaming validation support
- **PLAT-V3-03**: GX 2.x migration when stable

## Out of Scope

| Feature | Reason |
|---------|--------|
| Dashboard UI | Expose structured data for Power BI/Jupyter instead; building a UI is a separate product |
| Generic webhook alerting | Teams and email cover org needs; extensible architecture allows future additions |
| Slack integration | Microsoft-shop org uses Teams; YAGNI |
| Real-time streaming validation | Fundamentally different architecture from batch; GX 1.x is batch-oriented |
| Automatic schema migration/repair | Detection and alerting only; human decision required for config updates |
| GX Cloud integration | Self-hosted operation is the value proposition; SaaS dependency conflicts with it |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| PKG-01 | Phase 1 | Complete |
| PKG-02 | Phase 1 | Complete |
| PKG-03 | Phase 1 | Complete |
| PKG-04 | Phase 2 | Complete |
| PKG-05 | Phase 2 | Complete |
| PKG-06 | Phase 2 | Complete |
| BUG-01 | Phase 3 | Pending |
| BUG-02 | Phase 3 | Pending |
| BUG-03 | Phase 3 | Pending |
| BUG-04 | Phase 3 | Pending |
| BUG-05 | Phase 3 | Pending |
| TEST-01 | Phase 4 | Complete |
| TEST-02 | Phase 4 | Complete |
| TEST-03 | Phase 4 | Complete |
| TEST-04 | Phase 4 | Complete |
| ALRT-01 | Phase 7 | Pending |
| ALRT-02 | Phase 7 | Pending |
| ALRT-03 | Phase 6 | Pending |
| ALRT-04 | Phase 7 | Pending |
| ALRT-05 | Phase 6 | Pending |
| ALRT-06 | Phase 6 | Pending |
| ALRT-07 | Phase 6 | Pending |
| SCHM-01 | Phase 8 | Pending |
| SCHM-02 | Phase 8 | Pending |
| SCHM-03 | Phase 8 | Pending |
| SCHM-04 | Phase 8 | Pending |
| SCHM-05 | Phase 8 | Pending |
| SCHM-06 | Phase 8 | Pending |
| HIST-01 | Phase 9 | Pending |
| HIST-02 | Phase 9 | Pending |
| HIST-03 | Phase 9 | Pending |
| HIST-04 | Phase 9 | Pending |
| HIST-05 | Phase 9 | Pending |
| HIST-06 | Phase 9 | Pending |
| STOR-01 | Phase 5 | Pending |
| STOR-02 | Phase 5 | Pending |
| STOR-03 | Phase 5 | Pending |
| INTG-01 | Phase 10 | Pending |
| INTG-02 | Phase 10 | Pending |
| INTG-03 | Phase 10 | Pending |
| INTG-04 | Phase 10 | Pending |
| INTG-05 | Phase 10 | Pending |
| INTG-06 | Phase 10 | Pending |

**Coverage:**
- v2 requirements: 42 total
- Mapped to phases: 42
- Unmapped: 0 ✓

---
*Requirements defined: 2026-03-08*
*Last updated: 2026-03-08 after roadmap creation*
