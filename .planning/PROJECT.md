# dq_framework — Health Audit & Production Hardening

## What This Is

A Python data quality library (`dq_framework`) that wraps Great Expectations 1.x with YAML-driven configuration, designed for dual-platform use: local development (pandas) and Microsoft Fabric (Spark/Delta/Lakehouse). It serves as the validation engine for the AIMS Data Platform. This audit brings the framework to production-grade quality — fixing bugs, resolving internal conflicts, raising test coverage, modernizing tooling, and implementing missing features (alerting, schema evolution, validation history).

## Core Value

Reliable, configuration-driven data quality validation that works identically in local development and Microsoft Fabric production environments.

## Requirements

### Validated

<!-- Existing capabilities confirmed from codebase analysis -->

- ✓ YAML-driven validation configuration with expectation definitions — existing
- ✓ Great Expectations 1.x integration (ephemeral contexts, checkpoints) — existing
- ✓ Dual-platform operation (local pandas + Fabric Spark) with runtime detection — existing
- ✓ Auto-profiling pipeline that analyzes data and generates YAML configs — existing
- ✓ Multi-format data loading (CSV, Parquet, JSON, Excel) with encoding detection — existing
- ✓ Severity-based threshold system (critical/high/medium/low) — existing
- ✓ Batch profiling with parallel file processing — existing
- ✓ Fabric integration: Delta table validation, Lakehouse file I/O, ABFSS paths — existing
- ✓ Config validation and merging via ConfigLoader — existing
- ✓ Data ingestion (file copy/move between source and target) — existing
- ✓ Large dataset sampling and memory protection — existing
- ✓ Comprehensive constants centralization — existing

### Active

<!-- Current scope: audit, fix, modernize, extend -->

- [ ] Fix chunked Spark validation bug (monotonically_increasing_id misuse)
- [ ] Fix aggregated chunk results miscounting (inflated statistics)
- [ ] Remove legacy setup.py (conflicts with pyproject.toml v2.0.0)
- [ ] Remove committed build artifacts (build/, dist/, htmlcov/, .coverage, pipeline.log, egg-info)
- [ ] Update .gitignore to prevent re-committing artifacts
- [ ] Align CI matrix with pyproject.toml (Python 3.10-3.13, ruff instead of flake8/black/isort)
- [ ] Unify dev tooling on ruff (remove requirements-dev.txt conflicts)
- [ ] Remove or relocate stale check_data.py script
- [ ] Clean up dead code (DataIngester.engine unused parameter)
- [ ] Raise fabric_connector.py test coverage from 18% to 60%+
- [ ] Raise loader.py test coverage from 51% to 60%+
- [ ] Raise batch_profiler.py test coverage from 51% to 60%+
- [ ] Add tests for severity-based threshold logic
- [ ] Implement Microsoft Teams webhook alerting
- [ ] Implement email (SMTP) alerting
- [ ] Build schema evolution tracking (detect column additions, removals, type changes)
- [ ] Build validation result history and trend analysis
- [ ] Align with AIMS Data Platform dependency expectations (GX version, Python version)
- [ ] Fix _is_fabric_runtime exposure in __init__.py __all__ (private function in public API)

### Out of Scope

- Webapp improvements — keep documentation webapp as-is
- Generic webhook alerting — Teams and email cover the need
- Native Spark validation (without pandas conversion) — GX 1.x doesn't support it
- Dashboard UI for validation trends — validation history is API/data layer only
- Mobile or web-based monitoring interface
- Migration to GX 2.x — pin to 1.x, monitor for future

## Context

- **Companion to AIMS Data Platform** (`1_AIMS_LOCAL_2026/`). AIMS imports dq_framework as an editable sibling package. Changes here directly affect AIMS pipeline behavior.
- **AIMS audit completed** (v1.0 milestone, 28/36 requirements satisfied). This audit was identified as a follow-up from AIMS audit findings.
- **Known AIMS integration issues**: AIMS pins `great-expectations>=0.18.0,<1.0` while dq_framework uses `>=1.0.0,<2.0.0`. This version mismatch needs resolution.
- **Production environment**: Microsoft Fabric notebooks and pipelines. Local dev must remain viable.
- **Internal contradictions**: setup.py (v1.2.0) vs pyproject.toml (v2.0.0), CI uses flake8/black/isort but pyproject.toml configures ruff, requirements-dev.txt conflicts with pyproject.toml dev deps.

## Constraints

- **Dependency**: Great Expectations >=1.0.0, <2.0.0 — core validation engine, can't swap
- **Platform**: Must maintain dual-mode (local + Fabric) operation — Fabric deps are optional/runtime-detected
- **Python**: >=3.10 as declared in pyproject.toml — CI and all tooling must align
- **Testing**: Cannot run Fabric-specific tests locally (no Spark/mssparkutils) — must mock extensively
- **Compatibility**: Changes must not break AIMS Data Platform imports and usage patterns
- **Coverage**: Minimum 60% per module (enforced by pytest-cov config)

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Remove setup.py entirely | pyproject.toml is the modern standard, setup.py has conflicting versions/deps | — Pending |
| Unify linting on ruff | ruff replaces flake8+black+isort, already configured in pyproject.toml | — Pending |
| Implement Teams + Email alerting | User-requested channels for production pipeline notifications | — Pending |
| Full schema evolution tracking | Production pipelines need to detect and adapt to schema changes | — Pending |
| Full validation history | Teams need to answer "is data quality improving or degrading?" | — Pending |
| Keep webapp as-is | Documentation webapp is functional, not in scope for this audit | — Pending |

---
*Last updated: 2026-03-08 after initialization*
