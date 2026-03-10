---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: unknown
last_updated: "2026-03-10T18:28:06.314Z"
progress:
  total_phases: 10
  completed_phases: 9
  total_plans: 22
  completed_plans: 20
  percent: 91
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-08)

**Core value:** Reliable, configuration-driven data quality validation that works identically in local development and Microsoft Fabric production environments.
**Current focus:** Phase 10 in progress — Pipeline Integration 1/3 plans done.

## Current Milestone

**v2.0 — Health Audit & Production Hardening**

| Phase | Name | Status | Plans |
|-------|------|--------|-------|
| 1 | Repo Cleanup | Complete | 2/2 |
| 2 | CI and Tooling | Complete | 2/2 |
| 3 | Bug Fixes | Not started | 0/2 |
| 4 | Test Coverage | Complete | 3/3 |
| 5 | Storage Abstraction | Complete | 2/2 |
| 6 | Alert Infrastructure | Complete | 2/2 |
| 7 | Alert Channels | Complete | 2/2 |
| 8 | Schema Evolution | Complete | 2/2 |
| 9 | Validation History | Complete | 2/2 |
| 10 | Pipeline Integration | In Progress | 1/3 |

**Progress:** [█████████░] 91%

## Active Phase

**Phase 10: Pipeline Integration** -- IN PROGRESS
- Goal: Wire alerting, schema tracking, and validation history into the pipeline runner
- Requirements: INTG-02, INTG-03, INTG-04, INTG-05
- Status: In Progress
- Plans: 1/3 complete (10-01 config contracts and public API)

## Key Decisions

| Decision | Phase | Rationale |
|----------|-------|-----------|
| Remove setup.py | 1 | pyproject.toml v2.0.0 is canonical; setup.py has conflicting v1.2.0 deps |
| Unify on ruff | 2 | Already configured in pyproject.toml; replaces flake8+black+isort |
| Teams Adaptive Cards | 7 | O365 Connectors retire April 2026; must use Workflows format |
| httpx over requests | 7 | Modern async-capable client for webhook delivery |
| deepdiff for schema | 8 | Purpose-built for object comparison, avoids custom diff logic |
| SQLite for local history | 9 | Stdlib, zero new deps; SQLAlchemy already available via GX |
| Add *.whl/*.tar.gz to .gitignore | 1 | dist/ alone does not cover wheel/sdist files outside that directory |
| Add [tool.setuptools.packages.find] to pyproject.toml | 1 | Flat-layout repos need explicit package include to avoid multi-package discovery error |
| Remove safety pre-commit hook entirely | 2 | Safety has gone freemium; hook references requirements*.txt which may not exist |
| Replace flake8/black/isort with ruff in CI and Makefile | 2 | Ruff already configured in pyproject.toml; single tool replaces three |
| Delete requirements-dev.txt | 2 | Conflicting version pins with pyproject.toml; single source of truth via [dev] extras |
| Mock pa.Table via module attribute swap | 4 | pyarrow C extension types are immutable; mocker.patch cannot set attributes |
| Mock ProcessPoolExecutor in batch tests | 4 | Avoid spawning real processes; deterministic fast tests |
| Inject SparkSession mock via module attribute | 4 | SparkSession not importable without PySpark; @patch fails |
| Threshold equality is pass (strict <) | 4 | Discovered threshold=50 with 50% rate passes; uses strict less-than comparison |
| Module-level imports from utils for patchability | 5 | Lazy imports inside methods prevent unittest.mock.patch; module-level imports allow patching |
| Results always persisted via pluggable ResultStore | 5 | No more FABRIC_UTILS_AVAILABLE guard; get_store() auto-selects backend |
| Storage write failures fire-and-forget | 5 | Caught and logged without crashing validation, matching prior behavior |
| Jinja2 via transitive GX dependency | 6 | Already available via great-expectations; avoid redundant pyproject.toml entry |
| PackageLoader for built-in, FileSystemLoader for custom templates | 6 | Supports both installed and editable modes; user can override templates |
| Deep copy config dict before env var substitution | 6 | Prevents mutation of caller's data during config parsing |
| In-memory per-process circuit breaker state | 6 | Correct for batch pipeline usage; each run starts fresh |
| AlertChannel ABC with send() contract | 6 | Minimal interface for Phase 7 channel implementations |
| Workflows envelope format for Teams | 7 | type:message + attachments array required by Power Automate |
| Adaptive Card v1.3 for mobile compat | 7 | v1.5 features silently ignored on Teams mobile |
| httpx as required dependency | 7 | Teams alerting is core v2.0 feature, not optional |
| severity_routing=None for backwards compat | 7 | Existing configs without severity_routing send all alerts unconditionally |
| Router before message rendering | 7 | Avoid unnecessary Jinja2 work when suppressing alerts |
| Classify all dict additions/removals directly | 8 | DeepDiff operates on columns sub-dict, so all paths are column-level |
| diff.to_dict() for raw diff serialization | 8 | Avoids DeepDiff object serialization issues in ResultStore |
| Microsecond timestamps in history keys | 8 | Same-second key collisions would silently lose history entries |
| Any-typed dispatcher in alert_on_breaking_changes | 8 | Keeps alerting dependency optional; no hard import of AlertDispatcher |
| check_and_alert augments detect_changes result | 8 | Single return dict with history_key and alert_result rather than new structure |
| Import _is_fabric_runtime from utils with try/except | 9 | utils.py is canonical location; try/except allows tests without full package |
| JSON text for complex fields in both backends | 9 | severity_stats and failed_expectations stored as JSON strings in SQLite and Parquet |
| Single Parquet file with read-concat-write | 9 | Start simple; partitioning added later if performance requires it |
| Python-side JSON aggregation for failures | 9 | SQLite lacks JSON functions; Python parse-and-group ensures cross-backend consistency |
| Constructor defaults from constants.py | 9 | Centralizes magic values; try/except fallback for standalone usage |
| Filter-and-rewrite for Parquet retention | 9 | Consistent with existing append pattern; no new dependency |
| OPTIONAL_SECTION_VALIDATORS registry | 10 | Dict maps section names to validator functions for extensible config validation |
| AlertManager alias for AlertDispatcher | 10 | Matches INTG-04 requirement text for backward compatibility |

## Environment

- **Branch:** feature/10-pipeline-integration
- **Conda env:** fabric-dq (from environment.yml)
- **Python:** >=3.10

---
*Last updated: 2026-03-10 after completing 10-01-PLAN.md (config contracts and public API)*
