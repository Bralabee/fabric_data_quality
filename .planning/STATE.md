---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: unknown
last_updated: "2026-03-09T00:02:51.431Z"
progress:
  total_phases: 10
  completed_phases: 5
  total_plans: 13
  completed_plans: 12
  percent: 92
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-08)

**Core value:** Reliable, configuration-driven data quality validation that works identically in local development and Microsoft Fabric production environments.
**Current focus:** Phase 6 — Alert Infrastructure

## Current Milestone

**v2.0 — Health Audit & Production Hardening**

| Phase | Name | Status | Plans |
|-------|------|--------|-------|
| 1 | Repo Cleanup | Complete | 2/2 |
| 2 | CI and Tooling | Complete | 2/2 |
| 3 | Bug Fixes | Not started | 0/2 |
| 4 | Test Coverage | Complete | 3/3 |
| 5 | Storage Abstraction | Complete | 2/2 |
| 6 | Alert Infrastructure | In progress | 1/2 |
| 7 | Alert Channels | Not started | 0/2 |
| 8 | Schema Evolution | Not started | 0/3 |
| 9 | Validation History | Not started | 0/3 |
| 10 | Pipeline Integration | Not started | 0/3 |

**Progress:** [█████████░] 92%

## Active Phase

**Phase 6: Alert Infrastructure** -- IN PROGRESS
- Goal: Build shared alerting layer with formatting, config, dispatcher, and circuit breaker
- Requirements: ALRT-03, ALRT-05, ALRT-06, ALRT-07
- Status: In progress
- Plans: 1/2 complete
- Current Plan: 06-02-PLAN.md

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

## Environment

- **Branch:** feature/gx-1x-migration
- **Conda env:** fabric-dq (from environment.yml)
- **Python:** >=3.10

---
*Last updated: 2026-03-09 after completing 06-01-PLAN.md (alert formatting and config)*
