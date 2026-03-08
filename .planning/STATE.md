# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-08)

**Core value:** Reliable, configuration-driven data quality validation that works identically in local development and Microsoft Fabric production environments.
**Current focus:** Phase 1 — Repo Cleanup

## Current Milestone

**v2.0 — Health Audit & Production Hardening**

| Phase | Name | Status | Plans |
|-------|------|--------|-------|
| 1 | Repo Cleanup | Not started | 0/2 |
| 2 | CI and Tooling | Not started | 0/2 |
| 3 | Bug Fixes | Not started | 0/2 |
| 4 | Test Coverage | Not started | 0/3 |
| 5 | Storage Abstraction | Not started | 0/2 |
| 6 | Alert Infrastructure | Not started | 0/2 |
| 7 | Alert Channels | Not started | 0/2 |
| 8 | Schema Evolution | Not started | 0/3 |
| 9 | Validation History | Not started | 0/3 |
| 10 | Pipeline Integration | Not started | 0/3 |

**Progress:** 0/10 phases complete (0%)

## Active Phase

**Phase 1: Repo Cleanup**
- Goal: The repository has a single, authoritative packaging configuration with no legacy files or committed artifacts
- Requirements: PKG-01, PKG-02, PKG-03
- Status: Not started
- Plans: Not yet created — run `/gsd:plan-phase 1`

## Key Decisions

| Decision | Phase | Rationale |
|----------|-------|-----------|
| Remove setup.py | 1 | pyproject.toml v2.0.0 is canonical; setup.py has conflicting v1.2.0 deps |
| Unify on ruff | 2 | Already configured in pyproject.toml; replaces flake8+black+isort |
| Teams Adaptive Cards | 7 | O365 Connectors retire April 2026; must use Workflows format |
| httpx over requests | 7 | Modern async-capable client for webhook delivery |
| deepdiff for schema | 8 | Purpose-built for object comparison, avoids custom diff logic |
| SQLite for local history | 9 | Stdlib, zero new deps; SQLAlchemy already available via GX |

## Environment

- **Branch:** feature/gx-1x-migration
- **Conda env:** fabric-dq (from environment.yml)
- **Python:** >=3.10

---
*Last updated: 2026-03-08 after project initialization*
