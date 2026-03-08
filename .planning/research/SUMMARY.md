# Project Research Summary

**Project:** dq_framework -- Health Audit & Production Hardening
**Domain:** Data quality framework (Python library, GX 1.x wrapper, YAML-driven, dual-platform local/Fabric)
**Researched:** 2026-03-08
**Confidence:** MEDIUM-HIGH

## Executive Summary

The dq_framework is a mature validation library that needs production hardening, not a greenfield build. The three new capabilities -- alerting, schema evolution, and validation history -- are all post-validation concerns that extend the existing pipeline without restructuring it. This is architecturally clean: the core validate flow remains untouched while new modules consume its output. The recommended approach is to build a shared storage abstraction first (both history and schema tracking need persistence), then implement alerting and schema tracking in parallel, and finally wire everything into the FabricRunner integration point.

The stack choices are conservative and well-justified. Only three new runtime dependencies are needed: httpx (webhook delivery), jinja2 (message templating), and deepdiff (schema diffing), totalling roughly 3.7MB. SQLite (stdlib) and SQLAlchemy (already a GX dependency) handle validation history storage with zero new dependencies. The most important stack decision is using httpx with Adaptive Card JSON for Teams webhooks -- Microsoft is retiring O365 Connectors by April 2026, and building on the legacy format would result in a feature that breaks within weeks of shipping.

The primary risks are: (1) building Teams alerting on the deprecated connector model, (2) alert failures being silently swallowed (the existing code already demonstrates this anti-pattern), (3) validation history storage growing without bounds, and (4) threshold logic changes inadvertently breaking existing AIMS pipelines. All are preventable with the patterns identified in research. The most subtle risk is the AIMS compatibility concern -- this library is a live dependency for production pipelines, so characterization tests must precede any behavioral changes.

## Key Findings

### Recommended Stack

Three new runtime dependencies, zero new infrastructure. The stack stays lightweight and avoids adding anything that requires external services or heavy native binaries.

**Core technologies:**
- **httpx** (>=0.28.0): HTTP client for Teams Workflow webhook delivery -- modern async-capable client with built-in timeout/retry, cleaner than requests for JSON POST
- **jinja2** (>=3.1.0): Alert message templating -- shared by Teams (Adaptive Card JSON) and email (HTML) channels, enables user-customizable alert layouts via YAML config
- **deepdiff** (>=8.0.0): Schema diff computation -- purpose-built for deep object comparison, detects additions/removals/type changes in nested dicts without custom diff logic
- **SQLite** (stdlib) + **SQLAlchemy** (existing): Validation history storage -- zero new dependencies, works in both local dev and Fabric notebooks without external database infrastructure

**Critical version note:** Pin httpx to <1.0 (1.0 dev releases exist but are not stable). Verify whether GX 1.x already transitively depends on jinja2 -- if so, it is available without explicit declaration, but declare it anyway to prevent breakage.

### Expected Features

**Must have (table stakes):**
- Teams webhook alerting -- the primary notification channel for this Microsoft-shop org; no competing tool has built-in Teams support
- Email (SMTP) alerting -- secondary channel, HTML-formatted with summary tables
- Configurable alert routing by severity -- critical failures alert immediately, low-severity batched or suppressed
- Alert message formatting -- Adaptive Cards for Teams, HTML for email; raw JSON is unacceptable
- Schema change detection -- detect column additions, removals, type changes against a stored baseline
- Validation result persistence in queryable format -- replace scattered JSON files with structured storage

**Should have (differentiators):**
- Validation trend analysis -- "is data quality improving or degrading?" is the key question; paywalled in GX Cloud and Soda Cloud, free here
- Schema evolution tracking with diff history -- go beyond point-in-time detection to track what changed, when, and classify as breaking vs non-breaking
- Severity-aware alerting thresholds -- leverage the existing 4-level severity system (more granular than any competitor) for intelligent alert routing
- YAML-driven alert configuration -- consistent with the framework's config-driven philosophy
- Auto-generated schema baseline from profiler -- zero-config schema monitoring

**Defer (anti-features):**
- Dashboard UI -- expose structured data for Power BI/Jupyter instead
- Generic webhook alerting -- Teams and email cover the need; build extensible, not generic
- Slack integration -- YAGNI for this Microsoft-shop org
- Real-time streaming validation -- fundamentally different architecture from batch
- Automatic schema migration/repair -- detection and alerting only; human decision required for config updates

### Architecture Approach

All three new features attach to the existing pipeline as post-validation consumers. The architecture introduces four new modules: `store.py` (shared storage abstraction replacing inline `_save_results_to_lakehouse`), `alerting.py` (AlertManager with pluggable channels via Strategy pattern), `history.py` (ValidationHistory for append-only result storage and trend queries), and `schema.py` (SchemaTracker for baseline comparison). The integration point is FabricDataQualityRunner, not DataQualityValidator -- the core validator stays focused on validation only.

**Major components:**
1. **ResultStore** (`store.py`) -- Abstract storage interface with JSONFileStore (local) and LakehouseStore (Fabric) backends; shared by history and schema tracking
2. **AlertManager** (`alerting.py`) -- Routes validation results to notification channels (Teams, Email) based on YAML config; uses Strategy pattern for pluggable channels
3. **ValidationHistory** (`history.py`) -- Append-only result recording with trend analysis queries; returns DataFrames for downstream analysis
4. **SchemaTracker** (`schema.py`) -- Captures DataFrame schemas as snapshots, computes diffs via deepdiff, classifies changes as breaking/non-breaking

**Key pattern:** Keep the flat module structure (no subdirectories). The existing codebase is flat and adding subdirectories for 4 new files would be over-engineering.

### Critical Pitfalls

1. **Deprecated Teams connector model** -- Microsoft retires O365 Connectors April 2026. Use Power Automate Workflows webhooks with Adaptive Card JSON from day one. Avoid pymsteams entirely (legacy format only, seeking new maintainers).

2. **Silent alert failures** -- The existing `_send_alert` stub returns bool but the caller ignores it. Define explicit failure policies (warn/raise/fallback) and implement a dead-letter mechanism for undeliverable alerts.

3. **Unbounded history storage** -- JSON-file-per-run accumulates thousands of files within months. Use JSONL (one file per source) or SQLite locally, implement retention policy from day one (`retention_days: 90`), and maintain a summary/aggregate file for trend queries.

4. **Schema tracking without baseline strategy** -- Handle first-run gracefully (no baseline = create baseline, no alert). Store baselines in deterministic paths. Never auto-update baselines; require explicit acceptance of schema changes.

5. **Threshold logic changes breaking AIMS** -- Write characterization tests documenting current behavior before touching threshold logic. Treat any behavior change as a breaking change requiring coordination with AIMS operators.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Foundation -- Storage Abstraction and Packaging Cleanup
**Rationale:** ResultStore is a dependency for both history and schema tracking. Packaging cleanup (setup.py removal, .gitignore, build artifact cleanup) is low-risk housekeeping that should happen first to establish a clean baseline. The storage abstraction also replaces the inline `_save_results_to_lakehouse`, paying down tech debt immediately.
**Delivers:** `store.py` with JSONFileStore and LakehouseStore backends; clean repo state; modernized pyproject.toml
**Addresses:** Structured result storage, packaging modernization
**Avoids:** Pitfall 5 (setup.py removal breaking AIMS -- verify before removing), Pitfall 4 (unbounded storage -- design retention into ResultStore from the start)

### Phase 2: Bug Fixes and Test Coverage
**Rationale:** Bugs must be fixed before adding new features that depend on correct validation results. Test coverage must increase before modifying fabric_connector.py for integration. Writing characterization tests for threshold logic protects against regression during later phases.
**Delivers:** Fixed chunked Spark validation, fixed aggregated results, Fabric test fixtures, characterization tests for threshold logic, coverage improvements for fabric_connector.py/loader.py/batch_profiler.py
**Addresses:** All bug fix requirements, test coverage requirements
**Avoids:** Pitfall 6 (fixing bugs without tests), Pitfall 7 (threshold changes breaking AIMS)

### Phase 3: Alerting System
**Rationale:** Alerting is the highest-value new feature and is self-contained (no dependency on ResultStore). It can proceed in parallel with Phase 2 if resources allow, but sequencing after bug fixes ensures the validation results being alerted on are correct.
**Delivers:** AlertManager, TeamsAlertChannel (Adaptive Card format), EmailAlertChannel (HTML via smtplib), alert routing by severity, YAML config for alerting, jinja2 templates
**Uses:** httpx, jinja2, tenacity (optional for retries)
**Implements:** AlertManager component from architecture
**Avoids:** Pitfall 1 (deprecated connector -- use Workflows format), Pitfall 2 (silent failures -- implement failure policies and dead-letter)

### Phase 4: Schema Evolution
**Rationale:** Schema tracking depends on ResultStore (Phase 1) for baseline persistence. It is independent of alerting but integrating schema change alerts with the alerting system (Phase 3) adds value.
**Delivers:** SchemaTracker with capture/compare/detect_drift, baseline storage, schema change classification (breaking/non-breaking), schema evolution alerts wired into AlertManager, auto-baseline from profiler
**Uses:** deepdiff
**Implements:** SchemaTracker component from architecture
**Avoids:** Pitfall 3 (no baseline strategy -- handle first-run, deterministic paths, explicit acceptance)

### Phase 5: Validation History and Trending
**Rationale:** History depends on ResultStore (Phase 1) and benefits from alerting (Phase 3) being in place so trend degradation can trigger alerts. This is the differentiator feature and the most complex, so it comes last.
**Delivers:** ValidationHistory with record/get_trend/get_summary, retention policy enforcement, trend query API returning DataFrames, structured result schema
**Implements:** ValidationHistory component from architecture
**Avoids:** Pitfall 4 (unbounded storage -- JSONL format, retention policy, summary aggregates)

### Phase 6: Integration and Hardening
**Rationale:** All components must exist before wiring them into FabricDataQualityRunner. This phase modifies the existing integration point and updates public API exports.
**Delivers:** FabricRunner pipeline integration (schema check -> validate -> record history -> alert), updated YAML config schema in ConfigLoader, updated constants.py, updated __init__.py exports, end-to-end integration tests
**Avoids:** All integration gotchas from PITFALLS.md (credential handling, payload size limits, concurrent write safety, timezone handling)

### Phase Ordering Rationale

- **Storage first** because both history and schema tracking depend on it. Building it first also eliminates the inline `_save_results_to_lakehouse` tech debt.
- **Bug fixes before features** because new features consume validation results -- those results must be correct. Also, writing Fabric test fixtures in Phase 2 creates reusable infrastructure for integration testing in Phase 6.
- **Alerting before schema/history** because it is the highest user-value feature and is self-contained. Users get value from alerting even without schema tracking or history.
- **Schema before history** because schema tracking is simpler (MEDIUM complexity vs HIGH) and can feed change events into the alerting system for immediate value.
- **Integration last** because it requires all components and is the riskiest modification (touching FabricRunner, which has the lowest test coverage).

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 3 (Alerting):** Needs validation against a real Power Automate Workflows webhook endpoint. Adaptive Card rendering behavior varies by Teams client. Test actual card rendering, not just HTTP 200 responses.
- **Phase 5 (Validation History):** Trend analysis query patterns need validation with real usage data. The proposed API (get_trend, get_summary, compare_periods) is based on common patterns but should be validated against actual user questions.

Phases with standard patterns (skip research-phase):
- **Phase 1 (Storage):** Well-established patterns. JSONFileStore and LakehouseStore are straightforward file I/O abstractions.
- **Phase 2 (Bug Fixes):** The bugs are already identified and understood. Test-first approach is standard.
- **Phase 4 (Schema Evolution):** deepdiff handles the hard part. Schema capture and comparison is a well-documented pattern.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | All recommended dependencies are mature, actively maintained, and verified on PyPI. Version compatibility confirmed. Dependency weight is minimal (~3.7MB new). |
| Features | MEDIUM | Feature landscape based on training data for GX/Soda/dbt ecosystem. Competitor analysis could not be web-verified but aligns with well-established patterns. Priority ordering is sound. |
| Architecture | HIGH | Based on direct codebase analysis. The post-validation consumer pattern is clean and the existing code structure supports it. Build order is derived from actual component dependencies. |
| Pitfalls | HIGH | Critical pitfalls (Teams deprecation, silent alert failures) verified against codebase and Microsoft documentation. AIMS compatibility risks confirmed from PROJECT.md constraints. |

**Overall confidence:** MEDIUM-HIGH

### Gaps to Address

- **Teams Workflows webhook payload format:** The Adaptive Card JSON envelope format has been verified from Microsoft docs, but actual rendering behavior in Teams desktop/mobile/web clients should be tested with a real webhook during Phase 3 development. Card rendering inconsistencies between clients are a known issue in the ecosystem.
- **Fabric notebook pip version:** The setup.py removal (Phase 1) assumes pip >= 21.3 in Fabric notebooks for PEP 660 editable installs. This must be verified before removing setup.py. If pip is older, keep setup.py as a shim.
- **AIMS GX version mismatch:** AIMS pins `great-expectations>=0.18.0,<1.0` while dq_framework uses `>=1.0.0,<2.0.0`. This conflict is noted in PROJECT.md but not addressed by any research file. It must be resolved before or during Phase 6 integration.
- **Concurrent write safety for history:** If multiple pipeline runs write to the same history file simultaneously (common in Fabric), data corruption is possible. JSONL append is safer than JSON overwrite, but Lakehouse `mssparkutils.fs.put` may not be atomic. Needs investigation during Phase 5.
- **SMTP availability in Fabric:** Corporate environments may restrict SMTP access from Fabric notebooks. Microsoft Graph API for email sending may be needed as a fallback. Validate during Phase 3.

## Sources

### Primary (HIGH confidence)
- Existing codebase analysis: `validator.py`, `fabric_connector.py`, `config_loader.py`, `constants.py`, `data_profiler.py` -- direct code review
- `.planning/PROJECT.md` -- project constraints, AIMS dependency, scope definition
- `.planning/codebase/CONCERNS.md` -- known bugs, test coverage gaps
- [Microsoft Teams Connectors retirement](https://devblogs.microsoft.com/microsoft365dev/retirement-of-office-365-connectors-within-microsoft-teams/) -- April 2026 deadline
- [Adaptive Cards schema](https://adaptivecards.io/explorer/) -- v1.4-1.6 format
- PyPI package metadata: httpx v0.28.1, deepdiff v8.6.1, jinja2 v3.1.6, alembic v1.18.4

### Secondary (MEDIUM confidence)
- Great Expectations documentation -- GX 1.x Checkpoint Actions, notification patterns (training data, not web-verified)
- Soda Core documentation -- SodaCL schema checks, notification channels (training data)
- Microsoft Teams webhook documentation -- Workflows migration, Adaptive Card requirements (docs dated 2025-06-10)

### Tertiary (LOW confidence)
- SMTP availability in Fabric notebook environments -- inferred from corporate IT patterns, needs validation
- Concurrent file write behavior on Lakehouse -- inferred from mssparkutils API patterns, needs testing

---
*Research completed: 2026-03-08*
*Ready for roadmap: yes*
