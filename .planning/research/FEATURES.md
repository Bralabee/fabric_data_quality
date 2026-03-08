# Feature Research

**Domain:** Data quality framework (Python library wrapping Great Expectations, YAML-driven, dual-platform local/Fabric)
**Researched:** 2026-03-08
**Confidence:** MEDIUM (training data for ecosystem patterns, verified against existing codebase; web verification unavailable)

## Feature Landscape

### Table Stakes (Users Expect These)

Features that any production data quality framework must have. Missing these means the framework is not production-ready.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| **Failure alerting to at least one channel** | Production pipelines that fail silently are useless. Every DQ tool (GX Cloud, Soda, Monte Carlo, dbt) sends alerts on failure. | MEDIUM | Currently a TODO stub in `_send_alert()`. Teams webhook is the priority channel for this org (Fabric ecosystem). |
| **Configurable alert routing** | Different failures go to different people. Critical schema violations vs low-severity warnings need different handling. | MEDIUM | Tie to existing severity system (critical/high/medium/low). Route by severity level. |
| **Validation result persistence** | Results must survive the process that created them. Every DQ tool stores results. Without history, you cannot answer "what happened last Tuesday?" | LOW | Partially exists: `_save_results_to_lakehouse()` saves individual JSON files. Needs structured storage with queryable schema. |
| **Result summary with failed expectation details** | Users need to know what failed, not just that something failed. | LOW | Already implemented in `_format_results()`. The summary dict includes `failed_expectations` with column, type, severity, and details. |
| **Retry logic for transient alert failures** | Network calls fail. Alerts must retry with backoff. | LOW | Already stubbed with exponential backoff in `_send_alert()`. Just needs real transport underneath. |
| **Schema change detection (additions/removals)** | When upstream schemas change, pipelines break. Every production DQ setup detects column additions and removals at minimum. Soda has `schema` checks, dbt has `schema.yml` contracts, GX has `expect_table_columns_to_match_set`. | MEDIUM | No implementation exists. The profiler generates expectations from current schema but never compares against a baseline. |
| **Alert message formatting** | Alerts must be readable. Raw JSON payloads in Teams/email are useless in practice. Every tool that sends alerts formats them for human consumption. | LOW | Needs Teams Adaptive Card formatting and HTML email templates. |

### Differentiators (Competitive Advantage)

Features that elevate dq_framework beyond a basic GX wrapper. These are where the framework adds value over using GX directly.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| **Validation trend analysis** | Answer "is data quality improving or degrading?" over time. This is the key question for data teams. GX OSS does not offer this (GX Cloud does, behind paywall). Soda Cloud offers it. Building it as a local/Fabric capability is a genuine differentiator for teams that cannot use SaaS. | HIGH | Requires structured result history, aggregation logic, and a query API. Not a dashboard -- just the data layer and Python API for trends. |
| **Schema evolution tracking with diff history** | Go beyond detection: track what changed, when, and alert on breaking vs non-breaking changes. Classify changes as additive (safe), removal (breaking), type change (breaking). Most tools only detect current-state mismatches, not evolution over time. | MEDIUM | Store schema snapshots alongside validation results. Compare against previous snapshot on each run. |
| **Severity-aware alerting thresholds** | The existing severity system (critical/high/medium/low) with per-severity thresholds is already more sophisticated than most GX wrappers. Extending this to alerting -- only alert on critical failures, batch medium/low into daily digests -- is genuinely useful. | MEDIUM | Leverage existing `quality_thresholds` and `severity_stats` from `_format_results()`. |
| **YAML-driven alert configuration** | Configure alerting in the same YAML files that define expectations. No separate config files, no code changes. Consistent with the framework's config-driven philosophy. | LOW | Add `alerts:` section to existing YAML config schema. |
| **Auto-generated schema baseline from profiler** | When `DataProfiler` profiles a dataset, automatically save the detected schema as a baseline for future evolution tracking. Zero-config schema monitoring. | LOW | Profiler already detects column types, null rates, cardinality. Persist this as a schema snapshot. |
| **Structured result storage with partitioning** | Store results in a queryable format (Parquet in Lakehouse, SQLite locally) partitioned by date/dataset. Enables trend queries without parsing thousands of JSON files. | MEDIUM | Replace individual JSON file dumps with append-to-Parquet or SQLite approach. |

### Anti-Features (Commonly Requested, Often Problematic)

Features that seem valuable but would add complexity disproportionate to their benefit, or conflict with the framework's scope.

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| **Dashboard UI for validation trends** | Visual monitoring is appealing. Teams want charts and graphs. | Building and maintaining a web UI is a separate product. It pulls focus from the library's core mission. Fabric has Power BI for visualization. | Expose results as structured data (Parquet/SQLite). Let Power BI, Jupyter, or other tools visualize. Provide a Python API for trend queries, not a UI. |
| **Generic webhook alerting** | "Support any webhook" seems flexible. | Generic webhooks require per-target payload formatting, auth handling, header configuration. It becomes a webhook framework, not a DQ framework. | Support Teams and email (the two channels this org uses). If a third channel is needed later, the alert architecture should make adding one straightforward, but do not build a generic system upfront. |
| **Real-time streaming validation** | Streaming data quality is a growing field. | GX 1.x is batch-oriented. The framework's architecture (load data, validate, report) is batch. Streaming validation is a fundamentally different architecture. | Keep batch focus. For streaming, recommend a dedicated tool (e.g., Soda for Spark Streaming, or custom Spark Structured Streaming checks). |
| **Automatic schema migration/repair** | "If schema changes, automatically update configs." | Auto-updating validation configs silently could mask real data quality issues. A column removal should be a loud alert, not a quiet config update. | Detect and alert on schema changes. Require human decision to update configs. Provide a CLI command to regenerate configs from new schema when the user decides to accept the change. |
| **Slack integration** | Popular in many orgs. | This org uses Microsoft Fabric and Teams. Slack integration is YAGNI. Adding it means testing and maintaining an unused channel. | If needed later, the alert architecture should allow plugging in new channels. Do not build speculatively. |
| **GX Cloud integration** | GX Cloud offers hosted result storage and collaboration. | Adds a SaaS dependency. The framework's value proposition is self-contained operation in Fabric. GX Cloud pricing may not align with org budget. | Build local/Fabric-native result storage and trending. This is the actual differentiator. |

## Feature Dependencies

```
[Teams Alerting]
    +--requires--> [Alert Message Formatting]
    +--requires--> [Configurable Alert Routing]
                       +--requires--> [Severity-Aware Thresholds] (already exists in validator)

[Email Alerting]
    +--requires--> [Alert Message Formatting]
    +--requires--> [Configurable Alert Routing]

[Validation Trend Analysis]
    +--requires--> [Structured Result Storage]
                       +--requires--> [Validation Result Persistence] (partially exists)

[Schema Evolution Tracking]
    +--requires--> [Schema Baseline Storage]
                       +--enhanced-by--> [Auto-Generated Schema Baseline from Profiler]

[YAML-Driven Alert Configuration]
    +--enhances--> [Teams Alerting]
    +--enhances--> [Email Alerting]
    +--enhances--> [Configurable Alert Routing]

[Structured Result Storage]
    +--enhances--> [Schema Evolution Tracking] (can store schema snapshots alongside results)
```

### Dependency Notes

- **Teams/Email Alerting requires Alert Message Formatting:** Raw JSON payloads are unacceptable in production alerts. Formatting must be implemented before or alongside transport.
- **Validation Trend Analysis requires Structured Result Storage:** You cannot query trends from individual JSON files scattered across Lakehouse directories. A structured format (Parquet partitioned by date, or SQLite locally) is prerequisite.
- **Schema Evolution Tracking requires Schema Baseline Storage:** You cannot detect changes without a "before" to compare against. The profiler can generate the initial baseline, but it must be persisted.
- **YAML-Driven Alert Configuration enhances all alerting features:** This is not a blocker but a quality-of-life improvement. Alerting can work with hardcoded config initially, then move to YAML-driven.

## MVP Definition

### Launch With (Phase 1 of this milestone)

Minimum to call alerting and history "implemented."

- [ ] **Teams webhook alerting** -- the primary notification channel for the org. Use Adaptive Card format for readability.
- [ ] **Email (SMTP) alerting** -- secondary channel. HTML-formatted with summary table and failed expectation details.
- [ ] **Alert message formatting** -- shared formatting layer that Teams and email adapters consume.
- [ ] **Configurable alert routing by severity** -- critical failures alert immediately; low-severity failures can be batched or suppressed.
- [ ] **YAML-driven alert configuration** -- `alerts:` section in existing config YAML specifying channels, recipients, severity filters.

### Add After Alerting Works (Phase 2 of this milestone)

- [ ] **Schema baseline storage** -- persist schema snapshots (column names, types, nullability) as JSON/YAML alongside validation configs.
- [ ] **Schema change detection** -- compare current schema against stored baseline. Classify changes: addition (non-breaking), removal (breaking), type change (breaking), nullability change (warning).
- [ ] **Schema evolution alerts** -- wire schema change detection into the alerting system. Breaking changes trigger critical alerts.

### Add After Schema Tracking Works (Phase 3 of this milestone)

- [ ] **Structured result storage** -- replace individual JSON dumps with Parquet files (Fabric) or SQLite (local). Define a result schema with: timestamp, suite_name, batch_name, success, success_rate, evaluated_checks, failed_checks, severity_stats, duration.
- [ ] **Validation trend query API** -- Python functions: `get_trend(dataset, days=30)`, `get_failure_history(dataset)`, `compare_periods(dataset, period_a, period_b)`. Return pandas DataFrames.
- [ ] **Auto-generated schema baseline from profiler** -- when `DataProfiler.profile()` runs, optionally save the detected schema as a baseline for evolution tracking.

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Teams webhook alerting | HIGH | MEDIUM | P1 |
| Email (SMTP) alerting | HIGH | MEDIUM | P1 |
| Alert message formatting | HIGH | LOW | P1 |
| Configurable alert routing by severity | HIGH | MEDIUM | P1 |
| YAML-driven alert configuration | MEDIUM | LOW | P1 |
| Schema change detection | HIGH | MEDIUM | P1 |
| Schema baseline storage | MEDIUM | LOW | P1 |
| Schema evolution alerts | MEDIUM | LOW | P2 |
| Structured result storage | HIGH | MEDIUM | P2 |
| Validation trend query API | MEDIUM | HIGH | P2 |
| Auto-generated schema baseline from profiler | MEDIUM | LOW | P2 |

**Priority key:**
- P1: Must have for this milestone (alerting + schema detection are the stated goals)
- P2: Should have, implements the full vision (trend analysis, structured storage)
- P3: Nice to have, future consideration (not applicable -- all listed features are P1/P2)

## Competitor Feature Analysis

| Feature | Great Expectations (OSS) | Soda Core (OSS) | dbt (tests) | dq_framework (target) |
|---------|--------------------------|------------------|-------------|----------------------|
| **Alerting channels** | Slack, PagerDuty, email, Opsgenie (via Actions in Checkpoints) | Slack, email, webhooks, PagerDuty (via Soda Cloud) | None built-in (CI/CD handles) | Teams + email (org-specific) |
| **Alert formatting** | Basic text notifications | Formatted with check details | N/A | Adaptive Cards (Teams), HTML (email) |
| **Schema checks** | `expect_table_columns_to_match_set`, `expect_table_columns_to_match_ordered_list` | `schema` check with `fail` conditions for column changes | `schema.yml` contracts with `--warn-error` | Baseline comparison with change classification |
| **Schema evolution tracking** | No (point-in-time checks only) | No (point-in-time checks only) | No (contract enforcement only) | Diff history with breaking/non-breaking classification |
| **Result history** | GX Cloud only (paid) | Soda Cloud only (paid) | `dbt artifacts` (JSON, self-managed) | Local SQLite + Fabric Parquet (self-managed, free) |
| **Trend analysis** | GX Cloud only (paid) | Soda Cloud only (paid) | Third-party tools | Python API returning DataFrames |
| **Severity levels** | No built-in severity | Warn vs fail (2 levels) | Warn vs error (2 levels) | 4 levels (critical/high/medium/low) with per-level thresholds |
| **Config-driven** | Python code or YAML (GX Cloud) | SodaCL YAML | schema.yml YAML | YAML with auto-profiling |

### Key Competitive Observations

1. **Alerting is table stakes but channel selection is org-specific.** GX OSS supports Slack and email. Soda supports Slack and webhooks. Neither has built-in Teams support. Teams support is a genuine gap in the ecosystem for Microsoft-shop organizations.

2. **Schema evolution tracking (not just detection) is rare.** All tools can check current schema against expectations. None of the OSS tools track schema changes over time with a diff history. This is a real differentiator.

3. **Validation history and trends are paywalled in the ecosystem.** Both GX Cloud and Soda Cloud charge for this. Offering it as a self-hosted, free capability -- even without a UI -- is valuable for teams that cannot or will not use SaaS.

4. **The 4-level severity system is already more granular than competitors.** GX OSS has no severity concept. Soda and dbt have 2 levels (warn/fail). Extending this to alerting routing is a natural advantage.

## How Production Tools Implement These Features

### Alerting Patterns

**Transport layer separation:** Every mature tool separates the "what to send" (message content) from "how to send it" (transport). GX has `ValidationAction` subclasses. Soda has notification channels. The pattern is:

1. Formatter produces a message payload (title, body, severity, metadata)
2. Transport adapter sends it (Teams webhook, SMTP, etc.)
3. Router decides which transports get which messages (based on severity, dataset, etc.)

**Teams webhook specifics:** Microsoft Teams incoming webhooks accept Adaptive Card JSON payloads (the newer format) or MessageCard (legacy, being deprecated). Use Adaptive Card format. The webhook is a simple HTTP POST with a JSON body. No OAuth needed for incoming webhooks.

**Email specifics:** SMTP with TLS. Use Python's `smtplib` + `email.mime`. HTML body with a summary table. In Fabric environments, consider using Microsoft Graph API for email sending (org may restrict SMTP).

**Retry and circuit-breaking:** The existing exponential backoff stub is the right pattern. Add a circuit breaker: if alerts fail N times consecutively, stop trying for M minutes and log a critical error. Prevents alert storms from blocking pipeline execution.

### Schema Evolution Patterns

**Baseline comparison approach:**
1. On first run (or explicit init), snapshot the current schema: column names, dtypes, nullable flags
2. On subsequent runs, compare current schema against stored baseline
3. Classify changes:
   - **Addition:** New column not in baseline. Non-breaking. Log as info/warning.
   - **Removal:** Column in baseline missing from current data. Breaking. Alert as critical.
   - **Type change:** Column exists but dtype differs. Breaking. Alert as critical.
   - **Nullability change:** Column was non-null, now has nulls (or vice versa). Warning.
4. Optionally auto-update baseline after human approval

**Storage format:** Schema snapshots should be simple JSON/YAML: `{column_name: {dtype: "int64", nullable: false, ...}}`. Store alongside validation configs or in a dedicated `schema_history/` directory.

**What Soda does:** `schema` check in SodaCL compares against a defined list of expected columns and types. Fails if there are additions, removals, or type mismatches. No historical tracking.

**What dbt does:** `schema.yml` contracts define expected columns and types. `dbt build --warn-error` fails on violations. No evolution tracking -- it is enforcement, not monitoring.

### Validation History Patterns

**Storage:**
- GX Cloud stores results in a managed database with a REST API
- Soda Cloud stores results in a managed SaaS layer
- For self-hosted: Parquet files partitioned by `date/dataset/` in Lakehouse, SQLite locally

**Schema for result records:**

```
timestamp: ISO 8601
suite_name: string
batch_name: string
dataset: string (derived from batch_name)
success: boolean
success_rate: float
evaluated_checks: int
failed_checks: int
severity_stats: JSON (critical: {total, passed}, high: {total, passed}, ...)
failed_expectations: JSON array (optional, for failed runs only)
duration_ms: int
schema_hash: string (for correlating with schema snapshots)
```

**Trend queries people actually need:**
1. "Show me the success rate for dataset X over the last 30 days" -- time series
2. "Which datasets have degraded this week vs last week?" -- comparison
3. "What are the most frequently failing expectations?" -- aggregation
4. "When did dataset X last fail?" -- point lookup

## Sources

- Great Expectations documentation (docs.greatexpectations.io) -- verified GX 1.x uses Checkpoint Actions for notifications; OSS supports Slack, email, PagerDuty, Opsgenie, custom actions. Confidence: MEDIUM (based on training data, web verification failed).
- Soda documentation -- SodaCL schema checks, notification channels. Confidence: MEDIUM (training data).
- dbt documentation -- schema.yml contracts, warn-error flags. Confidence: HIGH (well-established pattern in training data).
- Existing codebase analysis -- verified `_send_alert()` stub, `_save_results_to_lakehouse()`, severity system, `_format_results()` output schema. Confidence: HIGH (direct code review).
- Microsoft Teams webhook documentation -- Adaptive Card format for incoming webhooks. Confidence: HIGH (stable API, well-documented in training data).

---
*Feature research for: dq_framework health audit -- alerting, schema evolution, validation history*
*Researched: 2026-03-08*
