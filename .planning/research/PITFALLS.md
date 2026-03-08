# Pitfalls Research

**Domain:** Data quality framework audit -- alerting, schema evolution, validation history, packaging modernization
**Researched:** 2026-03-08
**Confidence:** HIGH (codebase-verified) / MEDIUM (Teams deprecation timeline from Microsoft docs)

## Critical Pitfalls

### Pitfall 1: Building Teams Alerting on a Deprecated Connector Model

**What goes wrong:**
Microsoft is retiring Office 365 Connectors (including the classic Incoming Webhook connector) with a final deadline of **April 30, 2026**. Building the alerting system against the legacy `https://xxxxx.webhook.office.com/` URL format means the entire alerting feature could break within weeks of shipping. The legacy message card format (`MessageCard`) is also being replaced by Adaptive Cards.

**Why it happens:**
Most tutorials, Stack Overflow answers, and existing examples still show the legacy O365 Connector webhook approach. The new approach -- Power Automate Workflows with the "When a Teams webhook request is received" trigger -- is less documented and has a different URL format and payload structure. Developers implement what they find in search results without checking the deprecation timeline.

**How to avoid:**
- Use the Power Automate Workflows webhook trigger from the start, not O365 Connectors.
- Payload format must be Adaptive Card JSON (the `"type": "message", "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", ...}]` format), not the legacy MessageCard format.
- Design the alerting interface to be channel-agnostic: abstract behind a `NotificationChannel` protocol/ABC so that Teams, email, and future channels are interchangeable.
- Store webhook URLs in configuration (YAML or environment variables), never hardcoded.
- Known limitation: Workflows can't post in private channels as a flow bot (only on behalf of a user). Document this constraint.

**Warning signs:**
- Webhook URLs containing `webhook.office.com` without the Workflows path structure.
- Code using `MessageCard` JSON format instead of Adaptive Card format.
- No abstraction layer -- Teams-specific logic mixed directly into the alert sending method.

**Phase to address:**
Alerting implementation phase. Design the abstraction before implementing any channel. Validate against a real Workflows webhook endpoint during development, not just unit tests.

**Source:** [Microsoft retirement announcement](https://devblogs.microsoft.com/microsoft365dev/retirement-of-office-365-connectors-within-microsoft-teams/), [Teams webhook docs](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook) -- confirmed April 2026 deadline.

---

### Pitfall 2: Alert Failures Silently Swallowing Pipeline Errors

**What goes wrong:**
The existing `_send_alert` method (fabric_connector.py:457-508) has retry logic with exponential backoff but returns `bool` -- and the caller `handle_failure` ignores the return value. If alerting fails after all retries, the pipeline continues silently. Production teams think they have alerting but are actually flying blind during outages, network partitions, or credential expiry.

**Why it happens:**
Alerting is treated as "fire and forget" side-effect rather than a critical pipeline concern. The existing code structure already demonstrates this: `handle_failure(action="alert")` calls `_send_alert()` but does nothing with the `False` return. When the TODO is replaced with real HTTP calls, failed alerts will be silently swallowed.

**How to avoid:**
- Define explicit failure policies: `alert_on_failure_policy: "warn" | "raise" | "fallback"`.
- When `policy="raise"`: if all alert channels fail, raise an exception so the pipeline doesn't silently proceed.
- When `policy="fallback"`: try Teams first, fall back to email, fall back to logging. Only swallow if ALL channels fail and policy allows it.
- Always log alert failures at ERROR level regardless of policy.
- Add a `dead_letter` mechanism: if alerts can't be delivered, write them to a local file or Lakehouse location for later inspection.

**Warning signs:**
- Alert method returns bool but caller ignores it (already present in codebase).
- No fallback chain between channels.
- Integration tests only test the happy path (successful send).
- No monitoring of alert delivery success rate.

**Phase to address:**
Alerting implementation phase. Fix the `handle_failure` caller to respect return values as part of the same work that implements the actual sending logic.

---

### Pitfall 3: Schema Evolution Tracking Without a Baseline Snapshot Strategy

**What goes wrong:**
Schema evolution detection requires comparing "current schema" against "previous schema," but developers implement it without defining where baselines are stored, how they are versioned, or what happens on first run. This leads to: (1) false positives on first run because there is no previous baseline, (2) lost baselines when Lakehouse files are reorganized, (3) race conditions when multiple pipeline runs detect changes simultaneously, (4) no distinction between intentional schema changes and data corruption.

**Why it happens:**
Schema tracking seems simple ("just diff the columns") but the persistence and lifecycle questions are harder than the detection logic. Developers focus on the diff algorithm and defer storage decisions, resulting in fragile file-based persistence that breaks in production.

**How to avoid:**
- Define the baseline storage format early: a JSON/YAML schema snapshot with timestamp, source identifier, and column metadata (name, dtype, nullable, stats).
- Store baselines in a deterministic location per data source (e.g., `{results_location}/schema_baselines/{source_name}_baseline.json`).
- Handle first-run gracefully: no baseline = create baseline, no alert. Log it as INFO.
- Include a `schema_change_policy` config option: `"alert"`, `"fail"`, `"accept"` -- so intentional changes (like adding a column after a migration) can be acknowledged without false alarms.
- Make baseline updates explicit: never auto-update the baseline on detection. Require either manual approval or a config flag like `accept_current_schema: true`.

**Warning signs:**
- Schema detection works in tests but crashes on first production run (no prior baseline).
- No config option to suppress known/expected changes.
- Baselines stored in memory or temp locations instead of persistent storage.
- No way to reset or manually set a baseline.

**Phase to address:**
Schema evolution phase. Design the storage and lifecycle model before writing detection logic. The persistence design must work in both local (filesystem) and Fabric (Lakehouse) modes.

---

### Pitfall 4: Validation History Storage Growing Without Bounds

**What goes wrong:**
The current `_save_results_to_lakehouse` (fabric_connector.py:510-536) saves each validation run as a separate JSON file with timestamp in the filename. Adding validation history and trend analysis on top of this pattern means: (1) thousands of small JSON files accumulate over weeks/months, (2) trend queries require reading and deserializing all files, (3) no retention policy means storage grows linearly forever, (4) listing directories with thousands of files becomes slow on both local filesystems and Lakehouse.

**Why it happens:**
Append-only file-per-run is the simplest implementation and works fine for the first few weeks. The problem only manifests after months of production use, when trend queries slow down and storage costs accumulate. By then, the format is entrenched and migration is painful.

**How to avoid:**
- Use a structured storage format from the start: a single append-only JSON Lines (`.jsonl`) file per data source, or a partitioned structure (`results/{source}/{year}/{month}/results.jsonl`).
- Implement a retention policy from day one: `validation_history_retention_days: 90` config option. Run cleanup on each write.
- For trend analysis, maintain a lightweight summary file (e.g., `{source}_trends.json`) that stores aggregated daily/weekly stats, separate from the raw results. Query the summary for trends, raw files for drill-down.
- Cap the number of raw result files per source (e.g., keep last 100). Rotate on write.
- On local mode: use SQLite for history instead of flat files (single file, SQL queries, built into Python).

**Warning signs:**
- No retention policy in the config schema.
- Trend analysis function reads all historical files on every call.
- Test suite only tests with 1-5 result files, never tests performance at 500+.
- No consideration for the local vs. Fabric storage divergence.

**Phase to address:**
Validation history phase. Design the storage schema and retention policy before implementing trend analysis. The trend analysis API should work against the summary/aggregate, not raw files.

---

### Pitfall 5: Removing setup.py Before Verifying AIMS Editable Install Still Works

**What goes wrong:**
AIMS imports dq_framework as an editable sibling package (`pip install -e .`). Removing `setup.py` and relying solely on `pyproject.toml` is correct for modern Python, but the AIMS project may have: (1) scripts or CI that invoke `python setup.py install/develop`, (2) a pip version old enough to not support PEP 660 editable installs via `pyproject.toml`, (3) Fabric notebook environments with specific pip versions that behave differently. The removal succeeds locally but breaks the AIMS integration or Fabric deployment.

**Why it happens:**
The migration is logically simple ("delete the file") but the downstream effects span multiple environments. Developers test locally with modern pip (>= 21.3) but don't verify the Fabric notebook pip version or AIMS CI configuration.

**How to avoid:**
- Before removing `setup.py`, verify AIMS' pip version in CI and in Fabric notebooks. PEP 660 editable installs require pip >= 21.3 and setuptools >= 64.0.
- Test `pip install -e .` in a clean venv with only `pyproject.toml` (no `setup.py`) before committing the removal.
- Check AIMS `requirements.txt` or install scripts for any reference to `setup.py` or `python setup.py develop`.
- Remove `setup.py` in a dedicated commit so it can be reverted independently if something breaks downstream.
- Verify `pyproject.toml` build-system section has `requires = ["setuptools>=65.0", "wheel"]` (already present -- confirmed).

**Warning signs:**
- AIMS CI installs dq_framework with `python setup.py develop` instead of `pip install -e .`.
- Fabric notebooks use pip < 21.3.
- The `setup.py` removal commit is bundled with other changes, making it hard to revert.

**Phase to address:**
Packaging modernization phase (should be early). This is a prerequisite gate -- verify downstream compatibility before removing anything.

---

### Pitfall 6: Fixing Bugs in Untested Code Without Writing Tests First

**What goes wrong:**
The chunked Spark validation bug (monotonically_increasing_id) and aggregated results bug are in `fabric_connector.py`, which has 18.5% test coverage. Fixing these bugs without first writing tests that reproduce them means: (1) no proof the fix actually works, (2) risk of introducing regressions, (3) the fix may address symptoms but not root cause, (4) future developers can re-break the same code without test failures.

**Why it happens:**
The bugs are understood ("monotonically_increasing_id doesn't guarantee sequential values" and "chunk counts are inflated"), so developers jump to the fix. Writing tests for Fabric-dependent code requires extensive mocking of Spark/mssparkutils, which feels like overhead. The "just fix it" instinct overrides test discipline.

**How to avoid:**
- Test-first for every bug fix: write a failing test that demonstrates the bug, then fix the code, then verify the test passes.
- For `fabric_connector.py`, create a test fixture pattern early: a reusable mock setup for Spark DataFrames (using pandas DataFrames as stand-ins) and mssparkutils. Invest in this fixture once; reuse for all Fabric tests.
- For the monotonically_increasing_id bug specifically: the test should create a multi-partition-like scenario (non-sequential IDs) and verify correct chunking.
- For the aggregation bug: the test should verify that statistics are per-source, not inflated by chunk count.

**Warning signs:**
- Bug fix PRs with no new test files or test functions.
- Coverage of `fabric_connector.py` doesn't increase after bug fixes.
- Bug fix descriptions say "tested manually" instead of citing test names.

**Phase to address:**
Bug fix and testing phase. Write the Fabric mock fixture first, then use it for bug reproduction tests, then fix bugs. Coverage improvement and bug fixes are the same work, not separate phases.

---

### Pitfall 7: Threshold Logic Changes Breaking Existing AIMS Behavior

**What goes wrong:**
The severity-based threshold logic in `_format_results` (validator.py:201-337) has three interacting threshold systems. Adding tests for this logic may reveal that the current behavior is subtly wrong, tempting developers to "fix" it. But AIMS pipelines in production depend on the current behavior -- even if it's technically incorrect. "Fixing" the threshold logic changes which pipelines pass or fail, potentially causing production pipeline failures.

**Why it happens:**
The distinction between "buggy" and "working as depended-upon" is subtle. When you write tests that expose edge cases in threshold logic, the natural instinct is to fix the logic. But the fix changes production behavior.

**How to avoid:**
- Write characterization tests first: tests that document current behavior, not desired behavior. These tests capture what `_format_results` actually does today, including edge cases.
- Treat threshold behavior changes as breaking changes. If a fix changes which pipelines pass/fail, it must be communicated to AIMS users and gated behind a config flag or version bump.
- If the logic is wrong, add a `threshold_version: "v1" | "v2"` config option so existing users stay on v1 behavior and new users can opt into corrected v2 behavior.
- Never change threshold logic and add new features (alerting, history) in the same commit/PR. Isolate behavior changes.

**Warning signs:**
- Tests written for "correct" behavior rather than documenting existing behavior.
- Threshold logic changes bundled with unrelated features.
- No communication plan for AIMS pipeline operators.

**Phase to address:**
Testing/stabilization phase. Write characterization tests before any threshold logic changes. If fixes are needed, they belong in a separate, clearly communicated change.

---

## Technical Debt Patterns

Shortcuts that seem reasonable but create long-term problems.

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Hardcoding Teams webhook URL in config examples | Quick to demo | Users copy examples and hardcode URLs in production configs; URLs rotate or expire | Never -- always use env var references in examples |
| Storing validation history as individual JSON files | Simple implementation | Thousands of small files, slow queries, no retention | Only for initial prototype; migrate to JSONL/SQLite before release |
| Using `time.sleep()` in retry logic | Simple backoff | Blocks the pipeline thread during retries; Fabric notebooks may timeout | Acceptable for alerting (non-critical path) but use async/threading for critical paths |
| Skipping type hints on alert payloads | Faster implementation | Alert payload schema drifts between Teams and email adapters; runtime errors in production | Never -- TypedDict or dataclass from the start |
| Mocking at the wrong level for Fabric tests | Tests pass quickly | Tests don't catch real integration failures; mock Spark behavior diverges from real Spark | Never -- mock at the boundary (mssparkutils), not inside framework methods |

## Integration Gotchas

Common mistakes when connecting to external services.

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| Teams Workflows Webhook | Sending legacy MessageCard JSON format | Use Adaptive Card format wrapped in `{"type": "message", "attachments": [...]}` envelope. Workflows only support Adaptive Cards. |
| Teams Workflows Webhook | Not handling HTTP 429 (rate limiting) | Implement retry with exponential backoff. Teams throttles at >4 requests/second per webhook. The existing retry skeleton in `_send_alert` is a good foundation. |
| Teams Workflows Webhook | Assuming private channel support | Workflows can't post to private channels as a flow bot. Document this limitation and offer email as fallback for private channel notifications. |
| SMTP Email | Using `smtplib` without TLS/STARTTLS | Always use `smtplib.SMTP_SSL` or `SMTP.starttls()`. Corporate mail servers (Exchange/M365) require TLS. |
| SMTP Email | Hardcoding SMTP credentials in config | Use environment variables or Azure Key Vault references. Never put passwords in YAML config files, even if gitignored. |
| SMTP Email | Not handling SMTP authentication failures gracefully | Catch `smtplib.SMTPAuthenticationError` separately from connection errors. Auth failures should not trigger retries (the credentials are wrong, retrying won't help). |
| Lakehouse Results Storage | Using `mssparkutils.fs.put()` without checking path exists | Lakehouse paths may not exist on first run. Create parent directories explicitly or handle the error. |

## Performance Traps

Patterns that work at small scale but fail as usage grows.

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Reading all history files for trend analysis | Trend queries take >10s, then >60s, then timeout | Maintain a summary/aggregate file; query that instead of raw files | >500 result files per source (~2-3 months of hourly validation) |
| Synchronous alerting in the validation pipeline | Pipeline latency increases by alert round-trip time (1-5s per channel) | Send alerts asynchronously or in a background thread; don't block validation result return | When alerting to multiple channels or when network latency is high |
| Schema comparison loading full DataFrames for column info | Memory spike and slow comparison on large datasets | Use `pd.read_csv(..., nrows=0)` or `pd.read_parquet(...).columns` to get schema without loading data | Datasets >1GB |
| Unbounded retry loops on alert failures | Pipeline hangs for minutes on network outage (3 retries * exponential backoff) | Set a max total timeout (e.g., 30s) in addition to max retries; fail fast if pipeline is time-constrained | When Fabric pipeline has tight SLA timeouts |

## Security Mistakes

Domain-specific security issues beyond general web security.

| Mistake | Risk | Prevention |
|---------|------|------------|
| Logging webhook URLs in alert failure messages | Webhook URLs are secrets -- anyone with the URL can post to the Teams channel | Log only the last 8 characters or a hash of the URL in error messages |
| Storing SMTP passwords in YAML config files | Config files may be committed to git or shared in Fabric notebooks | Use environment variables (`SMTP_PASSWORD`) or reference Azure Key Vault; validate that password fields are never written to logs |
| Including full validation result data in alert payloads | Alert payloads may contain sensitive column names, data samples, or business metrics | Sanitize alert payloads: include only suite name, pass/fail, counts, timestamp -- never raw data values |
| Schema baseline files containing data samples | If schema snapshots include example values for type inference, they leak production data | Schema baselines should contain only structural metadata (column names, types, nullable) -- never data values |

## UX Pitfalls

Common user experience mistakes in this domain.

| Pitfall | User Impact | Better Approach |
|---------|-------------|-----------------|
| Alert messages with no actionable information | User receives "Validation failed" but doesn't know which dataset, which checks, or what to do | Include: dataset name, failure count, top 3 failed checks, link to full results, suggested action |
| No way to silence known/expected alerts | Schema change alerts fire repeatedly after an intentional migration until someone manually updates baseline | Provide `accept_schema` CLI command or config flag to acknowledge known changes |
| Validation history only accessible via code | Non-technical stakeholders can't check data quality trends | Expose history as structured data (CSV/JSONL export) that can be opened in Excel or Power BI |
| Alert configuration requires code changes | Adding a new webhook URL or email recipient requires a code deploy | All alert configuration in YAML: channels, recipients, webhook URLs, severity filter (which severities trigger alerts) |

## "Looks Done But Isn't" Checklist

Things that appear complete but are missing critical pieces.

- [ ] **Teams alerting:** Often missing Adaptive Card format validation -- verify the JSON payload renders correctly in Teams (not just that the HTTP POST succeeds with 200)
- [ ] **Teams alerting:** Often missing handling of the Workflows-specific response format -- verify error responses are parsed, not just status codes
- [ ] **Email alerting:** Often missing HTML email rendering -- verify emails render in Outlook/M365 (not just checked in a dev mail client)
- [ ] **Email alerting:** Often missing CC/BCC support -- verify the YAML config schema supports multiple recipients
- [ ] **Schema evolution:** Often missing first-run handling -- verify the system works when no prior baseline exists
- [ ] **Schema evolution:** Often missing type change detection -- verify that a column changing from `int64` to `float64` is detected (not just column add/remove)
- [ ] **Validation history:** Often missing timezone handling -- verify timestamps are timezone-aware (UTC) so trends work across Fabric regions
- [ ] **Validation history:** Often missing concurrent write safety -- verify two simultaneous pipeline runs don't corrupt the history file
- [ ] **Packaging:** Often missing downstream verification -- verify AIMS `pip install -e ../2_DATA_QUALITY_LIBRARY` still works after removing `setup.py`
- [ ] **Packaging:** Often missing CI alignment verification -- verify the CI pipeline passes with the new tool versions (ruff, pytest 9.x) before merging

## Recovery Strategies

When pitfalls occur despite prevention, how to recover.

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Built on deprecated Teams connector | MEDIUM | Replace webhook URL with Workflows URL; convert MessageCard payload to Adaptive Card format; retest. If abstraction layer exists, only the Teams adapter changes. |
| Silent alert failures in production | LOW | Add return value checking and logging. Deploy. Review pipeline logs for past silent failures. |
| Schema baseline lost or corrupted | LOW | Re-run profiler to regenerate baseline from current data. Accept that one cycle of changes since last baseline is lost. |
| Validation history files unbounded | MEDIUM | Write a one-off cleanup script to aggregate old files into JSONL summary. Implement retention policy. May lose granularity for old results. |
| setup.py removal breaks AIMS | LOW | Revert the single commit that removed setup.py. Investigate pip version in AIMS environment. Fix forward with pip upgrade or keep setup.py as shim. |
| Threshold logic change breaks AIMS pipelines | HIGH | Revert the threshold change. Write characterization tests for old behavior. Implement versioned threshold logic. Coordinate with AIMS operators for rollout. |
| Bug fix without tests introduces regression | MEDIUM | Revert to pre-fix state. Write failing test first. Re-apply fix. Verify test passes. Coverage increase proves the fix is tested. |

## Pitfall-to-Phase Mapping

How roadmap phases should address these pitfalls.

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Deprecated Teams connector model | Alerting implementation | Verify webhook URL format is Workflows-compatible; verify Adaptive Card payload renders in Teams |
| Silent alert failures | Alerting implementation | Integration test that simulates HTTP failure and verifies error is raised/logged per policy |
| No schema baseline strategy | Schema evolution design | First-run test passes without prior baseline; baseline file is created in expected location |
| Unbounded history storage | Validation history design | Test with 500+ result files; verify retention policy deletes old files; verify trend query performance |
| setup.py removal breaks AIMS | Packaging modernization (early) | `pip install -e .` succeeds in clean venv with only pyproject.toml; AIMS import test passes |
| Bug fixes without tests | Bug fix / testing phase | Every bug fix PR includes a regression test; fabric_connector.py coverage increases |
| Threshold logic changes break AIMS | Testing / stabilization | Characterization tests document current behavior; any behavior changes are behind config flags |
| Hardcoded secrets in config | Alerting implementation | Config schema uses env var references for secrets; no raw URLs/passwords in example configs |

## Sources

- [Microsoft: Retirement of Office 365 Connectors within Microsoft Teams](https://devblogs.microsoft.com/microsoft365dev/retirement-of-office-365-connectors-within-microsoft-teams/) -- April 2026 deadline confirmed
- [Microsoft: Create an Incoming Webhook (Teams docs)](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook) -- Workflows migration guidance, Adaptive Card requirement, private channel limitation
- Codebase analysis: `fabric_connector.py` lines 457-536 (alert stub, result saving), `validator.py` lines 201-337 (threshold logic)
- `.planning/codebase/CONCERNS.md` -- known bugs, test coverage gaps, tech debt inventory
- `.planning/PROJECT.md` -- project constraints, AIMS dependency, scope

---
*Pitfalls research for: dq_framework health audit and production hardening*
*Researched: 2026-03-08*
