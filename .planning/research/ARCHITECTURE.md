# Architecture Research

**Domain:** Data quality framework extension -- alerting, schema evolution, validation history
**Researched:** 2026-03-08
**Confidence:** HIGH

## Current Architecture Context

The existing dq_framework has a clean layered pipeline:

```
Profile -> Configure -> Validate -> Report
```

The three new features (alerting, schema evolution, validation history) are all **post-validation concerns**. They consume validation results but do not modify the core validation pipeline. This is architecturally significant: they extend the existing flow rather than restructuring it.

```
                        EXISTING FLOW
                        =============
┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│   Profile    │──>│  Configure   │──>│   Validate   │──>│    Report    │
│              │   │              │   │              │   │              │
│ DataProfiler │   │ ConfigLoader │   │ DQValidator  │   │ (inline in   │
│ BatchProfiler│   │ constants.py │   │ FabricRunner │   │  validator)  │
└──────────────┘   └──────────────┘   └──────┬───────┘   └──────────────┘
                                             │
                                     results dict
                                             │
                        ┌────────────────────┼────────────────────┐
                        │                    │                    │
                        v                    v                    v
                   NEW FEATURES
                   ============
              ┌──────────────┐   ┌──────────────────┐   ┌──────────────┐
              │   Alerting   │   │ Validation History │   │    Schema    │
              │              │   │                    │   │  Evolution   │
              │ alert.py     │   │ history.py         │   │              │
              │              │   │                    │   │ schema.py    │
              └──────────────┘   └──────────────────┘   └──────────────┘
```

## System Overview: New Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Validation Pipeline (existing)                      │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐                     │
│  │ ConfigLoader │  │ DQValidator │  │ FabricDQRunner   │                     │
│  └─────────────┘  └──────┬──────┘  └────────┬─────────┘                     │
│                          │ results dict      │ results dict                  │
├──────────────────────────┴──────────────────┴───────────────────────────────┤
│                          Post-Validation Layer (new)                         │
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────────┐  ┌────────────────────┐       │
│  │  AlertManager   │  │  ValidationHistory  │  │  SchemaTracker     │       │
│  │                 │  │                     │  │                    │       │
│  │  - TeamsAlert   │  │  - store_result()   │  │  - capture()      │       │
│  │  - EmailAlert   │  │  - get_trend()      │  │  - compare()      │       │
│  │  - AlertRouter  │  │  - get_history()    │  │  - detect_drift() │       │
│  └────────┬────────┘  └─────────┬───────────┘  └────────┬───────────┘       │
│           │                     │                       │                   │
├───────────┴─────────────────────┴───────────────────────┴───────────────────┤
│                          Storage Layer (new)                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │  ResultStore (abstract)                                             │     │
│  │    - JSONFileStore (local: JSON files in a directory)               │     │
│  │    - LakehouseStore (Fabric: JSON via mssparkutils.fs.put)         │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

| Component | Responsibility | Location | Depends On |
|-----------|----------------|----------|------------|
| **AlertManager** | Route validation results to notification channels based on config | `dq_framework/alerting.py` | Results dict from validator |
| **TeamsAlertChannel** | Format and send alerts to MS Teams via webhook | `dq_framework/alerting.py` | `requests` (HTTP), Teams webhook URL |
| **EmailAlertChannel** | Format and send alerts via SMTP | `dq_framework/alerting.py` | `smtplib` (stdlib), SMTP config |
| **ValidationHistory** | Store, retrieve, and analyze validation results over time | `dq_framework/history.py` | ResultStore |
| **SchemaTracker** | Capture DataFrame schemas, compare across runs, detect drift | `dq_framework/schema.py` | ResultStore (for persisting snapshots) |
| **ResultStore** | Abstract storage interface for persistence across local/Fabric | `dq_framework/store.py` | FileSystemHandler (existing) |

## Recommended Project Structure

```
dq_framework/
├── __init__.py              # Add new public exports
├── validator.py             # UNCHANGED - core validation
├── fabric_connector.py      # MODIFIED - wire in alerting, history, schema hooks
├── config_loader.py         # UNCHANGED
├── constants.py             # ADD alerting/history/schema constants
├── data_profiler.py         # UNCHANGED
├── batch_profiler.py        # UNCHANGED
├── loader.py                # UNCHANGED
├── ingestion.py             # UNCHANGED
├── utils.py                 # UNCHANGED
├── alerting.py              # NEW - AlertManager, TeamsAlertChannel, EmailAlertChannel
├── history.py               # NEW - ValidationHistory, trend analysis
├── schema.py                # NEW - SchemaTracker, schema comparison
└── store.py                 # NEW - ResultStore, JSONFileStore, LakehouseStore
```

### Structure Rationale

- **One file per concern:** Each new feature gets its own module. Alerting, history, and schema evolution are distinct responsibilities that should not be tangled.
- **Shared storage abstraction:** Both ValidationHistory and SchemaTracker need to persist data. A shared `ResultStore` in `store.py` avoids duplicating local-vs-Fabric file I/O logic. This also replaces the inline `_save_results_to_lakehouse` method currently in `fabric_connector.py`.
- **No subdirectories:** The existing codebase is flat (all modules in `dq_framework/`). Adding subdirectories for 3-4 new files would be over-engineering. Stay flat.

## Architectural Patterns

### Pattern 1: Channel-Based Alerting (Strategy Pattern)

**What:** AlertManager dispatches to pluggable alert channels. Each channel (Teams, Email) implements a common interface. The manager decides which channels to fire based on YAML config.

**When to use:** When you need multiple notification backends with a unified trigger point.

**Trade-offs:** Slightly more code than hardcoding, but makes adding Slack/PagerDuty/etc. trivial later. Worth it even with just two channels.

**Example:**

```python
from abc import ABC, abstractmethod
from typing import Any

class AlertChannel(ABC):
    """Base class for alert channels."""

    @abstractmethod
    def send(self, alert_payload: dict[str, Any]) -> bool:
        """Send an alert. Returns True on success."""
        ...

class TeamsAlertChannel(AlertChannel):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send(self, alert_payload: dict[str, Any]) -> bool:
        # POST adaptive card JSON to webhook URL
        ...

class EmailAlertChannel(AlertChannel):
    def __init__(self, smtp_host: str, smtp_port: int, recipients: list[str], **kwargs):
        ...

    def send(self, alert_payload: dict[str, Any]) -> bool:
        # Send HTML email via smtplib
        ...

class AlertManager:
    def __init__(self, channels: list[AlertChannel], alert_on: str = "failure"):
        self.channels = channels
        self.alert_on = alert_on  # "failure", "always", "threshold"

    @classmethod
    def from_config(cls, alert_config: dict) -> "AlertManager":
        """Build AlertManager from YAML alert configuration."""
        channels = []
        if "teams" in alert_config:
            channels.append(TeamsAlertChannel(alert_config["teams"]["webhook_url"]))
        if "email" in alert_config:
            channels.append(EmailAlertChannel(**alert_config["email"]))
        return cls(channels, alert_on=alert_config.get("alert_on", "failure"))

    def notify(self, results: dict[str, Any]) -> None:
        """Send alerts based on results and alert_on policy."""
        ...
```

### Pattern 2: Append-Only History with JSON-per-Run

**What:** Each validation run appends a JSON file to a results directory. ValidationHistory reads all files to compute trends. No database required.

**When to use:** When you need persistence but cannot assume database availability (Fabric notebooks have filesystem access, not SQL databases).

**Trade-offs:** Simple and robust. Scales to thousands of runs easily (each file is ~1-2KB). For millions of runs, you would need a database -- but that is far beyond the scope here. The existing `_save_results_to_lakehouse` already writes JSON files this way.

**Example:**

```python
class ValidationHistory:
    def __init__(self, store: "ResultStore"):
        self.store = store

    def record(self, results: dict[str, Any], dataset_name: str) -> None:
        """Persist a validation result."""
        entry = {
            "dataset": dataset_name,
            "timestamp": results["timestamp"],
            "success": results["success"],
            "success_rate": results["success_rate"],
            "evaluated_checks": results["evaluated_checks"],
            "failed_checks": results["failed_checks"],
            "severity_stats": results.get("severity_stats", {}),
            "threshold_failures": results.get("threshold_failures", []),
        }
        self.store.save("history", dataset_name, entry)

    def get_trend(self, dataset_name: str, last_n: int = 10) -> list[dict]:
        """Get recent validation results for trend analysis."""
        ...

    def get_summary(self, dataset_name: str) -> dict:
        """Compute summary stats: improving/degrading, average rate, failure frequency."""
        ...
```

### Pattern 3: Schema Snapshots with Diff Comparison

**What:** SchemaTracker captures a DataFrame's schema (column names, dtypes, nullable, cardinality) as a snapshot dict. On subsequent runs, it loads the previous snapshot and computes a diff: added columns, removed columns, type changes.

**When to use:** When upstream data sources may change schema without warning (common in production data pipelines).

**Trade-offs:** Lightweight -- only stores metadata, not data. Cannot detect semantic changes (e.g., a column that used to hold emails now holds phone numbers) -- only structural changes. That limitation is acceptable for a validation framework.

**Example:**

```python
class SchemaTracker:
    def __init__(self, store: "ResultStore"):
        self.store = store

    @staticmethod
    def capture(df: "pd.DataFrame") -> dict[str, Any]:
        """Capture schema from a DataFrame."""
        schema = {
            "columns": {},
            "row_count": len(df),
            "column_count": len(df.columns),
            "captured_at": datetime.now().isoformat(),
        }
        for col in df.columns:
            schema["columns"][col] = {
                "dtype": str(df[col].dtype),
                "nullable": bool(df[col].isnull().any()),
                "null_pct": float(df[col].isnull().mean() * 100),
                "n_unique": int(df[col].nunique()),
            }
        return schema

    def compare(self, current: dict, previous: dict) -> dict[str, Any]:
        """Compare two schema snapshots. Returns a diff."""
        diff = {
            "added_columns": [],
            "removed_columns": [],
            "type_changes": [],
            "has_breaking_changes": False,
        }
        curr_cols = set(current["columns"].keys())
        prev_cols = set(previous["columns"].keys())

        diff["added_columns"] = list(curr_cols - prev_cols)
        diff["removed_columns"] = list(prev_cols - curr_cols)

        for col in curr_cols & prev_cols:
            if current["columns"][col]["dtype"] != previous["columns"][col]["dtype"]:
                diff["type_changes"].append({
                    "column": col,
                    "from": previous["columns"][col]["dtype"],
                    "to": current["columns"][col]["dtype"],
                })

        # Breaking = removed columns or type changes
        diff["has_breaking_changes"] = bool(
            diff["removed_columns"] or diff["type_changes"]
        )
        return diff
```

## Data Flow

### Alerting Flow

```
validate() returns results dict
    |
    v
AlertManager.notify(results)
    |
    ├── Check alert_on policy (failure/always/threshold)
    │   └── Skip if policy not met
    |
    ├── TeamsAlertChannel.send(payload)
    │   └── POST to webhook URL with adaptive card
    |
    └── EmailAlertChannel.send(payload)
        └── SMTP send with HTML body
```

### Validation History Flow

```
validate() returns results dict
    |
    v
ValidationHistory.record(results, dataset_name)
    |
    v
ResultStore.save("history", dataset_name, entry)
    |
    ├── JSONFileStore: write to .dq_results/history/{dataset}/{timestamp}.json
    └── LakehouseStore: mssparkutils.fs.put to Files/dq_results/history/...
```

### Schema Evolution Flow

```
Before validation:
    |
    v
SchemaTracker.capture(df) -> current_schema
    |
    v
ResultStore.load_latest("schema", dataset_name) -> previous_schema (or None)
    |
    v
SchemaTracker.compare(current, previous) -> diff
    |
    ├── diff has breaking changes?
    │   ├── YES: Log warning, include in results, optionally alert
    │   └── NO: Log info, include in results
    |
    v
ResultStore.save("schema", dataset_name, current_schema)
```

### Integration Point: FabricDataQualityRunner

The key integration question is: where do these three features get wired in? The answer is **FabricDataQualityRunner** and a new optional wrapper around `DataQualityValidator.validate()`.

```
FabricDataQualityRunner.validate_spark_dataframe(spark_df)
    |
    v
1. Convert Spark -> pandas (existing)
    |
    v
2. SchemaTracker.capture(pdf)                          # NEW: before validation
   SchemaTracker.compare(current, previous)            # NEW: detect drift
    |
    v
3. DataQualityValidator.validate(pdf)                  # EXISTING: core validation
    |
    v
4. ValidationHistory.record(results, dataset_name)     # NEW: persist results
    |
    v
5. AlertManager.notify(results)                        # NEW: send notifications
    |
    v
6. Return results (enriched with schema_diff)          # MODIFIED: add schema info
```

For users who use `DataQualityValidator` directly (not through FabricRunner), provide a convenience function or let them wire the components themselves. Do not force these features into the core validator -- keep it focused on validation only.

### YAML Configuration Extension

The alerting, history, and schema config should live in the existing YAML config files:

```yaml
validation_name: "my_table"
expectations:
  - ...

# NEW SECTIONS (all optional)
alerting:
  alert_on: "failure"          # failure | always | threshold
  teams:
    webhook_url: "https://..."
  email:
    smtp_host: "smtp.office365.com"
    smtp_port: 587
    sender: "dq@company.com"
    recipients: ["team@company.com"]
    use_tls: true

history:
  enabled: true
  results_path: "Files/dq_results"    # or local path
  retention_days: 90                   # auto-cleanup old results

schema_tracking:
  enabled: true
  alert_on_breaking: true              # trigger alert on breaking schema changes
  schemas_path: "Files/dq_schemas"     # or local path
```

## Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| Validator -> AlertManager | AlertManager receives results dict | One-way. Alerting never modifies results. |
| Validator -> ValidationHistory | History receives results dict | One-way. History never modifies results. |
| SchemaTracker -> Validator | Schema diff added to results dict | Schema runs before validation. Diff is informational. |
| AlertManager -> TeamsChannel | Manager calls channel.send() | Strategy pattern. Manager owns retry logic. |
| AlertManager -> EmailChannel | Manager calls channel.send() | Same interface as Teams. |
| History/Schema -> ResultStore | Both use store for persistence | Shared abstraction prevents duplicated I/O logic. |
| FabricRunner -> All new components | FabricRunner orchestrates the pipeline | FabricRunner is the integration point, not Validator. |

## Anti-Patterns

### Anti-Pattern 1: Baking Alerting Into the Validator

**What people do:** Add Teams/email logic directly into `DataQualityValidator.validate()`.

**Why it is wrong:** The validator's job is to validate data and return results. Adding I/O (HTTP calls, SMTP) makes it untestable without mocking external services, violates single responsibility, and couples validation to notification infrastructure.

**Do this instead:** Keep the validator pure. Let the caller (FabricRunner or user code) decide whether to alert. The AlertManager is a separate component that receives results.

### Anti-Pattern 2: Using a Database for History Storage

**What people do:** Add SQLite or PostgreSQL dependency for validation history.

**Why it is wrong:** The framework runs in MS Fabric notebooks where database access is not guaranteed. It also runs locally for development. Adding a database dependency increases complexity and breaks the "works anywhere" principle.

**Do this instead:** Use JSON files via the existing FileSystemHandler abstraction. JSON files work on local filesystem and in Fabric Lakehouse via mssparkutils. If a database is ever needed, implement a new ResultStore backend -- the abstraction supports it.

### Anti-Pattern 3: Schema Tracking That Blocks Validation

**What people do:** Make schema evolution checks a hard gate that prevents validation from running when schema changes are detected.

**Why it is wrong:** Schema changes are informational. The validation expectations themselves will catch data quality issues caused by schema changes. Blocking validation on schema drift means you lose the validation results that would tell you what actually broke.

**Do this instead:** Schema tracking is advisory. Capture the diff, include it in results, optionally alert on breaking changes, but always run validation.

### Anti-Pattern 4: Monolithic Alert Payload

**What people do:** Send the entire validation results dict (including all expectation details) as the alert body.

**Why it is wrong:** Teams messages have payload size limits (~28KB for adaptive cards). Email bodies become unreadable. The full results dict includes GX internal objects that are not serializable.

**Do this instead:** Build a concise alert payload: dataset name, pass/fail, success rate, top 3-5 failures, timestamp, link to full results if available.

## Build Order (Dependencies Between Components)

The components have a clear dependency chain that dictates build order:

```
Phase 1: ResultStore (store.py)
    No dependencies on other new components.
    Required by History and SchemaTracker.
    Replaces inline _save_results_to_lakehouse.

Phase 2: AlertManager (alerting.py)
    Depends on: nothing new (receives plain dicts)
    Can be built independently of store.
    Teams channel needs: requests library
    Email channel needs: smtplib (stdlib)

Phase 3: ValidationHistory (history.py)
    Depends on: ResultStore (Phase 1)
    Needs store to persist and query results.

Phase 4: SchemaTracker (schema.py)
    Depends on: ResultStore (Phase 1)
    Needs store to persist and load schema snapshots.

Phase 5: Integration (fabric_connector.py modifications)
    Depends on: All of the above
    Wire AlertManager, ValidationHistory, SchemaTracker into
    FabricDataQualityRunner pipeline.
    Update YAML config schema in ConfigLoader.
    Add constants to constants.py.
    Update __init__.py exports.
```

**Why this order:**
- ResultStore first because both History and SchemaTracker depend on it. Building it first also lets you immediately refactor `_save_results_to_lakehouse` out of fabric_connector.py.
- AlertManager second because it is self-contained (no store dependency) and can be tested immediately with mock results.
- History and Schema (Phases 3-4) are independent of each other but both need ResultStore.
- Integration last because it wires everything together and requires all components to exist.

**Parallelism opportunity:** Phases 2, 3, and 4 can be built in parallel once Phase 1 is complete.

## Scaling Considerations

| Scale | Architecture Adjustments |
|-------|--------------------------|
| 1-50 datasets | JSON files in a single directory. No indexing needed. Linear scan for trends is fast. |
| 50-500 datasets | Organize by dataset subdirectory (already in the design). One directory per dataset keeps file counts manageable. |
| 500+ datasets | Consider a manifest/index file per dataset that tracks available history files. Avoids listing hundreds of files. Not needed upfront -- add when the need arises. |

### First Bottleneck: History File Accumulation

At ~10 validation runs per day per dataset across 50 datasets, you get ~500 files/day. After a year, ~180K files. This is manageable with directory-per-dataset structure but warrants a retention policy (e.g., `retention_days: 90`). Build cleanup into ValidationHistory from day one.

### Second Bottleneck: Alert Latency

Teams webhooks can be slow (1-5 seconds). Email SMTP can be slow (2-10 seconds). Both should be fire-and-forget with retry -- never block the validation pipeline return. Use the existing exponential backoff pattern from `_send_alert`.

## External Services

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| MS Teams | HTTP POST to incoming webhook URL | Adaptive Card format. Payload limit ~28KB. Webhook URL configured in YAML. |
| Email (SMTP) | `smtplib.SMTP` with TLS | Office 365: smtp.office365.com:587. Auth credentials via environment variables, never in YAML. |
| Fabric Lakehouse | `mssparkutils.fs.put/head` | Already used for results storage. Extend for history and schema files. |

## Sources

- Existing codebase analysis: `dq_framework/validator.py`, `dq_framework/fabric_connector.py`, `dq_framework/constants.py`
- Existing architecture document: `.planning/codebase/ARCHITECTURE.md`
- MS Teams webhook adaptive card format: standard Teams incoming webhook pattern (HIGH confidence, well-documented Microsoft API)
- Python `smtplib` for email: stdlib, no external dependency needed (HIGH confidence)

---
*Architecture research for: dq_framework alerting, schema evolution, and validation history*
*Researched: 2026-03-08*
