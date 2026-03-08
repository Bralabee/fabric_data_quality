# Stack Research

**Domain:** Data quality framework extensions -- alerting, schema evolution, validation history
**Researched:** 2026-03-08
**Confidence:** MEDIUM-HIGH

## Context

This research covers NEW dependencies needed for Milestone 2 capabilities only. The existing stack (GX 1.x, pandas, pyarrow, SQLAlchemy, etc.) is already established and not re-evaluated here. The three capability areas are:

1. **Alerting** -- Teams webhooks and SMTP email notifications
2. **Schema evolution** -- detecting column additions, removals, type changes between runs
3. **Validation history** -- storing and querying validation results over time for trending

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended | Confidence |
|------------|---------|---------|-----------------|------------|
| `httpx` | >=0.28.0,<1.0 | HTTP client for Teams webhook delivery | Modern async-capable HTTP client. Already widely adopted. Cleaner API than requests for JSON POST. Timeout/retry built-in. The project has no existing HTTP dependency so we pick the best current option. | HIGH |
| `jinja2` | >=3.1.0 | Alert message templating | Industry standard for Python templating. Lets users customize alert card layouts without touching code. Lightweight, zero surprises. v3.1.6 is current stable. | HIGH |
| `deepdiff` | >=8.0.0 | Schema diff computation | Purpose-built for deep object comparison. Detects additions, removals, type changes in nested dicts -- exactly what schema evolution needs. Production-stable, actively maintained (v8.6.1 Sept 2025). Eliminates need to write custom diff logic. | HIGH |
| SQLite (stdlib) | -- | Validation history storage | Already in Python stdlib. Zero new dependencies. SQLAlchemy (already a project dep via GX) provides the ORM layer. Perfect for an embedded library that must work both locally and in Fabric notebooks without requiring external database infrastructure. | HIGH |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `sqlalchemy` | >=2.0.0 (existing) | ORM for validation history tables | Already a dependency (required by GX). Use its Core API (not ORM sessions) to define history tables and query results. Avoids adding SQLModel/another ORM. |
| `alembic` | >=1.18.0 | Database migrations for history schema | Only if history schema evolves between versions. Can defer -- start with `CREATE TABLE IF NOT EXISTS` for v1, add Alembic when schema changes are needed. v1.18.4 is current. |
| `tenacity` | >=8.0.0 | Retry logic for alert delivery | Use for webhook/SMTP retries with exponential backoff. Cleaner than hand-rolling retry loops. Only needed if reliability requirements justify the dependency. |

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| `pytest-httpx` | Mock httpx calls in tests | Pairs with httpx for clean request/response mocking in alert tests |
| `aiosmtpd` | Fake SMTP server for email tests | Run a local SMTP server in test fixtures. v1.4.x is fine for testing. Avoids needing a real mail server. |

## Installation

```bash
# New core dependencies (add to pyproject.toml [project.dependencies])
pip install httpx jinja2 deepdiff

# New optional dependencies (add to pyproject.toml [project.optional-dependencies])
# alerting = ["httpx>=0.28.0,<1.0", "jinja2>=3.1.0"]
# history  = []  # No new deps -- uses stdlib sqlite3 + existing sqlalchemy

# New dev dependencies (add to [project.optional-dependencies] dev)
pip install -D pytest-httpx aiosmtpd
```

### Proposed pyproject.toml Changes

```toml
[project]
dependencies = [
    # ... existing deps ...
    "httpx>=0.28.0,<1.0",
    "jinja2>=3.1.0",
    "deepdiff>=8.0.0",
]

[project.optional-dependencies]
# Keep alerting deps in core since they are small and expected features
dev = [
    # ... existing dev deps ...
    "pytest-httpx>=0.35.0",
    "aiosmtpd>=1.4.0",
]
```

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| `httpx` for Teams webhooks | `requests` | If httpx causes dependency conflicts. requests (v2.32.5) is also fine but httpx has better async story and timeout handling. |
| `httpx` for Teams webhooks | `pymsteams` | NEVER. pymsteams (v0.2.5) only supports legacy O365 Connector Card format. Microsoft is deprecating O365 Connectors in favor of Workflows webhooks which use Adaptive Cards. pymsteams is also seeking new maintainers. |
| `jinja2` for templating | Python f-strings / str.format | Only for the simplest alerts with no user customization. Jinja2 is worth it the moment you want configurable templates. |
| `deepdiff` for schema comparison | Custom dict diff logic | If you want zero new dependencies. But deepdiff handles nested type changes, reordering, and edge cases that custom code will miss. The library is 1.2MB -- worth it. |
| SQLite + SQLAlchemy for history | DuckDB (v1.4.4) | If you need analytical queries over millions of validation records. DuckDB is columnar and fast for aggregations. Overkill for this use case -- validation history will have thousands of records at most. Also adds a 50MB+ native dependency. |
| SQLite + SQLAlchemy for history | SQLModel (v0.0.37) | If you prefer Pydantic-style model definitions. But project already depends on SQLAlchemy via GX, and SQLModel is still 0.x. Using SQLAlchemy Core directly avoids adding another abstraction layer. |
| SQLite for storage | PostgreSQL / cloud DB | Only if history must be shared across multiple Fabric workspaces. SQLite works per-environment which is the right scope for this library. |
| No migration tool initially | Alembic from day one | If you expect frequent schema changes to history tables. For v1, `CREATE TABLE IF NOT EXISTS` is simpler. |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| `pymsteams` | Supports only legacy O365 Connector Cards which Microsoft is actively deprecating. The library is looking for new maintainers. Teams Workflows webhooks require Adaptive Card JSON format, which pymsteams does not support. | Direct `httpx` POST with Adaptive Card JSON payload |
| `smtplib` alone (stdlib) | Works but lacks retry, connection pooling, and timeout handling. Leads to fragile code with manual error handling. | `smtplib` wrapped with `tenacity` for retries, or `httpx` + SMTP relay API if available |
| `pandera` for schema evolution | Pandera is a validation framework, not a schema diff tool. It validates data against a schema but does not detect how schemas change between runs. | `deepdiff` on schema dictionaries extracted from DataFrames |
| DuckDB for history | Massive native dependency (50MB+). Columnar storage is designed for analytical workloads on large datasets. Validation history is small (thousands of rows). Adds complexity to Fabric deployment. | SQLite (stdlib) + SQLAlchemy (existing dep) |
| MongoDB / external databases | Requires infrastructure the library should not assume. This is an embedded library, not a service. | SQLite -- zero infrastructure, works everywhere Python runs |
| `email` stdlib package alone | Low-level, verbose API for constructing MIME messages. Error-prone for HTML emails with attachments. | `email` stdlib for message construction (unavoidable) but wrapped in a clean abstraction with `smtplib` |

## Stack Patterns by Capability

### Alerting (Teams Webhooks)

**Architecture:** Use `httpx` to POST Adaptive Card JSON to Teams Workflow webhook URLs.

Microsoft is retiring O365 Connectors. The replacement is Power Automate Workflows with the "When a Teams webhook request is received" trigger. The webhook accepts Adaptive Card JSON (not the legacy MessageCard format).

**Payload format (verified from Microsoft docs):**
```json
{
  "type": "message",
  "attachments": [
    {
      "contentType": "application/vnd.microsoft.card.adaptive",
      "content": {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.4",
        "body": [
          {"type": "TextBlock", "text": "Validation Failed", "weight": "Bolder"},
          {"type": "TextBlock", "text": "3 critical expectations failed"}
        ]
      }
    }
  ]
}
```

**Key constraints:**
- Message size limit: 28 KB
- Rate limit: 4 requests/second (need retry with backoff)
- Workflows support Adaptive Cards ONLY (not legacy MessageCard)
- Workflows cannot post in private channels as flow bot

Use `jinja2` to template the Adaptive Card JSON so users can customize alert appearance via YAML config.

### Alerting (SMTP Email)

**Architecture:** Use stdlib `smtplib` + `email.mime` for email construction and delivery. No new dependency needed for basic SMTP.

```python
# Minimal dependencies -- stdlib only
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
```

Use `jinja2` (shared with Teams) to template HTML email bodies. Configuration via YAML:
```yaml
alerting:
  email:
    smtp_host: "smtp.office365.com"
    smtp_port: 587
    use_tls: true
    sender: "dq-alerts@org.com"
    recipients: ["team@org.com"]
```

Consider `tenacity` for SMTP retry logic (transient failures are common with corporate mail servers).

### Schema Evolution Detection

**Architecture:** Extract schema metadata from DataFrames as dictionaries, use `deepdiff` to compute diffs between baseline and current schema.

```python
# Schema extraction (no new deps)
def extract_schema(df: pd.DataFrame) -> dict:
    return {
        col: {"dtype": str(df[col].dtype), "nullable": df[col].isna().any()}
        for col in df.columns
    }

# Schema diff (deepdiff)
from deepdiff import DeepDiff
diff = DeepDiff(baseline_schema, current_schema, ignore_order=True)
# Returns: dictionary_item_added, dictionary_item_removed, values_changed, type_changes
```

Schema baselines stored as JSON alongside validation YAML configs. `deepdiff` provides structured output that maps directly to "column added", "column removed", "type changed" events.

### Validation History & Trending

**Architecture:** SQLite database (one per environment) with SQLAlchemy Core for table definitions and queries.

**Why SQLite + SQLAlchemy Core (not ORM):**
- SQLite is stdlib -- zero deployment friction in Fabric notebooks
- SQLAlchemy is already a dependency (GX requires it)
- SQLAlchemy Core (Table, select, insert) is lighter than ORM (Session, relationship) and sufficient for append-only history
- History DB is per-environment (local dev vs Fabric workspace), which matches SQLite's single-file model

**Table design (SQLAlchemy Core):**
```python
from sqlalchemy import MetaData, Table, Column, String, Float, Integer, DateTime, JSON

metadata = MetaData()

validation_results = Table(
    "validation_results", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, nullable=False),
    Column("validation_name", String, nullable=False),
    Column("timestamp", DateTime, nullable=False),
    Column("overall_success", Integer, nullable=False),  # 0/1
    Column("success_percent", Float),
    Column("total_expectations", Integer),
    Column("successful_expectations", Integer),
    Column("failed_expectations", Integer),
    Column("severity_level", String),
    Column("details_json", JSON),  # Full GX result for drill-down
)
```

**Trending queries:** Simple SQL aggregations over time windows. No need for pandas -- SQLAlchemy Core handles this cleanly.

## Version Compatibility

| Package | Compatible With | Notes |
|---------|-----------------|-------|
| `httpx` >=0.28.0 | Python >=3.10 | Fully compatible. 1.0 dev releases exist but pin to <1.0 for stability. |
| `jinja2` >=3.1.0 | Python >=3.10 | No issues. Widely compatible. |
| `deepdiff` >=8.0.0 | Python >=3.9 | Compatible. Uses `orderly-set` internally, no conflicts with existing deps. |
| `sqlalchemy` >=2.0.0 | SQLite (stdlib) | Already a dependency. SQLite support is built-in (no driver needed). |
| All new deps | Great Expectations 1.x | No conflicts. GX already pulls in SQLAlchemy, requests, jinja2 (verify -- GX may already bring jinja2). |

**Important:** Check if GX 1.x already transitively depends on `jinja2` and `requests`. If so, `jinja2` may already be available without adding it explicitly. However, declare it explicitly in `pyproject.toml` to avoid breakage if GX changes its deps.

## Dependency Weight Assessment

| New Dependency | Install Size | Transitive Deps | Justification |
|----------------|-------------|-----------------|---------------|
| `httpx` | ~1.5MB | httpcore, certifi, idna, sniffio, h11 | Essential for webhook delivery. Modern HTTP client. |
| `jinja2` | ~1MB | markupsafe | May already be transitive via GX. Essential for templating. |
| `deepdiff` | ~1.2MB | orderly-set | Best-in-class for schema diffing. No alternative avoids this work. |
| **Total new** | **~3.7MB** | **~5 transitive** | Minimal footprint for three major features. |

## Sources

- [Microsoft Teams Webhooks docs](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook) -- Verified O365 Connector deprecation, Workflows replacement, Adaptive Card requirement (MEDIUM confidence, docs dated 2025-06-10)
- [Adaptive Cards schema](https://adaptivecards.io/explorer/) -- Version 1.4-1.6 schema format (HIGH confidence)
- [PyPI: httpx](https://pypi.org/project/httpx/) -- v0.28.1, released 2024-12-06 (HIGH confidence)
- [PyPI: deepdiff](https://pypi.org/project/deepdiff/) -- v8.6.1, released 2025-09-03 (HIGH confidence)
- [PyPI: pymsteams](https://pypi.org/project/pymsteams/) -- v0.2.5, released 2025-01-07, seeking maintainers, legacy format only (HIGH confidence)
- [PyPI: jinja2](https://pypi.org/project/jinja2/) -- v3.1.6, released 2025-03-05 (HIGH confidence)
- [PyPI: alembic](https://pypi.org/project/alembic/) -- v1.18.4, released 2026-02-10 (HIGH confidence)
- [PyPI: sqlmodel](https://pypi.org/project/sqlmodel/) -- v0.0.37, 0.x maturity, not recommended (HIGH confidence)
- [PyPI: duckdb](https://pypi.org/project/duckdb/) -- v1.4.4, overkill for this use case (HIGH confidence)
- [PyPI: aiosmtplib](https://pypi.org/project/aiosmtplib/) -- v5.1.0, released 2026-01-25 (HIGH confidence)
- Python stdlib `smtplib`, `email`, `sqlite3` -- no version concerns, ships with Python 3.10+ (HIGH confidence)

---
*Stack research for: dq_framework Milestone 2 extensions (alerting, schema evolution, validation history)*
*Researched: 2026-03-08*
