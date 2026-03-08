# External Integrations

**Analysis Date:** 2026-03-08

## APIs & External Services

**Great Expectations (GX 1.x):**
- Core validation engine used throughout the framework
- SDK/Client: `great-expectations` >=1.0.0, <2.0.0
- Auth: None (local library)
- Usage: `dq_framework/validator.py` creates ephemeral GX contexts, expectation suites, validation definitions, and checkpoints per validation run
- GX telemetry disabled via `os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")` in `dq_framework/validator.py`

**Microsoft Fabric / Azure:**
- MS Fabric Lakehouse integration for data access and results storage
- SDK/Client: `notebookutils.mssparkutils` (Fabric-native, not pip-installable)
- Auth: Inherited from Fabric workspace session (no explicit credential handling)
- Detection: `_is_fabric_runtime()` in `dq_framework/utils.py` checks for `/lakehouse/default/Files` path
- File operations: `mssparkutils.fs.head()`, `mssparkutils.fs.put()`, `mssparkutils.fs.ls()`, `mssparkutils.fs.isDirectory()`
- Used in: `dq_framework/fabric_connector.py`, `dq_framework/utils.py`
- Supports ABFSS paths (`abfss://`), Lakehouse paths (`Files/`), and HTTPS paths

**PySpark:**
- Spark DataFrame validation via pandas conversion
- SDK/Client: `pyspark` >=3.3.0, <4.0.0 (optional dependency)
- Auth: SparkSession from Fabric environment (`SparkSession.builder.getOrCreate()`)
- Used in: `dq_framework/fabric_connector.py` for `validate_spark_dataframe()`, `validate_delta_table()`, `validate_lakehouse_file()`

## Data Storage

**Databases:**
- No direct database connections in framework code
- `sqlalchemy` >=2.0.0 listed as dependency (required by Great Expectations internally)
- Delta Lake tables accessed via PySpark in Fabric environment (`spark.table()`, `spark.read.format("delta")`)

**File Storage:**
- Local filesystem (primary for non-Fabric usage)
- Azure Data Lake Storage Gen2 via ABFSS paths (in Fabric environment)
- MS Fabric Lakehouse (`Files/` paths)
- Supported file formats: CSV, Parquet, JSON, Excel (.xlsx/.xls), Delta
- File handling: `dq_framework/loader.py` (DataLoader), `dq_framework/utils.py` (FileSystemHandler)

**Caching:**
- None - Each validation creates a fresh ephemeral GX context (`gx.get_context()`)

## Authentication & Identity

**Auth Provider:**
- No explicit auth handling in framework code
- Fabric workspace authentication inherited from runtime session
- `workspace_id` parameter accepted by `FabricDataQualityRunner` but used for logging only (`dq_framework/fabric_connector.py` line 63)

## Monitoring & Observability

**Error Tracking:**
- None (no Sentry, Application Insights, or similar)

**Logs:**
- Python `logging` module used throughout all modules
- Logger per module: `logger = logging.getLogger(__name__)`
- Log levels used: INFO (validation progress), WARNING (fallbacks, memory), ERROR (failures)
- No structured logging or log aggregation configured

**Alerting:**
- Placeholder alert system in `dq_framework/fabric_connector.py` (`_send_alert()` method)
- TODO comment: "Implement actual alert logic (Teams, email, webhook, etc.)"
- Retry logic implemented (exponential backoff) but actual notification delivery not wired up
- Config templates reference `notification.email` and `notification.slack_webhook` but no implementation exists

**Validation Results Storage:**
- JSON files saved to Fabric Lakehouse at `{results_location}/validation_{batch_name}_{timestamp}.json`
- Default results location: `Files/dq_results`
- Only saves when `FABRIC_UTILS_AVAILABLE` is True (Fabric environment)
- Uses `mssparkutils.fs.put()` for writing

## CI/CD & Deployment

**Hosting:**
- Microsoft Fabric (notebooks and pipelines) for production
- Local development supported with graceful degradation

**CI Pipeline:**
- GitHub Actions (`.github/workflows/ci.yml`)
- Jobs: lint -> test (matrix: Python 3.8-3.11) -> build, security (parallel with test)
- Lint: flake8, black, isort
- Test: pytest with coverage (60% minimum, Codecov upload for Python 3.10)
- Build: `python -m build` + `twine check`
- Security: bandit scan + safety dependency check
- Triggers: push/PR to master, main, develop branches

**Pre-commit Hooks:**
- `.pre-commit-config.yaml` with: black, isort, flake8 (+ docstrings, bugbear), mypy, bandit, pydocstyle (google convention), pre-commit-hooks (yaml/json/toml check, large file check, merge conflict check, private key detection, whitespace fixes), safety dependency check

## Environment Configuration

**Required env vars:**
- None strictly required for local operation
- `GX_ANALYTICS_ENABLED` - Auto-set to `False` in code

**Optional env vars:**
- No `.env` file detected; `python-dotenv` in dev deps but not used in framework code

**Secrets location:**
- No secrets management in framework code
- Fabric workspace handles authentication natively

## Webhooks & Callbacks

**Incoming:**
- None

**Outgoing:**
- Placeholder only in `dq_framework/fabric_connector.py` (`_send_alert()`)
- Config templates reference `slack_webhook` and `email` notification but no implementation exists
- Failure handling supports three modes: `log` (default), `halt` (raise exception), `alert` (placeholder)

## Integration Architecture Notes

**Dual-mode Design:**
The framework operates in two modes detected at import time:

1. **Fabric Mode** (`FABRIC_AVAILABLE = True`):
   - `dq_framework/utils.py` detects `/lakehouse/default/Files` path
   - Imports `notebookutils.mssparkutils`
   - Enables ABFSS file access, Lakehouse result storage, Spark DataFrame validation
   - Config loaded via `mssparkutils.fs.head()` for Fabric/ABFSS paths

2. **Local Mode** (`FABRIC_AVAILABLE = False`):
   - Standard filesystem operations via `pathlib.Path`
   - Pandas-only DataFrame validation
   - No result persistence (results returned in-memory only)

**Data Flow:**
- Input: File path or DataFrame -> DataLoader/FabricDataQualityRunner
- Config: YAML file -> ConfigLoader -> dict
- Validation: DataFrame + Config -> GX ephemeral context -> checkpoint -> results dict
- Output: Results dict (JSON to Lakehouse in Fabric mode)

---

*Integration audit: 2026-03-08*
