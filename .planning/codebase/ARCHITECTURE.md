# Architecture

**Analysis Date:** 2026-03-08

## Pattern Overview

**Overall:** Library/Framework pattern with YAML-driven configuration and a layered pipeline design (Profile -> Configure -> Validate -> Report). The framework is a reusable Python package (`dq_framework`) that wraps Great Expectations 1.x with YAML-based configuration, designed for dual-platform use (local development and MS Fabric/Spark).

**Key Characteristics:**
- Configuration-driven validation: YAML files define all expectations, no code changes needed per dataset
- Dual-platform abstraction: Same API works locally (pandas) and in MS Fabric (Spark -> pandas conversion)
- Auto-profiling pipeline: DataProfiler analyzes data and generates YAML configs automatically
- Severity-based threshold system: Expectations have severity levels (critical/high/medium/low) with per-severity pass thresholds
- Great Expectations 1.x as the validation engine under the hood

## Layers

**Configuration Layer:**
- Purpose: Define and load validation rules from YAML
- Location: `dq_framework/config_loader.py`, `dq_framework/constants.py`
- Contains: ConfigLoader class (load, validate, merge YAML configs), all framework constants
- Depends on: PyYAML
- Used by: DataQualityValidator, FabricDataQualityRunner, DataProfiler (output)

**Data Loading Layer:**
- Purpose: Load data from various file formats with memory protection
- Location: `dq_framework/loader.py`
- Contains: DataLoader static class with smart sampling, encoding detection, format handling
- Depends on: pandas, pyarrow (optional), FileSystemHandler from utils
- Used by: BatchProfiler, CLI scripts (`scripts/profile_data.py`)

**Profiling Layer:**
- Purpose: Analyze data and auto-generate validation expectations
- Location: `dq_framework/data_profiler.py`, `dq_framework/batch_profiler.py`
- Contains: DataProfiler (single DataFrame analysis, expectation generation), BatchProfiler (parallel multi-file processing)
- Depends on: pandas, numpy, DataLoader, constants
- Used by: CLI scripts, end users for config generation

**Validation Layer (Core):**
- Purpose: Execute Great Expectations checks against DataFrames
- Location: `dq_framework/validator.py`
- Contains: DataQualityValidator class -- builds GX suites, runs checkpoints, formats results with severity-based thresholds
- Depends on: great_expectations, ConfigLoader, constants
- Used by: FabricDataQualityRunner, quick_validate(), end users directly

**Fabric Integration Layer:**
- Purpose: MS Fabric-specific operations (Spark, Delta tables, Lakehouse)
- Location: `dq_framework/fabric_connector.py`
- Contains: FabricDataQualityRunner (Spark->pandas conversion, chunked processing, Delta table validation, Lakehouse file I/O, failure handling, alerting), quick_validate() helper
- Depends on: DataQualityValidator, PySpark (optional), notebookutils/mssparkutils (optional)
- Used by: Fabric notebooks, ETL pipelines

**Platform Abstraction Layer:**
- Purpose: Abstract filesystem operations across local and ABFSS (Azure Blob) paths
- Location: `dq_framework/utils.py`
- Contains: FileSystemHandler (list_files, exists, is_dir for local + ABFSS), Fabric runtime detection, mssparkutils accessor
- Depends on: pathlib, notebookutils (optional, Fabric-only)
- Used by: DataLoader, BatchProfiler, CLI scripts

**Ingestion Layer:**
- Purpose: File copy/move operations between source and target
- Location: `dq_framework/ingestion.py`
- Contains: DataIngester class (local file copy via shutil, Fabric parquet read/write)
- Depends on: pandas, shutil
- Used by: ETL pipelines (data movement step)

## Data Flow

**Profiling Flow (one-time setup):**

1. User points CLI (`scripts/profile_data.py`) or BatchProfiler at data files
2. DataLoader reads files with encoding detection, format handling, auto-sampling for large files (>500MB)
3. DataProfiler analyzes each column: detects semantic type (id, date, categorical, numeric, monetary, etc.), calculates null rates, uniqueness, value ranges
4. DataProfiler generates a YAML config with appropriate Great Expectations expectations, tagged with severity levels
5. YAML config is saved to disk for review and customization

**Validation Flow (recurring):**

1. User creates DataQualityValidator with a YAML config path or dict
2. ConfigLoader loads and validates the YAML structure (must have `validation_name` and `expectations`)
3. On `validate(df)`, validator creates an ephemeral GX context with unique IDs to avoid name collisions
4. Builds an ExpectationSuite from config, creates a Checkpoint, runs it against the DataFrame
5. Results are formatted with per-severity statistics and threshold checking
6. Returns a summary dict with success/failure, rates, failed expectation details

**Fabric Validation Flow:**

1. FabricDataQualityRunner loads config (from ABFSS via mssparkutils.fs.head or local path)
2. For Delta tables: `spark.table()` -> Spark DataFrame
3. For Lakehouse files: `spark.read.format()` -> Spark DataFrame
4. Large datasets are auto-sampled (>100K rows) or chunked
5. Spark DataFrame converted to pandas via `toPandas()`
6. Core DataQualityValidator runs the validation
7. Results optionally saved to Lakehouse as JSON via mssparkutils.fs.put

**State Management:**
- No persistent state between validations; each `validate()` call creates a fresh ephemeral GX context
- Configuration is loaded once at init and reused
- UUID suffixes prevent name collisions across concurrent calls

## Key Abstractions

**DataQualityValidator:**
- Purpose: Core validation engine wrapping Great Expectations 1.x
- Examples: `dq_framework/validator.py`
- Pattern: Config-driven; accepts YAML path or dict at init, runs validation against any pandas DataFrame

**DataProfiler:**
- Purpose: Automatic expectation generation from data analysis
- Examples: `dq_framework/data_profiler.py`
- Pattern: Analyze-then-generate; profiles columns to detect types, then generates appropriate GX expectations with severity metadata

**FabricDataQualityRunner:**
- Purpose: Adapts the core validator for MS Fabric's Spark/Delta/Lakehouse environment
- Examples: `dq_framework/fabric_connector.py`
- Pattern: Adapter pattern; converts Spark DataFrames to pandas, handles ABFSS paths, stores results in Lakehouse

**FileSystemHandler:**
- Purpose: Unified filesystem abstraction for local and cloud (ABFSS) paths
- Examples: `dq_framework/utils.py`
- Pattern: Static utility class with branching logic based on path prefix (`abfss://`)

**ConfigLoader:**
- Purpose: YAML configuration loading, validation, and merging
- Examples: `dq_framework/config_loader.py`
- Pattern: Loader/validator; enforces schema (validation_name + expectations list with expectation_type + kwargs)

## Entry Points

**Python Package Import:**
- Location: `dq_framework/__init__.py`
- Triggers: `from dq_framework import DataQualityValidator, FabricDataQualityRunner`
- Responsibilities: Exposes all public classes and utilities

**CLI Profiler Script:**
- Location: `scripts/profile_data.py`
- Triggers: `python scripts/profile_data.py <path> [options]`
- Responsibilities: Profiles data files or directories, generates YAML configs. Supports parallel processing, sampling, encoding options.

**Standalone Check Script:**
- Location: `check_data.py`
- Triggers: `python check_data.py`
- Responsibilities: Ad-hoc data inspection script (hardcoded path, checks for OWNERID column)

**Quick Validate Helper:**
- Location: `dq_framework/fabric_connector.py` (module-level function `quick_validate()`)
- Triggers: `from dq_framework.fabric_connector import quick_validate`
- Responsibilities: One-liner validation for Spark or pandas DataFrames, auto-detects type

## Error Handling

**Strategy:** Graceful degradation with optional imports and comprehensive logging

**Patterns:**
- Optional dependency guards: `try: import X; AVAILABLE = True except: AVAILABLE = False` used for great_expectations, pandas, pyspark, pyarrow, notebookutils
- Validation errors raise ValueError/FileNotFoundError with descriptive messages from ConfigLoader
- DataLoader tries multiple encodings (utf-8, latin-1, iso-8859-1, cp1252) before failing
- FabricDataQualityRunner falls back from mssparkutils to standard file I/O on failure
- BatchProfiler catches per-file errors and returns error status without stopping batch
- Chunked Spark validation catches per-chunk errors and continues processing remaining chunks
- Alerting uses exponential backoff retry (up to 3 attempts)

## Cross-Cutting Concerns

**Logging:** Python standard `logging` module throughout. Each module creates `logger = logging.getLogger(__name__)`. Info for success, warning for degradation, error for failures.

**Validation:** Two levels: (1) ConfigLoader.validate() enforces YAML schema at load time, (2) GX runtime validation checks data against expectations at validate time.

**Authentication:** Not handled by the framework itself. Fabric authentication is delegated to the runtime environment (mssparkutils handles ABFSS auth). No credentials stored or managed.

**Configuration Constants:** All magic numbers centralized in `dq_framework/constants.py` with documented categories (validation thresholds, profiling params, loading limits, Fabric settings).

---

*Architecture analysis: 2026-03-08*
