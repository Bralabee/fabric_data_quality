# Codebase Concerns

**Analysis Date:** 2026-03-08

## Tech Debt

**Version Mismatch Between `setup.py` and `pyproject.toml`:**
- Issue: `setup.py` declares version `1.2.0` with `great-expectations>=0.18.0,<1.0.0` and `python_requires=">=3.8"`, while `pyproject.toml` declares version `2.0.0` with `great-expectations>=1.0.0,<2.0.0` and `requires-python = ">=3.10"`. The two files have fundamentally incompatible dependency specs.
- Files: `setup.py`, `pyproject.toml`
- Impact: Installing via `pip install -e .` vs `python setup.py install` produces different dependency sets. `setup.py` also includes `jsonschema` and `marshmallow` which are not used anywhere in the codebase.
- Fix approach: Remove `setup.py` entirely. The project uses `pyproject.toml` with setuptools backend, so `setup.py` is redundant. Ensure `pip install -e .` uses `pyproject.toml` only.

**CI Pipeline Tests Against Python 3.8/3.9 But `pyproject.toml` Requires 3.10+:**
- Issue: `.github/workflows/ci.yml` matrix includes `["3.8", "3.9", "3.10", "3.11"]` but `pyproject.toml` declares `requires-python = ">=3.10"`. The CI also uses `flake8`/`black`/`isort` while `pyproject.toml` configures `ruff` as the linter/formatter.
- Files: `.github/workflows/ci.yml`, `pyproject.toml`
- Impact: CI will either fail on 3.8/3.9 or give false confidence. Linting results differ between CI (flake8+black+isort) and local dev (ruff).
- Fix approach: Update CI matrix to `["3.10", "3.11", "3.12", "3.13"]` to match pyproject.toml classifiers. Replace flake8/black/isort in CI with `ruff check` and `ruff format --check`.

**`requirements-dev.txt` Conflicts With `pyproject.toml` Dev Dependencies:**
- Issue: `requirements-dev.txt` pins `pytest>=7.4.0,<8.0.0` and `black`/`flake8`/`isort`, while `pyproject.toml` declares `pytest>=9.0.0` and `ruff>=0.15.0`. Two conflicting sources of truth for dev tooling.
- Files: `requirements-dev.txt`, `pyproject.toml`
- Impact: Developers get different tool versions depending on which file they install from. The extensive list in `requirements-dev.txt` (sphinx, jupyter, memory-profiler, etc.) is mostly unused.
- Fix approach: Simplify `requirements-dev.txt` to just `-r requirements.txt` plus `pip install -e ".[dev]"`, or remove it entirely and use `pip install -e ".[dev]"`.

**Build Artifacts Committed to Repository:**
- Issue: `build/` and `dist/` directories contain stale v1.2.0 build artifacts. The `htmlcov/` directory, `.coverage` file, and `pipeline.log` (1121 lines) are also present. Additionally `fabric_data_quality.egg-info/` is tracked.
- Files: `build/`, `dist/fabric_data_quality-1.2.0-py3-none-any.whl`, `dist/fabric_data_quality-1.2.0.tar.gz`, `htmlcov/`, `.coverage`, `pipeline.log`, `fabric_data_quality.egg-info/`
- Impact: Repository bloat. Stale build artifacts (v1.2.0) could confuse users who try to install from them when the current version is 2.0.0.
- Fix approach: Add `build/`, `dist/`, `*.egg-info/`, `pipeline.log` to `.gitignore` (some are listed but not enforced since files were committed before). Remove these with `git rm -r --cached`.

**Stale `check_data.py` Script With Hardcoded Paths:**
- Issue: Root-level `check_data.py` contains a hardcoded absolute path `/home/sanmi/Documents/HS2/HS2_PROJECTS_2025/AIMS_LOCAL/data/...` and uses `sys.path.append` for imports.
- Files: `check_data.py`
- Impact: Script is developer-specific and non-portable. Not useful for other contributors.
- Fix approach: Either delete it (it's a one-off exploration script) or move to `scripts/` and make it configurable via CLI arguments.

**`DataIngester.engine` Property Is Unused:**
- Issue: `DataIngester.__init__` accepts an `engine` parameter and stores it as `self.engine`, but no method references `self.engine` for any operation. The docstring says "Reserved for future use."
- Files: `dq_framework/ingestion.py`
- Impact: Dead code that misleads users into thinking engine selection affects behavior.
- Fix approach: Either implement engine-based parquet read/write selection or remove the parameter entirely.

## Known Bugs

**Chunked Spark Validation Uses `monotonically_increasing_id` Incorrectly:**
- Symptoms: Chunked processing may miss rows or produce inconsistent chunks because `monotonically_increasing_id()` does not guarantee sequential values -- it guarantees only uniqueness and monotonic increase within partitions.
- Files: `dq_framework/fabric_connector.py` (lines 243-253)
- Trigger: Call `validate_spark_dataframe()` with `chunk_size` on a multi-partition Spark DataFrame.
- Workaround: Avoid using `chunk_size` parameter; rely on `sample_large_data=True` instead.

**Aggregated Chunk Results Use Wrong Success Criteria:**
- Symptoms: `_aggregate_chunk_results` sets `"success": total_failed == 0`, but this counts failed expectations across all chunks as a sum. Since each chunk runs the same expectations independently, the `total_evaluated` and `total_failed` counts are inflated by `num_chunks`, giving misleading statistics.
- Files: `dq_framework/fabric_connector.py` (lines 287-325)
- Trigger: Use chunked processing on any dataset.
- Workaround: None. Avoid chunked processing until fixed.

## Security Considerations

**YAML Config Loading Is Safe (Uses `safe_load`):**
- Risk: Low. All YAML loading uses `yaml.safe_load()` throughout `dq_framework/config_loader.py` and `dq_framework/fabric_connector.py`.
- Files: `dq_framework/config_loader.py`, `dq_framework/fabric_connector.py`
- Current mitigation: `yaml.safe_load` is used consistently.
- Recommendations: None needed. This is well handled.

**No Input Validation on ABFSS Paths:**
- Risk: ABFSS paths are passed directly to `mssparkutils.fs.ls()` and `mssparkutils.fs.head()` without sanitization. A malicious config path could potentially access unintended storage locations.
- Files: `dq_framework/utils.py`, `dq_framework/fabric_connector.py`
- Current mitigation: Fabric's own auth layer provides protection.
- Recommendations: Add path allowlisting or prefix validation for ABFSS paths in enterprise deployments.

**`.env` Files Properly Gitignored:**
- Risk: Low. `.gitignore` correctly excludes `.env`, `.env.local`, `*.pem`, `*.key`, `secrets.yml`, `credentials.yml`.
- Files: `.gitignore`
- Current mitigation: Comprehensive gitignore rules.
- Recommendations: None needed.

## Performance Bottlenecks

**Spark-to-Pandas Conversion for Validation:**
- Problem: All Spark DataFrame validation requires `toPandas()` conversion, loading data into driver memory.
- Files: `dq_framework/fabric_connector.py` (line 193)
- Cause: Great Expectations 1.x validation runs on pandas DataFrames. There is no native Spark integration in the current GX version being used.
- Improvement path: The framework already has sampling (`FABRIC_LARGE_DATASET_THRESHOLD = 100000` rows) and memory warnings. Long-term, investigate GX Spark datasource support or native Spark validation.

**Parquet Loading Falls Back to Full File Read:**
- Problem: When `PYARROW_AVAILABLE` is False or reading from ABFSS, `pd.read_parquet()` reads the entire file into memory, then truncates with `.head(sample_size)`.
- Files: `dq_framework/loader.py` (lines 112-119)
- Cause: No row-level filtering available for parquet without PyArrow.
- Improvement path: Ensure PyArrow is always available (it's already a required dependency in `pyproject.toml`). For ABFSS, consider using Spark for initial data loading.

**Fresh GX Context Created Per Validation Call:**
- Problem: Each `validator.validate()` call creates a new ephemeral Great Expectations context with UUID-named objects. This adds overhead for repeated validations.
- Files: `dq_framework/validator.py` (lines 126-153)
- Cause: Prevents name collisions across validation calls (correct approach).
- Improvement path: Accept this tradeoff -- the UUID approach prevents name collision bugs and the GX context creation overhead is modest compared to the actual validation.

## Fragile Areas

**Type Detection in `DataProfiler._detect_column_type`:**
- Files: `dq_framework/data_profiler.py` (lines 179-218)
- Why fragile: Column type detection relies on column name heuristics (`'id' in series.name.lower()`, `'code' in series.name.lower()`, etc.) and uniqueness ratios. Non-English column names, unconventional naming, or edge-case data distributions can cause misclassification.
- Safe modification: Use `type_overrides` parameter to force correct types. When modifying detection logic, ensure all tests in `tests/test_data_profiler.py` pass, especially the date detection tests.
- Test coverage: Good -- `test_data_profiler.py` is comprehensive (600+ lines).

**Threshold Logic in `DataQualityValidator._format_results`:**
- Files: `dq_framework/validator.py` (lines 201-337)
- Why fragile: The success determination involves three interacting threshold systems: (1) global `threshold` parameter, (2) per-severity `quality_thresholds` from config, and (3) GX's own `validation_result.success`. The logic at lines 250-270 has a subtle path where `threshold >= DEFAULT_VALIDATION_THRESHOLD` delegates to GX's result while `threshold < DEFAULT_VALIDATION_THRESHOLD` uses its own calculation.
- Safe modification: Write test cases that exercise all three threshold paths. The existing tests in `tests/test_validator.py` cover basic pass/fail but do not test severity-based thresholds.
- Test coverage: Moderate -- severity threshold logic is untested.

**`__init__.py` Exposes Private Function:**
- Files: `dq_framework/__init__.py` (line 38)
- Why fragile: `_is_fabric_runtime` (leading underscore = private convention) is exported in `__all__`. Changing its behavior or removing it would be a breaking API change.
- Safe modification: Mark clearly in documentation or rename to `is_fabric_runtime` (without underscore) in a future major version.
- Test coverage: Tested indirectly via `test_utils.py`.

## Scaling Limits

**Batch Profiler Uses `ProcessPoolExecutor`:**
- Current capacity: Processes files in parallel with configurable `workers` parameter (default 1).
- Limit: For very large numbers of files, the process pool creates full Python processes per worker. Memory-intensive profiling tasks can exhaust system memory with high worker counts.
- Scaling path: Default worker count of 1 is conservative. For large batches, consider adding a memory-aware worker limit or switching to `ThreadPoolExecutor` since the actual computation is pandas-bound (GIL-released for many operations).
- Files: `dq_framework/batch_profiler.py` (line 115)

**Single-Node Pandas Limitation:**
- Current capacity: Validates DataFrames that fit in driver memory.
- Limit: Datasets larger than available RAM cannot be validated without sampling.
- Scaling path: Current sampling approach (100K rows or 10% of dataset) is the intended mitigation. For true large-scale validation, a native Spark/Polars validation engine would be needed.
- Files: `dq_framework/validator.py`, `dq_framework/fabric_connector.py`

## Dependencies at Risk

**Great Expectations 1.x Is Rapidly Evolving:**
- Risk: GX 1.x has undergone significant API changes from 0.x. The framework uses GX 1.x APIs (`get_context()`, `add_pandas()`, `add_dataframe_asset()`, `ValidationDefinition`, `Checkpoint`) that may change in GX 2.x.
- Impact: Upgrading to GX 2.x could require significant refactoring of `dq_framework/validator.py`.
- Migration plan: Pin to `great-expectations>=1.0.0,<2.0.0` (already done). Monitor GX 2.x release notes for API stability.
- Files: `dq_framework/validator.py`

**`sqlalchemy>=2.0.0` Is Listed But Not Used:**
- Risk: Unnecessary dependency adds install time and potential vulnerability surface.
- Impact: Low -- it's pulled in as a transitive dependency by GX anyway.
- Migration plan: Remove from explicit requirements if confirmed as GX transitive dependency.
- Files: `pyproject.toml`, `requirements.txt`

## Missing Critical Features

**Alert System Is Unimplemented:**
- Problem: `FabricDataQualityRunner.handle_failure(action="alert")` calls `_send_alert()` which contains a TODO placeholder: "Implement actual alert logic (Teams, email, webhook, etc.)". The method logs a message but sends no actual notification.
- Blocks: Pipeline failure alerting in production. Teams relying on automated notifications will not receive them.
- Files: `dq_framework/fabric_connector.py` (lines 457-508)

**No Schema Evolution Tracking:**
- Problem: The profiler generates expectations based on current data shape, but there is no mechanism to detect or handle schema evolution over time (e.g., new columns added, columns removed, type changes).
- Blocks: Long-running production pipelines that need to adapt to schema changes without regenerating all configs.
- Files: `dq_framework/data_profiler.py`, `dq_framework/config_loader.py`

**No Validation Result History or Trend Analysis:**
- Problem: Validation results are returned as dictionaries and (in Fabric) saved as individual JSON files. There is no aggregation, trend tracking, or dashboard integration.
- Blocks: Data quality monitoring over time. Teams cannot answer "is data quality improving or degrading?"
- Files: `dq_framework/fabric_connector.py` (lines 510-536)

## Test Coverage Gaps

**`fabric_connector.py` Has 18.5% Coverage (154/189 statements missing):**
- What's not tested: Spark DataFrame validation, Delta table validation, Lakehouse file validation, chunked processing, result saving to Lakehouse, alert sending, and the `quick_validate` helper function.
- Files: `dq_framework/fabric_connector.py`, `tests/test_fabric_connector.py`
- Risk: The most complex module with the most production-critical Fabric integration code has the lowest coverage. Regressions in Spark conversion, chunked processing, or result serialization would go undetected.
- Priority: High -- but requires mocking Spark/Fabric dependencies since tests cannot run in a real Fabric environment locally.

**`loader.py` Has 51.2% Coverage (41/84 statements missing):**
- What's not tested: Parquet loading with PyArrow optimization, Excel file loading, JSON file loading, ABFSS path handling, encoding fallback logic beyond the first encoding.
- Files: `dq_framework/loader.py`, `tests/test_loader.py`
- Risk: Data loading is the entry point for all validation workflows. Untested file format handlers could silently fail.
- Priority: Medium -- add tests for each file format path and the PyArrow optimization path.

**`batch_profiler.py` Has 51.9% Coverage (26/54 statements missing):**
- What's not tested: `run_parallel_profiling` method, including file discovery, parallel execution, and result aggregation.
- Files: `dq_framework/batch_profiler.py`, `tests/test_batch_profiler.py`
- Risk: Parallel processing bugs (race conditions, error handling in workers) would go undetected.
- Priority: Medium -- the test only covers `process_single_file` with full mocking.

**Severity-Based Threshold Logic Is Untested:**
- What's not tested: The per-severity threshold evaluation in `_format_results()` -- specifically the interaction between global threshold, quality_thresholds config, and per-severity pass rates.
- Files: `dq_framework/validator.py` (lines 246-283)
- Risk: Complex threshold logic could produce incorrect pass/fail decisions for production validation suites that use severity levels.
- Priority: High -- this directly affects whether pipelines halt or continue on quality failures.

---

*Concerns audit: 2026-03-08*
