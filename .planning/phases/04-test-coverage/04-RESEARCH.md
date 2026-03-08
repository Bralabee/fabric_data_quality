# Phase 4: Test Coverage - Research

**Researched:** 2026-03-08
**Domain:** Python unit testing, mocking PySpark/Fabric, pytest-cov, characterization testing
**Confidence:** HIGH

## Summary

Phase 4 raises test coverage for three under-tested modules (fabric_connector.py at 18%, loader.py at 51%, batch_profiler.py at 51%) to 60%+ each, adds characterization tests documenting the current severity-based threshold logic, and establishes reusable Spark/Fabric mock fixtures for later phases.

The primary challenge is that fabric_connector.py depends heavily on PySpark and Microsoft Fabric's `mssparkutils`, neither of which is available in the local test environment. The solution is systematic mocking: mock PySpark's SparkSession, DataFrame, and Window/functions at the module boundary, and mock `FABRIC_UTILS_AVAILABLE`/`FABRIC_AVAILABLE` flags to control code paths. The existing test_fabric_connector.py already demonstrates this pattern for chunked validation (Phase 3 bug fixes) -- Phase 4 extends it to the remaining untested methods.

For characterization tests (TEST-04), the validator's `_format_results` method contains the core threshold logic: a global threshold check, per-severity threshold checks, and fallback behavior when no thresholds are defined. These must be captured as snapshot-style tests that document "current behavior is X" before any future modifications.

**Primary recommendation:** Use pytest exclusively (migrate existing unittest.TestCase tests), build a shared `conftest.py` fixtures module for Spark/Fabric mocks, and write characterization tests as parameterized pytest tests covering all threshold logic branches.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| TEST-01 | Raise fabric_connector.py test coverage from 18% to 60%+ with Spark/Fabric mock fixtures | Mock patterns for SparkSession, SparkDataFrame, mssparkutils, FABRIC_UTILS_AVAILABLE flag; coverage gap analysis shows 13 untested methods/paths |
| TEST-02 | Raise loader.py test coverage from 51% to 60%+ covering all file formats and PyArrow path | Mock patterns for PyArrow ParquetFile, pd.read_excel, pd.read_json, FileSystemHandler; 5 untested code paths identified |
| TEST-03 | Raise batch_profiler.py test coverage from 51% to 60%+ covering parallel processing | Mock patterns for ProcessPoolExecutor, FileSystemHandler.list_files; 3 untested code paths identified |
| TEST-04 | Add characterization tests for severity-based threshold logic (document current behavior before modifications) | Validator._format_results threshold logic analyzed: 6 distinct branches requiring characterization |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| pytest | >=9.0.0 | Test runner and assertions | Already in pyproject.toml [dev]; superior to unittest for parameterized tests and fixtures |
| pytest-cov | >=7.0.0 | Coverage measurement and reporting | Already in pyproject.toml [dev]; configured with --cov=dq_framework |
| pytest-mock | >=3.15.0 | Mocker fixture for cleaner mock setup | Already in pyproject.toml [dev]; cleaner than unittest.mock.patch decorators |
| unittest.mock | stdlib | MagicMock, patch, PropertyMock | Standard library; already used extensively in existing tests |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pyyaml | >=6.0 | Creating test config files | Already a project dependency; used for YAML config fixtures |
| pandas | >=2.0.0 | Creating test DataFrames | Already a project dependency; test data creation |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| unittest.TestCase | pytest classes | pytest classes are simpler; existing unittest tests in test_loader.py and test_batch_profiler.py should be migrated to pytest style for consistency |
| pyspark (real) | MagicMock SparkDataFrame | Real PySpark requires Java + 500MB+ install; mocking is standard for unit tests |

**Installation:**
```bash
pip install -e ".[dev]"
```

No new dependencies needed -- everything is already in pyproject.toml [dev] extras.

## Architecture Patterns

### Recommended Test Structure
```
tests/
  conftest.py              # Shared fixtures (Spark mocks, Fabric mocks, config helpers)
  test_fabric_connector.py # TEST-01: FabricDataQualityRunner coverage
  test_loader.py           # TEST-02: DataLoader coverage
  test_batch_profiler.py   # TEST-03: BatchProfiler coverage
  test_threshold_characterization.py  # TEST-04: Severity threshold behavior docs
```

### Pattern 1: Spark/Fabric Mock Fixtures (Reusable)
**What:** Centralized pytest fixtures that mock PySpark classes and Fabric environment flags
**When to use:** Any test that touches fabric_connector.py or code depending on SPARK_AVAILABLE/FABRIC_UTILS_AVAILABLE
**Example:**
```python
# tests/conftest.py
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd


@pytest.fixture
def mock_spark_session():
    """Mock SparkSession that returns mock DataFrames."""
    session = MagicMock()
    session.builder.getOrCreate.return_value = session
    return session


@pytest.fixture
def mock_spark_df():
    """Mock Spark DataFrame with count/columns/toPandas support."""
    df = MagicMock()
    df.count.return_value = 100
    df.columns = ["id", "name", "age"]
    df.toPandas.return_value = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
        "age": [10, 20, 30],
    })
    df.limit.return_value = df  # For sampling path
    return df


@pytest.fixture
def mock_mssparkutils():
    """Mock Fabric mssparkutils for Lakehouse operations."""
    utils = MagicMock()
    utils.fs.head.return_value = "validation_name: test\nexpectations: []"
    utils.fs.put.return_value = None
    utils.fs.ls.return_value = []
    return utils


@pytest.fixture
def fabric_runner(tmp_path):
    """Create a FabricDataQualityRunner with a minimal YAML config."""
    import yaml
    from dq_framework.fabric_connector import FabricDataQualityRunner

    config = {
        "validation_name": "test_validation",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1},
            }
        ],
    }
    config_file = tmp_path / "test_config.yml"
    config_file.write_text(yaml.dump(config))
    return FabricDataQualityRunner(str(config_file))
```

### Pattern 2: Characterization Tests (Snapshot-Style)
**What:** Tests that document existing behavior without asserting "correctness" -- they assert "this is what the code does now"
**When to use:** TEST-04 -- documenting threshold logic before future modifications
**Example:**
```python
class TestThresholdCharacterization:
    """Document current severity-based threshold behavior.

    These tests capture the CURRENT behavior of the threshold logic.
    If any test fails after a code change, it means behavior changed --
    review whether the change was intentional.
    """

    def test_no_thresholds_no_global_threshold_enforces_100_percent(self, ...):
        """When no quality_thresholds and no threshold arg, 100% success required."""
        # Document: fallback behavior is strict 100%
        ...

    def test_global_threshold_at_100_uses_gx_success_flag(self, ...):
        """When threshold >= DEFAULT_VALIDATION_THRESHOLD (100), uses GX success flag."""
        # Document: >=100 threshold delegates to validation_result.success
        ...

    def test_global_threshold_below_100_compares_success_rate(self, ...):
        """When threshold < 100, compares success_rate against threshold."""
        ...

    def test_severity_thresholds_checked_independently(self, ...):
        """Each severity level is checked against its own threshold."""
        ...
```

### Pattern 3: Patching Module-Level Flags
**What:** Use `@patch` on module-level boolean flags to control code paths
**When to use:** Testing SPARK_AVAILABLE, FABRIC_UTILS_AVAILABLE, PYARROW_AVAILABLE branches
**Example:**
```python
@patch("dq_framework.fabric_connector.SPARK_AVAILABLE", True)
@patch("dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE", False)
def test_validate_spark_without_fabric(self, fabric_runner, mock_spark_df):
    """Validate flow when Spark is available but Fabric is not."""
    ...
```

### Anti-Patterns to Avoid
- **Testing with real PySpark:** Do NOT install PySpark for unit tests. Mock it. Integration tests with real Spark belong in a separate CI stage.
- **Mixing unittest.TestCase and pytest fixtures:** Fixtures do not work with unittest.TestCase. Migrate existing unittest tests to pytest classes.
- **Over-mocking validator internals:** For characterization tests, use the real DataQualityValidator with real GX -- mock only external dependencies (file system, Spark). The validator's threshold logic must execute for real.
- **Fragile datetime assertions:** Use `pytest.approx` for success_rate and avoid asserting exact timestamp strings.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Coverage measurement | Custom line counting | pytest-cov (already configured) | Handles branch coverage, exclusions, reporting |
| Spark DataFrame mocking | Custom fake Spark classes | unittest.mock.MagicMock | Spark API surface is huge; MagicMock auto-specs chains |
| Parameterized test cases | Separate test methods per input | @pytest.mark.parametrize | Cleaner, shows each case in output |
| Temporary config files | Manual file creation/cleanup | pytest tmp_path fixture | Auto-cleanup, unique per test |
| Test isolation | Manual module patching | pytest-mock mocker fixture | Auto-cleanup on test teardown |

**Key insight:** The existing test infrastructure (pytest + pytest-cov + pyproject.toml config) is already well-configured. No new tools are needed -- the work is writing tests, not setting up infrastructure.

## Common Pitfalls

### Pitfall 1: MagicMock Chains Not Propagating
**What goes wrong:** `mock_spark_df.withColumn.return_value.filter.return_value.drop.return_value.toPandas.return_value` -- missing any link breaks the chain silently (returns another MagicMock instead of expected value).
**Why it happens:** PySpark uses method chaining extensively (builder pattern).
**How to avoid:** Set up the full chain explicitly. The existing `_make_spark_df_mock` in test_fabric_connector.py is a good reference. Extract into conftest.py fixtures.
**Warning signs:** Tests pass but coverage doesn't increase -- the mock returned a MagicMock where real code expected data, so branches weren't taken.

### Pitfall 2: Patching at Wrong Module Path
**What goes wrong:** `@patch("pyspark.sql.SparkSession")` doesn't affect `fabric_connector.py` because it imports SparkSession at module load time.
**Why it happens:** Python's import system copies references.
**How to avoid:** Patch where the name is USED, not where it's DEFINED: `@patch("dq_framework.fabric_connector.SparkSession")`.
**Warning signs:** Patch has no effect; real code still executes.

### Pitfall 3: Module-Level Code in utils.py Runs at Import Time
**What goes wrong:** `_is_fabric_runtime()` and the `FABRIC_AVAILABLE` assignment run when `utils.py` is imported. Patching after import is too late.
**Why it happens:** Module-level code executes once at first import.
**How to avoid:** For fabric_connector tests, patch `dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE` and `dq_framework.fabric_connector.SPARK_AVAILABLE` (the module-level copies), not the originals in utils.py.
**Warning signs:** Tests unexpectedly follow the "not available" code path regardless of patches.

### Pitfall 4: Coverage Not Increasing Despite More Tests
**What goes wrong:** Tests run but coverage report shows same percentage.
**Why it happens:** Common causes: (a) tests import from wrong module, (b) mocks prevent real code from executing, (c) coverage source config excludes the file.
**How to avoid:** Run `pytest --cov=dq_framework --cov-report=term-missing` after each batch of new tests to verify specific lines are now covered. Check the "Missing" column.
**Warning signs:** New tests pass instantly without actually testing anything meaningful.

### Pitfall 5: Characterization Tests Asserting Wrong Thing
**What goes wrong:** Tests assert what you THINK the code does rather than what it ACTUALLY does.
**Why it happens:** Reading code and writing expectations without running the code path first.
**How to avoid:** For characterization tests, FIRST run the code path with debug output to capture actual behavior, THEN encode that behavior as assertions. Use the real validator with a real DataFrame and GX.
**Warning signs:** Tests fail on first run -- means you documented expected behavior, not actual behavior.

### Pitfall 6: --cov-fail-under=60 Blocks Test Runs
**What goes wrong:** pytest exits with error because OVERALL coverage is below 60%, even though individual module coverage is improving.
**Why it happens:** pyproject.toml has `--cov-fail-under=60` in addopts, which applies to aggregate coverage.
**How to avoid:** During development, run with `pytest --no-cov` or `pytest --override-ini="addopts="` to bypass the threshold. Verify individual module coverage with `pytest --cov=dq_framework --cov-report=term-missing` without --cov-fail-under.
**Warning signs:** "FAIL Required test coverage of 60% not reached" even when individual module targets are met.

## Code Examples

### Example 1: Testing fabric_connector __init__ with Fabric Config Path
```python
@patch("dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE", True)
@patch("dq_framework.fabric_connector.mssparkutils")
def test_init_loads_config_from_fabric_path(self, mock_msutils, tmp_path):
    """Test that __init__ uses mssparkutils.fs.head for Fabric paths."""
    import yaml

    config = {"validation_name": "test", "expectations": [
        {"expectation_type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1}}
    ]}
    mock_msutils.fs.head.return_value = yaml.dump(config)

    runner = FabricDataQualityRunner(config_path="Files/dq_configs/test.yml")
    assert runner.validator is not None
    mock_msutils.fs.head.assert_called_once()
```

### Example 2: Testing validate_spark_dataframe Main Flow
```python
@patch("dq_framework.fabric_connector.SPARK_AVAILABLE", True)
@patch("dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE", False)
def test_validate_spark_small_dataset(self, fabric_runner, mock_spark_df):
    """Test validation of a small Spark DataFrame (no sampling, no chunking)."""
    mock_spark_df.count.return_value = 50  # Below FABRIC_LARGE_DATASET_THRESHOLD

    with patch.object(fabric_runner.validator, "validate") as mock_validate:
        mock_validate.return_value = {
            "success": True, "success_rate": 100.0,
            "evaluated_checks": 1, "successful_checks": 1,
            "failed_checks": 0, "suite_name": "test",
            "batch_name": "test_batch",
            "timestamp": "2026-03-08T00:00:00",
            "failed_expectations": [],
        }
        result = fabric_runner.validate_spark_dataframe(mock_spark_df, batch_name="test_batch")

    assert result["success"] is True
    mock_spark_df.toPandas.assert_called_once()
```

### Example 3: Testing handle_failure Action Modes
```python
def test_handle_failure_halt_raises(self, fabric_runner):
    """handle_failure with action='halt' raises ValueError."""
    results = {
        "success": False, "suite_name": "test", "batch_name": "test",
        "failed_checks": 2, "evaluated_checks": 5, "success_rate": 60.0,
        "failed_expectations": [
            {"expectation": "exp1", "column": "col1"},
        ],
    }
    with pytest.raises(ValueError, match="Data quality validation failed"):
        fabric_runner.handle_failure(results, action="halt")

def test_handle_failure_success_noop(self, fabric_runner):
    """handle_failure does nothing when results show success."""
    results = {"success": True, "suite_name": "test", "batch_name": "test"}
    fabric_runner.handle_failure(results)  # Should not raise
```

### Example 4: Testing DataLoader PyArrow Path
```python
@patch("dq_framework.utils.FileSystemHandler.exists", return_value=True)
@patch("dq_framework.utils.FileSystemHandler.is_abfss", return_value=False)
@patch("dq_framework.loader.PYARROW_AVAILABLE", True)
@patch("dq_framework.loader.pq")
def test_load_parquet_with_pyarrow_sampling(self, mock_pq, mock_is_abfss, mock_exists):
    """PyArrow batch reading used when sample_size specified and pyarrow available."""
    import pyarrow as pa

    mock_batch = MagicMock()
    mock_batch.num_rows = 100
    mock_parquet_file = MagicMock()
    mock_parquet_file.iter_batches.return_value = [mock_batch]
    mock_pq.ParquetFile.return_value = mock_parquet_file

    mock_table = MagicMock()
    mock_table.to_pandas.return_value = pd.DataFrame({"a": range(100)})

    with patch("dq_framework.loader.pa") as mock_pa:
        mock_pa.Table.from_batches.return_value = mock_table
        df = DataLoader.load_data("test.parquet", sample_size=50)

    assert len(df) <= 50
```

### Example 5: Characterization Test for Severity Thresholds
```python
class TestSeverityThresholdCharacterization:
    """Capture current threshold behavior as characterization tests."""

    @pytest.fixture
    def validator_with_severity_thresholds(self):
        """Validator with per-severity quality thresholds configured."""
        config = {
            "validation_name": "severity_test",
            "quality_thresholds": {
                "critical": 100.0,
                "high": 95.0,
                "medium": 80.0,
                "low": 50.0,
            },
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"},
                    "meta": {"severity": "critical"},
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "optional_field"},
                    "meta": {"severity": "low"},
                },
            ],
        }
        return DataQualityValidator(config_dict=config)

    def test_critical_failure_causes_overall_failure(self, validator_with_severity_thresholds):
        """CHARACTERIZATION: A failing critical expectation causes overall failure
        even if low-severity expectations pass."""
        df = pd.DataFrame({
            "id": [1, None, 3],  # Will fail critical null check
            "optional_field": ["a", "b", "c"],  # Will pass low null check
        })
        result = validator_with_severity_thresholds.validate(df)
        # Document: critical failure -> overall failure
        assert result["success"] is False
        assert any("critical" in f for f in result.get("threshold_failures", []))
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| unittest.TestCase | pytest native classes/functions | Ongoing | Fixtures, parametrize, better assertions |
| Manual coverage tracking | pytest-cov with --cov-fail-under | Already configured | Automated threshold enforcement |
| No Spark mocking patterns | MagicMock with method chaining | Phase 3 (existing) | test_fabric_connector.py already demonstrates |

**Deprecated/outdated:**
- `sys.path.insert(0, ...)` in test files: Not needed when package is installed in dev mode (`pip install -e .`). Present in test_loader.py and test_batch_profiler.py -- should be removed during migration.

## Open Questions

1. **Should existing unittest.TestCase tests be migrated to pytest style?**
   - What we know: test_loader.py and test_batch_profiler.py use unittest.TestCase, which prevents using pytest fixtures. test_fabric_connector.py already uses pytest style.
   - What's unclear: Whether the migration is in scope for this phase or should be done as part of writing new tests.
   - Recommendation: Migrate as part of this phase. The files need significant rework anyway to add coverage, and pytest-style tests are more consistent with the rest of the codebase.

2. **Should --cov-fail-under=60 be per-module or aggregate?**
   - What we know: pyproject.toml sets `--cov-fail-under=60` globally. Phase 4 targets specific modules at 60%, but aggregate coverage may differ.
   - What's unclear: Whether aggregate coverage will reach 60% after this phase.
   - Recommendation: The target is per-module 60%. Keep the global --cov-fail-under=60 as-is; if aggregate is below 60% after this phase, address in later phases.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest >=9.0.0 + pytest-cov >=7.0.0 |
| Config file | pyproject.toml [tool.pytest.ini_options] |
| Quick run command | `pytest tests/test_fabric_connector.py tests/test_loader.py tests/test_batch_profiler.py -x --no-cov` |
| Full suite command | `pytest --cov=dq_framework --cov-report=term-missing` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| TEST-01 | fabric_connector.py 60%+ coverage | unit | `pytest tests/test_fabric_connector.py --cov=dq_framework.fabric_connector --cov-report=term-missing --no-cov` | Exists (needs expansion) |
| TEST-02 | loader.py 60%+ coverage | unit | `pytest tests/test_loader.py --cov=dq_framework.loader --cov-report=term-missing --no-cov` | Exists (needs expansion) |
| TEST-03 | batch_profiler.py 60%+ coverage | unit | `pytest tests/test_batch_profiler.py --cov=dq_framework.batch_profiler --cov-report=term-missing --no-cov` | Exists (needs expansion) |
| TEST-04 | Characterization tests for threshold logic | unit | `pytest tests/test_threshold_characterization.py -x` | Does not exist (Wave 0) |

### Sampling Rate
- **Per task commit:** `pytest tests/test_fabric_connector.py tests/test_loader.py tests/test_batch_profiler.py tests/test_threshold_characterization.py -x --no-cov`
- **Per wave merge:** `pytest --cov=dq_framework --cov-report=term-missing`
- **Phase gate:** All four module coverage targets met + characterization tests pass

### Wave 0 Gaps
- [ ] `tests/test_threshold_characterization.py` -- covers TEST-04 (new file)
- [ ] `tests/conftest.py` -- needs Spark/Fabric mock fixtures added (file exists but only has --fabric marker config)
- [ ] Migrate test_loader.py from unittest.TestCase to pytest classes (to enable fixtures)
- [ ] Migrate test_batch_profiler.py from unittest.TestCase to pytest classes (to enable fixtures)

## Coverage Gap Analysis

### fabric_connector.py (18% -> 60%+ target)
Lines requiring coverage (approximate):

| Method/Block | Lines | Currently Tested | What to Test |
|-------------|-------|-----------------|--------------|
| `__init__` (Fabric path loading) | 62-113 | Partially (basic init only) | abfss/Files/https config paths, mssparkutils.fs.head, fallback to file I/O |
| `config` property | 115-118 | No | Property access returns validator.config |
| `validate_spark_dataframe` (main flow) | 120-209 | No | Small dataset, large dataset sampling, memory warning, SPARK_AVAILABLE=False |
| `_validate_spark_chunked` | 211-289 | Yes (Phase 3) | Already covered |
| `_aggregate_chunk_results` | 291-391 | Yes (Phase 3) | Already covered |
| `validate_delta_table` | 393-427 | No | SparkSession.table mock, SPARK_AVAILABLE=False |
| `validate_lakehouse_file` | 429-477 | No | All 4 file formats, unsupported format error |
| `handle_failure` | 479-521 | No | log/halt/alert actions, success noop |
| `_send_alert` | 523-574 | No | Success path, retry logic, max retries exhausted |
| `_save_results_to_lakehouse` | 576-603 | No | Success save, FABRIC_UTILS_AVAILABLE=False, exception handling |
| `quick_validate` | 605-639 | No | Spark path, pandas path, halt_on_failure |

### loader.py (51% -> 60%+ target)
| Method/Block | Currently Tested | What to Test |
|-------------|-----------------|--------------|
| CSV with encoding fallback | Partially | Multiple encoding failures, explicit encoding arg |
| Parquet with PyArrow | No | PYARROW_AVAILABLE=True + sample_size, batch iteration, fallback |
| Excel loading | No | .xlsx and .xls paths |
| JSON loading | No | pd.read_json + sample_size head() |
| Unsupported format | No | ValueError for unknown extensions |
| File not found | No | FileNotFoundError |
| ABFSS parquet (no PyArrow) | No | is_abfss=True path |

### batch_profiler.py (51% -> 60%+ target)
| Method/Block | Currently Tested | What to Test |
|-------------|-----------------|--------------|
| process_single_file success | Yes | Already covered |
| process_single_file error | No | Exception in load/profile/save |
| process_single_file with thresholds | No | Custom thresholds passed through |
| run_parallel_profiling success | No | Multiple files, workers>1 |
| run_parallel_profiling no files | No | Empty directory, no supported files |
| run_parallel_profiling input not exist | No | Non-existent input_dir |

## Sources

### Primary (HIGH confidence)
- Project source code: dq_framework/fabric_connector.py, loader.py, batch_profiler.py, validator.py, constants.py, utils.py
- Project test files: tests/test_fabric_connector.py, test_loader.py, test_batch_profiler.py, conftest.py
- pyproject.toml: pytest and coverage configuration

### Secondary (MEDIUM confidence)
- pytest documentation patterns for mocking and fixtures (established patterns)
- unittest.mock documentation for MagicMock chaining (stdlib)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all tools already in pyproject.toml, no new dependencies
- Architecture: HIGH - existing test patterns in codebase provide clear templates
- Pitfalls: HIGH - identified from direct code analysis of import-time execution and mock chain patterns
- Coverage gaps: HIGH - derived from line-by-line analysis of source files vs existing tests

**Research date:** 2026-03-08
**Valid until:** 2026-04-08 (stable -- test tooling doesn't change rapidly)
