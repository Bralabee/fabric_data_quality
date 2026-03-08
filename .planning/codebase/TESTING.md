# Testing Patterns

**Analysis Date:** 2026-03-08

## Test Framework

**Runner:**
- pytest >= 9.0.0 (dev dependency in `pyproject.toml`)
- Config: `pyproject.toml` under `[tool.pytest.ini_options]`

**Assertion Library:**
- Built-in pytest assertions (`assert`)
- `pytest.raises()` for exception testing
- `pd.testing.assert_frame_equal()` for DataFrame comparison (in `tests/test_ingestion.py`)

**Run Commands:**
```bash
make test               # Run all tests (pytest tests/ -v)
make test-cov           # Tests with HTML + terminal coverage report
make test-fast          # Skip slow tests (pytest -m "not slow")
make test-integration   # Integration tests only (pytest -m "integration")
pytest tests/ -v        # Direct pytest invocation
pytest tests/ -v --cov=dq_framework --cov-report=term-missing  # With coverage
```

## Test File Organization

**Location:**
- Separate `tests/` directory at project root (not co-located with source)
- Test data in `tests/data_parallel/` (CSV files for batch profiler tests)
- Test output in `tests/output_parallel/` and `tests/output_parallel_verify/` (generated YAML configs)

**Naming:**
- Test files: `test_<module_name>.py` mirroring source module names
- Test classes: `Test<ClassName>` or `Test<ClassName><Aspect>` (e.g., `TestDataIngesterIngestFileLocal`, `TestFileSystemHandlerIsAbfss`)
- Test functions: `test_<description_of_behavior>` (e.g., `test_validate_success`, `test_load_csv`, `test_is_abfss_with_valid_abfss_path`)

**Structure:**
```
tests/
  conftest.py                  # Shared fixtures, custom pytest options (--fabric)
  test_validator.py            # Tests for DataQualityValidator + ConfigLoader
  test_config_loader.py        # Dedicated ConfigLoader tests
  test_data_profiler.py        # Comprehensive DataProfiler tests (~600 lines)
  test_loader.py               # DataLoader tests (unittest-based)
  test_ingestion.py            # DataIngester tests (pytest-based)
  test_fabric_connector.py     # FabricDataQualityRunner tests
  test_batch_profiler.py       # BatchProfiler tests (unittest-based)
  test_utils.py                # FileSystemHandler + utility tests
  data_parallel/               # Test fixture CSV files
    file1.csv
    file2.csv
    file3.csv
  output_parallel/             # Generated test output
  output_parallel_verify/      # Verification output
```

## Test Structure

**Suite Organization:**
- Tests are grouped into classes by component or behavior area
- Two patterns coexist:
  1. **pytest-native** (preferred, used in newer tests): pytest classes with fixtures
  2. **unittest-based** (legacy, in `test_loader.py`, `test_batch_profiler.py`): `unittest.TestCase` subclasses

**pytest-native pattern (use this for new tests):**
```python
class TestDataQualityValidator:
    """Test suite for DataQualityValidator"""

    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing"""
        return {
            "validation_name": "test_validation",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1, "max_value": 100}
                }
            ]
        }

    @pytest.fixture
    def sample_dataframe(self):
        """Sample DataFrame for testing"""
        return pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45]
        })

    def test_validate_success(self, sample_config, sample_dataframe):
        """Test successful validation"""
        validator = DataQualityValidator(sample_config)
        results = validator.validate(sample_dataframe)

        assert results["success"] is True
        assert results["statistics"]["evaluated_expectations"] == 2
```

**unittest-based pattern (legacy, do not use for new tests):**
```python
class TestDataLoader(unittest.TestCase):

    @patch('dq_framework.utils.FileSystemHandler.exists')
    @patch('pandas.read_csv')
    def test_load_csv(self, mock_read_csv, mock_exists):
        mock_exists.return_value = True
        mock_read_csv.return_value = pd.DataFrame({'a': [1, 2, 3]})

        df = DataLoader.load_data('test.csv')

        self.assertIsInstance(df, pd.DataFrame)
        mock_read_csv.assert_called()
```

**Setup/Teardown:**
- Use pytest fixtures for setup (no `setUp`/`tearDown` methods)
- `tmp_path` fixture used extensively for temporary file/directory creation
- Manual cleanup with `Path.unlink()` in some older tests (e.g., `test_config_loader.py`)
- `conftest.py` defines shared configuration: `--fabric` flag, custom markers

## Custom Markers

Defined in `pyproject.toml` under `[tool.pytest.ini_options]` and `tests/conftest.py`:

- `slow` - marks tests as slow (skip with `-m "not slow"`)
- `integration` - marks integration tests (run with `-m "integration"`)
- `fabric` - marks tests requiring MS Fabric environment (skip unless `--fabric` flag passed)

**Usage:**
```python
@pytest.mark.fabric
def test_validate_spark_dataframe(self, sample_config_path):
    """Test validation of Spark DataFrame (requires Fabric)"""
    ...

@pytest.mark.slow
def test_large_dataset_profiling(self):
    ...
```

**conftest.py pattern for conditional skip:**
```python
def pytest_collection_modifyitems(config, items):
    if config.getoption("--fabric"):
        return
    skip_fabric = pytest.mark.skip(reason="need --fabric option to run")
    for item in items:
        if "fabric" in item.keywords:
            item.add_marker(skip_fabric)
```

## Mocking

**Framework:** `unittest.mock` (stdlib)

**Patterns:**

**Decorator-based mocking (common in loader/batch tests):**
```python
@patch('dq_framework.utils.FileSystemHandler.exists')
@patch('dq_framework.utils.FileSystemHandler.is_abfss')
@patch('pandas.read_csv')
def test_load_csv(self, mock_read_csv, mock_is_abfss, mock_exists):
    mock_exists.return_value = True
    mock_is_abfss.return_value = False
    mock_read_csv.return_value = pd.DataFrame({'a': [1, 2, 3]})

    df = DataLoader.load_data('test.csv')

    self.assertIsInstance(df, pd.DataFrame)
    mock_read_csv.assert_called()
```

**Context manager mocking (for module-level constants):**
```python
with patch('dq_framework.loader.PYARROW_AVAILABLE', False):
    df = DataLoader.load_data('test.parquet')
```

**Patching module-level variables for Fabric detection:**
```python
@patch('dq_framework.utils.FABRIC_AVAILABLE', True)
@patch('dq_framework.utils._mssparkutils')
def test_list_files_abfss_with_fabric_success(self, mock_mssparkutils):
    mock_file1 = MagicMock()
    mock_file1.path = "abfss://container@account/file1.csv"
    mock_file1.isDir = False
    mock_mssparkutils.fs.ls.return_value = [mock_file1]
    ...
```

**Logger mocking:**
```python
@patch('dq_framework.ingestion.logger')
def test_ingest_file_logs_success(self, mock_logger, tmp_path):
    ...
    ingester.ingest_file(source_file, target_file, is_fabric=False)
    mock_logger.info.assert_called()
```

**What to Mock:**
- External file system operations (`FileSystemHandler.exists`, `FileSystemHandler.is_abfss`)
- Pandas I/O functions (`pd.read_csv`, `pd.read_parquet`, `pd.read_excel`)
- Module-level availability flags (`PYARROW_AVAILABLE`, `FABRIC_AVAILABLE`)
- Fabric-specific utilities (`_mssparkutils`)
- Loggers for verifying log output

**What NOT to Mock:**
- Core business logic (profiling, validation, config loading)
- In-memory DataFrame operations
- Actual file I/O in integration-style tests (use `tmp_path` instead)

## Fixtures and Factories

**Test Data:**
```python
@pytest.fixture
def sample_dataframe():
    """Standard sample DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45],
        "email": ["a@test.com", "b@test.com", "c@test.com", "d@test.com", "e@test.com"],
        "status": ["active", "active", "inactive", "active", "inactive"],
    })

@pytest.fixture
def sample_config(self):
    """Sample configuration for testing"""
    return {
        "validation_name": "test_validation",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1, "max_value": 100}
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "id"}
            }
        ]
    }
```

**Temporary Config Files:**
```python
@pytest.fixture
def temp_config_file(self):
    """Create a temporary config file"""
    config = {
        "validation_name": "test_validation",
        "expectations": [...]
    }
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config, f)
        return f.name
```

**Location:**
- Fixtures are defined inline within test classes (method-level `@pytest.fixture`)
- Module-level fixtures in `tests/test_data_profiler.py` (shared across classes in that module)
- Shared configuration in `tests/conftest.py` (markers, custom options)
- Static test data files in `tests/data_parallel/`
- No separate `fixtures/` or `factories/` directory

## Coverage

**Requirements:**
- Minimum: 60% overall (enforced via `--cov-fail-under=60` in `pyproject.toml` and CI)
- Target for new code: >80% (stated in `CONTRIBUTING.md`)

**Current Coverage (from `htmlcov/status.json`):**

| Module | Statements | Missing | Coverage |
|--------|-----------|---------|----------|
| `constants.py` | 26 | 0 | 100% |
| `ingestion.py` | 22 | 0 | 100% |
| `utils.py` | 68 | 6 | 91% |
| `__init__.py` | 15 | 2 | 87% |
| `data_profiler.py` | 174 | 18 | 90% |
| `validator.py` | 128 | 26 | 80% |
| `config_loader.py` | 63 | 16 | 75% |
| `batch_profiler.py` | 54 | 26 | 52% |
| `loader.py` | 84 | 41 | 51% |
| `fabric_connector.py` | 189 | 154 | 19% |

**View Coverage:**
```bash
make test-cov                    # Generate HTML report
open htmlcov/index.html          # View in browser
pytest tests/ --cov=dq_framework --cov-report=term-missing  # Terminal report
```

## Test Types

**Unit Tests:**
- Majority of tests are unit tests
- Test individual methods and classes in isolation
- Mock external dependencies (file system, Fabric, pandas I/O)
- Located in all `test_*.py` files

**Integration Tests:**
- `tests/test_ingestion.py` includes integration tests with real parquet file I/O (class `TestDataIngesterParquetIntegration`)
- `tests/test_data_profiler.py` includes end-to-end profiling and expectation generation tests
- `tests/test_validator.py` runs actual Great Expectations validation against DataFrames

**Fabric/Environment Tests:**
- `tests/test_fabric_connector.py` has `@pytest.mark.fabric` tests requiring Spark
- Skipped by default; run with `pytest --fabric`
- Non-fabric initialization test runs without the marker

**E2E Tests:**
- Not formalized as a separate test suite
- Example scripts in `examples/` serve as informal E2E validation

## Common Patterns

**Async Testing:**
- Not applicable; the codebase is synchronous

**Error Testing:**
```python
def test_validate_missing_expectations(self):
    """Test validation fails without expectations"""
    config = {"validation_name": "test"}

    loader = ConfigLoader()
    with pytest.raises(ValueError, match="expectations"):
        loader.validate(config)

def test_load_nonexistent_file(self):
    """Test loading nonexistent file raises error"""
    loader = ConfigLoader()
    with pytest.raises(FileNotFoundError):
        loader.load("/nonexistent/path/config.yml")
```

**Boolean Return Testing:**
```python
def test_ingest_file_source_not_exists(self, tmp_path):
    """Test ingestion fails gracefully for non-existent source."""
    ingester = DataIngester()
    source_file = tmp_path / "nonexistent.csv"
    target_file = tmp_path / "target" / "dest.csv"

    result = ingester.ingest_file(source_file, target_file, is_fabric=False)

    assert result is False
```

**Validation Result Testing:**
```python
def test_validate_success(self, sample_config, sample_dataframe):
    """Test successful validation"""
    validator = DataQualityValidator(sample_config)
    results = validator.validate(sample_dataframe)

    assert results["success"] is True
    assert results["statistics"]["evaluated_expectations"] == 2
    assert results["statistics"]["successful_expectations"] == 2
```

**Testing with Temporary Files:**
```python
def test_ingest_file_local_success(self, tmp_path):
    """Test successful local file ingestion."""
    ingester = DataIngester()

    source_dir = tmp_path / "source"
    source_dir.mkdir()
    source_file = source_dir / "test.csv"
    source_file.write_text("col1,col2\n1,2\n3,4")

    target_dir = tmp_path / "target"
    target_file = target_dir / "test.csv"

    result = ingester.ingest_file(source_file, target_file, is_fabric=False)

    assert result is True
    assert target_file.exists()
    assert target_file.read_text() == source_file.read_text()
```

## CI Integration

**GitHub Actions:** `.github/workflows/ci.yml`
- **Lint job:** flake8, black, isort checks
- **Test job:** pytest with coverage on Python 3.8, 3.9, 3.10, 3.11 (matrix strategy)
  - Coverage uploaded to Codecov on Python 3.10
  - `--cov-fail-under=60` enforced
- **Build job:** package build + twine check (depends on test)
- **Security job:** bandit scan + safety check on dependencies

**pytest Configuration (pyproject.toml):**
```toml
[tool.pytest.ini_options]
minversion = "7.0"
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = [
    "-ra",
    "--strict-markers",
    "--strict-config",
    "--showlocals",
    "--cov=dq_framework",
    "--cov-report=html",
    "--cov-report=term-missing",
    "--cov-fail-under=60",
]
```

---

*Testing analysis: 2026-03-08*
