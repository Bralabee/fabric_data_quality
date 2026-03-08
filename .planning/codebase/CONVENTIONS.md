# Coding Conventions

**Analysis Date:** 2026-03-08

## Naming Patterns

**Files:**
- Use `snake_case.py` for all Python modules: `data_profiler.py`, `config_loader.py`, `batch_profiler.py`
- Test files mirror source files with `test_` prefix: `test_validator.py` tests `validator.py`
- Configuration files use `snake_case.yml`: `causeway_validation.yml`

**Functions:**
- Use `snake_case` for all functions and methods: `load_data()`, `generate_expectations()`, `validate_spark_dataframe()`
- Private/internal methods use single leading underscore: `_build_expectation_suite()`, `_format_results()`, `_detect_column_type()`
- Static detection/check methods follow `is_*` or `looks_like_*` pattern: `is_abfss()`, `_looks_like_date()`, `_is_numeric_id_pattern()`

**Variables:**
- Use `snake_case` for all variables: `sample_size`, `config_path`, `null_tolerance`
- Constants use `UPPER_SNAKE_CASE` and live in `dq_framework/constants.py`: `DEFAULT_VALIDATION_THRESHOLD`, `LARGE_FILE_SIZE_MB`
- Boolean flags use `is_*` or descriptive names: `is_fabric`, `sample_large_data`, `include_structural`
- Module-level availability flags: `GX_AVAILABLE`, `SPARK_AVAILABLE`, `PYARROW_AVAILABLE`, `FABRIC_AVAILABLE`

**Classes:**
- Use `PascalCase`: `DataQualityValidator`, `FabricDataQualityRunner`, `DataProfiler`, `BatchProfiler`, `ConfigLoader`, `DataLoader`, `DataIngester`, `FileSystemHandler`

**Types:**
- Use Python 3.10+ type hints with `typing` module: `Optional[str]`, `dict[str, Any]`, `list[Dict[str, Any]]`
- Note: Some files use older `Dict`, `List`, `Optional` from typing (e.g., `config_loader.py`, `data_profiler.py`) while newer files use built-in generics (`dict[str, Any]` in `validator.py`, `fabric_connector.py`). Prefer the modern built-in syntax for new code.

## Code Style

**Formatting:**
- Line length: 100 characters (configured in `pyproject.toml`, `.pre-commit-config.yaml`, `Makefile`)
- Formatter: black with `--line-length=100`
- Quote style: double quotes (configured in `[tool.ruff.format]`)
- Indent: 4 spaces

**Linting:**
- Primary: ruff (configured in `pyproject.toml` under `[tool.ruff]`)
  - Rules enabled: E (pycodestyle errors), F (pyflakes), W (pycodestyle warnings), I (isort), N (pep8-naming), UP (pyupgrade), B (flake8-bugbear), SIM (flake8-simplify), RUF (ruff-specific)
  - E501 ignored (line length handled by formatter)
- Legacy: flake8 still referenced in Makefile and CI (`--max-line-length=100 --extend-ignore=E203,W503`)
- Type checking: mypy (`--ignore-missing-imports`, `--strict-optional`)
- Pre-commit hooks: black, isort, flake8, mypy, bandit, pydocstyle (Google convention)

**Pre-commit Configuration:** `.pre-commit-config.yaml`
- Includes: black, isort, flake8 (with flake8-docstrings, flake8-bugbear), mypy, bandit, pydocstyle (Google convention), YAML/JSON/TOML validation, trailing whitespace, end-of-file fixer, detect-private-key, check-added-large-files (max 1000KB)

## Import Organization

**Order:**
1. Standard library imports (`os`, `logging`, `uuid`, `pathlib`, `datetime`, `typing`)
2. Third-party imports (`pandas`, `numpy`, `yaml`, `great_expectations`, `pyarrow`)
3. Local imports (`from .config_loader import ConfigLoader`, `from .constants import ...`)

**Path Aliases:**
- No path aliases configured. All imports use relative imports within `dq_framework` package.
- Tests import directly from the package: `from dq_framework import DataQualityValidator`
- Some test files add parent to sys.path (legacy pattern): `sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))`

**Import Style:**
- isort configured with `profile=black` and `line-length=100`
- Known first-party: `dq_framework` (configured in `[tool.ruff.lint.isort]`)

**Conditional Imports Pattern:**
- Optional dependencies use try/except with availability flags:
```python
try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import great_expectations as gx
    GX_AVAILABLE = True
except ImportError:
    GX_AVAILABLE = False
```
- This pattern appears in: `dq_framework/validator.py`, `dq_framework/loader.py`, `dq_framework/fabric_connector.py`, `dq_framework/utils.py`

## Error Handling

**Patterns:**
- Raise specific exceptions with descriptive messages:
  - `FileNotFoundError` for missing files (`config_loader.py`, `loader.py`)
  - `ValueError` for invalid configuration or unsupported formats (`config_loader.py`, `loader.py`, `fabric_connector.py`)
  - `ImportError` for missing required dependencies (`validator.py`, `fabric_connector.py`)
- Log errors before raising or returning:
```python
except Exception as e:
    logger.error(f"Error loading file {path_str}: {e}")
    raise
```
- Return boolean success for operations that can fail gracefully:
```python
# dq_framework/ingestion.py
def ingest_file(self, ...) -> bool:
    try:
        # ... operation
        return True
    except Exception as e:
        logger.error(f"Failed to ingest {source_path}: {e}")
        return False
```
- Validation methods raise `ValueError` with match-friendly messages for testing
- The `handle_failure()` method in `dq_framework/fabric_connector.py` supports configurable actions: `'log'`, `'halt'` (raises `ValueError`), `'alert'`

## Logging

**Framework:** Python standard `logging` module

**Patterns:**
- Every module creates its own logger: `logger = logging.getLogger(__name__)`
- Some classes also create instance loggers: `self.logger = logging.getLogger(__name__)` (in `config_loader.py`)
- Use `logger.info()` for operational messages (validation passed, config loaded, file ingested)
- Use `logger.warning()` for non-fatal issues (large file auto-sampling, fallback encoding, ABFSS without mssparkutils)
- Use `logger.error()` for failures (validation failed, file load errors)
- Use `logger.debug()` for diagnostic info (date detection details, config validation)
- Use f-strings in log messages throughout (not lazy % formatting)
- Pipeline log file exists at `pipeline.log` (root level)

## Comments

**When to Comment:**
- Module-level docstrings with title, description, and usage examples (all modules follow this)
- Section separators using `# ===...===` for major sections in `dq_framework/constants.py` and `Makefile`
- Inline comments for non-obvious logic (e.g., encoding fallback chain, PyArrow optimization)
- `# TODO:` for planned but unimplemented features (e.g., alert implementation in `fabric_connector.py`)

**Docstrings:**
- Use Google-style docstrings (enforced by pydocstyle in pre-commit)
- All public classes and methods have docstrings with `Args:`, `Returns:`, `Raises:` sections
- Include `Example:` sections using doctest format (`>>>`) in class-level docstrings
- Private methods have shorter docstrings (one-liner or brief description)

## Function Design

**Size:** Functions are generally focused and under 50 lines. Larger methods like `_format_results()` in `dq_framework/validator.py` (~130 lines) and `validate_spark_dataframe()` in `dq_framework/fabric_connector.py` (~90 lines) are the exceptions.

**Parameters:**
- Use `Optional[type] = None` for optional parameters
- Use keyword arguments with defaults for configuration: `sample_large_data: bool = True`, `severity_threshold: str = "medium"`
- Accept both file paths and dictionaries where appropriate (see `ConfigLoader.load()` and `DataQualityValidator.__init__()`)
- Use `**kwargs` pass-through for pandas read functions (in `DataLoader.load_data()`)

**Return Values:**
- Validation methods return `dict[str, Any]` with standardized keys: `success`, `evaluated_checks`, `failed_checks`, `success_rate`, `statistics`, `timestamp`
- Profiling methods return `Dict[str, Any]` with structured results
- Boolean returns for simple success/failure operations (`ingest_file()`, `exists()`, `validate_yaml_syntax()`)

## Module Design

**Exports:**
- All public API classes and utilities exported from `dq_framework/__init__.py`
- Explicit `__all__` list defined in `dq_framework/__init__.py`
- Package version via `importlib.metadata` with fallback: `__version__ = "2.0.0"`

**Barrel Files:**
- Single barrel file: `dq_framework/__init__.py`
- Re-exports all major classes: `DataQualityValidator`, `FabricDataQualityRunner`, `ConfigLoader`, `DataProfiler`, `BatchProfiler`, `DataLoader`, `DataIngester`, `FileSystemHandler`
- Also exports utility constants and functions: `FABRIC_AVAILABLE`, `FABRIC_UTILS_AVAILABLE`, `_is_fabric_runtime`, `get_mssparkutils`

**Class Design:**
- Classes use composition over inheritance (no class hierarchies)
- Static methods used for stateless utility operations: `DataLoader.load_data()`, `FileSystemHandler.is_abfss()`
- Class methods used for factory-like patterns: `BatchProfiler.run_parallel_profiling()`

## Constants Pattern

- All magic numbers and configuration defaults centralized in `dq_framework/constants.py`
- Constants organized by category with section headers: VALIDATION THRESHOLDS, DATA PROFILING, DATA LOADING, FABRIC INTEGRATION
- Each constant has a comment explaining its purpose and units
- Import specific constants: `from .constants import DEFAULT_VALIDATION_THRESHOLD`

---

*Convention analysis: 2026-03-08*
