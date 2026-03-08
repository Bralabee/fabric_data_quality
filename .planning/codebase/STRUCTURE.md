# Codebase Structure

**Analysis Date:** 2026-03-08

## Directory Layout

```
2_DATA_QUALITY_LIBRARY/
├── dq_framework/               # Core Python package (the library)
│   ├── __init__.py             # Package exports, version
│   ├── batch_profiler.py       # Parallel multi-file profiling
│   ├── config_loader.py        # YAML config loading/validation
│   ├── constants.py            # All magic numbers and defaults
│   ├── data_profiler.py        # Data analysis and expectation generation
│   ├── fabric_connector.py     # MS Fabric/Spark integration
│   ├── ingestion.py            # File ingestion operations
│   ├── loader.py               # Multi-format data loading
│   ├── utils.py                # FileSystemHandler, Fabric detection
│   └── validator.py            # Core GX validation engine
├── config/                     # Production validation configs
│   ├── causeway_validation.yml # Causeway financial data rules
│   └── hss_incidents_validation.yml # HSS incidents rules
├── config_templates/           # Reusable config templates by data layer
│   ├── bronze_layer_template.yml
│   ├── silver_layer_template.yml
│   ├── gold_layer_template.yml
│   └── custom_template.yml
├── tests/                      # Test suite
│   ├── conftest.py             # Shared fixtures
│   ├── test_batch_profiler.py
│   ├── test_config_loader.py
│   ├── test_data_profiler.py
│   ├── test_fabric_connector.py
│   ├── test_ingestion.py
│   ├── test_loader.py
│   ├── test_utils.py
│   ├── test_validator.py
│   ├── data_parallel/          # Test input data (CSV files)
│   ├── output_parallel/        # Test output data
│   └── output_parallel_verify/ # Expected output for verification
├── examples/                   # Usage examples and demos
│   ├── complete_workflow_example.py
│   ├── demo_causeway_validation.py
│   ├── demo_usage.py
│   ├── fabric_etl_example.py
│   ├── profile_causeway_data.py
│   ├── simple_demo.py
│   ├── universal_profiler_demo.py
│   ├── usage_examples.py
│   └── *.yml                   # Example YAML configs
├── scripts/                    # CLI tools
│   ├── profile_data.py         # Universal data profiler CLI
│   └── activate_and_test.sh    # Dev environment setup
├── docs/                       # Documentation (markdown + PDF)
├── webapp/                     # Interactive HTML documentation
│   ├── index.html
│   ├── app.js
│   └── styles.css
├── sample_source_data/         # Sample data for demos
├── build/                      # Build artifacts (generated)
├── dist/                       # Distribution packages (generated)
├── htmlcov/                    # Coverage reports (generated)
├── pyproject.toml              # Package config, tool settings
├── requirements.txt            # Production dependencies
├── requirements-dev.txt        # Development dependencies
├── environment.yml             # Conda environment definition
├── Makefile                    # Build/test shortcuts
├── .flake8                     # Legacy linter config
├── .pre-commit-config.yaml     # Pre-commit hooks
├── .github/workflows/ci.yml   # CI pipeline
├── check_data.py               # Ad-hoc data inspection script
└── README.md                   # Project overview
```

## Directory Purposes

**`dq_framework/`:**
- Purpose: The installable Python package containing all framework code
- Contains: 9 Python modules covering config, loading, profiling, validation, Fabric integration
- Key files: `validator.py` (core engine), `data_profiler.py` (auto-generation), `fabric_connector.py` (Spark/Fabric adapter)

**`config/`:**
- Purpose: Production-ready YAML validation configurations for specific datasets
- Contains: YAML files with named expectations for real HS2 data sources
- Key files: `causeway_validation.yml` (39-column financial data), `hss_incidents_validation.yml`

**`config_templates/`:**
- Purpose: Starter templates organized by medallion architecture layer (bronze/silver/gold)
- Contains: Template YAML files with placeholder values and comments explaining each check type
- Key files: `bronze_layer_template.yml` (basic ingestion checks), `silver_layer_template.yml`, `gold_layer_template.yml`

**`tests/`:**
- Purpose: pytest test suite with one test file per framework module
- Contains: Unit tests, test fixtures in `conftest.py`, test data in `data_parallel/`
- Key files: `conftest.py` (shared fixtures), `test_validator.py`, `test_data_profiler.py`

**`examples/`:**
- Purpose: Runnable Python scripts and YAML configs demonstrating framework usage
- Contains: Demo scripts for various workflows (profiling, validation, Fabric ETL)
- Key files: `complete_workflow_example.py`, `fabric_etl_example.py`

**`scripts/`:**
- Purpose: CLI tools for operational use
- Contains: Data profiler CLI, environment setup script
- Key files: `profile_data.py` (main CLI entry point for profiling)

**`docs/`:**
- Purpose: Comprehensive project documentation
- Contains: Markdown guides for API reference, configuration, deployment, Fabric integration

**`webapp/`:**
- Purpose: Interactive HTML documentation site (static, no server)
- Contains: Single-page HTML app with JavaScript for tabs and theme toggle

## Key File Locations

**Entry Points:**
- `dq_framework/__init__.py`: Package entry point, exports all public classes
- `scripts/profile_data.py`: CLI entry point for data profiling
- `dq_framework/fabric_connector.py`: Contains `quick_validate()` convenience function

**Configuration:**
- `pyproject.toml`: Package metadata, dependencies, ruff/pytest/mypy/coverage config
- `dq_framework/constants.py`: All framework constants and thresholds
- `config/`: Production YAML validation configs
- `config_templates/`: Starter templates

**Core Logic:**
- `dq_framework/validator.py`: Great Expectations validation engine (DataQualityValidator)
- `dq_framework/data_profiler.py`: Data analysis and expectation generation (DataProfiler)
- `dq_framework/fabric_connector.py`: Fabric/Spark adapter (FabricDataQualityRunner)
- `dq_framework/config_loader.py`: YAML loading and validation (ConfigLoader)
- `dq_framework/loader.py`: Multi-format data loading (DataLoader)
- `dq_framework/utils.py`: Filesystem abstraction and Fabric detection

**Testing:**
- `tests/conftest.py`: Shared pytest fixtures
- `tests/test_*.py`: One test file per framework module
- `tests/data_parallel/`: CSV test data files

## Naming Conventions

**Files:**
- Python modules: `snake_case.py` (e.g., `config_loader.py`, `data_profiler.py`, `batch_profiler.py`)
- Test files: `test_<module_name>.py` matching the source module name
- YAML configs: `<dataset>_validation.yml` for production, `<layer>_layer_template.yml` for templates
- Example files: descriptive `snake_case` names (e.g., `complete_workflow_example.py`)

**Directories:**
- All lowercase with underscores: `dq_framework/`, `config_templates/`, `sample_source_data/`
- Test data dirs: `data_parallel/`, `output_parallel/`

**Classes:**
- PascalCase: `DataQualityValidator`, `DataProfiler`, `BatchProfiler`, `ConfigLoader`, `DataLoader`, `DataIngester`, `FabricDataQualityRunner`, `FileSystemHandler`

**Functions/Methods:**
- snake_case: `validate()`, `profile()`, `generate_expectations()`, `load_data()`, `quick_validate()`
- Private methods prefixed with underscore: `_build_expectation_suite()`, `_format_results()`, `_detect_column_type()`

**Constants:**
- UPPER_SNAKE_CASE in `constants.py`: `DEFAULT_VALIDATION_THRESHOLD`, `FABRIC_LARGE_DATASET_THRESHOLD`
- Boolean flags: `GX_AVAILABLE`, `SPARK_AVAILABLE`, `FABRIC_AVAILABLE`, `PYARROW_AVAILABLE`

## Where to Add New Code

**New Validation Feature:**
- Primary code: `dq_framework/validator.py` (new methods on DataQualityValidator)
- Tests: `tests/test_validator.py`
- If Fabric-specific: `dq_framework/fabric_connector.py` and `tests/test_fabric_connector.py`

**New Data Source Support:**
- Implementation: `dq_framework/loader.py` (add new format handler in `DataLoader.load_data()`)
- Tests: `tests/test_loader.py`
- If ABFSS-aware: also update `dq_framework/utils.py` FileSystemHandler

**New Profiling Rule/Type Detection:**
- Implementation: `dq_framework/data_profiler.py` (add to `_detect_column_type()` and `_generate_validity_expectations()`)
- Constants: Add thresholds to `dq_framework/constants.py`
- Tests: `tests/test_data_profiler.py`

**New Configuration Template:**
- Add YAML file to `config_templates/` following the existing pattern
- Use `bronze_layer_template.yml` as a structural reference

**New Production Config:**
- Add YAML file to `config/` named `<dataset>_validation.yml`
- Can auto-generate from profiling, then customize

**New CLI Tool/Script:**
- Add to `scripts/` directory
- Import from `dq_framework` package

**New Example:**
- Add to `examples/` directory with descriptive name

**New Utility Function:**
- Add to `dq_framework/utils.py`
- Export from `dq_framework/__init__.py` if public

## Special Directories

**`build/`:**
- Purpose: Python build artifacts (contains stale copy of dq_framework)
- Generated: Yes (by `python -m build`)
- Committed: Should not be committed

**`dist/`:**
- Purpose: Distribution packages (wheel and tarball for v1.2.0)
- Generated: Yes
- Committed: Should not be committed

**`htmlcov/`:**
- Purpose: HTML coverage reports from pytest-cov
- Generated: Yes (by `pytest --cov-report=html`)
- Committed: Has its own .gitignore

**`sample_source_data/`:**
- Purpose: Real sample data files for demos (Causeway CSV)
- Generated: No
- Committed: Yes

**`config_templates/`:**
- Purpose: Starter templates users copy and customize for their datasets
- Generated: No
- Committed: Yes

---

*Structure analysis: 2026-03-08*
