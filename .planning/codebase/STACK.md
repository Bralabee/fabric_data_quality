# Technology Stack

**Analysis Date:** 2026-03-08

## Languages

**Primary:**
- Python >=3.10 - All framework code, tests, examples, scripts

**Secondary:**
- JavaScript (vanilla) - Interactive documentation webapp (`webapp/app.js`)
- HTML/CSS - Documentation webapp (`webapp/index.html`, `webapp/styles.css`)
- YAML - Validation configuration files (`config/`, `config_templates/`)

## Runtime

**Environment:**
- Python >=3.10, <3.14 (tested against 3.10, 3.11, 3.12, 3.13 per `pyproject.toml`)
- CI tests against 3.8, 3.9, 3.10, 3.11 (note: CI matrix is outdated vs `pyproject.toml`)
- Conda environment name: `fabric-dq` (defined in `environment.yml`)

**Package Manager:**
- pip (primary)
- conda (alternative, via `environment.yml`)
- Lockfile: Not present (no pip-compile output committed)

## Frameworks

**Core:**
- Great Expectations >=1.0.0, <2.0.0 - Data quality validation engine (`dq_framework/validator.py`)
- pandas >=2.0.0 - DataFrame manipulation throughout
- PySpark >=3.3.0, <4.0.0 (optional) - Spark DataFrame support in Fabric (`dq_framework/fabric_connector.py`)

**Testing:**
- pytest >=9.0.0 (pyproject.toml) / >=7.4.0 (requirements-dev.txt) - Test runner
- pytest-cov >=7.0.0 - Coverage reporting (minimum 60% enforced)
- pytest-mock >=3.15.0 - Mocking support
- pytest-xdist >=3.3.0 - Parallel test execution
- pytest-timeout >=2.1.0 - Test timeout support
- pytest-benchmark >=4.0.0 - Performance benchmarking

**Build/Dev:**
- setuptools >=65.0 + wheel - Build system (`pyproject.toml` PEP 518)
- python -m build - Package build command
- twine - Package upload/validation
- Makefile - Developer task runner (`Makefile`)

## Key Dependencies

**Critical:**
- `great-expectations` >=1.0.0, <2.0.0 - Core validation engine; the entire framework wraps GX
- `pandas` >=2.0.0 - All data manipulation and DataFrame operations
- `pyyaml` >=6.0 - YAML config file parsing (`dq_framework/config_loader.py`)
- `pyarrow` >=14.0.0 - Parquet file reading with batch optimization (`dq_framework/loader.py`)
- `sqlalchemy` >=2.0.0 - Required by Great Expectations for data context operations

**Infrastructure:**
- `numpy` >=1.24.0 - Numerical operations in profiling (`dq_framework/data_profiler.py`)
- `openpyxl` >=3.0.0 - Excel (.xlsx) file reading (`dq_framework/loader.py`)
- `xlrd` >=2.0.0 - Legacy Excel (.xls) file reading (`dq_framework/loader.py`)
- `typing-extensions` >=4.5.0 - Extended type hints
- `python-dateutil` >=2.8.0 - Date parsing utilities

**Dev-only (notable):**
- `ruff` >=0.15.0 - Fast linter/formatter (configured in `pyproject.toml`, newer choice)
- `black` >=23.0.0 - Code formatter (used in Makefile, pre-commit, CI)
- `flake8` >=6.0.0 - Linter (used in Makefile, pre-commit, CI)
- `isort` >=5.12.0 - Import sorting (used in Makefile, pre-commit, CI)
- `mypy` >=1.19.0 - Static type checking
- `bandit` >=1.7.0 - Security scanning
- `sphinx` >=6.0.0 - Documentation generation
- `pre-commit` >=4.0.0 - Git hooks framework

## Configuration

**Environment:**
- `GX_ANALYTICS_ENABLED` env var set to `False` by default in `dq_framework/validator.py` to disable GX telemetry
- No `.env` file detected in repository
- `python-dotenv` listed in dev dependencies but not imported in framework code

**Build:**
- `pyproject.toml` - Primary project configuration (PEP 518), tool configs for ruff, pytest, coverage, mypy
- `environment.yml` - Conda environment definition
- `Makefile` - Developer workflow commands (install, test, lint, format, build, docs, security, CI)
- `.pre-commit-config.yaml` - Git hook configuration (black, isort, flake8, mypy, bandit, pydocstyle, pre-commit-hooks)
- `.github/workflows/ci.yml` - GitHub Actions CI pipeline

**Validation Configuration:**
- YAML-based validation configs stored in `config/` (production) and `config_templates/` (templates)
- Config structure: `validation_name`, `expectations[]`, `data_source{}`, `quality_thresholds{}`, `metadata{}`
- Templates provided for bronze, silver, gold data layers and custom use cases

## Platform Requirements

**Development:**
- Python >=3.10
- pip or conda for dependency management
- Make (for Makefile targets)
- Git with pre-commit hooks (optional but recommended)
- Run `make dev-setup` or `pip install -e ".[dev]"` for development install

**Production:**
- Microsoft Fabric (primary target platform)
- PySpark runtime (provided by Fabric environment)
- `notebookutils` / `mssparkutils` (Fabric-native, detected at runtime in `dq_framework/utils.py`)
- Supports local execution without Fabric (graceful degradation via try/except imports)
- Package distributable as wheel via `python -m build`

**Dual-mode Operation:**
- Fabric mode: Uses `mssparkutils` for ABFSS file access, Lakehouse integration, Spark DataFrames
- Local mode: Uses standard filesystem, pandas DataFrames, local file I/O
- Runtime detection via `_is_fabric_runtime()` in `dq_framework/utils.py` (checks `/lakehouse/default/Files` path)

---

*Stack analysis: 2026-03-08*
