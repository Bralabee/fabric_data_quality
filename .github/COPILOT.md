# AI Assistant Instructions for Fabric Data Quality Framework

> This file provides context and guidelines for AI assistants (GitHub Copilot, etc.) working with this codebase.

## Project Overview

The **Fabric Data Quality Framework** is a reusable data quality validation library built on Great Expectations. It provides YAML-based configuration for data validation rules with Microsoft Fabric integration support.

**Key Facts:**
- **Version:** 1.2.0
- **Python:** 3.8+ (tested on 3.8, 3.9, 3.10, 3.11)
- **Core Dependency:** Great Expectations 0.18.x
- **Package Name:** `fabric-data-quality`
- **Conda Environment:** `fabric-dq`

## Architecture

```
dq_framework/
├── __init__.py          # Main exports (import from here)
├── validator.py         # DataQualityValidator - core validation
├── fabric_runner.py     # FabricDataQualityRunner - Fabric integration
├── config_loader.py     # ConfigLoader - YAML parsing
├── data_profiler.py     # DataProfiler - auto-generate expectations
├── batch_profiler.py    # BatchProfiler - parallel processing
├── data_loader.py       # DataLoader - file loading utilities
├── ingestion.py         # DataIngester - data ingestion
├── utils.py             # FileSystemHandler, Fabric detection
└── constants.py         # Centralized constants
```

## Coding Standards

### Imports
Always import from the package root when possible:
```python
# Preferred
from dq_framework import DataQualityValidator, DataProfiler

# Also acceptable for internal code
from dq_framework.validator import DataQualityValidator
```

### Constants
Use constants from `dq_framework.constants`:
```python
from dq_framework.constants import (
    DEFAULT_SEVERITY_THRESHOLDS,
    SAMPLE_SIZE_DEFAULT,
    QUALITY_SCORE_PRECISION,
)
```

**Key Constants:**
- `DEFAULT_SEVERITY_THRESHOLDS`: {"critical": 100, "high": 95, "medium": 85, "low": 70}
- `SAMPLE_SIZE_DEFAULT`: 10000
- `NULL_THRESHOLD_PCT`: 50
- `UNIQUENESS_THRESHOLD_PCT`: 95

### Error Handling
- Log errors with context, don't suppress silently
- Use specific exception types where possible
- Always clean up resources in finally blocks

### Fabric Environment Detection
Use the centralized detection from utils:
```python
from dq_framework.utils import is_fabric_environment, get_fabric_workspace_id
```

## Testing Guidelines

### Running Tests
```bash
# Activate environment first
conda activate fabric-dq

# Run all tests with coverage
pytest tests/ -v --cov=dq_framework --cov-report=term-missing

# Run specific test file
pytest tests/test_validator.py -v

# Run with markers
pytest tests/ -v -m "not integration"
```

### Test Coverage Target
- **Minimum:** 60%
- **Goal:** 80%+
- **Current:** ~70%

### Writing Tests
- Use `pytest` fixtures in `conftest.py`
- Mock Fabric-specific functionality for unit tests
- Use `tmp_path` fixture for file operations
- Test both success and failure paths

## Configuration

### YAML Config Structure
```yaml
validation_name: "example_validation"
description: "Description of this validation"

data_source:
  type: "lakehouse"  # or "delta_table", "file"
  lakehouse_name: "MyLakehouse"
  file_path: "Files/bronze/data"
  file_format: "parquet"

expectations:
  - expectation_type: "expect_column_to_exist"
    kwargs:
      column: "id"
    meta:
      severity: "critical"
      description: "ID column must exist"
```

### Severity Levels
- **critical**: Must pass 100%
- **high**: Must pass 95%
- **medium**: Must pass 85%
- **low**: Must pass 70%

## Common Patterns

### Basic Validation
```python
from dq_framework import DataQualityValidator
import pandas as pd

df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
validator = DataQualityValidator(config_path="config/my_config.yml")
results = validator.validate(df)
print(f"Passed: {results['success']}")
```

### Data Profiling
```python
from dq_framework import DataProfiler

profiler = DataProfiler()
expectations = profiler.profile(df, output_path="config/auto_generated.yml")
```

### Fabric Integration
```python
from dq_framework import FabricDataQualityRunner

runner = FabricDataQualityRunner(config_dir="/lakehouse/default/Files/configs")
results = runner.run_validation("my_dataset")
```

## Dependencies

### Required
- great_expectations>=0.18.0,<0.19.0
- pandas>=1.5.0
- pyyaml>=6.0
- numpy>=1.21.0
- typing_extensions>=4.0.0

### Development
- pytest>=7.0.0
- pytest-cov>=4.0.0
- black>=23.0.0
- flake8>=6.0.0
- mypy>=1.0.0

## Known Limitations

1. **Spark DataFrames**: Must be converted to Pandas before validation
2. **Large Datasets**: Use `sample_size` parameter in DataProfiler
3. **Fabric Notebooks**: Some imports behave differently in Fabric runtime
4. **GE Version**: Pinned to 0.18.x for stability

## File Locations

- **Configs**: `config/` directory
- **Templates**: `config_templates/` for starting points
- **Examples**: `examples/` for real-world configs
- **Tests**: `tests/` with pytest conventions
- **Docs**: `docs/` for detailed documentation

## CI/CD

GitHub Actions workflow at `.github/workflows/ci.yml`:
- Runs on push to main and PRs
- Tests Python 3.8, 3.9, 3.10, 3.11
- Enforces 60% minimum coverage
- Runs linting (flake8) and security scan (bandit)

## Best Practices for Changes

1. **Before making changes:**
   - Read relevant test files
   - Understand the component's role in the architecture
   - Check if constants.py has relevant values

2. **When adding features:**
   - Add corresponding tests
   - Update docstrings with examples
   - Consider backward compatibility

3. **When fixing bugs:**
   - Add regression test first
   - Document the issue being fixed
   - Update CHANGELOG if significant

4. **Code quality:**
   - Line length: 100 characters max
   - Format with black
   - Type hints encouraged

---

*Last Updated: January 2026 | Framework v1.2.0*
