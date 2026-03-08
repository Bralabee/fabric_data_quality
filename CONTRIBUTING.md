# Contributing to Fabric Data Quality Framework

Thank you for your interest in contributing to the Fabric Data Quality Framework! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Documentation](#documentation)

## Code of Conduct

Please be respectful and constructive in all interactions. We aim to maintain a welcoming environment for all contributors.

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Conda (recommended) or virtualenv
- Git
- Access to the repository

### Issues

- Check existing issues before creating a new one
- Use issue templates when available
- Include reproduction steps for bugs
- Be specific about feature requests

## Development Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd 2_DATA_QUALITY_LIBRARY
```

### 2. Create and Activate Environment

**Using Conda (Recommended):**
```bash
# Create environment from file
conda env create -f environment.yml

# Activate
conda activate fabric-dq
```

**Using pip:**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e ".[dev]"
```

### 4. Verify Setup

```bash
# Run tests
pytest tests/ -v

# Check imports work
python -c "from dq_framework import DataQualityValidator; print('OK')"
```

## Making Changes

### Branch Naming

Use descriptive branch names:
- `feature/add-batch-profiling`
- `fix/null-handling-bug`
- `docs/update-api-reference`
- `refactor/consolidate-utils`

### Commit Messages

Follow conventional commit format:
```
type(scope): short description

Longer description if needed.

Fixes #123
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

Examples:
- `feat(profiler): add support for custom thresholds`
- `fix(validator): handle empty dataframes gracefully`
- `docs(readme): update installation instructions`

## Testing

### Running Tests

```bash
# All tests with coverage
pytest tests/ -v --cov=dq_framework --cov-report=term-missing

# Specific test file
pytest tests/test_validator.py -v

# Specific test function
pytest tests/test_validator.py::test_validate_with_valid_config -v

# Run with verbose output on failure
pytest tests/ -v --tb=short
```

### Writing Tests

1. **Location**: Add tests to the appropriate file in `tests/`
2. **Naming**: Use `test_` prefix for test functions
3. **Coverage**: Aim for >80% coverage on new code
4. **Fixtures**: Use shared fixtures from `conftest.py`

Example test:
```python
import pytest
import pandas as pd
from dq_framework import DataQualityValidator

def test_validator_with_valid_config(tmp_path, sample_config):
    """Test validator successfully validates data with valid config."""
    # Arrange
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    validator = DataQualityValidator(config_path=sample_config)
    
    # Act
    results = validator.validate(df)
    
    # Assert
    assert results["success"] is True
    assert results["statistics"]["evaluated_expectations"] > 0
```

### Test Coverage Requirements

- Minimum: 60% overall coverage
- New features: Must include tests
- Bug fixes: Should include regression test

## Pull Request Process

### 1. Before Submitting

- [ ] Tests pass locally (`pytest tests/ -v`)
- [ ] Coverage meets threshold (`--cov-fail-under=60`)
- [ ] Code formatted with black (`black dq_framework/ tests/`)
- [ ] Linting passes (`flake8 dq_framework/ tests/`)
- [ ] Documentation updated if needed

### 2. PR Description

Include:
- Summary of changes
- Related issue number(s)
- Testing performed
- Any breaking changes

### 3. Review Process

1. Automated CI checks must pass
2. At least one reviewer approval required
3. Address reviewer feedback
4. Squash commits if requested

## Coding Standards

### Style Guide

- **Line Length**: 100 characters maximum
- **Formatter**: black with default settings
- **Linter**: flake8
- **Type Hints**: Encouraged but not required

### Imports

```python
# Standard library
import os
from pathlib import Path

# Third party
import pandas as pd
import numpy as np
from great_expectations.core import ExpectationSuite

# Local
from dq_framework.constants import DEFAULT_SEVERITY_THRESHOLDS
from dq_framework.utils import is_fabric_environment
```

### Docstrings

Use Google-style docstrings:
```python
def validate(self, df: pd.DataFrame, context: Optional[str] = None) -> Dict[str, Any]:
    """Validate a DataFrame against configured expectations.
    
    Args:
        df: The DataFrame to validate.
        context: Optional context name for results.
        
    Returns:
        Dictionary containing validation results with keys:
        - success (bool): Whether validation passed
        - statistics (dict): Validation statistics
        - results (list): Individual expectation results
        
    Raises:
        ValueError: If DataFrame is empty or config is invalid.
        
    Example:
        >>> validator = DataQualityValidator("config.yml")
        >>> results = validator.validate(df)
        >>> print(results["success"])
        True
    """
```

### Error Handling

```python
# Good: Specific exception with context
try:
    config = load_config(path)
except FileNotFoundError:
    logger.error(f"Config file not found: {path}")
    raise ValueError(f"Configuration file does not exist: {path}")

# Bad: Bare except or generic handling
try:
    config = load_config(path)
except:
    pass  # Don't do this!
```

## Documentation

### When to Update

- New features: Add to README and API Reference
- Changed behavior: Update relevant docs
- Breaking changes: Highlight in CHANGELOG

### Documentation Files

| File | Purpose |
|------|---------|
| `README.md` | Project overview and quick start |
| `docs/API_REFERENCE.md` | Complete API documentation |
| `docs/FABRIC_DEPLOYMENT_GUIDE.md` | Fabric-specific setup |
| `CONTRIBUTING.md` | This file |
| `.github/COPILOT.md` | AI assistant instructions |

### Building Documentation

Documentation is in Markdown format. Preview locally using any Markdown viewer or:
```bash
# Using grip (GitHub Readme Instant Preview)
pip install grip
grip README.md
```

## Questions?

- Check existing documentation
- Search closed issues
- Open a new issue with the question label

---

*Thank you for contributing! 🎉*
