# Development Environment Setup Guide

Complete guide for setting up the Fabric Data Quality Framework development environment.

## Quick Start

### Option 1: Conda Environment (Recommended)

```bash
# Create environment
conda env create -f environment.yml

# Activate environment
conda activate fabric-dq

# Verify installation
python -c "import dq_framework; print('✅ Framework installed!')"
```

### Option 2: Virtual Environment with pip

```bash
# Create virtual environment
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

### Option 3: Development Setup with All Tools

```bash
# Using Makefile (easiest)
make install-conda  # Create conda environment
conda activate fabric-dq
make dev-setup      # Install dev dependencies and pre-commit hooks

# Or manually
conda env create -f environment.yml
conda activate fabric-dq
pip install -r requirements-dev.txt
pip install -e .
pre-commit install
```

## Environment Details

### Python Version
- **Minimum**: Python 3.8
- **Recommended**: Python 3.10
- **Maximum tested**: Python 3.11

### Core Dependencies
- `great-expectations>=0.18.0` - Data validation framework
- `pandas>=1.5.0` - Data manipulation
- `pyyaml>=6.0` - Configuration parsing
- `pyarrow>=12.0.0` - Parquet support

### Development Dependencies
- `pytest` - Testing framework
- `black` - Code formatter
- `flake8` - Linter
- `mypy` - Type checker
- `pre-commit` - Git hooks

## Makefile Commands

The project includes a comprehensive Makefile for common tasks:

```bash
# Show all available commands
make help

# Installation
make install          # Install package in development mode
make install-dev      # Install with development dependencies
make install-conda    # Create conda environment

# Testing
make test            # Run all tests
make test-cov        # Run tests with coverage report
make test-fast       # Run tests (skip slow tests)

# Code Quality
make lint            # Run all linters
make format          # Format code with black and isort
make format-check    # Check formatting without changes
make security        # Run security checks

# Pre-commit
make pre-commit-install  # Install pre-commit hooks
make pre-commit-run      # Run hooks on all files

# Documentation
make docs            # Build documentation
make docs-serve      # Serve documentation locally

# Cleaning
make clean           # Clean build artifacts
make clean-all       # Deep clean including environment

# Development Workflow
make dev-setup       # Complete development setup
make check-all       # Run all quality checks
make ci              # Run full CI pipeline
```

## Verification Steps

After setting up your environment, run these verification steps:

### 1. Test Python Installation
```bash
python --version  # Should be 3.8 or higher
```

### 2. Test Package Import
```bash
python -c "from dq_framework import DataQualityValidator; print('✅ Core module works')"
python -c "from dq_framework import FabricDataQualityRunner; print('✅ Fabric connector works')"
python -c "from dq_framework import ConfigLoader; print('✅ Config loader works')"
```

### 3. Test Dependencies
```bash
python -c "import great_expectations; print('✅ Great Expectations:', great_expectations.__version__)"
python -c "import pandas; print('✅ Pandas:', pandas.__version__)"
python -c "import yaml; print('✅ PyYAML works')"
```

### 4. Run Tests
```bash
# Run all tests
make test

# Or with pytest directly
pytest tests/ -v

# Check coverage
make test-cov
```

### 5. Check Code Quality
```bash
# Run linters
make lint

# Format code
make format

# Security check
make security
```

## Troubleshooting

### Issue: Conda environment creation fails

**Solution:**
```bash
# Update conda
conda update -n base -c defaults conda

# Clear conda cache
conda clean --all

# Try again
conda env create -f environment.yml --force
```

### Issue: Great Expectations import error

**Solution:**
```bash
# Reinstall Great Expectations
pip uninstall great-expectations -y
pip install great-expectations>=0.18.0
```

### Issue: Module not found errors

**Solution:**
```bash
# Ensure you're in the correct environment
conda activate fabric-dq  # or source venv/bin/activate

# Reinstall package in development mode
pip install -e .

# Verify PYTHONPATH
python -c "import sys; print('\n'.join(sys.path))"
```

### Issue: Pre-commit hooks failing

**Solution:**
```bash
# Update pre-commit
pip install --upgrade pre-commit

# Clean and reinstall hooks
pre-commit clean
pre-commit install

# Run with verbose output
pre-commit run --all-files --verbose
```

### Issue: Test failures

**Solution:**
```bash
# Check test environment
pytest --version
python -m pytest --version

# Run tests with verbose output
pytest tests/ -vv --tb=long

# Run specific test
pytest tests/test_config_loader.py -v
```

## Best Practices

### 1. Always Use Virtual Environments
Never install packages globally. Always use conda environment or venv.

### 2. Keep Dependencies Updated
```bash
# Check outdated packages
pip list --outdated

# Update environment
conda env update -f environment.yml --prune
```

### 3. Run Tests Before Committing
```bash
# Quick check
make check-all

# Or use pre-commit
make pre-commit-run
```

### 4. Format Code Consistently
```bash
# Auto-format before committing
make format

# Or configure your IDE to use black and isort
```

### 5. Document Changes
- Update CHANGELOG.md for significant changes
- Update documentation for API changes
- Add docstrings for new functions/classes

## IDE Setup

### VS Code
Install recommended extensions:
- Python (Microsoft)
- Pylance
- Python Test Explorer
- Black Formatter
- isort

Add to `.vscode/settings.json`:
```json
{
    "python.linting.enabled": true,
    "python.linting.flake8Enabled": true,
    "python.linting.pylintEnabled": true,
    "python.formatting.provider": "black",
    "python.formatting.blackArgs": ["--line-length=100"],
    "python.sortImports.args": ["--profile=black", "--line-length=100"],
    "python.testing.pytestEnabled": true,
    "python.testing.unittestEnabled": false,
    "editor.formatOnSave": true
}
```

### PyCharm
1. File → Settings → Project → Python Interpreter
2. Add conda environment or virtualenv
3. Enable black formatter: Settings → Tools → Black
4. Enable pytest: Settings → Tools → Python Integrated Tools

## Environment Variables

Optional environment variables for configuration:

```bash
# Set in ~/.bashrc or ~/.zshrc

# Great Expectations
export GE_HOME="${HOME}/.great_expectations"

# Fabric (if running locally)
export FABRIC_WORKSPACE="/path/to/workspace"

# Testing
export PYTEST_ADDOPTS="--cov --cov-report=html"
```

## Next Steps

After setup:

1. **Read the documentation**
   - `README.md` - Project overview
   - `QUICK_REFERENCE.md` - Common operations
   - `docs/` - Detailed guides

2. **Run the examples**
   ```bash
   # Try the demo
   cd examples
   python demo_causeway_validation.py
   ```

3. **Create your first validation**
   - Copy a template from `config_templates/`
   - Customize for your data
   - Test with your dataset

4. **Integrate with your project**
   - See `examples/usage_examples.py` for patterns
   - Review project-specific examples (HSS, AIMS, ACA)
   - Read `docs/FABRIC_INTEGRATION.md`

## Getting Help

- Check the documentation in `docs/`
- Review examples in `examples/`
- See troubleshooting section above
- Contact the data engineering team

## Contributing

If contributing to the framework:

1. Fork and clone the repository
2. Set up development environment: `make dev-setup`
3. Create a feature branch
4. Make changes and add tests
5. Run quality checks: `make check-all`
6. Submit pull request

## License

MIT License - See LICENSE file for details
