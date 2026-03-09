# Installation Guide

This guide provides detailed installation instructions for the Fabric Data Quality Framework.

## Prerequisites

- Python 3.10 or higher
- Microsoft Fabric workspace access (for Fabric deployment)
- Git (for cloning the repository)

## Installation Methods

### Method 1: Editable Package Installation (Recommended for Development)

This method installs the framework as an editable package, allowing you to modify the code and see changes immediately.

```bash
# Navigate to the project directory
cd /Workspace/2_DATA_QUALITY_LIBRARY

# Install in editable mode with development dependencies
pip install -e ".[dev]"
```

**Advantages:**
- Changes to the code are immediately available
- Easy to contribute back to the framework
- Best for active development

### Method 2: Direct Import (Recommended for Production)

This method uses the framework directly without installation, suitable for Fabric notebooks.

```python
# Add to your notebook's first cell
import sys
sys.path.insert(0, '/Workspace/2_DATA_QUALITY_LIBRARY')

from dq_framework import FabricDataQualityRunner, DataQualityValidator, ConfigLoader
```

**Advantages:**
- No installation required
- Works immediately in Fabric notebooks
- Easy to version control with your project

### Method 3: Standard Package Installation

Install as a standard Python package:

```bash
cd /Workspace/2_DATA_QUALITY_LIBRARY
pip install .
```

**Advantages:**
- Clean installation
- Works like any other Python package
- Good for production environments

### Method 4: Dependencies Only

Install just the dependencies without the framework (if copying code directly):

```bash
cd /Workspace/2_DATA_QUALITY_LIBRARY
pip install -r requirements.txt
```

## Verifying Installation

After installation, verify it works:

```python
# Test import
from dq_framework import FabricDataQualityRunner, DataQualityValidator, ConfigLoader

print("✅ Fabric Data Quality Framework installed successfully!")
```

## Installing in Different Environments

### Fabric Notebook Environment

1. **Upload to Workspace:**
   - Upload the `2_DATA_QUALITY_LIBRARY` folder to your Fabric workspace
   - Place it in a location accessible by your notebooks

2. **Use Direct Import:**
   ```python
   import sys
   sys.path.insert(0, '/Workspace/2_DATA_QUALITY_LIBRARY')
   from dq_framework import FabricDataQualityRunner
   ```

3. **Install Dependencies (if needed):**
   ```python
   # Run in notebook cell
   %pip install "great-expectations>=1.0.0,<2.0.0" pyyaml pandas
   ```

### Local Development Environment

1. **Clone or copy the project:**
   ```bash
   cd /your/local/path
   git clone <repository-url>
   cd 2_DATA_QUALITY_LIBRARY
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install in editable mode:**
   ```bash
   pip install -e ".[dev]"
   ```

### Azure DevOps Pipeline

Add to your `azure-pipelines.yml`:

```yaml
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.10'

- script: |
    pip install -r requirements.txt
    pip install -e .
  displayName: 'Install Data Quality Framework'

- script: |
    pytest tests/ -v
  displayName: 'Run Tests'
```

## Troubleshooting

### Issue: "Module not found" error

**Solution:**
```python
# Verify sys.path includes the framework directory
import sys
print(sys.path)

# Add the path if missing
sys.path.insert(0, '/Workspace/2_DATA_QUALITY_LIBRARY')
```

### Issue: Great Expectations import error

**Solution:**
```bash
# Install/upgrade Great Expectations (v2.0.0 requires GX 1.x)
pip install "great-expectations>=1.0.0,<2.0.0"
```

### Issue: YAML parsing error

**Solution:**
```bash
# Install/upgrade PyYAML
pip install --upgrade pyyaml
```

### Issue: PySpark not found (local development)

**Solution:**
```bash
# Install PySpark for local development
pip install pyspark
```

Note: In Fabric, PySpark is pre-installed.

## Updating the Framework

### For Editable Installation:
```bash
cd /Workspace/2_DATA_QUALITY_LIBRARY
git pull  # If using git
# Changes are immediately available
```

### For Standard Installation:
```bash
cd /Workspace/2_DATA_QUALITY_LIBRARY
git pull  # If using git
pip install --upgrade .
```

### For Direct Import:
Simply replace the files in your workspace.

## Uninstalling

```bash
pip uninstall fabric-data-quality
```

## Next Steps

After installation, proceed to:
- [Configuration Guide](CONFIGURATION_GUIDE.md) - Learn how to create validation configs
- [Fabric Integration Guide](FABRIC_INTEGRATION.md) - Integrate with Fabric notebooks
- [Usage Examples](../examples/usage_examples.py) - See practical examples

## Support

For issues or questions:
1. Check the [README](../README.md)
2. Review [troubleshooting section](#troubleshooting)
3. Contact the data engineering team
