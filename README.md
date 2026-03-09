# Fabric Data Quality Framework

A reusable, configurable data quality framework using Great Expectations, designed for Microsoft Fabric environments and usable across all your HS2 projects.

## What's New in v2.0.0
- **Great Expectations 1.x Migration**: Full rewrite of validation engine for GX 1.x API (DataSources, ExpectationSuite, ValidationDefinition)
- **Python 3.10+**: Minimum Python version raised from 3.8 to 3.10
- **Unified Linting**: Replaced black/flake8/isort/pylint with ruff
- **Storage Abstraction**: Pluggable result stores (JSON, Lakehouse)
- **Alert Infrastructure**: Shared alerting layer with formatting, config, dispatcher, and circuit breaker
- **65% Test Coverage**: 213+ tests passing

## 🧭 Environment & Path Support

- **Local Python**: use local filesystem paths (e.g., `config/my_table.yml`, `/tmp/data.parquet`).
- **Microsoft Fabric**: `abfss://...` and `Files/...` paths require Fabric utilities (`mssparkutils`). Outside Fabric these paths are not supported and will fail fast.

## 🎯 Purpose

This standalone framework provides data quality validation capabilities that can be used by:
- `full_stack_hss` - Incident data analysis
- `AIMS_LOCAL` - AIMS data processing
- `ACA_COMMERCIAL` - Commercial data pipelines
- Any other project in your workspace

## 📁 Project Structure

```
2_DATA_QUALITY_LIBRARY/
├── README.md                          # This file
├── requirements.txt                   # Dependencies
│
├── dq_framework/                      # Core framework package
│   ├── __init__.py                   # Package exports
│   ├── validator.py                  # Core validation engine
│   ├── fabric_connector.py           # MS Fabric integration
│   ├── config_loader.py              # YAML configuration loader
│   ├── data_profiler.py              # Data profiling engine
│   ├── batch_profiler.py             # Parallel batch profiling
│   ├── loader.py                     # Robust data loading (local/ABFSS)
│   └── utils.py                      # File system utilities
│
├── scripts/                           # Utility scripts
│   ├── profile_data.py               # Data profiling CLI tool
│   └── activate_and_test.sh          # Environment setup & test
│
├── config_templates/                  # Reusable YAML templates
│   ├── bronze_layer_template.yml     # Raw data validation
│   ├── silver_layer_template.yml     # Cleaned data validation
│   ├── gold_layer_template.yml       # Business logic validation
│   └── custom_template.yml           # Blank template
│
├── examples/                          # Project-specific examples
│   ├── hss_incidents_example.yml     # HSS project
│   ├── aims_data_example.yml         # AIMS project
│   ├── aca_commercial_example.yml    # ACA project
│   └── usage_examples.py             # Code examples
│
├── tests/                             # Unit tests
│   ├── test_validator.py
│   └── test_config_loader.py
│
└── docs/                              # Documentation
    ├── CONFIGURATION_GUIDE.md
    ├── FABRIC_INTEGRATION.md
    ├── FABRIC_ETL_INTEGRATION.md     # Complete ETL integration guide
    ├── FABRIC_QUICK_START.md         # 5-minute Fabric setup
    ├── PROFILING_WORKFLOW.md         # Profiling guide
    └── ...                           # Other documentation
```

## 🚀 Quick Start

### 1. Setup (One Time)

```bash
# Activate the conda environment
conda activate fabric-dq

# Verify installation
python -c "from dq_framework import DataProfiler; print('✅ Ready')"
```

### 2. Profile Your Data (One Time Per Dataset)

```bash
# Profile any data source (CSV, Parquet, Excel, JSON)
python scripts/profile_data.py path/to/your/data.csv \
    --output config/my_validation.yml \
    --null-tolerance 10 \
    --severity medium

# Profile a directory of mixed files (Batch Mode)
# Use --workers to speed up processing with parallel execution
python scripts/profile_data.py path/to/data_folder/ --output config/ --workers 4

# Example: Profile CAUSEWAY data
python scripts/profile_data.py sample_source_data/CAUSEWAY_combined_scr_2024.csv \
    --output config/causeway_validation.yml \
    --sample 50000
```

### 3. Advanced Features

- **Parallel Processing**: Use `--workers N` to profile multiple files simultaneously.
- **Smart Sampling**: Automatically limits large files (>500MB) to 100k rows to prevent memory crashes.
- **Efficient Parquet Reading**: Uses `pyarrow` batch reading for massive parquet files.
- **Batch Mode**: Point to a directory to profile all supported files (CSV, Parquet, Excel, JSON) at once.
- **Fabric Native Support**: Works directly with `abfss://` paths in MS Fabric environments.

### 4. Enhance Config (One Time Per Dataset)

Review and add business rules to `config/my_validation.yml`:

```yaml
expectations:
  # Auto-generated rules
  - expectation_type: expect_column_to_exist
    kwargs: {column: customer_id}
  
  # Add your business rules
  - expectation_type: expect_column_values_to_be_unique
    kwargs: {column: customer_id}
    meta: {severity: critical, description: "Customer IDs must be unique"}
```

## 📦 Deployment to Microsoft Fabric

To use this framework as a standard library in Fabric:

1.  **Build the Wheel**:
    ```bash
    python -m build --wheel --no-isolation
    ```
    This creates `dist/fabric_data_quality-2.0.0-py3-none-any.whl`.

2.  **Upload to Fabric**:
    *   Go to your Fabric Workspace -> **Manage environments**.
    *   Select your environment -> **Custom libraries** -> **Upload**.
    *   Select the `.whl` file.
    *   **Publish** the environment.

3.  **Use in Notebooks**:
    ```python
    from dq_framework import FabricDataQualityRunner
    ```

## 🚀 Quick Start (Local)

### 1. Setup (One Time)

```python
from dq_framework import DataQualityValidator, ConfigLoader
import pandas as pd

# Load new data
df = pd.read_csv('new_data_batch.csv')

# Validate using config created once
config = ConfigLoader().load('config/my_validation.yml')
validator = DataQualityValidator(config_dict=config)
results = validator.validate(df)

print(f"Success: {results['success']}")
```

**Key Principle**: Profile once, validate forever!

## 🔍 Universal Data Profiler

The framework includes a **universal data profiler** that works with any data source:

### Supported Formats
- ✅ CSV (auto-detects encoding)
- ✅ Parquet
- ✅ Excel (.xlsx, .xls)
- ✅ JSON
- ✅ Any pandas DataFrame

### CLI Usage

```bash
# Basic profiling
python scripts/profile_data.py data/file.csv

# With all options
python scripts/profile_data.py data/file.csv \
    --output config/validation.yml \
    --name "my_validation" \
    --null-tolerance 5.0 \
    --severity high \
    --sample 100000

# Just profile (no config generation)
python scripts/profile_data.py data/file.csv --profile-only
```

See **[PROFILING_WORKFLOW.md](docs/PROFILING_WORKFLOW.md)** for complete guide.

#### Option A: Install as editable package (Recommended for development)
```bash
cd /home/sanmi/Documents/HS2/HS2_PROJECTS_2025/2_DATA_QUALITY_LIBRARY
pip install -e .
```

#### Option B: Direct import (Add to Python path)
```python
import sys
sys.path.append('/home/sanmi/Documents/HS2/HS2_PROJECTS_2025/2_DATA_QUALITY_LIBRARY')
```

#### Option C: Install dependencies only
```bash
pip install -r requirements.txt
```

### 2. Basic Usage

#### In ANY project (e.g., full_stack_hss):

```python
from dq_framework import DataQualityValidator

# Load your data
import pandas as pd
df = pd.read_parquet('data/my_data.parquet')

# Create validator with config
validator = DataQualityValidator(
    config_path='/home/sanmi/Documents/HS2/HS2_PROJECTS_2025/2_DATA_QUALITY_LIBRARY/config_templates/bronze_layer_template.yml'
)

# Validate
results = validator.validate(df)

# Check results
if results['success']:
    print("✅ Data quality checks passed!")
else:
    print(f"❌ {results['failed_checks']} checks failed")
```

### 3. MS Fabric Usage

```python
from dq_framework import FabricDataQualityRunner

# Initialize for Fabric
runner = FabricDataQualityRunner(
    config_path="Files/dq_configs/my_table_config.yml"
)

# Validate Delta table
results = runner.validate_delta_table("my_table_name")

# Handle results
if not results['success']:
    runner.handle_failure(results, action="alert")
```

## 🔧 Integration Guide

### Using in `full_stack_hss` project:

```python
# In full_stack_hss/src/transform/transform.py
import sys
sys.path.append('/home/sanmi/Documents/HS2/HS2_PROJECTS_2025/2_DATA_QUALITY_LIBRARY')

from dq_framework import DataQualityValidator

def transform_with_quality_checks():
    # Your existing code
    df = load_data()
    
    # Add DQ check
    validator = DataQualityValidator(
        config_path='../../2_DATA_QUALITY_LIBRARY/examples/hss_incidents_example.yml'
    )
    results = validator.validate(df)
    
    if not results['success']:
        logger.warning(f"Data quality issues detected: {results['summary']}")
    
    # Continue transformation
    df_transformed = transform(df)
    return df_transformed
```

### Using in `AIMS_LOCAL` project:

```python
# In AIMS_LOCAL/src/your_script.py
import sys
sys.path.append('/home/sanmi/Documents/HS2/HS2_PROJECTS_2025/2_DATA_QUALITY_LIBRARY')

from dq_framework import DataQualityValidator

# Use AIMS-specific config
validator = DataQualityValidator(
    config_path='../../2_DATA_QUALITY_LIBRARY/examples/aims_data_example.yml'
)

df_aims = pd.read_parquet('data/aims_data.parquet')
results = validator.validate(df_aims)
```

### Using in `ACA_COMMERCIAL` project:

```python
# In ACA_COMMERCIAL notebooks
import sys
sys.path.append('/home/sanmi/Documents/HS2/HS2_PROJECTS_2025/2_DATA_QUALITY_LIBRARY')

from dq_framework import DataQualityValidator

validator = DataQualityValidator(
    config_path='../../2_DATA_QUALITY_LIBRARY/examples/aca_commercial_example.yml'
)

results = validator.validate(df_commercial)
```

## 📝 Creating Custom Configurations

### Step 1: Copy a template

```bash
cp config_templates/bronze_layer_template.yml my_configs/my_data_checks.yml
```

### Step 2: Customize expectations

```yaml
# my_configs/my_data_checks.yml
data_source:
  name: "my_data_source"
  description: "My custom data validation"

expectations:
  - expectation_type: "expect_column_values_to_not_be_null"
    kwargs:
      column: "id"
    meta:
      severity: "critical"
  
  - expectation_type: "expect_column_values_to_be_unique"
    kwargs:
      column: "email"
```

### Step 3: Use in your project

```python
validator = DataQualityValidator(
    config_path='path/to/my_data_checks.yml'
)
results = validator.validate(df)
```

## 🎨 Configuration Templates Available

### 1. Bronze Layer Template
- Basic schema validation
- Row count checks
- Null checks for critical columns
- **Use for:** Raw data landing validation

### 2. Silver Layer Template
- Data type validation
- Format validation (emails, dates, etc.)
- Range checks
- **Use for:** Cleaned/transformed data

### 3. Gold Layer Template
- Business rule validation
- Aggregation checks
- Cross-column validation
- **Use for:** Final business-ready data

### 4. Custom Template
- Blank template with examples
- **Use for:** Your specific needs

## 📊 Features

✅ **Configurable** - YAML-based rules, no code changes needed  
✅ **Reusable** - One framework, multiple projects  
✅ **Fabric-Native** - Works with Spark DataFrames and Delta tables  
✅ **Flexible** - Severity levels, sampling, smart error handling  
✅ **Documented** - Examples for each project type  

## 📚 Documentation

- **[Installation Guide](docs/INSTALLATION.md)** - Setup instructions
- **[Configuration Guide](docs/CONFIGURATION_GUIDE.md)** - How to create configs
- **[Fabric Integration](docs/FABRIC_INTEGRATION.md)** - MS Fabric specific guidance
- **[Examples](examples/usage_examples.py)** - Code examples

## 📚 Documentation

### For MS Fabric Users

- **[FABRIC_QUICK_START.md](docs/FABRIC_QUICK_START.md)** - 5-minute setup guide for MS Fabric
- **[FABRIC_ETL_INTEGRATION.md](docs/FABRIC_ETL_INTEGRATION.md)** - Complete ETL pipeline integration
- **[fabric_etl_example.py](examples/fabric_etl_example.py)** - Copy-paste Fabric notebook code

### For All Users

- **[PROFILING_WORKFLOW.md](docs/PROFILING_WORKFLOW.md)** - "Profile once, validate forever" workflow
- **[CONFIGURATION_GUIDE.md](docs/CONFIGURATION_GUIDE.md)** - YAML configuration reference
- **[FABRIC_INTEGRATION.md](docs/FABRIC_INTEGRATION.md)** - PySpark integration patterns
- **[QUICK_REFERENCE.md](docs/QUICK_REFERENCE.md)** - One-page API cheat sheet

## 🧪 Testing

```bash
# Run all tests
pytest tests/
```

## 🔄 Version History

- **v2.0.0** (2026-03-07) - Great Expectations 1.x migration, storage abstraction, alert infrastructure
- **v1.2.0** (2026-01-19) - Enhanced test coverage (~70%, 213+ tests), stability improvements
- **v1.1.3** (2025-12-06) - Added configurable global thresholds and ABFSS support
- **v1.1.0** (2025-10-28) - Added MS Fabric ETL integration guides
- **v1.0.0** (2025-10-28) - Initial standalone framework with universal profiler

## 💻 Code Example: Using Thresholds

```python
from dq_framework import DataQualityValidator

# Initialize
validator = DataQualityValidator("config/my_checks.yml")

# Validate with a custom 95% threshold
# (Overrides config file settings if provided)
result = validator.validate(df, threshold=95.0)

if result['success']:
    print(f"Passed! Score: {result['success_rate']}%")
else:
    print(f"Failed. Score: {result['success_rate']}%")
```

## 🤝 Contributing

To add new features or templates:
1. Create your feature in `dq_framework/`
2. Add tests in `tests/`
3. Add examples in `examples/`
4. Update this README

## 📞 Support

For questions or issues:
- Check examples in `examples/`
- Review configuration templates in `config_templates/`
- See detailed docs in `docs/`

---

**Framework Owner:** Data Engineering Team  
**Last Updated:** March 2026
**Status:** Production Ready
