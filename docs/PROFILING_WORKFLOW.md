# Universal Data Profiling - Proper Workflow Guide

## Overview

The Data Profiler is a **universal, reusable tool** that works with ANY data source:
- ✅ CSV files (any encoding)
- ✅ Parquet files
- ✅ Excel files (.xlsx, .xls)
- ✅ JSON files
- ✅ Any pandas DataFrame

**Key Principle**: **Profile once, use forever** - Profiling is a ONE-TIME setup step per dataset/project.

## The Proper Workflow

### Step 1: Profile Your Data (ONE TIME ONLY)

When you start working with a new dataset, profile it ONCE to understand its structure:

```bash
# Activate environment
conda activate fabric-dq

# Profile a single file
python scripts/profile_data.py path/to/your/data.csv --output config/my_project_validation.yml

# Profile a directory of mixed files (generates multiple configs)
# Use --workers for parallel processing
python scripts/profile_data.py path/to/data_folder/ --output config/ --workers 4
```

**This generates:**
- Data quality assessment report
- Initial validation configuration (`.yml` file)
- Column-by-column analysis

### Step 2: Review & Enhance (ONE TIME ONLY)

Open the generated config file and enhance it with your business rules:

```yaml
# config/my_project_validation.yml
validation_name: my_project_validation
description: Auto-generated + business rules
expectations:
  # Auto-generated expectations
  - expectation_type: expect_column_to_exist
    kwargs:
      column: customer_id
  
  # ADD YOUR BUSINESS RULES HERE
  - expectation_type: expect_column_values_to_match_regex
    kwargs:
      column: email
      regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    meta:
      severity: high
      description: Email must be valid format
```

### Step 3: Save to Project Config (ONE TIME ONLY)

Move the enhanced config to your project:

```bash
# For HSS project
cp config/my_validation.yml ../full_stack_hss/config/data_quality/

# For AIMS project
cp config/my_validation.yml ../AIMS_LOCAL/config/

# For ACA project
cp config/my_validation.yml ../ACA_COMMERCIAL/config/
```

### Step 4: Use for ALL Future Runs (REPEATED)

In your ETL pipeline, notebooks, or scripts:

```python
from dq_framework import DataQualityValidator, ConfigLoader
import pandas as pd

# Load your data
df = pd.read_csv('data/new_batch_20251028.csv')

# Load the SAME config you created once
config = ConfigLoader().load('config/my_project_validation.yml')

# Validate (no re-profiling needed!)
validator = DataQualityValidator(config_dict=config)
results = validator.validate(df)

if not results['success']:
    print(f"Data quality issues found: {results['failed_checks']} failures")
    # Handle failures (log, alert, reject batch, etc.)
```

## Universal Profiler CLI

### Basic Usage

```bash
# Profile CSV file
python profile_data.py data/transactions.csv

# Profile Parquet file
python profile_data.py data/events.parquet

# Profile Excel file
python profile_data.py data/survey.xlsx

# Profile JSON file
python profile_data.py data/records.json
```

### Advanced Options

```bash
# Custom output location
python profile_data.py data.csv --output config/my_validation.yml

# Custom validation name
python profile_data.py data.csv --name "production_data_validation"

# Adjust null tolerance (percentage)
python profile_data.py data.csv --null-tolerance 10.0

# Set default severity
python profile_data.py data.csv --severity high

# Sample large files (for performance)
python profile_data.py huge_file.csv --sample 100000

# Just profile, don't generate config
python profile_data.py data.csv --profile-only

# Specify encoding for CSV
python profile_data.py data.csv --encoding utf-8
```

### Complete Example

```bash
# Profile CAUSEWAY data (ONE TIME)
python profile_data.py \
    sample_source_data/CAUSEWAY_combined_scr_2024.csv \
    --output config/causeway_validation.yml \
    --name "causeway_financial_validation" \
    --description "CAUSEWAY financial transactions validation" \
    --null-tolerance 50 \
    --severity medium \
    --sample 50000

# Output: config/causeway_validation.yml
# Now use this file for ALL future CAUSEWAY validations!
```

## Real-World Examples

### Example 1: HSS Incidents (First Time Setup)

```bash
# ONE TIME: Profile your HSS incidents data
python profile_data.py \
    ../full_stack_hss/data/incidents.parquet \
    --output ../full_stack_hss/config/incidents_validation.yml \
    --name "hss_incidents_validation" \
    --null-tolerance 5 \
    --severity high
```

**Then in your HSS ETL pipeline** (every run):
```python
from dq_framework import DataQualityValidator, ConfigLoader

# Load new incidents
df = load_daily_incidents()

# Validate using the SAME config
config = ConfigLoader().load('config/incidents_validation.yml')
validator = DataQualityValidator(config_dict=config)
results = validator.validate(df)
```

### Example 2: AIMS Data (First Time Setup)

```bash
# ONE TIME: Profile AIMS data
python profile_data.py \
    ../AIMS_LOCAL/data/aims_data.parquet \
    --output ../AIMS_LOCAL/config/aims_validation.yml \
    --name "aims_data_validation" \
    --null-tolerance 1 \
    --severity critical
```

**Then in your AIMS pipeline** (every run):
```python
# No profiling! Just validate with existing config
config = ConfigLoader().load('../AIMS_LOCAL/config/aims_validation.yml')
validator = DataQualityValidator(config_dict=config)
results = validator.validate(aims_df)
```

### Example 3: ACA Commercial (First Time Setup)

```bash
# ONE TIME: Profile SharePoint data
python profile_data.py \
    ../ACA_COMMERCIAL/sharepoint_data.csv \
    --output ../ACA_COMMERCIAL/config/sharepoint_validation.yml \
    --name "aca_sharepoint_validation" \
    --null-tolerance 10
```

**Then in your ACA migration scripts** (every run):
```python
# Validate each SharePoint file using same config
for file in sharepoint_files:
    df = pd.read_csv(file)
    results = validator.validate(df)  # Same config!
```

## When to Re-Profile

**Re-profiling is only needed when:**

1. **Schema Changes**: New columns added, columns removed
2. **Data Type Changes**: Column types fundamentally change
3. **Business Rule Changes**: New validation requirements
4. **Data Source Changes**: Migrating to different data source

**Don't re-profile for:**
- ❌ Each new data batch
- ❌ Daily/weekly data loads
- ❌ Different date ranges of same data
- ❌ Incremental updates

## Directory Structure Recommendation

```
your_project/
├── config/
│   └── data_quality/
│       ├── source1_validation.yml      # Created once, used forever
│       ├── source2_validation.yml      # Created once, used forever
│       └── source3_validation.yml      # Created once, used forever
├── data/
│   ├── batch_20251028.csv              # New data arrives
│   ├── batch_20251029.csv              # New data arrives
│   └── batch_20251030.csv              # New data arrives
└── pipelines/
    └── validate_data.py                # Uses config/ files
```

**ETL Pipeline** (`pipelines/validate_data.py`):
```python
from dq_framework import DataQualityValidator, ConfigLoader

def validate_batch(batch_file: str) -> bool:
    """Validate any batch using the same config."""
    df = pd.read_csv(batch_file)
    
    # Load config created ONCE during setup
    config = ConfigLoader().load('config/data_quality/source1_validation.yml')
    
    # Validate
    validator = DataQualityValidator(config_dict=config)
    results = validator.validate(df)
    
    return results['success']

# Use for ANY batch
validate_batch('data/batch_20251028.csv')
validate_batch('data/batch_20251029.csv')
validate_batch('data/batch_20251030.csv')
```

## Summary

### ✅ DO:
- Profile once when starting with new data source
- Save the generated config
- Enhance with business rules
- Use same config for all future runs
- Version control your config files

### ❌ DON'T:
- Re-profile for every data batch
- Embed profiling in ETL pipelines
- Generate new configs for same data source
- Profile the same data repeatedly

### 🎯 Key Benefits:
1. **Performance**: Profiling is slow, validation is fast
2. **Consistency**: Same rules applied to all batches
3. **Maintainability**: Config files are your source of truth
4. **Flexibility**: Works with any data source, any project
5. **Reusability**: One profiler for all your data needs

---

**The profiler is universal and flexible. Profile once, validate forever.**
