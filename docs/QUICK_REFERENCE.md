# Fabric Data Quality Framework - Quick Reference

## CLI Commands (Profiler)

| Task | Command |
|------|---------|
| **Profile Single File** | `python scripts/profile_data.py data.csv` |
| **Profile Directory** | `python scripts/profile_data.py data_folder/` |
| **Parallel Processing** | `python scripts/profile_data.py data_folder/ --workers 4` |
| **Specify Output** | `python scripts/profile_data.py data.csv --output config/` |
| **Manual Sampling** | `python scripts/profile_data.py big_data.parquet --sample 50000` |
| **Help** | `python scripts/profile_data.py --help` |

## Quick Start

```python
# 1. Setup (first cell in notebook)
import sys
sys.path.insert(0, '/Workspace/fabric_data_quality')
from dq_framework import FabricDataQualityRunner

# 2. Validate DataFrame
runner = FabricDataQualityRunner("config.yml")
results = runner.validate_spark_dataframe(df)

# 3. Check results
if results["success"]:
    print("✅ Validation passed!")
else:
    print("❌ Validation failed!")
```

## Common Expectations

| Validation Type | Expectation | Example |
|----------------|-------------|---------|
| **Row count** | `expect_table_row_count_to_be_between` | `min_value: 1, max_value: null` |
| **Column exists** | `expect_column_to_exist` | `column: "id"` |
| **Not null** | `expect_column_values_to_not_be_null` | `column: "customer_id"` |
| **Unique** | `expect_column_values_to_be_unique` | `column: "email"` |
| **Type check** | `expect_column_values_to_be_of_type` | `column: "age", type_: "integer"` |
| **Value range** | `expect_column_values_to_be_between` | `column: "price", min_value: 0` |
| **In set** | `expect_column_values_to_be_in_set` | `column: "status", value_set: ["active", "inactive"]` |
| **Regex match** | `expect_column_values_to_match_regex` | `column: "email", regex: "^[^@]+@[^@]+$"` |
| **Date valid** | `expect_column_values_to_be_dateutil_parseable` | `column: "created_date"` |
| **Cross-column** | `expect_column_pair_values_A_to_be_greater_than_B` | `column_A: "end_date", column_B: "start_date"` |

## Config Template

```yaml
validation_name: "my_validation"
description: "What this validates"

data_source:
  type: "spark_dataframe"  # or "delta_table", "lakehouse"

expectations:
  - expectation_type: "expect_table_row_count_to_be_between"
    kwargs:
      min_value: 1
    meta:
      severity: "critical"
      description: "Table not empty"
      
  - expectation_type: "expect_column_values_to_not_be_null"
    kwargs:
      column: "id"
    meta:
      severity: "critical"

validation_settings:
  result_format: "COMPLETE"
  catch_exceptions: true

failure_handling:
  on_failure: "log"
```

## Data Source Types

### Spark DataFrame (Most Common)
```python
df = spark.read.table("my_table")
runner = FabricDataQualityRunner("config.yml")
results = runner.validate_spark_dataframe(df)
```

### Delta Table
```yaml
data_source:
  type: "delta_table"
  table_name: "bronze.my_table"
```
```python
results = runner.validate_delta_table("bronze.my_table")
```

### Lakehouse File
```yaml
data_source:
  type: "lakehouse"
  lakehouse_name: "MyLakehouse"
  file_path: "Files/data.parquet"
  file_format: "parquet"
```
```python
results = runner.validate_lakehouse_file(
    lakehouse_name="MyLakehouse",
    file_path="Files/data.parquet",
    file_format="parquet"
)
```

## Results Structure

```python
{
    "success": True/False,
    "validation_name": "my_validation",
    "statistics": {
        "evaluated_expectations": 10,
        "successful_expectations": 9,
        "unsuccessful_expectations": 1,
        "success_percent": 90.0
    },
    "results": [
        {
            "success": True/False,
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "id"},
            "meta": {
                "severity": "critical",
                "description": "..."
            }
        }
    ]
}
```

## Severity Levels

| Severity | Use Case | Recommended Action |
|----------|----------|-------------------|
| **critical** | Must pass, data unusable if fails | Raise error, stop pipeline |
| **high** | Important business rules | Alert team, log failure |
| **medium** | Standard quality checks | Log for monitoring |
| **low** | Nice-to-have checks | Info only |

## Layer-Specific Patterns

### Bronze Layer
```yaml
# Focus: Basic structure, raw data validation
expectations:
  - expect_table_row_count_to_be_between
  - expect_column_to_exist
  - expect_column_values_to_not_be_null  # for keys only

failure_handling:
  on_failure: "log"  # Preserve raw data
```

### Silver Layer
```yaml
# Focus: Business rules, data quality
expectations:
  - expect_column_values_to_be_unique
  - expect_column_values_to_be_in_set
  - expect_column_values_to_be_between
  - expect_column_pair_values_A_to_be_greater_than_B

failure_handling:
  on_failure: "log"  # or "raise" for critical
```

### Gold Layer
```yaml
# Focus: Business metrics, strict validation
expectations:
  - expect_column_sum_to_be_between
  - expect_column_mean_to_be_between
  - expect_column_values_to_be_between  # strict ranges

failure_handling:
  on_failure: "raise"  # Always fail on issues
```

## Common Patterns

### Pattern: Primary Key Validation
```yaml
expectations:
  - expectation_type: "expect_column_to_exist"
    kwargs: {column: "id"}
  - expectation_type: "expect_column_values_to_not_be_null"
    kwargs: {column: "id"}
  - expectation_type: "expect_column_values_to_be_unique"
    kwargs: {column: "id"}
```

### Pattern: Email Validation
```yaml
- expectation_type: "expect_column_values_to_match_regex"
  kwargs:
    column: "email"
    regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

### Pattern: Date Range
```yaml
- expectation_type: "expect_column_values_to_be_dateutil_parseable"
  kwargs: {column: "date"}
- expectation_type: "expect_column_min_to_be_between"
  kwargs:
    column: "date"
    min_value: "2020-01-01"
    parse_strings_as_datetimes: true
```

### Pattern: Categorical with Nulls Allowed
```yaml
- expectation_type: "expect_column_values_to_be_in_set"
  kwargs:
    column: "status"
    value_set: ["active", "inactive", "pending", null]
```

## Error Handling

### Basic
```python
if not results["success"]:
    print("Validation failed!")
```

### By Severity
```python
critical_failures = [
    r for r in results["results"] 
    if not r["success"] 
    and r.get("meta", {}).get("severity") == "critical"
]

if critical_failures:
    raise ValueError("Critical validation failures!")
```

### With Logging
```python
# Save to audit table
audit_df = spark.createDataFrame([{
    "timestamp": datetime.now(),
    "validation_name": results["validation_name"],
    "success": results["success"],
    "failed_checks": results["statistics"]["unsuccessful_expectations"]
}])
audit_df.write.mode("append").saveAsTable("monitoring.dq_audit")
```

## CLI Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Install as package
pip install -e .

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=dq_framework
```

## CLI Tools

### Data Profiler
```bash
# Profile single file
python scripts/profile_data.py data.csv --output config.yml

# Profile directory (batch mode)
python scripts/profile_data.py data_folder/ --output config_dir/

# Parallel processing (faster for many files)
python scripts/profile_data.py data_folder/ --output config_dir/ --workers 4
```

## Files Location

```
fabric_data_quality/
├── dq_framework/          # Core code
├── config_templates/      # Bronze/Silver/Gold templates
├── examples/              # Project-specific examples
│   ├── hss_incidents_example.yml
│   ├── aims_data_example.yml
│   └── aca_commercial_example.yml
├── tests/                 # Unit tests
└── docs/                  # Documentation
    ├── INSTALLATION.md
    ├── CONFIGURATION_GUIDE.md
    └── FABRIC_INTEGRATION.md
```

## Help & Resources

- **Full Documentation**: See `docs/` folder
- **Examples**: See `examples/usage_examples.py`
- **Templates**: See `config_templates/` folder
- **Great Expectations Docs**: https://docs.greatexpectations.io/

## Common Issues

| Issue | Solution |
|-------|----------|
| Module not found | Check `sys.path`, verify file location |
| Config not loading | Use absolute paths, check YAML syntax |
| Spark not available | Restart notebook kernel |
| Validation too slow | Sample large DataFrames first |
| Import errors | Install dependencies: `%pip install great-expectations pyyaml` |
