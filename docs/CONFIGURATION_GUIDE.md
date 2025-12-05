# Configuration Guide

This guide explains how to create and customize data quality validation configurations using YAML files.

## Configuration Structure

A validation configuration consists of:

1. **Validation Metadata** - Name, description, metadata
2. **Data Source Configuration** - Where to find the data
3. **Expectations** - Data quality rules to validate
4. **Validation Settings** - How to run the validation
5. **Failure Handling** - What to do when validation fails

## Basic Configuration Template

```yaml
# Validation metadata
validation_name: "my_validation"
description: "Description of what this validation does"

# Data source
data_source:
  type: "delta_table"  # or "lakehouse", "spark_dataframe"
  table_name: "schema.table_name"

# Expectations (data quality rules)
expectations:
  - expectation_type: "expect_table_row_count_to_be_between"
    kwargs:
      min_value: 1
      max_value: null
    meta:
      severity: "critical"
      description: "Table must not be empty"

# Validation settings
validation_settings:
  result_format: "COMPLETE"
  catch_exceptions: true

# Failure handling
failure_handling:
  on_failure: "log"  # or "raise", "continue"

# Metadata
metadata:
  owner: "your_team"
  last_updated: "2025-01-01"
```

## Data Source Configuration

### Option 1: Delta Table

```yaml
data_source:
  type: "delta_table"
  table_name: "bronze.raw_data"
```

### Option 2: Lakehouse File

```yaml
data_source:
  type: "lakehouse"
  lakehouse_name: "MyLakehouse"
  file_path: "Files/bronze/data.parquet"
  file_format: "parquet"  # or "csv", "delta", "json"
  read_options:
    header: true
    inferSchema: false
```

### Option 3: Spark DataFrame (Passed in Code)

```yaml
data_source:
  type: "spark_dataframe"
  # DataFrame will be passed to validate_spark_dataframe() method
```

## Expectations (Data Quality Rules)

Expectations are the core of data quality validation. Each expectation tests a specific aspect of your data.

### Table-Level Expectations

#### Row Count Validation

```yaml
- expectation_type: "expect_table_row_count_to_be_between"
  kwargs:
    min_value: 1
    max_value: 1000000
  meta:
    severity: "critical"
    description: "Validate row count is within expected range"
```

#### Column Count Validation

```yaml
- expectation_type: "expect_table_column_count_to_equal"
  kwargs:
    value: 10
  meta:
    severity: "high"
    description: "Table should have exactly 10 columns"
```

### Column Existence

```yaml
- expectation_type: "expect_column_to_exist"
  kwargs:
    column: "customer_id"
  meta:
    severity: "critical"
    description: "Customer ID column must exist"
```

### Null/Completeness Checks

```yaml
- expectation_type: "expect_column_values_to_not_be_null"
  kwargs:
    column: "customer_id"
  meta:
    severity: "critical"
    description: "Customer ID cannot be null"
```

### Uniqueness Checks

```yaml
- expectation_type: "expect_column_values_to_be_unique"
  kwargs:
    column: "customer_id"
  meta:
    severity: "critical"
    description: "Customer ID must be unique"
```

### Data Type Validation

```yaml
- expectation_type: "expect_column_values_to_be_of_type"
  kwargs:
    column: "created_date"
    type_: "timestamp"  # Options: string, integer, float, boolean, timestamp, date
  meta:
    severity: "high"
    description: "Created date must be timestamp type"
```

### Value Range Validation

```yaml
- expectation_type: "expect_column_values_to_be_between"
  kwargs:
    column: "age"
    min_value: 0
    max_value: 120
    mostly: 0.99  # Allow 1% outliers
  meta:
    severity: "high"
    description: "Age should be between 0 and 120"
```

### Categorical Validation

```yaml
- expectation_type: "expect_column_values_to_be_in_set"
  kwargs:
    column: "status"
    value_set: ["active", "inactive", "pending"]
  meta:
    severity: "high"
    description: "Status must be one of the allowed values"
```

### String Pattern Validation

```yaml
- expectation_type: "expect_column_values_to_match_regex"
  kwargs:
    column: "email"
    regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
  meta:
    severity: "medium"
    description: "Email must match valid format"
```

### String Length Validation

```yaml
- expectation_type: "expect_column_value_lengths_to_be_between"
  kwargs:
    column: "description"
    min_value: 10
    max_value: 500
  meta:
    severity: "medium"
    description: "Description length must be between 10 and 500 characters"
```

### Date Validation

```yaml
- expectation_type: "expect_column_values_to_be_dateutil_parseable"
  kwargs:
    column: "created_date"
  meta:
    severity: "high"
    description: "Created date must be valid date format"
```

### Cross-Column Validation

```yaml
- expectation_type: "expect_column_pair_values_A_to_be_greater_than_B"
  kwargs:
    column_A: "end_date"
    column_B: "start_date"
    or_equal: true
  meta:
    severity: "high"
    description: "End date must be after or equal to start date"
```

### Compound Key Uniqueness

```yaml
- expectation_type: "expect_compound_columns_to_be_unique"
  kwargs:
    column_list: ["customer_id", "order_date"]
  meta:
    severity: "high"
    description: "Combination of customer_id and order_date must be unique"
```

### Statistical Validations

```yaml
# Mean validation
- expectation_type: "expect_column_mean_to_be_between"
  kwargs:
    column: "score"
    min_value: 0
    max_value: 100
  meta:
    severity: "medium"
    description: "Average score should be between 0 and 100"

# Standard deviation validation
- expectation_type: "expect_column_stdev_to_be_between"
  kwargs:
    column: "metric"
    min_value: 0
    max_value: 50
  meta:
    severity: "low"
    description: "Standard deviation within expected range"

# Sum validation
- expectation_type: "expect_column_sum_to_be_between"
  kwargs:
    column: "quantity"
    min_value: 0
    max_value: 10000
  meta:
    severity: "medium"
    description: "Total quantity should be reasonable"
```

## Severity Levels

Use the `meta.severity` field to indicate importance:

- **critical**: Must pass, failure should stop pipeline
- **high**: Important checks, should alert team
- **medium**: Standard checks, log failures
- **low**: Nice-to-have checks, monitoring only

## Layer-Specific Configurations

### Bronze Layer (Raw Data)

Focus on:
- Basic structure (row count, column existence)
- Primary key constraints
- Data freshness
- File format integrity

See: `config_templates/bronze_layer_template.yml`

### Silver Layer (Cleaned Data)

Focus on:
- Business logic validation
- Data type correctness
- Value range validation
- Cross-column relationships
- Duplicate detection

See: `config_templates/silver_layer_template.yml`

### Gold Layer (Business Data)

Focus on:
- Aggregation accuracy
- Business metric validation
- Dimension integrity
- Statistical consistency
- SLA compliance

See: `config_templates/gold_layer_template.yml`

## Advanced Configuration

### Using Variables

You can use environment variables or parameters:

```python
from dq_framework import ConfigLoader
import os

# Load config
loader = ConfigLoader()
config = loader.load("config.yml")

# Override values from environment
config["data_source"]["table_name"] = os.getenv("TABLE_NAME", "default.table")
```

### Merging Multiple Configs

```python
from dq_framework import ConfigLoader

loader = ConfigLoader()

# Load multiple configs
configs = loader.load([
    "base_config.yml",
    "project_specific_config.yml"
])

# Merge them
merged_config = loader.merge_configs(configs)
```

### Conditional Expectations

Use `mostly` parameter for partial validation:

```yaml
- expectation_type: "expect_column_values_to_not_be_null"
  kwargs:
    column: "optional_field"
    mostly: 0.90  # At least 90% must be non-null
  meta:
    severity: "medium"
```

## Best Practices

1. **Start Simple**: Begin with basic validations, add complexity as needed
2. **Use Descriptive Names**: Make validation names clear and meaningful
3. **Set Appropriate Severity**: Match severity to business impact
4. **Document Everything**: Use descriptions to explain why rules exist
5. **Test Configurations**: Validate configs work before deploying
6. **Version Control**: Keep configs in git with your code
7. **Layer-Specific Rules**: Different rules for bronze/silver/gold
8. **Regular Review**: Update rules as data evolves

## Common Patterns

### Pattern 1: Primary Key Validation

```yaml
expectations:
  - expectation_type: "expect_column_to_exist"
    kwargs:
      column: "id"
  - expectation_type: "expect_column_values_to_not_be_null"
    kwargs:
      column: "id"
  - expectation_type: "expect_column_values_to_be_unique"
    kwargs:
      column: "id"
```

### Pattern 2: Date Range Validation

```yaml
expectations:
  - expectation_type: "expect_column_values_to_be_dateutil_parseable"
    kwargs:
      column: "date"
  - expectation_type: "expect_column_min_to_be_between"
    kwargs:
      column: "date"
      min_value: "2020-01-01"
      parse_strings_as_datetimes: true
  - expectation_type: "expect_column_max_to_be_between"
    kwargs:
      column: "date"
      max_value: null  # Current date
      parse_strings_as_datetimes: true
```

### Pattern 3: Required Business Fields

```yaml
expectations:
  - expectation_type: "expect_column_to_exist"
    kwargs:
      column: "customer_name"
  - expectation_type: "expect_column_values_to_not_be_null"
    kwargs:
      column: "customer_name"
  - expectation_type: "expect_column_value_lengths_to_be_between"
    kwargs:
      column: "customer_name"
      min_value: 1
      max_value: 100
```

## Next Steps

- Review [template configs](../config_templates/) for examples
- Check [project examples](../examples/) for real-world usage
- Read [Fabric Integration Guide](FABRIC_INTEGRATION.md) for deployment
