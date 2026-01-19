# Fabric Data Quality Framework - API Reference

> **Version:** 1.2.0  
> **Last Updated:** January 2026  
> **Author:** HS2 Data Engineering Team

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Import Guide](#quick-import-guide)
3. [Core Classes](#core-classes)
   - [DataQualityValidator](#dataqualityvalidator)
   - [FabricDataQualityRunner](#fabricdataqualityrunner)
   - [ConfigLoader](#configloader)
   - [DataProfiler](#dataprofiler)
   - [BatchProfiler](#batchprofiler)
   - [DataLoader](#dataloader)
   - [DataIngester](#dataingester)
   - [FileSystemHandler](#filesystemhandler)
4. [Utility Functions](#utility-functions)
5. [Constants Reference](#constants-reference)
6. [Type Definitions](#type-definitions)

---

## Overview

The **Fabric Data Quality Framework** is a reusable data quality validation library built on [Great Expectations](https://greatexpectations.io/). It provides:

- **YAML-based configuration** for maintainable validation rules
- **Pandas DataFrame validation** with detailed reporting
- **Microsoft Fabric integration** for Delta tables and Lakehouse files
- **Automatic data profiling** and expectation generation
- **Batch processing** with parallel execution support
- **Flexible severity-based thresholds** for pass/fail criteria

---

## Quick Import Guide

### Basic Usage

```python
from dq_framework import (
    DataQualityValidator,
    FabricDataQualityRunner,
    ConfigLoader,
    DataProfiler,
    BatchProfiler,
    DataLoader,
    DataIngester,
    FileSystemHandler,
)
```

### Fabric Detection Utilities

```python
from dq_framework import (
    FABRIC_AVAILABLE,           # bool: True if running in MS Fabric
    FABRIC_UTILS_AVAILABLE,     # bool: Alias for FABRIC_AVAILABLE
    _is_fabric_runtime,         # func: Check if in Fabric runtime
    get_mssparkutils,           # func: Get mssparkutils module
)
```

### Quick Validation Helper

```python
from dq_framework.fabric_connector import quick_validate

# Returns True if validation passed
success = quick_validate(df, "config.yml", halt_on_failure=True)
```

---

## Core Classes

---

## DataQualityValidator

Core validation engine using Great Expectations. Validates pandas DataFrames against YAML-configured expectations.

### Constructor

```python
DataQualityValidator(
    config_path: Optional[str] = None,
    config_dict: Optional[Dict] = None
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_path` | `Optional[str]` | `None` | Path to YAML configuration file |
| `config_dict` | `Optional[Dict]` | `None` | Dictionary configuration (alternative to file) |

**Raises:**

| Exception | Condition |
|-----------|-----------|
| `ImportError` | Great Expectations or Pandas not installed |
| `ValueError` | Neither `config_path` nor `config_dict` provided |
| `FileNotFoundError` | Config file doesn't exist (when using `config_path`) |

**Example:**

```python
from dq_framework import DataQualityValidator

# From YAML file
validator = DataQualityValidator(config_path='config/checks.yml')

# From dictionary
config = {
    'validation_name': 'my_validation',
    'expectations': [
        {
            'expectation_type': 'expect_column_to_exist',
            'kwargs': {'column': 'id'}
        }
    ]
}
validator = DataQualityValidator(config_dict=config)
```

---

### Methods

#### `validate()`

Validate a DataFrame against configured expectations.

```python
def validate(
    self,
    df: pd.DataFrame,
    batch_name: Optional[str] = None,
    suite_name: Optional[str] = None,
    threshold: Optional[float] = None
) -> Dict[str, Any]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `pd.DataFrame` | *required* | DataFrame to validate |
| `batch_name` | `Optional[str]` | Auto-generated | Name for this validation batch |
| `suite_name` | `Optional[str]` | From config | Override suite name from config |
| `threshold` | `Optional[float]` | From config or 100.0 | Success threshold percentage (0-100) |

**Returns:**

`Dict[str, Any]` - Validation results dictionary:

| Key | Type | Description |
|-----|------|-------------|
| `success` | `bool` | Overall validation passed |
| `suite_name` | `str` | Name of the expectation suite |
| `batch_name` | `str` | Batch identifier |
| `timestamp` | `str` | ISO format timestamp |
| `evaluated_checks` | `int` | Total expectations evaluated |
| `successful_checks` | `int` | Expectations that passed |
| `failed_checks` | `int` | Expectations that failed |
| `success_rate` | `float` | Percentage of passed checks |
| `threshold` | `float` | Threshold used for evaluation |
| `quality_thresholds` | `Dict` | Per-severity thresholds |
| `severity_stats` | `Dict` | Stats broken down by severity |
| `threshold_failures` | `List[str]` | Descriptions of threshold violations |
| `statistics` | `Dict` | Detailed GE statistics |
| `failed_expectations` | `List[Dict]` | Details of failed checks (if any) |
| `validation_result` | `object` | Raw GE validation result object |

**Example:**

```python
import pandas as pd

df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie']
})

results = validator.validate(df, batch_name='test_batch')

if results['success']:
    print(f"✓ All {results['evaluated_checks']} checks passed!")
else:
    print(f"✗ {results['failed_checks']} checks failed")
    for fail in results.get('failed_expectations', []):
        print(f"  - {fail['expectation']} on column '{fail['column']}'")
```

---

#### `get_expectation_list()`

Get list of configured expectations.

```python
def get_expectation_list(self) -> List[Dict[str, Any]]
```

**Returns:**

`List[Dict[str, Any]]` - List of expectation configurations

**Example:**

```python
expectations = validator.get_expectation_list()
for exp in expectations:
    print(f"Check: {exp['expectation_type']}")
```

---

#### `get_config_summary()`

Get summary of current configuration.

```python
def get_config_summary(self) -> Dict[str, Any]
```

**Returns:**

`Dict[str, Any]` with keys:

| Key | Type | Description |
|-----|------|-------------|
| `data_source` | `Dict` | Data source configuration |
| `suite_name` | `str` | Expectation suite name |
| `expectation_count` | `int` | Number of configured expectations |
| `expectations` | `List[str]` | List of expectation type names |

---

## FabricDataQualityRunner

MS Fabric-specific data quality runner with native Spark DataFrame support and Lakehouse integration.

### Constructor

```python
FabricDataQualityRunner(
    config_path: str,
    workspace_id: Optional[str] = None,
    results_location: Optional[str] = "Files/dq_results"
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_path` | `str` | *required* | Path to YAML configuration (Lakehouse or absolute path) |
| `workspace_id` | `Optional[str]` | `None` | MS Fabric workspace ID |
| `results_location` | `Optional[str]` | `"Files/dq_results"` | Where to store results in Lakehouse |

**Example:**

```python
from dq_framework import FabricDataQualityRunner

# In Fabric notebook
runner = FabricDataQualityRunner(
    config_path="Files/dq_configs/my_table.yml",
    workspace_id="abc123",
    results_location="Files/dq_results"
)
```

---

### Properties

#### `config`

Get current configuration dictionary.

```python
@property
def config(self) -> Dict[str, Any]
```

---

### Methods

#### `validate_spark_dataframe()`

Validate a Spark DataFrame.

```python
def validate_spark_dataframe(
    self,
    spark_df: SparkDataFrame,
    batch_name: Optional[str] = None,
    sample_large_data: bool = True
) -> Dict[str, Any]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `spark_df` | `SparkDataFrame` | *required* | Spark DataFrame to validate |
| `batch_name` | `Optional[str]` | Auto-generated | Name for this validation batch |
| `sample_large_data` | `bool` | `True` | Auto-sample if >100K rows |

**Returns:**

`Dict[str, Any]` - Same structure as `DataQualityValidator.validate()`

**Raises:**

| Exception | Condition |
|-----------|-----------|
| `ImportError` | PySpark not available |

**Example:**

```python
# In Fabric notebook
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark_df = spark.table("bronze.customers")

results = runner.validate_spark_dataframe(spark_df)
```

---

#### `validate_delta_table()`

Validate a Delta table in Lakehouse.

```python
def validate_delta_table(
    self,
    table_name: str,
    batch_name: Optional[str] = None
) -> Dict[str, Any]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table_name` | `str` | *required* | Name of Delta table (e.g., `"bronze.incidents"`) |
| `batch_name` | `Optional[str]` | Auto-generated | Name for this validation batch |

**Returns:**

`Dict[str, Any]` - Validation results dictionary

**Raises:**

| Exception | Condition |
|-----------|-----------|
| `ImportError` | PySpark not available |
| `Exception` | Failed to load table |

**Example:**

```python
results = runner.validate_delta_table("bronze.incidents")
if not results['success']:
    runner.handle_failure(results, action='halt')
```

---

#### `validate_lakehouse_file()`

Validate a file in Lakehouse.

```python
def validate_lakehouse_file(
    self,
    file_path: str,
    file_format: str = "parquet",
    batch_name: Optional[str] = None
) -> Dict[str, Any]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `file_path` | `str` | *required* | Path to file (e.g., `"Files/data/mydata.parquet"`) |
| `file_format` | `str` | `"parquet"` | File format: `"parquet"`, `"csv"`, `"json"`, `"delta"` |
| `batch_name` | `Optional[str]` | Auto-generated | Name for this validation batch |

**Returns:**

`Dict[str, Any]` - Validation results dictionary

**Raises:**

| Exception | Condition |
|-----------|-----------|
| `ImportError` | PySpark not available |
| `ValueError` | Unsupported file format |
| `Exception` | Failed to read file |

**Example:**

```python
results = runner.validate_lakehouse_file(
    file_path="Files/raw/sales_data.csv",
    file_format="csv"
)
```

---

#### `handle_failure()`

Handle validation failure with configurable actions.

```python
def handle_failure(
    self,
    results: Dict[str, Any],
    action: str = "log"
) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `results` | `Dict[str, Any]` | *required* | Validation results from `validate_*` methods |
| `action` | `str` | `"log"` | Action: `"log"`, `"halt"`, or `"alert"` |

**Actions:**

| Action | Behavior |
|--------|----------|
| `"log"` | Log failure details (always executed) |
| `"halt"` | Raise `ValueError` to stop pipeline execution |
| `"alert"` | Send notification (placeholder - requires implementation) |

**Raises:**

| Exception | Condition |
|-----------|-----------|
| `ValueError` | When `action="halt"` and validation failed |

**Example:**

```python
results = runner.validate_delta_table("my_table")
if not results['success']:
    runner.handle_failure(results, action='halt')  # Raises ValueError
```

---

## ConfigLoader

Loads and validates YAML configuration files for data quality checks.

### Constructor

```python
ConfigLoader()
```

No parameters required.

---

### Methods

#### `load()`

Load configuration from YAML file, dictionary, or list.

```python
def load(self, config_path: Any) -> Any
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_path` | `Any` | *required* | Path to YAML file, config dictionary, or list of them |

**Returns:**

`Dict[str, Any]` or `List[Dict[str, Any]]` - Configuration dictionary or list

**Raises:**

| Exception | Condition |
|-----------|-----------|
| `FileNotFoundError` | Config file doesn't exist |
| `ValueError` | Invalid YAML syntax or missing required keys |

**Example:**

```python
from dq_framework import ConfigLoader

loader = ConfigLoader()

# Single file
config = loader.load('config/my_checks.yml')

# Dictionary
config = loader.load({'validation_name': 'test', 'expectations': []})

# Multiple files
configs = loader.load(['config1.yml', 'config2.yml'])
```

---

#### `validate()`

Validate that configuration has required structure.

```python
def validate(self, config: Dict[str, Any]) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config` | `Dict[str, Any]` | *required* | Configuration dictionary |

**Raises:**

| Exception | Condition |
|-----------|-----------|
| `ValueError` | Missing `validation_name`, `expectations`, or invalid expectation structure |

---

#### `load_multiple()`

Load multiple configuration files.

```python
def load_multiple(self, config_paths: list) -> Dict[str, Dict[str, Any]]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_paths` | `list` | *required* | List of paths to config files |

**Returns:**

`Dict[str, Dict[str, Any]]` - Dictionary mapping file stems to configurations

**Example:**

```python
configs = loader.load_multiple(['sales.yml', 'inventory.yml'])
# {'sales': {...}, 'inventory': {...}}
```

---

#### `merge_configs()`

Merge multiple configuration files into one.

```python
def merge_configs(self, config_paths: list) -> Dict[str, Any]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_paths` | `list` | *required* | List of paths to config files |

**Returns:**

`Dict[str, Any]` - Merged configuration with combined expectations

---

#### `validate_yaml_syntax()` (static)

Check if YAML file has valid syntax without fully loading it.

```python
@staticmethod
def validate_yaml_syntax(config_path: str) -> bool
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_path` | `str` | *required* | Path to YAML file |

**Returns:**

`bool` - `True` if valid, `False` otherwise

---

## DataProfiler

Profiles data and automatically generates appropriate data quality expectations based on patterns found.

### Constructor

```python
DataProfiler(
    df: pd.DataFrame,
    sample_size: Optional[int] = None
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `pd.DataFrame` | *required* | DataFrame to profile |
| `sample_size` | `Optional[int]` | `None` | Sample rows for analysis (for large datasets) |

**Example:**

```python
from dq_framework import DataProfiler
import pandas as pd

df = pd.read_csv('my_data.csv')

# Full profiling
profiler = DataProfiler(df)

# Sampled profiling for large datasets
profiler = DataProfiler(df, sample_size=10000)
```

---

### Methods

#### `profile()`

Profile the DataFrame and return summary statistics.

```python
def profile(self) -> Dict[str, Any]
```

**Returns:**

`Dict[str, Any]` with keys:

| Key | Type | Description |
|-----|------|-------------|
| `row_count` | `int` | Total rows in original DataFrame |
| `sampled_rows` | `Optional[int]` | Sampled row count (if sampled) |
| `column_count` | `int` | Total columns |
| `columns` | `Dict[str, Dict]` | Detailed info per column |
| `data_quality_score` | `float` | Overall score (0-100) |
| `profiled_at` | `str` | ISO format timestamp |

**Column Profile Structure:**

| Key | Type | Description |
|-----|------|-------------|
| `dtype` | `str` | Pandas dtype |
| `null_count` | `int` | Number of null values |
| `null_percent` | `float` | Percentage of nulls |
| `unique_count` | `int` | Number of unique values |
| `unique_percent` | `float` | Percentage of unique values |
| `detected_type` | `str` | Semantic type (see below) |
| `min`, `max`, `mean`, `median`, `std` | `float` | Numeric statistics (if numeric) |
| `min_length`, `max_length`, `avg_length` | `float` | String statistics (if string) |
| `unique_values` | `List` | Values (if low cardinality) |
| `sample_values` | `List` | Sample values (if high cardinality) |

**Detected Types:**

| Type | Description |
|------|-------------|
| `id` | Identifier column (high uniqueness, 'id' in name) |
| `code` | Code/reference column |
| `date` | Date/datetime column |
| `numeric` | Generic numeric |
| `monetary` | Amount/value/price column |
| `percentage` | Percentage/rate column |
| `categorical` | Low cardinality categorical |
| `string` | Short string |
| `text` | Long text (avg length > 50) |
| `empty` | Column with no data |
| `unknown` | Could not determine type |

**Example:**

```python
profile = profiler.profile()

print(f"Rows: {profile['row_count']}")
print(f"Quality Score: {profile['data_quality_score']:.1f}/100")

for col, info in profile['columns'].items():
    print(f"{col}: {info['detected_type']} ({info['null_percent']:.1f}% nulls)")
```

---

#### `generate_expectations()`

Generate data quality expectations based on profiling results.

```python
def generate_expectations(
    self,
    validation_name: str,
    description: Optional[str] = None,
    severity_threshold: str = "medium",
    include_structural: bool = True,
    include_completeness: bool = True,
    include_validity: bool = True,
    null_tolerance: float = 5.0,
    quality_thresholds: Optional[Dict[str, float]] = None
) -> Dict[str, Any]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `validation_name` | `str` | *required* | Name for the validation suite |
| `description` | `Optional[str]` | Auto-generated | Optional description |
| `severity_threshold` | `str` | `"medium"` | Default severity: `"critical"`, `"high"`, `"medium"`, `"low"` |
| `include_structural` | `bool` | `True` | Include table shape checks |
| `include_completeness` | `bool` | `True` | Include null/completeness checks |
| `include_validity` | `bool` | `True` | Include type/range/pattern checks |
| `null_tolerance` | `float` | `5.0` | Percentage of nulls to tolerate |
| `quality_thresholds` | `Optional[Dict[str, float]]` | Default thresholds | Success thresholds per severity level |

**Returns:**

`Dict[str, Any]` - YAML-compatible configuration dictionary

**Example:**

```python
config = profiler.generate_expectations(
    validation_name="customer_validation",
    severity_threshold="high",
    null_tolerance=2.0,
    quality_thresholds={
        'critical': 100.0,
        'high': 98.0,
        'medium': 90.0,
        'low': 75.0
    }
)
```

---

#### `save_config()`

Save generated config to YAML file.

```python
def save_config(self, config: Dict[str, Any], output_path: str) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config` | `Dict[str, Any]` | *required* | Configuration from `generate_expectations()` |
| `output_path` | `str` | *required* | Path to save YAML file |

**Example:**

```python
config = profiler.generate_expectations(validation_name="my_validation")
profiler.save_config(config, 'config/my_validation.yml')
```

---

#### `print_summary()`

Print a human-readable summary of the profiling results.

```python
def print_summary(self) -> None
```

**Example Output:**

```
================================================================================
DATA PROFILE SUMMARY
================================================================================
Rows: 10,000
Columns: 15
Data Quality Score: 87.3/100

COLUMN DETAILS:
--------------------------------------------------------------------------------
Column                         Type            Nulls      Unique    
--------------------------------------------------------------------------------
customer_id                    id              0.0%       100.0%    
name                           string          0.5%       95.2%     
category                       categorical     0.0%       0.3%      
================================================================================
```

---

## BatchProfiler

Handles batch profiling of multiple files in parallel.

### Static Methods

#### `process_single_file()`

Profile a single file and save the configuration.

```python
@staticmethod
def process_single_file(
    file_path: str,
    output_dir: str,
    sample_size: Optional[int] = None,
    thresholds: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `file_path` | `str` | *required* | Path to input file |
| `output_dir` | `str` | *required* | Directory to save generated config |
| `sample_size` | `Optional[int]` | `None` | Number of rows to sample |
| `thresholds` | `Optional[Dict[str, Any]]` | `None` | Additional threshold parameters |

**Returns:**

`Dict[str, Any]` - Result dictionary:

| Key | Type | Description |
|-----|------|-------------|
| `status` | `str` | `"success"` or `"error"` |
| `file` | `str` | File name |
| `rows` | `int` | Rows processed (on success) |
| `expectations` | `int` | Expectations generated (on success) |
| `output` | `str` | Output path (on success) |
| `error` | `str` | Error message (on error) |

---

### Class Methods

#### `run_parallel_profiling()`

Run profiling in parallel for all supported files in a directory.

```python
@classmethod
def run_parallel_profiling(
    cls,
    input_dir: str,
    output_dir: str,
    workers: int = 1,
    sample_size: Optional[int] = None,
    thresholds: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `input_dir` | `str` | *required* | Directory containing files to profile |
| `output_dir` | `str` | *required* | Directory to save generated configs |
| `workers` | `int` | `1` | Number of parallel workers |
| `sample_size` | `Optional[int]` | `None` | Rows to sample per file |
| `thresholds` | `Optional[Dict[str, Any]]` | `None` | Additional threshold parameters |

**Supported File Formats:**

- `.parquet`
- `.csv`
- `.json`
- `.xlsx`
- `.xls`

**Returns:**

`List[Dict[str, Any]]` - List of result dictionaries for each file

**Example:**

```python
from dq_framework import BatchProfiler

results = BatchProfiler.run_parallel_profiling(
    input_dir='data/raw/',
    output_dir='config/generated/',
    workers=4,
    sample_size=50000
)

for result in results:
    if result['status'] == 'success':
        print(f"✓ {result['file']}: {result['expectations']} expectations")
    else:
        print(f"✗ {result['file']}: {result['error']}")
```

---

## DataLoader

Handles efficient loading of data from various sources with memory optimization.

### Static Methods

#### `load_data()`

Load data from various file formats with memory optimization.

```python
@staticmethod
def load_data(
    file_path: Union[Path, str],
    sample_size: Optional[int] = None,
    **kwargs
) -> pd.DataFrame
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `file_path` | `Union[Path, str]` | *required* | Path to the file |
| `sample_size` | `Optional[int]` | `None` | Number of rows to load (all if None) |
| `**kwargs` | `Any` | - | Additional arguments for pandas read functions |

**Supported Formats:**

| Extension | Reader | Notes |
|-----------|--------|-------|
| `.csv` | `pd.read_csv()` | Auto-detects encoding |
| `.parquet` | `pd.read_parquet()` | Uses PyArrow optimization if available |
| `.xlsx`, `.xls` | `pd.read_excel()` | |
| `.json` | `pd.read_json()` | |

**Features:**

- **Auto-sampling for large files:** Files > 500 MB auto-sample to 100,000 rows
- **Encoding detection:** CSV files try UTF-8, Latin-1, ISO-8859-1, CP1252
- **ABFSS path support:** Works with Fabric Lakehouse paths

**Returns:**

`pd.DataFrame` - Loaded data

**Raises:**

| Exception | Condition |
|-----------|-----------|
| `FileNotFoundError` | File not found |
| `ValueError` | Unsupported file format or encoding detection failed |

**Example:**

```python
from dq_framework import DataLoader

# Basic load
df = DataLoader.load_data('data/sales.parquet')

# With sampling
df = DataLoader.load_data('data/large_file.csv', sample_size=10000)

# With custom encoding
df = DataLoader.load_data('data/legacy.csv', encoding='cp1252')
```

---

## DataIngester

Handles data ingestion operations for both local and Fabric environments.

### Constructor

```python
DataIngester(engine: str = "fastparquet")
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `engine` | `str` | `"fastparquet"` | Parquet engine: `"fastparquet"` or `"pyarrow"` |

---

### Methods

#### `ingest_file()`

Ingest a single file from source to target.

```python
def ingest_file(
    self,
    source_path: Path,
    target_path: Path,
    is_fabric: bool = False
) -> bool
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_path` | `Path` | *required* | Path to source file |
| `target_path` | `Path` | *required* | Path to target file |
| `is_fabric` | `bool` | `False` | Whether running in Fabric environment |

**Behavior:**

| Environment | Method |
|-------------|--------|
| Local (`is_fabric=False`) | Uses `shutil.copy2()` for efficiency |
| Fabric (`is_fabric=True`) | Reads/writes with Pandas for validation |

**Returns:**

`bool` - `True` if successful, `False` otherwise

**Example:**

```python
from dq_framework import DataIngester
from pathlib import Path

ingester = DataIngester()
success = ingester.ingest_file(
    source_path=Path('data/raw/sales.parquet'),
    target_path=Path('data/bronze/sales.parquet'),
    is_fabric=False
)
```

---

## FileSystemHandler

Handles file system operations for both local and ABFSS (Azure Blob File System) paths.

### Static Methods

#### `is_abfss()`

Check if path is an ABFSS path.

```python
@staticmethod
def is_abfss(path: str) -> bool
```

**Returns:** `True` if path starts with `"abfss://"`

---

#### `list_files()`

List files in a directory (local or ABFSS).

```python
@staticmethod
def list_files(path: str) -> List[str]
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str` | *required* | Directory path (local or ABFSS) |

**Returns:**

`List[str]` - List of file paths

**Raises:**

| Exception | Condition |
|-----------|-----------|
| `ImportError` | ABFSS path but not in Fabric environment |

---

#### `exists()`

Check if path exists.

```python
@staticmethod
def exists(path: str) -> bool
```

---

#### `is_dir()`

Check if path is a directory.

```python
@staticmethod
def is_dir(path: str) -> bool
```

---

#### `get_suffix()`

Get file extension (lowercase).

```python
@staticmethod
def get_suffix(path: str) -> str
```

**Example:** `FileSystemHandler.get_suffix("data/file.CSV")` → `".csv"`

---

#### `get_name()`

Get file name from path.

```python
@staticmethod
def get_name(path: str) -> str
```

**Example:** `FileSystemHandler.get_name("data/raw/sales.parquet")` → `"sales.parquet"`

---

## Utility Functions

### `quick_validate()`

Quick validation helper function for both Spark and Pandas DataFrames.

```python
def quick_validate(
    df,
    config_path: str,
    halt_on_failure: bool = False
) -> bool
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | *required* | Spark or Pandas DataFrame |
| `config_path` | `str` | *required* | Path to configuration file |
| `halt_on_failure` | `bool` | `False` | Raise exception on failure |

**Returns:**

`bool` - `True` if validation passed

**Raises:**

| Exception | Condition |
|-----------|-----------|
| `ValueError` | When `halt_on_failure=True` and validation failed |

**Example:**

```python
from dq_framework.fabric_connector import quick_validate

# Simple check
if quick_validate(df, "config.yml"):
    print("Data is valid!")

# Halt pipeline on failure
quick_validate(df, "config.yml", halt_on_failure=True)
# Raises ValueError if validation fails
```

---

### `_is_fabric_runtime()`

Check if running in Microsoft Fabric environment.

```python
def _is_fabric_runtime() -> bool
```

**Returns:** `True` if `/lakehouse/default/Files` exists

---

### `get_mssparkutils()`

Get mssparkutils module if available.

```python
def get_mssparkutils() -> Optional[module]
```

**Returns:** `mssparkutils` module or `None`

---

## Constants Reference

### Validation Thresholds

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_VALIDATION_THRESHOLD` | `100.0` | Default success threshold percentage |
| `QUALITY_THRESHOLD_CRITICAL` | `100.0` | Threshold for critical severity |
| `QUALITY_THRESHOLD_HIGH` | `95.0` | Threshold for high severity |
| `QUALITY_THRESHOLD_MEDIUM` | `80.0` | Threshold for medium severity |
| `QUALITY_THRESHOLD_LOW` | `50.0` | Threshold for low severity |

```python
DEFAULT_QUALITY_THRESHOLDS = {
    'critical': 100.0,
    'high': 95.0,
    'medium': 80.0,
    'low': 50.0
}
```

### Data Profiling - Type Detection

| Constant | Value | Description |
|----------|-------|-------------|
| `ID_UNIQUENESS_THRESHOLD` | `0.9` | Uniqueness ratio to detect ID columns (>90%) |
| `CATEGORICAL_UNIQUENESS_THRESHOLD` | `0.05` | Uniqueness ratio to detect categorical (<5%) |
| `DATE_DETECTION_THRESHOLD` | `0.8` | Minimum valid date ratio (>80%) |
| `TEXT_LENGTH_THRESHOLD` | `50` | Average length to classify as text vs string |

### Data Profiling - Sampling & Limits

| Constant | Value | Description |
|----------|-------|-------------|
| `TYPE_DETECTION_SAMPLE_SIZE` | `100` | Sample size for type detection |
| `MAX_SAMPLE_VALUES` | `10` | Maximum sample values to store |
| `MAX_UNIQUE_VALUES_DISPLAY` | `10` | Maximum unique values for low-cardinality |

### Data Profiling - Quality Scoring

| Constant | Value | Description |
|----------|-------|-------------|
| `QUALITY_SCORE_NULL_WEIGHT` | `0.6` | Weight for null penalty (60%) |
| `QUALITY_SCORE_UNIQUENESS_WEIGHT` | `0.4` | Weight for uniqueness (40%) |
| `UNIQUENESS_SWEET_SPOT` | `0.5` | Ideal uniqueness for non-ID columns (50%) |

### Data Profiling - Expectation Generation

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_NULL_TOLERANCE` | `5.0` | Default null tolerance percentage |
| `ID_NULL_THRESHOLD_FOR_UNIQUENESS` | `50` | Max null % to generate uniqueness checks |

### Data Loading - Memory Protection

| Constant | Value | Description |
|----------|-------|-------------|
| `LARGE_FILE_SIZE_MB` | `500` | File size threshold for auto-sampling (MB) |
| `DEFAULT_AUTO_SAMPLE_ROWS` | `100000` | Default rows for auto-sampling |

### Fabric Integration

| Constant | Value | Description |
|----------|-------|-------------|
| `FABRIC_CONFIG_MAX_BYTES` | `1000000` | Max bytes for config files (1MB) |
| `FABRIC_LARGE_DATASET_THRESHOLD` | `100000` | Row count for auto-sampling |
| `FABRIC_SAMPLE_FRACTION` | `0.1` | Sample fraction for large datasets (10%) |
| `MAX_FAILURE_DISPLAY` | `10` | Max failed expectations to display |

---

## Type Definitions

### Validation Results Dictionary

Returned by `validate()`, `validate_spark_dataframe()`, `validate_delta_table()`, and `validate_lakehouse_file()`:

```python
{
    'success': bool,                    # Overall pass/fail
    'suite_name': str,                  # Expectation suite name
    'batch_name': str,                  # Batch identifier
    'timestamp': str,                   # ISO format timestamp
    'evaluated_checks': int,            # Total expectations
    'successful_checks': int,           # Passed expectations
    'failed_checks': int,               # Failed expectations
    'success_rate': float,              # Percentage passed
    'threshold': Optional[float],       # Threshold used
    'quality_thresholds': Dict[str, float],  # Per-severity thresholds
    'severity_stats': Dict[str, Dict[str, int]],  # Stats by severity
    'threshold_failures': List[str],    # Threshold violation messages
    'statistics': {
        'evaluated_expectations': int,
        'successful_expectations': int,
        'unsuccessful_expectations': int,
        'success_percent': float
    },
    'failed_expectations': [            # Only if failures exist
        {
            'expectation': str,         # Expectation type
            'column': str,              # Column name or 'N/A'
            'severity': str,            # Severity level
            'details': Dict             # GE result details
        }
    ],
    'validation_result': object         # Raw GE ValidationResult
}
```

### Profile Results Dictionary

Returned by `DataProfiler.profile()`:

```python
{
    'row_count': int,                   # Total rows
    'sampled_rows': Optional[int],      # Sampled rows (if sampled)
    'column_count': int,                # Total columns
    'profiled_at': str,                 # ISO timestamp
    'data_quality_score': float,        # Score 0-100
    'columns': {
        'column_name': {
            'dtype': str,               # Pandas dtype
            'null_count': int,
            'null_percent': float,
            'unique_count': int,
            'unique_percent': float,
            'detected_type': str,       # Semantic type
            # Numeric columns only:
            'min': float,
            'max': float,
            'mean': float,
            'median': float,
            'std': float,
            # String columns only:
            'min_length': int,
            'max_length': int,
            'avg_length': float,
            # Low cardinality:
            'unique_values': List,
            # High cardinality:
            'sample_values': List
        }
    }
}
```

### Configuration Dictionary

Structure for YAML config files:

```python
{
    'validation_name': str,             # Required: validation identifier
    'description': str,                 # Optional: description
    'suite_name': str,                  # Optional: GE suite name
    'data_asset_name': str,             # Optional: GE asset name
    'threshold': float,                 # Optional: global threshold
    'quality_thresholds': {             # Optional: per-severity thresholds
        'critical': float,
        'high': float,
        'medium': float,
        'low': float
    },
    'data_source': {                    # Optional: data source info
        'type': str,
        'connection_string': str,
        # ... other source-specific fields
    },
    'expectations': [                   # Required: list of expectations
        {
            'expectation_type': str,    # Required: GE expectation type
            'kwargs': {                 # Required: expectation parameters
                'column': str,
                # ... other kwargs
            },
            'meta': {                   # Optional: metadata
                'severity': str,        # 'critical', 'high', 'medium', 'low'
                'description': str
            }
        }
    ]
}
```

---

## See Also

- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Project README](../README.md)
- [Configuration Examples](../config_templates/)
