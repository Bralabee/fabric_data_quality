# Microsoft Fabric Integration Guide

This guide explains how to integrate the Data Quality Framework with Microsoft Fabric notebooks, pipelines, and workflows.

## Table of Contents

1. [Setup in Fabric Workspace](#setup-in-fabric-workspace)
2. [Using in Fabric Notebooks](#using-in-fabric-notebooks)
3. [Integration with Fabric Pipelines](#integration-with-fabric-pipelines)
4. [Data Layer Integration](#data-layer-integration)
5. [Best Practices](#best-practices)
6. [Troubleshooting](#troubleshooting)

## Setup in Fabric Workspace

There are two ways to deploy the framework: **Library (Recommended)** or **Source Code**.

### Method 1: Custom Library (Recommended)

This method installs the framework as a proper Python library in your Fabric Environment.

1.  **Build the Library (Locally)**:
    ```bash
    python -m build --wheel
    ```
    This generates a file like `dist/fabric_data_quality-2.0.0-py3-none-any.whl`.

2.  **Upload to Fabric Environment**:
    *   In Fabric, go to **Workspaces** -> Select your workspace.
    *   Click **New** -> **Environment** (or select an existing one).
    *   In **Public Libraries**, add: `great-expectations`, `pyyaml`.
    *   In **Custom Libraries**, click **Upload** and select the `.whl` file you built.
    *   Click **Publish**.

3.  **Attach to Notebook**:
    *   Open your notebook.
    *   Click the **Environment** dropdown in the top menu.
    *   Select your published environment.

### Method 2: Source Code Upload (Quick Testing)

1. **Upload the folder** to your Fabric workspace (Lakehouse Files):
   - Upload the `fabric_data_quality` folder to `/Lakehouse/Files/libs/`.

2. **Add to Path**:
   ```python
   import sys
   sys.path.insert(0, "/lakehouse/default/Files/libs/fabric_data_quality")
   ```

### Step 3: Test Installation

```python
from dq_framework import FabricDataQualityRunner
print("✅ Framework loaded successfully")
```

## Using in Fabric Notebooks

### Basic Pattern

```python
# Cell 1: Setup
import sys
sys.path.insert(0, '/Workspace/shared/fabric_data_quality')
from dq_framework import FabricDataQualityRunner

# Cell 2: Load data
df = spark.read.table("bronze.my_data")

# Cell 3: Validate
runner = FabricDataQualityRunner(
    config_path="/Workspace/shared/fabric_data_quality/examples/my_config.yml"
)
results = runner.validate_spark_dataframe(df)

# Cell 4: Handle results
if not results["success"]:
    print(f"❌ Validation failed: {results['statistics']['unsuccessful_expectations']} checks failed")
    # Optionally raise error to stop notebook
    raise ValueError("Data quality validation failed!")
else:
    print("✅ All data quality checks passed!")
```

### Bronze Layer Notebook Example

```python
"""
Bronze Layer: Raw Data Ingestion with Validation
"""

# Setup
import sys
sys.path.insert(0, '/Workspace/shared/fabric_data_quality')
from dq_framework import FabricDataQualityRunner

# Configuration
SOURCE_PATH = "Files/landing/raw_data.csv"
TARGET_TABLE = "bronze.raw_data"
CONFIG_PATH = "/Workspace/shared/fabric_data_quality/config_templates/bronze_layer_template.yml"

# Step 1: Read raw data
print("📥 Reading raw data...")
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(SOURCE_PATH)

print(f"  Rows read: {df_raw.count()}")

# Step 2: Validate raw data
print("✓ Validating data quality...")
runner = FabricDataQualityRunner(CONFIG_PATH)
validation_results = runner.validate_spark_dataframe(df_raw)

# Step 3: Handle validation results
if validation_results["success"]:
    print("✅ Validation passed!")
    
    # Write to bronze table
    print(f"💾 Writing to {TARGET_TABLE}...")
    df_raw.write.mode("overwrite").saveAsTable(TARGET_TABLE)
    print("✅ Bronze layer ingestion complete!")
    
else:
    print("❌ Validation failed!")
    print(f"  Failed checks: {validation_results['statistics']['unsuccessful_expectations']}")
    
    # Write to quarantine table for investigation
    quarantine_table = f"{TARGET_TABLE}_quarantine"
    print(f"⚠️ Writing data to quarantine: {quarantine_table}")
    df_raw.write.mode("overwrite").saveAsTable(quarantine_table)
    
    # Optionally send alert
    # send_teams_alert(validation_results)
    
    # Stop execution
    from notebookutils import mssparkutils
    mssparkutils.notebook.exit("Data quality validation failed")
```

### Silver Layer Notebook Example

```python
"""
Silver Layer: Data Transformation with Validation
"""

# Setup
import sys
sys.path.insert(0, '/Workspace/shared/fabric_data_quality')
from dq_framework import FabricDataQualityRunner

# Configuration
SOURCE_TABLE = "bronze.raw_data"
TARGET_TABLE = "silver.cleaned_data"
CONFIG_PATH = "/Workspace/shared/fabric_data_quality/config_templates/silver_layer_template.yml"

# Step 1: Read bronze data
print("📥 Reading bronze data...")
df_bronze = spark.read.table(SOURCE_TABLE)

# Step 2: Transform data
print("🔄 Transforming data...")
df_silver = df_bronze \
    .dropDuplicates(["id"]) \
    .filter("status IN ('active', 'pending')") \
    .withColumn("processed_date", current_timestamp())

print(f"  Rows after transformation: {df_silver.count()}")

# Step 3: Validate transformed data
print("✓ Validating silver layer...")
runner = FabricDataQualityRunner(CONFIG_PATH)
validation_results = runner.validate_spark_dataframe(df_silver)

# Step 4: Write to silver if validation passes
if validation_results["success"]:
    print("✅ Silver layer validation passed!")
    df_silver.write.mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"✅ Data written to {TARGET_TABLE}")
else:
    print("❌ Silver layer validation failed!")
    raise ValueError("Silver layer data quality checks failed")
```

### Gold Layer Notebook Example

```python
"""
Gold Layer: Business Aggregations with Strict Validation
"""

# Setup
import sys
sys.path.insert(0, '/Workspace/shared/fabric_data_quality')
from dq_framework import FabricDataQualityRunner

# Configuration
SOURCE_TABLE = "silver.cleaned_data"
TARGET_TABLE = "gold.business_metrics"
CONFIG_PATH = "/Workspace/shared/fabric_data_quality/config_templates/gold_layer_template.yml"

# Step 1: Read silver data
df_silver = spark.read.table(SOURCE_TABLE)

# Step 2: Create business aggregations
print("📊 Creating business aggregations...")
df_gold = df_silver.groupBy("region", "product_category", "date") \
    .agg(
        sum("revenue").alias("total_revenue"),
        count("order_id").alias("order_count"),
        avg("order_value").alias("avg_order_value")
    )

# Step 3: Validate gold layer (strict validation)
print("✓ Validating gold layer (business-critical)...")
runner = FabricDataQualityRunner(CONFIG_PATH)
validation_results = runner.validate_spark_dataframe(df_gold)

# Step 4: Handle results strictly for gold layer
if validation_results["success"]:
    print("✅ Gold layer validation passed!")
    df_gold.write.mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"✅ Business metrics published to {TARGET_TABLE}")
else:
    # Gold layer failures are critical
    print("⛔ CRITICAL: Gold layer validation failed!")
    print("Business metrics NOT published.")
    
    # Log detailed failure information
    for result in validation_results["results"]:
        if not result["success"]:
            severity = result.get("meta", {}).get("severity", "unknown")
            print(f"  [{severity.upper()}] {result['expectation_type']}")
    
    # Alert business team
    # send_critical_alert(validation_results)
    
    raise ValueError("Gold layer validation failed - business metrics not reliable")
```

## Integration with Fabric Pipelines

### Pipeline Activity: Notebook with Data Quality

1. **Create Pipeline** in Fabric workspace
2. **Add Notebook Activity**
3. **Configure notebook** to include validation
4. **Set failure handling**

### Example Pipeline Structure

```
Pipeline: end_to_end_etl
+-- Activity 1: bronze_ingestion (Notebook)
|   +-- Validates with bronze_layer_template.yml
+-- Activity 2: silver_transformation (Notebook)
|   +-- Validates with silver_layer_template.yml
|   +-- Depends on: bronze_ingestion (success)
+-- Activity 3: gold_aggregation (Notebook)
    +-- Validates with gold_layer_template.yml
    +-- Depends on: silver_transformation (success)
```

### Pipeline Notebook Template

```python
"""
Pipeline-compatible notebook with validation
"""
import sys
sys.path.insert(0, '/Workspace/shared/fabric_data_quality')
from dq_framework import FabricDataQualityRunner
from notebookutils import mssparkutils

# Get parameters from pipeline
source_table = mssparkutils.notebook.getArgument("source_table", "bronze.default")
target_table = mssparkutils.notebook.getArgument("target_table", "silver.default")
config_path = mssparkutils.notebook.getArgument("config_path", "")

try:
    # Load data
    df = spark.read.table(source_table)
    
    # Transform (your logic here)
    df_transformed = df.dropDuplicates()
    
    # Validate
    runner = FabricDataQualityRunner(config_path)
    results = runner.validate_spark_dataframe(df_transformed)
    
    if results["success"]:
        # Write to target
        df_transformed.write.mode("overwrite").saveAsTable(target_table)
        
        # Return success to pipeline
        mssparkutils.notebook.exit({
            "status": "success",
            "rows_processed": df_transformed.count(),
            "validation_passed": True
        })
    else:
        # Return failure to pipeline
        mssparkutils.notebook.exit({
            "status": "failed",
            "validation_passed": False,
            "failed_checks": results["statistics"]["unsuccessful_expectations"]
        })
        
except Exception as e:
    # Return error to pipeline
    mssparkutils.notebook.exit({
        "status": "error",
        "error_message": str(e)
    })
```

## Data Layer Integration

### Bronze Layer Strategy

**Purpose**: Validate raw data ingestion

**Approach**:
- Focus on structural integrity
- Check file format and schema
- Validate primary keys
- Log failures but continue (raw data preservation)

**Config**: Use `bronze_layer_template.yml`

**Failure Handling**: `on_failure: log`

### Silver Layer Strategy

**Purpose**: Validate cleaned and transformed data

**Approach**:
- Enforce business rules
- Validate data types and ranges
- Check cross-column relationships
- Raise errors for critical failures

**Config**: Use `silver_layer_template.yml`

**Failure Handling**: `on_failure: log` or `raise` for critical

### Gold Layer Strategy

**Purpose**: Validate business-ready aggregated data

**Approach**:
- Strict validation of business metrics
- Ensure aggregation accuracy
- Validate dimension integrity
- Always raise errors on failure

**Config**: Use `gold_layer_template.yml`

**Failure Handling**: `on_failure: raise`

## Best Practices

### 1. Path Management

```python
# Use consistent paths across notebooks
DQ_FRAMEWORK_PATH = '/Workspace/shared/fabric_data_quality'
sys.path.insert(0, DQ_FRAMEWORK_PATH)
```

### 2. Configuration Management

```python
# Store configs in central location
CONFIG_BASE_PATH = '/Workspace/shared/fabric_data_quality/configs'

# Use naming convention
bronze_config = f"{CONFIG_BASE_PATH}/bronze/{project_name}_config.yml"
```

### 3. Error Handling

```python
try:
    results = runner.validate_spark_dataframe(df)
    if not results["success"]:
        # Handle based on layer
        if data_layer == "gold":
            raise ValueError("Gold layer validation failed")
        else:
            print("⚠️ Validation warnings detected")
except Exception as e:
    # Log to Fabric monitoring
    print(f"Error: {e}")
    # Optionally exit notebook
    mssparkutils.notebook.exit(str(e))
```

### 4. Logging Results

```python
# Save validation results for audit trail
validation_history = spark.createDataFrame([{
    "validation_date": datetime.now(),
    "table_name": target_table,
    "validation_passed": results["success"],
    "total_checks": results["statistics"]["evaluated_expectations"],
    "failed_checks": results["statistics"]["unsuccessful_expectations"]
}])

validation_history.write.mode("append") \
    .saveAsTable("monitoring.dq_validation_history")
```

### 5. Performance Optimization

```python
# For large datasets, sample before validation
if df.count() > 1000000:
    df_sample = df.sample(fraction=0.1)
    results = runner.validate_spark_dataframe(df_sample)
else:
    results = runner.validate_spark_dataframe(df)
```

## Troubleshooting

### Issue: Import errors in Fabric notebook

**Solution**:
```python
# Verify path
import sys
print(sys.path)

# Verify files exist
from notebookutils import mssparkutils
files = mssparkutils.fs.ls('/Workspace/shared/fabric_data_quality')
print(files)
```

### Issue: Config file not found

**Solution**:
```python
# Use absolute paths
config_path = "/Workspace/shared/fabric_data_quality/examples/my_config.yml"

# Verify file exists
try:
    with open(config_path, 'r') as f:
        print("✅ Config file found")
except FileNotFoundError:
    print("❌ Config file not found")
    print(f"Looked in: {config_path}")
```

### Issue: Spark context not available

**Solution**:
```python
# Fabric provides spark automatically
# Verify it's available
try:
    print(f"Spark version: {spark.version}")
    print("✅ Spark available")
except NameError:
    print("❌ Spark not available - restart kernel")
```

## Advanced Integration Patterns

### Pattern: Conditional Processing Based on Quality

```python
results = validator.validate(df)

if results['success_rate'] >= 95:
    # High quality: Standard processing
    process_standard(df)
elif results['success_rate'] >= 80:
    # Medium quality: Apply corrections
    df_corrected = apply_data_corrections(df)
    process_standard(df_corrected)
elif results['success_rate'] >= 60:
    # Low quality: Manual review required
    save_for_manual_review(df)
    send_alert("Data quality requires manual review")
else:
    # Very low quality: Reject batch
    raise ValueError("Data quality unacceptable - rejecting batch")
```

### Pattern: Incremental Validation

```python
# Validate only new/changed records in incremental loads
previous_batch = spark.read.table("metadata").toPandas()
last_processed_date = previous_batch['max_date'].max()

new_records = df[df['date'] > last_processed_date]

if len(new_records) > 0:
    validator = FabricDataQualityValidator('Files/dq_configs/incremental_validation.yml')
    results = validator.validate(new_records, fail_on_error=True)
    spark.createDataFrame(new_records).write.mode("append").saveAsTable("my_silver")
```

### Pattern: Quality Monitoring Dashboard

```python
def log_quality_metrics(layer: str, dataset: str, results: dict, row_count: int):
    """Log quality metrics for Power BI dashboard"""
    metrics = {
        'timestamp': datetime.now(),
        'layer': layer,
        'dataset': dataset,
        'success_rate': results['success_rate'],
        'passed_checks': results['passed_count'],
        'failed_checks': results['failed_count'],
        'row_count': row_count,
    }
    spark.createDataFrame([metrics]).write.mode("append").saveAsTable("dq_monitoring_metrics")

# Use in pipeline
results = validator.validate(df)
log_quality_metrics('silver', 'causeway', results, len(df))
```

## Complete ETL Pipeline Example (CAUSEWAY)

```python
# Notebook: CAUSEWAY_ETL_Pipeline
%run DQ_Module
from pyspark.sql import functions as F

# === BRONZE LAYER ===
raw_df = spark.read.csv("Files/raw_data/causeway_data.csv",
                        header=True, inferSchema=True, encoding="latin-1").toPandas()

validator_bronze = FabricDataQualityValidator('Files/dq_configs/causeway_bronze_validation.yml')
results_bronze = validator_bronze.validate(raw_df, fail_on_error=False)

if results_bronze['success_rate'] < 50:
    raise ValueError(f"Bronze quality too low: {results_bronze['success_rate']:.1f}%")
spark.createDataFrame(raw_df).write.mode("overwrite").saveAsTable("causeway_bronze")

# === SILVER LAYER ===
df_silver = raw_df.copy()
df_silver.columns = [col.strip().replace(' ', '_').lower() for col in df_silver.columns]

validator_silver = FabricDataQualityValidator('Files/dq_configs/causeway_silver_validation.yml')
results_silver = validator_silver.validate(df_silver, fail_on_error=True)
spark.createDataFrame(df_silver).write.mode("overwrite").saveAsTable("causeway_silver")

# === GOLD LAYER ===
df_gold = df_silver.groupby(['vendor_name']).agg({'base_value': 'sum'}).reset_index()

validator_gold = FabricDataQualityValidator('Files/dq_configs/causeway_gold_validation.yml')
results_gold = validator_gold.validate(df_gold, fail_on_error=True)
spark.createDataFrame(df_gold).write.mode("overwrite").saveAsTable("causeway_gold")

print(f"Bronze: {results_bronze['success_rate']:.1f}%")
print(f"Silver: {results_silver['success_rate']:.1f}%")
print(f"Gold: {results_gold['success_rate']:.1f}%")
```

## Integration Workflow Overview

```
+------------------------------------------------------------------+
|                     LOCAL DEVELOPMENT                              |
+------------------------------------------------------------------+
|  1. Profile your data:                                            |
|     python scripts/profile_data.py data.csv --output config.yml   |
|  2. Review & enhance the generated config                         |
+------------------------------------------------------------------+
                            v Upload
+------------------------------------------------------------------+
|                    MS FABRIC LAKEHOUSE                             |
+------------------------------------------------------------------+
|  Files/dq_configs/    <- Upload configs here                      |
|  Notebooks/DQ_Module  <- Validator class                          |
|  Tables/              <- Bronze/Silver/Gold layers                |
+------------------------------------------------------------------+
                            v Query
+------------------------------------------------------------------+
|                        POWER BI                                    |
+------------------------------------------------------------------+
|  Dashboard: Data Quality Monitoring                                |
|  - Quality trends, failed checks, data volume metrics              |
+------------------------------------------------------------------+
```

### Validation Strategies by Layer

| Layer  | Strategy    | Threshold | Fail? | Purpose                          |
|--------|-------------|-----------|-------|----------------------------------|
| Bronze | Lenient     | 50-60%    | No    | Catch obvious issues, log        |
| Silver | Strict      | 80-90%    | Yes   | Ensure clean, standardized data  |
| Gold   | Very Strict | 95-100%   | Yes   | Perfect business metrics         |

## Configuration Management in Fabric

```
# Store configs in Lakehouse Files with versioning
Files/
  dq_configs/
    causeway/
      v1_bronze_validation.yml
      v1_silver_validation.yml
      v1_gold_validation.yml
    hss/
      v1_incidents_validation.yml
```

## Error Handling and Alerts

```python
from notebookutils import mssparkutils

def send_quality_alert(results: dict, dataset: str, layer: str):
    """Send email alert on quality failure"""
    if results['success_rate'] < 80:
        message = f"Data Quality Alert: {dataset} - {layer} Layer\n"
        message += f"Success Rate: {results['success_rate']:.1f}%\n"
        message += f"Passed: {results['passed_count']}, Failed: {results['failed_count']}"
        mssparkutils.notify.sendEmail(
            recipients=['data-team@example.com'],
            subject=f'DQ Alert: {dataset} - {layer}',
            body=message
        )
```

## Next Steps

- Review [Usage Examples](../examples/usage_examples.py)
- Check [Configuration Guide](CONFIGURATION_GUIDE.md)
- See project-specific examples in `examples/` folder
- See [Quick Start Guide](FABRIC_QUICK_START.md) for 5-minute setup
- See [Profiling Workflow](PROFILING_WORKFLOW.md) for data profiling
