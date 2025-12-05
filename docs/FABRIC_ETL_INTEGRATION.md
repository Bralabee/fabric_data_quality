# MS Fabric ETL Integration Guide

## Overview

This guide shows you how to integrate the Data Quality framework into your Microsoft Fabric ETL pipelines to validate data at every stage (Bronze → Silver → Gold).

---

## Architecture: Data Quality in Fabric ETL

```
┌─────────────────────────────────────────────────────────────────┐
│                    MS Fabric Lakehouse                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Source Data → [DQ Check] → Bronze → [DQ Check] → Silver →     │
│                                       [DQ Check] → Gold          │
│                                                                  │
│  Each stage:                                                     │
│  1. Load data                                                    │
│  2. Run quality validation                                       │
│  3. Log results                                                  │
│  4. Decide: Continue or Fail                                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Quick Start: 3 Steps to Integration

### Step 1: Upload Framework to Fabric

```python
# In your Fabric workspace, create a notebook and install the framework
%pip install pyyaml great_expectations==0.18.22 pandas pyarrow

# Upload your validation configs to Lakehouse Files section
# Example structure:
# Files/
#   dq_configs/
#     causeway_bronze_validation.yml
#     causeway_silver_validation.yml
#     causeway_gold_validation.yml
```

### Step 2: Create Reusable DQ Module

Create a notebook called `DQ_Module` with this code:

```python
# DQ_Module notebook
import pandas as pd
from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core import ExpectationConfiguration
import yaml
from datetime import datetime

class FabricDataQualityValidator:
    """Data Quality validator for MS Fabric pipelines"""
    
    def __init__(self, config_path: str):
        """
        Initialize validator with config from Lakehouse Files
        
        Args:
            config_path: Path to YAML config in Lakehouse
                        e.g., 'Files/dq_configs/causeway_bronze_validation.yml'
        """
        self.config = self._load_config(config_path)
        self.context = EphemeralDataContext()
        
    def _load_config(self, path: str) -> dict:
        """Load config from Lakehouse Files"""
        with open(f"/lakehouse/default/{path}", 'r') as f:
            return yaml.safe_load(f)
    
    def validate(self, df: pd.DataFrame, fail_on_error: bool = False) -> dict:
        """
        Validate DataFrame against expectations
        
        Args:
            df: DataFrame to validate
            fail_on_error: If True, raise exception on validation failure
            
        Returns:
            dict: Validation results with success rate and details
        """
        # Create expectation suite
        suite = ExpectationSuite(
            expectation_suite_name=self.config.get('validation_name', 'validation')
        )
        
        # Add expectations from config
        for exp in self.config.get('expectations', []):
            suite.add_expectation(ExpectationConfiguration(**exp))
        
        # Add suite to context
        self.context.add_expectation_suite(expectation_suite=suite)
        
        # Create batch and validate
        batch = self.context.sources.add_pandas("pandas_source")\
            .add_dataframe_asset(name="df_asset")\
            .add_batch_definition_whole_dataframe("batch_def")\
            .get_batch(batch_parameters={"dataframe": df})
        
        validation_result = batch.validate(suite)
        
        # Parse results
        results = {
            'success': validation_result.success,
            'timestamp': datetime.now().isoformat(),
            'statistics': validation_result.statistics,
            'results': []
        }
        
        for result in validation_result.results:
            results['results'].append({
                'expectation_type': result.expectation_config.expectation_type,
                'success': result.success,
                'column': result.expectation_config.kwargs.get('column'),
                'details': str(result.result) if hasattr(result, 'result') else None
            })
        
        # Calculate metrics
        total = len(results['results'])
        passed = sum(1 for r in results['results'] if r['success'])
        results['success_rate'] = (passed / total * 100) if total > 0 else 0
        results['passed_count'] = passed
        results['failed_count'] = total - passed
        
        # Log to Fabric
        self._log_results(results)
        
        # Fail pipeline if needed
        if fail_on_error and not results['success']:
            raise ValueError(f"Data quality validation failed! Success rate: {results['success_rate']:.1f}%")
        
        return results
    
    def _log_results(self, results: dict):
        """Log validation results to Lakehouse"""
        log_path = f"/lakehouse/default/Files/dq_logs/validation_{results['timestamp']}.json"
        
        import json
        import os
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        
        with open(log_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"✅ Validation results logged to: {log_path}")
```

### Step 3: Use in Your ETL Notebooks

```python
# In your ETL notebook (e.g., CAUSEWAY_Bronze_to_Silver)

# Import the DQ module
%run DQ_Module

# Your ETL code
df = spark.read.csv("Files/raw_data/causeway_data.csv").toPandas()

# VALIDATE at Bronze layer
validator = FabricDataQualityValidator('Files/dq_configs/causeway_bronze_validation.yml')
results = validator.validate(df, fail_on_error=False)

print(f"Bronze Layer Quality: {results['success_rate']:.1f}%")
print(f"Passed: {results['passed_count']}, Failed: {results['failed_count']}")

# If quality is acceptable, continue to Silver
if results['success_rate'] >= 70:
    # Transform data
    df_silver = transform_to_silver(df)
    
    # VALIDATE at Silver layer
    validator_silver = FabricDataQualityValidator('Files/dq_configs/causeway_silver_validation.yml')
    results_silver = validator_silver.validate(df_silver, fail_on_error=True)
    
    # Save to Silver layer
    spark.createDataFrame(df_silver).write.mode("overwrite").saveAsTable("causeway_silver")
else:
    raise ValueError("Bronze layer quality below threshold!")
```

---

## Integration Patterns

### Pattern 1: Bronze Layer Validation (Lenient)

**Purpose:** Catch obvious data issues early
**Strategy:** Lenient validation, log issues but continue

```python
# Bronze: Validate raw ingested data
# Config: causeway_bronze_validation.yml (null_tolerance=70%, severity=low)

validator = FabricDataQualityValidator('Files/dq_configs/causeway_bronze_validation.yml')
results = validator.validate(raw_df, fail_on_error=False)

# Log quality metrics to monitoring table
quality_metrics = spark.createDataFrame([{
    'layer': 'bronze',
    'dataset': 'causeway',
    'timestamp': results['timestamp'],
    'success_rate': results['success_rate'],
    'row_count': len(raw_df)
}])
quality_metrics.write.mode("append").saveAsTable("dq_monitoring")

# Continue even if some checks fail
print(f"Bronze Quality: {results['success_rate']:.1f}% - Proceeding to transformation")
```

### Pattern 2: Silver Layer Validation (Strict)

**Purpose:** Ensure cleaned data meets business rules
**Strategy:** Strict validation, fail pipeline on critical issues

```python
# Silver: Validate cleaned/transformed data
# Config: causeway_silver_validation.yml (null_tolerance=30%, severity=high)

validator = FabricDataQualityValidator('Files/dq_configs/causeway_silver_validation.yml')

try:
    results = validator.validate(cleaned_df, fail_on_error=True)
    print(f"✅ Silver validation passed: {results['success_rate']:.1f}%")
    
    # Save to Silver layer
    spark.createDataFrame(cleaned_df).write.mode("overwrite").saveAsTable("causeway_silver")
    
except ValueError as e:
    # Log failure and stop pipeline
    print(f"❌ Silver validation failed: {e}")
    raise
```

### Pattern 3: Gold Layer Validation (Business Rules)

**Purpose:** Validate aggregated metrics and business KPIs
**Strategy:** Custom business rules validation

```python
# Gold: Validate business metrics
# Config: causeway_gold_validation.yml (custom business rules)

validator = FabricDataQualityValidator('Files/dq_configs/causeway_gold_validation.yml')
results = validator.validate(gold_df, fail_on_error=True)

# Additional business rule checks
assert gold_df['total_value'].sum() > 0, "Total value cannot be zero"
assert gold_df['transaction_count'].min() >= 0, "Negative transaction count detected"

print(f"✅ Gold layer validated and ready for reporting")
```

---

## Complete ETL Pipeline Example

### Scenario: CAUSEWAY Financial Data Pipeline

```python
# Notebook: CAUSEWAY_ETL_Pipeline
# Purpose: Ingest, clean, aggregate CAUSEWAY financial data with DQ checks

%run DQ_Module

from pyspark.sql import functions as F
from datetime import datetime

# ============================================================================
# STAGE 1: BRONZE LAYER - Raw Data Ingestion
# ============================================================================
print("=" * 80)
print("STAGE 1: BRONZE LAYER - Raw Data Ingestion")
print("=" * 80)

# Load raw data
raw_df = spark.read.csv(
    "Files/raw_data/causeway_combined_scr_2024.csv",
    header=True,
    inferSchema=True,
    encoding="latin-1"
).toPandas()

print(f"Loaded {len(raw_df)} rows from source")

# DQ Check: Bronze Layer
validator_bronze = FabricDataQualityValidator('Files/dq_configs/causeway_bronze_validation.yml')
results_bronze = validator_bronze.validate(raw_df, fail_on_error=False)

print(f"Bronze Quality Score: {results_bronze['success_rate']:.1f}%")

# Decision: Continue only if basic quality threshold met
if results_bronze['success_rate'] < 50:
    raise ValueError(f"Bronze quality too low: {results_bronze['success_rate']:.1f}%")

# Save to Bronze
spark.createDataFrame(raw_df).write.mode("overwrite").saveAsTable("causeway_bronze")
print("✅ Bronze layer saved")

# ============================================================================
# STAGE 2: SILVER LAYER - Data Cleaning
# ============================================================================
print("\n" + "=" * 80)
print("STAGE 2: SILVER LAYER - Data Cleaning")
print("=" * 80)

# Transform: Clean and standardize
df_silver = raw_df.copy()

# Remove fully null columns
null_cols = [col for col in df_silver.columns if df_silver[col].isna().all()]
df_silver = df_silver.drop(columns=null_cols)
print(f"Removed {len(null_cols)} empty columns")

# Standardize column names
df_silver.columns = [col.strip().replace(' ', '_').lower() for col in df_silver.columns]

# Parse dates
date_columns = ['cost_date', 'accounting_period_yyyy/pp']
for col in date_columns:
    if col in df_silver.columns:
        df_silver[col] = pd.to_datetime(df_silver[col], errors='coerce')

# DQ Check: Silver Layer (STRICT)
validator_silver = FabricDataQualityValidator('Files/dq_configs/causeway_silver_validation.yml')
results_silver = validator_silver.validate(df_silver, fail_on_error=True)

print(f"Silver Quality Score: {results_silver['success_rate']:.1f}%")

# Save to Silver
spark.createDataFrame(df_silver).write.mode("overwrite").saveAsTable("causeway_silver")
print("✅ Silver layer saved")

# ============================================================================
# STAGE 3: GOLD LAYER - Business Metrics
# ============================================================================
print("\n" + "=" * 80)
print("STAGE 3: GOLD LAYER - Business Metrics")
print("=" * 80)

# Aggregate: Monthly spending by vendor
df_gold = df_silver.groupby(['vendor_name', 'accounting_period_yyyy/pp']).agg({
    'base_value': 'sum',
    'quantity': 'sum',
    'reference': 'count'
}).reset_index()

df_gold.columns = ['vendor_name', 'period', 'total_value', 'total_quantity', 'transaction_count']

# DQ Check: Gold Layer (Business Rules)
validator_gold = FabricDataQualityValidator('Files/dq_configs/causeway_gold_validation.yml')
results_gold = validator_gold.validate(df_gold, fail_on_error=True)

print(f"Gold Quality Score: {results_gold['success_rate']:.1f}%")

# Additional business validation
assert df_gold['total_value'].sum() > 0, "Total spend is zero!"
assert not df_gold['vendor_name'].isna().any(), "Missing vendor names in gold layer"

# Save to Gold
spark.createDataFrame(df_gold).write.mode("overwrite").saveAsTable("causeway_gold")
print("✅ Gold layer saved")

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("PIPELINE COMPLETE - QUALITY SUMMARY")
print("=" * 80)
print(f"Bronze Quality: {results_bronze['success_rate']:.1f}%")
print(f"Silver Quality: {results_silver['success_rate']:.1f}%")
print(f"Gold Quality:   {results_gold['success_rate']:.1f}%")
print(f"\nFinal Record Count: {len(df_gold)} aggregated records")
print("=" * 80)
```

---

## Advanced Patterns

### Pattern 4: Conditional Processing Based on Quality

```python
# Use quality scores to determine processing path

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

### Pattern 5: Incremental Validation

```python
# Validate only new/changed records in incremental loads

# Load previous batch metadata
previous_batch = spark.read.table("causeway_silver_metadata").toPandas()
last_processed_date = previous_batch['max_date'].max()

# Get new records only
new_records = df[df['date'] > last_processed_date]

if len(new_records) > 0:
    # Validate only new records
    validator = FabricDataQualityValidator('Files/dq_configs/causeway_incremental_validation.yml')
    results = validator.validate(new_records, fail_on_error=True)
    
    print(f"Validated {len(new_records)} new records: {results['success_rate']:.1f}%")
    
    # Append to Silver
    spark.createDataFrame(new_records).write.mode("append").saveAsTable("causeway_silver")
```

### Pattern 6: Quality Monitoring Dashboard

```python
# Create monitoring table for Power BI dashboard

def log_quality_metrics(layer: str, dataset: str, results: dict, row_count: int):
    """Log quality metrics for monitoring"""
    
    metrics = {
        'timestamp': datetime.now(),
        'layer': layer,
        'dataset': dataset,
        'success_rate': results['success_rate'],
        'passed_checks': results['passed_count'],
        'failed_checks': results['failed_count'],
        'row_count': row_count,
        'validation_config': results.get('config_name', 'unknown')
    }
    
    # Append to monitoring table
    spark.createDataFrame([metrics]).write.mode("append").saveAsTable("dq_monitoring_metrics")
    
    # Track failed expectations
    for result in results['results']:
        if not result['success']:
            failure_detail = {
                'timestamp': datetime.now(),
                'layer': layer,
                'dataset': dataset,
                'expectation': result['expectation_type'],
                'column': result.get('column'),
                'details': result.get('details')
            }
            spark.createDataFrame([failure_detail]).write.mode("append").saveAsTable("dq_failures")

# Use in pipeline
results = validator.validate(df)
log_quality_metrics('silver', 'causeway', results, len(df))
```

---

## Configuration Management

### Best Practices for Fabric

```python
# Store configs in Lakehouse Files with versioning
Files/
  dq_configs/
    causeway/
      v1_bronze_validation.yml
      v1_silver_validation.yml
      v1_gold_validation.yml
    hss/
      v1_incidents_validation.yml
    aims/
      v1_bronze_validation.yml
```

### Config Loader with Version Control

```python
class FabricConfigLoader:
    """Load configs with version support"""
    
    def load_config(self, dataset: str, layer: str, version: str = 'v1'):
        """
        Load versioned config from Lakehouse
        
        Args:
            dataset: 'causeway', 'hss', 'aims'
            layer: 'bronze', 'silver', 'gold'
            version: 'v1', 'v2', etc.
        """
        path = f"/lakehouse/default/Files/dq_configs/{dataset}/{version}_{layer}_validation.yml"
        with open(path, 'r') as f:
            return yaml.safe_load(f)

# Usage
loader = FabricConfigLoader()
config = loader.load_config('causeway', 'silver', 'v1')
```

---

## Fabric Pipeline Integration

### Using Fabric Data Pipelines

```python
# Create a Fabric Pipeline with multiple activities:

# Activity 1: Run Bronze Validation Notebook
# - Notebook: DQ_Bronze_Validation
# - Output: validation_results_bronze

# Activity 2: Conditional Activity
# - Condition: @equals(activity('DQ_Bronze_Validation').output.success, true)
# - If True: Continue to Silver transformation
# - If False: Send failure notification

# Activity 3: Run Silver Transformation Notebook
# - Notebook: Transform_to_Silver
# - Includes DQ validation at end

# Activity 4: Run Gold Aggregation Notebook
# - Notebook: Create_Gold_Layer
# - Final DQ validation

# Activity 5: Send Success Notification
# - Teams/Email notification
```

### Pipeline Notebook Template

```python
# Notebook: DQ_Bronze_Validation
# Returns results to pipeline

%run DQ_Module

# Validate
validator = FabricDataQualityValidator('Files/dq_configs/causeway_bronze_validation.yml')
df = spark.read.table("causeway_bronze").toPandas()
results = validator.validate(df, fail_on_error=False)

# Return results to pipeline
import json
mssparkutils.notebook.exit(json.dumps({
    'success': results['success'],
    'success_rate': results['success_rate'],
    'row_count': len(df)
}))
```

---

## Error Handling and Alerts

### Email Alerts on Failure

```python
from notebookutils import mssparkutils

def send_quality_alert(results: dict, dataset: str, layer: str):
    """Send email alert on quality failure"""
    
    if results['success_rate'] < 80:
        message = f"""
        Data Quality Alert: {dataset} - {layer} Layer
        
        Success Rate: {results['success_rate']:.1f}%
        Passed: {results['passed_count']}
        Failed: {results['failed_count']}
        
        Failed Checks:
        {format_failed_checks(results)}
        
        Please review the data quality logs in Lakehouse.
        """
        
        # Send via Fabric email integration
        mssparkutils.notify.sendEmail(
            recipients=['data-team@hs2.org.uk'],
            subject=f'DQ Alert: {dataset} - {layer}',
            body=message
        )

# Use in validation
results = validator.validate(df)
send_quality_alert(results, 'causeway', 'silver')
```

---

## Testing in Fabric

### Test Your Integration

```python
# Test notebook: Test_DQ_Integration

%run DQ_Module

# Test 1: Load config
try:
    validator = FabricDataQualityValidator('Files/dq_configs/causeway_bronze_validation.yml')
    print("✅ Config loaded successfully")
except Exception as e:
    print(f"❌ Config load failed: {e}")

# Test 2: Create sample data
import pandas as pd
test_df = pd.DataFrame({
    'reference': [1, 2, 3],
    'base_value': [100.0, 200.0, 300.0],
    'vendor_name': ['Vendor A', 'Vendor B', 'Vendor C']
})

# Test 3: Run validation
try:
    results = validator.validate(test_df, fail_on_error=False)
    print(f"✅ Validation executed: {results['success_rate']:.1f}%")
except Exception as e:
    print(f"❌ Validation failed: {e}")

# Test 4: Check logging
import os
if os.path.exists('/lakehouse/default/Files/dq_logs'):
    print("✅ Logging directory exists")
else:
    print("❌ Logging directory missing")
```

---

## Summary: Integration Checklist

- [ ] **Step 1:** Profile your data once using `profile_data.py`
- [ ] **Step 2:** Upload generated YAML configs to Fabric Lakehouse Files
- [ ] **Step 3:** Create `DQ_Module` notebook with validator class
- [ ] **Step 4:** Integrate DQ checks into Bronze layer (lenient)
- [ ] **Step 5:** Integrate DQ checks into Silver layer (strict)
- [ ] **Step 6:** Integrate DQ checks into Gold layer (business rules)
- [ ] **Step 7:** Set up quality monitoring table
- [ ] **Step 8:** Configure alerts for failures
- [ ] **Step 9:** Test end-to-end pipeline
- [ ] **Step 10:** Document for your team

---

## Next Steps

1. **Start with one dataset:** Use CAUSEWAY as pilot project
2. **Test in dev environment:** Validate integration works
3. **Set up monitoring:** Create Power BI dashboard for quality metrics
4. **Expand gradually:** Add HSS, AIMS, ACA projects one by one
5. **Refine configs:** Update validation rules based on learnings

**Questions?** See `PROFILING_WORKFLOW.md` for profiling guidance or `QUICK_REFERENCE.md` for API details.
