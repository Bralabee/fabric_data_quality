"""
MS Fabric ETL Pipeline Example with Data Quality Integration

This example demonstrates a complete ETL pipeline with DQ checks at each stage.
Copy this code into a Fabric notebook to use it.

Author: Data Quality Framework
Date: 2025-10-28
"""

# ============================================================================
# FABRIC NOTEBOOK SETUP
# ============================================================================

# Cell 1: Install dependencies (run once)
# %pip install pyyaml great_expectations==0.18.22 pandas pyarrow

# Cell 2: Import libraries
import pandas as pd
from pyspark.sql import functions as F
from datetime import datetime
import yaml
from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core import ExpectationConfiguration

# ============================================================================
# DATA QUALITY VALIDATOR CLASS
# ============================================================================

# Cell 3: Define validator class
class FabricDataQualityValidator:
    """
    Lightweight Data Quality validator for MS Fabric pipelines
    
    Usage:
        validator = FabricDataQualityValidator('Files/dq_configs/my_validation.yml')
        results = validator.validate(df, fail_on_error=True)
    """
    
    def __init__(self, config_path: str):
        """
        Initialize validator with config from Lakehouse Files
        
        Args:
            config_path: Relative path from /lakehouse/default/
                        e.g., 'Files/dq_configs/causeway_bronze_validation.yml'
        """
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.context = EphemeralDataContext()
        print(f"✓ Loaded config: {self.config.get('validation_name', 'unnamed')}")
        
    def _load_config(self, path: str) -> dict:
        """Load YAML config from Lakehouse Files"""
        try:
            # Try absolute path first
            full_path = f"/lakehouse/default/{path}"
            with open(full_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Fallback to relative path
            with open(path, 'r') as f:
                return yaml.safe_load(f)
    
    def validate(self, df: pd.DataFrame, fail_on_error: bool = False) -> dict:
        """
        Validate DataFrame against expectations
        
        Args:
            df: Pandas DataFrame to validate
            fail_on_error: If True, raise exception on validation failure
            
        Returns:
            dict: Validation results with success rate and details
            
        Example:
            results = validator.validate(df, fail_on_error=True)
            print(f"Quality: {results['success_rate']:.1f}%")
        """
        print(f"\n{'='*60}")
        print(f"Validating: {self.config.get('validation_name', 'unnamed')}")
        print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")
        print(f"{'='*60}")
        
        # Create expectation suite
        suite_name = self.config.get('validation_name', 'validation_suite')
        suite = ExpectationSuite(expectation_suite_name=suite_name)
        
        # Add expectations from config
        expectations = self.config.get('expectations', [])
        for exp in expectations:
            suite.add_expectation(ExpectationConfiguration(**exp))
        
        print(f"Running {len(expectations)} expectations...")
        
        # Add suite to context
        self.context.add_expectation_suite(expectation_suite=suite)
        
        # Create batch and validate
        try:
            batch = self.context.sources.add_pandas("pandas_source")\
                .add_dataframe_asset(name="df_asset")\
                .add_batch_definition_whole_dataframe("batch_def")\
                .get_batch(batch_parameters={"dataframe": df})
            
            validation_result = batch.validate(suite)
        except Exception as e:
            print(f"❌ Validation error: {e}")
            raise
        
        # Parse results
        results = {
            'success': validation_result.success,
            'timestamp': datetime.now().isoformat(),
            'config_name': suite_name,
            'data_rows': len(df),
            'data_columns': len(df.columns),
            'statistics': validation_result.statistics,
            'results': []
        }
        
        # Extract individual results
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
        
        # Print summary
        self._print_summary(results)
        
        # Log to Lakehouse
        try:
            self._log_results(results)
        except Exception as e:
            print(f"⚠️  Logging failed: {e}")
        
        # Fail pipeline if needed
        if fail_on_error and not results['success']:
            raise ValueError(
                f"Data quality validation failed!\n"
                f"Success rate: {results['success_rate']:.1f}%\n"
                f"Failed checks: {results['failed_count']}/{total}"
            )
        
        return results
    
    def _print_summary(self, results: dict):
        """Print validation summary"""
        print(f"\n{'='*60}")
        
        status_icon = "✅" if results['success'] else "❌"
        print(f"{status_icon} VALIDATION SUMMARY")
        print(f"{'='*60}")
        print(f"Success Rate:    {results['success_rate']:>6.1f}%")
        print(f"Passed:          {results['passed_count']:>6}")
        print(f"Failed:          {results['failed_count']:>6}")
        print(f"Total Checks:    {results['passed_count'] + results['failed_count']:>6}")
        
        # Show failed checks
        if results['failed_count'] > 0:
            print(f"\n❌ Failed Expectations:")
            for i, result in enumerate(results['results'], 1):
                if not result['success']:
                    exp_type = result['expectation_type']
                    column = result.get('column', 'N/A')
                    print(f"   {i}. {exp_type} (column: {column})")
        
        print(f"{'='*60}\n")
    
    def _log_results(self, results: dict):
        """Log validation results to Lakehouse Files"""
        import json
        import os
        
        # Create log directory
        log_dir = "/lakehouse/default/Files/dq_logs"
        os.makedirs(log_dir, exist_ok=True)
        
        # Save results
        timestamp = results['timestamp'].replace(':', '-').replace('.', '-')
        log_file = f"{log_dir}/validation_{timestamp}.json"
        
        with open(log_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"📝 Results logged: {log_file}")


# ============================================================================
# EXAMPLE PIPELINE: CAUSEWAY ETL
# ============================================================================

# Cell 4: Configuration
DATASET_NAME = "causeway"
SOURCE_PATH = "Files/raw_data/causeway_combined_scr_2024.csv"
CONFIG_BRONZE = f"Files/dq_configs/{DATASET_NAME}_bronze_validation.yml"
CONFIG_SILVER = f"Files/dq_configs/{DATASET_NAME}_silver_validation.yml"
CONFIG_GOLD = f"Files/dq_configs/{DATASET_NAME}_gold_validation.yml"

# Quality thresholds
BRONZE_MIN_QUALITY = 50.0  # Lenient for raw data
SILVER_MIN_QUALITY = 80.0  # Strict for cleaned data
GOLD_MIN_QUALITY = 95.0    # Very strict for business metrics

print(f"Pipeline Configuration")
print(f"Dataset: {DATASET_NAME}")
print(f"Bronze threshold: {BRONZE_MIN_QUALITY}%")
print(f"Silver threshold: {SILVER_MIN_QUALITY}%")
print(f"Gold threshold: {GOLD_MIN_QUALITY}%")


# Cell 5: BRONZE LAYER - Ingestion with validation
print("\n" + "="*80)
print("STAGE 1: BRONZE LAYER - Raw Data Ingestion")
print("="*80)

# Load raw data (use Spark for large files)
df_bronze = spark.read.csv(
    SOURCE_PATH,
    header=True,
    inferSchema=True,
    encoding="latin-1"
).toPandas()

print(f"✓ Loaded {len(df_bronze):,} rows from source")

# Validate Bronze layer
validator_bronze = FabricDataQualityValidator(CONFIG_BRONZE)
results_bronze = validator_bronze.validate(df_bronze, fail_on_error=False)

# Decision: Continue only if meets minimum threshold
if results_bronze['success_rate'] < BRONZE_MIN_QUALITY:
    raise ValueError(
        f"Bronze quality too low: {results_bronze['success_rate']:.1f}% "
        f"(minimum: {BRONZE_MIN_QUALITY}%)"
    )

# Save to Bronze table
spark.createDataFrame(df_bronze).write.mode("overwrite").saveAsTable(f"{DATASET_NAME}_bronze")
print(f"✅ Bronze layer saved: {DATASET_NAME}_bronze")


# Cell 6: SILVER LAYER - Transformation with validation
print("\n" + "="*80)
print("STAGE 2: SILVER LAYER - Data Cleaning & Transformation")
print("="*80)

# Start with Bronze data
df_silver = df_bronze.copy()

# Data cleaning operations
print("Applying transformations...")

# 1. Remove empty columns
null_cols = [col for col in df_silver.columns if df_silver[col].isna().all()]
if null_cols:
    df_silver = df_silver.drop(columns=null_cols)
    print(f"  ✓ Removed {len(null_cols)} empty columns")

# 2. Standardize column names
df_silver.columns = [
    col.strip().replace(' ', '_').replace(':', '').lower() 
    for col in df_silver.columns
]
print(f"  ✓ Standardized column names")

# 3. Parse dates
date_columns = ['cost_date', 'date_reported', 'date_record_created']
for col in date_columns:
    if col in df_silver.columns:
        df_silver[col] = pd.to_datetime(df_silver[col], errors='coerce')
print(f"  ✓ Parsed date columns")

# 4. Remove duplicates
initial_rows = len(df_silver)
df_silver = df_silver.drop_duplicates()
if len(df_silver) < initial_rows:
    print(f"  ✓ Removed {initial_rows - len(df_silver)} duplicates")

print(f"\nCleaned data: {len(df_silver):,} rows, {len(df_silver.columns)} columns")

# Validate Silver layer (STRICT)
validator_silver = FabricDataQualityValidator(CONFIG_SILVER)
results_silver = validator_silver.validate(df_silver, fail_on_error=False)

# Enforce strict threshold
if results_silver['success_rate'] < SILVER_MIN_QUALITY:
    raise ValueError(
        f"Silver quality below threshold: {results_silver['success_rate']:.1f}% "
        f"(minimum: {SILVER_MIN_QUALITY}%)"
    )

# Save to Silver table
spark.createDataFrame(df_silver).write.mode("overwrite").saveAsTable(f"{DATASET_NAME}_silver")
print(f"✅ Silver layer saved: {DATASET_NAME}_silver")


# Cell 7: GOLD LAYER - Aggregation with validation
print("\n" + "="*80)
print("STAGE 3: GOLD LAYER - Business Metrics & Aggregation")
print("="*80)

# Business aggregation: Monthly spend by vendor
print("Creating business metrics...")

# Group by vendor and period
df_gold = df_silver.groupby(['vendor_name', 'accounting_period_yyyy/pp']).agg({
    'base_value': 'sum',
    'quantity': 'sum',
    'reference': 'count'
}).reset_index()

# Rename for clarity
df_gold.columns = [
    'vendor_name',
    'period',
    'total_value',
    'total_quantity',
    'transaction_count'
]

# Add derived metrics
df_gold['avg_transaction_value'] = df_gold['total_value'] / df_gold['transaction_count']
df_gold['created_date'] = datetime.now()

print(f"  ✓ Aggregated to {len(df_gold):,} vendor-period combinations")

# Validate Gold layer (VERY STRICT)
validator_gold = FabricDataQualityValidator(CONFIG_GOLD)
results_gold = validator_gold.validate(df_gold, fail_on_error=False)

# Additional business rules validation
print("\nApplying business rules...")
assert df_gold['total_value'].sum() > 0, "Total spend cannot be zero"
assert df_gold['transaction_count'].min() >= 0, "Negative transaction count detected"
assert not df_gold['vendor_name'].isna().any(), "Missing vendor names"
print("  ✓ Business rules validated")

# Enforce gold threshold
if results_gold['success_rate'] < GOLD_MIN_QUALITY:
    raise ValueError(
        f"Gold quality below threshold: {results_gold['success_rate']:.1f}% "
        f"(minimum: {GOLD_MIN_QUALITY}%)"
    )

# Save to Gold table
spark.createDataFrame(df_gold).write.mode("overwrite").saveAsTable(f"{DATASET_NAME}_gold")
print(f"✅ Gold layer saved: {DATASET_NAME}_gold")


# Cell 8: Pipeline Summary
print("\n" + "="*80)
print("PIPELINE COMPLETE - QUALITY SUMMARY")
print("="*80)
print(f"Dataset:        {DATASET_NAME}")
print(f"Timestamp:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\nQuality Scores:")
print(f"  Bronze:       {results_bronze['success_rate']:>6.1f}%  (threshold: {BRONZE_MIN_QUALITY}%)")
print(f"  Silver:       {results_silver['success_rate']:>6.1f}%  (threshold: {SILVER_MIN_QUALITY}%)")
print(f"  Gold:         {results_gold['success_rate']:>6.1f}%  (threshold: {GOLD_MIN_QUALITY}%)")
print(f"\nRecord Counts:")
print(f"  Bronze:       {len(df_bronze):>10,} rows")
print(f"  Silver:       {len(df_silver):>10,} rows")
print(f"  Gold:         {len(df_gold):>10,} rows")
print("="*80)

# Log pipeline summary to monitoring table
pipeline_summary = {
    'pipeline_run_id': datetime.now().isoformat(),
    'dataset': DATASET_NAME,
    'bronze_quality': results_bronze['success_rate'],
    'silver_quality': results_silver['success_rate'],
    'gold_quality': results_gold['success_rate'],
    'bronze_rows': len(df_bronze),
    'silver_rows': len(df_silver),
    'gold_rows': len(df_gold),
    'status': 'SUCCESS'
}

# Save to monitoring table
spark.createDataFrame([pipeline_summary]).write.mode("append").saveAsTable("dq_pipeline_monitoring")
print("✅ Pipeline metrics logged to dq_pipeline_monitoring")


# ============================================================================
# USAGE NOTES
# ============================================================================
"""
TO USE THIS IN MS FABRIC:

1. PREREQUISITES:
   - Upload your validation YAML configs to: Files/dq_configs/
   - Ensure source data is in: Files/raw_data/
   - Create tables: causeway_bronze, causeway_silver, causeway_gold

2. SETUP IN FABRIC:
   - Create a new notebook in your workspace
   - Copy cells 1-8 into separate notebook cells
   - Update paths and config names as needed

3. EXECUTION:
   - Run cells in order
   - Monitor quality scores at each stage
   - Check dq_pipeline_monitoring table for history

4. CUSTOMIZATION:
   - Adjust quality thresholds (BRONZE_MIN_QUALITY, etc.)
   - Modify transformations in Silver stage
   - Add custom business rules in Gold stage
   - Change fail_on_error behavior as needed

5. MONITORING:
   - View logs in: Files/dq_logs/
   - Query dq_pipeline_monitoring for trends
   - Create Power BI dashboard from monitoring table

6. ALERTS:
   - Add email notifications using mssparkutils.notify.sendEmail()
   - Integrate with Teams using webhooks
   - Set up Fabric data alerts on monitoring table

For more examples, see:
- docs/FABRIC_ETL_INTEGRATION.md
- examples/complete_workflow_example.py
"""
