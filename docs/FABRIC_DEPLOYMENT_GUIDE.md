# MS Fabric Deployment Guide - Step by Step

## 📋 Overview

This guide walks you through deploying the Data Quality Framework to Microsoft Fabric from start to finish. Follow each step in order.

**Estimated Time:** 45-60 minutes for first deployment  
**Prerequisites:** Access to MS Fabric workspace with Contributor or Admin rights

---

## 🎯 Deployment Phases

```
Phase 1: Prepare Locally (15 min)
   v
Phase 2: Setup Fabric Workspace (10 min)
   v
Phase 3: Upload Files & Configs (15 min)
   v
Phase 4: Create Validation Module (10 min)
   v
Phase 5: Test & Validate (10 min)
```

---

## PHASE 1: Prepare Locally (15 minutes)

### Step 1.1: Verify Your Environment

```bash
# Navigate to framework directory
cd /home/sanmi/Documents/HS2/HS2_PROJECTS_2025/fabric_data_quality

# Activate conda environment
conda activate fabric-dq

# Verify installation
python -c "from dq_framework import DataQualityValidator; print('✅ Framework ready')"
```

**Expected Output:** `✅ Framework ready`

**If this fails:** 
- Check environment: `conda env list`
- Reinstall: `conda create -n fabric-dq python=3.10 -y && conda activate fabric-dq && pip install -r requirements.txt`

---

### Step 1.2: Profile Your Data (If Not Already Done)

For this example, we'll use CAUSEWAY data. If you need to profile different data:

```bash
# Profile your data
python profile_data.py /path/to/your/data.csv \
    --output config/your_dataset_validation.yml \
    --name "your_dataset_validation" \
    --description "Your dataset description" \
    --null-tolerance 50 \
    --severity medium \
    --sample 50000
```

**What this does:**
- Analyzes your data structure
- Generates validation expectations
- Creates YAML configuration file

**Example output:**
```
✓ Loaded 50,000 rows and 39 columns
✓ Data Quality Score: 57.3/100
✓ Generated 65 expectations
✅ Configuration saved: config/your_dataset_validation.yml
```

---

### Step 1.3: Review and Enhance Configurations

Open the generated YAML file and review:

```bash
# View the configuration
cat config/causeway_validation.yml | head -50
```

**Key sections to review:**

1. **Validation Name** - Clear identifier
2. **Expectations** - Auto-generated validation rules
3. **Severity Levels** - critical/high/medium/low

**Enhancement example:**

```yaml
# Auto-generated (keep this)
- expectation_type: expect_column_to_exist
  kwargs:
    column: customer_id
  meta:
    severity: critical

# Add business rule (enhance with this)
- expectation_type: expect_column_values_to_be_unique
  kwargs:
    column: customer_id
  meta:
    severity: critical
    description: "Customer IDs must be unique for data integrity"
```

**Tips:**
- Add `expect_column_values_to_be_unique` for ID columns
- Add `expect_column_values_to_be_in_set` for categorical columns
- Add `expect_column_values_to_be_between` for numeric ranges
- Adjust `severity` based on business impact

---

### Step 1.4: Create Layer-Specific Configurations

Create three versions for Bronze, Silver, and Gold layers:

```bash
# Copy base config to create layer-specific versions
cp config/causeway_validation.yml config/causeway_bronze_validation.yml
cp config/causeway_validation.yml config/causeway_silver_validation.yml
cp config/causeway_validation.yml config/causeway_gold_validation.yml
```

**Edit each file:**

**Bronze (lenient):**
```yaml
validation_name: causeway_bronze_validation
description: Bronze layer - Raw data validation (lenient)
# Keep mostly parameter at 0.5 (50% threshold)
```

**Silver (strict):**
```yaml
validation_name: causeway_silver_validation
description: Silver layer - Cleaned data validation (strict)
# Change mostly parameter to 0.9 (90% threshold)
```

**Gold (very strict):**
```yaml
validation_name: causeway_gold_validation
description: Gold layer - Business metrics validation (very strict)
# Change mostly parameter to 0.95 (95% threshold)
# Add business-specific rules
```

---

### Step 1.5: Prepare Files for Upload

Create a deployment package:

```bash
# Create deployment directory
mkdir -p fabric_deployment_package

# Copy configurations
mkdir -p fabric_deployment_package/dq_configs
cp config/*_validation.yml fabric_deployment_package/dq_configs/

# Copy sample data (optional, for testing)
mkdir -p fabric_deployment_package/sample_data
cp sample_source_data/CAUSEWAY_combined_scr_2024.csv fabric_deployment_package/sample_data/

# List what you have
tree fabric_deployment_package
```

**Expected structure:**
```
fabric_deployment_package/
+-- dq_configs/
|   +-- causeway_bronze_validation.yml
|   +-- causeway_silver_validation.yml
|   +-- causeway_gold_validation.yml
|   +-- hss_incidents_validation.yml
+-- sample_data/
    +-- CAUSEWAY_combined_scr_2024.csv (optional)
```

---

## PHASE 2: Setup Fabric Workspace (10 minutes)

### Step 2.1: Access MS Fabric

1. Open browser and navigate to: **https://app.fabric.microsoft.com**
2. Sign in with your organizational credentials
3. Select your workspace (or create a new one)

**To create a new workspace:**
- Click "Workspaces" in left navigation
- Click "+ New workspace"
- Name: `HS2_Data_Quality` (or your preferred name)
- Description: "Data Quality Validation Framework"
- Click "Apply"

---

### Step 2.2: Create a Lakehouse

1. In your workspace, click **+ New**
2. Select **Lakehouse**
3. Name: `dq_framework_lakehouse` (or your preferred name)
4. Click **Create**

**What this creates:**
- Storage for your data files
- Tables for Bronze/Silver/Gold layers
- File storage for configs and logs

---

### Step 2.3: Verify Lakehouse Structure

Once created, you should see:

```
dq_framework_lakehouse/
+-- Tables/          <- SQL tables (Bronze/Silver/Gold will go here)
+-- Files/           <- File storage (configs and logs go here)
```

---

## PHASE 3: Upload Files & Configs (15 minutes)

### Step 3.1: Create Directory Structure in Lakehouse

1. In your Lakehouse, click on **Files** section
2. Click **New folder** → Name: `dq_configs`
3. Click **New folder** → Name: `dq_logs` (will be auto-used by framework)
4. Click **New folder** → Name: `raw_data` (for your source data)

**Result:**
```
Files/
+-- dq_configs/      <- Validation YAML files
+-- dq_logs/         <- Auto-generated validation logs
+-- raw_data/        <- Source data files
```

---

### Step 3.2: Upload Configuration Files

#### Option A: Via Web Interface (Recommended for First Time)

1. Click on **dq_configs** folder
2. Click **Upload** button
3. Click **Upload files**
4. Navigate to: `/home/sanmi/Documents/HS2/HS2_PROJECTS_2025/2_DATA_QUALITY_LIBRARY/fabric_deployment_package/dq_configs/`
5. Select all `*_validation.yml` files
6. Click **Open**
7. Wait for upload to complete

**Verify:**
- Click on each YAML file to preview
- Check that content looks correct

#### Option B: Via Notebook (For Bulk Uploads)

Create a temporary notebook:

```python
# Upload via Python
import os
from notebookutils import mssparkutils

# Local path to your configs
local_config_path = "/home/sanmi/Documents/HS2/HS2_PROJECTS_2025/2_DATA_QUALITY_LIBRARY/config/"

# Fabric path
fabric_path = "Files/dq_configs/"

# Upload each file
for file in os.listdir(local_config_path):
    if file.endswith('.yml'):
        local_file = os.path.join(local_config_path, file)
        fabric_file = os.path.join(fabric_path, file)
        
        mssparkutils.fs.cp(f"file:///{local_file}", f"abfss://.../{fabric_file}")
        print(f"✓ Uploaded: {file}")
```

---

### Step 3.3: Upload Sample Data (Optional)

If you want to test with actual data:

1. Click on **raw_data** folder
2. Click **Upload** → **Upload files**
3. Select your CSV/Parquet files
4. Click **Open**

**Note:** For large files (>100MB), consider:
- Using Azure Storage Explorer
- Using `azcopy` command-line tool
- Uploading directly from Azure Data Lake

---

### Step 3.4: Verify Uploads

```python
# Create a quick verification notebook
# List uploaded configs
display(mssparkutils.fs.ls("Files/dq_configs/"))

# Should show:
# - causeway_bronze_validation.yml
# - causeway_silver_validation.yml
# - causeway_gold_validation.yml
# - hss_incidents_validation.yml
```

---

## PHASE 4: Create Validation Module (10 minutes)

### Step 4.1: Create DQ_Module Notebook

1. In your workspace, click **+ New**
2. Select **Notebook**
3. Name: `DQ_Module`
4. Click **Create**

---

### Step 4.2: Install Dependencies

**Cell 1: Install Libraries**

```python
# Cell 1: Install dependencies
%pip install pyyaml great-expectations>=1.0.0,<2.0.0 pandas pyarrow --quiet
print("✅ Dependencies installed")
```

**Run this cell** (takes ~30 seconds)

**Expected output:**
```
✅ Dependencies installed
```

---

### Step 4.3: Import Libraries

**Cell 2: Imports**

```python
# Cell 2: Import required libraries
import pandas as pd
from pyspark.sql import functions as F
from datetime import datetime
import yaml
import os
import json

from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core import ExpectationConfiguration

print("✅ Libraries imported successfully")
```

**Run this cell**

---

### Step 4.4: Create Validator Class

**Cell 3: FabricDataQualityValidator Class**

Copy the entire class from `docs/FABRIC_QUICK_START.md` or use this:

```python
# Cell 3: Data Quality Validator Class
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
        # Create log directory
        log_dir = "/lakehouse/default/Files/dq_logs"
        os.makedirs(log_dir, exist_ok=True)
        
        # Save results
        timestamp = results['timestamp'].replace(':', '-').replace('.', '-')
        log_file = f"{log_dir}/validation_{timestamp}.json"
        
        with open(log_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"📝 Results logged: {log_file}")

print("✅ FabricDataQualityValidator class created")
```

**Run this cell**

---

### Step 4.5: Test the Module

**Cell 4: Quick Test**

```python
# Cell 4: Test the validator
print("Testing validator with sample data...")

# Create test DataFrame
test_df = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['A', 'B', 'C', 'D', 'E'],
    'value': [100, 200, 300, 400, 500]
})

print(f"Test data: {len(test_df)} rows, {len(test_df.columns)} columns")
print("\nAttempting to load config...")

# Try to load a config (should work if uploads were successful)
try:
    validator = FabricDataQualityValidator('Files/dq_configs/causeway_bronze_validation.yml')
    print("✅ Config loaded successfully!")
    print(f"   Validation name: {validator.config.get('validation_name')}")
    print(f"   Expectations: {len(validator.config.get('expectations', []))}")
except Exception as e:
    print(f"❌ Config load failed: {e}")
    print("   Check that YAML files were uploaded to Files/dq_configs/")
```

**Run this cell**

**Expected output:**
```
Testing validator with sample data...
Test data: 5 rows, 3 columns

Attempting to load config...
✓ Loaded config: causeway_bronze_validation
✅ Config loaded successfully!
   Validation name: causeway_bronze_validation
   Expectations: 65
```

---

### Step 4.6: Save the DQ_Module Notebook

1. Click **File** → **Save**
2. Confirm name: `DQ_Module`

**This notebook is now reusable** - you can import it into any other notebook with `%run DQ_Module`

---

## PHASE 5: Test & Validate (10 minutes)

### Step 5.1: Create Test ETL Notebook

1. Create a new notebook: **+ New** → **Notebook**
2. Name: `Test_DQ_Integration`

---

### Step 5.2: Import DQ Module

**Cell 1: Import Module**

```python
# Cell 1: Import the DQ module
%run DQ_Module

print("✅ DQ Module imported successfully")
```

**Run this cell**

---

### Step 5.3: Load Sample Data

**Cell 2: Load Data**

```python
# Cell 2: Load sample data

# Option A: If you uploaded CAUSEWAY data
df = spark.read.csv(
    "Files/raw_data/CAUSEWAY_combined_scr_2024.csv",
    header=True,
    inferSchema=True,
    encoding="latin-1"
).limit(10000).toPandas()  # Limit for testing

print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns")
display(df.head())
```

**OR**

```python
# Cell 2: Create synthetic test data
df = pd.DataFrame({
    'reference': range(1, 1001),
    'base_value': [100.0 * i for i in range(1, 1001)],
    'vendor_name': [f'Vendor_{i%10}' for i in range(1, 1001)],
    'cost_date': pd.date_range('2024-01-01', periods=1000, freq='H')
})

print(f"✅ Created {len(df):,} rows of test data")
display(df.head())
```

**Run this cell**

---

### Step 5.4: Run Bronze Layer Validation

**Cell 3: Bronze Validation**

```python
# Cell 3: Bronze layer validation (lenient)
print("="*80)
print("BRONZE LAYER VALIDATION")
print("="*80)

validator_bronze = FabricDataQualityValidator('Files/dq_configs/causeway_bronze_validation.yml')
results_bronze = validator_bronze.validate(df, fail_on_error=False)

print(f"\n✅ Bronze validation complete!")
print(f"   Quality Score: {results_bronze['success_rate']:.1f}%")
print(f"   Passed: {results_bronze['passed_count']}")
print(f"   Failed: {results_bronze['failed_count']}")

# Decision logic
if results_bronze['success_rate'] >= 50:
    print("\n✅ Bronze quality acceptable - proceeding to Silver")
else:
    print(f"\n❌ Bronze quality too low: {results_bronze['success_rate']:.1f}%")
```

**Run this cell**

**Expected output:**
```
============================================================
Validating: causeway_bronze_validation
Rows: 10,000 | Columns: 39
============================================================
Running 65 expectations...

============================================================
✅ VALIDATION SUMMARY
============================================================
Success Rate:      57.3%
Passed:              37
Failed:              28
Total Checks:        65
...
✅ Bronze validation complete!
   Quality Score: 57.3%
   Passed: 37
   Failed: 28

✅ Bronze quality acceptable - proceeding to Silver
```

---

### Step 5.5: Run Silver Layer Validation

**Cell 4: Transform and Validate Silver**

```python
# Cell 4: Silver layer validation (strict)
print("="*80)
print("SILVER LAYER VALIDATION")
print("="*80)

# Transform data for Silver layer
df_silver = df.copy()

# Remove empty columns
null_cols = [col for col in df_silver.columns if df_silver[col].isna().all()]
if null_cols:
    df_silver = df_silver.drop(columns=null_cols)
    print(f"✓ Removed {len(null_cols)} empty columns")

# Remove duplicates
initial_rows = len(df_silver)
df_silver = df_silver.drop_duplicates()
print(f"✓ Removed {initial_rows - len(df_silver)} duplicates")

print(f"✓ Silver data: {len(df_silver):,} rows, {len(df_silver.columns)} columns\n")

# Validate with strict rules
try:
    validator_silver = FabricDataQualityValidator('Files/dq_configs/causeway_silver_validation.yml')
    results_silver = validator_silver.validate(df_silver, fail_on_error=False)
    
    print(f"\n✅ Silver validation complete!")
    print(f"   Quality Score: {results_silver['success_rate']:.1f}%")
    
    if results_silver['success_rate'] >= 80:
        print("   ✅ Silver quality meets threshold (80%)")
    else:
        print(f"   ⚠️  Silver quality below threshold: {results_silver['success_rate']:.1f}%")
        
except Exception as e:
    print(f"❌ Silver validation failed: {e}")
```

**Run this cell**

---

### Step 5.6: Save Data to Tables

**Cell 5: Save to Lakehouse Tables**

```python
# Cell 5: Save validated data to tables
print("="*80)
print("SAVING DATA TO LAKEHOUSE TABLES")
print("="*80)

# Save Bronze
spark.createDataFrame(df).write.mode("overwrite").saveAsTable("causeway_bronze")
print(f"✅ Saved to table: causeway_bronze ({len(df):,} rows)")

# Save Silver (if quality is acceptable)
if results_silver['success_rate'] >= 80:
    spark.createDataFrame(df_silver).write.mode("overwrite").saveAsTable("causeway_silver")
    print(f"✅ Saved to table: causeway_silver ({len(df_silver):,} rows)")
else:
    print(f"⚠️  Silver data not saved (quality below threshold)")

print("\n✅ Pipeline complete!")
```

**Run this cell**

---

### Step 5.7: Verify Tables Were Created

**Cell 6: Verify Tables**

```python
# Cell 6: Verify tables exist
print("Verifying tables...")

tables = spark.sql("SHOW TABLES").toPandas()
print(f"\nTables in Lakehouse:")
display(tables)

# Query Bronze table
bronze_count = spark.sql("SELECT COUNT(*) as count FROM causeway_bronze").collect()[0]['count']
print(f"\ncauseway_bronze: {bronze_count:,} rows")

# Query Silver table (if exists)
try:
    silver_count = spark.sql("SELECT COUNT(*) as count FROM causeway_silver").collect()[0]['count']
    print(f"causeway_silver: {silver_count:,} rows")
except:
    print("causeway_silver: Table not created")
```

**Run this cell**

---

### Step 5.8: Check Validation Logs

**Cell 7: Check Logs**

```python
# Cell 7: Check validation logs
print("Checking validation logs...")

# List log files
log_files = mssparkutils.fs.ls("Files/dq_logs/")

print(f"\nFound {len(log_files)} log files:")
for log in log_files[-5:]:  # Show last 5
    print(f"  - {log.name}")

# Read the most recent log
if log_files:
    latest_log = log_files[-1].path
    log_content = mssparkutils.fs.head(latest_log, 1000)
    
    print(f"\nLatest log preview:")
    print(log_content[:500])
```

**Run this cell**

---

## ✅ DEPLOYMENT COMPLETE!

### What You Now Have in Fabric

```
Workspace: HS2_Data_Quality
+-- Lakehouse: dq_framework_lakehouse
|   +-- Files/
|   |   +-- dq_configs/
|   |   |   +-- causeway_bronze_validation.yml
|   |   |   +-- causeway_silver_validation.yml
|   |   |   +-- hss_incidents_validation.yml
|   |   +-- dq_logs/
|   |   |   +-- validation_*.json (auto-generated)
|   |   +-- raw_data/
|   |       +-- (your source data)
|   +-- Tables/
|       +-- causeway_bronze
|       +-- causeway_silver
+-- Notebooks/
|   +-- DQ_Module (reusable validator)
|   +-- Test_DQ_Integration (test pipeline)
```

---

## 🚀 Next Steps

### Immediate Actions

1. **Test with Your Real Data**
   ```python
   # Load your actual data source
   df = spark.read.format("delta").load("Tables/your_source_table").toPandas()
   
   # Validate
   validator = FabricDataQualityValidator('Files/dq_configs/your_validation.yml')
   results = validator.validate(df, fail_on_error=True)
   ```

2. **Create Your Production ETL Pipeline**
   - Copy the test notebook structure
   - Add your transformation logic
   - Include DQ checks at Bronze/Silver/Gold layers

3. **Set Up Monitoring**
   ```python
   # Create monitoring table
   monitoring_df = spark.createDataFrame([{
       'timestamp': datetime.now(),
       'dataset': 'causeway',
       'layer': 'bronze',
       'success_rate': results['success_rate'],
       'row_count': len(df)
   }])
   
   monitoring_df.write.mode("append").saveAsTable("dq_pipeline_monitoring")
   ```

### This Week

1. **Profile Additional Datasets**
   - Run `profile_data.py` on HSS data
   - Run on AIMS data
   - Generate configs for each

2. **Create Pipeline Orchestration**
   - Use Fabric Data Pipelines
   - Link notebooks in sequence
   - Add conditional logic based on quality scores

3. **Build Power BI Dashboard**
   - Connect to `dq_pipeline_monitoring` table
   - Create quality trend charts
   - Set up alerts for quality drops

### This Month

1. **Expand to All Projects**
   - Integrate with full_stack_hss
   - Integrate with AIMS_LOCAL
   - Integrate with ACA_COMMERCIAL

2. **Implement Alerting**
   ```python
   # Add to your validator
   if results['success_rate'] < 80:
       mssparkutils.notify.sendEmail(
           recipients=['team@hs2.org.uk'],
           subject='Data Quality Alert',
           body=f"Quality dropped to {results['success_rate']:.1f}%"
       )
   ```

3. **Document Team Processes**
   - Create runbooks for common scenarios
   - Train team on framework usage
   - Set up code review process

---

## 🐛 Troubleshooting

### Issue: "Config file not found"

**Symptom:**
```
FileNotFoundError: Files/dq_configs/causeway_bronze_validation.yml
```

**Solution:**
```python
# Check if file exists
display(mssparkutils.fs.ls("Files/dq_configs/"))

# Check path format
# Correct: 'Files/dq_configs/file.yml'
# Wrong:   '/Files/dq_configs/file.yml' (no leading slash)
```

---

### Issue: "Module not found: great_expectations"

**Symptom:**
```
ModuleNotFoundError: No module named 'great_expectations'
```

**Solution:**
```python
# Reinstall in notebook
%pip install --upgrade great-expectations>=1.0.0,<2.0.0
```

---

### Issue: "Validation takes too long"

**Symptom:** Validation running for >5 minutes on large datasets

**Solution:**
```python
# Sample large datasets before validation
if len(df) > 100000:
    df_sample = df.sample(n=100000, random_state=42)
    results = validator.validate(df_sample)
else:
    results = validator.validate(df)
```

---

### Issue: "Out of memory error"

**Symptom:**
```
OutOfMemoryError: Java heap space
```

**Solution:**
```python
# Process in chunks
chunk_size = 50000
for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i+chunk_size]
    results = validator.validate(chunk)
    # Aggregate results
```

---

### Issue: "Cannot write to Tables"

**Symptom:**
```
AnalysisException: Table or view not found
```

**Solution:**
```python
# Ensure table is created first
spark.createDataFrame(df).write.mode("overwrite").saveAsTable("my_table")

# Or use delta format explicitly
spark.createDataFrame(df).write.format("delta").mode("overwrite").save("Tables/my_table")
```

---

## 📞 Support

### Getting Help

1. **Check Documentation**
   - `FABRIC_QUICK_START.md` - Quick reference
   - `FABRIC_INTEGRATION.md` - Detailed patterns
   - `PROFILING_WORKFLOW.md` - Data profiling guide

2. **Review Examples**
   - `examples/fabric_etl_example.py` - Complete pipeline
   - `examples/complete_workflow_example.py` - End-to-end workflow

3. **Test Locally First**
   ```bash
   # Test configs locally before uploading
   python examples/complete_workflow_example.py
   ```

---

## ✅ Deployment Checklist

Use this to verify your deployment:

- [ ] **Phase 1: Local Preparation**
  - [ ] Framework installed and tested locally
  - [ ] Data profiled and configs generated
  - [ ] Layer-specific configs created (Bronze/Silver/Gold)
  - [ ] Configs reviewed and enhanced with business rules
  - [ ] Deployment package prepared

- [ ] **Phase 2: Workspace Setup**
  - [ ] MS Fabric workspace created/accessed
  - [ ] Lakehouse created
  - [ ] Directory structure verified

- [ ] **Phase 3: File Upload**
  - [ ] dq_configs folder created
  - [ ] YAML configs uploaded
  - [ ] Sample data uploaded (optional)
  - [ ] Files verified and accessible

- [ ] **Phase 4: Module Creation**
  - [ ] DQ_Module notebook created
  - [ ] Dependencies installed
  - [ ] Validator class implemented
  - [ ] Module tested successfully
  - [ ] Notebook saved

- [ ] **Phase 5: Testing**
  - [ ] Test notebook created
  - [ ] DQ Module imported successfully
  - [ ] Bronze validation working
  - [ ] Silver validation working
  - [ ] Tables created in Lakehouse
  - [ ] Logs generated successfully

- [ ] **Post-Deployment**
  - [ ] Production pipeline created
  - [ ] Monitoring table set up
  - [ ] Team trained on usage
  - [ ] Documentation updated for team

---

**Deployment Status:** ✅ Ready for Production Use  
**Framework Version:** 2.0.0  
**Last Updated:** 2026-01-19  
**Contact:** Data Engineering Team

---

## 🎓 What's Next?

Now that your framework is deployed:

1. **Start Small**: Begin with one dataset (CAUSEWAY recommended)
2. **Monitor Closely**: Check logs and quality scores daily for first week
3. **Iterate**: Adjust thresholds and rules based on actual results
4. **Expand Gradually**: Add more datasets and projects one at a time
5. **Share Knowledge**: Train team members on framework usage

**Congratulations! Your Data Quality Framework is now live in MS Fabric! 🎉**
