# MS Fabric Quick Start Guide

## 5-Minute Setup

### Step 1: Upload Framework to Fabric (2 minutes)

```python
# In a Fabric notebook, Cell 1:
%pip install pyyaml great_expectations==0.18.22 pandas pyarrow
```

### Step 2: Upload Your Configs (1 minute)

1. In your Fabric Lakehouse, go to **Files** section
2. Create folder structure:
   ```
   Files/
     dq_configs/
       causeway_bronze_validation.yml
       causeway_silver_validation.yml
       hss_incidents_validation.yml
   ```
3. Upload the YAML configs you generated with `profile_data.py`

### Step 3: Copy Validator Class (1 minute)

Create a notebook called `DQ_Module` and paste this:

```python
import pandas as pd
from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core import ExpectationConfiguration
import yaml
from datetime import datetime

class FabricDataQualityValidator:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.context = EphemeralDataContext()
        
    def _load_config(self, path: str) -> dict:
        with open(f"/lakehouse/default/{path}", 'r') as f:
            return yaml.safe_load(f)
    
    def validate(self, df: pd.DataFrame, fail_on_error: bool = False) -> dict:
        suite = ExpectationSuite(
            expectation_suite_name=self.config.get('validation_name', 'validation')
        )
        
        for exp in self.config.get('expectations', []):
            suite.add_expectation(ExpectationConfiguration(**exp))
        
        self.context.add_expectation_suite(expectation_suite=suite)
        
        batch = self.context.sources.add_pandas("pandas_source")\
            .add_dataframe_asset(name="df_asset")\
            .add_batch_definition_whole_dataframe("batch_def")\
            .get_batch(batch_parameters={"dataframe": df})
        
        validation_result = batch.validate(suite)
        
        results = {
            'success': validation_result.success,
            'timestamp': datetime.now().isoformat(),
            'results': []
        }
        
        for result in validation_result.results:
            results['results'].append({
                'expectation_type': result.expectation_config.expectation_type,
                'success': result.success,
                'column': result.expectation_config.kwargs.get('column')
            })
        
        total = len(results['results'])
        passed = sum(1 for r in results['results'] if r['success'])
        results['success_rate'] = (passed / total * 100) if total > 0 else 0
        results['passed_count'] = passed
        results['failed_count'] = total - passed
        
        print(f"Quality Score: {results['success_rate']:.1f}%")
        print(f"Passed: {passed}/{total}")
        
        if fail_on_error and not results['success']:
            raise ValueError(f"Validation failed: {results['success_rate']:.1f}%")
        
        return results
```

### Step 4: Use in Your ETL (1 minute)

```python
# Your ETL notebook
%run DQ_Module

# Load data
df = spark.read.table("my_table").toPandas()

# Validate
validator = FabricDataQualityValidator('Files/dq_configs/my_validation.yml')
results = validator.validate(df, fail_on_error=True)

# Continue processing...
```

---

## Common Patterns

### Pattern 1: Bronze → Silver → Gold Pipeline

```python
# Bronze: Lenient validation
validator_bronze = FabricDataQualityValidator('Files/dq_configs/causeway_bronze_validation.yml')
results_bronze = validator_bronze.validate(df_raw, fail_on_error=False)

if results_bronze['success_rate'] >= 50:
    # Transform to Silver
    df_clean = clean_data(df_raw)
    
    # Silver: Strict validation
    validator_silver = FabricDataQualityValidator('Files/dq_configs/causeway_silver_validation.yml')
    results_silver = validator_silver.validate(df_clean, fail_on_error=True)
    
    # Save
    spark.createDataFrame(df_clean).write.mode("overwrite").saveAsTable("causeway_silver")
```

### Pattern 2: Conditional Processing

```python
results = validator.validate(df)

if results['success_rate'] >= 95:
    # High quality - standard processing
    process_data(df)
elif results['success_rate'] >= 70:
    # Medium quality - apply corrections
    df_corrected = apply_corrections(df)
    process_data(df_corrected)
else:
    # Low quality - manual review
    send_alert("Manual review required")
    raise ValueError("Quality too low")
```

### Pattern 3: Incremental Loads

```python
# Get new records only
last_date = spark.read.table("metadata").select(max("date")).collect()[0][0]
new_records = df[df['date'] > last_date]

# Validate only new records
validator = FabricDataQualityValidator('Files/dq_configs/incremental_validation.yml')
results = validator.validate(new_records, fail_on_error=True)

# Append
spark.createDataFrame(new_records).write.mode("append").saveAsTable("my_table")
```

---

## Quality Thresholds by Layer

| Layer  | Threshold | Fail on Error | Purpose |
|--------|-----------|---------------|---------|
| Bronze | 50-60%    | No            | Catch obvious issues |
| Silver | 80-90%    | Yes           | Ensure clean data |
| Gold   | 95-100%   | Yes           | Perfect business metrics |

---

## Troubleshooting

### Issue: Config file not found
```python
# Check path
import os
os.path.exists('/lakehouse/default/Files/dq_configs/my_config.yml')

# List configs
os.listdir('/lakehouse/default/Files/dq_configs/')
```

### Issue: Module not found
```python
# Reinstall in notebook
%pip install --upgrade pyyaml great_expectations==0.18.22
```

### Issue: Memory error with large files
```python
# Sample large files before validation
df_sample = df.sample(n=100000, random_state=42)
results = validator.validate(df_sample)
```

---

## Complete Example

```python
# ===== SETUP =====
%pip install pyyaml great_expectations==0.18.22
%run DQ_Module

# ===== BRONZE LAYER =====
df_bronze = spark.read.csv("Files/raw_data/data.csv", header=True).toPandas()
validator_bronze = FabricDataQualityValidator('Files/dq_configs/bronze_validation.yml')
results_bronze = validator_bronze.validate(df_bronze, fail_on_error=False)

if results_bronze['success_rate'] < 50:
    raise ValueError("Bronze quality too low")

spark.createDataFrame(df_bronze).write.mode("overwrite").saveAsTable("my_bronze")

# ===== SILVER LAYER =====
df_silver = df_bronze.drop_duplicates()
df_silver = df_silver.dropna(subset=['key_column'])

validator_silver = FabricDataQualityValidator('Files/dq_configs/silver_validation.yml')
results_silver = validator_silver.validate(df_silver, fail_on_error=True)

spark.createDataFrame(df_silver).write.mode("overwrite").saveAsTable("my_silver")

# ===== GOLD LAYER =====
df_gold = df_silver.groupby('category').agg({'value': 'sum'}).reset_index()

validator_gold = FabricDataQualityValidator('Files/dq_configs/gold_validation.yml')
results_gold = validator_gold.validate(df_gold, fail_on_error=True)

spark.createDataFrame(df_gold).write.mode("overwrite").saveAsTable("my_gold")

# ===== SUMMARY =====
print(f"Bronze: {results_bronze['success_rate']:.1f}%")
print(f"Silver: {results_silver['success_rate']:.1f}%")
print(f"Gold: {results_gold['success_rate']:.1f}%")
```

---

## Next Steps

1. **Generate configs locally:**
   ```bash
   python profile_data.py your_data.csv --output config.yml
   ```

2. **Upload to Fabric:**
   - Upload config.yml to `Files/dq_configs/`

3. **Test in notebook:**
   - Create test notebook with validator
   - Run validation on sample data

4. **Integrate into pipeline:**
   - Add DQ checks to existing ETL notebooks
   - Set up monitoring table

5. **Create dashboard:**
   - Power BI report from monitoring table
   - Track quality trends over time

---

## Resources

- Full integration guide: `docs/FABRIC_ETL_INTEGRATION.md`
- Complete example: `examples/fabric_etl_example.py`
- Profiling workflow: `PROFILING_WORKFLOW.md`
- Quick reference: `QUICK_REFERENCE.md`
