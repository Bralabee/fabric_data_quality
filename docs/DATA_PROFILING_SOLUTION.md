# Data Profiling & Assessment - Solution Overview

## Your Question: "Is data assessment included in the solution?"

**YES! And it's now a core feature.** I've added a `DataProfiler` class that:

1. **Analyzes your data FIRST** before generating any config
2. **Understands the actual structure** (columns, types, nulls, patterns)
3. **Auto-generates appropriate validation rules** based on what it finds
4. **Provides data quality scores** to help you understand data health

## The Problem You Identified

You were absolutely right - the original `.yml` files I created were **generic templates** that didn't truly reflect YOUR actual CAUSEWAY data. They had:
- ❌ Wrong assumptions about column names
- ❌ Unrealistic null tolerances  
- ❌ Generic expectations not tailored to your data patterns
- ❌ No connection to the actual 359K rows of data you provided

## The Solution: Data-Driven Configuration

### 🔍 New Component: DataProfiler

Located at: `dq_framework/data_profiler.py`

**What it does:**
```python
from dq_framework import DataProfiler
import pandas as pd

# Load YOUR data
df = pd.read_csv('your_data.csv')

# Profile it
profiler = DataProfiler(df)
profile = profiler.profile()

# See what you're working with
profiler.print_summary()  # Shows columns, types, nulls, uniqueness

# Generate appropriate validation config
config = profiler.generate_expectations(
    validation_name="my_validation",
    null_tolerance=10.0,  # Adjust based on your data
)

# Save it
profiler.save_config(config, 'my_validation.yml')
```

### 📊 What the Profiler Discovers

For your CAUSEWAY data, it found:

**1. Data Quality Issues**
- **Score**: 41.7/100 (highlighting real problems!)
- **18 columns** with < 50% nulls (good quality)
- **12 columns** with > 90% nulls (unused/problematic)

**2. Column Patterns**
```
Column                         Type            Nulls      Unique
-----------------------------------------------------------------
Allocation Uniqid              id              45.5%      54.5%
Base Value                     monetary        45.5%      21.2%
Contract description           string          45.5%      0.0%
Value type (A,D,E)             categorical     45.5%      0.0%
Period Accruals                unknown        100.0%      0.0%
```

**3. Real Data Characteristics**
- 359,595 rows of financial transactions
- Many columns have ~45% nulls (this is YOUR data's pattern)
- Some columns completely empty (100% null)
- Monetary values use £ symbol (encoding handled)

## Complete Workflow Example

I've created **`examples/complete_workflow_example.py`** that demonstrates:

### Phase 1: Profile Your Data
```python
profiler = DataProfiler(df, sample_size=10000)
profile = profiler.profile()
# Shows: 41.7/100 quality score, identifies problem columns
```

### Phase 2: Understand Patterns
```
KEY OBSERVATIONS:
  1. Many columns have ~45% nulls - this is the data pattern
  2. Some columns 100% null - unused/future columns
  3. 'Allocation Uniqid' is unique identifier
  4. 'Base Value' is monetary (£)
  5. Contract, Activity are descriptive text
```

### Phase 3: Auto-Generate Config
```python
config = profiler.generate_expectations(
    validation_name="causeway_validation",
    null_tolerance=50.0,  # Based on actual data
)
# Generates 59 expectations based on actual structure
```

### Phase 4: Enhance with Business Rules
```python
# Add domain knowledge
business_rules = [
    "Allocation Uniqid must be unique",
    "Value type must be A/D/E",
    "Source Type from known list",
]
# Results in 68 total expectations (59 auto + 9 business)
```

### Phase 5: Validate
```
VALIDATION RESULTS:
  Overall Status: ❌ FAILED (as expected - found issues!)
  Success Rate: 71.0%
  Checks Evaluated: 62
  Checks Passed: 44
  Checks Failed: 18
```

## Files Generated

### 1. Auto-Generated Baseline
**`examples/causeway_auto_generated_v1.yml`**
- Based on actual data structure
- Relaxed null tolerance (50%) matching your data
- 59 expectations covering structure + completeness

### 2. Enhanced with Business Rules
**`examples/causeway_financial_enhanced.yml`**
- Includes auto-generated baseline
- Plus 9 business-specific rules
- Total: 68 expectations
- **This is what you should use!**

### 3. Profile Script
**`examples/profile_causeway_data.py`**
- Standalone script to profile any CSV
- Generates summary report
- Creates initial config

### 4. Complete Workflow
**`examples/complete_workflow_example.py`**
- End-to-end demonstration
- Shows all 5 phases
- Validates on real data
- **Run this to see everything in action!**

## How to Use This for Your Projects

### For CAUSEWAY Data (Current)
```bash
conda activate fabric-dq
cd /home/sanmi/Documents/HS2/HS2_PROJECTS_2025/2_DATA_QUALITY_LIBRARY

# Run the complete workflow
python examples/complete_workflow_example.py

# Use the enhanced config
python -c "
from dq_framework import DataQualityValidator, ConfigLoader
import pandas as pd

df = pd.read_csv('sample_source_data/CAUSEWAY_combined_scr_2024.csv', 
                 encoding='latin-1', low_memory=False)
config = ConfigLoader().load('examples/causeway_financial_enhanced.yml')
validator = DataQualityValidator(config_dict=config)
results = validator.validate(df)
print(f'Success: {results[\"success\"]} ({results[\"success_rate\"]:.1f}%)')
"
```

### For HSS Incidents (`full_stack_hss`)
```bash
# Profile your HSS data
python -c "
from dq_framework import DataProfiler
import pandas as pd

df = pd.read_parquet('../full_stack_hss/data/incidents.parquet')
profiler = DataProfiler(df)
profiler.print_summary()

config = profiler.generate_expectations(
    validation_name='hss_incidents_validation',
    null_tolerance=5.0  # Stricter for cleaned data
)
profiler.save_config(config, 'config/hss_validation.yml')
"

# Then enhance with HSS-specific rules
# (incident severity, category values, date ranges, etc.)
```

### For AIMS Data (`AIMS_LOCAL`)
```bash
# Profile AIMS parquet files
python -c "
from dq_framework import DataProfiler
import pandas as pd

df = pd.read_parquet('../AIMS_LOCAL/data/aims_data.parquet')
profiler = DataProfiler(df)
config = profiler.generate_expectations(
    validation_name='aims_data_validation',
    null_tolerance=1.0  # Very strict
)
profiler.save_config(config, 'config/aims_validation.yml')
"
```

### For ACA Commercial (`ACA_COMMERCIAL`)
```bash
# Profile SharePoint data
python -c "
from dq_framework import DataProfiler
import pandas as pd

df = pd.read_csv('../ACA_COMMERCIAL/sharepoint_data.csv')
profiler = DataProfiler(df)
config = profiler.generate_expectations(
    validation_name='aca_commercial_validation'
)
profiler.save_config(config, 'config/aca_validation.yml')
"
```

## Key Benefits

### ✅ Data-Driven (Not Template-Driven)
- Analyzes YOUR actual data
- Discovers patterns you might miss
- Suggests appropriate thresholds

### ✅ Transparency
- Shows you what it finds (data quality score, null %, uniqueness)
- Explains why it suggests certain rules
- You can adjust parameters

### ✅ Reproducible
- Profile → Generate → Enhance → Validate
- Same process for any dataset
- Documented workflow

### ✅ Customizable
- Start with auto-generated baseline
- Add your business rules
- Adjust severity and tolerances
- Iterate based on results

## Comparison: Before vs After

### Before (What I Created Initially)
```yaml
# causeway_financial_example.yml - WRONG!
expectations:
  - expect_column_to_exist:
      column: "Allocation Uniqid"  # OK
  - expect_column_values_to_not_be_null:
      column: "Contract code"  # WRONG - 100% null in reality!
      severity: critical  # Would always fail!
```

### After (Data-Driven)
```yaml
# causeway_financial_enhanced.yml - CORRECT!
expectations:
  - expect_column_to_exist:
      column: "Allocation Uniqid"  # Verified exists
  - expect_column_values_to_be_unique:
      column: "Allocation Uniqid"  # Discovered it's an ID
      severity: high
  # No null check on "Contract code" because profiler found 100% nulls
```

## The Assessment is Now Part of the Solution

**Before**: Manual inspection → guess patterns → write config → hope it works

**Now**: 
1. **DataProfiler.profile()** - Automated assessment
2. **generate_expectations()** - Data-driven config generation
3. **Manual enhancement** - Add business knowledge
4. **validate()** - Test on real data
5. **Iterate** - Adjust based on results

## Run It Yourself

```bash
# Activate environment
conda activate fabric-dq

# Run complete workflow (all 5 phases)
cd /home/sanmi/Documents/HS2/HS2_PROJECTS_2025/2_DATA_QUALITY_LIBRARY
python examples/complete_workflow_example.py

# Review the generated configs
cat examples/causeway_auto_generated_v1.yml
cat examples/causeway_financial_enhanced.yml

# Use DataProfiler on any CSV
python examples/profile_causeway_data.py
```

## Summary

✅ **YES**, data assessment is now a core feature via `DataProfiler`
✅ **YES**, configs are generated based on YOUR actual data structure  
✅ **YES**, you can see exactly what the profiler discovered
✅ **YES**, you can customize and enhance the generated rules
✅ **YES**, the same workflow works for HSS, AIMS, ACA, and any future project

**The framework now starts with understanding YOUR data, not generic templates!**

---

*Files Added:*
- `dq_framework/data_profiler.py` - Core profiling class
- `examples/profile_causeway_data.py` - Profile script
- `examples/complete_workflow_example.py` - End-to-end demo
- `examples/causeway_auto_generated_v1.yml` - Auto-generated config
- `examples/causeway_financial_enhanced.yml` - Enhanced config

*Run `python examples/complete_workflow_example.py` to see it all in action!*
