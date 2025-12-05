#!/usr/bin/env python3
"""
Simple demonstration of the Fabric Data Quality Framework
Using CAUSEWAY financial transaction data
"""

import pandas as pd
from dq_framework import DataQualityValidator, ConfigLoader

print("="*80)
print("FABRIC DATA QUALITY FRAMEWORK - Simple Demo")
print("="*80)

# Step 1: Load data
print("\n Step 1: Loading CAUSEWAY financial data...")
df = pd.read_csv('sample_source_data/CAUSEWAY_combined_scr_2024.csv', 
                 encoding='latin-1', low_memory=False)
print(f"   ✓ Loaded {len(df):,} rows and {len(df.columns)} columns")
print(f"   ✓ Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

# Step 2: Load validation configuration
print("\n📋 Step 2: Loading validation configuration...")
config_loader = ConfigLoader()
config = config_loader.load('examples/causeway_financial_example.yml')
print(f"   ✓ Validation: {config.get('validation_name', 'N/A')}")
print(f"   ✓ Total expectations: {len(config['expectations'])}")

# Step 3: Create validator and run validation
print("\n🔍 Step 3: Running validation...")
validator = DataQualityValidator(config_dict=config)
results = validator.validate(df)

# Step 4: Show results
print("\n📊 Step 4: Validation Results")
print("-" * 80)
print(f"Overall Status: {'✅ PASSED' if results['success'] else '❌ FAILED'}")
print(f"Success Rate: {results['success_rate']:.1f}%")
print(f"Checks Evaluated: {results['evaluated_checks']}")
print(f"Checks Passed: {results['successful_checks']}")
print(f"Checks Failed: {results['failed_checks']}")

# Step 5: Show sample expectations that were tested
print("\n📝 Step 5: Sample Expectations Tested:")
print("-" * 80)

if 'validation_result' in results:
    vr = results['validation_result']
    for i, exp_result in enumerate(vr.results[:10], 1):  # Show first 10
        status = "✅" if exp_result.success else "❌"
        exp_type = exp_result.expectation_config.expectation_type
        col = exp_result.expectation_config.kwargs.get('column', 'N/A')
        
        print(f"{i:2d}. {status} {exp_type}")
        if col != 'N/A':
            print(f"     Column: {col}")
        
        # Show some result details for failed checks
        if not exp_result.success and hasattr(exp_result, 'result'):
            print(f"     Result: {str(exp_result.result)[:100]}")

# Step 6: Show which data quality dimensions were checked
print("\n🎯 Step 6: Data Quality Dimensions Covered:")
print("-" * 80)

dimensions = {
    'Completeness': ['expect_column_values_to_not_be_null'],
    'Validity': ['expect_column_values_to_be_in_set', 'expect_column_values_to_match_regex'],
    'Accuracy': ['expect_column_min_to_be_between', 'expect_column_max_to_be_between'],
    'Consistency': ['expect_column_values_to_be_of_type'],
    'Uniqueness': ['expect_column_values_to_be_unique']
}

expectations_tested = [exp.expectation_config.expectation_type for exp in vr.results] if 'validation_result' in results else []

for dimension, exp_types in dimensions.items():
    count = sum(1 for exp in expectations_tested if exp in exp_types)
    if count > 0:
        print(f"✓ {dimension}: {count} checks")

print("\n" + "="*80)
print("✅ Demo Complete!")
print("="*80)
print("\nNext Steps:")
print("1. Review the configuration file: examples/causeway_financial_example.yml")
print("2. Customize expectations for your data")
print("3. Integrate into your ETL pipeline")
print("4. Check docs/CONFIGURATION_GUIDE.md for more details")
