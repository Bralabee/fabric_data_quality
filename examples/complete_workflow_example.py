"""
Complete Example: Profile Data → Generate Config → Enhance → Validate
======================================================================

This example shows the complete workflow:
1. Profile your data to understand its structure
2. Auto-generate initial validation config
3. Manually enhance with business rules
4. Run validation and review results
"""

import pandas as pd
from dq_framework import DataProfiler, DataQualityValidator, ConfigLoader

print("=" * 80)
print("COMPLETE DATA QUALITY WORKFLOW")
print("=" * 80)
print()

# ============================================================================
# PHASE 1: DATA PROFILING
# ============================================================================
print("PHASE 1: DATA PROFILING")
print("-" * 80)

print("\n📥 Loading CAUSEWAY financial data...")
df = pd.read_csv(
    'sample_source_data/CAUSEWAY_combined_scr_2024.csv',
    encoding='latin-1',
    low_memory=False
)
print(f"   ✓ Loaded {len(df):,} rows, {len(df.columns)} columns")

print("\n🔍 Profiling data...")
profiler = DataProfiler(df, sample_size=10000)
profile = profiler.profile()

print(f"\n📊 KEY INSIGHTS:")
print(f"   • Total Rows: {profile['row_count']:,}")
print(f"   • Total Columns: {profile['column_count']}")
print(f"   • Data Quality Score: {profile['data_quality_score']:.1f}/100")

# Show columns with good data quality (low nulls)
print(f"\n   Good Quality Columns (< 50% nulls):")
good_cols = [col for col, info in profile['columns'].items() if info['null_percent'] < 50]
print(f"   {len(good_cols)} columns: {', '.join(good_cols[:5])}...")

# Show problematic columns
print(f"\n   Problematic Columns (> 90% nulls):")
bad_cols = [col for col, info in profile['columns'].items() if info['null_percent'] > 90]
print(f"   {len(bad_cols)} columns: {', '.join(bad_cols)}")

# ============================================================================
# PHASE 2: UNDERSTAND YOUR DATA
# ============================================================================
print("\n\n" + "=" * 80)
print("PHASE 2: UNDERSTANDING YOUR DATA")
print("-" * 80)

print("\n📋 Sample of actual data:")
print(df[['Contract description', 'Activity', 'Resource code', 'Base Value', 'Allocation Uniqid']].head(3))

print("\n💡 KEY OBSERVATIONS:")
print("   1. Many columns have ~45% nulls - this appears to be the data pattern")
print("   2. Some columns have 100% nulls - these might be unused/future columns")
print("   3. 'Allocation Uniqid' could be a unique identifier")
print("   4. 'Base Value' appears to be monetary (has £ symbol)")
print("   5. 'Contract description', 'Activity', etc. are descriptive text")

# ============================================================================
# PHASE 3: GENERATE INITIAL CONFIG
# ============================================================================
print("\n\n" + "=" * 80)
print("PHASE 3: AUTO-GENERATE INITIAL VALIDATION CONFIG")
print("-" * 80)

print("\n⚙️  Generating config with relaxed null tolerance...")
config = profiler.generate_expectations(
    validation_name="causeway_financial_validation_v1",
    description="Initial auto-generated validation for CAUSEWAY data",
    null_tolerance=50.0,  # Only flag if > 50% nulls
    include_structural=True,
    include_completeness=True,
    include_validity=False,  # Skip for now, will add manually
)

print(f"   ✓ Generated {len(config['expectations'])} expectations")

# Save it
profiler.save_config(config, 'examples/causeway_auto_generated_v1.yml')

# ============================================================================
# PHASE 4: MANUALLY ENHANCE WITH BUSINESS RULES
# ============================================================================
print("\n\n" + "=" * 80)
print("PHASE 4: ENHANCE WITH BUSINESS-SPECIFIC RULES")
print("-" * 80)

print("\n📝 Adding business-specific expectations...")

# Identify key business columns
key_business_cols = {
    'Allocation Uniqid': 'Unique transaction identifier',
    'Base Value': 'Transaction monetary value',
    'Contract description': 'Contract identifier',
    'Activity': 'Activity code',
    'Vendor name': 'Supplier information',
    'Cost date': 'Transaction date',
}

# Add business expectations
business_expectations = []

# 1. Key columns should exist
for col, desc in key_business_cols.items():
    business_expectations.append({
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {'column': col},
        'meta': {
            'severity': 'critical',
            'description': f'{desc} - must exist',
            'business_rule': True,
        }
    })

# 2. Allocation Uniqid uniqueness (where not null)
business_expectations.append({
    'expectation_type': 'expect_column_values_to_be_unique',
    'kwargs': {
        'column': 'Allocation Uniqid',
    },
    'meta': {
        'severity': 'high',
        'description': 'Transaction IDs must be unique where present',
        'business_rule': True,
    }
})

# 3. Value type should be in set
business_expectations.append({
    'expectation_type': 'expect_column_values_to_be_in_set',
    'kwargs': {
        'column': 'Value type (A,D,E)',
        'value_set': ['A', 'D', 'E', 'a', 'd', 'e'],  # Allow lowercase
    },
    'meta': {
        'severity': 'high',
        'description': 'Value type must be A (Actual), D (Delta), or E (Estimate)',
        'business_rule': True,
    }
})

# 4. Source Type should be known
business_expectations.append({
    'expectation_type': 'expect_column_values_to_be_in_set',
    'kwargs': {
        'column': 'Source Type',
        'value_set': list(df['Source Type'].dropna().unique()),
    },
    'meta': {
        'severity': 'medium',
        'description': 'Source Type should be one of the known sources',
        'business_rule': True,
    }
})

# Add to config
enhanced_config = config.copy()
enhanced_config['validation_name'] = 'causeway_financial_validation_enhanced'
enhanced_config['description'] = 'Enhanced validation with business rules for CAUSEWAY financial data'
enhanced_config['expectations'].extend(business_expectations)

print(f"   ✓ Added {len(business_expectations)} business-specific expectations")
print(f"   ✓ Total expectations: {len(enhanced_config['expectations'])}")

# Save enhanced config
import yaml
with open('examples/causeway_financial_enhanced.yml', 'w') as f:
    yaml.dump(enhanced_config, f, default_flow_style=False, sort_keys=False)
print("   ✓ Saved to: examples/causeway_financial_enhanced.yml")

# ============================================================================
# PHASE 5: RUN VALIDATION
# ============================================================================
print("\n\n" + "=" * 80)
print("PHASE 5: RUN VALIDATION WITH ENHANCED CONFIG")
print("-" * 80)

print("\n✓ Running validation...")
validator = DataQualityValidator(config_dict=enhanced_config)
results = validator.validate(df)

print(f"\n📊 VALIDATION RESULTS:")
print(f"   Overall Status: {'✅ PASSED' if results['success'] else '❌ FAILED'}")
print(f"   Success Rate: {results['success_rate']:.1f}%")
print(f"   Checks Evaluated: {results['evaluated_checks']}")
print(f"   Checks Passed: {results['successful_checks']}")
print(f"   Checks Failed: {results['failed_checks']}")

if not results['success'] and 'failed_expectations' in results:
    print(f"\n❌ FAILED CHECKS:")
    for i, failure in enumerate(results['failed_expectations'][:5], 1):
        print(f"   {i}. {failure['expectation']} - Column: {failure['column']}")
    if len(results['failed_expectations']) > 5:
        print(f"   ... and {len(results['failed_expectations']) - 5} more")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n\n" + "=" * 80)
print("✅ WORKFLOW COMPLETE!")
print("=" * 80)

print("\n📚 WHAT YOU LEARNED:")
print("   1. How to profile your data first (DataProfiler)")
print("   2. How to auto-generate initial config")
print("   3. How to enhance with business rules")
print("   4. How to run validation and interpret results")

print("\n📁 FILES GENERATED:")
print("   • examples/causeway_auto_generated_v1.yml - Auto-generated baseline")
print("   • examples/causeway_financial_enhanced.yml - With business rules")

print("\n💡 NEXT STEPS:")
print("   1. Review the enhanced config file")
print("   2. Adjust severity levels based on your needs")
print("   3. Add more business-specific rules")
print("   4. Integrate into your ETL pipeline")
print("   5. Use this same workflow for HSS, AIMS, ACA projects!")

print("\n🎯 KEY TAKEAWAY:")
print("   Always start by profiling your data, then enhance with business knowledge!")
print()
