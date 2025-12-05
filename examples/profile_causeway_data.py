"""
Profile CAUSEWAY Data and Generate Validation Config
=====================================================

This script analyzes the actual CAUSEWAY data structure and generates
an appropriate validation configuration based on what it finds.
"""

import pandas as pd
from dq_framework import DataProfiler

print("=" * 80)
print("PROFILING CAUSEWAY FINANCIAL DATA")
print("=" * 80)
print()

# Step 1: Load the data
print("📥 Step 1: Loading data...")
df = pd.read_csv(
    'sample_source_data/CAUSEWAY_combined_scr_2024.csv',
    encoding='latin-1',
    low_memory=False
)
print(f"   ✓ Loaded {len(df):,} rows and {len(df.columns)} columns")
print(f"   ✓ Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
print()

# Step 2: Profile the data
print("🔍 Step 2: Profiling data structure...")
profiler = DataProfiler(df, sample_size=50000)  # Sample for speed
profile = profiler.profile()
print(f"   ✓ Profiled {profile['row_count']:,} rows")
print(f"   ✓ Data Quality Score: {profile['data_quality_score']:.1f}/100")
print()

# Step 3: Show summary
print("📊 Step 3: Data Profile Summary")
print("-" * 80)
profiler.print_summary()
print()

# Step 4: Generate validation config
print("⚙️  Step 4: Generating validation configuration...")
config = profiler.generate_expectations(
    validation_name="causeway_financial_validation",
    description="Auto-generated validation for CAUSEWAY financial transaction data based on actual data structure",
    severity_threshold="medium",
    include_structural=True,
    include_completeness=True,
    include_validity=True,
    null_tolerance=10.0,  # More tolerant given the data has many nulls
)

print(f"   ✓ Generated {len(config['expectations'])} expectations")
print()

# Step 5: Save the config
output_path = 'examples/causeway_financial_generated.yml'
profiler.save_config(config, output_path)
print()

# Step 6: Show what was generated
print("📝 Step 5: Generated Expectations Summary")
print("-" * 80)

# Count by type
from collections import Counter
exp_types = Counter([exp['expectation_type'] for exp in config['expectations']])

print("Expectations by type:")
for exp_type, count in exp_types.most_common():
    print(f"  • {exp_type}: {count}")
print()

# Count by severity
severities = Counter([exp['meta']['severity'] for exp in config['expectations']])
print("Expectations by severity:")
for severity, count in severities.most_common():
    print(f"  • {severity}: {count}")
print()

print("=" * 80)
print("✅ PROFILING COMPLETE!")
print("=" * 80)
print()
print("Next steps:")
print(f"1. Review the generated config: {output_path}")
print("2. Adjust null_tolerance or severity levels if needed")
print("3. Run validation with: python examples/simple_demo.py")
print("4. Use this as a template for other datasets")
print()
