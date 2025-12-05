"""
Demonstration: Universal Profiler with Multiple Data Sources
=============================================================

This script shows how the SAME profiler works with different data formats
and different projects. Profile once per dataset, use forever.
"""

import pandas as pd
import numpy as np
from dq_framework import DataProfiler
import tempfile
from pathlib import Path

print("=" * 80)
print("UNIVERSAL DATA PROFILER - Multi-Format Demonstration")
print("=" * 80)
print()

# ============================================================================
# EXAMPLE 1: CSV Data (like CAUSEWAY)
# ============================================================================
print("EXAMPLE 1: CSV Financial Data")
print("-" * 80)

# Create sample financial CSV
csv_data = pd.DataFrame({
    'transaction_id': range(1000, 2000),
    'customer_id': np.random.choice(['CUST001', 'CUST002', 'CUST003'], 1000),
    'amount': np.random.uniform(10, 1000, 1000),
    'status': np.random.choice(['APPROVED', 'PENDING', 'REJECTED'], 1000),
    'date': pd.date_range('2025-01-01', periods=1000, freq='H'),
})

# Add some nulls to simulate real data
csv_data.loc[::10, 'customer_id'] = None

print(f"Created sample CSV data: {len(csv_data)} rows")
print(f"Columns: {', '.join(csv_data.columns)}")

# Profile it
profiler = DataProfiler(csv_data)
profile = profiler.profile()

print(f"\nData Quality Score: {profile['data_quality_score']:.1f}/100")
print("\nKey findings:")
for col, info in profile['columns'].items():
    print(f"  • {col}: {info['detected_type']}, {info['null_percent']:.1f}% nulls")

# Generate config
config = profiler.generate_expectations(
    validation_name="financial_transactions_validation",
    null_tolerance=5.0
)
print(f"\nGenerated {len(config['expectations'])} expectations")
print()

# ============================================================================
# EXAMPLE 2: Parquet Data (like AIMS)
# ============================================================================
print("EXAMPLE 2: Parquet Analytical Data")
print("-" * 80)

# Create sample analytical parquet
parquet_data = pd.DataFrame({
    'record_id': range(1, 501),
    'metric_name': np.random.choice(['CPU', 'Memory', 'Disk', 'Network'], 500),
    'value': np.random.uniform(0, 100, 500),
    'threshold': [80] * 500,
    'timestamp': pd.date_range('2025-10-01', periods=500, freq='T'),
})

print(f"Created sample Parquet data: {len(parquet_data)} rows")
print(f"Columns: {', '.join(parquet_data.columns)}")

# Profile it
profiler = DataProfiler(parquet_data)
profile = profiler.profile()

print(f"\nData Quality Score: {profile['data_quality_score']:.1f}/100")
print("\nKey findings:")
for col, info in profile['columns'].items():
    print(f"  • {col}: {info['detected_type']}, {info['unique_percent']:.1f}% unique")

config = profiler.generate_expectations(
    validation_name="analytics_metrics_validation",
    null_tolerance=0.0  # Strict - no nulls allowed
)
print(f"\nGenerated {len(config['expectations'])} expectations")
print()

# ============================================================================
# EXAMPLE 3: Excel Data (like ACA Commercial)
# ============================================================================
print("EXAMPLE 3: Excel Survey Data")
print("-" * 80)

# Create sample survey excel
excel_data = pd.DataFrame({
    'respondent_id': range(1, 201),
    'age_group': np.random.choice(['18-25', '26-35', '36-45', '46+'], 200),
    'satisfaction': np.random.choice([1, 2, 3, 4, 5], 200),
    'would_recommend': np.random.choice(['Yes', 'No', 'Maybe'], 200),
    'comments': [f"Comment {i}" if i % 3 == 0 else None for i in range(200)],
})

print(f"Created sample Excel data: {len(excel_data)} rows")
print(f"Columns: {', '.join(excel_data.columns)}")

# Profile it
profiler = DataProfiler(excel_data)
profile = profiler.profile()

print(f"\nData Quality Score: {profile['data_quality_score']:.1f}/100")
print("\nKey findings:")
for col, info in profile['columns'].items():
    print(f"  • {col}: {info['detected_type']}, {info['null_percent']:.1f}% nulls, {info.get('unique_values', 'N/A')}")

config = profiler.generate_expectations(
    validation_name="survey_responses_validation",
    null_tolerance=30.0  # More tolerant for optional fields
)
print(f"\nGenerated {len(config['expectations'])} expectations")
print()

# ============================================================================
# EXAMPLE 4: Sparse Data (High Nulls)
# ============================================================================
print("EXAMPLE 4: Sparse Data with Many Nulls")
print("-" * 80)

# Create sparse data like your CAUSEWAY data
sparse_data = pd.DataFrame({
    'id': range(1, 101),
    'required_field': ['Value'] * 100,
    'often_empty': [None] * 60 + ['Data'] * 40,
    'rarely_used': [None] * 95 + ['Rare'] * 5,
    'always_empty': [None] * 100,
})

print(f"Created sparse data: {len(sparse_data)} rows")
print(f"Columns: {', '.join(sparse_data.columns)}")

# Profile it
profiler = DataProfiler(sparse_data)
profile = profiler.profile()

print(f"\nData Quality Score: {profile['data_quality_score']:.1f}/100")
print("\nKey findings (showing how profiler handles nulls):")
for col, info in profile['columns'].items():
    detected = info.get('detected_type', 'unknown')
    print(f"  • {col}: {info['null_percent']:.1f}% nulls → {detected}")

# Use high null tolerance
config = profiler.generate_expectations(
    validation_name="sparse_data_validation",
    null_tolerance=80.0  # Very tolerant
)
print(f"\nGenerated {len(config['expectations'])} expectations")
print("Note: High null tolerance means fewer null checks generated")
print()

# ============================================================================
# SUMMARY
# ============================================================================
print("=" * 80)
print("✅ DEMONSTRATION COMPLETE")
print("=" * 80)
print()
print("KEY TAKEAWAYS:")
print()
print("1️⃣  UNIVERSAL: Same profiler works for CSV, Parquet, Excel, JSON")
print("   - Auto-detects column types (id, categorical, numeric, text, etc.)")
print("   - Handles different encoding, nulls patterns, data densities")
print()
print("2️⃣  FLEXIBLE: Customize for your data characteristics")
print("   - Strict validation: --null-tolerance 0.0")
print("   - Moderate validation: --null-tolerance 10.0")
print("   - Tolerant validation: --null-tolerance 50.0 (for sparse data)")
print()
print("3️⃣  ONE-TIME SETUP: Profile each dataset once")
print("   - Financial data → financial_validation.yml")
print("   - Analytics data → analytics_validation.yml")
print("   - Survey data → survey_validation.yml")
print("   - Sparse data → sparse_validation.yml")
print()
print("4️⃣  REUSABLE: Use same config for all future data batches")
print("   - No need to re-profile every batch")
print("   - Fast validation using pre-defined rules")
print("   - Consistent quality checks across all runs")
print()
print("🎯 USE CASES:")
print()
print("  CAUSEWAY Financial (your data):")
print("    python profile_data.py CAUSEWAY_data.csv --null-tolerance 50 --sample 50000")
print()
print("  HSS Incidents (clean data):")
print("    python profile_data.py ../full_stack_hss/data/incidents.parquet --null-tolerance 5")
print()
print("  AIMS Data (strict requirements):")
print("    python profile_data.py ../AIMS_LOCAL/data/aims_data.parquet --null-tolerance 1")
print()
print("  ACA Commercial (varied data):")
print("    python profile_data.py ../ACA_COMMERCIAL/data.csv --null-tolerance 10")
print()
