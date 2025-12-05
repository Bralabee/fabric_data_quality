#!/usr/bin/env python3
"""
Demonstration: Data Quality Validation on CAUSEWAY Financial Data

This script demonstrates the complete workflow of using the Fabric Data Quality
Framework with real data, showing:
1. How to load CSV data
2. How to run validation with custom config
3. How to interpret and handle results
4. How the same framework works with different data types
"""

import sys
import os
from pathlib import Path

# Add framework to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from datetime import datetime


def demo_basic_validation():
    """Demo 1: Basic validation with pandas DataFrame"""
    print("=" * 80)
    print("DEMO 1: Basic Validation with Pandas DataFrame")
    print("=" * 80)
    
    from dq_framework import DataQualityValidator, ConfigLoader
    
    # Step 1: Load the data
    print("\n📥 Step 1: Loading CAUSEWAY data...")
    data_path = "sample_source_data/CAUSEWAY_combined_scr_2024.csv"
    # Use latin-1 encoding to handle special characters (£ symbol, etc.)
    df = pd.read_csv(data_path, encoding='latin-1', low_memory=False)
    print(f"   Loaded {len(df):,} rows and {len(df.columns)} columns")
    print(f"   Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Step 2: Load configuration
    print("\n📋 Step 2: Loading validation configuration...")
    config_path = "examples/causeway_financial_example.yml"
    loader = ConfigLoader()
    config = loader.load(config_path)
    print(f"   Config: {config['validation_name']}")
    print(f"   Expectations: {len(config['expectations'])} quality checks")
    
    # Step 3: Run validation
    print("\n✓ Step 3: Running data quality validation...")
    validator = DataQualityValidator(config_dict=config)  # Pass config as dict
    results = validator.validate(df)
    
    # Step 4: Display results
    print("\n📊 Step 4: Validation Results")
    print("-" * 80)
    print(f"Overall Status: {'✅ PASSED' if results['success'] else '❌ FAILED'}")
    print(f"Total Checks: {results['evaluated_checks']}")
    print(f"Passed: {results['successful_checks']}")
    print(f"Failed: {results['failed_checks']}")
    print(f"Success Rate: {results['success_rate']:.2f}%")
    
    # Show details if validation has the original validation result
    if 'validation_result' in results:
        print("\n📝 Detailed Results by Expectation:")
        print("-" * 80)
        
        for i, result in enumerate(results['validation_result'].results, 1):
            status = "✅" if result.success else "❌"
            severity = result.expectation_config.meta.get('severity', 'unknown') if result.expectation_config.meta else 'unknown'
            description = result.expectation_config.meta.get('description', 'No description') if result.expectation_config.meta else 'No description'
            expectation_type = result.expectation_config.expectation_type
        
        print(f"\n{i}. {status} [{severity.upper()}] {expectation_type}")
        print(f"   Description: {description}")
        
        if not result['success']:
            # Show why it failed
            if 'exception_message' in result:
                print(f"   ⚠️  Failure: {result['exception_message']}")
            if 'result' in result and isinstance(result['result'], dict):
                if 'unexpected_count' in result['result']:
                    print(f"   Unexpected values: {result['result']['unexpected_count']}")
                if 'unexpected_percent' in result['result']:
                    print(f"   Unexpected %: {result['result']['unexpected_percent']:.2f}%")
    
    return results


def demo_failure_by_severity():
    """Demo 2: Group failures by severity level"""
    print("\n\n" + "=" * 80)
    print("DEMO 2: Analyze Failures by Severity Level")
    print("=" * 80)
    
    from dq_framework import DataQualityValidator, ConfigLoader
    
    # Load data and config
    df = pd.read_csv("sample_source_data/CAUSEWAY_combined_scr_2024.csv", encoding='latin-1', low_memory=False)
    loader = ConfigLoader()
    config = loader.load("examples/causeway_financial_example.yml")
    
    # Run validation
    validator = DataQualityValidator(config)
    results = validator.validate(df)
    
    # Group by severity
    severity_groups = {
        'critical': [],
        'high': [],
        'medium': [],
        'low': []
    }
    
    if 'validation_result' in results and not results['success']:
        for result in results['validation_result'].results:
            if not result.success:
                severity = result.expectation_config.meta.get('severity', 'unknown') if result.expectation_config.meta else 'unknown'
                if severity in severity_groups:
                    severity_groups[severity].append(result)
    
    # Display by severity
    print("\n🔍 Failures Grouped by Severity:")
    print("-" * 80)
    
    for severity in ['critical', 'high', 'medium', 'low']:
        failures = severity_groups[severity]
        if failures:
            print(f"\n⚠️  {severity.upper()} Severity: {len(failures)} failures")
            for failure in failures:
                print(f"   - {failure['expectation_type']}")
                print(f"     {failure.get('meta', {}).get('description', '')}")
        else:
            print(f"\n✅ {severity.upper()} Severity: No failures")
    
    # Decision logic
    print("\n🎯 Recommended Actions:")
    print("-" * 80)
    
    if severity_groups['critical']:
        print("⛔ CRITICAL FAILURES DETECTED")
        print("   → STOP: Do not proceed with this data")
        print("   → Action: Investigate and fix source data issues")
    elif severity_groups['high']:
        print("⚠️  HIGH SEVERITY FAILURES DETECTED")
        print("   → WARNING: Proceed with caution")
        print("   → Action: Alert data team, log for investigation")
    elif severity_groups['medium'] or severity_groups['low']:
        print("ℹ️  MINOR ISSUES DETECTED")
        print("   → CONTINUE: Data quality is acceptable")
        print("   → Action: Log for monitoring, no immediate action required")
    else:
        print("✅ ALL QUALITY CHECKS PASSED")
        print("   → PROCEED: Data quality is excellent")


def demo_data_profiling():
    """Demo 3: Use validation results for data profiling"""
    print("\n\n" + "=" * 80)
    print("DEMO 3: Data Profiling from Validation Results")
    print("=" * 80)
    
    # Load data
    print("\n📊 Loading and profiling CAUSEWAY data...")
    df = pd.read_csv("sample_source_data/CAUSEWAY_combined_scr_2024.csv", encoding='latin-1', low_memory=False)
    
    print("\n1️⃣ Dataset Overview:")
    print(f"   Total Rows: {len(df):,}")
    print(f"   Total Columns: {len(df.columns)}")
    print(f"   Memory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    print("\n2️⃣ Key Column Statistics:")
    
    # Allocation Uniqid (Primary Key)
    print(f"\n   Allocation Uniqid (Primary Key):")
    print(f"   - Total values: {df['Allocation Uniqid'].count():,}")
    print(f"   - Unique values: {df['Allocation Uniqid'].nunique():,}")
    print(f"   - Null values: {df['Allocation Uniqid'].isna().sum():,}")
    print(f"   - Duplicate check: {'✅ All unique' if df['Allocation Uniqid'].is_unique else '❌ Has duplicates'}")
    
    # Base Value (Financial Amount)
    print(f"\n   Base Value (Financial Amount):")
    print(f"   - Non-null values: {df['Base Value'].count():,}")
    print(f"   - Null values: {df['Base Value'].isna().sum():,}")
    # Clean the value (remove commas and handle strings)
    try:
        base_values = df['Base Value'].astype(str).str.replace(',', '').astype(float)
        print(f"   - Min: £{base_values.min():,.2f}")
        print(f"   - Max: £{base_values.max():,.2f}")
        print(f"   - Mean: £{base_values.mean():,.2f}")
        print(f"   - Total Sum: £{base_values.sum():,.2f}")
        print(f"   - Negative values: {(base_values < 0).sum():,} ({(base_values < 0).sum() / len(base_values) * 100:.2f}%)")
    except:
        print(f"   - Unable to parse numeric values")
    
    # Contract code
    print(f"\n   Contract Code:")
    print(f"   - Non-null values: {df['Contract code'].count():,}")
    print(f"   - Unique contracts: {df['Contract code'].nunique():,}")
    print(f"   - Top 5 contracts:")
    for code, count in df['Contract code'].value_counts().head(5).items():
        print(f"     • {code}: {count:,} transactions")
    
    # Accounting Period
    print(f"\n   Accounting Period:")
    print(f"   - Non-null values: {df['Accounting Period YYYY/PP'].count():,}")
    print(f"   - Unique periods: {df['Accounting Period YYYY/PP'].nunique():,}")
    print(f"   - Date range:")
    for period in sorted(df['Accounting Period YYYY/PP'].dropna().unique())[:5]:
        count = (df['Accounting Period YYYY/PP'] == period).sum()
        print(f"     • {period}: {count:,} transactions")
    
    # Vendor analysis
    print(f"\n   Vendor Information:")
    print(f"   - Non-null vendor names: {df['Vendor name'].count():,}")
    print(f"   - Unique vendors: {df['Vendor name'].nunique():,}")
    print(f"   - Top 5 vendors by transaction count:")
    for vendor, count in df['Vendor name'].value_counts().head(5).items():
        print(f"     • {vendor}: {count:,} transactions")
    
    # Source Type
    print(f"\n   Source Type Distribution:")
    for source_type, count in df['Source Type'].value_counts().items():
        print(f"   - {source_type}: {count:,} ({count / len(df) * 100:.2f}%)")


def demo_comparison_different_configs():
    """Demo 4: Compare validation with different configuration levels"""
    print("\n\n" + "=" * 80)
    print("DEMO 4: Configuration Flexibility - Same Data, Different Rules")
    print("=" * 80)
    
    from dq_framework import DataQualityValidator
    
    # Load data
    df = pd.read_csv("sample_source_data/CAUSEWAY_combined_scr_2024.csv", encoding='latin-1', low_memory=False)
    
    # Config 1: Strict validation (Gold layer standards)
    print("\n📋 Configuration 1: STRICT Validation (Gold Layer)")
    print("-" * 80)
    strict_config = {
        "validation_name": "strict_causeway",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "Contract code"},
                "meta": {"severity": "critical", "description": "Contract code must be 100% complete"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "Vendor name"},
                "meta": {"severity": "critical", "description": "Vendor name must be 100% complete"}
            }
        ]
    }
    
    validator_strict = DataQualityValidator(strict_config)
    results_strict = validator_strict.validate(df)
    print(f"Result: {'✅ PASSED' if results_strict['success'] else '❌ FAILED'}")
    print(f"Success Rate: {results_strict['statistics']['success_percent']:.2f}%")
    
    # Config 2: Lenient validation (Bronze layer standards)
    print("\n📋 Configuration 2: LENIENT Validation (Bronze Layer)")
    print("-" * 80)
    lenient_config = {
        "validation_name": "lenient_causeway",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "Contract code", "mostly": 0.70},
                "meta": {"severity": "medium", "description": "Contract code should be at least 70% complete"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "Vendor name", "mostly": 0.60},
                "meta": {"severity": "low", "description": "Vendor name should be at least 60% complete"}
            }
        ]
    }
    
    validator_lenient = DataQualityValidator(lenient_config)
    results_lenient = validator_lenient.validate(df)
    print(f"Result: {'✅ PASSED' if results_lenient['success'] else '❌ FAILED'}")
    print(f"Success Rate: {results_lenient['statistics']['success_percent']:.2f}%")
    
    print("\n💡 Key Insight:")
    print("-" * 80)
    print("The SAME data can pass or fail depending on the validation rules!")
    print("• Bronze Layer: Use lenient rules to preserve all raw data")
    print("• Silver Layer: Use moderate rules for cleaned data")
    print("• Gold Layer: Use strict rules for business-critical data")


def main():
    """Run all demonstrations"""
    print("\n" + "=" * 80)
    print("FABRIC DATA QUALITY FRAMEWORK - LIVE DEMONSTRATION")
    print("Data Source: CAUSEWAY Financial Transactions (359,596 rows)")
    print("=" * 80)
    
    try:
        # Demo 1: Basic validation workflow
        results = demo_basic_validation()
        
        # Demo 2: Severity-based failure handling
        demo_failure_by_severity()
        
        # Demo 3: Data profiling
        demo_data_profiling()
        
        # Demo 4: Configuration flexibility
        demo_comparison_different_configs()
        
        print("\n\n" + "=" * 80)
        print("✅ DEMONSTRATION COMPLETE")
        print("=" * 80)
        print("\nKey Takeaways:")
        print("1. Same framework works for ANY data type (incidents, parquet, CSV, etc.)")
        print("2. Configuration determines what gets validated - no code changes needed")
        print("3. Severity levels help prioritize issues")
        print("4. Different rules for different data layers (bronze/silver/gold)")
        print("5. Results provide both pass/fail and detailed diagnostics")
        
        print("\n📁 Files Created:")
        print("   • examples/causeway_financial_example.yml (validation config)")
        print("   • examples/demo_causeway_validation.py (this demo script)")
        
        print("\n🎓 Next Steps:")
        print("   • Modify causeway_financial_example.yml to customize rules")
        print("   • Try with your own CSV/data files")
        print("   • Integrate into your Fabric notebooks")
        
    except Exception as e:
        print(f"\n❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
