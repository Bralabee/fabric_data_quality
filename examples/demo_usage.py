"""
Demo Usage of Fabric Data Quality Library
=========================================

This script demonstrates how to use the library in another project.

Prerequisites:
1. Install the library:
   pip install path/to/fabric_data_quality-1.0.0-py3-none-any.whl

2. Or ensure 'dq_framework' folder is in your PYTHONPATH.
"""

import sys
import os
import pandas as pd

# Add the parent directory to path if running from source without installation
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from dq_framework import DataProfiler, DataQualityValidator
    print("✅ Successfully imported dq_framework")
except ImportError:
    print("❌ Could not import dq_framework. Please install the library or check your PYTHONPATH.")
    sys.exit(1)

def demo_profiling():
    print("\n--- Demo: Profiling Data ---")
    # Create sample data
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'david@example.com', 'eve@example.com']
    })
    
    print("Sample Data:")
    print(df.head())
    
    # Initialize Profiler
    profiler = DataProfiler()
    
    # Profile the dataframe
    print("\nProfiling data...")
    expectations = profiler.profile_dataframe(df)
    
    print(f"Generated {len(expectations)} expectations.")
    for exp in expectations[:3]:
        print(f" - {exp['expectation_type']}: {exp['kwargs']}")

def demo_validation():
    print("\n--- Demo: Validating Data ---")
    # In a real scenario, you would load config from a file
    # validator = DataQualityValidator(config_path='my_config.yml')
    
    # For demo, we'll just show the class is available
    print("DataQualityValidator is ready to use.")

if __name__ == "__main__":
    demo_profiling()
    demo_validation()
