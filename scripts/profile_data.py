#!/usr/bin/env python3
"""
Generic Data Profiler CLI Tool
================================

Universal data profiler that works with ANY data source:
- CSV files
- Parquet files
- Excel files
- JSON files
- Delta tables (via PySpark)
- SQL databases

Usage:
    # Profile any file
    python scripts/profile_data.py path/to/data.csv --output my_validation.yml
    
    # Profile with options
    python scripts/profile_data.py data.parquet --null-tolerance 5 --severity high
    
    # Profile and review only (no config generation)
    python scripts/profile_data.py data.xlsx --profile-only
    
    # Profile with custom name
    python scripts/profile_data.py data.csv --name "my_project_validation" --description "Production data validation"

This is a ONE-TIME setup tool. After profiling and generating your config:
1. Review and enhance the generated .yml file with business rules
2. Save it to your project's config directory
3. Use it for all future validation runs (no need to re-profile)
"""

import argparse
import sys
from pathlib import Path
import pandas as pd
from typing import Optional
import yaml

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from dq_framework import DataProfiler


def load_data(file_path: str, sample_size: Optional[int] = None, **kwargs) -> pd.DataFrame:
    """
    Load data from various file formats.
    
    Supports: CSV, Parquet, Excel, JSON
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    suffix = file_path.suffix.lower()
    
    print(f"📥 Loading data from: {file_path.name}")
    
    try:
        if suffix == '.csv':
            # Auto-detect encoding
            encoding_arg = kwargs.get('encoding')
            if encoding_arg:
                encodings = [encoding_arg] if isinstance(encoding_arg, str) else encoding_arg
            else:
                encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            
            df = None
            last_error = None
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, low_memory=False, nrows=sample_size)
                    print(f"   ✓ Detected encoding: {encoding}")
                    break
                except (UnicodeDecodeError, Exception) as e:
                    last_error = e
                    continue
            
            if df is None:
                raise ValueError(f"Could not detect file encoding. Last error: {last_error}")
                
        elif suffix == '.parquet':
            df = pd.read_parquet(file_path)
            if sample_size:
                df = df.head(sample_size)
                
        elif suffix in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path, nrows=sample_size)
            
        elif suffix == '.json':
            df = pd.read_json(file_path)
            if sample_size:
                df = df.head(sample_size)
                
        else:
            raise ValueError(f"Unsupported file format: {suffix}")
        
        print(f"   ✓ Loaded {len(df):,} rows and {len(df.columns)} columns")
        return df
        
    except Exception as e:
        print(f"   ❌ Error loading file: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Profile data and generate data quality validation configs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Profile CSV and generate validation config
  python profile_data.py data/transactions.csv --output config/transactions_validation.yml
  
  # Profile parquet with custom settings
  python profile_data.py data/events.parquet --null-tolerance 10 --severity medium
  
  # Profile only (no config generation)
  python profile_data.py data/survey.xlsx --profile-only
  
  # Profile large file with sampling
  python profile_data.py data/huge.csv --sample 100000 --output config/huge_validation.yml
        """
    )
    
    # Required arguments
    parser.add_argument(
        'input_file',
        help='Path to data file (CSV, Parquet, Excel, JSON)'
    )
    
    # Output options
    parser.add_argument(
        '-o', '--output',
        help='Output path for generated validation config (default: auto-generated name)',
        default=None
    )
    
    parser.add_argument(
        '--profile-only',
        action='store_true',
        help='Only show data profile, do not generate config'
    )
    
    # Validation config options
    parser.add_argument(
        '--name',
        help='Validation name (default: derived from filename)',
        default=None
    )
    
    parser.add_argument(
        '--description',
        help='Validation description',
        default=None
    )
    
    parser.add_argument(
        '--null-tolerance',
        type=float,
        default=5.0,
        help='Percentage of nulls to tolerate (default: 5.0)'
    )
    
    parser.add_argument(
        '--severity',
        choices=['critical', 'high', 'medium', 'low'],
        default='medium',
        help='Default severity level (default: medium)'
    )
    
    # Performance options
    parser.add_argument(
        '--sample',
        type=int,
        default=None,
        help='Sample size for large datasets (e.g., 10000)'
    )
    
    parser.add_argument(
        '--encoding',
        default=None,
        help='File encoding for CSV files (default: auto-detect)'
    )
    
    # Feature flags
    parser.add_argument(
        '--no-structural',
        action='store_true',
        help='Exclude structural checks'
    )
    
    parser.add_argument(
        '--no-completeness',
        action='store_true',
        help='Exclude completeness checks'
    )
    
    parser.add_argument(
        '--no-validity',
        action='store_true',
        help='Exclude validity checks'
    )
    
    args = parser.parse_args()
    
    # Print header
    print("=" * 80)
    print("DATA PROFILER - Universal Data Quality Assessment")
    print("=" * 80)
    print()
    
    # Load data
    try:
        df = load_data(
            args.input_file,
            sample_size=args.sample,
            encoding=args.encoding
        )
    except Exception as e:
        print(f"\n❌ Failed to load data: {e}")
        return 1
    
    print()
    
    # Profile data
    print("🔍 Profiling data structure...")
    profiler = DataProfiler(df, sample_size=args.sample)
    profile = profiler.profile()
    
    print(f"   ✓ Analysis complete")
    print(f"   ✓ Data Quality Score: {profile['data_quality_score']:.1f}/100")
    print()
    
    # Show profile summary
    profiler.print_summary()
    print()
    
    # Generate config unless --profile-only
    if not args.profile_only:
        print("⚙️  Generating validation configuration...")
        
        # Determine validation name
        validation_name = args.name
        if not validation_name:
            file_stem = Path(args.input_file).stem
            validation_name = f"{file_stem}_validation"
        
        # Determine output path
        output_path = args.output
        if not output_path:
            output_path = f"config/{validation_name}.yml"
        
        # Generate config
        config = profiler.generate_expectations(
            validation_name=validation_name,
            description=args.description or f"Auto-generated validation for {Path(args.input_file).name}",
            severity_threshold=args.severity,
            include_structural=not args.no_structural,
            include_completeness=not args.no_completeness,
            include_validity=not args.no_validity,
            null_tolerance=args.null_tolerance,
        )
        
        print(f"   ✓ Generated {len(config['expectations'])} expectations")
        
        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Save config
        profiler.save_config(config, str(output_file))
        print()
        
        # Show next steps
        print("=" * 80)
        print("✅ PROFILING COMPLETE!")
        print("=" * 80)
        print()
        print("📁 Generated file:")
        print(f"   {output_file}")
        print()
        print("💡 NEXT STEPS:")
        print("   1. Review the generated config file")
        print("   2. Enhance with business-specific rules")
        print("   3. Save to your project's config directory")
        print("   4. Use for all future validations (no need to re-profile)")
        print()
        print("📋 Example usage in code:")
        print(f"""
   from dq_framework import DataQualityValidator, ConfigLoader
   
   config = ConfigLoader().load('{output_file}')
   validator = DataQualityValidator(config_dict=config)
   results = validator.validate(df)
        """)
    else:
        print("=" * 80)
        print("✅ PROFILING COMPLETE!")
        print("=" * 80)
        print()
        print("💡 To generate validation config, run without --profile-only flag")
    
    print()
    return 0


if __name__ == '__main__':
    sys.exit(main())
