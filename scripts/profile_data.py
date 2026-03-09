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

    # Profile a directory of mixed files (generates multiple configs)
    python scripts/profile_data.py path/to/data_folder/ --output config/

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
import concurrent.futures
import sys
from pathlib import Path

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

# Try to import mssparkutils for Fabric support
try:
    from notebookutils import mssparkutils  # noqa: F401
    FABRIC_AVAILABLE = True
except ImportError:
    FABRIC_AVAILABLE = False

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from dq_framework import DataLoader, DataProfiler
from dq_framework.utils import FileSystemHandler

# FileSystemHandler and load_data are now imported from dq_framework



def process_single_file(file_path: Path | str, args, output_dir: Path | None = None, quiet: bool = False):
    """
    Process a single file: load, profile, and generate config.
    """
    path_str = str(file_path)
    file_name = FileSystemHandler.get_name(path_str)

    if not quiet:
        print(f"\n{'='*40}")
        print(f"Processing: {file_name}")
        print(f"{'='*40}")
        print(f"📥 Loading data from: {file_name}")

    # Check file size and auto-sample if too large (Local only for now)
    local_sample = args.sample
    # Note: DataLoader handles auto-sampling for large files internally now

    # Load data
    try:
        df = DataLoader.load_data(
            path_str,
            sample_size=local_sample,
            encoding=args.encoding
        )
        if not quiet:
            print(f"   ✓ Loaded {len(df):,} rows and {len(df.columns)} columns")
    except Exception as e:
        msg = f"❌ Skipping {file_name}: {e}"
        if not quiet:
            print(msg)
        return msg

    if not quiet:
        print()
        print("🔍 Profiling data structure...")

    profiler = DataProfiler(df, sample_size=args.sample)
    profile = profiler.profile()

    if not quiet:
        print("   ✓ Analysis complete")
        print(f"   ✓ Data Quality Score: {profile['data_quality_score']:.1f}/100")
        print()
        # Show profile summary
        profiler.print_summary()
        print()

    # Generate config unless --profile-only
    if not args.profile_only:
        if not quiet:
            print("⚙️  Generating validation configuration...")

        # Determine validation name
        stem = Path(file_name).stem
        validation_name = args.name if args.name else f"{stem}_validation"

        # Determine output path
        if args.output:
            # If output is a directory (or ends in /), treat as dir
            if Path(args.output).suffix == '' or args.output.endswith('/'):
                out_dir = Path(args.output)
                # Ensure output dir exists (local only)
                if not FileSystemHandler.is_abfss(str(out_dir)):
                    out_dir.mkdir(parents=True, exist_ok=True)
                output_path = out_dir / f"{validation_name}.yml"
            else:
                # If explicit file path given, use it (only valid for single file mode really)
                output_path = Path(args.output)
        else:
            output_path = Path(f"config/{validation_name}.yml")

        # Generate config
        config = profiler.generate_expectations(
            validation_name=validation_name,
            description=args.description or f"Auto-generated validation for {file_name}",
            severity_threshold=args.severity,
            include_structural=not args.no_structural,
            include_completeness=not args.no_completeness,
            include_validity=not args.no_validity,
            null_tolerance=args.null_tolerance,
        )

        if not quiet:
            print(f"   ✓ Generated {len(config['expectations'])} expectations")

        # Ensure output directory exists (local only)
        if not FileSystemHandler.is_abfss(str(output_path)):
            output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save config
        profiler.save_config(config, str(output_path))

        if not quiet:
            print(f"   ✓ Saved to: {output_path}")

        return f"✅ {file_name} -> {output_path} ({len(config['expectations'])} rules)"

    return f"✅ {file_name} profiled (Score: {profile['data_quality_score']:.1f})"


def main():
    parser = argparse.ArgumentParser(
        description="Profile data and generate data quality validation configs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Profile CSV and generate validation config
  python scripts/profile_data.py data/transactions.csv --output config/transactions_validation.yml

  # Profile ALL files in a directory
  python scripts/profile_data.py data/raw_files/ --output config/

  # Profile parquet with custom settings
  python scripts/profile_data.py data/events.parquet --null-tolerance 10 --severity medium
        """
    )

    # Required arguments
    parser.add_argument(
        'input_path',
        help='Path to data file OR directory containing data files'
    )

    # Output options
    parser.add_argument(
        '-o', '--output',
        help='Output path (file path for single file, directory for folder input)',
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

    parser.add_argument(
        '--workers',
        type=int,
        default=1,
        help='Number of parallel workers for batch processing (default: 1)'
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

    input_path_str = args.input_path
    FileSystemHandler.is_abfss(input_path_str)

    if not FileSystemHandler.exists(input_path_str):
        print(f"❌ Error: Path not found: {input_path_str}")
        return 1

    # Check if directory
    if FileSystemHandler.is_dir(input_path_str):
        # Validate output path for directory input
        if args.output and Path(args.output).suffix != '' and not args.output.endswith('/'):
            print("❌ Error: When input is a directory, output must be a directory (or end with /).")
            print(f"   Received: {args.output}")
            return 1

        print(f"📂 Directory detected: {input_path_str}")
        print("   Scanning for supported files (csv, parquet, xlsx, json)...")

        supported_extensions = {'.csv', '.parquet', '.xlsx', '.xls', '.json'}

        try:
            all_files = FileSystemHandler.list_files(input_path_str)
            files_to_process = [
                p for p in all_files
                if FileSystemHandler.get_suffix(p) in supported_extensions
            ]
        except Exception as e:
            print(f"❌ Error scanning directory: {e}")
            return 1

        if not files_to_process:
            print("❌ No supported files found in directory.")
            return 1

        print(f"   Found {len(files_to_process)} files to process.")

        if args.workers > 1:
            print(f"🚀 Starting parallel processing with {args.workers} workers...")
            print(f"{'='*80}")

            # Note: Multiprocessing with mssparkutils might be tricky if workers don't inherit the context
            # But for file reading via pandas (which uses fsspec/adlfs under the hood), it should be fine
            # provided the environment is set up correctly.

            with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
                # Submit all tasks
                futures = [
                    executor.submit(process_single_file, fp, args, None, True)
                    for fp in files_to_process
                ]

                # Process results as they complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        print(result)
                    except Exception as e:
                        print(f"❌ Worker failed: {e}")
        else:
            # Sequential processing
            for file_path in files_to_process:
                process_single_file(file_path, args)

    else:
        # Single file mode
        process_single_file(input_path_str, args)

    print()
    print("=" * 80)
    print("✅ ALL OPERATIONS COMPLETE!")
    print("=" * 80)

if __name__ == "__main__":
    main()
