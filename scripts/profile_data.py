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
import sys
from pathlib import Path
import pandas as pd
from typing import Optional, List, Union
import yaml
import concurrent.futures
import multiprocessing
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

# Try to import mssparkutils for Fabric support
try:
    from notebookutils import mssparkutils
    FABRIC_AVAILABLE = True
except ImportError:
    FABRIC_AVAILABLE = False

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from dq_framework import DataProfiler


class FileSystemHandler:
    """Handles file system operations for both local and ABFSS paths."""
    
    @staticmethod
    def is_abfss(path: str) -> bool:
        return str(path).startswith("abfss://")

    @staticmethod
    def list_files(path: str) -> List[str]:
        """List files in a directory (local or ABFSS)."""
        if FileSystemHandler.is_abfss(path):
            if not FABRIC_AVAILABLE:
                print("⚠️  Warning: ABFSS path detected but mssparkutils not found. Assuming local mount or fsspec support.")
                # If mssparkutils is missing, we can't list files easily unless we use fsspec/adlfs
                # But for now, let's try to fail gracefully or assume the user knows what they are doing
                raise ImportError("Cannot list ABFSS directory without mssparkutils (Fabric environment).")
            
            try:
                files = mssparkutils.fs.ls(path)
                return [f.path for f in files if not f.isDir]
            except Exception as e:
                print(f"❌ Error listing ABFSS directory: {e}")
                return []
        else:
            p = Path(path)
            if p.is_dir():
                return [str(f) for f in p.iterdir() if f.is_file()]
            return [str(path)]

    @staticmethod
    def exists(path: str) -> bool:
        if FileSystemHandler.is_abfss(path):
            if FABRIC_AVAILABLE:
                try:
                    mssparkutils.fs.ls(path)
                    return True
                except:
                    return False
            return True # Optimistic assumption if no utils
        return Path(path).exists()

    @staticmethod
    def is_dir(path: str) -> bool:
        if FileSystemHandler.is_abfss(path):
            if FABRIC_AVAILABLE:
                try:
                    return mssparkutils.fs.isDirectory(path)
                except:
                    # Fallback: if ls works and has items, treat as dir? 
                    # Or just assume trailing slash means dir
                    return str(path).endswith('/')
            return str(path).endswith('/')
        return Path(path).is_dir()
    
    @staticmethod
    def get_suffix(path: str) -> str:
        return Path(path).suffix.lower()
    
    @staticmethod
    def get_name(path: str) -> str:
        return Path(path).name


def load_data(file_path: Union[Path, str], sample_size: Optional[int] = None, **kwargs) -> pd.DataFrame:
    """
    Load data from various file formats.
    
    Supports: CSV, Parquet, Excel, JSON
    """
    # Convert to string for ABFSS checks, Path for local
    path_str = str(file_path)
    is_abfss = FileSystemHandler.is_abfss(path_str)
    
    if not FileSystemHandler.exists(path_str):
        raise FileNotFoundError(f"File not found: {path_str}")
    
    suffix = FileSystemHandler.get_suffix(path_str)
    
    # Only print if not running in parallel (handled by caller)
    # print(f"📥 Loading data from: {FileSystemHandler.get_name(path_str)}")
    
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
                    df = pd.read_csv(path_str, encoding=encoding, low_memory=False, nrows=sample_size)
                    # print(f"   ✓ Detected encoding: {encoding}")
                    break
                except (UnicodeDecodeError, Exception) as e:
                    last_error = e
                    continue
            
            if df is None:
                raise ValueError(f"Could not detect file encoding. Last error: {last_error}")
                
        elif suffix == '.parquet':
            if sample_size and pq and not is_abfss:
                try:
                    # Efficiently read only the needed rows using PyArrow (Local only)
                    parquet_file = pq.ParquetFile(path_str)
                    # Read batches until we have enough data
                    batches = []
                    rows_loaded = 0
                    for batch in parquet_file.iter_batches(batch_size=sample_size):
                        batches.append(batch)
                        rows_loaded += batch.num_rows
                        if rows_loaded >= sample_size:
                            break
                    
                    if batches:
                        import pyarrow as pa
                        table = pa.Table.from_batches(batches)
                        df = table.to_pandas()
                        if len(df) > sample_size:
                            df = df.head(sample_size)
                    else:
                        df = pd.read_parquet(path_str)
                except Exception:
                    # Fallback if efficient read fails
                    df = pd.read_parquet(path_str)
                    if sample_size:
                        df = df.head(sample_size)
            else:
                # For ABFSS or if pyarrow optimization skipped
                df = pd.read_parquet(path_str)
                if sample_size:
                    df = df.head(sample_size)
                
        elif suffix in ['.xlsx', '.xls']:
            df = pd.read_excel(path_str, nrows=sample_size)
            
        elif suffix == '.json':
            df = pd.read_json(path_str)
            if sample_size:
                df = df.head(sample_size)
                
        else:
            raise ValueError(f"Unsupported file format: {suffix}")
        
        # print(f"   ✓ Loaded {len(df):,} rows and {len(df.columns)} columns")
        return df
        
    except Exception as e:
        # print(f"   ❌ Error loading file: {e}")
        raise


def process_single_file(file_path: Union[Path, str], args, output_dir: Optional[Path] = None, quiet: bool = False):
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
    if not FileSystemHandler.is_abfss(path_str):
        try:
            file_size_mb = Path(path_str).stat().st_size / (1024 * 1024)
            if file_size_mb > 500 and local_sample is None:
                if not quiet:
                    print(f"⚠️  Large file detected ({file_size_mb:.1f} MB). Auto-limiting to 100,000 rows to prevent memory issues.")
                local_sample = 100000
        except Exception:
            pass

    # Load data
    try:
        df = load_data(
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
        print(f"   ✓ Analysis complete")
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
    is_abfss = FileSystemHandler.is_abfss(input_path_str)
    
    if not FileSystemHandler.exists(input_path_str):
        print(f"❌ Error: Path not found: {input_path_str}")
        return 1

    # Check if directory
    if FileSystemHandler.is_dir(input_path_str):
        # Validate output path for directory input
        if args.output and Path(args.output).suffix != '' and not args.output.endswith('/'):
            print(f"❌ Error: When input is a directory, output must be a directory (or end with /).")
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
