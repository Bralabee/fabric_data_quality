"""
Batch Profiler
==============

Handles batch profiling of multiple files in parallel.
"""

import concurrent.futures
import time
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import logging

from .data_profiler import DataProfiler
from .utils import FileSystemHandler
from .loader import DataLoader

logger = logging.getLogger(__name__)

class BatchProfiler:
    """
    Profiles multiple files in parallel and generates expectations.
    """
    
    @staticmethod
    def process_single_file(file_path: str, output_dir: str, sample_size: Optional[int] = None, thresholds: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Profiles a single file and saves the configuration.
        """
        try:
            file_path_obj = Path(file_path)
            file_name = FileSystemHandler.get_name(file_path)
            
            # Load Data using robust DataLoader
            df = DataLoader.load_data(file_path, sample_size=sample_size)
                
            # Profile Data
            profiler = DataProfiler(df)
            profiler.profile()
            
            # Generate Expectations
            gen_kwargs = {
                "validation_name": f"validation_{file_path_obj.stem}",
                "severity_threshold": "medium"
            }
            if thresholds:
                gen_kwargs.update(thresholds)

            config = profiler.generate_expectations(**gen_kwargs)
            
            # Save Configuration
            output_path = Path(output_dir) / f"{file_path_obj.stem}_validation.yml"
            if not FileSystemHandler.is_abfss(output_dir):
                output_path.parent.mkdir(parents=True, exist_ok=True)
            
            profiler.save_config(config, str(output_path))
            
            return {
                "status": "success",
                "file": file_name,
                "rows": len(df),
                "expectations": len(config['expectations']),
                "output": str(output_path)
            }
            
        except Exception as e:
            return {
                "status": "error",
                "file": str(file_path),
                "error": str(e)
            }

    @classmethod
    def run_parallel_profiling(cls, input_dir: str, output_dir: str, workers: int = 1, sample_size: Optional[int] = None, thresholds: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Runs profiling in parallel for all supported files in the directory.
        """
        
        if not FileSystemHandler.exists(input_dir):
            print(f"Input path does not exist: {input_dir}")
            return []

        # Find all files
        all_files = FileSystemHandler.list_files(input_dir)
        supported_extensions = {'.parquet', '.csv', '.json', '.xlsx', '.xls'}
        files = [f for f in all_files if FileSystemHandler.get_suffix(f) in supported_extensions]
        
        if not files:
            print(f"No supported files found in {input_dir}")
            return []
        
        print(f"Found {len(files)} files. Starting processing with {workers} workers...")
        
        start_time = time.time()
        results = []
        
        # Use ProcessPoolExecutor for CPU-bound tasks
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            # Submit tasks
            future_to_file = {
                executor.submit(cls.process_single_file, f, output_dir, sample_size, thresholds): f 
                for f in files
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_file):
                result = future.result()
                results.append(result)
                
                if result['status'] == 'success':
                    print(f"{result['file']}: {result['rows']} rows -> {result['expectations']} rules")
                else:
                    print(f"{result['file']}: Failed - {result['error']}")
                    
        duration = time.time() - start_time
        print(f"\n✨ Completed in {duration:.2f} seconds.")
        return results
