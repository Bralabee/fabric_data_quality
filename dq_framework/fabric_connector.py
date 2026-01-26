"""
MS Fabric Data Quality Connector
=================================

Fabric-specific integration for data quality validation.
"""

import logging
import warnings
from typing import Dict, List, Optional, Any
from datetime import datetime

try:
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkDataFrame = Any

# Import Fabric detection utilities from centralized location
from .utils import (
    _is_fabric_runtime,
    FABRIC_AVAILABLE,
    FABRIC_UTILS_AVAILABLE,
    get_mssparkutils,
)

# Get mssparkutils reference (None if not available)
mssparkutils = get_mssparkutils()

from .validator import DataQualityValidator
from .constants import (
    FABRIC_CONFIG_MAX_BYTES,
    FABRIC_LARGE_DATASET_THRESHOLD,
    FABRIC_SAMPLE_FRACTION,
    MAX_FAILURE_DISPLAY,
)

logger = logging.getLogger(__name__)


class FabricDataQualityRunner:
    """
    MS Fabric-specific data quality runner.
    
    Features:
    - Native Spark DataFrame support
    - Delta table validation
    - Lakehouse integration
    - Results storage in Fabric
    
    Example (in Fabric Notebook):
        >>> runner = FabricDataQualityRunner(
        ...     config_path="Files/dq_configs/my_table.yml"
        ... )
        >>> results = runner.validate_delta_table("my_table_name")
        >>> if not results['success']:
        ...     runner.handle_failure(results)
    """
    
    def __init__(
        self,
        config_path: str,
        workspace_id: Optional[str] = None,
        results_location: Optional[str] = "Files/dq_results"
    ):
        """
        Initialize Fabric DQ Runner.
        
        Args:
            config_path: Path to YAML configuration (Lakehouse or absolute path)
            workspace_id: MS Fabric workspace ID (optional)
            results_location: Where to store results in Lakehouse
        """
        self.config_path = config_path
        self.workspace_id = workspace_id
        self.results_location = results_location
        
        # Try to load config using Fabric utils if available (supports abfss)
        config_dict = None
        if FABRIC_UTILS_AVAILABLE:
            try:
                # Check if path looks like abfss or if it's a Fabric path
                if config_path.startswith("abfss://") or config_path.startswith("Files/") or config_path.startswith("https://"):
                    logger.info(f"Attempting to load config from Fabric path: {config_path}")
                    # Read file content using mssparkutils
                    # head reads the first N bytes. 1MB should be enough for config files.
                    content = mssparkutils.fs.head(config_path, FABRIC_CONFIG_MAX_BYTES)
                    import yaml
                    config_dict = yaml.safe_load(content)
                    logger.info("Successfully loaded config using mssparkutils")
            except Exception as e:
                logger.warning(f"Could not load config using mssparkutils: {e}. Falling back to standard file I/O.")
        
        # Initialize core validator
        if config_dict:
            self.validator = DataQualityValidator(config_dict=config_dict)
        else:
            self.validator = DataQualityValidator(config_path=config_path)
        
        logger.info(f"FabricDataQualityRunner initialized for workspace: {workspace_id}")

    @property
    def config(self) -> Dict[str, Any]:
        """Get current configuration."""
        return self.validator.config
    
    def validate_spark_dataframe(
        self,
        spark_df: SparkDataFrame,
        batch_name: Optional[str] = None,
        sample_large_data: bool = True,
        warn_memory_threshold_mb: int = 500,
        chunk_size: Optional[int] = None,
        use_spark_native: bool = False
    ) -> Dict[str, Any]:
        """
        Validate a Spark DataFrame.
        
        Args:
            spark_df: Spark DataFrame to validate
            batch_name: Name for this validation batch
            sample_large_data: Auto-sample if >100K rows (default: True)
            warn_memory_threshold_mb: Warn if estimated memory exceeds this MB (default: 500)
                                     Set to 0 to disable warning.
            chunk_size: If set, process in chunks of this many rows and aggregate results.
                       Useful for very large datasets that may cause OOM. (default: None)
            use_spark_native: If True, use Great Expectations SparkDFDataset for native
                             Spark validation without toPandas() conversion. (default: False)
                             Recommended for multi-GB datasets to avoid memory issues.
        
        Returns:
            Validation results dictionary
        """
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark not available. Install or run in Fabric environment.")
        
        logger.info("Validating Spark DataFrame...")
        
        # Get row count and schema info
        try:
            row_count = spark_df.count()
            col_count = len(spark_df.columns)
            logger.info(f"DataFrame has {row_count:,} rows and {col_count} columns")
        except Exception as e:
            logger.warning(f"Could not get row count: {e}")
            row_count = None
            col_count = len(spark_df.columns)
        
        # Use Spark-native validation if requested
        if use_spark_native:
            logger.info("Using Spark-native validation (no toPandas conversion)")
            return self._validate_spark_native(spark_df, batch_name, row_count)
        
        # Memory estimation and warning
        if row_count and warn_memory_threshold_mb > 0:
            # Rough estimate: ~100 bytes per cell is a conservative estimate
            estimated_mb = (row_count * col_count * 100) / (1024 * 1024)
            if estimated_mb > warn_memory_threshold_mb:
                logger.warning(
                    f"⚠️ MEMORY WARNING: Estimated DataFrame size is ~{estimated_mb:.0f} MB "
                    f"(threshold: {warn_memory_threshold_mb} MB). "
                    f"Consider using use_spark_native=True, sample_large_data=True, or chunk_size "
                    f"to avoid potential OOM errors during toPandas() conversion."
                )
        
        # Chunked processing mode
        if chunk_size and row_count and row_count > chunk_size:
            logger.info(f"Using chunked processing: {chunk_size:,} rows per chunk")
            return self._validate_spark_chunked(spark_df, batch_name, chunk_size, row_count)
        
        # Sample if large dataset
        if row_count and row_count > FABRIC_LARGE_DATASET_THRESHOLD and sample_large_data:
            sample_size = min(FABRIC_LARGE_DATASET_THRESHOLD, int(row_count * FABRIC_SAMPLE_FRACTION))
            logger.info(f"Sampling {sample_size:,} rows for validation")
            spark_df = spark_df.limit(sample_size)
        
        # Convert to Pandas
        try:
            pdf = spark_df.toPandas()
        except Exception as e:
            logger.error(f"Failed to convert Spark DataFrame to Pandas: {e}")
            raise
        
        # Run validation
        results = self.validator.validate(
            df=pdf,
            batch_name=batch_name or f"spark_df_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        # Store results if in Fabric
        if FABRIC_UTILS_AVAILABLE:
            self._save_results_to_lakehouse(results)
        
        return results
    
    def _validate_spark_native(
        self,
        spark_df: SparkDataFrame,
        batch_name: Optional[str],
        row_count: Optional[int]
    ) -> Dict[str, Any]:
        """
        Validate Spark DataFrame natively using Great Expectations SparkDFDataset.
        
        This avoids the memory overhead of toPandas() conversion, making it suitable
        for validating multi-GB datasets directly on Spark.
        
        Args:
            spark_df: Spark DataFrame to validate
            batch_name: Name for this validation batch
            row_count: Pre-computed row count (or None)
            
        Returns:
            Validation results dictionary
        """
        try:
            from great_expectations.dataset import SparkDFDataset
        except ImportError:
            logger.warning(
                "SparkDFDataset not available. Falling back to pandas-based validation. "
                "Install great_expectations[spark] for native Spark support."
            )
            # Fallback to pandas-based validation
            return self.validate_spark_dataframe(
                spark_df, batch_name, 
                sample_large_data=True, 
                use_spark_native=False
            )
        
        batch_name = batch_name or f"spark_native_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        timestamp = datetime.now().isoformat()
        
        try:
            # Wrap Spark DataFrame in Great Expectations SparkDFDataset
            ge_spark_df = SparkDFDataset(spark_df)
            
            # Run expectations from config
            expectations = self.validator.config.get('expectations', [])
            results_list = []
            passed = 0
            failed = 0
            failed_expectations = []
            
            for exp in expectations:
                exp_type = exp.get('expectation_type')
                kwargs = exp.get('kwargs', {})
                meta = exp.get('meta', {})
                
                try:
                    # Dynamically call the expectation method on SparkDFDataset
                    if hasattr(ge_spark_df, exp_type):
                        exp_method = getattr(ge_spark_df, exp_type)
                        result = exp_method(**kwargs)
                        
                        if result.get('success', False):
                            passed += 1
                        else:
                            failed += 1
                            failed_expectations.append({
                                'expectation': exp_type,
                                'column': kwargs.get('column', 'N/A'),
                                'severity': meta.get('severity', 'medium'),
                                'result': result
                            })
                        results_list.append(result)
                    else:
                        logger.warning(f"Expectation {exp_type} not supported in SparkDFDataset")
                        
                except Exception as e:
                    logger.warning(f"Expectation {exp_type} failed: {e}")
                    failed += 1
                    failed_expectations.append({
                        'expectation': exp_type,
                        'column': kwargs.get('column', 'N/A'),
                        'error': str(e)
                    })
            
            total = passed + failed
            success_rate = (passed / total * 100) if total > 0 else 0.0
            
            results = {
                'success': failed == 0,
                'batch_name': batch_name,
                'suite_name': self.validator.config.get('validation_name', 'spark_native'),
                'timestamp': timestamp,
                'evaluated_checks': total,
                'passed_checks': passed,
                'failed_checks': failed,
                'success_rate': success_rate,
                'failed_expectations': failed_expectations[:10],
                'spark_native': True,
                'row_count': row_count
            }
            
            logger.info(f"Spark-native validation complete: {passed}/{total} passed ({success_rate:.1f}%)")
            
            # Store results if in Fabric
            if FABRIC_UTILS_AVAILABLE:
                self._save_results_to_lakehouse(results)
            
            return results
            
        except Exception as e:
            logger.error(f"Spark-native validation failed: {e}")
            return {
                'success': False,
                'batch_name': batch_name,
                'suite_name': self.validator.config.get('validation_name', 'spark_native'),
                'timestamp': timestamp,
                'error': str(e),
                'spark_native': True
            }
    
    def _validate_spark_chunked(
        self,
        spark_df: SparkDataFrame,
        batch_name: Optional[str],
        chunk_size: int,
        total_rows: int
    ) -> Dict[str, Any]:
        """
        Validate a Spark DataFrame in chunks to avoid memory issues.
        
        Aggregates validation results across all chunks.
        
        Args:
            spark_df: Spark DataFrame to validate
            batch_name: Name for this validation batch
            chunk_size: Number of rows per chunk
            total_rows: Total row count
            
        Returns:
            Aggregated validation results
        """
        from math import ceil
        
        num_chunks = ceil(total_rows / chunk_size)
        logger.info(f"Processing {num_chunks} chunks of {chunk_size:,} rows each")
        
        all_results = []
        offset = 0
        
        # Add monotonically increasing id for offset-based chunking
        spark_df_with_id = spark_df.withColumn("__chunk_id__", 
            __import__('pyspark.sql.functions', fromlist=['monotonically_increasing_id']).monotonically_increasing_id())
        
        for chunk_idx in range(num_chunks):
            chunk_df = spark_df_with_id.filter(
                (spark_df_with_id["__chunk_id__"] >= offset) & 
                (spark_df_with_id["__chunk_id__"] < offset + chunk_size)
            ).drop("__chunk_id__")
            
            try:
                pdf_chunk = chunk_df.toPandas()
                chunk_result = self.validator.validate(
                    df=pdf_chunk,
                    batch_name=f"{batch_name or 'spark_df'}__chunk_{chunk_idx + 1}"
                )
                all_results.append(chunk_result)
                logger.info(f"Chunk {chunk_idx + 1}/{num_chunks}: "
                           f"success_rate={chunk_result['success_rate']:.1f}%")
            except Exception as e:
                logger.error(f"Chunk {chunk_idx + 1} failed: {e}")
                all_results.append({
                    'success': False,
                    'error': str(e),
                    'chunk_index': chunk_idx
                })
            
            offset += chunk_size
        
        # Aggregate results
        aggregated = self._aggregate_chunk_results(all_results, batch_name)
        
        # Store results if in Fabric
        if FABRIC_UTILS_AVAILABLE:
            self._save_results_to_lakehouse(aggregated)
        
        return aggregated
    
    def _aggregate_chunk_results(
        self,
        chunk_results: List[Dict[str, Any]],
        batch_name: Optional[str]
    ) -> Dict[str, Any]:
        """Aggregate validation results from multiple chunks."""
        total_evaluated = sum(r.get('evaluated_checks', 0) for r in chunk_results)
        total_passed = sum(r.get('passed_checks', 0) for r in chunk_results)
        total_failed = sum(r.get('failed_checks', 0) for r in chunk_results)
        
        all_failures = []
        for r in chunk_results:
            all_failures.extend(r.get('failed_expectations', []))
        
        success_rate = (total_passed / total_evaluated * 100) if total_evaluated > 0 else 0.0
        
        return {
            'success': total_failed == 0,
            'batch_name': batch_name or f"spark_df_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'suite_name': chunk_results[0].get('suite_name', 'unknown') if chunk_results else 'unknown',
            'timestamp': datetime.now().isoformat(),
            'evaluated_checks': total_evaluated,
            'passed_checks': total_passed,
            'failed_checks': total_failed,
            'success_rate': success_rate,
            'failed_expectations': all_failures[:10],  # Limit to first 10
            'chunked_processing': True,
            'num_chunks': len(chunk_results)
        }
    
    def validate_delta_table(
        self,
        table_name: str,
        batch_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate a Delta table in Lakehouse.
        
        Args:
            table_name: Name of Delta table (e.g., "bronze.incidents")
            batch_name: Name for this validation batch
        
        Returns:
            Validation results dictionary
        """
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark not available. Run in Fabric environment.")
        
        logger.info(f"Validating Delta table: {table_name}")
        
        # Get Spark session
        spark = SparkSession.builder.getOrCreate()
        
        # Load table
        try:
            spark_df = spark.table(table_name)
        except Exception as e:
            logger.error(f"Failed to load table {table_name}: {e}")
            raise
        
        # Validate
        return self.validate_spark_dataframe(
            spark_df=spark_df,
            batch_name=batch_name or f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
    
    def validate_lakehouse_file(
        self,
        file_path: str,
        file_format: str = "parquet",
        batch_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate a file in Lakehouse.
        
        Args:
            file_path: Path to file (e.g., "Files/data/mydata.parquet")
            file_format: File format (parquet, csv, json, delta)
            batch_name: Name for this validation batch
        
        Returns:
            Validation results dictionary
        """
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark not available. Run in Fabric environment.")
        
        logger.info(f"Validating file: {file_path} (format: {file_format})")
        
        spark = SparkSession.builder.getOrCreate()
        
        # Read file based on format
        try:
            if file_format.lower() == "parquet":
                spark_df = spark.read.parquet(file_path)
            elif file_format.lower() == "csv":
                spark_df = spark.read.csv(file_path, header=True, inferSchema=True)
            elif file_format.lower() == "json":
                spark_df = spark.read.json(file_path)
            elif file_format.lower() == "delta":
                spark_df = spark.read.format("delta").load(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            raise
        
        # Validate
        return self.validate_spark_dataframe(
            spark_df=spark_df,
            batch_name=batch_name or f"file_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
    
    def handle_failure(
        self,
        results: Dict[str, Any],
        action: str = "log"
    ) -> None:
        """
        Handle validation failure with configurable actions.
        
        Args:
            results: Validation results dictionary from validate_* methods
            action: Action to take on failure:
                - 'log': Log failure details (default, always executed)
                - 'halt': Raise ValueError to stop pipeline execution
                - 'alert': Send notification (placeholder - requires implementation)
        """
        if results['success']:
            logger.info("Validation passed. No failure handling needed.")
            return
        
        logger.error("=" * 60)
        logger.error("DATA QUALITY VALIDATION FAILED")
        logger.error("=" * 60)
        logger.error(f"Suite: {results['suite_name']}")
        logger.error(f"Batch: {results['batch_name']}")
        logger.error(f"Failed checks: {results['failed_checks']}/{results['evaluated_checks']}")
        logger.error(f"Success rate: {results['success_rate']:.1f}%")
        
        if 'failed_expectations' in results:
            logger.error("\nFailed expectations:")
            for fail in results['failed_expectations'][:MAX_FAILURE_DISPLAY]:  # Show first N
                logger.error(f"  - {fail['expectation']} on column '{fail['column']}'")
        
        if action == "halt":
            raise ValueError(
                f"Data quality validation failed for {results['suite_name']}. "
                f"{results['failed_checks']} checks failed."
            )
        elif action == "alert":
            self._send_alert(results)
    
    def _send_alert(
        self,
        results: Dict[str, Any],
        max_retries: int = 3,
        retry_delay_seconds: float = 1.0
    ) -> bool:
        """
        Send alert about validation failure with retry logic.
        
        Uses exponential backoff for reliability in case of transient failures.
        
        Args:
            results: Validation results dictionary
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay_seconds: Initial delay between retries in seconds (default: 1.0)
                                Delay doubles with each retry (exponential backoff)
        
        Returns:
            True if alert was sent successfully, False otherwise
        """
        import time
        
        alert_payload = {
            'suite_name': results.get('suite_name', 'unknown'),
            'batch_name': results.get('batch_name', 'unknown'),
            'success_rate': results.get('success_rate', 0),
            'failed_checks': results.get('failed_checks', 0),
            'timestamp': results.get('timestamp', datetime.now().isoformat()),
        }
        
        for attempt in range(max_retries + 1):
            try:
                # TODO: Implement actual alert logic (Teams, email, webhook, etc.)
                # Example webhook implementation:
                # response = requests.post(webhook_url, json=alert_payload)
                # response.raise_for_status()
                
                logger.info(f"Alert notification sent (attempt {attempt + 1}): {alert_payload}")
                return True
                
            except Exception as e:
                if attempt < max_retries:
                    delay = retry_delay_seconds * (2 ** attempt)  # Exponential backoff
                    logger.warning(
                        f"Alert attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"Alert failed after {max_retries + 1} attempts. "
                        f"Last error: {e}"
                    )
                    return False
        
        return False
    
    def _save_results_to_lakehouse(self, results: Dict[str, Any]) -> None:
        """Save validation results to Lakehouse."""
        if not FABRIC_UTILS_AVAILABLE:
            logger.warning("Fabric utilities not available. Cannot save results to Lakehouse.")
            return
        
        import json
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"{self.results_location}/validation_{results['batch_name']}_{timestamp}.json"
        
        try:
            results_json = json.dumps(results, indent=2, default=str)
            mssparkutils.fs.put(results_file, results_json, overwrite=True)
            logger.info(f"Results saved to: {results_file}")
        except Exception as e:
            logger.error(f"Failed to save results to Lakehouse: {e}")


def quick_validate(
    df,
    config_path: str,
    halt_on_failure: bool = False
) -> bool:
    """
    Quick validation helper function.
    
    Args:
        df: DataFrame (Spark or Pandas)
        config_path: Path to configuration file
        halt_on_failure: Raise exception on failure
    
    Returns:
        True if validation passed, False otherwise
    
    Example:
        >>> if not quick_validate(df, "Files/dq_configs/my_table.yml", halt_on_failure=True):
        ...     # This won't run if validation fails
        ...     pass
    """
    # Check if Spark DataFrame
    if SPARK_AVAILABLE and isinstance(df, SparkDataFrame):
        runner = FabricDataQualityRunner(config_path=config_path)
        results = runner.validate_spark_dataframe(df)
    else:
        # Assume pandas DataFrame
        validator = DataQualityValidator(config_path=config_path)
        results = validator.validate(df)
    
    if not results['success'] and halt_on_failure:
        raise ValueError(
            f"Data quality validation failed: {results['failed_checks']} checks failed"
        )
    
    return results['success']
