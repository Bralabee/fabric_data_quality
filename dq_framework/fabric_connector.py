"""
MS Fabric Data Quality Connector
=================================

Fabric-specific integration for data quality validation.
"""

import logging
from typing import Dict, Optional, Any
from datetime import datetime

try:
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkDataFrame = Any

try:
    from notebookutils import mssparkutils
    FABRIC_UTILS_AVAILABLE = True
except ImportError:
    FABRIC_UTILS_AVAILABLE = False

from .validator import DataQualityValidator

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
        
        # Initialize core validator
        self.validator = DataQualityValidator(config_path=config_path)
        
        logger.info(f"FabricDataQualityRunner initialized for workspace: {workspace_id}")
    
    def validate_spark_dataframe(
        self,
        spark_df: SparkDataFrame,
        batch_name: Optional[str] = None,
        sample_large_data: bool = True
    ) -> Dict[str, Any]:
        """
        Validate a Spark DataFrame.
        
        Args:
            spark_df: Spark DataFrame to validate
            batch_name: Name for this validation batch
            sample_large_data: Auto-sample if >100K rows
        
        Returns:
            Validation results dictionary
        """
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark not available. Install or run in Fabric environment.")
        
        logger.info("Validating Spark DataFrame...")
        
        # Get row count
        try:
            row_count = spark_df.count()
            logger.info(f"DataFrame has {row_count:,} rows")
        except Exception as e:
            logger.warning(f"Could not get row count: {e}")
            row_count = None
        
        # Sample if large dataset
        if row_count and row_count > 100000 and sample_large_data:
            sample_size = min(100000, int(row_count * 0.1))
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
        Handle validation failure.
        
        Args:
            results: Validation results dictionary
            action: Action to take (log, halt, alert)
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
            for fail in results['failed_expectations'][:10]:  # Show first 10
                logger.error(f"  - {fail['expectation']} on column '{fail['column']}'")
        
        if action == "halt":
            raise ValueError(
                f"Data quality validation failed for {results['suite_name']}. "
                f"{results['failed_checks']} checks failed."
            )
        elif action == "alert":
            self._send_alert(results)
    
    def _send_alert(self, results: Dict[str, Any]) -> None:
        """Send alert about validation failure."""
        # TODO: Implement alert logic (Teams, email, etc.)
        logger.info("Alert notification sent (placeholder)")
    
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
