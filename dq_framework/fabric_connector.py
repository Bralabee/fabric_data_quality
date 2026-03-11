"""
MS Fabric Data Quality Connector
=================================

Fabric-specific integration for data quality validation.
"""

import logging
import time
import warnings
from datetime import datetime
from typing import Any

try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkDataFrame = Any

# Import Fabric detection utilities from centralized location
from .utils import (
    FABRIC_UTILS_AVAILABLE,
    get_mssparkutils,
)

# Get mssparkutils reference (None if not available)
mssparkutils = get_mssparkutils()

from .alerting import AlertConfig, AlertDispatcher, AlertFormatter, create_channel
from .constants import (
    DEFAULT_RETENTION_DAYS,
    FABRIC_CONFIG_MAX_BYTES,
    FABRIC_LARGE_DATASET_THRESHOLD,
    FABRIC_SAMPLE_FRACTION,
    MAX_FAILURE_DISPLAY,
)
from .schema_tracker import SchemaTracker
from .storage import get_store, make_result_key
from .validation_history import ValidationHistory
from .validator import DataQualityValidator

logger = logging.getLogger(__name__)


class FabricDataQualityRunner:
    """
    MS Fabric-specific data quality runner.

    Features:
    - Native Spark DataFrame support (via pandas conversion)
    - Delta table validation
    - Lakehouse integration
    - Results storage in Fabric
    - Chunked processing for large datasets

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
        workspace_id: str | None = None,
        results_location: str | None = "Files/dq_results",
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
        self._store = get_store(results_dir=results_location)

        # Try to load config using Fabric utils if available (supports abfss)
        config_dict = None
        if FABRIC_UTILS_AVAILABLE:
            try:
                if (
                    config_path.startswith("abfss://")
                    or config_path.startswith("Files/")
                    or config_path.startswith("https://")
                ):
                    logger.info(f"Attempting to load config from Fabric path: {config_path}")
                    content = mssparkutils.fs.head(config_path, FABRIC_CONFIG_MAX_BYTES)
                    import yaml

                    config_dict = yaml.safe_load(content)
                    logger.info("Successfully loaded config using mssparkutils")
            except Exception as e:
                logger.warning(
                    f"Could not load config using mssparkutils: {e}. "
                    "Falling back to standard file I/O."
                )

        # Initialize core validator
        if config_dict:
            self.validator = DataQualityValidator(config_dict=config_dict)
        else:
            self.validator = DataQualityValidator(config_path=config_path)

        logger.info(f"FabricDataQualityRunner initialized for workspace: {workspace_id}")

        # ----- Lazy initialization of optional pipeline components -----
        self._alert_dispatcher = None
        self._schema_tracker = None
        self._history = None

        # Alerting (optional)
        alerts_cfg = self.validator.config.get("alerts")
        if alerts_cfg and alerts_cfg.get("enabled", False):
            try:
                alert_config = AlertConfig.from_dict(alerts_cfg)
                formatter = AlertFormatter()
                self._alert_dispatcher = AlertDispatcher(config=alert_config, formatter=formatter)
                for ch_cfg in alert_config.channels:
                    if ch_cfg.enabled:
                        channel = create_channel(ch_cfg)
                        ch_name = getattr(ch_cfg, "name", None) or ch_cfg.type
                        self._alert_dispatcher.register_channel(ch_name, channel)
                logger.info("AlertDispatcher initialized with %d channel(s)", len(alert_config.channels))
            except Exception as e:
                logger.warning("Failed to initialize AlertDispatcher: %s", e)
                self._alert_dispatcher = None

        # Schema tracking (optional)
        schema_cfg = self.validator.config.get("schema_tracking")
        if schema_cfg and schema_cfg.get("enabled", True) is not False:
            try:
                dataset_name = schema_cfg.get("dataset_name", self.validator.config.get("validation_name", "unknown"))
                self._schema_tracker = SchemaTracker(store=self._store, dataset_name=dataset_name)
                logger.info("SchemaTracker initialized for dataset: %s", dataset_name)
            except Exception as e:
                logger.warning("Failed to initialize SchemaTracker: %s", e)
                self._schema_tracker = None

        # Validation history (optional)
        history_cfg = self.validator.config.get("history")
        if history_cfg and history_cfg.get("enabled", True) is not False:
            try:
                dataset_name = history_cfg.get("dataset_name", self.validator.config.get("validation_name", "unknown"))
                retention_days = history_cfg.get("retention_days", DEFAULT_RETENTION_DAYS)
                self._history = ValidationHistory(dataset_name=dataset_name, retention_days=retention_days)
                logger.info("ValidationHistory initialized for dataset: %s", dataset_name)
            except Exception as e:
                logger.warning("Failed to initialize ValidationHistory: %s", e)
                self._history = None

    @property
    def config(self) -> dict[str, Any]:
        """Get current configuration."""
        return self.validator.config

    def _build_schema_from_df(self, df) -> dict:
        """Build a schema dict from a pandas DataFrame for schema tracking.

        Args:
            df: pandas DataFrame

        Returns:
            Dict with dataset_name, column_count, and columns sub-dict.
        """
        columns = {}
        for col in df.columns:
            null_count = int(df[col].isna().sum())
            total = len(df)
            columns[col] = {
                "dtype": str(df[col].dtype),
                "nullable": null_count > 0,
                "null_percent": round((null_count / total * 100) if total > 0 else 0.0, 2),
            }
        return {
            "dataset_name": self.validator.config.get("validation_name", "unknown"),
            "column_count": len(df.columns),
            "columns": columns,
        }

    def _determine_severity(self, results: dict) -> str:
        """Determine alert severity from validation results.

        Extracts severity_stats from results and returns highest failing severity.
        Falls back to 'medium' if no severity_stats present.
        """
        severity_stats = results.get("severity_stats")
        if not severity_stats:
            return "medium"
        for level in ("critical", "high", "medium", "low"):
            stats = severity_stats.get(level, {})
            if stats.get("total", 0) - stats.get("passed", 0) > 0:
                return level
        return "medium"

    def validate_spark_dataframe(
        self,
        spark_df: SparkDataFrame,
        batch_name: str | None = None,
        sample_large_data: bool = True,
        warn_memory_threshold_mb: int = 500,
        chunk_size: int | None = None,
    ) -> dict[str, Any]:
        """
        Validate a Spark DataFrame by converting to pandas.

        Args:
            spark_df: Spark DataFrame to validate
            batch_name: Name for this validation batch
            sample_large_data: Auto-sample if >100K rows (default: True)
            warn_memory_threshold_mb: Warn if estimated memory exceeds this MB (default: 500).
                                     Set to 0 to disable warning.
            chunk_size: If set, process in chunks of this many rows and aggregate results.
                       Useful for very large datasets that may cause OOM. (default: None)

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

        # Memory estimation and warning
        if row_count and warn_memory_threshold_mb > 0:
            estimated_mb = (row_count * col_count * 100) / (1024 * 1024)
            if estimated_mb > warn_memory_threshold_mb:
                logger.warning(
                    f"MEMORY WARNING: Estimated DataFrame size is ~{estimated_mb:.0f} MB "
                    f"(threshold: {warn_memory_threshold_mb} MB). "
                    f"Consider using sample_large_data=True or chunk_size "
                    f"to avoid potential OOM errors during toPandas() conversion."
                )

        # Chunked processing mode
        if chunk_size and row_count and row_count > chunk_size:
            logger.info(f"Using chunked processing: {chunk_size:,} rows per chunk")
            return self._validate_spark_chunked(spark_df, batch_name, chunk_size, row_count)

        # Sample if large dataset
        if row_count and row_count > FABRIC_LARGE_DATASET_THRESHOLD and sample_large_data:
            sample_size = min(
                FABRIC_LARGE_DATASET_THRESHOLD,
                int(row_count * FABRIC_SAMPLE_FRACTION),
            )
            logger.info(f"Sampling {sample_size:,} rows for validation")
            spark_df = spark_df.limit(sample_size)

        # Convert to Pandas
        try:
            pdf = spark_df.toPandas()
        except Exception as e:
            logger.error(f"Failed to convert Spark DataFrame to Pandas: {e}")
            raise

        start_time = time.time()

        # --- Stage 1: Schema check (fire-and-forget) ---
        schema_result = None
        if self._schema_tracker:
            try:
                current_schema = self._build_schema_from_df(pdf)
                schema_result = self._schema_tracker.check_and_alert(
                    current_schema, dispatcher=self._alert_dispatcher
                )
            except Exception as e:
                logger.error("Schema check failed (continuing): %s", e)
                schema_result = None

        # --- Stage 2: Validation (core, unchanged) ---
        results = self.validator.validate(
            df=pdf,
            batch_name=batch_name or f"spark_df_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        )

        # Persist results via pluggable store
        try:
            key = make_result_key(results.get("batch_name", "unknown"))
            self._store.write(key, results)
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

        elapsed = time.time() - start_time

        # --- Stage 3: Record history (fire-and-forget) ---
        history_recorded = False
        if self._history:
            try:
                self._history.record(results, duration_seconds=elapsed)
                self._history.apply_retention()
                history_recorded = True
            except Exception as e:
                logger.error("History recording failed (continuing): %s", e)

        # --- Stage 4: Alert on failure (fire-and-forget) ---
        if self._alert_dispatcher and not results.get("success", True):
            try:
                severity = self._determine_severity(results)
                self._alert_dispatcher.dispatch(results, severity=severity)
            except Exception as e:
                logger.error("Alert dispatch failed (continuing): %s", e)

        # --- Augment results with pipeline metadata ---
        if schema_result is not None:
            results["schema_check"] = schema_result
        if history_recorded:
            results["history_recorded"] = True

        return results

    def _validate_spark_chunked(
        self,
        spark_df: SparkDataFrame,
        batch_name: str | None,
        chunk_size: int,
        total_rows: int,
    ) -> dict[str, Any]:
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

        # row_number() produces consecutive 1-based integers.
        # orderBy(lit(1)) preserves existing row order.
        # Non-deterministic across runs is acceptable — chunking is a
        # memory optimization, not a row-assignment guarantee.
        from pyspark.sql.functions import lit, row_number
        from pyspark.sql.window import Window

        window = Window.orderBy(lit(1))
        spark_df_with_id = spark_df.withColumn("__chunk_row_num__", row_number().over(window))

        for chunk_idx in range(num_chunks):
            lower = chunk_idx * chunk_size + 1
            upper = (chunk_idx + 1) * chunk_size
            chunk_df = spark_df_with_id.filter(
                (spark_df_with_id["__chunk_row_num__"] >= lower)
                & (spark_df_with_id["__chunk_row_num__"] <= upper)
            ).drop("__chunk_row_num__")

            try:
                pdf_chunk = chunk_df.toPandas()
                chunk_result = self.validator.validate(
                    df=pdf_chunk,
                    batch_name=f"{batch_name or 'spark_df'}__chunk_{chunk_idx + 1}",
                )
                all_results.append(chunk_result)
                logger.info(
                    f"Chunk {chunk_idx + 1}/{num_chunks}: "
                    f"success_rate={chunk_result['success_rate']:.1f}%"
                )
            except Exception as e:
                logger.error(f"Chunk {chunk_idx + 1} failed: {e}")
                all_results.append(
                    {
                        "success": False,
                        "error": str(e),
                        "chunk_index": chunk_idx,
                    }
                )

        # Aggregate results
        aggregated = self._aggregate_chunk_results(all_results, batch_name)

        # Persist results via pluggable store
        try:
            key = make_result_key(aggregated.get("batch_name", "unknown"))
            self._store.write(key, aggregated)
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

        return aggregated

    def _aggregate_chunk_results(
        self,
        chunk_results: list[dict[str, Any]],
        batch_name: str | None,
    ) -> dict[str, Any]:
        """Aggregate validation results from multiple chunks.

        Uses per-expectation averaging: all chunks run the same suite so
        evaluated_checks equals the per-chunk count (not the sum).
        success_rate is the mean across chunks.
        """
        from .constants import DEFAULT_VALIDATION_THRESHOLD

        valid_results = [r for r in chunk_results if "error" not in r]
        error_results = [r for r in chunk_results if "error" in r]

        if not valid_results:
            return {
                "success": False,
                "batch_name": batch_name or f"spark_df_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "suite_name": "unknown",
                "timestamp": datetime.now().isoformat(),
                "evaluated_checks": 0,
                "successful_checks": 0,
                "failed_checks": 0,
                "success_rate": 0.0,
                "failed_expectations": [],
                "chunked_processing": True,
                "num_chunks": len(chunk_results),
                "chunk_errors": [r.get("error") for r in error_results],
            }

        # All chunks run the same expectation suite
        evaluated_checks = valid_results[0].get("evaluated_checks", 0)
        threshold = valid_results[0].get("threshold", DEFAULT_VALIDATION_THRESHOLD)

        # Mean success_rate across chunks
        avg_success_rate = sum(r.get("success_rate", 0.0) for r in valid_results) / len(
            valid_results
        )

        # Derive failed_checks from average rate
        avg_successful = round(evaluated_checks * avg_success_rate / 100.0)
        avg_failed = evaluated_checks - avg_successful

        # Deduplicate failed_expectations by (expectation_type, column)
        seen = set()
        deduped_failures = []
        for r in valid_results:
            for fail in r.get("failed_expectations", []):
                key = (fail.get("expectation", ""), fail.get("column", ""))
                if key not in seen:
                    seen.add(key)
                    deduped_failures.append(fail)

        # Per-chunk breakdown
        chunks_detail = []
        for idx, r in enumerate(chunk_results):
            if "error" in r:
                chunks_detail.append(
                    {
                        "chunk_index": idx,
                        "success": False,
                        "error": r["error"],
                    }
                )
            else:
                chunks_detail.append(
                    {
                        "chunk_index": idx,
                        "success": r.get("success", False),
                        "success_rate": r.get("success_rate", 0.0),
                        "evaluated_checks": r.get("evaluated_checks", 0),
                        "failed_checks": r.get("failed_checks", 0),
                        "failed_expectations": r.get("failed_expectations", []),
                    }
                )

        return {
            "success": avg_success_rate >= threshold,
            "batch_name": batch_name or f"spark_df_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "suite_name": valid_results[0].get("suite_name", "unknown"),
            "timestamp": datetime.now().isoformat(),
            "evaluated_checks": evaluated_checks,
            "successful_checks": avg_successful,
            "failed_checks": avg_failed,
            "success_rate": avg_success_rate,
            "failed_expectations": deduped_failures[:MAX_FAILURE_DISPLAY],
            "chunked_processing": True,
            "num_chunks": len(chunk_results),
            "chunks": chunks_detail,
            "chunk_errors": [r.get("error") for r in error_results] if error_results else None,
        }

    def validate_delta_table(
        self,
        table_name: str,
        batch_name: str | None = None,
    ) -> dict[str, Any]:
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

        spark = SparkSession.builder.getOrCreate()

        try:
            spark_df = spark.table(table_name)
        except Exception as e:
            logger.error(f"Failed to load table {table_name}: {e}")
            raise

        return self.validate_spark_dataframe(
            spark_df=spark_df,
            batch_name=batch_name or f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        )

    def validate_lakehouse_file(
        self,
        file_path: str,
        file_format: str = "parquet",
        batch_name: str | None = None,
    ) -> dict[str, Any]:
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

        try:
            fmt = file_format.lower()
            if fmt == "parquet":
                spark_df = spark.read.parquet(file_path)
            elif fmt == "csv":
                spark_df = spark.read.csv(file_path, header=True, inferSchema=True)
            elif fmt == "json":
                spark_df = spark.read.json(file_path)
            elif fmt == "delta":
                spark_df = spark.read.format("delta").load(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            raise

        return self.validate_spark_dataframe(
            spark_df=spark_df,
            batch_name=batch_name or f"file_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        )

    def handle_failure(
        self,
        results: dict[str, Any],
        action: str = "log",
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
        if results["success"]:
            logger.info("Validation passed. No failure handling needed.")
            return

        logger.error("=" * 60)
        logger.error("DATA QUALITY VALIDATION FAILED")
        logger.error("=" * 60)
        logger.error(f"Suite: {results['suite_name']}")
        logger.error(f"Batch: {results['batch_name']}")
        logger.error(f"Failed checks: {results['failed_checks']}/{results['evaluated_checks']}")
        logger.error(f"Success rate: {results['success_rate']:.1f}%")

        if "failed_expectations" in results:
            logger.error("\nFailed expectations:")
            for fail in results["failed_expectations"][:MAX_FAILURE_DISPLAY]:
                logger.error(f"  - {fail['expectation']} on column '{fail['column']}'")

        if action == "halt":
            raise ValueError(
                f"Data quality validation failed for {results['suite_name']}. "
                f"{results['failed_checks']} checks failed."
            )
        elif action == "alert":
            self._send_alert(results)

    def _send_alert(self, results: dict[str, Any], **_kwargs: Any) -> bool:
        """Send alert about validation failure (DEPRECATED — use AlertDispatcher)."""
        warnings.warn(
            "_send_alert is deprecated, use AlertDispatcher",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._alert_dispatcher:
            try:
                severity = self._determine_severity(results)
                self._alert_dispatcher.dispatch(results, severity=severity)
                return True
            except Exception as e:
                logger.error("AlertDispatcher.dispatch failed in _send_alert: %s", e)
                return False

        logger.warning("No AlertDispatcher configured — alert not sent")
        return False


def quick_validate(
    df,
    config_path: str,
    halt_on_failure: bool = False,
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
        ...     pass  # This won't run if validation fails
    """
    # Check if Spark DataFrame
    if SPARK_AVAILABLE and isinstance(df, SparkDataFrame):
        runner = FabricDataQualityRunner(config_path=config_path)
        results = runner.validate_spark_dataframe(df)
    else:
        # Assume pandas DataFrame
        validator = DataQualityValidator(config_path=config_path)
        results = validator.validate(df)

    if not results["success"] and halt_on_failure:
        raise ValueError(
            f"Data quality validation failed: {results['failed_checks']} checks failed"
        )

    return results["success"]
