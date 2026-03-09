"""
Integration tests for Fabric connector
Note: Some tests require a Fabric environment with Spark.
Unit tests for chunked validation and aggregation use mocks.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml

from dq_framework.constants import (
    DEFAULT_VALIDATION_THRESHOLD,
)
from dq_framework.fabric_connector import FabricDataQualityRunner, quick_validate


class TestFabricDataQualityRunner:
    """Test suite for FabricDataQualityRunner"""

    @pytest.fixture
    def sample_config_path(self, tmp_path):
        """Create a temporary config file"""
        import yaml

        config = {
            "validation_name": "test_validation",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1},
                }
            ],
        }

        config_file = tmp_path / "test_config.yml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        return str(config_file)

    @pytest.mark.fabric
    def test_validate_spark_dataframe(self, sample_config_path):
        """Test validation of Spark DataFrame (requires Fabric)"""
        from pyspark.sql import SparkSession

        from dq_framework import FabricDataQualityRunner

        # Initialize Spark (in Fabric, spark is pre-initialized)
        spark = SparkSession.builder.getOrCreate()

        # Create test DataFrame
        df = spark.createDataFrame(
            [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)], ["id", "name", "age"]
        )

        # Validate
        runner = FabricDataQualityRunner(sample_config_path)
        results = runner.validate_spark_dataframe(df)

        assert results["success"] is True
        assert results["statistics"]["evaluated_expectations"] > 0

    def test_runner_initialization(self, sample_config_path):
        """Test runner initialization"""
        from dq_framework import FabricDataQualityRunner

        runner = FabricDataQualityRunner(sample_config_path)
        assert runner is not None
        assert runner.config is not None


# NOTE: pytest_addoption for --fabric is defined in tests/conftest.py
# Do not duplicate it here as pytest only allows one definition


class TestChunkedValidation:
    """Tests for _validate_spark_chunked method (BUG-01 fix)."""

    @pytest.fixture
    def runner(self, tmp_path):
        """Create a FabricDataQualityRunner with a minimal config."""
        import yaml

        config = {
            "validation_name": "chunked_test",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1},
                }
            ],
        }
        config_file = tmp_path / "test_config.yml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)
        return FabricDataQualityRunner(str(config_file))

    def _make_spark_df_mock(self):
        """Create a mock Spark DataFrame that supports the withColumn/filter/drop/toPandas chain."""
        import pandas as pd

        mock_spark_df = MagicMock()
        df_with_id = MagicMock()
        mock_spark_df.withColumn.return_value = df_with_id

        # Make __getitem__ return a MagicMock that supports comparison operators
        col_mock = MagicMock()
        col_mock.__ge__ = MagicMock(return_value=MagicMock())
        col_mock.__le__ = MagicMock(return_value=MagicMock())
        col_mock.__lt__ = MagicMock(return_value=MagicMock())
        df_with_id.__getitem__ = MagicMock(return_value=col_mock)

        filtered_df = MagicMock()
        df_with_id.filter.return_value = filtered_df
        dropped_df = MagicMock()
        filtered_df.drop.return_value = dropped_df
        dropped_df.toPandas.return_value = pd.DataFrame({"a": [1]})

        return mock_spark_df, df_with_id

    def _pyspark_mock_modules(self):
        """Return dict of mock pyspark modules for patching sys.modules."""
        return {
            "pyspark.sql.functions": MagicMock(),
            "pyspark.sql.window": MagicMock(),
        }

    @patch("dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE", False)
    def test_uses_row_number_not_monotonically_increasing_id(self, runner):
        """BUG-01: _validate_spark_chunked must use row_number(), not
        monotonically_increasing_id, so that every row gets a consecutive
        1-based integer and no rows are skipped by chunk filters."""
        import sys

        mock_spark_df, _df_with_id = self._make_spark_df_mock()
        runner.validator.validate = MagicMock(
            return_value={
                "success": True,
                "evaluated_checks": 1,
                "successful_checks": 1,
                "failed_checks": 0,
                "success_rate": 100.0,
                "suite_name": "chunked_test",
                "batch_name": "test__chunk_1",
                "timestamp": datetime.now().isoformat(),
                "failed_expectations": [],
                "threshold": DEFAULT_VALIDATION_THRESHOLD,
            }
        )

        with patch.dict(sys.modules, self._pyspark_mock_modules()):
            runner._validate_spark_chunked(mock_spark_df, "test", 5, 5)

        # Check that withColumn was called with "__chunk_row_num__" (row_number)
        # and NOT "__chunk_id__" (the old monotonically_increasing_id column)
        call_args = mock_spark_df.withColumn.call_args
        col_name = call_args[0][0]
        assert col_name == "__chunk_row_num__", (
            f"Expected column name '__chunk_row_num__' (row_number), got '{col_name}'"
        )

    @patch("dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE", False)
    def test_chunk_boundaries_are_1_based_consecutive(self, runner):
        """BUG-01: Chunk filter boundaries must be 1-based and consecutive,
        so chunk 0 covers rows 1..chunk_size, chunk 1 covers chunk_size+1..2*chunk_size, etc."""
        import sys

        mock_spark_df, df_with_id = self._make_spark_df_mock()
        runner.validator.validate = MagicMock(
            return_value={
                "success": True,
                "evaluated_checks": 1,
                "successful_checks": 1,
                "failed_checks": 0,
                "success_rate": 100.0,
                "suite_name": "chunked_test",
                "batch_name": "test__chunk_1",
                "timestamp": datetime.now().isoformat(),
                "failed_expectations": [],
                "threshold": DEFAULT_VALIDATION_THRESHOLD,
            }
        )

        chunk_size = 5
        total_rows = 12  # 3 chunks: 1-5, 6-10, 11-12
        with patch.dict(sys.modules, self._pyspark_mock_modules()):
            runner._validate_spark_chunked(mock_spark_df, "test", chunk_size, total_rows)

        # 3 chunks should have been processed
        assert df_with_id.filter.call_count == 3, (
            f"Expected 3 filter calls for 12 rows / 5 per chunk, got {df_with_id.filter.call_count}"
        )


class TestAggregateChunkResults:
    """Tests for _aggregate_chunk_results method (BUG-02 fix)."""

    @pytest.fixture
    def runner(self, tmp_path):
        """Create a FabricDataQualityRunner with a minimal config."""
        import yaml

        config = {
            "validation_name": "agg_test",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1},
                }
            ],
        }
        config_file = tmp_path / "test_config.yml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)
        return FabricDataQualityRunner(str(config_file))

    def _make_chunk_result(
        self,
        evaluated=10,
        successful=9,
        failed=1,
        success_rate=90.0,
        success=False,
        suite_name="test_suite",
        batch_name="test__chunk_1",
        failed_expectations=None,
        threshold=None,
    ):
        """Helper to create a chunk result dict matching validator output."""
        return {
            "success": success,
            "evaluated_checks": evaluated,
            "successful_checks": successful,
            "failed_checks": failed,
            "success_rate": success_rate,
            "suite_name": suite_name,
            "batch_name": batch_name,
            "timestamp": datetime.now().isoformat(),
            "failed_expectations": failed_expectations or [],
            "threshold": threshold if threshold is not None else DEFAULT_VALIDATION_THRESHOLD,
        }

    def test_evaluated_checks_is_single_chunk_count(self, runner):
        """BUG-02: evaluated_checks must equal the per-chunk count (all chunks
        run the same expectation suite), NOT the sum across chunks."""
        chunks = [
            self._make_chunk_result(evaluated=10, successful=9, failed=1, success_rate=90.0),
            self._make_chunk_result(evaluated=10, successful=8, failed=2, success_rate=80.0),
            self._make_chunk_result(evaluated=10, successful=10, failed=0, success_rate=100.0),
        ]
        result = runner._aggregate_chunk_results(chunks, "test_batch")
        assert result["evaluated_checks"] == 10, (
            f"Expected evaluated_checks=10 (per-chunk), got {result['evaluated_checks']}"
        )

    def test_mean_success_rate_across_chunks(self, runner):
        """BUG-02: success_rate must be the mean across chunks, not a ratio of summed totals."""
        chunks = [
            self._make_chunk_result(evaluated=10, successful=9, failed=1, success_rate=90.0),
            self._make_chunk_result(evaluated=10, successful=8, failed=2, success_rate=80.0),
            self._make_chunk_result(evaluated=10, successful=10, failed=0, success_rate=100.0),
        ]
        result = runner._aggregate_chunk_results(chunks, "test_batch")
        assert result["success_rate"] == pytest.approx(90.0), (
            f"Expected mean success_rate=90.0, got {result['success_rate']}"
        )

    def test_success_true_when_avg_meets_threshold(self, runner):
        """success should be True when average success_rate >= threshold."""
        # All 100% -> avg 100% >= DEFAULT_VALIDATION_THRESHOLD (100%)
        chunks = [
            self._make_chunk_result(
                evaluated=5, successful=5, failed=0, success_rate=100.0, success=True
            ),
            self._make_chunk_result(
                evaluated=5, successful=5, failed=0, success_rate=100.0, success=True
            ),
        ]
        result = runner._aggregate_chunk_results(chunks, "test_batch")
        assert result["success"] is True

    def test_success_false_when_avg_below_threshold(self, runner):
        """success should be False when average success_rate < threshold."""
        # 90% + 80% -> avg 85% < 100% threshold
        chunks = [
            self._make_chunk_result(evaluated=10, successful=9, failed=1, success_rate=90.0),
            self._make_chunk_result(evaluated=10, successful=8, failed=2, success_rate=80.0),
        ]
        result = runner._aggregate_chunk_results(chunks, "test_batch")
        assert result["success"] is False

    def test_chunks_key_with_per_chunk_breakdown(self, runner):
        """Aggregated result must include a 'chunks' key with per-chunk detail."""
        chunks = [
            self._make_chunk_result(
                evaluated=5, successful=5, failed=0, success_rate=100.0, success=True
            ),
            self._make_chunk_result(
                evaluated=5, successful=4, failed=1, success_rate=80.0, success=False
            ),
        ]
        result = runner._aggregate_chunk_results(chunks, "test_batch")

        assert "chunks" in result, "Result must contain a 'chunks' key"
        assert len(result["chunks"]) == 2

        chunk_detail = result["chunks"][0]
        # Each chunk breakdown should contain these keys
        for key in ["chunk_index", "success", "success_rate", "evaluated_checks", "failed_checks"]:
            assert key in chunk_detail, f"Chunk detail missing key '{key}'"

    def test_handle_failure_compatible_keys(self, runner):
        """Result dict must have failed_checks, evaluated_checks, success_rate for handle_failure."""
        chunks = [
            self._make_chunk_result(
                evaluated=5, successful=5, failed=0, success_rate=100.0, success=True
            ),
        ]
        result = runner._aggregate_chunk_results(chunks, "test_batch")
        for key in [
            "failed_checks",
            "evaluated_checks",
            "success_rate",
            "success",
            "suite_name",
            "batch_name",
        ]:
            assert key in result, f"Result missing key '{key}' required by handle_failure"

    def test_failed_expectations_deduplicated(self, runner):
        """failed_expectations at top level should be deduplicated across chunks."""
        dup_failure = {
            "expectation": "expect_column_values_to_not_be_null",
            "column": "email",
            "severity": "high",
            "details": {},
        }
        chunks = [
            self._make_chunk_result(failed_expectations=[dup_failure]),
            self._make_chunk_result(failed_expectations=[dup_failure]),
            self._make_chunk_result(failed_expectations=[dup_failure]),
        ]
        result = runner._aggregate_chunk_results(chunks, "test_batch")
        # Should have only 1 unique failure, not 3 duplicates
        assert len(result["failed_expectations"]) == 1, (
            f"Expected 1 deduplicated failure, got {len(result['failed_expectations'])}"
        )

    def test_all_error_chunks_produce_error_result(self, runner):
        """When all chunks have errors, result should be an error dict."""
        error_chunks = [
            {"success": False, "error": "OOM chunk 0", "chunk_index": 0},
            {"success": False, "error": "OOM chunk 1", "chunk_index": 1},
        ]
        result = runner._aggregate_chunk_results(error_chunks, "test_batch")
        assert result["success"] is False
        # Should indicate error state
        assert "chunk_errors" in result or "error" in result


class TestAllExports:
    """Verify __all__ does not export private symbols."""

    def test_is_fabric_runtime_not_in_all(self):
        """_is_fabric_runtime is a private utility and should not be in __all__."""
        import dq_framework

        assert "_is_fabric_runtime" not in dq_framework.__all__, (
            "_is_fabric_runtime should not be in __all__"
        )


# ---------------------------------------------------------------------------
# Phase 4 — New unit-test groups for fabric_connector.py coverage
# ---------------------------------------------------------------------------


class TestInitPaths:
    """Group 1: __init__ paths."""

    def test_init_loads_config_from_local_path(self, fabric_runner):
        """Verify validator is initialized from a YAML file on local filesystem."""
        assert fabric_runner is not None
        assert fabric_runner.config is not None
        assert fabric_runner.config["validation_name"] == "test_validation"

    @patch("dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE", True)
    @patch("dq_framework.fabric_connector.mssparkutils")
    def test_init_loads_config_from_fabric_path(self, mock_msutils, tmp_path):
        """When FABRIC_UTILS_AVAILABLE is True and path starts with 'Files/',
        config should be loaded via mssparkutils.fs.head."""
        config = {
            "validation_name": "fabric_loaded",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1},
                }
            ],
        }
        mock_msutils.fs.head.return_value = yaml.dump(config)

        runner = FabricDataQualityRunner("Files/dq_configs/my_table.yml")
        assert runner.config["validation_name"] == "fabric_loaded"
        mock_msutils.fs.head.assert_called_once()

    def test_config_property_returns_validator_config(self, fabric_runner):
        """runner.config should return the validator's config dict."""
        cfg = fabric_runner.config
        assert isinstance(cfg, dict)
        assert "validation_name" in cfg


class TestValidateSparkDataframe:
    """Group 2: validate_spark_dataframe."""

    @patch("dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE", False)
    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", True)
    def test_validate_spark_small_dataset(self, fabric_runner, mock_spark_df):
        """Small dataset (count < threshold) should call toPandas and return result."""
        mock_spark_df.count.return_value = 50

        expected_result = {
            "success": True,
            "suite_name": "test_validation",
            "batch_name": "test_batch",
            "success_rate": 100.0,
            "evaluated_checks": 1,
            "successful_checks": 1,
            "failed_checks": 0,
            "timestamp": datetime.now().isoformat(),
            "failed_expectations": [],
        }
        fabric_runner.validator.validate = MagicMock(return_value=expected_result)

        result = fabric_runner.validate_spark_dataframe(mock_spark_df, batch_name="test_batch")

        mock_spark_df.toPandas.assert_called_once()
        assert result["success"] is True

    @patch("dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE", False)
    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", True)
    def test_validate_spark_large_dataset_with_sampling(self, fabric_runner, mock_spark_df):
        """Large dataset (count > threshold) should trigger limit-based sampling."""
        mock_spark_df.count.return_value = 200_000

        expected_result = {
            "success": True,
            "suite_name": "test_validation",
            "batch_name": "large_batch",
            "success_rate": 100.0,
            "evaluated_checks": 1,
            "successful_checks": 1,
            "failed_checks": 0,
            "timestamp": datetime.now().isoformat(),
            "failed_expectations": [],
        }
        fabric_runner.validator.validate = MagicMock(return_value=expected_result)

        result = fabric_runner.validate_spark_dataframe(mock_spark_df, batch_name="large_batch")

        # limit() should be called for sampling (not sample())
        mock_spark_df.limit.assert_called_once()
        assert result["success"] is True

    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", False)
    def test_validate_spark_not_available(self, fabric_runner, mock_spark_df):
        """When SPARK_AVAILABLE is False, should raise ImportError."""
        with pytest.raises(ImportError, match="PySpark not available"):
            fabric_runner.validate_spark_dataframe(mock_spark_df)


class TestValidateDeltaTable:
    """Group 3: validate_delta_table."""

    @patch("dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE", False)
    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", True)
    def test_validate_delta_table_reads_table(self, fabric_runner, mock_spark_df):
        """validate_delta_table should read the table via SparkSession.table()."""
        import dq_framework.fabric_connector as fc_mod

        mock_session = MagicMock()
        mock_session.table.return_value = mock_spark_df
        mock_spark_cls = MagicMock()
        mock_spark_cls.builder.getOrCreate.return_value = mock_session
        mock_spark_df.count.return_value = 50

        expected_result = {
            "success": True,
            "suite_name": "test_validation",
            "batch_name": "my_table",
            "success_rate": 100.0,
            "evaluated_checks": 1,
            "successful_checks": 1,
            "failed_checks": 0,
            "timestamp": datetime.now().isoformat(),
            "failed_expectations": [],
        }
        fabric_runner.validator.validate = MagicMock(return_value=expected_result)

        original = getattr(fc_mod, "SparkSession", None)
        try:
            fc_mod.SparkSession = mock_spark_cls
            result = fabric_runner.validate_delta_table("my_table")
        finally:
            if original is None:
                delattr(fc_mod, "SparkSession")
            else:
                fc_mod.SparkSession = original

        mock_session.table.assert_called_once_with("my_table")
        assert result["success"] is True

    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", False)
    def test_validate_delta_table_spark_not_available(self, fabric_runner):
        """When SPARK_AVAILABLE is False, should raise ImportError."""
        with pytest.raises(ImportError, match="PySpark not available"):
            fabric_runner.validate_delta_table("some_table")


class TestValidateLakehouseFile:
    """Group 4: validate_lakehouse_file."""

    @patch("dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE", False)
    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", True)
    def test_validate_lakehouse_file_csv(self, fabric_runner, mock_spark_df):
        """CSV file should be loaded via spark.read.csv."""
        import dq_framework.fabric_connector as fc_mod

        mock_session = MagicMock()
        mock_session.read.csv.return_value = mock_spark_df
        mock_spark_cls = MagicMock()
        mock_spark_cls.builder.getOrCreate.return_value = mock_session
        mock_spark_df.count.return_value = 50

        expected_result = {
            "success": True,
            "suite_name": "test_validation",
            "batch_name": "csv_test",
            "success_rate": 100.0,
            "evaluated_checks": 1,
            "successful_checks": 1,
            "failed_checks": 0,
            "timestamp": datetime.now().isoformat(),
            "failed_expectations": [],
        }
        fabric_runner.validator.validate = MagicMock(return_value=expected_result)

        original = getattr(fc_mod, "SparkSession", None)
        try:
            fc_mod.SparkSession = mock_spark_cls
            result = fabric_runner.validate_lakehouse_file(
                "Files/data/test.csv", file_format="csv", batch_name="csv_test"
            )
        finally:
            if original is None:
                delattr(fc_mod, "SparkSession")
            else:
                fc_mod.SparkSession = original

        mock_session.read.csv.assert_called_once()
        assert result["success"] is True

    @patch("dq_framework.fabric_connector.FABRIC_UTILS_AVAILABLE", False)
    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", True)
    def test_validate_lakehouse_file_parquet(self, fabric_runner, mock_spark_df):
        """Parquet file should be loaded via spark.read.parquet."""
        import dq_framework.fabric_connector as fc_mod

        mock_session = MagicMock()
        mock_session.read.parquet.return_value = mock_spark_df
        mock_spark_cls = MagicMock()
        mock_spark_cls.builder.getOrCreate.return_value = mock_session
        mock_spark_df.count.return_value = 50

        expected_result = {
            "success": True,
            "suite_name": "test_validation",
            "batch_name": "parquet_test",
            "success_rate": 100.0,
            "evaluated_checks": 1,
            "successful_checks": 1,
            "failed_checks": 0,
            "timestamp": datetime.now().isoformat(),
            "failed_expectations": [],
        }
        fabric_runner.validator.validate = MagicMock(return_value=expected_result)

        original = getattr(fc_mod, "SparkSession", None)
        try:
            fc_mod.SparkSession = mock_spark_cls
            result = fabric_runner.validate_lakehouse_file(
                "Files/data/test.parquet", file_format="parquet", batch_name="parquet_test"
            )
        finally:
            if original is None:
                delattr(fc_mod, "SparkSession")
            else:
                fc_mod.SparkSession = original

        mock_session.read.parquet.assert_called_once()
        assert result["success"] is True

    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", True)
    def test_validate_lakehouse_file_unsupported(self, fabric_runner):
        """Unsupported file format should raise ValueError."""
        import dq_framework.fabric_connector as fc_mod

        mock_session = MagicMock()
        mock_spark_cls = MagicMock()
        mock_spark_cls.builder.getOrCreate.return_value = mock_session

        original = getattr(fc_mod, "SparkSession", None)
        try:
            fc_mod.SparkSession = mock_spark_cls
            with pytest.raises(ValueError, match="Unsupported file format"):
                fabric_runner.validate_lakehouse_file("Files/data/test.xlsx", file_format="xlsx")
        finally:
            if original is None:
                delattr(fc_mod, "SparkSession")
            else:
                fc_mod.SparkSession = original


class TestHandleFailure:
    """Group 5: handle_failure."""

    def test_handle_failure_success_noop(self, fabric_runner, sample_validation_result):
        """Successful results with action='log' should not raise."""
        # sample_validation_result has success=True
        fabric_runner.handle_failure(sample_validation_result, action="log")
        # No exception means pass

    def test_handle_failure_halt_raises(self, fabric_runner, sample_validation_result):
        """Failed results with action='halt' should raise ValueError."""
        failed_result = dict(sample_validation_result)
        failed_result["success"] = False
        failed_result["failed_checks"] = 2
        failed_result["success_rate"] = 60.0

        with pytest.raises(ValueError, match="Data quality validation failed"):
            fabric_runner.handle_failure(failed_result, action="halt")

    def test_handle_failure_alert_calls_send_alert(self, fabric_runner, sample_validation_result):
        """Failed results with action='alert' should call _send_alert."""
        failed_result = dict(sample_validation_result)
        failed_result["success"] = False
        failed_result["failed_checks"] = 1
        failed_result["success_rate"] = 80.0

        with patch.object(fabric_runner, "_send_alert") as mock_alert:
            fabric_runner.handle_failure(failed_result, action="alert")
            mock_alert.assert_called_once_with(failed_result)


class TestResultStoreIntegration:
    """Group 6: ResultStore integration in FabricDataQualityRunner."""

    def test_runner_has_store_attribute(self, fabric_runner):
        """FabricDataQualityRunner.__init__ should create a _store attribute."""
        assert hasattr(fabric_runner, "_store"), (
            "FabricDataQualityRunner must have a _store attribute"
        )

    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", True)
    def test_validate_spark_calls_store_write(self, fabric_runner, mock_spark_df):
        """validate_spark_dataframe should call self._store.write()."""
        mock_spark_df.count.return_value = 50

        expected_result = {
            "success": True,
            "suite_name": "test_validation",
            "batch_name": "store_test",
            "success_rate": 100.0,
            "evaluated_checks": 1,
            "successful_checks": 1,
            "failed_checks": 0,
            "timestamp": datetime.now().isoformat(),
            "failed_expectations": [],
        }
        fabric_runner.validator.validate = MagicMock(return_value=expected_result)
        fabric_runner._store = MagicMock()

        fabric_runner.validate_spark_dataframe(mock_spark_df, batch_name="store_test")

        fabric_runner._store.write.assert_called_once()
        call_args = fabric_runner._store.write.call_args
        assert call_args[0][1] == expected_result

    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", True)
    def test_validate_spark_chunked_calls_store_write(self, fabric_runner):
        """_validate_spark_chunked should call self._store.write()."""
        import sys

        # Build a mock Spark DF that supports the withColumn/filter/drop/toPandas chain
        mock_spark_df = MagicMock(name="SparkDataFrame")
        mock_spark_df.columns = ["id", "name", "age"]
        df_with_id = MagicMock(name="df_with_id")
        mock_spark_df.withColumn.return_value = df_with_id

        col_mock = MagicMock()
        col_mock.__ge__ = MagicMock(return_value=MagicMock())
        col_mock.__le__ = MagicMock(return_value=MagicMock())
        df_with_id.__getitem__ = MagicMock(return_value=col_mock)

        filtered_df = MagicMock()
        df_with_id.filter.return_value = filtered_df
        dropped_df = MagicMock()
        filtered_df.drop.return_value = dropped_df
        dropped_df.toPandas.return_value = pd.DataFrame({"a": [1]})

        chunk_result = {
            "success": True,
            "evaluated_checks": 1,
            "successful_checks": 1,
            "failed_checks": 0,
            "success_rate": 100.0,
            "suite_name": "test_validation",
            "batch_name": "chunked_store__chunk_1",
            "timestamp": datetime.now().isoformat(),
            "failed_expectations": [],
            "threshold": DEFAULT_VALIDATION_THRESHOLD,
        }
        fabric_runner.validator.validate = MagicMock(return_value=chunk_result)
        fabric_runner._store = MagicMock()

        pyspark_mocks = {
            "pyspark.sql.functions": MagicMock(),
            "pyspark.sql.window": MagicMock(),
        }

        with patch.dict(sys.modules, pyspark_mocks):
            fabric_runner._validate_spark_chunked(mock_spark_df, "chunked_store", 5, 5)

        fabric_runner._store.write.assert_called_once()

    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", True)
    def test_store_write_failure_does_not_crash(self, fabric_runner, mock_spark_df):
        """Storage failure in write() should be caught and logged, not crash validation."""
        mock_spark_df.count.return_value = 50

        expected_result = {
            "success": True,
            "suite_name": "test_validation",
            "batch_name": "fail_store",
            "success_rate": 100.0,
            "evaluated_checks": 1,
            "successful_checks": 1,
            "failed_checks": 0,
            "timestamp": datetime.now().isoformat(),
            "failed_expectations": [],
        }
        fabric_runner.validator.validate = MagicMock(return_value=expected_result)
        fabric_runner._store = MagicMock()
        fabric_runner._store.write.side_effect = OSError("Disk full")

        # Should not raise
        result = fabric_runner.validate_spark_dataframe(mock_spark_df, batch_name="fail_store")
        assert result["success"] is True

    def test_save_results_to_lakehouse_removed(self, fabric_runner):
        """_save_results_to_lakehouse method should no longer exist."""
        assert not hasattr(fabric_runner, "_save_results_to_lakehouse"), (
            "_save_results_to_lakehouse should be removed from FabricDataQualityRunner"
        )


class TestQuickValidate:
    """Group 7: quick_validate (module-level function)."""

    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", False)
    def test_quick_validate_pandas_df(self, tmp_path):
        """quick_validate with a pandas DataFrame should use DataQualityValidator."""
        config = {
            "validation_name": "quick_test",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1},
                }
            ],
        }
        config_file = tmp_path / "quick_config.yml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        with patch("dq_framework.fabric_connector.DataQualityValidator") as MockValidator:
            mock_instance = MockValidator.return_value
            mock_instance.validate.return_value = {
                "success": True,
                "failed_checks": 0,
            }
            result = quick_validate(df, str(config_file))
            assert result is True
            mock_instance.validate.assert_called_once()

    @patch("dq_framework.fabric_connector.SPARK_AVAILABLE", False)
    def test_quick_validate_halt_on_failure(self, tmp_path):
        """quick_validate with halt_on_failure=True and failed result should raise."""
        config = {
            "validation_name": "quick_halt",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1},
                }
            ],
        }
        config_file = tmp_path / "quick_halt_config.yml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        df = pd.DataFrame({"id": [1]})

        with patch("dq_framework.fabric_connector.DataQualityValidator") as MockValidator:
            mock_instance = MockValidator.return_value
            mock_instance.validate.return_value = {
                "success": False,
                "failed_checks": 3,
            }
            with pytest.raises(ValueError, match="Data quality validation failed"):
                quick_validate(df, str(config_file), halt_on_failure=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
