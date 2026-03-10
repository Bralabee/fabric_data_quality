"""Tests for ValidationHistory class with dual-backend storage."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_result() -> dict:
    """Return a dict matching validator._format_results() output shape."""
    return {
        "success": True,
        "suite_name": "orders_suite",
        "batch_name": "batch_001",
        "timestamp": "2026-03-10T12:00:00",
        "evaluated_checks": 10,
        "successful_checks": 9,
        "failed_checks": 1,
        "success_rate": 90.0,
        "severity_stats": {
            "critical": {"total": 5, "passed": 5},
            "high": {"total": 3, "passed": 2},
            "medium": {"total": 2, "passed": 2},
        },
        "failed_expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "column": "order_id",
                "severity": "high",
            }
        ],
    }


@pytest.fixture()
def sample_failure_result(sample_result: dict) -> dict:
    """Return a failed validation result."""
    result = sample_result.copy()
    result["success"] = False
    result["successful_checks"] = 5
    result["failed_checks"] = 5
    result["success_rate"] = 50.0
    return result


# ===========================================================================
# TestSQLiteBackend
# ===========================================================================

class TestSQLiteBackend:
    """Tests for SQLite backend initialization and basic record operations."""

    def test_creates_table_on_init(self, tmp_path: Path) -> None:
        """ValidationHistory creates SQLite DB with table and index on init."""
        from dq_framework.validation_history import ValidationHistory

        db_path = str(tmp_path / "test.db")
        vh = ValidationHistory(dataset_name="orders", backend="local", db_path=db_path)

        # Verify table exists
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='validation_history'"
        )
        assert cursor.fetchone() is not None, "validation_history table should exist"

        # Verify index exists
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_history_dataset_ts'"
        )
        assert cursor.fetchone() is not None, "idx_history_dataset_ts index should exist"
        conn.close()

    def test_record_inserts_row(
        self, tmp_path: Path, sample_result: dict
    ) -> None:
        """record() inserts a row; SELECT returns 1 row."""
        from dq_framework.validation_history import ValidationHistory

        db_path = str(tmp_path / "test.db")
        vh = ValidationHistory(dataset_name="orders", backend="local", db_path=db_path)
        vh.record(sample_result)

        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM validation_history")
        assert cursor.fetchone()[0] == 1
        conn.close()

    def test_record_multiple(
        self, tmp_path: Path, sample_result: dict
    ) -> None:
        """Recording 3 results yields 3 rows in the table."""
        from dq_framework.validation_history import ValidationHistory

        db_path = str(tmp_path / "test.db")
        vh = ValidationHistory(dataset_name="orders", backend="local", db_path=db_path)

        for _ in range(3):
            vh.record(sample_result)

        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM validation_history")
        assert cursor.fetchone()[0] == 3
        conn.close()


# ===========================================================================
# TestRecordSchema
# ===========================================================================

class TestRecordSchema:
    """Tests for the record schema: field presence, types, and round-trips."""

    def _get_row(self, tmp_path: Path, result: dict, **kwargs) -> sqlite3.Row:
        """Helper: record a result and return the first row."""
        from dq_framework.validation_history import ValidationHistory

        db_path = str(tmp_path / "test.db")
        vh = ValidationHistory(dataset_name="orders", backend="local", db_path=db_path)
        vh.record(result, **kwargs)

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM validation_history").fetchone()
        conn.close()
        return row

    def test_all_fields_present(
        self, tmp_path: Path, sample_result: dict
    ) -> None:
        """Recorded row has all required columns."""
        row = self._get_row(tmp_path, sample_result, duration_seconds=1.5)
        expected_columns = {
            "id",
            "timestamp",
            "dataset",
            "suite_name",
            "success",
            "success_rate",
            "evaluated_checks",
            "failed_checks",
            "severity_stats",
            "duration_seconds",
            "failed_expectations",
        }
        assert set(row.keys()) == expected_columns

    def test_severity_stats_roundtrip(
        self, tmp_path: Path, sample_result: dict
    ) -> None:
        """severity_stats dict survives json round-trip through SQLite TEXT."""
        row = self._get_row(tmp_path, sample_result)
        recovered = json.loads(row["severity_stats"])
        assert recovered == sample_result["severity_stats"]

    def test_failed_expectations_roundtrip(
        self, tmp_path: Path, sample_result: dict
    ) -> None:
        """failed_expectations list survives json round-trip through SQLite TEXT."""
        row = self._get_row(tmp_path, sample_result)
        recovered = json.loads(row["failed_expectations"])
        assert recovered == sample_result["failed_expectations"]

    def test_success_stored_as_int(
        self, tmp_path: Path, sample_result: dict, sample_failure_result: dict
    ) -> None:
        """success=True stored as 1, success=False stored as 0."""
        row_true = self._get_row(tmp_path, sample_result)
        assert row_true["success"] == 1

        # Use a different tmp_path sub-dir for the second DB
        sub = tmp_path / "fail"
        sub.mkdir()
        row_false = self._get_row(sub, sample_failure_result)
        assert row_false["success"] == 0

    def test_optional_duration(
        self, tmp_path: Path, sample_result: dict
    ) -> None:
        """record() without duration_seconds stores None/NULL."""
        row = self._get_row(tmp_path, sample_result)
        assert row["duration_seconds"] is None

    def test_dataset_from_constructor(
        self, tmp_path: Path, sample_result: dict
    ) -> None:
        """dataset column populated from constructor dataset_name."""
        row = self._get_row(tmp_path, sample_result)
        assert row["dataset"] == "orders"


# ===========================================================================
# TestParquetBackend
# ===========================================================================

class TestParquetBackend:
    """Tests for the Parquet backend (Fabric environment)."""

    def test_creates_parquet_on_first_record(
        self, tmp_path: Path, sample_result: dict
    ) -> None:
        """record() creates parquet file when none exists."""
        from dq_framework.validation_history import ValidationHistory

        pq_dir = str(tmp_path / "history")
        vh = ValidationHistory(
            dataset_name="orders", backend="fabric", parquet_dir=pq_dir
        )
        vh.record(sample_result)

        pq_file = tmp_path / "history" / "validation_history.parquet"
        assert pq_file.exists(), "Parquet file should be created on first record"

    def test_record_appends(
        self, tmp_path: Path, sample_result: dict
    ) -> None:
        """Second record() appends row (read_parquet returns 2 rows)."""
        from dq_framework.validation_history import ValidationHistory

        pq_dir = str(tmp_path / "history")
        vh = ValidationHistory(
            dataset_name="orders", backend="fabric", parquet_dir=pq_dir
        )
        vh.record(sample_result)
        vh.record(sample_result)

        df = pd.read_parquet(tmp_path / "history" / "validation_history.parquet")
        assert len(df) == 2

    def test_schema_matches_sqlite(
        self, tmp_path: Path, sample_result: dict
    ) -> None:
        """Parquet columns match SQLite columns."""
        from dq_framework.validation_history import ValidationHistory

        pq_dir = str(tmp_path / "history")
        vh = ValidationHistory(
            dataset_name="orders", backend="fabric", parquet_dir=pq_dir
        )
        vh.record(sample_result)

        df = pd.read_parquet(tmp_path / "history" / "validation_history.parquet")
        expected_cols = {
            "timestamp",
            "dataset",
            "suite_name",
            "success",
            "success_rate",
            "evaluated_checks",
            "failed_checks",
            "severity_stats",
            "duration_seconds",
            "failed_expectations",
        }
        assert set(df.columns) == expected_cols

    def test_severity_stats_as_string(
        self, tmp_path: Path, sample_result: dict
    ) -> None:
        """severity_stats stored as string column in Parquet (JSON text)."""
        from dq_framework.validation_history import ValidationHistory

        pq_dir = str(tmp_path / "history")
        vh = ValidationHistory(
            dataset_name="orders", backend="fabric", parquet_dir=pq_dir
        )
        vh.record(sample_result)

        df = pd.read_parquet(tmp_path / "history" / "validation_history.parquet")
        val = df["severity_stats"].iloc[0]
        assert isinstance(val, str)
        recovered = json.loads(val)
        assert recovered == sample_result["severity_stats"]

    @patch("dq_framework.validation_history._is_fabric_runtime", return_value=False)
    def test_backend_auto_detect_local(
        self, mock_runtime: Any, tmp_path: Path
    ) -> None:
        """backend=None with _is_fabric_runtime()=False uses SQLite."""
        from dq_framework.validation_history import ValidationHistory

        db_path = str(tmp_path / "test.db")
        vh = ValidationHistory(dataset_name="orders", db_path=db_path)
        assert vh._is_fabric is False

    @patch("dq_framework.validation_history._is_fabric_runtime", return_value=True)
    def test_backend_auto_detect_fabric(
        self, mock_runtime: Any, tmp_path: Path
    ) -> None:
        """backend=None with _is_fabric_runtime()=True uses Parquet."""
        from dq_framework.validation_history import ValidationHistory

        pq_dir = str(tmp_path / "history")
        vh = ValidationHistory(dataset_name="orders", parquet_dir=pq_dir)
        assert vh._is_fabric is True


# ===========================================================================
# Query test helpers
# ===========================================================================

def _make_result(
    *,
    timestamp: str,
    success: bool = True,
    success_rate: float = 90.0,
    failed_checks: int = 1,
    dataset: str = "orders",
    failed_expectations: list | None = None,
    duration_seconds: float | None = 1.5,
) -> dict:
    """Build a result dict with specified timestamp and optional overrides."""
    return {
        "success": success,
        "suite_name": "orders_suite",
        "batch_name": "batch_001",
        "timestamp": timestamp,
        "evaluated_checks": 10,
        "successful_checks": 10 - failed_checks,
        "failed_checks": failed_checks,
        "success_rate": success_rate,
        "severity_stats": {"critical": {"total": 5, "passed": 5}},
        "failed_expectations": failed_expectations,
    }


@pytest.fixture()
def history_with_data(tmp_path: Path):
    """Create a ValidationHistory with sample records spanning 40 days."""
    from dq_framework.validation_history import ValidationHistory

    db_path = str(tmp_path / "trend.db")
    vh = ValidationHistory(dataset_name="orders", backend="local", db_path=db_path)

    now = datetime.now()

    # Recent records (within 7 days)
    for i in range(3):
        ts = (now - timedelta(days=i + 1)).isoformat()
        vh.record(
            _make_result(timestamp=ts, success_rate=90.0 + i, failed_checks=1),
            duration_seconds=1.0 + i,
        )

    # Older records (10-15 days ago)
    for i in range(2):
        ts = (now - timedelta(days=10 + i)).isoformat()
        vh.record(
            _make_result(timestamp=ts, success_rate=80.0, failed_checks=2),
            duration_seconds=2.0,
        )

    # Very old records (35-40 days ago)
    for i in range(2):
        ts = (now - timedelta(days=35 + i)).isoformat()
        vh.record(
            _make_result(timestamp=ts, success_rate=70.0, failed_checks=3),
            duration_seconds=3.0,
        )

    # Record for a different dataset
    ts = (now - timedelta(days=2)).isoformat()
    vh.record(
        _make_result(timestamp=ts, success_rate=95.0, failed_checks=0),
        duration_seconds=0.5,
    )
    # Manually update the dataset for this record
    vh._conn.execute(
        "UPDATE validation_history SET dataset='shipments' WHERE rowid=?",
        (vh._conn.execute("SELECT MAX(id) FROM validation_history").fetchone()[0],),
    )
    vh._conn.commit()

    return vh


@pytest.fixture()
def history_with_failures(tmp_path: Path):
    """Create a ValidationHistory with failure records for aggregation tests."""
    from dq_framework.validation_history import ValidationHistory

    db_path = str(tmp_path / "failures.db")
    vh = ValidationHistory(dataset_name="orders", backend="local", db_path=db_path)

    now = datetime.now()

    # 3 records with the same failure type
    for i in range(3):
        ts = (now - timedelta(days=i + 1)).isoformat()
        vh.record(
            _make_result(
                timestamp=ts,
                success=False,
                success_rate=50.0,
                failed_checks=2,
                failed_expectations=[
                    {
                        "expectation_type": "expect_column_values_to_not_be_null",
                        "column": "order_id",
                        "severity": "high",
                    },
                    {
                        "expectation_type": "expect_column_values_to_be_between",
                        "column": "amount",
                        "severity": "medium",
                    },
                ],
            ),
        )

    # 1 record with a different failure
    ts = (now - timedelta(days=5)).isoformat()
    vh.record(
        _make_result(
            timestamp=ts,
            success=False,
            success_rate=80.0,
            failed_checks=1,
            failed_expectations=[
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "column": "order_id",
                    "severity": "high",
                },
            ],
        ),
    )

    # 1 success record (no failures)
    ts = (now - timedelta(days=6)).isoformat()
    vh.record(
        _make_result(timestamp=ts, success=True, success_rate=100.0, failed_checks=0),
    )

    return vh


# ===========================================================================
# TestGetTrend
# ===========================================================================

class TestGetTrend:
    """Tests for the get_trend() query method."""

    def test_returns_dataframe(self, history_with_data) -> None:
        """get_trend() returns a pandas DataFrame."""
        result = history_with_data.get_trend()
        assert isinstance(result, pd.DataFrame)

    def test_columns(self, history_with_data) -> None:
        """DataFrame has the expected columns."""
        result = history_with_data.get_trend()
        expected = {"timestamp", "success", "success_rate", "failed_checks", "duration_seconds"}
        assert set(result.columns) == expected

    def test_filters_by_days(self, history_with_data) -> None:
        """get_trend(days=7) only returns records from last 7 days."""
        result = history_with_data.get_trend(days=7)
        # Should have 3 recent orders records (not the 10+ day old ones)
        assert len(result) == 3

    def test_filters_by_dataset(self, history_with_data) -> None:
        """get_trend(dataset='shipments') only returns shipments records."""
        result = history_with_data.get_trend(dataset="shipments")
        assert len(result) == 1

    def test_default_dataset(self, history_with_data) -> None:
        """get_trend() without dataset arg uses constructor dataset_name ('orders')."""
        result = history_with_data.get_trend(days=365)
        # All 7 orders records (3 recent + 2 older + 2 very old), not the shipments one
        assert len(result) == 7

    def test_empty_result(self, history_with_data) -> None:
        """Returns empty DataFrame (not error) when no matching records."""
        result = history_with_data.get_trend(dataset="nonexistent")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_ordered_by_timestamp(self, history_with_data) -> None:
        """Results sorted ascending by timestamp."""
        result = history_with_data.get_trend(days=365)
        timestamps = result["timestamp"].tolist()
        assert timestamps == sorted(timestamps)


# ===========================================================================
# TestGetFailureHistory
# ===========================================================================

class TestGetFailureHistory:
    """Tests for the get_failure_history() query method."""

    def test_returns_dataframe(self, history_with_failures) -> None:
        """get_failure_history() returns a DataFrame."""
        result = history_with_failures.get_failure_history()
        assert isinstance(result, pd.DataFrame)

    def test_aggregates_failures(self, history_with_failures) -> None:
        """Multiple records with same failure type produce aggregated row with frequency count."""
        result = history_with_failures.get_failure_history()
        # order_id null check appears in 4 records (3 + 1)
        null_rows = result[
            (result["expectation_type"] == "expect_column_values_to_not_be_null")
            & (result["column"] == "order_id")
        ]
        assert len(null_rows) == 1
        assert null_rows.iloc[0]["frequency"] == 4

        # amount between check appears in 3 records
        between_rows = result[
            (result["expectation_type"] == "expect_column_values_to_be_between")
            & (result["column"] == "amount")
        ]
        assert len(between_rows) == 1
        assert between_rows.iloc[0]["frequency"] == 3

    def test_includes_most_recent(self, history_with_failures) -> None:
        """Result includes most_recent_at column with latest occurrence timestamp."""
        result = history_with_failures.get_failure_history()
        assert "most_recent_at" in result.columns
        # All entries should have a non-null most_recent_at
        assert result["most_recent_at"].notna().all()

    def test_empty_when_no_failures(self, tmp_path: Path) -> None:
        """Returns empty DataFrame when all records have no failed expectations."""
        from dq_framework.validation_history import ValidationHistory

        db_path = str(tmp_path / "nofails.db")
        vh = ValidationHistory(dataset_name="orders", backend="local", db_path=db_path)

        now = datetime.now()
        vh.record(
            _make_result(
                timestamp=now.isoformat(),
                success=True,
                success_rate=100.0,
                failed_checks=0,
            ),
        )
        result = vh.get_failure_history()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


# ===========================================================================
# TestComparePeriods
# ===========================================================================

class TestComparePeriods:
    """Tests for the compare_periods() query method."""

    def test_returns_dataframe(self, history_with_data) -> None:
        """compare_periods() returns a DataFrame."""
        now = datetime.now()
        period_a = (
            (now - timedelta(days=40)).strftime("%Y-%m-%d"),
            (now - timedelta(days=30)).strftime("%Y-%m-%d"),
        )
        period_b = (
            (now - timedelta(days=7)).strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
        )
        result = history_with_data.compare_periods(period_a=period_a, period_b=period_b)
        assert isinstance(result, pd.DataFrame)

    def test_columns(self, history_with_data) -> None:
        """DataFrame has metric, period_a_value, period_b_value, change, change_pct columns."""
        now = datetime.now()
        period_a = (
            (now - timedelta(days=40)).strftime("%Y-%m-%d"),
            (now - timedelta(days=30)).strftime("%Y-%m-%d"),
        )
        period_b = (
            (now - timedelta(days=7)).strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
        )
        result = history_with_data.compare_periods(period_a=period_a, period_b=period_b)
        expected = {"metric", "period_a_value", "period_b_value", "change", "change_pct"}
        assert set(result.columns) == expected

    def test_metrics_included(self, history_with_data) -> None:
        """Metrics include mean_success_rate, total_runs, total_failures."""
        now = datetime.now()
        period_a = (
            (now - timedelta(days=40)).strftime("%Y-%m-%d"),
            (now - timedelta(days=30)).strftime("%Y-%m-%d"),
        )
        period_b = (
            (now - timedelta(days=7)).strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
        )
        result = history_with_data.compare_periods(period_a=period_a, period_b=period_b)
        metrics = set(result["metric"].tolist())
        assert "mean_success_rate" in metrics
        assert "total_runs" in metrics
        assert "total_failures" in metrics

    def test_change_calculation(self, history_with_data) -> None:
        """change = period_b_value - period_a_value."""
        now = datetime.now()
        period_a = (
            (now - timedelta(days=40)).strftime("%Y-%m-%d"),
            (now - timedelta(days=30)).strftime("%Y-%m-%d"),
        )
        period_b = (
            (now - timedelta(days=7)).strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
        )
        result = history_with_data.compare_periods(period_a=period_a, period_b=period_b)
        for _, row in result.iterrows():
            expected_change = row["period_b_value"] - row["period_a_value"]
            assert row["change"] == pytest.approx(expected_change)
