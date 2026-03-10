"""Tests for ValidationHistory class with dual-backend storage."""

from __future__ import annotations

import json
import sqlite3
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
