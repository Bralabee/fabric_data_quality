"""
Validation History
==================

Structured storage for validation results with dual-backend support:
- **Local:** SQLite database (stdlib sqlite3, zero new dependencies)
- **Fabric:** Parquet files (pyarrow/pandas, already project dependencies)

Replaces scattered JSON file storage with queryable structured storage,
enabling trend analysis and failure tracking.

Usage::

    from dq_framework.validation_history import ValidationHistory

    history = ValidationHistory(dataset_name="orders")
    history.record(result=validation_summary, duration_seconds=elapsed)
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Runtime detection (lazy import, safe in test environments)
# ---------------------------------------------------------------------------

try:
    from .utils import _is_fabric_runtime
except ImportError:  # pragma: no cover

    def _is_fabric_runtime() -> bool:  # type: ignore[misc]
        return False


# ---------------------------------------------------------------------------
# SQL constants
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS validation_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    dataset TEXT NOT NULL,
    suite_name TEXT NOT NULL,
    success INTEGER NOT NULL,
    success_rate REAL NOT NULL,
    evaluated_checks INTEGER NOT NULL,
    failed_checks INTEGER NOT NULL,
    severity_stats TEXT,
    duration_seconds REAL,
    failed_expectations TEXT
)
"""

CREATE_INDEX_SQL = """\
CREATE INDEX IF NOT EXISTS idx_history_dataset_ts
ON validation_history (dataset, timestamp)
"""

INSERT_SQL = """\
INSERT INTO validation_history
    (timestamp, dataset, suite_name, success, success_rate,
     evaluated_checks, failed_checks, severity_stats,
     duration_seconds, failed_expectations)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Parquet columns -- mirrors SQLite schema (without auto-increment id)
PARQUET_COLUMNS = [
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
]


# ---------------------------------------------------------------------------
# ValidationHistory
# ---------------------------------------------------------------------------


class ValidationHistory:
    """Structured storage for validation results with dual-backend support.

    Parameters
    ----------
    dataset_name:
        Logical name for the dataset being validated.
    backend:
        ``"local"`` for SQLite, ``"fabric"`` for Parquet, or ``None``
        for auto-detection via ``_is_fabric_runtime()``.
    db_path:
        Path to the SQLite database file (local backend only).
    parquet_dir:
        Directory for Parquet history files (Fabric backend only).
    retention_days:
        Number of days to retain history. Used by ``apply_retention()``.
    """

    def __init__(
        self,
        dataset_name: str = "default",
        backend: str | None = None,
        db_path: str = "dq_results/validation_history.db",
        parquet_dir: str = "Files/dq_results/history",
        retention_days: int = 90,
    ) -> None:
        self._dataset_name = dataset_name
        self._retention_days = retention_days

        # Determine backend
        if backend is None:
            self._is_fabric = _is_fabric_runtime()
        elif backend == "local":
            self._is_fabric = False
        elif backend == "fabric":
            self._is_fabric = True
        else:
            raise ValueError(f"Unknown backend: {backend!r}. Use 'local', 'fabric', or None.")

        # Initialise chosen backend
        if self._is_fabric:
            self._init_parquet(parquet_dir)
        else:
            self._init_sqlite(db_path)

    # ------------------------------------------------------------------
    # SQLite backend
    # ------------------------------------------------------------------

    def _init_sqlite(self, db_path: str) -> None:
        """Create SQLite database, table, and index."""
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute(CREATE_TABLE_SQL)
        self._conn.execute(CREATE_INDEX_SQL)
        self._conn.commit()
        logger.debug("SQLite history initialised at %s", db_path)

    def _record_sqlite(self, result: dict[str, Any], duration_seconds: float | None) -> None:
        """Insert a single validation result into the SQLite table."""
        self._conn.execute(
            INSERT_SQL,
            (
                result.get("timestamp", datetime.now().isoformat()),
                self._dataset_name,
                result.get("suite_name", "unknown"),
                int(bool(result.get("success", False))),
                float(result.get("success_rate", 0.0)),
                int(result.get("evaluated_checks", 0)),
                int(result.get("failed_checks", 0)),
                json.dumps(result.get("severity_stats")) if result.get("severity_stats") is not None else None,
                duration_seconds,
                json.dumps(result.get("failed_expectations")) if result.get("failed_expectations") is not None else None,
            ),
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Parquet backend
    # ------------------------------------------------------------------

    def _init_parquet(self, parquet_dir: str) -> None:
        """Prepare Parquet storage directory."""
        self._parquet_dir = Path(parquet_dir)
        self._parquet_dir.mkdir(parents=True, exist_ok=True)
        self._parquet_path = self._parquet_dir / "validation_history.parquet"
        logger.debug("Parquet history initialised at %s", self._parquet_path)

    def _record_parquet(self, result: dict[str, Any], duration_seconds: float | None) -> None:
        """Append a single validation result to the Parquet file."""
        row = {
            "timestamp": result.get("timestamp", datetime.now().isoformat()),
            "dataset": self._dataset_name,
            "suite_name": result.get("suite_name", "unknown"),
            "success": bool(result.get("success", False)),
            "success_rate": float(result.get("success_rate", 0.0)),
            "evaluated_checks": int(result.get("evaluated_checks", 0)),
            "failed_checks": int(result.get("failed_checks", 0)),
            "severity_stats": json.dumps(result.get("severity_stats")) if result.get("severity_stats") is not None else None,
            "duration_seconds": duration_seconds,
            "failed_expectations": json.dumps(result.get("failed_expectations")) if result.get("failed_expectations") is not None else None,
        }
        new_df = pd.DataFrame([row], columns=PARQUET_COLUMNS)

        if self._parquet_path.exists():
            existing = pd.read_parquet(self._parquet_path)
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df

        combined.to_parquet(self._parquet_path, index=False)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record(
        self,
        result: dict[str, Any],
        duration_seconds: float | None = None,
    ) -> None:
        """Persist a validation result.

        Parameters
        ----------
        result:
            Dict matching the output of ``DataQualityValidator.validate()``.
        duration_seconds:
            Optional elapsed time for the validation run.
        """
        if self._is_fabric:
            self._record_parquet(result, duration_seconds)
        else:
            self._record_sqlite(result, duration_seconds)
        logger.info(
            "Recorded validation result for dataset=%s suite=%s success=%s",
            self._dataset_name,
            result.get("suite_name", "unknown"),
            result.get("success"),
        )
