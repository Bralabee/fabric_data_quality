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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Runtime detection (lazy import, safe in test environments)
# ---------------------------------------------------------------------------

try:
    from .constants import DEFAULT_HISTORY_DB, DEFAULT_HISTORY_PARQUET_DIR, DEFAULT_RETENTION_DAYS
except ImportError:  # pragma: no cover
    DEFAULT_RETENTION_DAYS = 90
    DEFAULT_HISTORY_DB = "dq_results/validation_history.db"
    DEFAULT_HISTORY_PARQUET_DIR = "Files/dq_results/history"

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
        db_path: str = DEFAULT_HISTORY_DB,
        parquet_dir: str = DEFAULT_HISTORY_PARQUET_DIR,
        retention_days: int = DEFAULT_RETENTION_DAYS,
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
                json.dumps(result.get("severity_stats"))
                if result.get("severity_stats") is not None
                else None,
                duration_seconds,
                json.dumps(result.get("failed_expectations"))
                if result.get("failed_expectations") is not None
                else None,
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
            "severity_stats": json.dumps(result.get("severity_stats"))
            if result.get("severity_stats") is not None
            else None,
            "duration_seconds": duration_seconds,
            "failed_expectations": json.dumps(result.get("failed_expectations"))
            if result.get("failed_expectations") is not None
            else None,
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

    # ------------------------------------------------------------------
    # Query APIs
    # ------------------------------------------------------------------

    _TREND_COLUMNS: ClassVar[list[str]] = [
        "timestamp",
        "success",
        "success_rate",
        "failed_checks",
        "duration_seconds",
    ]

    def get_trend(
        self,
        dataset: str | None = None,
        days: int = 30,
    ) -> pd.DataFrame:
        """Return quality metrics filtered by date range.

        Parameters
        ----------
        dataset:
            Filter by dataset name. Defaults to ``self._dataset_name``.
        days:
            Number of days to look back from now.

        Returns
        -------
        pd.DataFrame
            Columns: timestamp, success, success_rate, failed_checks, duration_seconds.
            Sorted ascending by timestamp.
        """
        ds = dataset or self._dataset_name
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()

        if self._is_fabric:
            return self._get_trend_parquet(ds, cutoff)
        return self._get_trend_sqlite(ds, cutoff)

    def _get_trend_sqlite(self, dataset: str, cutoff: str) -> pd.DataFrame:
        sql = (
            "SELECT timestamp, success, success_rate, failed_checks, duration_seconds "
            "FROM validation_history "
            "WHERE dataset = ? AND timestamp >= ? "
            "ORDER BY timestamp ASC"
        )
        df = pd.read_sql(sql, self._conn, params=(dataset, cutoff))
        if df.empty:
            return pd.DataFrame(columns=self._TREND_COLUMNS)
        return df

    def _get_trend_parquet(self, dataset: str, cutoff: str) -> pd.DataFrame:
        if not self._parquet_path.exists():
            return pd.DataFrame(columns=self._TREND_COLUMNS)
        df = pd.read_parquet(self._parquet_path)
        mask = (df["dataset"] == dataset) & (df["timestamp"] >= cutoff)
        result = df.loc[mask, self._TREND_COLUMNS].sort_values("timestamp").reset_index(drop=True)
        return result

    def get_failure_history(
        self,
        dataset: str | None = None,
    ) -> pd.DataFrame:
        """Return aggregated failure data with frequency and recency.

        Parameters
        ----------
        dataset:
            Filter by dataset name. Defaults to ``self._dataset_name``.

        Returns
        -------
        pd.DataFrame
            Columns: expectation_type, column, frequency, most_recent_at, severity.
        """
        ds = dataset or self._dataset_name

        if self._is_fabric:
            return self._get_failure_history_parquet(ds)
        return self._get_failure_history_sqlite(ds)

    _FAILURE_COLUMNS: ClassVar[list[str]] = [
        "expectation_type",
        "column",
        "frequency",
        "most_recent_at",
        "severity",
    ]

    def _get_failure_history_sqlite(self, dataset: str) -> pd.DataFrame:
        sql = (
            "SELECT timestamp, failed_expectations "
            "FROM validation_history "
            "WHERE dataset = ? AND failed_expectations IS NOT NULL"
        )
        cursor = self._conn.execute(sql, (dataset,))
        rows = cursor.fetchall()
        return self._aggregate_failures(rows)

    def _get_failure_history_parquet(self, dataset: str) -> pd.DataFrame:
        if not self._parquet_path.exists():
            return pd.DataFrame(columns=self._FAILURE_COLUMNS)
        df = pd.read_parquet(self._parquet_path)
        mask = (df["dataset"] == dataset) & (df["failed_expectations"].notna())
        subset = df.loc[mask, ["timestamp", "failed_expectations"]]
        rows = list(subset.itertuples(index=False, name=None))
        return self._aggregate_failures(rows)

    def _aggregate_failures(self, rows: list) -> pd.DataFrame:
        """Aggregate failure rows into (expectation_type, column, frequency, most_recent_at, severity)."""
        aggregation: dict[tuple[str, str], dict[str, Any]] = {}

        for timestamp, failed_json in rows:
            expectations = json.loads(failed_json) if isinstance(failed_json, str) else failed_json
            if not expectations:
                continue
            for exp in expectations:
                key = (exp.get("expectation_type", ""), exp.get("column", ""))
                if key not in aggregation:
                    aggregation[key] = {
                        "expectation_type": key[0],
                        "column": key[1],
                        "frequency": 0,
                        "most_recent_at": timestamp,
                        "severity": exp.get("severity", "unknown"),
                    }
                aggregation[key]["frequency"] += 1
                if timestamp > aggregation[key]["most_recent_at"]:
                    aggregation[key]["most_recent_at"] = timestamp

        if not aggregation:
            return pd.DataFrame(columns=self._FAILURE_COLUMNS)
        return pd.DataFrame(list(aggregation.values()), columns=self._FAILURE_COLUMNS)

    def compare_periods(
        self,
        dataset: str | None = None,
        period_a: tuple[str, str] = ("", ""),
        period_b: tuple[str, str] = ("", ""),
    ) -> pd.DataFrame:
        """Compare quality metrics between two time ranges.

        Parameters
        ----------
        dataset:
            Filter by dataset name. Defaults to ``self._dataset_name``.
        period_a:
            ``(start_date, end_date)`` in ISO format for the first period.
        period_b:
            ``(start_date, end_date)`` in ISO format for the second period.

        Returns
        -------
        pd.DataFrame
            Columns: metric, period_a_value, period_b_value, change, change_pct.
        """
        ds = dataset or self._dataset_name
        agg_a = self._period_aggregates(ds, period_a)
        agg_b = self._period_aggregates(ds, period_b)

        metrics = []
        for metric_name in ("mean_success_rate", "total_runs", "total_failures"):
            val_a = agg_a[metric_name]
            val_b = agg_b[metric_name]
            change = val_b - val_a
            change_pct = (change / val_a * 100) if val_a != 0 else None
            metrics.append(
                {
                    "metric": metric_name,
                    "period_a_value": val_a,
                    "period_b_value": val_b,
                    "change": change,
                    "change_pct": change_pct,
                }
            )

        return pd.DataFrame(metrics)

    def _period_aggregates(self, dataset: str, period: tuple[str, str]) -> dict[str, float]:
        """Compute aggregates for a single time period."""
        start, end = period

        if self._is_fabric:
            df = self._query_period_parquet(dataset, start, end)
        else:
            df = self._query_period_sqlite(dataset, start, end)

        if df.empty:
            return {"mean_success_rate": 0.0, "total_runs": 0.0, "total_failures": 0.0}

        return {
            "mean_success_rate": float(df["success_rate"].mean()),
            "total_runs": float(len(df)),
            "total_failures": float(df["failed_checks"].sum()),
        }

    def _query_period_sqlite(self, dataset: str, start: str, end: str) -> pd.DataFrame:
        sql = (
            "SELECT success_rate, failed_checks "
            "FROM validation_history "
            "WHERE dataset = ? AND timestamp >= ? AND timestamp <= ?"
        )
        return pd.read_sql(sql, self._conn, params=(dataset, start, end))

    def _query_period_parquet(self, dataset: str, start: str, end: str) -> pd.DataFrame:
        if not self._parquet_path.exists():
            return pd.DataFrame(columns=["success_rate", "failed_checks"])
        df = pd.read_parquet(self._parquet_path)
        mask = (df["dataset"] == dataset) & (df["timestamp"] >= start) & (df["timestamp"] <= end)
        return df.loc[mask, ["success_rate", "failed_checks"]].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Retention
    # ------------------------------------------------------------------

    def apply_retention(self) -> int:
        """Delete records older than ``retention_days`` and return deleted count.

        Returns
        -------
        int
            Number of records deleted.
        """
        cutoff = (datetime.now() - timedelta(days=self._retention_days)).isoformat()

        if self._is_fabric:
            return self._apply_retention_parquet(cutoff)
        return self._apply_retention_sqlite(cutoff)

    def _apply_retention_sqlite(self, cutoff: str) -> int:
        cursor = self._conn.execute("DELETE FROM validation_history WHERE timestamp < ?", (cutoff,))
        self._conn.commit()
        return cursor.rowcount

    def _apply_retention_parquet(self, cutoff: str) -> int:
        if not self._parquet_path.exists():
            return 0
        df = pd.read_parquet(self._parquet_path)
        original_count = len(df)
        df = df[df["timestamp"] >= cutoff].reset_index(drop=True)
        deleted = original_count - len(df)
        df.to_parquet(self._parquet_path, index=False)
        return deleted
