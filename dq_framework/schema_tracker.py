"""Schema evolution tracking with baseline management and change detection.

Provides schema baseline CRUD operations, change detection via deepdiff,
change classification (breaking vs non-breaking), baseline generation
from DataProfiler output, evolution history tracking, and alert wiring
for breaking changes.

Usage::

    from dq_framework.schema_tracker import SchemaTracker, create_baseline_from_profile

    tracker = SchemaTracker(dataset_name="orders")
    tracker.save_baseline(schema)
    changes = tracker.detect_changes(current_schema)

    # Full workflow with history and alerting
    result = tracker.check_and_alert(current_schema, dispatcher=my_dispatcher)

    # Generate baseline from profiler output
    baseline = create_baseline_from_profile(profile_result, "orders")
"""

import logging
from datetime import datetime
from typing import Any

from deepdiff import DeepDiff

from .storage import ResultStore, get_store

logger = logging.getLogger(__name__)


def classify_changes(diff: DeepDiff) -> dict[str, list[dict[str, Any]]]:
    """Classify schema diff into breaking and non-breaking changes.

    Parses DeepDiff output to categorize changes:
    - Breaking: column removals, dtype changes, type changes
    - Non-breaking: column additions, nullability changes

    Args:
        diff: DeepDiff result comparing baseline and current schemas.

    Returns:
        Dict with 'breaking' and 'non_breaking' lists of change dicts.
    """
    breaking: list[dict[str, Any]] = []
    non_breaking: list[dict[str, Any]] = []

    # Column removals are breaking
    for path in diff.get("dictionary_item_removed", []):
        path_str = str(path)
        breaking.append({"type": "column_removed", "path": path_str})

    # Column additions are non-breaking
    for path in diff.get("dictionary_item_added", []):
        path_str = str(path)
        non_breaking.append({"type": "column_added", "path": path_str})

    # Type changes (Python type changed) are breaking
    for path, detail in diff.get("type_changes", {}).items():
        path_str = str(path)
        breaking.append(
            {
                "type": "type_changed",
                "path": path_str,
                "old": str(detail.get("old_type", "")),
                "new": str(detail.get("new_type", "")),
            }
        )

    # Value changes: dtype changes are breaking, nullability changes are non-breaking
    for path, detail in diff.get("values_changed", {}).items():
        path_str = str(path)
        if "dtype" in path_str:
            breaking.append(
                {
                    "type": "dtype_changed",
                    "path": path_str,
                    "old_value": detail.get("old_value"),
                    "new_value": detail.get("new_value"),
                }
            )
        elif "nullable" in path_str or "null_percent" in path_str:
            non_breaking.append(
                {
                    "type": "nullability_changed",
                    "path": path_str,
                    "old_value": detail.get("old_value"),
                    "new_value": detail.get("new_value"),
                }
            )

    return {"breaking": breaking, "non_breaking": non_breaking}


def create_baseline_from_profile(
    profile_result: dict[str, Any],
    dataset_name: str,
) -> dict[str, Any]:
    """Convert DataProfiler.profile() output to schema baseline format.

    Extracts column-level metadata (dtype, nullable, null_percent, detected_type)
    from profiler output and structures it as a canonical baseline dict.

    Args:
        profile_result: Output from DataProfiler.profile().
        dataset_name: Name to associate with this baseline.

    Returns:
        Schema baseline dict with columns, dataset_name, created_at, column_count.
    """
    columns: dict[str, dict[str, Any]] = {}
    for col_name, col_info in profile_result["columns"].items():
        columns[col_name] = {
            "dtype": col_info["dtype"],
            "nullable": col_info["null_percent"] > 0,
            "null_percent": col_info["null_percent"],
            "detected_type": col_info.get("detected_type", "unknown"),
        }

    return {
        "dataset_name": dataset_name,
        "created_at": datetime.now().isoformat(),
        "column_count": profile_result["column_count"],
        "columns": columns,
    }


def alert_on_breaking_changes(
    dispatcher: Any,
    dataset_name: str,
    classified: dict[str, list[dict[str, Any]]],
) -> dict[str, bool] | None:
    """Dispatch a critical alert if breaking schema changes exist.

    Uses the AlertDispatcher interface without a hard import, keeping
    the alerting dependency optional.

    Args:
        dispatcher: Object with a ``dispatch(results, severity=...)`` method.
        dataset_name: Dataset name for the alert payload.
        classified: Output from :func:`classify_changes`.

    Returns:
        Dispatch result dict, or None if no breaking changes.
    """
    if not classified["breaking"]:
        return None

    count = len(classified["breaking"])
    alert_payload: dict[str, Any] = {
        "success": False,
        "suite_name": f"schema_check_{dataset_name}",
        "batch_name": dataset_name,
        "success_rate": 0.0,
        "evaluated_checks": 1,
        "successful_checks": 0,
        "failed_checks": 1,
        "failed_expectations": [
            {
                "expectation_type": "schema_stability",
                "success": False,
                "description": f"{count} breaking schema change(s) detected",
                "meta": {"severity": "critical"},
            }
        ],
    }
    return dispatcher.dispatch(alert_payload, severity="critical")


class SchemaTracker:
    """Tracks schema baselines, detects changes, and classifies them.

    Uses ResultStore for persistence and DeepDiff for schema comparison.
    Each dataset gets its own baseline key: ``schema_{dataset_name}_baseline``.

    Args:
        store: ResultStore instance for persistence. Uses get_store() if None.
        dataset_name: Name of the dataset to track.
    """

    def __init__(
        self,
        store: ResultStore | None = None,
        dataset_name: str = "default",
    ) -> None:
        self._store = store or get_store()
        self._dataset_name = dataset_name

    def _baseline_key(self) -> str:
        """Return the ResultStore key for this dataset's baseline."""
        return f"schema_{self._dataset_name}_baseline"

    def save_baseline(self, schema: dict[str, Any]) -> None:
        """Save a schema baseline to the store.

        Args:
            schema: Schema dict with 'columns', 'dataset_name', etc.
        """
        self._store.write(self._baseline_key(), schema)
        logger.info("Schema baseline saved for dataset '%s'", self._dataset_name)

    def get_baseline(self) -> dict[str, Any] | None:
        """Retrieve the stored schema baseline.

        Returns:
            Baseline dict, or None if no baseline exists.
        """
        try:
            return self._store.read(self._baseline_key())
        except (FileNotFoundError, KeyError):
            logger.debug("No baseline found for dataset '%s'", self._dataset_name)
            return None

    def delete_baseline(self) -> bool:
        """Delete the stored schema baseline.

        Returns:
            True if deleted, False if no baseline existed.
        """
        deleted = self._store.delete(self._baseline_key())
        if deleted:
            logger.info("Schema baseline deleted for dataset '%s'", self._dataset_name)
        return deleted

    def record_change(
        self,
        classified: dict[str, list[dict[str, Any]]],
        diff_raw: dict[str, Any],
    ) -> str:
        """Record a schema change in the evolution history.

        Stores a timestamped entry via ResultStore with key
        ``schema_history_{dataset}_{timestamp}``.

        Args:
            classified: Output from :func:`classify_changes`.
            diff_raw: Serialized DeepDiff dict.

        Returns:
            The ResultStore key for the history entry.
        """
        now = datetime.now()
        ts_key = now.strftime("%Y%m%d_%H%M%S_%f")
        key = f"schema_history_{self._dataset_name}_{ts_key}"
        entry = {
            "timestamp": now.isoformat(),
            "dataset_name": self._dataset_name,
            "has_breaking_changes": len(classified.get("breaking", [])) > 0,
            "breaking": classified.get("breaking", []),
            "non_breaking": classified.get("non_breaking", []),
            "diff_raw": diff_raw,
        }
        self._store.write(key, entry)
        logger.info("Schema change recorded for dataset '%s' at %s", self._dataset_name, key)
        return key

    def get_history(self) -> list[dict[str, Any]]:
        """Retrieve all schema change history entries for this dataset.

        Returns:
            List of history entry dicts sorted by timestamp ascending.
            Empty list if no history exists.
        """
        prefix = f"schema_history_{self._dataset_name}_"
        keys = self._store.list(prefix=prefix)
        entries = []
        for key in keys:
            try:
                entries.append(self._store.read(key))
            except (FileNotFoundError, KeyError):
                logger.warning("History entry '%s' not readable, skipping", key)
        return sorted(entries, key=lambda e: e.get("timestamp", ""))

    def detect_changes(self, current_schema: dict[str, Any]) -> dict[str, Any]:
        """Detect schema changes between current and stored baseline.

        Compares the 'columns' sub-dict of both schemas using DeepDiff,
        then classifies changes as breaking or non-breaking.

        Args:
            current_schema: Current schema dict with 'columns' key.

        Returns:
            Dict with 'has_changes', 'breaking', 'non_breaking', 'diff_raw'.
            If no baseline exists, returns has_changes=False with a message.
        """
        baseline = self.get_baseline()
        if baseline is None:
            return {
                "has_changes": False,
                "breaking": [],
                "non_breaking": [],
                "diff_raw": {},
                "message": "No baseline found",
            }

        diff = DeepDiff(baseline["columns"], current_schema["columns"])

        if not diff:
            return {
                "has_changes": False,
                "breaking": [],
                "non_breaking": [],
                "diff_raw": {},
            }

        classified = classify_changes(diff)

        # Serialize diff for storage (DeepDiff objects aren't JSON-safe)
        try:
            diff_raw = diff.to_dict()
        except Exception:
            diff_raw = {}

        return {
            "has_changes": True,
            "breaking": classified["breaking"],
            "non_breaking": classified["non_breaking"],
            "diff_raw": diff_raw,
        }

    def check_and_alert(
        self,
        current_schema: dict[str, Any],
        dispatcher: Any = None,
    ) -> dict[str, Any]:
        """Detect changes, record history, and optionally alert on breaking changes.

        Convenience method combining :meth:`detect_changes`,
        :meth:`record_change`, and :func:`alert_on_breaking_changes`.

        Args:
            current_schema: Current schema dict with 'columns' key.
            dispatcher: Optional AlertDispatcher instance for breaking-change alerts.

        Returns:
            detect_changes result augmented with ``history_key`` (if recorded)
            and ``alert_result`` (if dispatched).
        """
        result = self.detect_changes(current_schema)

        history_key = None
        alert_result = None

        if result["has_changes"]:
            classified = {"breaking": result["breaking"], "non_breaking": result["non_breaking"]}
            history_key = self.record_change(classified, result["diff_raw"])

            if result["breaking"] and dispatcher is not None:
                alert_result = alert_on_breaking_changes(dispatcher, self._dataset_name, classified)

        result["history_key"] = history_key
        result["alert_result"] = alert_result
        return result
