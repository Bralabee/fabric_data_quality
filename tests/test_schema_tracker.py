"""Tests for dq_framework.schema_tracker module.

Covers SCHM-01 (baseline CRUD), SCHM-02 (change detection),
SCHM-03 (change classification), SCHM-05 (baseline from profile).
"""

import pytest

from dq_framework.schema_tracker import (
    SchemaTracker,
    classify_changes,
    create_baseline_from_profile,
)
from dq_framework.storage import JSONFileStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def store(tmp_path):
    """JSONFileStore backed by a temp directory."""
    return JSONFileStore(results_dir=str(tmp_path / "dq_results"))


@pytest.fixture()
def tracker(store):
    """SchemaTracker with a temp-backed store and dataset name 'orders'."""
    return SchemaTracker(store=store, dataset_name="orders")


@pytest.fixture()
def baseline_schema():
    """Sample baseline schema dict."""
    return {
        "dataset_name": "orders",
        "created_at": "2026-03-10T12:00:00",
        "column_count": 3,
        "columns": {
            "id": {
                "dtype": "int64",
                "nullable": False,
                "null_percent": 0.0,
                "detected_type": "numeric",
            },
            "name": {
                "dtype": "object",
                "nullable": True,
                "null_percent": 2.5,
                "detected_type": "string",
            },
            "amount": {
                "dtype": "float64",
                "nullable": False,
                "null_percent": 0.0,
                "detected_type": "numeric",
            },
        },
    }


@pytest.fixture()
def profile_result():
    """Sample DataProfiler.profile() output."""
    return {
        "row_count": 100,
        "column_count": 3,
        "columns": {
            "id": {
                "dtype": "int64",
                "null_count": 0,
                "null_percent": 0.0,
                "unique_count": 100,
                "unique_percent": 100.0,
                "detected_type": "numeric",
            },
            "name": {
                "dtype": "object",
                "null_count": 5,
                "null_percent": 5.0,
                "unique_count": 90,
                "unique_percent": 90.0,
                "detected_type": "string",
            },
            "status": {
                "dtype": "object",
                "null_count": 0,
                "null_percent": 0.0,
                "unique_count": 3,
                "unique_percent": 3.0,
                "detected_type": "categorical",
            },
        },
        "profiled_at": "2026-03-10T12:00:00",
    }


# ---------------------------------------------------------------------------
# TestSchemaBaseline (SCHM-01)
# ---------------------------------------------------------------------------


class TestSchemaBaseline:
    """Tests for baseline CRUD operations via ResultStore."""

    def test_save_baseline_stores_json(self, tracker, store, baseline_schema):
        """save_baseline persists schema to the ResultStore."""
        tracker.save_baseline(baseline_schema)
        stored = store.read("schema_orders_baseline")
        assert stored["dataset_name"] == "orders"
        assert "columns" in stored

    def test_get_baseline_retrieves_stored(self, tracker, baseline_schema):
        """get_baseline returns previously saved baseline."""
        tracker.save_baseline(baseline_schema)
        result = tracker.get_baseline()
        assert result is not None
        assert result["dataset_name"] == "orders"
        assert set(result["columns"].keys()) == {"id", "name", "amount"}

    def test_get_baseline_returns_none_for_missing(self, tracker):
        """get_baseline returns None when no baseline exists."""
        result = tracker.get_baseline()
        assert result is None

    def test_delete_baseline_removes_it(self, tracker, baseline_schema):
        """delete_baseline removes stored baseline."""
        tracker.save_baseline(baseline_schema)
        deleted = tracker.delete_baseline()
        assert deleted is True
        assert tracker.get_baseline() is None

    def test_delete_baseline_returns_false_when_missing(self, tracker):
        """delete_baseline returns False when no baseline exists."""
        deleted = tracker.delete_baseline()
        assert deleted is False


# ---------------------------------------------------------------------------
# TestSchemaDetection (SCHM-02)
# ---------------------------------------------------------------------------


class TestSchemaDetection:
    """Tests for schema change detection via deepdiff."""

    def test_identical_schemas_no_changes(self, tracker, baseline_schema):
        """detect_changes returns empty result for identical schemas."""
        tracker.save_baseline(baseline_schema)
        result = tracker.detect_changes(baseline_schema)
        assert result["has_changes"] is False
        assert result["breaking"] == []
        assert result["non_breaking"] == []

    def test_detect_column_addition(self, tracker, baseline_schema):
        """detect_changes finds column additions."""
        tracker.save_baseline(baseline_schema)
        current = {
            **baseline_schema,
            "columns": {
                **baseline_schema["columns"],
                "email": {
                    "dtype": "object",
                    "nullable": True,
                    "null_percent": 1.0,
                    "detected_type": "string",
                },
            },
        }
        result = tracker.detect_changes(current)
        assert result["has_changes"] is True
        added_types = [c["type"] for c in result["non_breaking"]]
        assert "column_added" in added_types

    def test_detect_column_removal(self, tracker, baseline_schema):
        """detect_changes finds column removals."""
        tracker.save_baseline(baseline_schema)
        current_columns = {
            k: v for k, v in baseline_schema["columns"].items() if k != "amount"
        }
        current = {**baseline_schema, "columns": current_columns}
        result = tracker.detect_changes(current)
        assert result["has_changes"] is True
        removed_types = [c["type"] for c in result["breaking"]]
        assert "column_removed" in removed_types

    def test_detect_dtype_change(self, tracker, baseline_schema):
        """detect_changes finds dtype changes."""
        tracker.save_baseline(baseline_schema)
        current = {
            **baseline_schema,
            "columns": {
                **baseline_schema["columns"],
                "id": {
                    **baseline_schema["columns"]["id"],
                    "dtype": "float64",
                },
            },
        }
        result = tracker.detect_changes(current)
        assert result["has_changes"] is True
        dtype_types = [c["type"] for c in result["breaking"]]
        assert "dtype_changed" in dtype_types

    def test_detect_changes_no_baseline(self, tracker, baseline_schema):
        """detect_changes returns no-change result when baseline missing."""
        result = tracker.detect_changes(baseline_schema)
        assert result["has_changes"] is False
        assert "message" in result


# ---------------------------------------------------------------------------
# TestChangeClassification (SCHM-03)
# ---------------------------------------------------------------------------


class TestChangeClassification:
    """Tests for classify_changes pure function."""

    def test_column_removal_is_breaking(self):
        """Column removals classified as breaking."""
        from deepdiff import DeepDiff

        old = {"columns": {"id": {"dtype": "int64"}, "name": {"dtype": "object"}}}
        new = {"columns": {"id": {"dtype": "int64"}}}
        diff = DeepDiff(old, new)
        result = classify_changes(diff)
        assert len(result["breaking"]) >= 1
        assert any(c["type"] == "column_removed" for c in result["breaking"])

    def test_dtype_change_is_breaking(self):
        """dtype changes classified as breaking."""
        from deepdiff import DeepDiff

        old = {"columns": {"id": {"dtype": "int64"}}}
        new = {"columns": {"id": {"dtype": "float64"}}}
        diff = DeepDiff(old, new)
        result = classify_changes(diff)
        assert len(result["breaking"]) >= 1
        assert any(c["type"] == "dtype_changed" for c in result["breaking"])

    def test_column_addition_is_non_breaking(self):
        """Column additions classified as non-breaking."""
        from deepdiff import DeepDiff

        old = {"columns": {"id": {"dtype": "int64"}}}
        new = {"columns": {"id": {"dtype": "int64"}, "email": {"dtype": "object"}}}
        diff = DeepDiff(old, new)
        result = classify_changes(diff)
        assert len(result["non_breaking"]) >= 1
        assert any(c["type"] == "column_added" for c in result["non_breaking"])

    def test_nullability_change_is_non_breaking(self):
        """Nullability changes classified as non-breaking."""
        from deepdiff import DeepDiff

        old = {"columns": {"id": {"dtype": "int64", "nullable": False}}}
        new = {"columns": {"id": {"dtype": "int64", "nullable": True}}}
        diff = DeepDiff(old, new)
        result = classify_changes(diff)
        assert len(result["non_breaking"]) >= 1
        assert any(c["type"] == "nullability_changed" for c in result["non_breaking"])

    def test_empty_diff_returns_empty_lists(self):
        """Empty diff produces empty breaking and non_breaking lists."""
        from deepdiff import DeepDiff

        old = {"columns": {"id": {"dtype": "int64"}}}
        diff = DeepDiff(old, old)
        result = classify_changes(diff)
        assert result["breaking"] == []
        assert result["non_breaking"] == []


# ---------------------------------------------------------------------------
# TestBaselineFromProfile (SCHM-05)
# ---------------------------------------------------------------------------


class TestBaselineFromProfile:
    """Tests for create_baseline_from_profile conversion function."""

    def test_extracts_dtype_per_column(self, profile_result):
        """Extracts dtype from profile output."""
        baseline = create_baseline_from_profile(profile_result, "test_ds")
        assert baseline["columns"]["id"]["dtype"] == "int64"
        assert baseline["columns"]["name"]["dtype"] == "object"

    def test_derives_nullable_from_null_percent(self, profile_result):
        """nullable is True when null_percent > 0."""
        baseline = create_baseline_from_profile(profile_result, "test_ds")
        assert baseline["columns"]["id"]["nullable"] is False  # 0.0%
        assert baseline["columns"]["name"]["nullable"] is True  # 5.0%

    def test_includes_detected_type(self, profile_result):
        """detected_type is included per column."""
        baseline = create_baseline_from_profile(profile_result, "test_ds")
        assert baseline["columns"]["id"]["detected_type"] == "numeric"
        assert baseline["columns"]["status"]["detected_type"] == "categorical"

    def test_includes_null_percent(self, profile_result):
        """null_percent is preserved in baseline."""
        baseline = create_baseline_from_profile(profile_result, "test_ds")
        assert baseline["columns"]["name"]["null_percent"] == 5.0

    def test_includes_dataset_name(self, profile_result):
        """Baseline includes dataset_name."""
        baseline = create_baseline_from_profile(profile_result, "my_dataset")
        assert baseline["dataset_name"] == "my_dataset"

    def test_includes_created_at(self, profile_result):
        """Baseline includes created_at timestamp."""
        baseline = create_baseline_from_profile(profile_result, "test_ds")
        assert "created_at" in baseline
        assert isinstance(baseline["created_at"], str)

    def test_includes_column_count(self, profile_result):
        """Baseline includes column_count from profile."""
        baseline = create_baseline_from_profile(profile_result, "test_ds")
        assert baseline["column_count"] == 3
