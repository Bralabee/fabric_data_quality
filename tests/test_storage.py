"""Tests for dq_framework.storage module.

Covers ResultStore ABC enforcement, JSONFileStore CRUD, LakehouseStore CRUD
(mocked), get_store factory, make_result_key, and _prepare_for_serialization.
"""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dq_framework.storage import (
    JSONFileStore,
    LakehouseStore,
    ResultStore,
    _prepare_for_serialization,
    get_store,
    make_result_key,
)

# ---------------------------------------------------------------------------
# TestResultStoreABC
# ---------------------------------------------------------------------------


class TestResultStoreABC:
    """Verify ABC enforcement on ResultStore."""

    def test_cannot_instantiate_incomplete_subclass(self):
        """Subclass missing abstract methods raises TypeError."""

        class IncompleteStore(ResultStore):
            pass

        with pytest.raises(TypeError):
            IncompleteStore()

    def test_complete_subclass_can_instantiate(self):
        """Subclass implementing all methods can be instantiated."""

        class CompleteStore(ResultStore):
            def write(self, key, data):
                pass

            def read(self, key):
                return {}

            def list(self, prefix=None):
                return []

            def delete(self, key):
                return False

        store = CompleteStore()
        assert isinstance(store, ResultStore)


# ---------------------------------------------------------------------------
# TestJSONFileStore
# ---------------------------------------------------------------------------


class TestJSONFileStore:
    """CRUD operations using tmp_path for real filesystem tests."""

    def test_init_creates_directory(self, tmp_path):
        """__init__ creates results_dir if it does not exist."""
        results_dir = tmp_path / "new_results"
        JSONFileStore(results_dir=str(results_dir))
        assert results_dir.is_dir()

    def test_write_creates_json_file(self, tmp_path):
        """write() creates a .json file with correct content."""
        store = JSONFileStore(results_dir=str(tmp_path))
        data = {"success": True, "batch_name": "test"}
        store.write("result_001", data)

        file_path = tmp_path / "result_001.json"
        assert file_path.exists()
        content = json.loads(file_path.read_text())
        assert content["success"] is True

    def test_read_returns_dict(self, tmp_path):
        """read() returns dict from stored JSON file."""
        store = JSONFileStore(results_dir=str(tmp_path))
        data = {"success": True, "count": 42}
        store.write("result_002", data)

        result = store.read("result_002")
        assert result["success"] is True
        assert result["count"] == 42

    def test_read_raises_for_missing_key(self, tmp_path):
        """read() raises FileNotFoundError for missing key."""
        store = JSONFileStore(results_dir=str(tmp_path))
        with pytest.raises(FileNotFoundError):
            store.read("nonexistent")

    def test_list_returns_sorted_keys(self, tmp_path):
        """list() returns sorted list of keys."""
        store = JSONFileStore(results_dir=str(tmp_path))
        store.write("beta", {"a": 1})
        store.write("alpha", {"b": 2})
        store.write("gamma", {"c": 3})

        keys = store.list()
        assert keys == ["alpha", "beta", "gamma"]

    def test_list_filters_by_prefix(self, tmp_path):
        """list() filters by prefix when provided."""
        store = JSONFileStore(results_dir=str(tmp_path))
        store.write("validation_a", {"a": 1})
        store.write("validation_b", {"b": 2})
        store.write("other_c", {"c": 3})

        keys = store.list(prefix="validation_")
        assert keys == ["validation_a", "validation_b"]

    def test_list_returns_empty_for_empty_dir(self, tmp_path):
        """list() returns empty list for empty directory."""
        store = JSONFileStore(results_dir=str(tmp_path))
        assert store.list() == []

    def test_delete_removes_existing_file(self, tmp_path):
        """delete() returns True and removes file for existing key."""
        store = JSONFileStore(results_dir=str(tmp_path))
        store.write("to_delete", {"x": 1})
        assert store.delete("to_delete") is True
        assert not (tmp_path / "to_delete.json").exists()

    def test_delete_returns_false_for_missing_key(self, tmp_path):
        """delete() returns False for missing key."""
        store = JSONFileStore(results_dir=str(tmp_path))
        assert store.delete("nonexistent") is False


# ---------------------------------------------------------------------------
# TestLakehouseStore
# ---------------------------------------------------------------------------


class TestLakehouseStore:
    """CRUD operations with mocked mssparkutils."""

    def test_init_raises_when_mssparkutils_unavailable(self):
        """__init__ raises RuntimeError when mssparkutils unavailable."""
        with (
            patch("dq_framework.storage.get_mssparkutils", return_value=None),
            patch("dq_framework.storage.FABRIC_AVAILABLE", False),
        ):
            with pytest.raises(RuntimeError, match="Fabric runtime"):
                LakehouseStore()

    def test_write_calls_fs_put(self, mock_mssparkutils):
        """write() calls mssparkutils.fs.put with correct path and data."""
        with (
            patch("dq_framework.storage.get_mssparkutils", return_value=mock_mssparkutils),
            patch("dq_framework.storage.FABRIC_AVAILABLE", True),
        ):
            store = LakehouseStore(results_dir="Files/test_results")
            store.write("result_001", {"success": True})

            mock_mssparkutils.fs.put.assert_called_once()
            call_args = mock_mssparkutils.fs.put.call_args
            assert "result_001.json" in call_args[0][0]

    def test_read_calls_fs_head(self, mock_mssparkutils):
        """read() calls mssparkutils.fs.head with 10MB maxBytes limit."""
        mock_mssparkutils.fs.head.return_value = '{"success": true}'
        with (
            patch("dq_framework.storage.get_mssparkutils", return_value=mock_mssparkutils),
            patch("dq_framework.storage.FABRIC_AVAILABLE", True),
        ):
            store = LakehouseStore(results_dir="Files/test_results")
            result = store.read("result_001")

            mock_mssparkutils.fs.head.assert_called_once_with(
                "Files/test_results/result_001.json", 10_000_000
            )
            assert result["success"] is True

    def test_list_returns_sorted_keys(self, mock_mssparkutils):
        """list() returns sorted keys from mssparkutils.fs.ls."""
        file_b = MagicMock()
        file_b.path = "Files/test_results/beta.json"
        file_b.isDir = False
        file_a = MagicMock()
        file_a.path = "Files/test_results/alpha.json"
        file_a.isDir = False
        mock_mssparkutils.fs.ls.return_value = [file_b, file_a]

        with (
            patch("dq_framework.storage.get_mssparkutils", return_value=mock_mssparkutils),
            patch("dq_framework.storage.FABRIC_AVAILABLE", True),
        ):
            store = LakehouseStore(results_dir="Files/test_results")
            keys = store.list()
            assert keys == ["alpha", "beta"]

    def test_list_handles_prefix_filter(self, mock_mssparkutils):
        """list() filters by prefix when provided."""
        file_v = MagicMock()
        file_v.path = "Files/test_results/validation_a.json"
        file_v.isDir = False
        file_o = MagicMock()
        file_o.path = "Files/test_results/other_b.json"
        file_o.isDir = False
        mock_mssparkutils.fs.ls.return_value = [file_v, file_o]

        with (
            patch("dq_framework.storage.get_mssparkutils", return_value=mock_mssparkutils),
            patch("dq_framework.storage.FABRIC_AVAILABLE", True),
        ):
            store = LakehouseStore(results_dir="Files/test_results")
            keys = store.list(prefix="validation_")
            assert keys == ["validation_a"]

    def test_list_returns_empty_on_error(self, mock_mssparkutils):
        """list() returns empty list on error."""
        mock_mssparkutils.fs.ls.side_effect = Exception("access denied")

        with (
            patch("dq_framework.storage.get_mssparkutils", return_value=mock_mssparkutils),
            patch("dq_framework.storage.FABRIC_AVAILABLE", True),
        ):
            store = LakehouseStore(results_dir="Files/test_results")
            assert store.list() == []

    def test_delete_returns_true_on_success(self, mock_mssparkutils):
        """delete() calls mssparkutils.fs.rm and returns True on success."""
        with (
            patch("dq_framework.storage.get_mssparkutils", return_value=mock_mssparkutils),
            patch("dq_framework.storage.FABRIC_AVAILABLE", True),
        ):
            store = LakehouseStore(results_dir="Files/test_results")
            assert store.delete("result_001") is True
            mock_mssparkutils.fs.rm.assert_called_once()

    def test_delete_returns_false_on_error(self, mock_mssparkutils):
        """delete() returns False on error."""
        mock_mssparkutils.fs.rm.side_effect = Exception("not found")

        with (
            patch("dq_framework.storage.get_mssparkutils", return_value=mock_mssparkutils),
            patch("dq_framework.storage.FABRIC_AVAILABLE", True),
        ):
            store = LakehouseStore(results_dir="Files/test_results")
            assert store.delete("nonexistent") is False


# ---------------------------------------------------------------------------
# TestGetStore
# ---------------------------------------------------------------------------


class TestGetStore:
    """Factory function with patched _is_fabric_runtime."""

    def test_returns_json_file_store_when_not_fabric(self):
        """get_store() returns JSONFileStore when _is_fabric_runtime() is False."""
        with patch("dq_framework.storage._is_fabric_runtime", return_value=False):
            store = get_store()
            assert isinstance(store, JSONFileStore)

    def test_returns_lakehouse_store_when_fabric(self, mock_mssparkutils):
        """get_store() returns LakehouseStore when _is_fabric_runtime() is True."""
        with (
            patch("dq_framework.storage._is_fabric_runtime", return_value=True),
            patch("dq_framework.storage.get_mssparkutils", return_value=mock_mssparkutils),
            patch("dq_framework.storage.FABRIC_AVAILABLE", True),
        ):
            store = get_store()
            assert isinstance(store, LakehouseStore)

    def test_explicit_local_backend(self):
        """get_store(backend='local') always returns JSONFileStore."""
        with patch("dq_framework.storage._is_fabric_runtime", return_value=True):
            store = get_store(backend="local")
            assert isinstance(store, JSONFileStore)

    def test_explicit_fabric_backend_in_fabric(self, mock_mssparkutils):
        """get_store(backend='fabric') returns LakehouseStore when in Fabric."""
        with (
            patch("dq_framework.storage._is_fabric_runtime", return_value=True),
            patch("dq_framework.storage.get_mssparkutils", return_value=mock_mssparkutils),
            patch("dq_framework.storage.FABRIC_AVAILABLE", True),
        ):
            store = get_store(backend="fabric")
            assert isinstance(store, LakehouseStore)

    def test_explicit_fabric_backend_not_in_fabric(self):
        """get_store(backend='fabric') raises RuntimeError when not in Fabric."""
        with (
            patch("dq_framework.storage._is_fabric_runtime", return_value=False),
            patch("dq_framework.storage.get_mssparkutils", return_value=None),
            patch("dq_framework.storage.FABRIC_AVAILABLE", False),
            pytest.raises(RuntimeError),
        ):
            get_store(backend="fabric")

    def test_invalid_backend_raises_value_error(self):
        """get_store(backend='invalid') raises ValueError."""
        with patch("dq_framework.storage._is_fabric_runtime", return_value=False):
            with pytest.raises(ValueError, match="Unknown backend"):
                get_store(backend="invalid")


# ---------------------------------------------------------------------------
# TestMakeResultKey
# ---------------------------------------------------------------------------


class TestMakeResultKey:
    """Key format validation."""

    def test_produces_expected_format(self):
        """make_result_key produces 'validation_{safe_name}_{timestamp}' format."""
        key = make_result_key("my_batch")
        assert key.startswith("validation_my_batch_")
        # Timestamp is last 15 chars: YYYYMMDD_HHMMSS
        timestamp_part = key[-15:]
        assert len(timestamp_part) == 15
        # Verify it looks like a timestamp (digits_digits)
        assert timestamp_part[8] == "_"

    def test_sanitizes_special_characters(self):
        """make_result_key replaces non-alphanumeric chars with underscore."""
        key = make_result_key("my-batch.name!")
        assert "validation_my_batch_name_" in key
        assert "-" not in key
        assert "." not in key
        assert "!" not in key


# ---------------------------------------------------------------------------
# TestPrepareForSerialization
# ---------------------------------------------------------------------------


class TestPrepareForSerialization:
    """Serialization helper tests."""

    def test_strips_validation_result_key(self):
        """_prepare_for_serialization strips 'validation_result' key."""
        data = {
            "success": True,
            "validation_result": MagicMock(),
            "batch_name": "test",
        }
        result_str = _prepare_for_serialization(data)
        result = json.loads(result_str)
        assert "validation_result" not in result
        assert result["success"] is True
        assert result["batch_name"] == "test"

    def test_serializes_with_default_str(self):
        """_prepare_for_serialization uses default=str for non-serializable types."""
        data = {
            "timestamp": datetime(2026, 3, 8, 12, 0, 0),
            "path": Path("/some/path"),
        }
        result_str = _prepare_for_serialization(data)
        result = json.loads(result_str)
        assert result["timestamp"] == "2026-03-08 12:00:00"
        assert "/some/path" in result["path"]
