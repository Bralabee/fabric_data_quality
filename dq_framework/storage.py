"""
Storage abstraction for validation result persistence.

Provides a pluggable ResultStore interface with two backends:
- JSONFileStore: Local JSON file storage for development
- LakehouseStore: MS Fabric Lakehouse storage for production

Usage::

    from dq_framework.storage import get_store, make_result_key

    store = get_store()  # auto-selects backend based on runtime
    key = make_result_key("my_batch")
    store.write(key, results)
    stored = store.read(key)
    all_keys = store.list(prefix="validation_")
    store.delete(key)
"""

import json
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from .utils import FABRIC_AVAILABLE, _is_fabric_runtime, get_mssparkutils

logger = logging.getLogger(__name__)


class ResultStore(ABC):
    """Abstract interface for validation result persistence.

    Subclasses must implement all four CRUD operations.
    Phase 9 will add SQLiteStore and ParquetStore backends.
    """

    @abstractmethod
    def write(self, key: str, data: dict[str, Any]) -> None:
        """Write a validation result.

        Args:
            key: Unique identifier (e.g., "validation_mydata_20260308_120000")
            data: Validation result dictionary

        Raises:
            OSError: If write fails
        """

    @abstractmethod
    def read(self, key: str) -> dict[str, Any]:
        """Read a validation result by key.

        Args:
            key: Result identifier

        Returns:
            Validation result dictionary

        Raises:
            FileNotFoundError: If key does not exist
        """

    @abstractmethod
    def list(self, prefix: Optional[str] = None) -> list[str]:
        """List available result keys.

        Args:
            prefix: Optional prefix filter

        Returns:
            Sorted list of result keys
        """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete a stored result.

        Args:
            key: Result identifier

        Returns:
            True if deleted, False if not found
        """


def _prepare_for_serialization(data: dict[str, Any]) -> str:
    """Remove non-serializable GX objects and convert to JSON string.

    Strips the 'validation_result' key (GX CheckpointResult object)
    and serializes remaining data with default=str for datetime, Path, etc.

    Args:
        data: Validation result dictionary

    Returns:
        JSON string
    """
    serializable = {k: v for k, v in data.items() if k != "validation_result"}
    return json.dumps(serializable, indent=2, default=str)


def make_result_key(batch_name: str) -> str:
    """Generate a storage key from batch name and current time.

    Produces keys in the format: validation_{safe_name}_{YYYYMMDD_HHMMSS}

    Args:
        batch_name: Name of the validation batch

    Returns:
        Filesystem-safe, sortable key string
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", batch_name)
    return f"validation_{safe_name}_{timestamp}"


class JSONFileStore(ResultStore):
    """Store validation results as JSON files in a local directory."""

    def __init__(self, results_dir: str = "dq_results"):
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"JSONFileStore initialized: {self.results_dir.resolve()}")

    def write(self, key: str, data: dict[str, Any]) -> None:
        """Write validation result as a JSON file."""
        file_path = self.results_dir / f"{key}.json"
        file_path.write_text(_prepare_for_serialization(data))
        logger.info(f"Result written: {file_path}")

    def read(self, key: str) -> dict[str, Any]:
        """Read validation result from a JSON file."""
        file_path = self.results_dir / f"{key}.json"
        if not file_path.exists():
            raise FileNotFoundError(f"No result found for key: {key}")
        return json.loads(file_path.read_text())

    def list(self, prefix: Optional[str] = None) -> list[str]:
        """List available result keys from JSON files in directory."""
        keys = [f.stem for f in self.results_dir.glob("*.json")]
        if prefix:
            keys = [k for k in keys if k.startswith(prefix)]
        return sorted(keys)

    def delete(self, key: str) -> bool:
        """Delete a stored JSON result file."""
        file_path = self.results_dir / f"{key}.json"
        if file_path.exists():
            file_path.unlink()
            logger.info(f"Result deleted: {file_path}")
            return True
        return False


class LakehouseStore(ResultStore):
    """Store validation results in MS Fabric Lakehouse via notebookutils/mssparkutils."""

    def __init__(self, results_dir: str = "Files/dq_results"):
        self._mssparkutils = get_mssparkutils()
        if not FABRIC_AVAILABLE or self._mssparkutils is None:
            raise RuntimeError(
                "LakehouseStore requires Fabric runtime with mssparkutils/notebookutils"
            )
        self.results_dir = results_dir
        logger.info(f"LakehouseStore initialized: {self.results_dir}")

    def write(self, key: str, data: dict[str, Any]) -> None:
        """Write validation result to Lakehouse via mssparkutils.fs.put."""
        file_path = f"{self.results_dir}/{key}.json"
        self._mssparkutils.fs.put(file_path, _prepare_for_serialization(data), True)
        logger.info(f"Result saved to Lakehouse: {file_path}")

    def read(self, key: str) -> dict[str, Any]:
        """Read validation result from Lakehouse via mssparkutils.fs.head."""
        file_path = f"{self.results_dir}/{key}.json"
        content = self._mssparkutils.fs.head(file_path, 10_000_000)
        return json.loads(content)

    def list(self, prefix: Optional[str] = None) -> list[str]:
        """List available result keys from Lakehouse directory."""
        try:
            files = self._mssparkutils.fs.ls(self.results_dir)
        except Exception:
            return []
        keys = [
            Path(f.path).stem
            for f in files
            if f.path.endswith(".json") and not f.isDir
        ]
        if prefix:
            keys = [k for k in keys if k.startswith(prefix)]
        return sorted(keys)

    def delete(self, key: str) -> bool:
        """Delete a stored result from Lakehouse via mssparkutils.fs.rm."""
        file_path = f"{self.results_dir}/{key}.json"
        try:
            self._mssparkutils.fs.rm(file_path, False)
            logger.info(f"Result deleted from Lakehouse: {file_path}")
            return True
        except Exception:
            return False


def get_store(
    results_dir: Optional[str] = None,
    backend: Optional[str] = None,
) -> ResultStore:
    """Create a ResultStore instance.

    Auto-selects backend based on runtime environment unless
    explicitly specified.

    Args:
        results_dir: Directory/path for results storage.
            Local: filesystem path (default: ./dq_results)
            Fabric: Lakehouse path (default: Files/dq_results)
        backend: Force a specific backend ("local" or "fabric").
            If None, auto-detects using _is_fabric_runtime().

    Returns:
        Configured ResultStore instance

    Raises:
        RuntimeError: If backend="fabric" but not in Fabric runtime
        ValueError: If backend is not "local", "fabric", or None
    """
    if backend is None:
        is_fabric = _is_fabric_runtime()
    elif backend == "local":
        return JSONFileStore(results_dir=results_dir or "dq_results")
    elif backend == "fabric":
        return LakehouseStore(results_dir=results_dir or "Files/dq_results")
    else:
        raise ValueError(f"Unknown backend: {backend!r}. Use 'local' or 'fabric'.")

    if is_fabric:
        return LakehouseStore(results_dir=results_dir or "Files/dq_results")
    else:
        return JSONFileStore(results_dir=results_dir or "dq_results")
