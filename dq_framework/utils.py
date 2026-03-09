"""
Utility functions for Fabric Data Quality Framework.
"""

from pathlib import Path


def _is_fabric_runtime() -> bool:
    """
    Best-effort check for Microsoft Fabric runtime without importing notebookutils.

    Returns:
        bool: True if running in MS Fabric environment (lakehouse detected)
    """
    return Path("/lakehouse/default/Files").exists()


# Centralized Fabric detection - single source of truth
if _is_fabric_runtime():
    try:
        from notebookutils import mssparkutils as _mssparkutils

        FABRIC_AVAILABLE = True
    except Exception:
        _mssparkutils = None
        FABRIC_AVAILABLE = False
else:
    _mssparkutils = None
    FABRIC_AVAILABLE = False


def get_mssparkutils():
    """
    Get mssparkutils if available in Fabric environment.

    Returns:
        mssparkutils module or None if not in Fabric environment
    """
    return _mssparkutils


# Aliases for backward compatibility
FABRIC_UTILS_AVAILABLE = FABRIC_AVAILABLE  # Alias for fabric_connector compatibility

# Alias for backward compatibility - can also be accessed via get_mssparkutils()
mssparkutils = _mssparkutils

import logging

logger = logging.getLogger(__name__)


class FileSystemHandler:
    """Handles file system operations for both local and ABFSS paths."""

    @staticmethod
    def is_abfss(path: str) -> bool:
        return str(path).startswith("abfss://")

    @staticmethod
    def list_files(path: str) -> list[str]:
        """List files in a directory (local or ABFSS)."""
        if FileSystemHandler.is_abfss(path):
            if not FABRIC_AVAILABLE:
                logger.warning("ABFSS path detected but mssparkutils not found.")
                raise ImportError(
                    "Cannot list ABFSS directory without mssparkutils (Fabric environment)."
                )

            try:
                files = _mssparkutils.fs.ls(path)
                return [f.path for f in files if not f.isDir]
            except Exception as e:
                logger.error(f"Error listing ABFSS directory: {e}")
                return []
        else:
            p = Path(path)
            if p.is_dir():
                return [str(f) for f in p.iterdir() if f.is_file()]
            return [str(path)]

    @staticmethod
    def exists(path: str) -> bool:
        if FileSystemHandler.is_abfss(path):
            if not FABRIC_AVAILABLE:
                return False
            try:
                _mssparkutils.fs.ls(path)
                return True
            except Exception:
                return False
        return Path(path).exists()

    @staticmethod
    def is_dir(path: str) -> bool:
        if FileSystemHandler.is_abfss(path):
            if FABRIC_AVAILABLE:
                try:
                    return _mssparkutils.fs.isDirectory(path)
                except Exception as e:
                    logger.debug(f"Could not determine if ABFSS path is directory: {e}")
                    return str(path).endswith("/")
            return str(path).endswith("/")
        return Path(path).is_dir()

    @staticmethod
    def get_suffix(path: str) -> str:
        return Path(path).suffix.lower()

    @staticmethod
    def get_name(path: str) -> str:
        return Path(path).name
