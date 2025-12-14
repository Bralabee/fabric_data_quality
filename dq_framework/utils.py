"""
Utility functions for Fabric Data Quality Framework.
"""
from pathlib import Path
from typing import List, Union

def _is_fabric_runtime() -> bool:
    """Best-effort check for Microsoft Fabric runtime without importing notebookutils."""
    return Path("/lakehouse/default/Files").exists()


if _is_fabric_runtime():
    try:
        from notebookutils import mssparkutils
        FABRIC_AVAILABLE = True
    except Exception:
        FABRIC_AVAILABLE = False
else:
    FABRIC_AVAILABLE = False

class FileSystemHandler:
    """Handles file system operations for both local and ABFSS paths."""
    
    @staticmethod
    def is_abfss(path: str) -> bool:
        return str(path).startswith("abfss://")

    @staticmethod
    def list_files(path: str) -> List[str]:
        """List files in a directory (local or ABFSS)."""
        if FileSystemHandler.is_abfss(path):
            if not FABRIC_AVAILABLE:
                print("⚠️  Warning: ABFSS path detected but mssparkutils not found.")
                raise ImportError("Cannot list ABFSS directory without mssparkutils (Fabric environment).")
            
            try:
                files = mssparkutils.fs.ls(path)
                return [f.path for f in files if not f.isDir]
            except Exception as e:
                print(f"❌ Error listing ABFSS directory: {e}")
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
                mssparkutils.fs.ls(path)
                return True
            except Exception:
                return False
        return Path(path).exists()

    @staticmethod
    def is_dir(path: str) -> bool:
        if FileSystemHandler.is_abfss(path):
            if FABRIC_AVAILABLE:
                try:
                    return mssparkutils.fs.isDirectory(path)
                except:
                    return str(path).endswith('/')
            return str(path).endswith('/')
        return Path(path).is_dir()
    
    @staticmethod
    def get_suffix(path: str) -> str:
        return Path(path).suffix.lower()
    
    @staticmethod
    def get_name(path: str) -> str:
        return Path(path).name
