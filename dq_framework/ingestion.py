"""Data ingestion module."""
from pathlib import Path
from typing import Optional
import pandas as pd
import logging
import shutil

logger = logging.getLogger(__name__)

class DataIngester:
    """Handles data ingestion operations."""

    def __init__(self):
        """Initialize DataIngester."""

    def ingest_file(self, source_path: Path, target_path: Path, is_fabric: bool = False) -> bool:
        """
        Ingest a single file from source to target.
        Handles hybrid environment (Local vs Fabric).
        
        Args:
            source_path: Path to source file
            target_path: Path to target file
            is_fabric: Whether running in Fabric environment
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure target directory exists
            if not is_fabric:
                target_path.parent.mkdir(parents=True, exist_ok=True)
            
            if is_fabric:
                # Fabric: Use Pandas (or Spark if available) for Lakehouse integration
                # Note: In a real Fabric pipeline, we might use mssparkutils.fs.cp
                # but Pandas ensures we validate the parquet format implicitly.
                df = pd.read_parquet(source_path)
                df.to_parquet(target_path, index=False)
            else:
                # Local: Use shutil for efficient file copy (avoids memory overhead)
                shutil.copy2(source_path, target_path)
            
            logger.info(f"Ingested {source_path.name} -> {target_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to ingest {source_path}: {e}")
            return False
