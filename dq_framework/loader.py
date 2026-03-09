"""
Data Loader
===========

Handles efficient loading of data from various sources with memory optimization.
"""

import logging
from pathlib import Path

import pandas as pd

from .constants import DEFAULT_AUTO_SAMPLE_ROWS, LARGE_FILE_SIZE_MB
from .utils import FileSystemHandler

logger = logging.getLogger(__name__)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


class DataLoader:
    """
    Handles loading of data with smart sampling and memory protection.
    """

    @staticmethod
    def load_data(file_path: Path | str, sample_size: int | None = None, **kwargs) -> pd.DataFrame:
        """
        Load data from various file formats with memory optimization.

        Args:
            file_path: Path to the file
            sample_size: Number of rows to load (if None, loads all)
            **kwargs: Additional arguments for pandas read functions

        Returns:
            pd.DataFrame: Loaded data
        """
        path_str = str(file_path)
        is_abfss = FileSystemHandler.is_abfss(path_str)

        if not FileSystemHandler.exists(path_str):
            raise FileNotFoundError(f"File not found: {path_str}")

        suffix = FileSystemHandler.get_suffix(path_str)

        # Auto-detect large files and enforce sampling if not specified
        # Only works for local files currently
        if not is_abfss and sample_size is None:
            try:
                file_size_mb = Path(path_str).stat().st_size / (1024 * 1024)
                if file_size_mb > LARGE_FILE_SIZE_MB:
                    logger.warning(
                        f"Large file detected ({file_size_mb:.1f} MB). Auto-limiting to {DEFAULT_AUTO_SAMPLE_ROWS:,} rows."
                    )
                    sample_size = DEFAULT_AUTO_SAMPLE_ROWS
            except Exception:
                pass

        try:
            if suffix == ".csv":
                # Auto-detect encoding if not provided
                encoding_arg = kwargs.get("encoding")
                if encoding_arg:
                    encodings = [encoding_arg] if isinstance(encoding_arg, str) else encoding_arg
                else:
                    encodings = ["utf-8", "latin-1", "iso-8859-1", "cp1252"]

                df = None
                last_error = None
                for encoding in encodings:
                    try:
                        df = pd.read_csv(
                            path_str, encoding=encoding, low_memory=False, nrows=sample_size
                        )
                        break
                    except Exception as e:
                        last_error = e
                        continue

                if df is None:
                    raise ValueError(f"Could not detect file encoding. Last error: {last_error}")

            elif suffix == ".parquet":
                # Optimization: Use PyArrow to read batches for local files
                if sample_size and PYARROW_AVAILABLE and not is_abfss:
                    try:
                        parquet_file = pq.ParquetFile(path_str)
                        batches = []
                        rows_loaded = 0

                        for batch in parquet_file.iter_batches(batch_size=sample_size):
                            batches.append(batch)
                            rows_loaded += batch.num_rows
                            if rows_loaded >= sample_size:
                                break

                        if batches:
                            table = pa.Table.from_batches(batches)
                            df = table.to_pandas()
                            if len(df) > sample_size:
                                df = df.head(sample_size)
                        else:
                            # Fallback
                            df = pd.read_parquet(path_str)
                            if sample_size:
                                df = df.head(sample_size)
                    except Exception as e:
                        logger.warning(
                            f"PyArrow optimization failed: {e}. Falling back to pandas read_parquet."
                        )
                        df = pd.read_parquet(path_str)
                        if sample_size:
                            df = df.head(sample_size)
                else:
                    # For ABFSS or if pyarrow missing
                    # Note: pd.read_parquet usually reads full file unless columns specified
                    # But we can't easily limit rows without reading.
                    # Spark would be better for ABFSS but this is a pandas-based tool.
                    df = pd.read_parquet(path_str)
                    if sample_size:
                        df = df.head(sample_size)

            elif suffix in [".xlsx", ".xls"]:
                df = pd.read_excel(path_str, nrows=sample_size)

            elif suffix == ".json":
                df = pd.read_json(path_str)
                if sample_size:
                    df = df.head(sample_size)

            else:
                raise ValueError(f"Unsupported file format: {suffix}")

            return df

        except Exception as e:
            logger.error(f"Error loading file {path_str}: {e}")
            raise
