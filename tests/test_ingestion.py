"""
Comprehensive tests for dq_framework/ingestion.py

Tests cover:
- DataIngester class initialization
- ingest_file() for local environment
- ingest_file() with is_fabric=True (mock dependencies)
- Error handling for invalid files
- Edge cases and error paths
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dq_framework.ingestion import DataIngester


class TestDataIngesterInitialization:
    """Tests for DataIngester initialization."""

    def test_init_no_parameters(self):
        """Test DataIngester initializes with no parameters."""
        ingester = DataIngester()
        assert isinstance(ingester, DataIngester)


class TestDataIngesterIngestFileLocal:
    """Tests for DataIngester.ingest_file() in local environment."""

    def test_ingest_file_local_success(self, tmp_path):
        """Test successful local file ingestion."""
        ingester = DataIngester()

        # Create source file
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        source_file = source_dir / "test.csv"
        source_file.write_text("col1,col2\n1,2\n3,4")

        # Define target path
        target_dir = tmp_path / "target"
        target_file = target_dir / "test.csv"

        # Ingest file
        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is True
        assert target_file.exists()
        assert target_file.read_text() == source_file.read_text()

    def test_ingest_file_local_creates_target_directory(self, tmp_path):
        """Test that ingest_file creates target directory if it doesn't exist."""
        ingester = DataIngester()

        # Create source file
        source_file = tmp_path / "source.csv"
        source_file.write_text("data")

        # Define nested target path
        target_file = tmp_path / "deeply" / "nested" / "target" / "file.csv"

        # Ingest file
        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is True
        assert target_file.exists()
        assert target_file.parent.exists()

    def test_ingest_file_local_preserves_metadata(self, tmp_path):
        """Test that local ingestion preserves file metadata (using copy2)."""
        ingester = DataIngester()

        # Create source file
        source_file = tmp_path / "source.txt"
        source_file.write_text("test data")

        # Get original stats
        import time

        time.sleep(0.1)  # Small delay to ensure different timestamps

        # Define target
        target_file = tmp_path / "target" / "dest.txt"

        # Ingest file
        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is True
        # copy2 preserves modification time
        assert abs(source_file.stat().st_mtime - target_file.stat().st_mtime) < 1

    def test_ingest_file_local_binary_file(self, tmp_path):
        """Test ingestion of binary files."""
        ingester = DataIngester()

        # Create binary source file
        source_file = tmp_path / "source.bin"
        source_file.write_bytes(b"\x00\x01\x02\x03\xff\xfe\xfd")

        # Define target
        target_file = tmp_path / "target" / "dest.bin"

        # Ingest file
        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is True
        assert target_file.read_bytes() == source_file.read_bytes()

    def test_ingest_file_local_overwrite_existing(self, tmp_path):
        """Test that ingestion overwrites existing target file."""
        ingester = DataIngester()

        # Create source file
        source_file = tmp_path / "source.csv"
        source_file.write_text("new data")

        # Create existing target file
        target_dir = tmp_path / "target"
        target_dir.mkdir()
        target_file = target_dir / "dest.csv"
        target_file.write_text("old data")

        # Ingest file
        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is True
        assert target_file.read_text() == "new data"


class TestDataIngesterIngestFileFabric:
    """Tests for DataIngester.ingest_file() with is_fabric=True."""

    @patch("pandas.read_parquet")
    @patch.object(pd.DataFrame, "to_parquet")
    def test_ingest_file_fabric_success(self, mock_to_parquet, mock_read_parquet, tmp_path):
        """Test successful Fabric file ingestion."""
        ingester = DataIngester()

        # Mock pandas operations
        test_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_read_parquet.return_value = test_df

        source_file = tmp_path / "source.parquet"
        target_file = tmp_path / "target.parquet"

        # Ingest file with Fabric mode
        result = ingester.ingest_file(source_file, target_file, is_fabric=True)

        assert result is True
        mock_read_parquet.assert_called_once_with(source_file)

    @patch("pandas.read_parquet")
    def test_ingest_file_fabric_read_error(self, mock_read_parquet, tmp_path):
        """Test Fabric ingestion handles read errors gracefully."""
        ingester = DataIngester()

        mock_read_parquet.side_effect = Exception("Cannot read parquet file")

        source_file = tmp_path / "source.parquet"
        target_file = tmp_path / "target.parquet"

        # Should return False on error
        result = ingester.ingest_file(source_file, target_file, is_fabric=True)

        assert result is False

    @patch("pandas.read_parquet")
    @patch.object(pd.DataFrame, "to_parquet")
    def test_ingest_file_fabric_write_error(self, mock_to_parquet, mock_read_parquet, tmp_path):
        """Test Fabric ingestion handles write errors gracefully."""
        ingester = DataIngester()

        test_df = pd.DataFrame({"col1": [1, 2, 3]})
        mock_read_parquet.return_value = test_df
        mock_to_parquet.side_effect = Exception("Cannot write parquet file")

        source_file = tmp_path / "source.parquet"
        target_file = tmp_path / "target.parquet"

        # Should return False on error
        result = ingester.ingest_file(source_file, target_file, is_fabric=True)

        assert result is False

    @patch("pandas.read_parquet")
    @patch.object(pd.DataFrame, "to_parquet")
    def test_ingest_file_fabric_does_not_create_directory(
        self, mock_to_parquet, mock_read_parquet, tmp_path
    ):
        """Test that Fabric mode doesn't try to create directories locally."""
        ingester = DataIngester()

        test_df = pd.DataFrame({"col1": [1, 2, 3]})
        mock_read_parquet.return_value = test_df

        source_file = tmp_path / "source.parquet"
        # Non-existent directory path
        target_file = tmp_path / "nonexistent_dir" / "target.parquet"

        # In Fabric mode, we don't create the directory
        # The to_parquet mock won't actually write, so this tests the logic path
        ingester.ingest_file(source_file, target_file, is_fabric=True)

        # Target directory should NOT be created in Fabric mode
        assert not (tmp_path / "nonexistent_dir").exists()


class TestDataIngesterErrorHandling:
    """Tests for error handling in DataIngester."""

    def test_ingest_file_source_not_exists(self, tmp_path):
        """Test ingestion fails gracefully for non-existent source."""
        ingester = DataIngester()

        source_file = tmp_path / "nonexistent.csv"
        target_file = tmp_path / "target" / "dest.csv"

        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is False

    def test_ingest_file_permission_error(self, tmp_path):
        """Test ingestion handles permission errors."""
        ingester = DataIngester()

        # Create source file
        source_file = tmp_path / "source.csv"
        source_file.write_text("data")

        # Create read-only target directory
        target_dir = tmp_path / "readonly_target"
        target_dir.mkdir()
        target_file = target_dir / "dest.csv"

        # Make directory read-only (on Unix systems)
        try:
            target_dir.chmod(0o444)

            result = ingester.ingest_file(source_file, target_file, is_fabric=False)

            # Should fail due to permission error
            assert result is False
        finally:
            # Restore permissions for cleanup
            target_dir.chmod(0o755)

    def test_ingest_file_invalid_path_type(self, tmp_path):
        """Test ingestion with Path objects (expected input type)."""
        ingester = DataIngester()

        # Create source file
        source_file = tmp_path / "source.csv"
        source_file.write_text("data")

        target_file = tmp_path / "target" / "dest.csv"

        # Both inputs should be Path objects
        assert isinstance(source_file, Path)
        assert isinstance(target_file, Path)

        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is True


class TestDataIngesterLogging:
    """Tests for logging in DataIngester."""

    @patch("dq_framework.ingestion.logger")
    def test_ingest_file_logs_success(self, mock_logger, tmp_path):
        """Test that successful ingestion is logged."""
        ingester = DataIngester()

        # Create source file
        source_file = tmp_path / "source.csv"
        source_file.write_text("data")

        target_file = tmp_path / "target" / "dest.csv"

        ingester.ingest_file(source_file, target_file, is_fabric=False)

        mock_logger.info.assert_called()

    @patch("dq_framework.ingestion.logger")
    def test_ingest_file_logs_error(self, mock_logger, tmp_path):
        """Test that failed ingestion is logged as error."""
        ingester = DataIngester()

        # Non-existent source
        source_file = tmp_path / "nonexistent.csv"
        target_file = tmp_path / "target" / "dest.csv"

        ingester.ingest_file(source_file, target_file, is_fabric=False)

        mock_logger.error.assert_called()


class TestDataIngesterEdgeCases:
    """Edge case tests for DataIngester."""

    def test_ingest_file_empty_file(self, tmp_path):
        """Test ingestion of empty file."""
        ingester = DataIngester()

        # Create empty source file
        source_file = tmp_path / "empty.csv"
        source_file.write_text("")

        target_file = tmp_path / "target" / "empty.csv"

        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is True
        assert target_file.exists()
        assert target_file.read_text() == ""

    def test_ingest_file_large_filename(self, tmp_path):
        """Test ingestion with long filename."""
        ingester = DataIngester()

        # Create source file with long name
        long_name = "a" * 200 + ".csv"
        source_file = tmp_path / long_name
        source_file.write_text("data")

        target_file = tmp_path / "target" / long_name

        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is True
        assert target_file.exists()

    def test_ingest_file_special_characters_in_path(self, tmp_path):
        """Test ingestion with special characters in file path."""
        ingester = DataIngester()

        # Create source file with special characters
        source_dir = tmp_path / "source dir with spaces"
        source_dir.mkdir()
        source_file = source_dir / "file-with_special.chars.csv"
        source_file.write_text("data")

        target_dir = tmp_path / "target dir"
        target_file = target_dir / "file-with_special.chars.csv"

        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is True
        assert target_file.exists()

    def test_ingest_file_same_source_and_target(self, tmp_path):
        """Test ingestion when source and target are the same returns False."""
        ingester = DataIngester()

        # Create source file
        source_file = tmp_path / "file.csv"
        source_file.write_text("original data")

        # Try to ingest to same location
        result = ingester.ingest_file(source_file, source_file, is_fabric=False)

        # shutil.copy2 raises SameFileError for same source/target, which is caught
        # and returns False (error handling path)
        assert result is False
        # File should remain intact
        assert source_file.read_text() == "original data"

    def test_ingest_file_unicode_content(self, tmp_path):
        """Test ingestion of file with unicode content."""
        ingester = DataIngester()

        # Create source file with unicode
        source_file = tmp_path / "unicode.csv"
        source_file.write_text("col1,col2\n日本語,中文\nкириллица,العربية", encoding="utf-8")

        target_file = tmp_path / "target" / "unicode.csv"

        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is True
        assert target_file.read_text(encoding="utf-8") == source_file.read_text(encoding="utf-8")


class TestDataIngesterParquetIntegration:
    """Integration tests for parquet file handling."""

    def test_ingest_parquet_file_local(self, tmp_path):
        """Test local ingestion of actual parquet file."""
        ingester = DataIngester()

        # Create a parquet source file
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        source_file = source_dir / "test.parquet"

        # Create test DataFrame and save as parquet
        test_df = pd.DataFrame(
            {"int_col": [1, 2, 3], "str_col": ["a", "b", "c"], "float_col": [1.1, 2.2, 3.3]}
        )
        test_df.to_parquet(source_file, index=False)

        target_file = tmp_path / "target" / "test.parquet"

        # Ingest file locally (should use shutil.copy2)
        result = ingester.ingest_file(source_file, target_file, is_fabric=False)

        assert result is True
        assert target_file.exists()

        # Verify content is identical
        result_df = pd.read_parquet(target_file)
        pd.testing.assert_frame_equal(test_df, result_df)

    def test_ingest_parquet_file_fabric_mode(self, tmp_path):
        """Test Fabric mode ingestion of actual parquet file."""
        ingester = DataIngester()

        # Create a parquet source file
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        source_file = source_dir / "test.parquet"

        # Create test DataFrame and save as parquet
        test_df = pd.DataFrame(
            {"int_col": [1, 2, 3], "str_col": ["a", "b", "c"], "float_col": [1.1, 2.2, 3.3]}
        )
        test_df.to_parquet(source_file, index=False)

        # For Fabric mode, we need to create target dir manually
        # since the code doesn't create it in Fabric mode
        target_dir = tmp_path / "target"
        target_dir.mkdir()
        target_file = target_dir / "test.parquet"

        # Ingest file in Fabric mode (uses pd.read_parquet/to_parquet)
        result = ingester.ingest_file(source_file, target_file, is_fabric=True)

        assert result is True
        assert target_file.exists()

        # Verify content is identical
        result_df = pd.read_parquet(target_file)
        pd.testing.assert_frame_equal(test_df, result_df)


class TestDataIngesterModuleLevel:
    """Test module-level components."""

    def test_logger_exists(self):
        """Test that logger is properly configured in ingestion module."""
        import logging

        from dq_framework.ingestion import logger

        assert isinstance(logger, logging.Logger)

    def test_imports_available(self):
        """Test that required imports are available."""
        from dq_framework import ingestion

        assert hasattr(ingestion, "DataIngester")
        assert hasattr(ingestion, "pd")
        assert hasattr(ingestion, "Path")
        assert hasattr(ingestion, "shutil")
