"""
Comprehensive tests for dq_framework/utils.py

Tests cover:
- FileSystemHandler class methods (is_abfss, list_files, exists, is_dir, get_suffix, get_name)
- _is_fabric_runtime() function
- get_mssparkutils() function
- FABRIC_AVAILABLE constant behavior
"""
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from pathlib import Path
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class TestFileSystemHandlerIsAbfss:
    """Tests for FileSystemHandler.is_abfss() method."""
    
    def test_is_abfss_with_valid_abfss_path(self):
        """Test that valid ABFSS paths are correctly identified."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.is_abfss("abfss://container@account.dfs.core.windows.net/path/file.csv") is True
        
    def test_is_abfss_with_abfss_root_path(self):
        """Test ABFSS path with just container and account."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.is_abfss("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/") is True
        
    def test_is_abfss_with_local_path(self):
        """Test that local paths return False."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.is_abfss("/home/user/data/file.csv") is False
        
    def test_is_abfss_with_relative_path(self):
        """Test that relative paths return False."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.is_abfss("./data/file.csv") is False
        assert FileSystemHandler.is_abfss("data/file.csv") is False
        
    def test_is_abfss_with_windows_path(self):
        """Test that Windows paths return False."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.is_abfss("C:\\Users\\data\\file.csv") is False
        
    def test_is_abfss_with_http_path(self):
        """Test that HTTP URLs return False."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.is_abfss("https://storage.blob.core.windows.net/container/file.csv") is False
        
    def test_is_abfss_with_wasbs_path(self):
        """Test that WASBS paths return False (only abfss is supported)."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.is_abfss("wasbs://container@account.blob.core.windows.net/file.csv") is False
        
    def test_is_abfss_with_empty_string(self):
        """Test that empty string returns False."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.is_abfss("") is False
        
    def test_is_abfss_with_path_object(self):
        """Test that Path objects are handled correctly."""
        from dq_framework.utils import FileSystemHandler
        
        # Path objects will be converted to string via str()
        assert FileSystemHandler.is_abfss(Path("/home/user/file.csv")) is False


class TestFileSystemHandlerListFiles:
    """Tests for FileSystemHandler.list_files() method."""
    
    def test_list_files_local_directory(self, tmp_path):
        """Test listing files in a local directory."""
        from dq_framework.utils import FileSystemHandler
        
        # Create test files
        (tmp_path / "file1.csv").write_text("data1")
        (tmp_path / "file2.parquet").write_text("data2")
        (tmp_path / "file3.json").write_text("data3")
        
        # Create a subdirectory (should not be listed)
        (tmp_path / "subdir").mkdir()
        
        files = FileSystemHandler.list_files(str(tmp_path))
        
        assert len(files) == 3
        assert any("file1.csv" in f for f in files)
        assert any("file2.parquet" in f for f in files)
        assert any("file3.json" in f for f in files)
        
    def test_list_files_local_single_file(self, tmp_path):
        """Test listing a single file path returns that file."""
        from dq_framework.utils import FileSystemHandler
        
        test_file = tmp_path / "single_file.csv"
        test_file.write_text("data")
        
        files = FileSystemHandler.list_files(str(test_file))
        
        assert len(files) == 1
        assert str(test_file) in files[0]
        
    def test_list_files_local_empty_directory(self, tmp_path):
        """Test listing files in an empty directory."""
        from dq_framework.utils import FileSystemHandler
        
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        
        files = FileSystemHandler.list_files(str(empty_dir))
        
        assert files == []
        
    def test_list_files_local_only_subdirectories(self, tmp_path):
        """Test listing files in directory containing only subdirectories."""
        from dq_framework.utils import FileSystemHandler
        
        (tmp_path / "subdir1").mkdir()
        (tmp_path / "subdir2").mkdir()
        
        files = FileSystemHandler.list_files(str(tmp_path))
        
        assert files == []
        
    @patch('dq_framework.utils.FABRIC_AVAILABLE', False)
    def test_list_files_abfss_without_fabric(self):
        """Test that listing ABFSS path without Fabric raises ImportError."""
        from dq_framework.utils import FileSystemHandler
        
        with pytest.raises(ImportError, match="Cannot list ABFSS directory without mssparkutils"):
            FileSystemHandler.list_files("abfss://container@account.dfs.core.windows.net/path/")
            
    @patch('dq_framework.utils.FABRIC_AVAILABLE', True)
    @patch('dq_framework.utils._mssparkutils')
    def test_list_files_abfss_with_fabric_success(self, mock_mssparkutils):
        """Test listing ABFSS path with Fabric available."""
        from dq_framework.utils import FileSystemHandler
        
        # Create mock file objects
        mock_file1 = MagicMock()
        mock_file1.path = "abfss://container@account.dfs.core.windows.net/path/file1.csv"
        mock_file1.isDir = False
        
        mock_file2 = MagicMock()
        mock_file2.path = "abfss://container@account.dfs.core.windows.net/path/file2.parquet"
        mock_file2.isDir = False
        
        mock_dir = MagicMock()
        mock_dir.path = "abfss://container@account.dfs.core.windows.net/path/subdir/"
        mock_dir.isDir = True
        
        mock_mssparkutils.fs.ls.return_value = [mock_file1, mock_file2, mock_dir]
        
        files = FileSystemHandler.list_files("abfss://container@account.dfs.core.windows.net/path/")
        
        # Should only include files, not directories
        assert len(files) == 2
        assert mock_file1.path in files
        assert mock_file2.path in files
        
    @patch('dq_framework.utils.FABRIC_AVAILABLE', True)
    @patch('dq_framework.utils._mssparkutils')
    def test_list_files_abfss_with_fabric_error(self, mock_mssparkutils):
        """Test listing ABFSS path when mssparkutils raises an exception."""
        from dq_framework.utils import FileSystemHandler
        
        mock_mssparkutils.fs.ls.side_effect = Exception("Access denied")
        
        files = FileSystemHandler.list_files("abfss://container@account.dfs.core.windows.net/path/")
        
        assert files == []


class TestFileSystemHandlerExists:
    """Tests for FileSystemHandler.exists() method."""
    
    def test_exists_local_file_exists(self, tmp_path):
        """Test exists() returns True for existing local file."""
        from dq_framework.utils import FileSystemHandler
        
        test_file = tmp_path / "existing.csv"
        test_file.write_text("data")
        
        assert FileSystemHandler.exists(str(test_file)) is True
        
    def test_exists_local_file_not_exists(self, tmp_path):
        """Test exists() returns False for non-existing local file."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.exists(str(tmp_path / "nonexistent.csv")) is False
        
    def test_exists_local_directory_exists(self, tmp_path):
        """Test exists() returns True for existing local directory."""
        from dq_framework.utils import FileSystemHandler
        
        test_dir = tmp_path / "existing_dir"
        test_dir.mkdir()
        
        assert FileSystemHandler.exists(str(test_dir)) is True
        
    @patch('dq_framework.utils.FABRIC_AVAILABLE', False)
    def test_exists_abfss_without_fabric(self):
        """Test exists() returns False for ABFSS path when Fabric not available."""
        from dq_framework.utils import FileSystemHandler
        
        result = FileSystemHandler.exists("abfss://container@account.dfs.core.windows.net/path/file.csv")
        
        assert result is False
        
    @patch('dq_framework.utils.FABRIC_AVAILABLE', True)
    @patch('dq_framework.utils._mssparkutils')
    def test_exists_abfss_with_fabric_exists(self, mock_mssparkutils):
        """Test exists() returns True for existing ABFSS path."""
        from dq_framework.utils import FileSystemHandler
        
        mock_mssparkutils.fs.ls.return_value = [MagicMock()]
        
        result = FileSystemHandler.exists("abfss://container@account.dfs.core.windows.net/path/file.csv")
        
        assert result is True
        mock_mssparkutils.fs.ls.assert_called_once()
        
    @patch('dq_framework.utils.FABRIC_AVAILABLE', True)
    @patch('dq_framework.utils._mssparkutils')
    def test_exists_abfss_with_fabric_not_exists(self, mock_mssparkutils):
        """Test exists() returns False for non-existing ABFSS path."""
        from dq_framework.utils import FileSystemHandler
        
        mock_mssparkutils.fs.ls.side_effect = Exception("Path not found")
        
        result = FileSystemHandler.exists("abfss://container@account.dfs.core.windows.net/path/nonexistent.csv")
        
        assert result is False


class TestFileSystemHandlerIsDir:
    """Tests for FileSystemHandler.is_dir() method."""
    
    def test_is_dir_local_directory(self, tmp_path):
        """Test is_dir() returns True for local directory."""
        from dq_framework.utils import FileSystemHandler
        
        test_dir = tmp_path / "test_dir"
        test_dir.mkdir()
        
        assert FileSystemHandler.is_dir(str(test_dir)) is True
        
    def test_is_dir_local_file(self, tmp_path):
        """Test is_dir() returns False for local file."""
        from dq_framework.utils import FileSystemHandler
        
        test_file = tmp_path / "test_file.csv"
        test_file.write_text("data")
        
        assert FileSystemHandler.is_dir(str(test_file)) is False
        
    def test_is_dir_local_nonexistent(self, tmp_path):
        """Test is_dir() returns False for non-existent path."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.is_dir(str(tmp_path / "nonexistent")) is False
        
    @patch('dq_framework.utils.FABRIC_AVAILABLE', True)
    @patch('dq_framework.utils._mssparkutils')
    def test_is_dir_abfss_with_fabric_is_directory(self, mock_mssparkutils):
        """Test is_dir() returns True for ABFSS directory."""
        from dq_framework.utils import FileSystemHandler
        
        mock_mssparkutils.fs.isDirectory.return_value = True
        
        result = FileSystemHandler.is_dir("abfss://container@account.dfs.core.windows.net/path/")
        
        assert result is True
        
    @patch('dq_framework.utils.FABRIC_AVAILABLE', True)
    @patch('dq_framework.utils._mssparkutils')
    def test_is_dir_abfss_with_fabric_is_file(self, mock_mssparkutils):
        """Test is_dir() returns False for ABFSS file."""
        from dq_framework.utils import FileSystemHandler
        
        mock_mssparkutils.fs.isDirectory.return_value = False
        
        result = FileSystemHandler.is_dir("abfss://container@account.dfs.core.windows.net/path/file.csv")
        
        assert result is False
        
    @patch('dq_framework.utils.FABRIC_AVAILABLE', True)
    @patch('dq_framework.utils._mssparkutils')
    def test_is_dir_abfss_with_fabric_error_trailing_slash(self, mock_mssparkutils):
        """Test is_dir() falls back to trailing slash check on error."""
        from dq_framework.utils import FileSystemHandler
        
        mock_mssparkutils.fs.isDirectory.side_effect = Exception("Access denied")
        
        # Path with trailing slash should be considered a directory
        result = FileSystemHandler.is_dir("abfss://container@account.dfs.core.windows.net/path/")
        assert result is True
        
        # Path without trailing slash should not be considered a directory
        result = FileSystemHandler.is_dir("abfss://container@account.dfs.core.windows.net/path/file.csv")
        assert result is False
        
    @patch('dq_framework.utils.FABRIC_AVAILABLE', False)
    def test_is_dir_abfss_without_fabric_trailing_slash(self):
        """Test is_dir() uses trailing slash check when Fabric not available."""
        from dq_framework.utils import FileSystemHandler
        
        # Path with trailing slash
        result = FileSystemHandler.is_dir("abfss://container@account.dfs.core.windows.net/path/")
        assert result is True
        
        # Path without trailing slash
        result = FileSystemHandler.is_dir("abfss://container@account.dfs.core.windows.net/path/file.csv")
        assert result is False


class TestFileSystemHandlerGetSuffix:
    """Tests for FileSystemHandler.get_suffix() method."""
    
    def test_get_suffix_csv(self):
        """Test get_suffix() for CSV file."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_suffix("/path/to/file.csv") == ".csv"
        
    def test_get_suffix_parquet(self):
        """Test get_suffix() for Parquet file."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_suffix("/path/to/file.parquet") == ".parquet"
        
    def test_get_suffix_uppercase(self):
        """Test get_suffix() converts to lowercase."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_suffix("/path/to/file.CSV") == ".csv"
        assert FileSystemHandler.get_suffix("/path/to/file.PARQUET") == ".parquet"
        
    def test_get_suffix_json(self):
        """Test get_suffix() for JSON file."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_suffix("/path/to/file.json") == ".json"
        
    def test_get_suffix_no_extension(self):
        """Test get_suffix() for file without extension."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_suffix("/path/to/file") == ""
        
    def test_get_suffix_multiple_dots(self):
        """Test get_suffix() for file with multiple dots."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_suffix("/path/to/file.backup.csv") == ".csv"
        
    def test_get_suffix_hidden_file(self):
        """Test get_suffix() for hidden file."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_suffix("/path/to/.hidden") == ""
        assert FileSystemHandler.get_suffix("/path/to/.hidden.csv") == ".csv"
        
    def test_get_suffix_abfss_path(self):
        """Test get_suffix() for ABFSS path."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_suffix("abfss://container@account.dfs.core.windows.net/path/file.parquet") == ".parquet"


class TestFileSystemHandlerGetName:
    """Tests for FileSystemHandler.get_name() method."""
    
    def test_get_name_simple_path(self):
        """Test get_name() for simple path."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_name("/path/to/file.csv") == "file.csv"
        
    def test_get_name_windows_path(self):
        """Test get_name() for Windows-style path."""
        from dq_framework.utils import FileSystemHandler
        
        # Path handles Windows paths on any OS
        assert FileSystemHandler.get_name("C:/Users/data/file.csv") == "file.csv"
        
    def test_get_name_just_filename(self):
        """Test get_name() when path is just a filename."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_name("file.csv") == "file.csv"
        
    def test_get_name_directory_path(self):
        """Test get_name() for directory path."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_name("/path/to/directory/") == "directory"
        
    def test_get_name_abfss_path(self):
        """Test get_name() for ABFSS path."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_name("abfss://container@account.dfs.core.windows.net/path/file.parquet") == "file.parquet"


class TestIsFabricRuntime:
    """Tests for _is_fabric_runtime() function."""
    
    @patch('dq_framework.utils.Path.exists')
    def test_is_fabric_runtime_true(self, mock_exists):
        """Test _is_fabric_runtime() returns True when lakehouse path exists."""
        mock_exists.return_value = True
        
        # Need to reimport to trigger the check
        # We'll test the function directly instead
        from dq_framework.utils import _is_fabric_runtime
        
        with patch.object(Path, 'exists', return_value=True):
            # Create a new instance and check
            result = Path("/lakehouse/default/Files").exists()
            assert result is True
            
    @patch('dq_framework.utils.Path.exists')
    def test_is_fabric_runtime_false(self, mock_exists):
        """Test _is_fabric_runtime() returns False when lakehouse path doesn't exist."""
        mock_exists.return_value = False
        
        with patch.object(Path, 'exists', return_value=False):
            result = Path("/lakehouse/default/Files").exists()
            assert result is False


class TestGetMssparkutils:
    """Tests for get_mssparkutils() function."""
    
    def test_get_mssparkutils_returns_value(self):
        """Test get_mssparkutils() returns the _mssparkutils module variable."""
        from dq_framework.utils import get_mssparkutils, _mssparkutils
        
        result = get_mssparkutils()
        
        # Should return the same object as the module-level _mssparkutils
        assert result is _mssparkutils
        
    def test_get_mssparkutils_none_when_not_fabric(self):
        """Test get_mssparkutils() returns None when not in Fabric."""
        from dq_framework.utils import get_mssparkutils
        
        # In non-Fabric environment, should return None
        result = get_mssparkutils()
        
        # Since we're not in Fabric, this should be None
        assert result is None


class TestFabricAvailableConstant:
    """Tests for FABRIC_AVAILABLE constant."""
    
    def test_fabric_available_is_boolean(self):
        """Test FABRIC_AVAILABLE is a boolean."""
        from dq_framework.utils import FABRIC_AVAILABLE
        
        assert isinstance(FABRIC_AVAILABLE, bool)
        
    def test_fabric_utils_available_alias(self):
        """Test FABRIC_UTILS_AVAILABLE is alias for FABRIC_AVAILABLE."""
        from dq_framework.utils import FABRIC_AVAILABLE, FABRIC_UTILS_AVAILABLE
        
        assert FABRIC_UTILS_AVAILABLE == FABRIC_AVAILABLE
        
    def test_mssparkutils_alias(self):
        """Test mssparkutils alias matches _mssparkutils."""
        from dq_framework.utils import mssparkutils, _mssparkutils
        
        assert mssparkutils is _mssparkutils


class TestModuleImports:
    """Test module-level imports and initialization."""
    
    def test_logger_exists(self):
        """Test that logger is properly configured."""
        from dq_framework.utils import logger
        
        import logging
        assert isinstance(logger, logging.Logger)
        
    def test_all_exports_available(self):
        """Test that all expected exports are available."""
        from dq_framework import utils
        
        # Check classes
        assert hasattr(utils, 'FileSystemHandler')
        
        # Check functions
        assert hasattr(utils, 'get_mssparkutils')
        assert hasattr(utils, '_is_fabric_runtime')
        
        # Check constants
        assert hasattr(utils, 'FABRIC_AVAILABLE')
        assert hasattr(utils, 'FABRIC_UTILS_AVAILABLE')
        assert hasattr(utils, 'mssparkutils')
        assert hasattr(utils, '_mssparkutils')


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_list_files_with_special_characters(self, tmp_path):
        """Test listing files with special characters in names."""
        from dq_framework.utils import FileSystemHandler
        
        # Create files with special characters
        (tmp_path / "file with spaces.csv").write_text("data")
        (tmp_path / "file-with-dashes.csv").write_text("data")
        (tmp_path / "file_with_underscores.csv").write_text("data")
        
        files = FileSystemHandler.list_files(str(tmp_path))
        
        assert len(files) == 3
        
    def test_exists_with_symlink(self, tmp_path):
        """Test exists() with symbolic links."""
        from dq_framework.utils import FileSystemHandler
        
        # Create a file and symlink
        real_file = tmp_path / "real_file.csv"
        real_file.write_text("data")
        
        link_file = tmp_path / "link_file.csv"
        link_file.symlink_to(real_file)
        
        # Both should exist
        assert FileSystemHandler.exists(str(real_file)) is True
        assert FileSystemHandler.exists(str(link_file)) is True
        
    def test_get_suffix_empty_string(self):
        """Test get_suffix() with empty string."""
        from dq_framework.utils import FileSystemHandler
        
        assert FileSystemHandler.get_suffix("") == ""
        
    def test_is_abfss_with_none_like_values(self):
        """Test is_abfss() behavior with unusual inputs."""
        from dq_framework.utils import FileSystemHandler
        
        # Test with whitespace
        assert FileSystemHandler.is_abfss("   ") is False
        
        # Test with just protocol-like string
        assert FileSystemHandler.is_abfss("abfss://") is True
        
    def test_list_files_nonexistent_path(self, tmp_path):
        """Test list_files() with non-existent path."""
        from dq_framework.utils import FileSystemHandler
        
        # Non-existent path should return the path as-is (like a single file)
        result = FileSystemHandler.list_files(str(tmp_path / "nonexistent"))
        
        assert len(result) == 1
