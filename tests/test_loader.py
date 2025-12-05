import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pathlib import Path
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dq_framework.loader import DataLoader
from dq_framework.utils import FileSystemHandler

class TestDataLoader(unittest.TestCase):
    
    @patch('dq_framework.utils.FileSystemHandler.exists')
    @patch('dq_framework.utils.FileSystemHandler.is_abfss')
    @patch('pandas.read_csv')
    def test_load_csv(self, mock_read_csv, mock_is_abfss, mock_exists):
        mock_exists.return_value = True
        mock_is_abfss.return_value = False
        mock_read_csv.return_value = pd.DataFrame({'a': [1, 2, 3]})
        
        df = DataLoader.load_data('test.csv')
        
        self.assertIsInstance(df, pd.DataFrame)
        mock_read_csv.assert_called()
        
    @patch('dq_framework.utils.FileSystemHandler.exists')
    @patch('dq_framework.utils.FileSystemHandler.is_abfss')
    @patch('pandas.read_parquet')
    def test_load_parquet_simple(self, mock_read_parquet, mock_is_abfss, mock_exists):
        mock_exists.return_value = True
        mock_is_abfss.return_value = False
        mock_read_parquet.return_value = pd.DataFrame({'a': [1, 2, 3]})
        
        # Mock pyarrow availability to False to force simple read
        with patch('dq_framework.loader.PYARROW_AVAILABLE', False):
            df = DataLoader.load_data('test.parquet')
            
        self.assertIsInstance(df, pd.DataFrame)
        mock_read_parquet.assert_called()

    @patch('dq_framework.utils.FileSystemHandler.exists')
    @patch('dq_framework.utils.FileSystemHandler.is_abfss')
    @patch('pathlib.Path.stat')
    @patch('pandas.read_csv')
    def test_large_file_auto_sample(self, mock_read_csv, mock_stat, mock_is_abfss, mock_exists):
        mock_exists.return_value = True
        mock_is_abfss.return_value = False
        
        # Mock file size > 500MB
        mock_stat_obj = MagicMock()
        mock_stat_obj.st_size = 600 * 1024 * 1024
        mock_stat.return_value = mock_stat_obj
        
        mock_read_csv.return_value = pd.DataFrame({'a': [1]})
        
        DataLoader.load_data('large.csv')
        
        # Check if nrows was set to 100000
        args, kwargs = mock_read_csv.call_args
        self.assertEqual(kwargs.get('nrows'), 100000)

if __name__ == '__main__':
    unittest.main()
