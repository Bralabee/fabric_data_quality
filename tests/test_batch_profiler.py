import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dq_framework.batch_profiler import BatchProfiler

class TestBatchProfiler(unittest.TestCase):
    
    @patch('dq_framework.batch_profiler.DataLoader.load_data')
    @patch('dq_framework.batch_profiler.DataProfiler')
    @patch('dq_framework.batch_profiler.FileSystemHandler')
    @patch('pathlib.Path.mkdir')
    def test_process_single_file(self, mock_mkdir, mock_fs, mock_profiler_cls, mock_load_data):
        # Setup mocks
        mock_fs.get_name.return_value = "test.parquet"
        mock_fs.is_abfss.return_value = False
        
        mock_df = MagicMock()
        mock_df.__len__.return_value = 100
        mock_load_data.return_value = mock_df
        
        mock_profiler = MagicMock()
        mock_profiler_cls.return_value = mock_profiler
        mock_profiler.generate_expectations.return_value = {'expectations': [1, 2]}
        
        # Run
        result = BatchProfiler.process_single_file('test.parquet', 'output_dir')
        
        # Assert
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['file'], 'test.parquet')
        self.assertEqual(result['expectations'], 2)
        
        mock_load_data.assert_called_with('test.parquet', sample_size=None)
        mock_profiler.profile.assert_called()
        mock_profiler.save_config.assert_called()

if __name__ == '__main__':
    unittest.main()
