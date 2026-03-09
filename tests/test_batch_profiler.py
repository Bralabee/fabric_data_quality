"""Tests for dq_framework.batch_profiler -- pytest style, no unittest.TestCase."""

from unittest.mock import MagicMock

from dq_framework.batch_profiler import BatchProfiler

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_BP = "dq_framework.batch_profiler"


def _mock_profiler_pipeline(mocker):
    """Patch DataLoader, DataProfiler, FileSystemHandler for process_single_file."""
    mock_load = mocker.patch(f"{_BP}.DataLoader.load_data")
    mock_df = MagicMock()
    mock_df.__len__ = MagicMock(return_value=100)
    mock_load.return_value = mock_df

    mock_profiler_cls = mocker.patch(f"{_BP}.DataProfiler")
    mock_profiler = MagicMock()
    mock_profiler_cls.return_value = mock_profiler
    mock_profiler.generate_expectations.return_value = {"expectations": [1, 2]}

    mock_fs = mocker.patch(f"{_BP}.FileSystemHandler")
    mock_fs.get_name.return_value = "test.parquet"
    mock_fs.is_abfss.return_value = False

    mocker.patch("pathlib.Path.mkdir")

    return mock_load, mock_profiler, mock_fs


# ===== process_single_file =================================================


class TestProcessSingleFile:
    def test_process_single_file_success(self, mocker):
        """Existing behaviour: successful profiling returns status=success."""
        mock_load, mock_profiler, _ = _mock_profiler_pipeline(mocker)

        result = BatchProfiler.process_single_file("test.parquet", "output_dir")

        assert result["status"] == "success"
        assert result["file"] == "test.parquet"
        assert result["expectations"] == 2
        mock_load.assert_called_with("test.parquet", sample_size=None)
        mock_profiler.profile.assert_called_once()
        mock_profiler.save_config.assert_called_once()

    def test_process_single_file_error(self, mocker):
        """DataLoader raises -- returns status=error with error message."""
        mocker.patch(f"{_BP}.FileSystemHandler")
        mocker.patch(
            f"{_BP}.DataLoader.load_data",
            side_effect=Exception("load error"),
        )

        result = BatchProfiler.process_single_file("bad.parquet", "output_dir")

        assert result["status"] == "error"
        assert result["file"] == "bad.parquet"
        assert result["error"] == "load error"

    def test_process_single_file_with_thresholds(self, mocker):
        """Thresholds dict is forwarded to generate_expectations via gen_kwargs."""
        _mock_load, mock_profiler, _ = _mock_profiler_pipeline(mocker)

        thresholds = {"critical_threshold": 95}
        BatchProfiler.process_single_file("test.parquet", "output_dir", thresholds=thresholds)

        call_kwargs = mock_profiler.generate_expectations.call_args[1]
        assert call_kwargs["critical_threshold"] == 95


# ===== run_parallel_profiling ==============================================


class TestRunParallelProfiling:
    def test_run_parallel_profiling_success(self, mocker):
        """Two supported files found -- returns two results."""
        mock_fs = mocker.patch(f"{_BP}.FileSystemHandler")
        mock_fs.exists.return_value = True
        mock_fs.list_files.return_value = ["a.parquet", "b.csv"]
        mock_fs.get_suffix.side_effect = lambda p: "." + p.rsplit(".", 1)[-1]

        # Mock ProcessPoolExecutor to avoid spawning processes
        mock_future_a = MagicMock()
        mock_future_a.result.return_value = {
            "status": "success",
            "file": "a.parquet",
            "rows": 10,
            "expectations": 3,
        }
        mock_future_b = MagicMock()
        mock_future_b.result.return_value = {
            "status": "success",
            "file": "b.csv",
            "rows": 20,
            "expectations": 5,
        }

        mock_executor = MagicMock()
        mock_executor.__enter__ = MagicMock(return_value=mock_executor)
        mock_executor.__exit__ = MagicMock(return_value=False)
        mock_executor.submit.side_effect = [mock_future_a, mock_future_b]

        mocker.patch("concurrent.futures.ProcessPoolExecutor", return_value=mock_executor)
        mocker.patch(
            "concurrent.futures.as_completed",
            return_value=iter([mock_future_a, mock_future_b]),
        )

        results = BatchProfiler.run_parallel_profiling("input_dir", "output_dir", workers=2)

        assert len(results) == 2
        assert all(r["status"] == "success" for r in results)

    def test_run_parallel_profiling_no_files(self, mocker):
        """Directory exists but contains no files -- returns empty list."""
        mock_fs = mocker.patch(f"{_BP}.FileSystemHandler")
        mock_fs.exists.return_value = True
        mock_fs.list_files.return_value = []

        results = BatchProfiler.run_parallel_profiling("empty_dir", "output_dir")

        assert results == []

    def test_run_parallel_profiling_nonexistent_dir(self, mocker):
        """Input dir does not exist -- returns empty list."""
        mock_fs = mocker.patch(f"{_BP}.FileSystemHandler")
        mock_fs.exists.return_value = False

        results = BatchProfiler.run_parallel_profiling("no_such_dir", "output_dir")

        assert results == []

    def test_run_parallel_profiling_filters_unsupported(self, mocker):
        """Unsupported extensions (.txt) are filtered out; only .parquet/.csv processed."""
        mock_fs = mocker.patch(f"{_BP}.FileSystemHandler")
        mock_fs.exists.return_value = True
        mock_fs.list_files.return_value = ["a.parquet", "b.txt", "c.csv"]
        mock_fs.get_suffix.side_effect = lambda p: "." + p.rsplit(".", 1)[-1]

        mock_future = MagicMock()
        mock_future.result.return_value = {
            "status": "success",
            "file": "x",
            "rows": 1,
            "expectations": 1,
        }

        mock_executor = MagicMock()
        mock_executor.__enter__ = MagicMock(return_value=mock_executor)
        mock_executor.__exit__ = MagicMock(return_value=False)
        mock_executor.submit.return_value = mock_future

        mocker.patch("concurrent.futures.ProcessPoolExecutor", return_value=mock_executor)
        mocker.patch(
            "concurrent.futures.as_completed",
            return_value=iter([mock_future, mock_future]),
        )

        BatchProfiler.run_parallel_profiling("input_dir", "output_dir")

        # Only 2 files submitted (a.parquet, c.csv), not b.txt
        assert mock_executor.submit.call_count == 2
        submitted_files = [call.args[1] for call in mock_executor.submit.call_args_list]
        assert "b.txt" not in submitted_files
