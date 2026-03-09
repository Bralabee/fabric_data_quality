"""Tests for dq_framework.loader -- pytest style, no unittest.TestCase."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from dq_framework.loader import DataLoader

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_PATCH_EXISTS = "dq_framework.utils.FileSystemHandler.exists"
_PATCH_IS_ABFSS = "dq_framework.utils.FileSystemHandler.is_abfss"
_PATCH_GET_SUFFIX = "dq_framework.utils.FileSystemHandler.get_suffix"


def _stub_fs(mocker, *, exists=True, is_abfss=False, suffix=".csv"):
    """Patch FileSystemHandler methods and return the mocks."""
    m_exists = mocker.patch(_PATCH_EXISTS, return_value=exists)
    m_abfss = mocker.patch(_PATCH_IS_ABFSS, return_value=is_abfss)
    m_suffix = mocker.patch(_PATCH_GET_SUFFIX, return_value=suffix)
    return m_exists, m_abfss, m_suffix


# ===== CSV tests ==========================================================


class TestLoadCSV:
    """CSV loading via DataLoader.load_data."""

    def test_load_csv(self, mocker):
        """Existing behaviour: simple CSV load succeeds."""
        _stub_fs(mocker, suffix=".csv")
        mocker.patch("pandas.read_csv", return_value=pd.DataFrame({"a": [1, 2, 3]}))

        df = DataLoader.load_data("test.csv")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_load_csv_encoding_fallback(self, mocker):
        """UTF-8 fails, latin-1 succeeds -- loader retries encodings."""
        _stub_fs(mocker, suffix=".csv")
        mock_read = mocker.patch("pandas.read_csv")
        # First call (utf-8) raises, second (latin-1) succeeds
        mock_read.side_effect = [
            UnicodeDecodeError("utf-8", b"", 0, 1, "bad"),
            pd.DataFrame({"x": [1]}),
        ]

        df = DataLoader.load_data("enc.csv")
        assert len(df) == 1
        assert mock_read.call_count == 2

    def test_load_csv_all_encodings_fail(self, mocker):
        """All encoding attempts fail -- raises ValueError."""
        _stub_fs(mocker, suffix=".csv")
        mocker.patch(
            "pandas.read_csv",
            side_effect=UnicodeDecodeError("utf-8", b"", 0, 1, "bad"),
        )

        with pytest.raises(ValueError, match="Could not detect file encoding"):
            DataLoader.load_data("bad_enc.csv")


# ===== Parquet tests =======================================================


class TestLoadParquet:
    def test_load_parquet_simple(self, mocker):
        """Existing behaviour: parquet without pyarrow falls back to pandas."""
        _stub_fs(mocker, suffix=".parquet")
        mocker.patch("pandas.read_parquet", return_value=pd.DataFrame({"a": [1, 2, 3]}))
        mocker.patch("dq_framework.loader.PYARROW_AVAILABLE", False)

        df = DataLoader.load_data("test.parquet")
        assert isinstance(df, pd.DataFrame)

    def test_load_parquet_with_pyarrow_sampling(self, mocker):
        """When pyarrow is available and sample_size given, use PyArrow batch path."""
        _stub_fs(mocker, suffix=".parquet")
        mocker.patch("dq_framework.loader.PYARROW_AVAILABLE", True)

        mock_batch = MagicMock()
        mock_batch.num_rows = 5

        mock_table = MagicMock()
        mock_table.to_pandas.return_value = pd.DataFrame({"a": range(5)})

        mock_pf = MagicMock()
        mock_pf.iter_batches.return_value = iter([mock_batch])

        mocker.patch("dq_framework.loader.pq.ParquetFile", return_value=mock_pf)

        # pa.Table is a C extension -- mock the whole pa module attribute
        mock_pa = MagicMock()
        mock_pa.Table.from_batches.return_value = mock_table
        mocker.patch.dict("sys.modules", {"pyarrow": mock_pa})
        import dq_framework.loader as _loader_mod

        orig_pa = _loader_mod.pa
        _loader_mod.pa = mock_pa
        try:
            df = DataLoader.load_data("test.parquet", sample_size=5)
        finally:
            _loader_mod.pa = orig_pa

        assert len(df) == 5
        mock_pf.iter_batches.assert_called_once_with(batch_size=5)

    def test_load_parquet_pyarrow_fallback(self, mocker):
        """PyArrow raises an error -- falls back to pd.read_parquet with warning."""
        _stub_fs(mocker, suffix=".parquet")
        mocker.patch("dq_framework.loader.PYARROW_AVAILABLE", True)
        mocker.patch(
            "dq_framework.loader.pq.ParquetFile",
            side_effect=Exception("pyarrow broke"),
        )
        mock_read = mocker.patch(
            "pandas.read_parquet",
            return_value=pd.DataFrame({"a": range(10)}),
        )

        df = DataLoader.load_data("test.parquet", sample_size=5)
        assert len(df) == 5
        mock_read.assert_called_once()

    def test_load_parquet_abfss_skips_pyarrow(self, mocker):
        """ABFSS paths must NOT use PyArrow batch reading."""
        _stub_fs(mocker, is_abfss=True, suffix=".parquet")
        mocker.patch("dq_framework.loader.PYARROW_AVAILABLE", True)
        mock_read = mocker.patch(
            "pandas.read_parquet",
            return_value=pd.DataFrame({"a": range(10)}),
        )

        df = DataLoader.load_data("abfss://container@account/test.parquet", sample_size=5)
        assert len(df) == 5
        mock_read.assert_called_once()


# ===== Excel test ==========================================================


class TestLoadExcel:
    def test_load_excel(self, mocker):
        _stub_fs(mocker, suffix=".xlsx")
        mock_read = mocker.patch(
            "pandas.read_excel",
            return_value=pd.DataFrame({"b": [10, 20]}),
        )

        df = DataLoader.load_data("report.xlsx", sample_size=50)
        assert isinstance(df, pd.DataFrame)
        mock_read.assert_called_once_with("report.xlsx", nrows=50)


# ===== JSON test ===========================================================


class TestLoadJSON:
    def test_load_json(self, mocker):
        _stub_fs(mocker, suffix=".json")
        mock_read = mocker.patch(
            "pandas.read_json",
            return_value=pd.DataFrame({"c": range(20)}),
        )

        df = DataLoader.load_data("data.json", sample_size=5)
        assert len(df) == 5
        mock_read.assert_called_once()


# ===== Error tests =========================================================


class TestLoadErrors:
    def test_load_unsupported_format(self, mocker):
        _stub_fs(mocker, suffix=".xyz")

        with pytest.raises(ValueError, match="Unsupported file format"):
            DataLoader.load_data("file.xyz")

    def test_load_file_not_found(self, mocker):
        _stub_fs(mocker, exists=False, suffix=".csv")

        with pytest.raises(FileNotFoundError, match="File not found"):
            DataLoader.load_data("missing.csv")


# ===== Large-file auto-sample ==============================================


class TestLargeFileAutoSample:
    def test_large_file_auto_sample(self, mocker):
        """Existing behaviour: files >500 MB trigger auto-sample to 100k rows."""
        _stub_fs(mocker, suffix=".csv")

        mock_stat_obj = MagicMock()
        mock_stat_obj.st_size = 600 * 1024 * 1024  # 600 MB
        mocker.patch("pathlib.Path.stat", return_value=mock_stat_obj)

        mock_read = mocker.patch("pandas.read_csv", return_value=pd.DataFrame({"a": [1]}))

        DataLoader.load_data("large.csv")

        _, kwargs = mock_read.call_args
        assert kwargs.get("nrows") == 100000
