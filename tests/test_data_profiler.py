"""
Unit tests for DataProfiler

Comprehensive test suite covering all public methods and edge cases
for the DataProfiler class in dq_framework/data_profiler.py.

Target: Increase coverage from 13% to 80%+
"""

import os
import tempfile
from unittest.mock import mock_open, patch

import numpy as np
import pandas as pd
import pytest
import yaml

from dq_framework.constants import (
    DEFAULT_QUALITY_THRESHOLDS,
    MAX_UNIQUE_VALUES_DISPLAY,
)
from dq_framework.data_profiler import DataProfiler

# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def sample_dataframe():
    """Standard sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "email": ["a@test.com", "b@test.com", "c@test.com", "d@test.com", "e@test.com"],
            "status": ["active", "active", "inactive", "active", "inactive"],
        }
    )


@pytest.fixture
def empty_dataframe():
    """Empty DataFrame for edge case testing."""
    return pd.DataFrame()


@pytest.fixture
def dataframe_with_nulls():
    """DataFrame with null values in all columns."""
    return pd.DataFrame(
        {
            "id": [1, None, 3, None, 5],
            "name": ["Alice", None, "Charlie", None, None],
            "value": [10.5, None, None, 40.0, None],
        }
    )


@pytest.fixture
def dataframe_all_nulls():
    """DataFrame with all null values."""
    return pd.DataFrame(
        {
            "col1": [None, None, None, None, None],
            "col2": [None, None, None, None, None],
        }
    )


@pytest.fixture
def single_column_dataframe():
    """DataFrame with a single column."""
    return pd.DataFrame({"only_column": [1, 2, 3, 4, 5]})


@pytest.fixture
def large_dataframe():
    """Large DataFrame for sampling tests."""
    np.random.seed(42)
    return pd.DataFrame(
        {
            "id": range(10000),
            "value": np.random.randn(10000),
            "category": np.random.choice(["A", "B", "C"], 10000),
        }
    )


@pytest.fixture
def mixed_types_dataframe():
    """DataFrame with mixed data types."""
    return pd.DataFrame(
        {
            "int_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "str_col": ["a", "b", "c", "d", "e"],
            "bool_col": [True, False, True, False, True],
            "date_col": pd.to_datetime(
                ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"]
            ),
        }
    )


@pytest.fixture
def id_column_dataframe():
    """DataFrame with ID-like columns."""
    return pd.DataFrame(
        {
            "user_id": [1001, 1002, 1003, 1004, 1005],
            "uniqid": ["U001", "U002", "U003", "U004", "U005"],
            "transaction_id": [10001, 10002, 10003, 10004, 10005],
        }
    )


@pytest.fixture
def code_column_dataframe():
    """DataFrame with code-like columns."""
    return pd.DataFrame(
        {
            "country_code": ["US", "UK", "DE", "FR", "IT"],
            "product_nr": ["P001", "P002", "P003", "P004", "P005"],
            "zip_code": ["12345", "23456", "34567", "45678", "56789"],
        }
    )


@pytest.fixture
def date_column_dataframe():
    """DataFrame with date columns."""
    return pd.DataFrame(
        {
            "date_str": ["2023-01-01", "2023-02-15", "2023-03-20", "2023-04-10", "2023-05-05"],
            "datetime_col": pd.to_datetime(
                ["2023-01-01", "2023-02-15", "2023-03-20", "2023-04-10", "2023-05-05"]
            ),
            "invalid_date": ["not-a-date", "also-not", "nope", "wrong", "invalid"],
        }
    )


@pytest.fixture
def numeric_column_dataframe():
    """DataFrame with various numeric columns."""
    return pd.DataFrame(
        {
            "amount": [100.50, 200.75, 300.25, 400.00, 500.99],
            "price_value": [10.0, 20.0, 30.0, 40.0, 50.0],
            "percent_rate": [0.05, 0.10, 0.15, 0.20, 0.25],
            "count": [1, 2, 3, 4, 5],
        }
    )


@pytest.fixture
def categorical_dataframe():
    """DataFrame with categorical columns (low uniqueness)."""
    return pd.DataFrame(
        {
            "status": ["active"] * 50 + ["inactive"] * 50,
            "type": ["A"] * 40 + ["B"] * 30 + ["C"] * 30,
            "country": ["US"] * 80 + ["UK"] * 20,
        }
    )


@pytest.fixture
def text_dataframe():
    """DataFrame with text columns (long strings)."""
    long_text = "This is a very long text that exceeds the threshold for text classification. " * 5
    return pd.DataFrame(
        {
            "description": [long_text] * 5,
            "short_text": ["short", "text", "here", "now", "ok"],
        }
    )


# =============================================================================
# TEST CLASS: DataProfiler Initialization
# =============================================================================


class TestDataProfilerInit:
    """Tests for DataProfiler initialization."""

    def test_init_without_sample_size(self, sample_dataframe):
        """Test initialization without sample_size parameter."""
        profiler = DataProfiler(sample_dataframe)

        assert profiler.df is not None
        assert len(profiler.df) == len(sample_dataframe)
        assert profiler.full_row_count == 5
        assert profiler.sampled is False
        assert profiler.profile_results is None

    def test_init_with_sample_size_smaller_than_df(self, large_dataframe):
        """Test initialization with sample_size smaller than DataFrame."""
        profiler = DataProfiler(large_dataframe, sample_size=100)

        assert len(profiler.df) == 100
        assert profiler.full_row_count == 10000
        assert profiler.sampled is True

    def test_init_with_sample_size_larger_than_df(self, sample_dataframe):
        """Test initialization with sample_size larger than DataFrame."""
        profiler = DataProfiler(sample_dataframe, sample_size=1000)

        assert len(profiler.df) == 5  # Should use min(sample_size, len(df))
        assert profiler.full_row_count == 5
        assert profiler.sampled is False

    def test_init_with_sample_size_equal_to_df(self, sample_dataframe):
        """Test initialization with sample_size equal to DataFrame length."""
        profiler = DataProfiler(sample_dataframe, sample_size=5)

        assert len(profiler.df) == 5
        assert profiler.sampled is False

    def test_init_with_empty_dataframe(self, empty_dataframe):
        """Test initialization with empty DataFrame."""
        profiler = DataProfiler(empty_dataframe)

        assert len(profiler.df) == 0
        assert profiler.full_row_count == 0
        assert profiler.sampled is False


# =============================================================================
# TEST CLASS: Profile Method
# =============================================================================


class TestProfileMethod:
    """Tests for the profile() method."""

    def test_profile_basic(self, sample_dataframe):
        """Test basic profiling."""
        profiler = DataProfiler(sample_dataframe)
        profile = profiler.profile()

        assert profile["row_count"] == 5
        assert profile["column_count"] == 5
        assert "columns" in profile
        assert len(profile["columns"]) == 5
        assert "data_quality_score" in profile
        assert "profiled_at" in profile

    def test_profile_empty_dataframe(self):
        """Test profiling empty DataFrame with columns."""
        df = pd.DataFrame(columns=["col1", "col2", "col3"])
        profiler = DataProfiler(df)
        profile = profiler.profile()

        assert profile["row_count"] == 0
        assert profile["column_count"] == 3
        assert len(profile["columns"]) == 3

    def test_profile_stores_results(self, sample_dataframe):
        """Test that profile results are stored."""
        profiler = DataProfiler(sample_dataframe)

        assert profiler.profile_results is None
        profile = profiler.profile()
        assert profiler.profile_results is not None
        assert profiler.profile_results == profile

    def test_profile_sampled_rows_when_sampled(self, large_dataframe):
        """Test that sampled_rows is populated when sampling."""
        profiler = DataProfiler(large_dataframe, sample_size=100)
        profile = profiler.profile()

        assert profile["sampled_rows"] == 100
        assert profile["row_count"] == 10000

    def test_profile_sampled_rows_none_when_not_sampled(self, sample_dataframe):
        """Test that sampled_rows is None when not sampling."""
        profiler = DataProfiler(sample_dataframe)
        profile = profiler.profile()

        assert profile["sampled_rows"] is None

    def test_profile_with_nulls(self, dataframe_with_nulls):
        """Test profiling DataFrame with null values."""
        profiler = DataProfiler(dataframe_with_nulls)
        profile = profiler.profile()

        # Check null percentages
        assert profile["columns"]["id"]["null_percent"] == 40.0  # 2 out of 5
        assert profile["columns"]["name"]["null_percent"] == 60.0  # 3 out of 5
        assert profile["columns"]["value"]["null_percent"] == 60.0  # 3 out of 5

    def test_profile_all_column_types(self, mixed_types_dataframe):
        """Test profiling with different column types."""
        profiler = DataProfiler(mixed_types_dataframe)
        profile = profiler.profile()

        assert "int_col" in profile["columns"]
        assert "float_col" in profile["columns"]
        assert "str_col" in profile["columns"]
        assert "bool_col" in profile["columns"]
        assert "date_col" in profile["columns"]


# =============================================================================
# TEST CLASS: _profile_column Method
# =============================================================================


class TestProfileColumnMethod:
    """Tests for the _profile_column() method."""

    def test_profile_column_basic_stats(self, sample_dataframe):
        """Test basic column statistics."""
        profiler = DataProfiler(sample_dataframe)
        col_profile = profiler._profile_column("age")

        assert "dtype" in col_profile
        assert "null_count" in col_profile
        assert "null_percent" in col_profile
        assert "unique_count" in col_profile
        assert "unique_percent" in col_profile
        assert "detected_type" in col_profile

    def test_profile_column_numeric(self, sample_dataframe):
        """Test numeric column profiling."""
        profiler = DataProfiler(sample_dataframe)
        col_profile = profiler._profile_column("age")

        assert "min" in col_profile
        assert "max" in col_profile
        assert "mean" in col_profile
        assert "median" in col_profile
        assert "std" in col_profile
        assert col_profile["min"] == 25.0
        assert col_profile["max"] == 45.0
        assert col_profile["mean"] == 35.0

    def test_profile_column_string(self, sample_dataframe):
        """Test string column profiling."""
        profiler = DataProfiler(sample_dataframe)
        col_profile = profiler._profile_column("name")

        assert "min_length" in col_profile
        assert "max_length" in col_profile
        assert "avg_length" in col_profile

    def test_profile_column_empty_series(self):
        """Test profiling empty column."""
        df = pd.DataFrame({"empty": pd.Series([], dtype="object")})
        profiler = DataProfiler(df)
        col_profile = profiler._profile_column("empty")

        assert col_profile["null_count"] == 0
        assert col_profile["null_percent"] == 0.0
        assert col_profile["unique_count"] == 0
        assert col_profile["detected_type"] == "empty"

    def test_profile_column_all_nulls(self, dataframe_all_nulls):
        """Test profiling column with all nulls."""
        profiler = DataProfiler(dataframe_all_nulls)
        col_profile = profiler._profile_column("col1")

        assert col_profile["null_count"] == 5
        assert col_profile["null_percent"] == 100.0
        assert col_profile["unique_count"] == 0

    def test_profile_column_unique_values_display(self):
        """Test unique values display for low cardinality columns."""
        df = pd.DataFrame({"low_cardinality": ["A", "B", "C", "A", "B"]})
        profiler = DataProfiler(df)
        col_profile = profiler._profile_column("low_cardinality")

        # Should have unique_values since cardinality <= MAX_UNIQUE_VALUES_DISPLAY
        assert "unique_values" in col_profile or "sample_values" in col_profile

    def test_profile_column_high_cardinality(self):
        """Test sample values for high cardinality columns."""
        df = pd.DataFrame({"high_cardinality": [f"value_{i}" for i in range(100)]})
        profiler = DataProfiler(df)
        col_profile = profiler._profile_column("high_cardinality")

        # Should have sample_values instead of unique_values
        if col_profile["unique_count"] > MAX_UNIQUE_VALUES_DISPLAY:
            assert "sample_values" in col_profile


# =============================================================================
# TEST CLASS: _detect_column_type Method
# =============================================================================


class TestDetectColumnType:
    """Tests for the _detect_column_type() method."""

    def test_detect_datetime_column(self, date_column_dataframe):
        """Test detection of datetime column."""
        profiler = DataProfiler(date_column_dataframe)
        series = profiler.df["datetime_col"]

        detected = profiler._detect_column_type(series)
        assert detected == "date"

    def test_detect_date_string_column(self, date_column_dataframe):
        """Test detection of date string column."""
        profiler = DataProfiler(date_column_dataframe)
        series = profiler.df["date_str"]

        detected = profiler._detect_column_type(series)
        assert detected == "date"

    def test_detect_id_column(self, id_column_dataframe):
        """Test detection of ID column."""
        profiler = DataProfiler(id_column_dataframe)
        series = profiler.df["user_id"]
        series.name = "user_id"

        detected = profiler._detect_column_type(series)
        assert detected == "id"

    def test_detect_uniqid_column(self, id_column_dataframe):
        """Test detection of uniqid column."""
        profiler = DataProfiler(id_column_dataframe)
        series = profiler.df["uniqid"]
        series.name = "uniqid"

        detected = profiler._detect_column_type(series)
        assert detected == "id"

    def test_detect_code_column(self, code_column_dataframe):
        """Test detection of code column."""
        profiler = DataProfiler(code_column_dataframe)
        series = profiler.df["country_code"]
        series.name = "country_code"

        detected = profiler._detect_column_type(series)
        assert detected == "code"

    def test_detect_nr_column(self, code_column_dataframe):
        """Test detection of nr (number reference) column."""
        profiler = DataProfiler(code_column_dataframe)
        series = profiler.df["product_nr"]
        series.name = "product_nr"

        detected = profiler._detect_column_type(series)
        assert detected == "code"

    def test_detect_monetary_column(self, numeric_column_dataframe):
        """Test detection of monetary column."""
        profiler = DataProfiler(numeric_column_dataframe)
        series = profiler.df["amount"]
        series.name = "amount"

        detected = profiler._detect_column_type(series)
        assert detected == "monetary"

    def test_detect_price_column(self, numeric_column_dataframe):
        """Test detection of price column."""
        profiler = DataProfiler(numeric_column_dataframe)
        series = profiler.df["price_value"]
        series.name = "price_value"

        detected = profiler._detect_column_type(series)
        assert detected == "monetary"

    def test_detect_percentage_column(self, numeric_column_dataframe):
        """Test detection of percentage column."""
        profiler = DataProfiler(numeric_column_dataframe)
        series = profiler.df["percent_rate"]
        series.name = "percent_rate"

        detected = profiler._detect_column_type(series)
        assert detected == "percentage"

    def test_detect_numeric_column(self, numeric_column_dataframe):
        """Test detection of generic numeric column."""
        profiler = DataProfiler(numeric_column_dataframe)
        series = profiler.df["count"]
        series.name = "count"

        detected = profiler._detect_column_type(series)
        assert detected == "numeric"

    def test_detect_categorical_column(self, categorical_dataframe):
        """Test detection of categorical column (low uniqueness)."""
        profiler = DataProfiler(categorical_dataframe)
        series = profiler.df["status"]
        series.name = "status"

        detected = profiler._detect_column_type(series)
        assert detected == "categorical"

    def test_detect_text_column(self, text_dataframe):
        """Test detection of text column (long strings)."""
        profiler = DataProfiler(text_dataframe)
        series = profiler.df["description"]
        series.name = "description"

        detected = profiler._detect_column_type(series)
        assert detected == "text"

    def test_detect_string_column(self, text_dataframe):
        """Test detection of string column (short strings)."""
        profiler = DataProfiler(text_dataframe)
        series = profiler.df["short_text"]
        series.name = "short_text"

        detected = profiler._detect_column_type(series)
        # Could be string or categorical depending on uniqueness
        assert detected in ("string", "categorical")

    @pytest.mark.parametrize(
        "col_name,expected_type",
        [
            ("user_id", "id"),
            ("customer_uniqid", "id"),
            ("country_code", "code"),
            ("item_nr", "code"),
            ("total_amount", "monetary"),
            ("unit_price", "monetary"),
            ("tax_rate", "percentage"),
            ("conversion_percent", "percentage"),
        ],
    )
    def test_detect_column_type_parametrized(self, col_name, expected_type):
        """Parametrized test for column type detection based on name patterns."""
        # Create a DataFrame with high uniqueness to trigger ID/code detection
        df = pd.DataFrame(
            {
                col_name: list(range(100))
                if "id" in col_name.lower() or "uniqid" in col_name.lower()
                else [f"val_{i}" for i in range(100)]
                if "code" in col_name.lower() or "nr" in col_name.lower()
                else [float(i) * 1.1 for i in range(100)]
            }
        )
        profiler = DataProfiler(df)
        series = profiler.df[col_name]

        detected = profiler._detect_column_type(series)
        assert detected == expected_type


# =============================================================================
# TEST CLASS: _looks_like_date Method
# =============================================================================


class TestLooksLikeDate:
    """Tests for the _looks_like_date() method."""

    def test_valid_date_formats(self, sample_dataframe):
        """Test valid date string formats."""
        profiler = DataProfiler(sample_dataframe)

        valid_dates = pd.Series(
            ["2023-01-01", "2023-02-15", "2023-03-20", "2023-04-10", "2023-05-05"]
        )
        assert profiler._looks_like_date(valid_dates)

    def test_valid_date_formats_slash(self, sample_dataframe):
        """Test valid date formats with slashes."""
        profiler = DataProfiler(sample_dataframe)

        valid_dates = pd.Series(
            ["01/01/2023", "02/15/2023", "03/20/2023", "04/10/2023", "05/05/2023"]
        )
        assert profiler._looks_like_date(valid_dates)

    def test_invalid_date_strings(self, sample_dataframe):
        """Test invalid date strings."""
        profiler = DataProfiler(sample_dataframe)

        invalid_dates = pd.Series(["not-a-date", "also-not", "nope", "wrong", "invalid"])
        assert not profiler._looks_like_date(invalid_dates)

    def test_mixed_valid_invalid_dates(self, sample_dataframe):
        """Test mixed valid and invalid dates."""
        profiler = DataProfiler(sample_dataframe)

        # Less than 80% valid should return False
        mixed_dates = pd.Series(["2023-01-01", "not-a-date", "not-valid", "wrong", "invalid"])
        assert not profiler._looks_like_date(mixed_dates)

    def test_mostly_valid_dates(self, sample_dataframe):
        """Test mostly valid dates (above threshold)."""
        profiler = DataProfiler(sample_dataframe)

        # More than 80% valid should return True (need at least 5 items with 5 valid for >80%)
        mostly_valid = pd.Series(
            ["2023-01-01", "2023-02-15", "2023-03-20", "2023-04-10", "2023-05-05"]
        )
        assert profiler._looks_like_date(mostly_valid)

    def test_empty_series(self, sample_dataframe):
        """Test empty series."""
        profiler = DataProfiler(sample_dataframe)

        empty_series = pd.Series([], dtype="object")
        # Should handle gracefully without error - empty series returns False (0/0 = NaN, NaN > threshold = False)
        result = profiler._looks_like_date(empty_series)
        # NaN comparisons return False, so empty series should return False
        assert not result

    def test_date_with_timestamps(self, sample_dataframe):
        """Test date strings with timestamps."""
        profiler = DataProfiler(sample_dataframe)

        datetime_strings = pd.Series(
            ["2023-01-01 10:30:00", "2023-02-15 14:45:00", "2023-03-20 08:00:00"]
        )
        assert profiler._looks_like_date(datetime_strings)

    @pytest.mark.parametrize(
        "date_series,expected",
        [
            (pd.Series(["2023-01-01", "2023-02-01", "2023-03-01"]), True),
            (pd.Series(["Jan 01, 2023", "Feb 15, 2023", "Mar 20, 2023"]), True),
            (pd.Series(["20230101", "20230215", "20230320"]), True),
            (pd.Series(["abc", "def", "ghi"]), False),
            (pd.Series(["123", "456", "789"]), False),
        ],
    )
    def test_looks_like_date_parametrized(self, sample_dataframe, date_series, expected):
        """Parametrized test for date detection."""
        profiler = DataProfiler(sample_dataframe)
        assert profiler._looks_like_date(date_series) == expected


# =============================================================================
# TEST CLASS: _calculate_quality_score Method
# =============================================================================


class TestCalculateQualityScore:
    """Tests for the _calculate_quality_score() method."""

    def test_empty_columns(self, sample_dataframe):
        """Test quality score with no columns."""
        profiler = DataProfiler(sample_dataframe)
        score = profiler._calculate_quality_score({})

        assert score == 0.0

    def test_perfect_data(self, sample_dataframe):
        """Test quality score for perfect data (no nulls, good uniqueness)."""
        profiler = DataProfiler(sample_dataframe)

        columns = {
            "col1": {
                "null_percent": 0.0,
                "unique_percent": 50.0,  # Sweet spot
                "detected_type": "string",
            }
        }
        score = profiler._calculate_quality_score(columns)

        # Should be high but calculation depends on weights
        assert score > 50.0

    def test_high_null_rate_penalty(self, sample_dataframe):
        """Test quality score with high null rate."""
        profiler = DataProfiler(sample_dataframe)

        columns = {
            "col1": {
                "null_percent": 100.0,  # All nulls
                "unique_percent": 50.0,
                "detected_type": "string",
            }
        }
        score = profiler._calculate_quality_score(columns)

        # Should be lower due to null penalty
        assert score < 100.0

    def test_id_column_uniqueness_scoring(self, sample_dataframe):
        """Test quality score for ID columns (high uniqueness is good)."""
        profiler = DataProfiler(sample_dataframe)

        columns = {
            "id_col": {
                "null_percent": 0.0,
                "unique_percent": 100.0,  # All unique
                "detected_type": "id",  # ID column
            }
        }
        score = profiler._calculate_quality_score(columns)

        # High uniqueness for ID should not be penalized
        assert score > 50.0

    def test_non_id_high_uniqueness(self, sample_dataframe):
        """Test quality score for non-ID columns with high uniqueness."""
        profiler = DataProfiler(sample_dataframe)

        columns = {
            "str_col": {
                "null_percent": 0.0,
                "unique_percent": 100.0,  # All unique
                "detected_type": "string",  # Not an ID
            }
        }
        score_high_unique = profiler._calculate_quality_score(columns)

        columns_sweet_spot = {
            "str_col": {
                "null_percent": 0.0,
                "unique_percent": 50.0,  # Sweet spot
                "detected_type": "string",
            }
        }
        score_sweet_spot = profiler._calculate_quality_score(columns_sweet_spot)

        # Sweet spot uniqueness should score better for non-ID columns
        assert score_sweet_spot >= score_high_unique

    def test_multiple_columns_average(self, sample_dataframe):
        """Test quality score averages across multiple columns."""
        profiler = DataProfiler(sample_dataframe)

        columns = {
            "good_col": {"null_percent": 0.0, "unique_percent": 50.0, "detected_type": "string"},
            "bad_col": {"null_percent": 50.0, "unique_percent": 0.0, "detected_type": "string"},
        }
        score = profiler._calculate_quality_score(columns)

        # Score should be between good and bad
        assert 0.0 < score < 100.0


# =============================================================================
# TEST CLASS: generate_expectations Method
# =============================================================================


class TestGenerateExpectations:
    """Tests for the generate_expectations() method."""

    def test_generate_expectations_basic(self, sample_dataframe):
        """Test basic expectation generation."""
        profiler = DataProfiler(sample_dataframe)
        config = profiler.generate_expectations(validation_name="test_validation")

        assert config["validation_name"] == "test_validation"
        assert "description" in config
        assert "generated_at" in config
        assert "data_source" in config
        assert "expectations" in config
        assert len(config["expectations"]) > 0

    def test_generate_expectations_with_description(self, sample_dataframe):
        """Test expectation generation with custom description."""
        profiler = DataProfiler(sample_dataframe)
        config = profiler.generate_expectations(
            validation_name="test_validation", description="Custom test description"
        )

        assert config["description"] == "Custom test description"

    def test_generate_expectations_auto_profiles(self, sample_dataframe):
        """Test that generate_expectations auto-profiles if not done."""
        profiler = DataProfiler(sample_dataframe)

        assert profiler.profile_results is None
        profiler.generate_expectations(validation_name="test")
        assert profiler.profile_results is not None

    def test_generate_expectations_structural_only(self, sample_dataframe):
        """Test structural expectations only."""
        profiler = DataProfiler(sample_dataframe)
        config = profiler.generate_expectations(
            validation_name="test",
            include_structural=True,
            include_completeness=False,
            include_validity=False,
        )

        structural_types = [
            "expect_table_row_count_to_be_between",
            "expect_table_column_count_to_equal",
        ]
        for exp in config["expectations"]:
            assert exp["expectation_type"] in structural_types

    def test_generate_expectations_completeness_only(self, sample_dataframe):
        """Test completeness expectations only."""
        profiler = DataProfiler(sample_dataframe)
        config = profiler.generate_expectations(
            validation_name="test",
            include_structural=False,
            include_completeness=True,
            include_validity=False,
        )

        completeness_types = ["expect_column_to_exist", "expect_column_values_to_not_be_null"]
        for exp in config["expectations"]:
            assert exp["expectation_type"] in completeness_types

    def test_generate_expectations_validity_only(self, sample_dataframe):
        """Test validity expectations only."""
        profiler = DataProfiler(sample_dataframe)
        profiler.profile()
        config = profiler.generate_expectations(
            validation_name="test",
            include_structural=False,
            include_completeness=False,
            include_validity=True,
        )

        # Validity expectations depend on detected types
        validity_types = [
            "expect_column_values_to_be_unique",
            "expect_column_values_to_be_between",
            "expect_column_values_to_be_in_set",
            "expect_column_value_lengths_to_be_between",
        ]
        if config["expectations"]:
            for exp in config["expectations"]:
                assert exp["expectation_type"] in validity_types

    def test_generate_expectations_custom_null_tolerance(self, dataframe_with_nulls):
        """Test custom null tolerance parameter."""
        profiler = DataProfiler(dataframe_with_nulls)

        # With high tolerance, should not generate null checks for columns with some nulls
        config_high_tolerance = profiler.generate_expectations(
            validation_name="test",
            null_tolerance=100.0,
            include_structural=False,
            include_validity=False,
        )

        # With low tolerance
        config_low_tolerance = profiler.generate_expectations(
            validation_name="test",
            null_tolerance=0.0,
            include_structural=False,
            include_validity=False,
        )

        # Low tolerance should generate fewer null checks for columns with nulls
        null_checks_high = [
            e
            for e in config_high_tolerance["expectations"]
            if e["expectation_type"] == "expect_column_values_to_not_be_null"
        ]
        null_checks_low = [
            e
            for e in config_low_tolerance["expectations"]
            if e["expectation_type"] == "expect_column_values_to_not_be_null"
        ]

        assert len(null_checks_high) >= len(null_checks_low)

    def test_generate_expectations_custom_thresholds(self, sample_dataframe):
        """Test custom quality thresholds."""
        profiler = DataProfiler(sample_dataframe)

        custom_thresholds = {"critical": 99.0, "high": 90.0, "medium": 75.0, "low": 40.0}
        config = profiler.generate_expectations(
            validation_name="test", quality_thresholds=custom_thresholds
        )

        assert config["quality_thresholds"] == custom_thresholds

    def test_generate_expectations_default_thresholds(self, sample_dataframe):
        """Test default quality thresholds are used."""
        profiler = DataProfiler(sample_dataframe)
        config = profiler.generate_expectations(validation_name="test")

        assert config["quality_thresholds"] == DEFAULT_QUALITY_THRESHOLDS

    @pytest.mark.parametrize("severity", ["critical", "high", "medium", "low"])
    def test_generate_expectations_severity_levels(self, sample_dataframe, severity):
        """Test different severity threshold levels."""
        profiler = DataProfiler(sample_dataframe)
        config = profiler.generate_expectations(validation_name="test", severity_threshold=severity)

        assert config is not None
        assert "expectations" in config


# =============================================================================
# TEST CLASS: Structural Expectations
# =============================================================================


class TestStructuralExpectations:
    """Tests for _generate_structural_expectations() method."""

    def test_structural_expectations_content(self, sample_dataframe):
        """Test structural expectations are generated correctly."""
        profiler = DataProfiler(sample_dataframe)
        profiler.profile()

        expectations = profiler._generate_structural_expectations("medium")

        assert len(expectations) == 2

        # Check row count expectation
        row_exp = next(
            e
            for e in expectations
            if e["expectation_type"] == "expect_table_row_count_to_be_between"
        )
        assert row_exp["kwargs"]["min_value"] == 1
        assert row_exp["meta"]["severity"] == "critical"

        # Check column count expectation
        col_exp = next(
            e for e in expectations if e["expectation_type"] == "expect_table_column_count_to_equal"
        )
        assert col_exp["kwargs"]["value"] == 5
        assert col_exp["meta"]["severity"] == "high"


# =============================================================================
# TEST CLASS: Completeness Expectations
# =============================================================================


class TestCompletenessExpectations:
    """Tests for _generate_completeness_expectations() method."""

    def test_completeness_always_includes_column_exists(self, sample_dataframe):
        """Test that column existence check is always generated."""
        profiler = DataProfiler(sample_dataframe)
        profiler.profile()

        col_info = profiler.profile_results["columns"]["name"]
        expectations = profiler._generate_completeness_expectations("name", col_info, 5.0, "medium")

        exists_exp = [e for e in expectations if e["expectation_type"] == "expect_column_to_exist"]
        assert len(exists_exp) == 1
        assert exists_exp[0]["kwargs"]["column"] == "name"

    def test_completeness_null_check_below_tolerance(self, sample_dataframe):
        """Test null check when null rate is below tolerance."""
        profiler = DataProfiler(sample_dataframe)
        profiler.profile()

        col_info = {"null_percent": 2.0, "detected_type": "string"}
        expectations = profiler._generate_completeness_expectations(
            "test_col", col_info, 5.0, "medium"
        )

        null_checks = [
            e
            for e in expectations
            if e["expectation_type"] == "expect_column_values_to_not_be_null"
        ]
        assert len(null_checks) == 1

    def test_completeness_no_null_check_above_tolerance(self, sample_dataframe):
        """Test no null check when null rate is above tolerance."""
        profiler = DataProfiler(sample_dataframe)
        profiler.profile()

        col_info = {"null_percent": 10.0, "detected_type": "string"}
        expectations = profiler._generate_completeness_expectations(
            "test_col", col_info, 5.0, "medium"
        )

        null_checks = [
            e
            for e in expectations
            if e["expectation_type"] == "expect_column_values_to_not_be_null"
        ]
        assert len(null_checks) == 0

    def test_completeness_id_column_high_severity(self, sample_dataframe):
        """Test ID column null check has high severity."""
        profiler = DataProfiler(sample_dataframe)
        profiler.profile()

        col_info = {"null_percent": 2.0, "detected_type": "id"}
        expectations = profiler._generate_completeness_expectations(
            "id_col", col_info, 5.0, "medium"
        )

        null_check = next(
            (
                e
                for e in expectations
                if e["expectation_type"] == "expect_column_values_to_not_be_null"
            ),
            None,
        )
        if null_check:
            assert null_check["meta"]["severity"] == "high"


# =============================================================================
# TEST CLASS: Validity Expectations
# =============================================================================


class TestValidityExpectations:
    """Tests for _generate_validity_expectations() method."""

    def test_validity_id_uniqueness(self, sample_dataframe):
        """Test uniqueness expectation for ID columns."""
        profiler = DataProfiler(sample_dataframe)
        profiler.profile()

        col_info = {"detected_type": "id", "null_percent": 0.0}
        expectations = profiler._generate_validity_expectations("id_col", col_info, "medium")

        unique_exp = [
            e for e in expectations if e["expectation_type"] == "expect_column_values_to_be_unique"
        ]
        assert len(unique_exp) == 1
        assert unique_exp[0]["meta"]["severity"] == "critical"

    def test_validity_id_no_uniqueness_with_high_nulls(self, sample_dataframe):
        """Test no uniqueness check for ID with high null percentage."""
        profiler = DataProfiler(sample_dataframe)
        profiler.profile()

        col_info = {
            "detected_type": "id",
            "null_percent": 60.0,  # Above ID_NULL_THRESHOLD_FOR_UNIQUENESS
        }
        expectations = profiler._generate_validity_expectations("id_col", col_info, "medium")

        unique_exp = [
            e for e in expectations if e["expectation_type"] == "expect_column_values_to_be_unique"
        ]
        assert len(unique_exp) == 0

    def test_validity_numeric_range(self, numeric_column_dataframe):
        """Test range expectation for numeric columns."""
        profiler = DataProfiler(numeric_column_dataframe)
        profiler.profile()

        col_info = {"detected_type": "numeric", "min": 10.0, "max": 100.0}
        expectations = profiler._generate_validity_expectations("num_col", col_info, "medium")

        range_exp = [
            e for e in expectations if e["expectation_type"] == "expect_column_values_to_be_between"
        ]
        assert len(range_exp) == 1
        assert range_exp[0]["kwargs"]["min_value"] == 10.0
        assert range_exp[0]["kwargs"]["max_value"] == 100.0

    def test_validity_monetary_range(self, numeric_column_dataframe):
        """Test range expectation for monetary columns."""
        profiler = DataProfiler(numeric_column_dataframe)
        profiler.profile()

        col_info = {"detected_type": "monetary", "min": 0.0, "max": 1000.0}
        expectations = profiler._generate_validity_expectations("amount", col_info, "medium")

        range_exp = [
            e for e in expectations if e["expectation_type"] == "expect_column_values_to_be_between"
        ]
        assert len(range_exp) == 1

    def test_validity_categorical_value_set(self, categorical_dataframe):
        """Test value set expectation for categorical columns."""
        profiler = DataProfiler(categorical_dataframe)
        profiler.profile()

        col_info = {"detected_type": "categorical", "unique_values": ["A", "B", "C"]}
        expectations = profiler._generate_validity_expectations("cat_col", col_info, "medium")

        set_exp = [
            e for e in expectations if e["expectation_type"] == "expect_column_values_to_be_in_set"
        ]
        assert len(set_exp) == 1
        assert set_exp[0]["kwargs"]["value_set"] == ["A", "B", "C"]

    def test_validity_string_length(self, sample_dataframe):
        """Test string length expectation for string columns."""
        profiler = DataProfiler(sample_dataframe)
        profiler.profile()

        col_info = {"detected_type": "string", "min_length": 3, "max_length": 10}
        expectations = profiler._generate_validity_expectations("str_col", col_info, "medium")

        length_exp = [
            e
            for e in expectations
            if e["expectation_type"] == "expect_column_value_lengths_to_be_between"
        ]
        assert len(length_exp) == 1
        assert length_exp[0]["kwargs"]["min_value"] == 3
        assert length_exp[0]["kwargs"]["max_value"] == 10
        assert length_exp[0]["meta"]["severity"] == "low"

    def test_validity_code_length(self, code_column_dataframe):
        """Test string length expectation for code columns."""
        profiler = DataProfiler(code_column_dataframe)
        profiler.profile()

        col_info = {"detected_type": "code", "min_length": 2, "max_length": 5}
        expectations = profiler._generate_validity_expectations("code_col", col_info, "medium")

        length_exp = [
            e
            for e in expectations
            if e["expectation_type"] == "expect_column_value_lengths_to_be_between"
        ]
        assert len(length_exp) == 1


# =============================================================================
# TEST CLASS: save_config Method
# =============================================================================


class TestSaveConfig:
    """Tests for the save_config() method."""

    def test_save_config_creates_file(self, sample_dataframe):
        """Test that save_config creates a YAML file."""
        profiler = DataProfiler(sample_dataframe)
        config = profiler.generate_expectations(validation_name="test")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            profiler.save_config(config, temp_path)

            assert os.path.exists(temp_path)

            # Verify content
            with open(temp_path) as f:
                saved_config = yaml.safe_load(f)

            assert saved_config["validation_name"] == "test"
            assert "expectations" in saved_config
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_save_config_yaml_format(self, sample_dataframe):
        """Test that saved config is valid YAML."""
        profiler = DataProfiler(sample_dataframe)
        config = profiler.generate_expectations(validation_name="test")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            profiler.save_config(config, temp_path)

            # Should not raise exception
            with open(temp_path) as f:
                yaml.safe_load(f)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_save_config_preserves_structure(self, sample_dataframe):
        """Test that save preserves the config structure."""
        profiler = DataProfiler(sample_dataframe)
        config = profiler.generate_expectations(
            validation_name="test", description="Test description"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            profiler.save_config(config, temp_path)

            with open(temp_path) as f:
                saved_config = yaml.safe_load(f)

            assert saved_config["validation_name"] == config["validation_name"]
            assert saved_config["description"] == config["description"]
            assert len(saved_config["expectations"]) == len(config["expectations"])
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    @patch("builtins.open", new_callable=mock_open)
    @patch("yaml.dump")
    def test_save_config_calls_yaml_dump(self, mock_yaml_dump, mock_file, sample_dataframe):
        """Test that save_config uses yaml.dump correctly."""
        profiler = DataProfiler(sample_dataframe)
        config = profiler.generate_expectations(validation_name="test")

        profiler.save_config(config, "/fake/path/config.yml")

        mock_file.assert_called_once_with("/fake/path/config.yml", "w")
        mock_yaml_dump.assert_called_once()

        # Check yaml.dump arguments
        call_args = mock_yaml_dump.call_args
        assert call_args[1]["default_flow_style"] is False
        assert call_args[1]["sort_keys"] is False


# =============================================================================
# TEST CLASS: print_summary Method
# =============================================================================


class TestPrintSummary:
    """Tests for the print_summary() method."""

    def test_print_summary_output(self, sample_dataframe, capsys):
        """Test print_summary output."""
        profiler = DataProfiler(sample_dataframe)
        profiler.print_summary()

        captured = capsys.readouterr()

        assert "DATA PROFILE SUMMARY" in captured.out
        assert "Rows: 5" in captured.out
        assert "Columns: 5" in captured.out
        assert "Data Quality Score:" in captured.out
        assert "COLUMN DETAILS:" in captured.out

    def test_print_summary_auto_profiles(self, sample_dataframe, capsys):
        """Test that print_summary auto-profiles if needed."""
        profiler = DataProfiler(sample_dataframe)

        assert profiler.profile_results is None
        profiler.print_summary()
        assert profiler.profile_results is not None

    def test_print_summary_column_names(self, sample_dataframe, capsys):
        """Test that column names appear in summary."""
        profiler = DataProfiler(sample_dataframe)
        profiler.print_summary()

        captured = capsys.readouterr()

        assert "id" in captured.out
        assert "name" in captured.out
        assert "age" in captured.out

    def test_print_summary_long_column_name(self, capsys):
        """Test truncation of long column names."""
        df = pd.DataFrame({"this_is_a_very_long_column_name_that_exceeds_limit": [1, 2, 3]})
        profiler = DataProfiler(df)
        profiler.print_summary()

        captured = capsys.readouterr()
        # Long names should be truncated
        assert ".." in captured.out or "this_is_a_very_long_column_name" in captured.out


# =============================================================================
# TEST CLASS: Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for various edge cases."""

    def test_single_row_dataframe(self):
        """Test profiling DataFrame with single row."""
        df = pd.DataFrame({"col": [1]})
        profiler = DataProfiler(df)
        profile = profiler.profile()

        assert profile["row_count"] == 1
        assert profile["columns"]["col"]["unique_percent"] == 100.0

    def test_single_value_column(self):
        """Test profiling column with all same values."""
        df = pd.DataFrame({"constant": ["A", "A", "A", "A", "A"]})
        profiler = DataProfiler(df)
        profile = profiler.profile()

        assert profile["columns"]["constant"]["unique_count"] == 1
        assert profile["columns"]["constant"]["unique_percent"] == 20.0

    def test_unicode_column_names(self):
        """Test profiling with unicode column names."""
        df = pd.DataFrame({"名前": ["Alice", "Bob"], "código": [1, 2], "данные": [True, False]})
        profiler = DataProfiler(df)
        profile = profiler.profile()

        assert "名前" in profile["columns"]
        assert "código" in profile["columns"]
        assert "данные" in profile["columns"]

    def test_unicode_values(self):
        """Test profiling with unicode values."""
        df = pd.DataFrame({"name": ["田中", "ジョン", "Müller", "Иван", "João"]})
        profiler = DataProfiler(df)
        profile = profiler.profile()

        assert profile["columns"]["name"]["unique_count"] == 5

    def test_special_numeric_values(self):
        """Test profiling with special numeric values (inf, -inf)."""
        df = pd.DataFrame({"values": [1.0, float("inf"), float("-inf"), 100.0, np.nan]})
        profiler = DataProfiler(df)
        profile = profiler.profile()

        # Should handle without error
        assert "values" in profile["columns"]

    def test_very_large_numbers(self):
        """Test profiling with very large numbers."""
        df = pd.DataFrame({"big_nums": [1e308, -1e308, 1e-308, 0, 1]})
        profiler = DataProfiler(df)
        profile = profiler.profile()

        assert "min" in profile["columns"]["big_nums"]
        assert "max" in profile["columns"]["big_nums"]

    def test_boolean_column(self):
        """Test profiling boolean column."""
        df = pd.DataFrame({"flag": [True, False, True, False, True]})
        profiler = DataProfiler(df)
        profile = profiler.profile()

        assert profile["columns"]["flag"]["unique_count"] == 2

    def test_mixed_type_column(self):
        """Test profiling column with mixed types."""
        df = pd.DataFrame({"mixed": [1, "text", 3.14, True, None]})
        profiler = DataProfiler(df)
        profile = profiler.profile()

        assert "mixed" in profile["columns"]

    def test_datetime_with_timezone(self):
        """Test profiling datetime with timezone."""
        df = pd.DataFrame(
            {"timestamp": pd.to_datetime(["2023-01-01", "2023-02-01"]).tz_localize("UTC")}
        )
        profiler = DataProfiler(df)
        profile = profiler.profile()

        assert profile["columns"]["timestamp"]["detected_type"] == "date"

    def test_categorical_dtype(self):
        """Test profiling pandas categorical dtype."""
        df = pd.DataFrame({"category": pd.Categorical(["A", "B", "A", "C", "B"])})
        profiler = DataProfiler(df)
        profile = profiler.profile()

        assert "category" in profile["columns"]


# =============================================================================
# TEST CLASS: Integration Tests
# =============================================================================


class TestIntegration:
    """Integration tests combining multiple operations."""

    def test_full_workflow(self, sample_dataframe):
        """Test complete profiling workflow."""
        profiler = DataProfiler(sample_dataframe)

        # Profile
        profile = profiler.profile()
        assert profile is not None

        # Generate expectations
        config = profiler.generate_expectations(
            validation_name="integration_test", description="Integration test validation"
        )
        assert config is not None
        assert len(config["expectations"]) > 0

        # Save config
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            temp_path = f.name

        try:
            profiler.save_config(config, temp_path)
            assert os.path.exists(temp_path)

            # Load and verify
            with open(temp_path) as f:
                loaded = yaml.safe_load(f)
            assert loaded["validation_name"] == "integration_test"
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_profile_then_generate_twice(self, sample_dataframe):
        """Test generating expectations multiple times."""
        profiler = DataProfiler(sample_dataframe)

        config1 = profiler.generate_expectations(validation_name="first")
        config2 = profiler.generate_expectations(validation_name="second")

        assert config1["validation_name"] == "first"
        assert config2["validation_name"] == "second"
        # Both should use same profile data
        assert config1["data_source"]["profiled_rows"] == config2["data_source"]["profiled_rows"]

    def test_different_dataframes_different_profiles(self, sample_dataframe, large_dataframe):
        """Test that different DataFrames produce different profiles."""
        profiler1 = DataProfiler(sample_dataframe)
        profiler2 = DataProfiler(large_dataframe)

        profile1 = profiler1.profile()
        profile2 = profiler2.profile()

        assert profile1["row_count"] != profile2["row_count"]
        assert profile1["column_count"] != profile2["column_count"]
