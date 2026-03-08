"""
Characterization tests for threshold logic in DataQualityValidator._format_results.

These tests document the CURRENT behavior of the threshold logic before any
modifications. They serve as a safety net: if a future change breaks one of
these tests, the developer knows that behavior changed and can evaluate whether
the change was intentional.

All tests use the REAL DataQualityValidator with REAL Great Expectations
execution against real pandas DataFrames. No mocking of validator internals.
"""

import pandas as pd
import pytest

from dq_framework.validator import DataQualityValidator


@pytest.fixture
def make_validator():
    """Factory fixture: accepts a config_dict, returns a DataQualityValidator."""

    def _make(config_dict):
        return DataQualityValidator(config_dict=config_dict)

    return _make


class TestThresholdCharacterization:
    """Characterization tests for the 6 threshold logic branches in _format_results."""

    def test_no_thresholds_all_pass_is_success(self, make_validator):
        """CHARACTERIZATION: When no quality_thresholds are configured and no
        threshold argument is passed, the validator falls back to enforcing
        100% success. If all expectations pass, the result is success=True
        with no threshold failures.
        """
        config = {
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "name"},
                }
            ]
        }
        df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})

        result = make_validator(config).validate(df, threshold=None)

        assert result["success"] is True
        assert result["threshold_failures"] == []

    def test_no_thresholds_any_fail_is_failure(self, make_validator):
        """CHARACTERIZATION: When no quality_thresholds are configured and no
        threshold argument is passed, the validator falls back to enforcing
        100% success. If any expectation fails, the result is success=False
        and threshold_failures mentions '100%'.
        """
        config = {
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "name"},
                }
            ]
        }
        df = pd.DataFrame({"name": ["Alice", None, "Charlie"]})

        result = make_validator(config).validate(df, threshold=None)

        assert result["success"] is False
        assert len(result["threshold_failures"]) == 1
        assert "100%" in result["threshold_failures"][0]

    def test_global_threshold_at_100_uses_gx_success(self, make_validator):
        """CHARACTERIZATION: When threshold=100 is passed (>= DEFAULT_VALIDATION_THRESHOLD),
        the validator delegates to GX's own validation_result.success flag.
        If any expectation fails, GX reports success=False and the threshold
        failure message references the DEFAULT_VALIDATION_THRESHOLD (100.0%).
        """
        config = {
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "name"},
                }
            ]
        }
        df = pd.DataFrame({"name": ["Alice", None, "Charlie"]})

        result = make_validator(config).validate(df, threshold=100)

        assert result["success"] is False
        assert len(result["threshold_failures"]) == 1
        assert "100.0%" in result["threshold_failures"][0]

    def test_global_threshold_below_100_compares_rate(self, make_validator):
        """CHARACTERIZATION: When threshold < 100 is passed, the validator
        compares success_rate against the threshold using strict less-than (<).
        With 2 expectations where exactly 1 passes, success_rate is 50%.
        A threshold of 50 means 50% < 50% is False, so the result is
        success=True (the threshold is met with equality).
        """
        config = {
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "a"},
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "b"},
                },
            ]
        }
        df = pd.DataFrame({"a": [1, 2, 3], "b": [1, None, 3]})

        result = make_validator(config).validate(df, threshold=50)

        assert result["success"] is True
        assert result["success_rate"] == 50.0
        assert result["threshold_failures"] == []

    def test_severity_thresholds_checked_independently(self, make_validator):
        """CHARACTERIZATION: When quality_thresholds are configured per severity,
        each severity is checked independently against its own threshold.
        A critical expectation that passes meets its 100% threshold, and a
        low expectation that fails still meets its 0% threshold. Overall
        result is success=True because both severity thresholds are satisfied.
        The 'no global threshold' fallback is skipped when quality_thresholds exist.
        """
        config = {
            "quality_thresholds": {"critical": 100, "low": 0},
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "a"},
                    "meta": {"severity": "critical"},
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "b"},
                    "meta": {"severity": "low"},
                },
            ],
        }
        df = pd.DataFrame({"a": [1, 2, 3], "b": [1, None, 3]})

        result = make_validator(config).validate(df, threshold=None)

        assert result["success"] is True
        assert result["threshold_failures"] == []
        assert result["severity_stats"]["critical"] == {"total": 1, "passed": 1}
        assert result["severity_stats"]["low"] == {"total": 1, "passed": 0}

    def test_severity_threshold_failure_causes_overall_failure(self, make_validator):
        """CHARACTERIZATION: When a severity's pass rate falls below its
        configured threshold, the overall result is success=False.
        With critical=100 threshold and the critical expectation failing,
        the pass rate is 0% < 100%, causing a threshold failure that
        mentions 'critical' in the failure message.
        """
        config = {
            "quality_thresholds": {"critical": 100},
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "a"},
                    "meta": {"severity": "critical"},
                },
            ],
        }
        df = pd.DataFrame({"a": [1, None, 3]})

        result = make_validator(config).validate(df, threshold=None)

        assert result["success"] is False
        assert len(result["threshold_failures"]) == 1
        assert "critical" in result["threshold_failures"][0]
        assert "100%" in result["threshold_failures"][0]
