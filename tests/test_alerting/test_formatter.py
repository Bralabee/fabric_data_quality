"""Unit tests for AlertFormatter -- Jinja2 template rendering for alert messages."""

import os
import tempfile

import pytest
from jinja2.exceptions import TemplateNotFound

from dq_framework.alerting import AlertFormatter


@pytest.fixture
def sample_results():
    """Validation result dict matching the structure from validator.py."""
    return {
        "suite_name": "financial_validation",
        "batch_name": "batch_2026_03_08",
        "success": False,
        "success_rate": 75.0,
        "evaluated_checks": 8,
        "failed_checks": 2,
        "failed_expectations": [
            {
                "expectation": "expect_column_values_to_not_be_null",
                "column": "transaction_id",
                "severity": "critical",
            },
            {
                "expectation": "expect_column_values_to_be_between",
                "column": "amount",
                "severity": "high",
            },
        ],
        "severity_stats": {
            "critical": {"total": 3, "passed": 2},
            "high": {"total": 5, "passed": 4},
        },
        "threshold_failures": [
            "Overall success rate 75.0% below threshold 90.0%",
        ],
        "timestamp": "2026-03-08T12:00:00Z",
    }


@pytest.fixture
def passing_results():
    """Validation result dict for a passing suite (no failures)."""
    return {
        "suite_name": "clean_validation",
        "batch_name": "batch_clean",
        "success": True,
        "success_rate": 100.0,
        "evaluated_checks": 5,
        "failed_checks": 0,
        "failed_expectations": [],
        "severity_stats": {},
        "threshold_failures": [],
        "timestamp": "2026-03-08T12:00:00Z",
    }


class TestAlertFormatter:
    """Tests for AlertFormatter Jinja2 template rendering."""

    def test_default_template_renders_suite_name(self, sample_results):
        """AlertFormatter with default templates renders plain-text containing suite_name."""
        formatter = AlertFormatter()
        output = formatter.render("summary.txt.j2", sample_results)
        assert "financial_validation" in output

    def test_default_template_renders_success_status(self, sample_results):
        """AlertFormatter renders success status (PASSED/FAILED)."""
        formatter = AlertFormatter()
        output = formatter.render("summary.txt.j2", sample_results)
        assert "FAILED" in output

    def test_default_template_renders_success_rate(self, sample_results):
        """AlertFormatter renders success_rate percentage."""
        formatter = AlertFormatter()
        output = formatter.render("summary.txt.j2", sample_results)
        assert "75.0%" in output

    def test_default_template_renders_failed_checks_count(self, sample_results):
        """AlertFormatter renders failed_checks count."""
        formatter = AlertFormatter()
        output = formatter.render("summary.txt.j2", sample_results)
        assert "2 failed" in output

    def test_custom_template_dir(self, sample_results):
        """AlertFormatter with custom template_dir uses FileSystemLoader."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write a minimal custom template
            template_path = os.path.join(tmpdir, "custom.txt.j2")
            with open(template_path, "w") as f:
                f.write("Custom: {{ suite_name }} - {{ success_rate }}%")

            formatter = AlertFormatter(template_dir=tmpdir)
            output = formatter.render("custom.txt.j2", sample_results)
            assert output == "Custom: financial_validation - 75.0%"

    def test_failed_expectations_with_severity_and_column(self, sample_results):
        """render summary.txt.j2 includes failed expectations with severity and column."""
        formatter = AlertFormatter()
        output = formatter.render("summary.txt.j2", sample_results)
        assert "CRITICAL" in output
        assert "transaction_id" in output
        assert "HIGH" in output
        assert "amount" in output

    def test_severity_breakdown_section(self, sample_results):
        """render summary.txt.j2 includes severity breakdown when severity_stats present."""
        formatter = AlertFormatter()
        output = formatter.render("summary.txt.j2", sample_results)
        assert "Severity Breakdown" in output
        assert "CRITICAL" in output
        assert "2/3" in output

    def test_threshold_violations_section(self, sample_results):
        """render summary.txt.j2 includes threshold violations when present."""
        formatter = AlertFormatter()
        output = formatter.render("summary.txt.j2", sample_results)
        assert "Threshold Violations" in output
        assert "75.0% below threshold 90.0%" in output

    def test_empty_sections_omitted(self, passing_results):
        """render summary.txt.j2 omits empty sections."""
        formatter = AlertFormatter()
        output = formatter.render("summary.txt.j2", passing_results)
        assert "Failed Expectations:" not in output
        assert "Threshold Violations:" not in output
        assert "PASSED" in output

    def test_html_template_renders_valid_html(self, sample_results):
        """render summary.html.j2 produces valid HTML with table structure."""
        formatter = AlertFormatter()
        output = formatter.render("summary.html.j2", sample_results)
        assert "<html" in output
        assert "<table" in output
        assert "financial_validation" in output
        assert "</html>" in output

    def test_template_not_found_raises(self):
        """AlertFormatter raises TemplateNotFound for nonexistent template name."""
        formatter = AlertFormatter()
        with pytest.raises(TemplateNotFound):
            formatter.render("nonexistent.j2", {})
