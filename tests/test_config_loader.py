"""
Tests for ConfigLoader
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from dq_framework import ConfigLoader


class TestConfigLoader:
    """Test suite for ConfigLoader"""

    @pytest.fixture
    def temp_config_file(self):
        """Create a temporary config file"""
        config = {
            "validation_name": "test_validation",
            "description": "Test configuration",
            "expectations": [
                {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "id"}},
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"},
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            yaml.dump(config, f)
            return f.name

    def test_load_config_from_file(self, temp_config_file):
        """Test loading config from file"""
        loader = ConfigLoader()
        config = loader.load(temp_config_file)

        assert config["validation_name"] == "test_validation"
        assert "expectations" in config
        assert len(config["expectations"]) == 2

        # Cleanup
        Path(temp_config_file).unlink()

    def test_load_config_from_dict(self):
        """Test loading config from dictionary"""
        config_dict = {
            "validation_name": "dict_test",
            "expectations": [
                {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "id"}}
            ],
        }

        loader = ConfigLoader()
        config = loader.load(config_dict)

        assert config == config_dict

    def test_load_multiple_configs(self, temp_config_file):
        """Test loading multiple configs"""
        config_dict = {
            "validation_name": "second_config",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1},
                }
            ],
        }

        loader = ConfigLoader()
        configs = loader.load([temp_config_file, config_dict])

        assert len(configs) == 2
        assert configs[0]["validation_name"] == "test_validation"
        assert configs[1]["validation_name"] == "second_config"

        # Cleanup
        Path(temp_config_file).unlink()

    def test_validate_valid_config(self):
        """Test validation of valid config"""
        config = {
            "validation_name": "valid_test",
            "expectations": [
                {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "id"}}
            ],
        }

        loader = ConfigLoader()
        # Should not raise exception
        loader.validate(config)

    def test_validate_missing_validation_name(self):
        """Test validation fails without validation_name"""
        config = {"expectations": []}

        loader = ConfigLoader()
        with pytest.raises(ValueError, match="validation_name"):
            loader.validate(config)

    def test_validate_missing_expectations(self):
        """Test validation fails without expectations"""
        config = {"validation_name": "test"}

        loader = ConfigLoader()
        with pytest.raises(ValueError, match="expectations"):
            loader.validate(config)

    def test_validate_invalid_expectation_format(self):
        """Test validation fails with invalid expectation format"""
        config = {
            "validation_name": "test",
            "expectations": [
                {
                    "kwargs": {"column": "id"}  # Missing expectation_type
                }
            ],
        }

        loader = ConfigLoader()
        with pytest.raises(ValueError, match="expectation_type"):
            loader.validate(config)

    def test_merge_configs(self):
        """Test merging multiple configs"""
        config1 = {
            "validation_name": "config1",
            "expectations": [
                {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "id"}}
            ],
            "metadata": {"owner": "team1"},
        }

        config2 = {
            "validation_name": "config2",
            "expectations": [
                {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "name"}}
            ],
            "metadata": {"owner": "team2"},
        }

        loader = ConfigLoader()
        merged = loader.merge_configs([config1, config2])

        assert merged["validation_name"] == "merged_validation"
        assert len(merged["expectations"]) == 2
        assert merged["expectations"][0]["kwargs"]["column"] == "id"
        assert merged["expectations"][1]["kwargs"]["column"] == "name"

    def test_load_nonexistent_file(self):
        """Test loading nonexistent file raises error"""
        loader = ConfigLoader()
        with pytest.raises(FileNotFoundError):
            loader.load("/nonexistent/path/config.yml")


class TestOptionalSectionValidation:
    """Tests for optional config sections: alerts, history, schema_tracking."""

    def _base_config(self, **extras):
        config = {
            "validation_name": "test",
            "expectations": [
                {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "id"}}
            ],
        }
        config.update(extras)
        return config

    def test_valid_alerts_section(self):
        config = self._base_config(alerts={"channels": [{"type": "teams", "webhook_url": "https://example.com"}]})
        loader = ConfigLoader()
        loader.validate(config)  # Should not raise

    def test_valid_history_section(self):
        config = self._base_config(history={"retention_days": 30})
        loader = ConfigLoader()
        loader.validate(config)

    def test_valid_schema_tracking_section(self):
        config = self._base_config(schema_tracking={"baseline_dir": "/tmp/baselines"})
        loader = ConfigLoader()
        loader.validate(config)

    def test_alerts_not_dict_raises(self):
        config = self._base_config(alerts="not_a_dict")
        loader = ConfigLoader()
        with pytest.raises(ValueError, match="alerts"):
            loader.validate(config)

    def test_alerts_channels_not_list_raises(self):
        config = self._base_config(alerts={"channels": "not_a_list"})
        loader = ConfigLoader()
        with pytest.raises(ValueError, match="channels"):
            loader.validate(config)

    def test_alerts_channel_missing_type_raises(self):
        config = self._base_config(alerts={"channels": [{"webhook_url": "https://example.com"}]})
        loader = ConfigLoader()
        with pytest.raises(ValueError, match="type"):
            loader.validate(config)

    def test_history_retention_days_not_positive_int_raises(self):
        config = self._base_config(history={"retention_days": -5})
        loader = ConfigLoader()
        with pytest.raises(ValueError, match="retention_days"):
            loader.validate(config)

    def test_history_retention_days_not_int_raises(self):
        config = self._base_config(history={"retention_days": "thirty"})
        loader = ConfigLoader()
        with pytest.raises(ValueError, match="retention_days"):
            loader.validate(config)

    def test_schema_tracking_not_dict_raises(self):
        config = self._base_config(schema_tracking="not_a_dict")
        loader = ConfigLoader()
        with pytest.raises(ValueError, match="schema_tracking"):
            loader.validate(config)

    def test_backward_compat_no_optional_sections(self):
        config = self._base_config()
        loader = ConfigLoader()
        loader.validate(config)  # Should not raise


class TestConstants:
    """Tests for new constants in constants.py."""

    def test_cb_failure_threshold(self):
        from dq_framework.constants import DEFAULT_CB_FAILURE_THRESHOLD
        assert DEFAULT_CB_FAILURE_THRESHOLD == 5

    def test_cb_cooldown_seconds(self):
        from dq_framework.constants import DEFAULT_CB_COOLDOWN_SECONDS
        assert DEFAULT_CB_COOLDOWN_SECONDS == 300.0

    def test_failure_policy(self):
        from dq_framework.constants import DEFAULT_FAILURE_POLICY
        assert DEFAULT_FAILURE_POLICY == "warn"

    def test_schema_baselines_dir(self):
        from dq_framework.constants import DEFAULT_SCHEMA_BASELINES_DIR
        assert DEFAULT_SCHEMA_BASELINES_DIR == "dq_results/schema_baselines"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
