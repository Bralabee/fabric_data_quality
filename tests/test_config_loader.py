"""
Tests for ConfigLoader
"""

import pytest
import tempfile
import yaml
from pathlib import Path
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
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "id"}
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"}
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
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
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "id"}
                }
            ]
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
                    "kwargs": {"min_value": 1}
                }
            ]
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
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "id"}
                }
            ]
        }
        
        loader = ConfigLoader()
        # Should not raise exception
        loader.validate(config)
    
    def test_validate_missing_validation_name(self):
        """Test validation fails without validation_name"""
        config = {
            "expectations": []
        }
        
        loader = ConfigLoader()
        with pytest.raises(ValueError, match="validation_name"):
            loader.validate(config)
    
    def test_validate_missing_expectations(self):
        """Test validation fails without expectations"""
        config = {
            "validation_name": "test"
        }
        
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
            ]
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
            "metadata": {"owner": "team1"}
        }
        
        config2 = {
            "validation_name": "config2",
            "expectations": [
                {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "name"}}
            ],
            "metadata": {"owner": "team2"}
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
