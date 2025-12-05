"""
Unit tests for DataQualityValidator
"""

import pytest
import pandas as pd
from dq_framework import DataQualityValidator, ConfigLoader


class TestDataQualityValidator:
    """Test suite for DataQualityValidator"""
    
    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing"""
        return {
            "validation_name": "test_validation",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1, "max_value": 100}
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "id"}
                }
            ]
        }
    
    @pytest.fixture
    def sample_dataframe(self):
        """Sample DataFrame for testing"""
        return pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45]
        })
    
    def test_validator_initialization(self, sample_config):
        """Test validator initialization"""
        validator = DataQualityValidator(sample_config)
        assert validator is not None
        assert validator.config == sample_config
    
    def test_validate_success(self, sample_config, sample_dataframe):
        """Test successful validation"""
        validator = DataQualityValidator(sample_config)
        results = validator.validate(sample_dataframe)
        
        assert results["success"] is True
        assert results["statistics"]["evaluated_expectations"] == 2
        assert results["statistics"]["successful_expectations"] == 2
    
    def test_validate_failure(self, sample_dataframe):
        """Test validation failure"""
        config = {
            "validation_name": "test_failure",
            "expectations": [
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "nonexistent_column"}
                }
            ]
        }
        
        validator = DataQualityValidator(config)
        results = validator.validate(sample_dataframe)
        
        assert results["success"] is False
        assert results["statistics"]["unsuccessful_expectations"] > 0
    
    def test_validate_null_checks(self):
        """Test null value validations"""
        df = pd.DataFrame({
            "id": [1, 2, None, 4, 5],
            "name": ["A", "B", "C", "D", "E"]
        })
        
        config = {
            "validation_name": "null_test",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"}
                }
            ]
        }
        
        validator = DataQualityValidator(config)
        results = validator.validate(df)
        
        assert results["success"] is False
    
    def test_validate_uniqueness(self):
        """Test uniqueness validations"""
        df = pd.DataFrame({
            "id": [1, 2, 2, 4, 5],  # Duplicate value
            "name": ["A", "B", "C", "D", "E"]
        })
        
        config = {
            "validation_name": "unique_test",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "id"}
                }
            ]
        }
        
        validator = DataQualityValidator(config)
        results = validator.validate(df)
        
        assert results["success"] is False


class TestConfigLoader:
    """Test suite for ConfigLoader"""
    
    def test_validate_config_valid(self):
        """Test validation of valid config"""
        config = {
            "validation_name": "test",
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
    
    def test_validate_config_missing_name(self):
        """Test validation fails without validation_name"""
        config = {
            "expectations": [
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "id"}
                }
            ]
        }
        
        loader = ConfigLoader()
        with pytest.raises(ValueError):
            loader.validate(config)
    
    def test_validate_config_missing_expectations(self):
        """Test validation fails without expectations"""
        config = {
            "validation_name": "test"
        }
        
        loader = ConfigLoader()
        with pytest.raises(ValueError):
            loader.validate(config)
    
    def test_merge_configs(self):
        """Test merging multiple configs"""
        config1 = {
            "validation_name": "test1",
            "expectations": [
                {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "id"}}
            ]
        }
        
        config2 = {
            "validation_name": "test2",
            "expectations": [
                {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "name"}}
            ]
        }
        
        loader = ConfigLoader()
        merged = loader.merge_configs([config1, config2])
        
        assert "expectations" in merged
        assert len(merged["expectations"]) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
