"""
Integration tests for Fabric connector
Note: These tests require a Fabric environment with Spark
"""

import pytest


class TestFabricDataQualityRunner:
    """Test suite for FabricDataQualityRunner"""
    
    @pytest.fixture
    def sample_config_path(self, tmp_path):
        """Create a temporary config file"""
        import yaml
        
        config = {
            "validation_name": "test_validation",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1}
                }
            ]
        }
        
        config_file = tmp_path / "test_config.yml"
        with open(config_file, 'w') as f:
            yaml.dump(config, f)
        
        return str(config_file)
    
    @pytest.mark.fabric
    def test_validate_spark_dataframe(self, sample_config_path):
        """Test validation of Spark DataFrame (requires Fabric)"""
        from dq_framework import FabricDataQualityRunner
        from pyspark.sql import SparkSession
        
        # Initialize Spark (in Fabric, spark is pre-initialized)
        spark = SparkSession.builder.getOrCreate()
        
        # Create test DataFrame
        df = spark.createDataFrame([
            (1, "Alice", 25),
            (2, "Bob", 30),
            (3, "Charlie", 35)
        ], ["id", "name", "age"])
        
        # Validate
        runner = FabricDataQualityRunner(sample_config_path)
        results = runner.validate_spark_dataframe(df)
        
        assert results["success"] is True
        assert results["statistics"]["evaluated_expectations"] > 0
    
    def test_runner_initialization(self, sample_config_path):
        """Test runner initialization"""
        from dq_framework import FabricDataQualityRunner
        
        runner = FabricDataQualityRunner(sample_config_path)
        assert runner is not None
        assert runner.config is not None


# NOTE: pytest_addoption for --fabric is defined in tests/conftest.py
# Do not duplicate it here as pytest only allows one definition


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
