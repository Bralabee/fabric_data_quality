"""
Fabric Data Quality Framework
==============================

A reusable data quality validation framework using Great Expectations.

Usage:
    from dq_framework import DataQualityValidator, FabricDataQualityRunner
    
    # Basic validation
    validator = DataQualityValidator(config_path='config.yml')
    results = validator.validate(df)
    
    # Fabric integration
    runner = FabricDataQualityRunner(config_path='config.yml')
    results = runner.validate_delta_table('table_name')
"""

__version__ = "1.0.0"
__author__ = "HS2 Data Engineering Team"

from .validator import DataQualityValidator
from .fabric_connector import FabricDataQualityRunner
from .config_loader import ConfigLoader
from .data_profiler import DataProfiler
from .batch_profiler import BatchProfiler
from .loader import DataLoader

__all__ = [
    "DataQualityValidator",
    "FabricDataQualityRunner",
    "ConfigLoader",
    "DataProfiler",
    "BatchProfiler",
    "DataLoader",
]
