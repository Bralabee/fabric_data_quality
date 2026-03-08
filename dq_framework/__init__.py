"""
Fabric Data Quality Framework
==============================

A reusable data quality validation framework using Great Expectations 1.x.

Usage:
    from dq_framework import DataQualityValidator, FabricDataQualityRunner

    # Basic validation
    validator = DataQualityValidator(config_path='config.yml')
    results = validator.validate(df)

    # Fabric integration
    runner = FabricDataQualityRunner(config_path='config.yml')
    results = runner.validate_delta_table('table_name')
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("fabric-data-quality")
except PackageNotFoundError:
    __version__ = "2.0.0"
__author__ = "HS2 Data Engineering Team"

from .batch_profiler import BatchProfiler
from .config_loader import ConfigLoader
from .data_profiler import DataProfiler
from .fabric_connector import FabricDataQualityRunner
from .ingestion import DataIngester
from .loader import DataLoader
from .utils import (
    FABRIC_AVAILABLE,
    FABRIC_UTILS_AVAILABLE,
    FileSystemHandler,
    _is_fabric_runtime,
    get_mssparkutils,
)
from .validator import DataQualityValidator

__all__ = [
    "DataQualityValidator",
    "FabricDataQualityRunner",
    "ConfigLoader",
    "DataProfiler",
    "BatchProfiler",
    "DataLoader",
    "DataIngester",
    "FileSystemHandler",
    # Fabric detection utilities
    "FABRIC_AVAILABLE",
    "FABRIC_UTILS_AVAILABLE",
    "get_mssparkutils",
]
