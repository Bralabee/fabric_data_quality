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

try:
    from importlib.metadata import PackageNotFoundError, version
except ImportError:  # pragma: no cover
    from importlib_metadata import PackageNotFoundError, version  # type: ignore


try:
    __version__ = version("fabric-data-quality")
except PackageNotFoundError:
    __version__ = "0.0.0"
__author__ = "HS2 Data Engineering Team"

from .validator import DataQualityValidator
from .fabric_connector import FabricDataQualityRunner
from .config_loader import ConfigLoader
from .data_profiler import DataProfiler
from .batch_profiler import BatchProfiler
from .loader import DataLoader
from .ingestion import DataIngester
from .utils import (
    FileSystemHandler,
    FABRIC_AVAILABLE,
    FABRIC_UTILS_AVAILABLE,
    _is_fabric_runtime,
    get_mssparkutils,
)

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
    "_is_fabric_runtime",
    "get_mssparkutils",
]
