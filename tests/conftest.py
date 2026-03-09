from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest
import yaml

from dq_framework.constants import DEFAULT_VALIDATION_THRESHOLD


def pytest_addoption(parser):
    parser.addoption(
        "--fabric", action="store_true", default=False, help="run fabric integration tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "fabric: mark test as requiring fabric environment")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--fabric"):
        # If --fabric is given, do not skip fabric tests
        return
    skip_fabric = pytest.mark.skip(reason="need --fabric option to run")
    for item in items:
        if "fabric" in item.keywords:
            item.add_marker(skip_fabric)


# ---------------------------------------------------------------------------
# Shared Spark / Fabric mock fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_spark_session():
    """MagicMock of SparkSession with builder.getOrCreate chain."""
    session = MagicMock(name="SparkSession")
    builder = MagicMock(name="SparkSession.builder")
    builder.getOrCreate.return_value = session
    session.builder = builder
    return session


@pytest.fixture
def mock_spark_df():
    """MagicMock Spark DataFrame with common method chains.

    - count() returns 100
    - columns returns ["id", "name", "age"]
    - toPandas() returns a small pandas DataFrame
    - withColumn, filter, drop, limit return the mock itself (chaining)
    - sample returns the mock itself
    """
    df = MagicMock(name="SparkDataFrame")
    df.count.return_value = 100
    df.columns = ["id", "name", "age"]
    df.toPandas.return_value = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        }
    )
    # Method chaining support
    df.withColumn.return_value = df
    df.filter.return_value = df
    df.drop.return_value = df
    df.limit.return_value = df
    df.sample.return_value = df
    return df


@pytest.fixture
def mock_mssparkutils():
    """MagicMock for mssparkutils.fs operations (head, put, ls, mkdirs)."""
    utils = MagicMock(name="mssparkutils")
    utils.fs.head.return_value = ""
    utils.fs.put.return_value = None
    utils.fs.ls.return_value = []
    utils.fs.mkdirs.return_value = True
    utils.fs.rm.return_value = None
    utils.fs.exists.return_value = True
    return utils


@pytest.fixture
def fabric_runner(tmp_path):
    """Creates a FabricDataQualityRunner with a minimal YAML config."""
    from dq_framework.fabric_connector import FabricDataQualityRunner

    config = {
        "validation_name": "test_validation",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1},
            }
        ],
    }
    config_file = tmp_path / "test_config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config, f)
    return FabricDataQualityRunner(str(config_file))


@pytest.fixture
def sample_validation_result():
    """Returns a dict matching the standard validation result structure."""
    return {
        "success": True,
        "suite_name": "test_suite",
        "batch_name": "test_batch",
        "success_rate": 100.0,
        "evaluated_checks": 5,
        "successful_checks": 5,
        "failed_checks": 0,
        "timestamp": datetime.now().isoformat(),
        "failed_expectations": [],
        "threshold": DEFAULT_VALIDATION_THRESHOLD,
    }
