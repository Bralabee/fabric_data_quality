import pytest

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
