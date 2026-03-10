"""
Integration tests for Phase 10 pipeline integration.

Verifies public API exports, constants defaults, and dependency compatibility.
"""

import pytest

pytestmark = pytest.mark.integration


class TestPublicExports:
    """Verify all new classes are exported from dq_framework top-level."""

    @pytest.mark.parametrize("name", [
        "AlertDispatcher",
        "AlertConfig",
        "AlertFormatter",
        "SeverityRouter",
        "ValidationHistory",
        "SchemaTracker",
        "AlertManager",
        "create_channel",
    ])
    def test_public_exports(self, name):
        import dq_framework
        obj = getattr(dq_framework, name, None)
        assert obj is not None, f"{name} not exported from dq_framework"
        assert name in dq_framework.__all__, f"{name} not in __all__"

    def test_alert_manager_alias(self):
        from dq_framework import AlertDispatcher, AlertManager
        assert AlertManager is AlertDispatcher, "AlertManager should be an alias for AlertDispatcher"


class TestConstantsDefaults:
    """Verify all pipeline integration constants have expected values."""

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

    def test_existing_history_constants(self):
        from dq_framework.constants import (
            DEFAULT_HISTORY_DB,
            DEFAULT_HISTORY_PARQUET_DIR,
            DEFAULT_RETENTION_DAYS,
        )
        assert DEFAULT_RETENTION_DAYS == 90
        assert DEFAULT_HISTORY_DB == "dq_results/validation_history.db"
        assert DEFAULT_HISTORY_PARQUET_DIR == "Files/dq_results/history"


class TestDependencyCompatibility:
    """Verify no dependency conflicts between dq_framework and AIMS."""

    def test_dependency_compatibility(self):
        import httpx  # noqa: F401 - Phase 7 additive dep
        import deepdiff  # noqa: F401 - Phase 8 additive dep
        import great_expectations  # noqa: F401 - core
        import pandas  # noqa: F401 - core
        import yaml  # noqa: F401 - core
        # All imports succeeding proves no version conflicts
