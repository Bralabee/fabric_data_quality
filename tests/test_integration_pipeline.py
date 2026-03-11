"""
Integration tests for Phase 10 pipeline integration.

Verifies public API exports, constants defaults, dependency compatibility,
and end-to-end pipeline flows with all components wired together.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

pytestmark = pytest.mark.integration


class TestPublicExports:
    """Verify all new classes are exported from dq_framework top-level."""

    @pytest.mark.parametrize(
        "name",
        [
            "AlertDispatcher",
            "AlertConfig",
            "AlertFormatter",
            "SeverityRouter",
            "ValidationHistory",
            "SchemaTracker",
            "AlertManager",
            "create_channel",
        ],
    )
    def test_public_exports(self, name):
        import dq_framework

        obj = getattr(dq_framework, name, None)
        assert obj is not None, f"{name} not exported from dq_framework"
        assert name in dq_framework.__all__, f"{name} not in __all__"

    def test_alert_manager_alias(self):
        from dq_framework import AlertDispatcher, AlertManager

        assert AlertManager is AlertDispatcher, (
            "AlertManager should be an alias for AlertDispatcher"
        )


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
        import deepdiff  # noqa: F401 - Phase 8 additive dep
        import great_expectations  # noqa: F401 - core
        import httpx  # noqa: F401 - Phase 7 additive dep
        import pandas  # noqa: F401 - core
        import yaml  # noqa: F401 - core
        # All imports succeeding proves no version conflicts


# ---------------------------------------------------------------------------
# Fixtures for E2E integration tests
# ---------------------------------------------------------------------------

FAILURE_RESULTS = {
    "success": False,
    "success_rate": 75.0,
    "failed_checks": 2,
    "evaluated_checks": 8,
    "suite_name": "test",
    "batch_name": "test_batch",
    "severity_stats": {"critical": {"total": 1, "failed": 1}},
    "failed_expectations": [],
}

SUCCESS_RESULTS = {
    "success": True,
    "success_rate": 100.0,
    "failed_checks": 0,
    "evaluated_checks": 8,
    "suite_name": "test",
    "batch_name": "test_batch",
    "severity_stats": {},
    "failed_expectations": [],
}


@pytest.fixture()
def full_config():
    """Full pipeline config with all optional sections enabled."""
    return {
        "validation_name": "test_validation",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1},
                "meta": {"severity": "critical"},
            }
        ],
        "alerts": {
            "enabled": True,
            "failure_policy": "warn",
            "channels": [
                {
                    "type": "teams",
                    "webhook_url": "https://example.com/webhook",
                    "enabled": True,
                }
            ],
        },
        "history": {
            "enabled": True,
            "retention_days": 30,
            "dataset_name": "test_dataset",
        },
        "schema_tracking": {
            "enabled": True,
            "dataset_name": "test_dataset",
        },
    }


@pytest.fixture()
def minimal_config():
    """Minimal config -- validation_name + expectations only."""
    return {
        "validation_name": "test_validation",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1},
                "meta": {"severity": "critical"},
            }
        ],
    }


@pytest.fixture()
def sample_pdf():
    """A small pandas DataFrame used as toPandas() return value."""
    return pd.DataFrame({"col_a": [1, 2, 3], "col_b": ["x", "y", "z"]})


@pytest.fixture()
def mock_spark_df(sample_pdf):
    """Mock Spark DataFrame with toPandas(), count(), columns."""
    spark_df = MagicMock()
    spark_df.toPandas.return_value = sample_pdf
    spark_df.count.return_value = 3
    spark_df.columns = list(sample_pdf.columns)
    return spark_df


def _build_runner(config_dict, validate_return):
    """Build a FabricDataQualityRunner with all internals mocked.

    Returns (runner, mocks_dict) where mocks_dict has keys:
    validator, store, schema_tracker, history, dispatcher.

    IMPORTANT: After building, SPARK_AVAILABLE is set to True on the module
    so that validate_spark_dataframe works outside the patch context.
    """
    import dq_framework.fabric_connector as fc_mod
    from dq_framework.fabric_connector import FabricDataQualityRunner

    with (
        patch.object(fc_mod, "SPARK_AVAILABLE", True),
        patch.object(fc_mod, "get_store") as mock_get_store,
        patch.object(fc_mod, "DataQualityValidator") as MockValidator,
        patch.object(fc_mod, "SchemaTracker") as MockST,
        patch.object(fc_mod, "ValidationHistory") as MockVH,
        patch.object(fc_mod, "AlertDispatcher") as MockAD,
        patch.object(fc_mod, "AlertConfig") as MockAC,
        patch.object(fc_mod, "create_channel") as mock_cc,
    ):
        # Validator mock
        mock_validator = MagicMock()
        mock_validator.config = config_dict
        mock_validator.validate.return_value = dict(validate_return)
        MockValidator.return_value = mock_validator

        # Store mock
        mock_store = MagicMock()
        mock_get_store.return_value = mock_store

        # AlertConfig mock
        mock_alert_config = MagicMock()
        mock_alert_config.channels = []
        if config_dict.get("alerts", {}).get("enabled"):
            ch_cfg = MagicMock()
            ch_cfg.enabled = True
            ch_cfg.type = "teams"
            mock_alert_config.channels = [ch_cfg]
        MockAC.from_dict.return_value = mock_alert_config

        # Channel mock
        mock_channel = MagicMock()
        mock_cc.return_value = mock_channel

        # Create runner (triggers __init__)
        runner = FabricDataQualityRunner(config_path="dummy.yml")

        # Capture the mocks that were injected during __init__
        mocks = {
            "validator": mock_validator,
            "store": mock_store,
            "schema_tracker": runner._schema_tracker,
            "history": runner._history,
            "dispatcher": runner._alert_dispatcher,
        }

    # Keep SPARK_AVAILABLE True so validate_spark_dataframe works
    fc_mod.SPARK_AVAILABLE = True

    return runner, mocks


# ---------------------------------------------------------------------------
# E2E integration test class
# ---------------------------------------------------------------------------


class TestFullPipeline:
    """End-to-end integration tests for the full validation pipeline."""

    def test_full_pipeline_failure_flow(self, full_config, mock_spark_df):
        """Full pipeline with failure: schema -> validate -> history -> alert."""
        runner, mocks = _build_runner(full_config, FAILURE_RESULTS)

        # Set up schema tracker mock
        mocks["schema_tracker"].check_and_alert.return_value = {"changes": [], "is_baseline": True}

        results = runner.validate_spark_dataframe(mock_spark_df)

        # Schema check was called
        mocks["schema_tracker"].check_and_alert.assert_called_once()
        # Validator ran
        mocks["validator"].validate.assert_called_once()
        # History recorded with duration > 0
        mocks["history"].record.assert_called_once()
        call_kwargs = mocks["history"].record.call_args
        duration = (
            call_kwargs[1].get("duration_seconds") or call_kwargs[0][1]
            if len(call_kwargs[0]) > 1
            else call_kwargs[1].get("duration_seconds", 0)
        )
        assert duration >= 0, "duration_seconds should be >= 0"
        # Alert dispatched (failure case)
        mocks["dispatcher"].dispatch.assert_called_once()
        dispatch_kwargs = mocks["dispatcher"].dispatch.call_args
        assert dispatch_kwargs[1].get("severity") == "critical"
        # Results augmented
        assert "schema_check" in results
        assert "history_recorded" in results

    def test_full_pipeline_success_no_alert(self, full_config, mock_spark_df):
        """On success: no alert dispatched, but history still recorded."""
        runner, mocks = _build_runner(full_config, SUCCESS_RESULTS)
        mocks["schema_tracker"].check_and_alert.return_value = {"changes": []}

        results = runner.validate_spark_dataframe(mock_spark_df)

        # History still recorded on success
        mocks["history"].record.assert_called_once()
        # No alert on success
        mocks["dispatcher"].dispatch.assert_not_called()

    def test_pipeline_stage_isolation_schema_failure(self, full_config, mock_spark_df):
        """Schema check raising should not block validation or history."""
        runner, mocks = _build_runner(full_config, FAILURE_RESULTS)
        mocks["schema_tracker"].check_and_alert.side_effect = RuntimeError("schema boom")

        results = runner.validate_spark_dataframe(mock_spark_df)

        # Validation still ran
        mocks["validator"].validate.assert_called_once()
        # History still recorded
        mocks["history"].record.assert_called_once()
        # No unhandled exception -- we got results
        assert "success" in results
        # schema_check NOT in results because it failed
        assert "schema_check" not in results

    def test_pipeline_stage_isolation_history_failure(self, full_config, mock_spark_df):
        """History recording raising should not block alerting."""
        runner, mocks = _build_runner(full_config, FAILURE_RESULTS)
        mocks["schema_tracker"].check_and_alert.return_value = {"changes": []}
        mocks["history"].record.side_effect = RuntimeError("history boom")

        results = runner.validate_spark_dataframe(mock_spark_df)

        # Alert still fires on failure
        mocks["dispatcher"].dispatch.assert_called_once()
        # Results still returned
        assert results["success"] is False

    def test_backward_compat_minimal_config(self, minimal_config, mock_spark_df):
        """Runner with minimal config produces results without new keys."""
        runner, mocks = _build_runner(minimal_config, SUCCESS_RESULTS)

        results = runner.validate_spark_dataframe(mock_spark_df)

        # No schema_check or history_recorded keys
        assert "schema_check" not in results
        assert "history_recorded" not in results
        # Core validation still works
        assert results["success"] is True

    def test_pipeline_order(self, full_config, mock_spark_df):
        """Stages execute in order: schema -> validate -> history -> alert."""
        call_order = []

        runner, mocks = _build_runner(full_config, FAILURE_RESULTS)

        # Wire side effects to track call order
        mocks["schema_tracker"].check_and_alert.side_effect = lambda *a, **kw: (
            call_order.append("schema"),
            {"changes": []},
        )[1]

        original_validate = mocks["validator"].validate

        def track_validate(*a, **kw):
            call_order.append("validate")
            return dict(FAILURE_RESULTS)

        mocks["validator"].validate.side_effect = track_validate

        mocks["history"].record.side_effect = lambda *a, **kw: call_order.append("history")
        mocks["dispatcher"].dispatch.side_effect = lambda *a, **kw: call_order.append("alert")

        runner.validate_spark_dataframe(mock_spark_df)

        assert call_order == ["schema", "validate", "history", "alert"]


class TestChannelRegistrationWiring:
    """Regression tests for channel registration name matching dispatch lookup.

    Bug 1 (v1.0 audit): channels were registered as 'teams_0' but dispatch
    looked up 'teams', causing all alerts to be silently dropped.
    """

    def test_channel_name_matches_dispatch_lookup(self):
        """Registered channel name must match the key used in dispatch()."""
        from dq_framework.alerting.config import AlertConfig, ChannelConfig
        from dq_framework.alerting.dispatcher import AlertChannel, AlertDispatcher
        from dq_framework.alerting.formatter import AlertFormatter

        ch_cfg = ChannelConfig(
            type="teams", enabled=True, settings={"webhook_url": "https://example.com"}
        )
        config = AlertConfig(enabled=True, channels=[ch_cfg])
        dispatcher = AlertDispatcher(config=config, formatter=AlertFormatter())

        mock_channel = MagicMock(spec=AlertChannel)
        mock_channel.send.return_value = True
        # Register using ch_cfg.type (the fix) — must match dispatch lookup
        dispatcher.register_channel(ch_cfg.type, mock_channel)

        results = {
            "success": False,
            "suite_name": "test",
            "severity_stats": {"critical": {"total": 1, "passed": 0}},
        }
        outcomes = dispatcher.dispatch(results)

        # Channel must have been called — not silently skipped
        assert mock_channel.send.called, "Channel was registered but never called during dispatch"
        assert "teams" in outcomes

    def test_determine_severity_uses_total_minus_passed(self, tmp_path):
        """_determine_severity must use total-passed, not a 'failed' key."""
        import yaml

        from dq_framework.fabric_connector import FabricDataQualityRunner

        config = {
            "validation_name": "sev_regression",
            "expectations": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1},
                }
            ],
        }
        config_file = tmp_path / "sev.yml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)
        runner = FabricDataQualityRunner(str(config_file))

        # Severity stats use total/passed (not 'failed')
        results = {
            "severity_stats": {
                "critical": {"total": 5, "passed": 3},  # 2 failures
                "high": {"total": 4, "passed": 4},  # 0 failures
            }
        }
        assert runner._determine_severity(results) == "critical"

        # All passed — should fall through to default
        results_ok = {
            "severity_stats": {
                "critical": {"total": 5, "passed": 5},
                "high": {"total": 4, "passed": 4},
            }
        }
        assert runner._determine_severity(results_ok) == "medium"
