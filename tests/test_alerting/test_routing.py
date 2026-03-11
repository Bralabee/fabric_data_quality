"""Unit tests for SeverityRouter and dispatcher routing integration."""

from __future__ import annotations

from dq_framework.alerting.config import (
    AlertConfig,
    ChannelConfig,
    SeverityRoutingConfig,
)
from dq_framework.alerting.dispatcher import AlertChannel, AlertDispatcher
from dq_framework.alerting.formatter import AlertFormatter
from dq_framework.alerting.routing import AlertAction, SeverityRouter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _passing_results() -> dict:
    """Validation results where all checks passed."""
    return {
        "suite_name": "test_suite",
        "batch_name": "batch_1",
        "success": True,
        "success_rate": 100.0,
        "evaluated_checks": 10,
        "failed_checks": 0,
        "failed_expectations": [],
        "severity_stats": {
            "critical": {"total": 5, "passed": 5},
            "high": {"total": 5, "passed": 5},
        },
        "threshold_failures": [],
        "timestamp": "2026-03-09",
    }


def _failing_results(severity: str = "critical", passed: int = 3, total: int = 5) -> dict:
    """Validation results with failures at a given severity."""
    return {
        "suite_name": "test_suite",
        "batch_name": "batch_1",
        "success": False,
        "success_rate": 80.0,
        "evaluated_checks": 10,
        "failed_checks": 2,
        "failed_expectations": [
            {"severity": severity, "expectation": "expect_not_null", "column": "id"},
        ],
        "severity_stats": {
            severity: {"total": total, "passed": passed},
        },
        "threshold_failures": [],
        "timestamp": "2026-03-09",
    }


def _low_only_results() -> dict:
    """Validation results with only low-severity failures."""
    return _failing_results(severity="low", passed=3, total=5)


class MockChannel(AlertChannel):
    """Test double implementing AlertChannel."""

    def __init__(self, succeed: bool = True):
        self._succeed = succeed
        self.calls: list[tuple[str, str, str]] = []

    def send(self, message: str, subject: str, severity: str) -> bool:
        self.calls.append((message, subject, severity))
        return self._succeed


# ---------------------------------------------------------------------------
# SeverityRouter tests
# ---------------------------------------------------------------------------


class TestSeverityRouter:
    """Tests for SeverityRouter route decisions."""

    def test_suppress_all_passing(self):
        """All-passing results -> SUPPRESS by default."""
        router = SeverityRouter()
        assert router.route(_passing_results()) == AlertAction.SUPPRESS

    def test_send_critical_failure(self):
        """Critical severity failures -> SEND."""
        router = SeverityRouter()
        assert router.route(_failing_results("critical")) == AlertAction.SEND

    def test_send_high_failure(self):
        """High severity failures -> SEND."""
        router = SeverityRouter()
        assert router.route(_failing_results("high")) == AlertAction.SEND

    def test_send_medium_failure_default(self):
        """Medium severity failures with default min_severity=medium -> SEND."""
        router = SeverityRouter()
        assert router.route(_failing_results("medium")) == AlertAction.SEND

    def test_suppress_low_only(self):
        """Low-severity-only failures with min_severity=medium -> SUPPRESS."""
        router = SeverityRouter()
        assert router.route(_low_only_results()) == AlertAction.SUPPRESS

    def test_custom_min_severity_high(self):
        """min_severity='high', medium failures -> SUPPRESS."""
        router = SeverityRouter(min_severity="high")
        assert router.route(_failing_results("medium")) == AlertAction.SUPPRESS

    def test_custom_min_severity_low(self):
        """min_severity='low', low failures -> SEND."""
        router = SeverityRouter(min_severity="low")
        assert router.route(_failing_results("low")) == AlertAction.SEND

    def test_alert_on_success_enabled(self):
        """success=True with alert_on_success=True -> SEND."""
        router = SeverityRouter(alert_on_success=True)
        assert router.route(_passing_results()) == AlertAction.SEND

    def test_no_severity_stats(self):
        """Results with no severity_stats key -> SUPPRESS."""
        router = SeverityRouter()
        results = _failing_results("critical")
        del results["severity_stats"]
        assert router.route(results) == AlertAction.SUPPRESS

    def test_severity_stats_all_passed(self):
        """severity_stats where all passed==total -> SUPPRESS."""
        router = SeverityRouter()
        results = _failing_results("critical")
        results["severity_stats"] = {
            "critical": {"total": 5, "passed": 5},
            "high": {"total": 3, "passed": 3},
        }
        # Even though success=False, there are no actual severity failures
        assert router.route(results) == AlertAction.SUPPRESS


# ---------------------------------------------------------------------------
# Dispatcher routing integration tests
# ---------------------------------------------------------------------------


class TestDispatcherRoutingIntegration:
    """Tests for AlertDispatcher + SeverityRouter integration."""

    def test_dispatch_suppressed_by_router(self):
        """Low-severity results -> dispatch returns empty dict, no send() called."""
        config = AlertConfig(
            enabled=True,
            channels=[ChannelConfig(type="test", enabled=True)],
            severity_routing=SeverityRoutingConfig(min_severity="medium"),
        )
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel(succeed=True)
        disp.register_channel("test", ch)

        outcomes = disp.dispatch(_low_only_results())
        assert outcomes == {}
        assert len(ch.calls) == 0

    def test_dispatch_sent_by_router(self):
        """Critical results -> dispatch calls channel.send()."""
        config = AlertConfig(
            enabled=True,
            channels=[ChannelConfig(type="test", enabled=True)],
            severity_routing=SeverityRoutingConfig(min_severity="medium"),
        )
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel(succeed=True)
        disp.register_channel("test", ch)

        outcomes = disp.dispatch(_failing_results("critical"))
        assert outcomes == {"test": True}
        assert len(ch.calls) == 1

    def test_dispatch_no_router_backwards_compatible(self):
        """Dispatcher without severity_routing config -> sends all (backwards compatible)."""
        config = AlertConfig(
            enabled=True,
            channels=[ChannelConfig(type="test", enabled=True)],
            # No severity_routing -- default should be None for backwards compat
        )
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel(succeed=True)
        disp.register_channel("test", ch)

        # Even low-severity results should be sent (no routing)
        outcomes = disp.dispatch(_low_only_results())
        assert outcomes == {"test": True}
        assert len(ch.calls) == 1
