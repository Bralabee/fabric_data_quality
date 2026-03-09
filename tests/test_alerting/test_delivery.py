"""Unit tests for AlertDispatcher with failure policies."""

import logging

import pytest

from dq_framework.alerting.circuit_breaker import CircuitBreaker, CircuitState
from dq_framework.alerting.config import (
    AlertConfig,
    AlertDeliveryError,
    ChannelConfig,
    CircuitBreakerConfig,
    FailurePolicy,
)
from dq_framework.alerting.dispatcher import AlertChannel, AlertDispatcher
from dq_framework.alerting.formatter import AlertFormatter


class MockChannel(AlertChannel):
    """Test double implementing AlertChannel."""

    def __init__(self, succeed: bool = True):
        self._succeed = succeed
        self.calls: list[tuple[str, str, str]] = []

    def send(self, message: str, subject: str, severity: str) -> bool:
        self.calls.append((message, subject, severity))
        return self._succeed


def _make_config(
    enabled: bool = True,
    failure_policy: FailurePolicy = FailurePolicy.WARN,
    channels: list[ChannelConfig] | None = None,
    cb_threshold: int = 5,
    cb_cooldown: float = 300.0,
) -> AlertConfig:
    return AlertConfig(
        enabled=enabled,
        failure_policy=failure_policy,
        channels=channels or [],
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=cb_threshold,
            cooldown_seconds=cb_cooldown,
        ),
    )


def _sample_results() -> dict:
    return {
        "suite_name": "test_suite",
        "success": False,
        "success_rate": 80,
        "batch_name": "batch_1",
        "evaluated_checks": 10,
        "failed_checks": 2,
        "failed_expectations": [],
        "severity_stats": {},
        "threshold_failures": [],
        "timestamp": "2026-03-09",
    }


class TestAlertDispatcher:
    """Tests for AlertDispatcher dispatch orchestration."""

    def test_dispatch_with_no_registered_channels_returns_empty_dict(self):
        config = _make_config()
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        result = disp.dispatch(_sample_results())
        assert result == {}

    def test_dispatch_calls_formatter_render(self, mocker):
        config = _make_config(channels=[ChannelConfig(type="test", enabled=True)])
        formatter = AlertFormatter()
        spy = mocker.patch.object(formatter, "render", return_value="rendered msg")
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel(succeed=True)
        disp.register_channel("test", ch)
        disp.dispatch(_sample_results())
        spy.assert_called_once()
        call_args = spy.call_args
        assert call_args[0][0] == "summary.txt.j2"  # default template

    def test_successful_send_records_success_and_returns_true(self):
        config = _make_config(channels=[ChannelConfig(type="test", enabled=True)])
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel(succeed=True)
        disp.register_channel("test", ch)
        outcomes = disp.dispatch(_sample_results())
        assert outcomes == {"test": True}
        assert len(ch.calls) == 1

    def test_failed_send_with_warn_policy_logs_warning_returns_false(self, caplog):
        config = _make_config(
            failure_policy=FailurePolicy.WARN,
            channels=[ChannelConfig(type="test", enabled=True)],
        )
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel(succeed=False)
        disp.register_channel("test", ch)
        with caplog.at_level(logging.WARNING):
            outcomes = disp.dispatch(_sample_results())
        assert outcomes == {"test": False}
        assert any("test" in r.message.lower() for r in caplog.records)

    def test_failed_send_with_raise_policy_raises_error(self):
        config = _make_config(
            failure_policy=FailurePolicy.RAISE,
            channels=[ChannelConfig(type="test", enabled=True)],
        )
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel(succeed=False)
        disp.register_channel("test", ch)
        with pytest.raises(AlertDeliveryError, match="test"):
            disp.dispatch(_sample_results())

    def test_failed_send_with_fallback_policy_tries_next_channel(self):
        config = _make_config(
            failure_policy=FailurePolicy.FALLBACK,
            channels=[
                ChannelConfig(type="primary", enabled=True),
                ChannelConfig(type="backup", enabled=True),
            ],
        )
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        primary = MockChannel(succeed=False)
        backup = MockChannel(succeed=True)
        disp.register_channel("primary", primary)
        disp.register_channel("backup", backup)
        outcomes = disp.dispatch(_sample_results())
        assert outcomes["primary"] is False
        assert outcomes["backup"] is True
        assert len(primary.calls) == 1
        assert len(backup.calls) == 1

    def test_fallback_all_channels_failing_logs_warning(self, caplog):
        config = _make_config(
            failure_policy=FailurePolicy.FALLBACK,
            channels=[
                ChannelConfig(type="ch1", enabled=True),
                ChannelConfig(type="ch2", enabled=True),
            ],
        )
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        disp.register_channel("ch1", MockChannel(succeed=False))
        disp.register_channel("ch2", MockChannel(succeed=False))
        with caplog.at_level(logging.WARNING):
            outcomes = disp.dispatch(_sample_results())
        assert outcomes == {"ch1": False, "ch2": False}
        # Should not raise

    def test_open_circuit_breaker_skips_channel(self):
        config = _make_config(
            channels=[ChannelConfig(type="test", enabled=True)],
            cb_threshold=1,
        )
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel(succeed=True)
        disp.register_channel("test", ch)
        # Manually open the breaker
        disp._breakers["test"].record_failure()
        assert disp._breakers["test"].state == CircuitState.OPEN
        outcomes = disp.dispatch(_sample_results())
        # Channel should not have been called
        assert len(ch.calls) == 0
        assert "test" not in outcomes  # skipped entirely

    def test_half_open_circuit_breaker_allows_probe(self):
        config = _make_config(
            channels=[ChannelConfig(type="test", enabled=True)],
            cb_threshold=1,
            cb_cooldown=0.0,  # immediate transition to HALF_OPEN
        )
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel(succeed=True)
        disp.register_channel("test", ch)
        # Open then immediately half-open
        disp._breakers["test"].record_failure()
        assert disp._breakers["test"].state == CircuitState.HALF_OPEN
        outcomes = disp.dispatch(_sample_results())
        assert outcomes == {"test": True}
        assert len(ch.calls) == 1

    def test_disabled_channel_is_skipped(self):
        config = _make_config(channels=[ChannelConfig(type="test", enabled=False)])
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel(succeed=True)
        disp.register_channel("test", ch)
        outcomes = disp.dispatch(_sample_results())
        assert len(ch.calls) == 0
        assert outcomes == {}

    def test_dispatch_returns_outcomes_for_all_attempted_channels(self):
        config = _make_config(
            channels=[
                ChannelConfig(type="a", enabled=True),
                ChannelConfig(type="b", enabled=True),
            ],
        )
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        disp.register_channel("a", MockChannel(succeed=True))
        disp.register_channel("b", MockChannel(succeed=False))
        outcomes = disp.dispatch(_sample_results())
        assert outcomes == {"a": True, "b": False}

    def test_dispatch_with_alerts_disabled_returns_empty_dict(self):
        config = _make_config(enabled=False)
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel(succeed=True)
        disp.register_channel("test", ch)
        outcomes = disp.dispatch(_sample_results())
        assert outcomes == {}
        assert len(ch.calls) == 0

    def test_register_channel_creates_circuit_breaker(self):
        config = _make_config(cb_threshold=10, cb_cooldown=60.0)
        formatter = AlertFormatter()
        disp = AlertDispatcher(config, formatter)
        ch = MockChannel()
        disp.register_channel("test", ch)
        assert "test" in disp._breakers
        breaker = disp._breakers["test"]
        assert isinstance(breaker, CircuitBreaker)
        assert breaker._failure_threshold == 10
        assert breaker._cooldown_seconds == 60.0
