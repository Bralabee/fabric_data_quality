"""Unit tests for CircuitBreaker state machine."""

from unittest.mock import patch

import pytest

from dq_framework.alerting.circuit_breaker import CircuitBreaker, CircuitState


class TestCircuitBreaker:
    """Tests for the CircuitBreaker state machine."""

    def test_initial_state_is_closed(self):
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED

    def test_allow_request_when_closed(self):
        cb = CircuitBreaker()
        assert cb.allow_request() is True

    def test_record_success_keeps_closed_and_resets_count(self):
        cb = CircuitBreaker()
        cb._failure_count = 2
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb._failure_count == 0

    def test_record_failure_increments_count_stays_closed_below_threshold(self):
        cb = CircuitBreaker(failure_threshold=5)
        cb.record_failure()
        assert cb._failure_count == 1
        assert cb.state == CircuitState.CLOSED

        cb.record_failure()
        assert cb._failure_count == 2
        assert cb.state == CircuitState.CLOSED

    def test_consecutive_failures_at_threshold_transitions_to_open(self):
        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_allow_request_returns_false_when_open(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.allow_request() is False

    @patch("dq_framework.alerting.circuit_breaker.time")
    def test_open_transitions_to_half_open_after_cooldown(self, mock_time):
        mock_time.monotonic.side_effect = [
            100.0,  # first failure
            100.0,  # second failure (opens circuit)
            500.0,  # state check: 400s > 300s cooldown
        ]
        cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=300.0)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.HALF_OPEN

    @patch("dq_framework.alerting.circuit_breaker.time")
    def test_open_stays_open_before_cooldown(self, mock_time):
        mock_time.monotonic.side_effect = [
            100.0,  # first failure
            100.0,  # second failure (opens circuit)
            200.0,  # state check: 100s < 300s cooldown
        ]
        cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=300.0)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_allow_request_returns_true_when_half_open(self):
        cb = CircuitBreaker(failure_threshold=1, cooldown_seconds=0.0)
        cb.record_failure()  # opens circuit
        # With cooldown=0, state check transitions to HALF_OPEN immediately
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.allow_request() is True

    @patch("dq_framework.alerting.circuit_breaker.time")
    def test_record_success_in_half_open_transitions_to_closed(self, mock_time):
        mock_time.monotonic.side_effect = [
            100.0,  # failure (opens circuit)
            500.0,  # state check -> HALF_OPEN
        ]
        cb = CircuitBreaker(failure_threshold=1, cooldown_seconds=300.0)
        cb.record_failure()
        # Access state to trigger HALF_OPEN transition
        assert cb.state == CircuitState.HALF_OPEN
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb._failure_count == 0

    @patch("dq_framework.alerting.circuit_breaker.time")
    def test_record_failure_in_half_open_transitions_back_to_open(self, mock_time):
        mock_time.monotonic.side_effect = [
            100.0,  # failure (opens circuit)
            500.0,  # state check -> HALF_OPEN
            500.0,  # failure records time again (reopens)
            600.0,  # state check (still within new cooldown)
        ]
        cb = CircuitBreaker(failure_threshold=1, cooldown_seconds=300.0)
        cb.record_failure()
        # Access state to trigger HALF_OPEN transition
        assert cb.state == CircuitState.HALF_OPEN
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_custom_threshold_and_cooldown_respected(self):
        cb = CircuitBreaker(failure_threshold=10, cooldown_seconds=60.0)
        for _ in range(9):
            cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_reset_returns_to_closed_with_zero_failures(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb._failure_count == 0
        assert cb.allow_request() is True
