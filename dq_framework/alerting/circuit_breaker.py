"""Circuit breaker state machine for alert channel delivery.

Prevents retrying dead channels by tracking consecutive failures and
transitioning through CLOSED -> OPEN -> HALF_OPEN states. Uses
``time.monotonic()`` for reliable cooldown timing.

State transitions::

    CLOSED  --[N failures]--> OPEN  --[cooldown elapsed]--> HALF_OPEN
    HALF_OPEN --[success]--> CLOSED
    HALF_OPEN --[failure]--> OPEN

Usage::

    breaker = CircuitBreaker(failure_threshold=5, cooldown_seconds=300.0)

    if breaker.allow_request():
        try:
            channel.send(message)
            breaker.record_success()
        except Exception:
            breaker.record_failure()
"""

from __future__ import annotations

import time
from enum import Enum


class CircuitState(Enum):
    """States of the circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """In-memory circuit breaker for alert channel delivery.

    Tracks consecutive delivery failures and opens the circuit after
    ``failure_threshold`` consecutive failures. After ``cooldown_seconds``
    elapse, transitions to HALF_OPEN to allow a single probe request.

    Note:
        State is intentionally in-memory and per-process. This is correct
        for batch pipeline usage where each run starts fresh.

    Args:
        failure_threshold: Consecutive failures before opening the circuit.
        cooldown_seconds: Seconds to wait before allowing a test request.
    """

    def __init__(
        self, failure_threshold: int = 5, cooldown_seconds: float = 300.0
    ) -> None:
        self._failure_threshold = failure_threshold
        self._cooldown_seconds = cooldown_seconds
        self._failure_count: int = 0
        self._state: CircuitState = CircuitState.CLOSED
        self._last_failure_time: float = 0.0

    @property
    def state(self) -> CircuitState:
        """Current circuit state, with automatic OPEN -> HALF_OPEN transition.

        When OPEN, checks whether the cooldown period has elapsed and
        auto-transitions to HALF_OPEN if so.
        """
        if self._state == CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self._cooldown_seconds:
                self._state = CircuitState.HALF_OPEN
        return self._state

    def allow_request(self) -> bool:
        """Whether a request should be attempted.

        Returns True for CLOSED and HALF_OPEN states, False for OPEN.
        """
        return self.state != CircuitState.OPEN

    def record_success(self) -> None:
        """Record a successful delivery.

        Resets failure count and transitions to CLOSED (relevant when
        called in HALF_OPEN state after a successful probe).
        """
        self._failure_count = 0
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        """Record a failed delivery.

        Increments consecutive failure count and transitions to OPEN
        if threshold is reached. Also resets the cooldown timer.
        """
        self._failure_count += 1
        self._last_failure_time = time.monotonic()
        if self._failure_count >= self._failure_threshold:
            self._state = CircuitState.OPEN

    def reset(self) -> None:
        """Reset to initial CLOSED state with zero failure count."""
        self._failure_count = 0
        self._state = CircuitState.CLOSED
        self._last_failure_time = 0.0
