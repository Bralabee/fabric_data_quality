"""Alerting subpackage -- message formatting, configuration, and delivery infrastructure.

Public API::

    from dq_framework.alerting import (
        AlertFormatter, AlertConfig, ChannelConfig,
        CircuitBreakerConfig, FailurePolicy, AlertDeliveryError,
        CircuitBreaker, CircuitState,
        AlertChannel, AlertDispatcher,
    )
"""

from .circuit_breaker import CircuitBreaker, CircuitState
from .config import (
    AlertConfig,
    AlertDeliveryError,
    ChannelConfig,
    CircuitBreakerConfig,
    FailurePolicy,
)
from .dispatcher import AlertChannel, AlertDispatcher
from .formatter import AlertFormatter

__all__ = [
    "AlertChannel",
    "AlertConfig",
    "AlertDeliveryError",
    "AlertDispatcher",
    "AlertFormatter",
    "ChannelConfig",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "FailurePolicy",
]
