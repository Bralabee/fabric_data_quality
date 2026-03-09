"""Alerting subpackage -- message formatting, configuration, and delivery infrastructure.

Public API::

    from dq_framework.alerting import (
        AlertFormatter, AlertConfig, ChannelConfig,
        CircuitBreakerConfig, FailurePolicy, AlertDeliveryError,
        CircuitBreaker, CircuitState,
        AlertChannel, AlertDispatcher,
        TeamsChannel, EmailChannel, create_channel,
    )
"""

from .channels import EmailChannel, TeamsChannel, create_channel
from .circuit_breaker import CircuitBreaker, CircuitState
from .config import (
    AlertConfig,
    AlertDeliveryError,
    ChannelConfig,
    CircuitBreakerConfig,
    FailurePolicy,
    SeverityRoutingConfig,
)
from .dispatcher import AlertChannel, AlertDispatcher
from .formatter import AlertFormatter
from .routing import AlertAction, SeverityRouter

__all__ = [
    "AlertAction",
    "AlertChannel",
    "AlertConfig",
    "AlertDeliveryError",
    "AlertDispatcher",
    "AlertFormatter",
    "ChannelConfig",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "EmailChannel",
    "FailurePolicy",
    "SeverityRouter",
    "SeverityRoutingConfig",
    "TeamsChannel",
    "create_channel",
]
