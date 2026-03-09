"""Alerting subpackage -- message formatting, configuration, and delivery infrastructure.

Public API::

    from dq_framework.alerting import (
        AlertFormatter, AlertConfig, ChannelConfig,
        CircuitBreakerConfig, FailurePolicy, AlertDeliveryError,
    )
"""

from .config import (
    AlertConfig,
    AlertDeliveryError,
    ChannelConfig,
    CircuitBreakerConfig,
    FailurePolicy,
)
from .formatter import AlertFormatter

__all__ = [
    "AlertFormatter",
    "AlertConfig",
    "ChannelConfig",
    "CircuitBreakerConfig",
    "FailurePolicy",
    "AlertDeliveryError",
]
