"""Alerting subpackage -- message formatting, configuration, and delivery infrastructure.

Public API::

    from dq_framework.alerting import AlertFormatter
"""

from .formatter import AlertFormatter

__all__ = ["AlertFormatter"]
