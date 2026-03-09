"""Severity-based alert routing for the alert dispatcher.

Determines whether validation results should trigger an alert (SEND) or
be suppressed (SUPPRESS) based on the highest failing severity level
and configurable thresholds.

Usage::

    from dq_framework.alerting.routing import SeverityRouter, AlertAction

    router = SeverityRouter(min_severity="medium", alert_on_success=False)
    action = router.route(validation_results)
    if action == AlertAction.SEND:
        # dispatch to channels
"""

from __future__ import annotations

import logging
from enum import Enum

logger = logging.getLogger(__name__)

# Ordered from lowest to highest severity
_SEVERITY_ORDER = ["low", "medium", "high", "critical"]


class AlertAction(Enum):
    """Routing decision for an alert.

    Members:
        SEND: Alert should be dispatched to channels.
        SUPPRESS: Alert should be silently dropped.
    """

    SEND = "send"
    SUPPRESS = "suppress"


class SeverityRouter:
    """Routes alerts based on the highest failing severity level.

    Suppresses alerts when:
    - All checks passed (unless ``alert_on_success`` is True)
    - The highest failing severity is below ``min_severity``
    - No severity statistics are available

    Args:
        min_severity: Minimum severity level that triggers an alert.
            Defaults to ``"medium"``.
        alert_on_success: If True, send alerts even when all checks pass.
            Defaults to False.
    """

    def __init__(
        self,
        min_severity: str = "medium",
        alert_on_success: bool = False,
    ) -> None:
        self._min_severity = min_severity
        self._alert_on_success = alert_on_success

    def route(self, results: dict) -> AlertAction:
        """Decide whether to send or suppress an alert.

        Args:
            results: Validation results dictionary containing at minimum
                ``success`` (bool) and optionally ``severity_stats`` (dict).

        Returns:
            AlertAction.SEND if the alert should be dispatched,
            AlertAction.SUPPRESS otherwise.
        """
        is_success = results.get("success", True)

        # Handle all-passing results
        if is_success:
            return AlertAction.SEND if self._alert_on_success else AlertAction.SUPPRESS

        # Find the highest severity with actual failures
        severity_stats = results.get("severity_stats")
        if not severity_stats:
            logger.debug("No severity_stats in results, suppressing alert")
            return AlertAction.SUPPRESS

        highest = self._find_highest_failing_severity(severity_stats)
        if highest is None:
            logger.debug("No failing severities found, suppressing alert")
            return AlertAction.SUPPRESS

        # Compare against minimum threshold
        if self._severity_rank(highest) >= self._severity_rank(self._min_severity):
            return AlertAction.SEND

        logger.debug(
            "Highest failing severity '%s' below min_severity '%s', suppressing",
            highest,
            self._min_severity,
        )
        return AlertAction.SUPPRESS

    @staticmethod
    def _severity_rank(severity: str) -> int:
        """Return the numeric rank of a severity level.

        Args:
            severity: Severity string (low, medium, high, critical).

        Returns:
            Index in ``_SEVERITY_ORDER``, or -1 for unknown levels.
        """
        try:
            return _SEVERITY_ORDER.index(severity)
        except ValueError:
            return -1

    @staticmethod
    def _find_highest_failing_severity(severity_stats: dict) -> str | None:
        """Find the highest severity level that has failures.

        A severity has failures when ``passed < total`` in its stats.

        Args:
            severity_stats: Dict mapping severity names to
                ``{"total": int, "passed": int}`` dicts.

        Returns:
            The highest failing severity string, or None if none found.
        """
        highest: str | None = None
        highest_rank = -1

        for severity, stats in severity_stats.items():
            total = stats.get("total", 0)
            passed = stats.get("passed", 0)
            if passed < total:
                rank = _SEVERITY_ORDER.index(severity) if severity in _SEVERITY_ORDER else -1
                if rank > highest_rank:
                    highest = severity
                    highest_rank = rank

        return highest
