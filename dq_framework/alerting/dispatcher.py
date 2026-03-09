"""Alert dispatch orchestration with failure policies and circuit breaker protection.

Routes formatted alert messages to registered channels, applying the
configured failure policy when delivery fails and skipping channels
whose circuit breaker is OPEN.

Usage::

    from dq_framework.alerting import (
        AlertConfig, AlertFormatter, AlertDispatcher, AlertChannel,
    )

    class TeamsChannel(AlertChannel):
        def send(self, message, subject, severity):
            # POST to Teams webhook
            return True

    config = AlertConfig.from_dict(yaml_config)
    formatter = AlertFormatter()
    dispatcher = AlertDispatcher(config, formatter)
    dispatcher.register_channel("teams", TeamsChannel())
    outcomes = dispatcher.dispatch(validation_results)
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from .circuit_breaker import CircuitBreaker
from .config import AlertConfig, AlertDeliveryError, FailurePolicy
from .formatter import AlertFormatter
from .routing import AlertAction, SeverityRouter

logger = logging.getLogger(__name__)


class AlertChannel(ABC):
    """Abstract base class for alert delivery channels.

    Phase 7 implements concrete subclasses (TeamsChannel, EmailChannel)
    that deliver messages via their respective transport.
    """

    @abstractmethod
    def send(self, message: str, subject: str, severity: str) -> bool:
        """Send an alert message.

        Args:
            message: Rendered alert message body.
            subject: Alert subject line.
            severity: Alert severity level (e.g. "low", "medium", "high").

        Returns:
            True if delivery succeeded, False otherwise.
        """


class AlertDispatcher:
    """Orchestrates alert delivery across registered channels.

    Formats validation results using the configured template, then
    dispatches to each enabled channel with circuit breaker protection
    and failure policy handling.

    Args:
        config: Alert configuration (enables/disables alerting, failure policy, etc.).
        formatter: Jinja2 alert formatter for rendering messages.
    """

    def __init__(self, config: AlertConfig, formatter: AlertFormatter) -> None:
        self._config = config
        self._formatter = formatter
        self._channels: dict[str, AlertChannel] = {}
        self._breakers: dict[str, CircuitBreaker] = {}

        # Severity routing (None = backwards compatible, send all)
        self._router: SeverityRouter | None = None
        if config.severity_routing is not None:
            self._router = SeverityRouter(
                min_severity=config.severity_routing.min_severity,
                alert_on_success=config.severity_routing.alert_on_success,
            )

    def register_channel(self, name: str, channel: AlertChannel) -> None:
        """Register an alert delivery channel.

        Creates a per-channel circuit breaker using the config settings.

        Args:
            name: Unique channel identifier (must match ChannelConfig.type).
            channel: AlertChannel implementation.
        """
        self._channels[name] = channel
        self._breakers[name] = CircuitBreaker(
            failure_threshold=self._config.circuit_breaker.failure_threshold,
            cooldown_seconds=self._config.circuit_breaker.cooldown_seconds,
        )

    def dispatch(self, results: dict, severity: str = "medium") -> dict[str, bool]:
        """Dispatch alert to all enabled channels.

        Args:
            results: Validation results dictionary for template rendering.
            severity: Alert severity level.

        Returns:
            Dict mapping channel names to delivery success booleans.
            Skipped channels (disabled or circuit-broken) are omitted.

        Raises:
            AlertDeliveryError: If delivery fails and failure_policy is RAISE.
        """
        if not self._config.enabled:
            return {}

        if not self._channels:
            return {}

        # Severity routing: suppress if router says so
        if self._router is not None:
            action = self._router.route(results)
            if action == AlertAction.SUPPRESS:
                logger.debug("Alert suppressed by severity router")
                return {}

        # Render message
        template_name = self._config.templates.get("summary", "summary.txt.j2")
        message = self._formatter.render(template_name, results)

        # Build subject line
        suite_name = results.get("suite_name", "unknown")
        status = "PASSED" if results.get("success") else "FAILED"
        subject = f"DQ Alert: {suite_name} - {status}"

        outcomes: dict[str, bool] = {}

        for ch_config in self._config.channels:
            name = ch_config.type

            # Skip disabled channels
            if not ch_config.enabled:
                continue

            # Skip unregistered channels
            if name not in self._channels:
                continue

            channel = self._channels[name]
            breaker = self._breakers[name]

            # Check circuit breaker
            if not breaker.allow_request():
                logger.warning("Circuit breaker OPEN for channel '%s', skipping", name)
                continue

            # Attempt delivery
            success = channel.send(message, subject, severity)

            if success:
                breaker.record_success()
                outcomes[name] = True
            else:
                breaker.record_failure()
                outcomes[name] = False
                self._apply_failure_policy(name)

        return outcomes

    def _apply_failure_policy(self, channel_name: str) -> None:
        """Apply the configured failure policy for a failed channel delivery.

        Args:
            channel_name: Name of the channel that failed.

        Raises:
            AlertDeliveryError: If failure_policy is RAISE.
        """
        policy = self._config.failure_policy

        if policy == FailurePolicy.WARN:
            logger.warning(
                "Alert delivery failed for channel '%s' (policy: warn)",
                channel_name,
            )
        elif policy == FailurePolicy.RAISE:
            raise AlertDeliveryError(f"Channel '{channel_name}' delivery failed")
        elif policy == FailurePolicy.FALLBACK:
            logger.info(
                "Alert delivery failed for channel '%s', trying next channel (policy: fallback)",
                channel_name,
            )
