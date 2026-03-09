"""YAML alert configuration parsing into typed dataclasses.

Parses the ``alerts:`` section from YAML config files into structured
dataclass instances with environment variable substitution support.

Example YAML::

    alerts:
      enabled: true
      failure_policy: warn
      channels:
        - type: teams
          webhook_url: "${TEAMS_WEBHOOK_URL}"
          enabled: true
      circuit_breaker:
        failure_threshold: 5
        cooldown_seconds: 300

Usage::

    from dq_framework.alerting import AlertConfig

    config = AlertConfig.from_dict(yaml_data.get("alerts"))
"""

from __future__ import annotations

import copy
import logging
import os
import re
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

# Pattern to match ${VAR_NAME} in strings
_ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


class FailurePolicy(Enum):
    """How to handle alert delivery failures.

    Members:
        WARN: Log a warning and continue the pipeline.
        RAISE: Raise ``AlertDeliveryError`` to halt the pipeline.
        FALLBACK: Try the next configured channel.
    """

    WARN = "warn"
    RAISE = "raise"
    FALLBACK = "fallback"


class AlertDeliveryError(Exception):
    """Raised when alert delivery fails and the failure policy is RAISE."""


@dataclass
class ChannelConfig:
    """Configuration for a single alert delivery channel.

    Attributes:
        type: Channel type identifier (e.g. ``"teams"``, ``"email"``).
        enabled: Whether this channel is active.
        settings: Channel-specific settings (webhook_url, smtp_host, etc.).
    """

    type: str
    enabled: bool = True
    settings: dict = field(default_factory=dict)


@dataclass
class CircuitBreakerConfig:
    """Configuration for the alert circuit breaker.

    Attributes:
        failure_threshold: Consecutive failures before opening the circuit.
        cooldown_seconds: Seconds to wait before allowing a test request.
    """

    failure_threshold: int = 5
    cooldown_seconds: float = 300.0


@dataclass
class AlertConfig:
    """Parsed alert configuration from the YAML ``alerts:`` section.

    Attributes:
        enabled: Whether alerting is active.
        failure_policy: How to handle delivery failures.
        channels: List of configured alert channels.
        circuit_breaker: Circuit breaker settings.
        templates: Template name overrides.
    """

    enabled: bool = True
    failure_policy: FailurePolicy = FailurePolicy.WARN
    channels: list[ChannelConfig] = field(default_factory=list)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    templates: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict | None) -> AlertConfig:
        """Parse an ``alerts:`` YAML section into an AlertConfig.

        Args:
            data: Dictionary from YAML parsing, or None/empty dict.

        Returns:
            Configured AlertConfig instance. Returns a disabled config
            if *data* is ``None`` or empty.

        Raises:
            ValueError: If ``failure_policy`` is not a valid FailurePolicy member.
        """
        if not data:
            return cls(enabled=False)

        # Deep copy to avoid mutating caller's dict
        data = copy.deepcopy(data)
        _resolve_env_vars(data)

        # Parse failure policy
        policy_str = data.get("failure_policy", "warn")
        try:
            failure_policy = FailurePolicy(policy_str)
        except ValueError:
            valid = [p.value for p in FailurePolicy]
            raise ValueError(
                f"Invalid failure_policy: {policy_str!r}. Must be one of {valid}"
            )

        # Parse channels
        channels = []
        for ch_data in data.get("channels", []):
            ch_type = ch_data.pop("type")
            ch_enabled = ch_data.pop("enabled", True)
            channels.append(
                ChannelConfig(type=ch_type, enabled=ch_enabled, settings=ch_data)
            )

        # Parse circuit breaker
        cb_data = data.get("circuit_breaker", {})
        circuit_breaker = CircuitBreakerConfig(
            failure_threshold=cb_data.get("failure_threshold", 5),
            cooldown_seconds=cb_data.get("cooldown_seconds", 300.0),
        )

        # Parse templates
        templates = data.get("templates", {})

        return cls(
            enabled=data.get("enabled", True),
            failure_policy=failure_policy,
            channels=channels,
            circuit_breaker=circuit_breaker,
            templates=templates,
        )


def _resolve_env_vars(data: dict) -> dict:
    """Recursively substitute ``${VAR_NAME}`` patterns in string values.

    Walks the dictionary in-place, replacing ``${VAR_NAME}`` with the
    corresponding environment variable value. Logs a warning for any
    unresolved variables (env var not set).

    Args:
        data: Dictionary to process (modified in-place).

    Returns:
        The same dictionary with substitutions applied.
    """
    for key, value in data.items():
        if isinstance(value, str):
            data[key] = _ENV_VAR_PATTERN.sub(_env_replacer, value)
        elif isinstance(value, dict):
            _resolve_env_vars(value)
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, str):
                    value[i] = _ENV_VAR_PATTERN.sub(_env_replacer, item)
                elif isinstance(item, dict):
                    _resolve_env_vars(item)
    return data


def _env_replacer(match: re.Match) -> str:
    """Replace a single ``${VAR_NAME}`` match with the env var value."""
    var_name = match.group(1)
    value = os.environ.get(var_name)
    if value is None:
        logger.warning("Unresolved environment variable: ${%s}", var_name)
        return match.group(0)  # Leave unchanged
    return value
