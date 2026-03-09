"""Unit tests for AlertConfig YAML parsing, FailurePolicy enum, and env var substitution."""

import pytest

from dq_framework.alerting import (
    AlertConfig,
    ChannelConfig,
    CircuitBreakerConfig,
    FailurePolicy,
)


@pytest.fixture
def full_alert_config():
    """Full alerts: YAML section parsed as a dict."""
    return {
        "enabled": True,
        "failure_policy": "warn",
        "channels": [
            {
                "type": "teams",
                "webhook_url": "https://hooks.example.com/webhook",
                "enabled": True,
            },
            {
                "type": "email",
                "smtp_host": "smtp.office365.com",
                "smtp_port": 587,
                "from_addr": "dq-alerts@hs2.org.uk",
                "to_addrs": ["data-team@hs2.org.uk"],
                "enabled": True,
            },
        ],
        "circuit_breaker": {
            "failure_threshold": 3,
            "cooldown_seconds": 120.0,
        },
        "templates": {
            "summary": "summary.txt.j2",
        },
    }


class TestAlertConfig:
    """Tests for AlertConfig.from_dict() YAML parsing."""

    def test_from_dict_none_returns_disabled(self):
        """AlertConfig.from_dict(None) returns AlertConfig(enabled=False)."""
        config = AlertConfig.from_dict(None)
        assert config.enabled is False

    def test_from_dict_empty_returns_disabled(self):
        """AlertConfig.from_dict({}) returns AlertConfig(enabled=False)."""
        config = AlertConfig.from_dict({})
        assert config.enabled is False

    def test_from_dict_full_config(self, full_alert_config):
        """AlertConfig.from_dict(full_config) parses all fields correctly."""
        config = AlertConfig.from_dict(full_alert_config)
        assert config.enabled is True
        assert config.failure_policy == FailurePolicy.WARN
        assert len(config.channels) == 2
        assert config.circuit_breaker.failure_threshold == 3
        assert config.circuit_breaker.cooldown_seconds == 120.0
        assert config.templates == {"summary": "summary.txt.j2"}

    def test_channel_config_parsing(self, full_alert_config):
        """ChannelConfig parses type, enabled flag, and extra settings dict."""
        config = AlertConfig.from_dict(full_alert_config)
        teams_channel = config.channels[0]
        assert teams_channel.type == "teams"
        assert teams_channel.enabled is True
        assert teams_channel.settings["webhook_url"] == "https://hooks.example.com/webhook"

        email_channel = config.channels[1]
        assert email_channel.type == "email"
        assert email_channel.settings["smtp_host"] == "smtp.office365.com"
        assert email_channel.settings["smtp_port"] == 587
        assert email_channel.settings["to_addrs"] == ["data-team@hs2.org.uk"]

    def test_circuit_breaker_defaults(self):
        """CircuitBreakerConfig has defaults (failure_threshold=5, cooldown_seconds=300.0)."""
        cb = CircuitBreakerConfig()
        assert cb.failure_threshold == 5
        assert cb.cooldown_seconds == 300.0

    def test_from_dict_missing_circuit_breaker_uses_defaults(self):
        """AlertConfig.from_dict with missing circuit_breaker key uses defaults."""
        data = {"enabled": True, "failure_policy": "warn", "channels": []}
        config = AlertConfig.from_dict(data)
        assert config.circuit_breaker.failure_threshold == 5
        assert config.circuit_breaker.cooldown_seconds == 300.0

    def test_failure_policy_enum_members(self):
        """FailurePolicy enum has WARN, RAISE, FALLBACK members."""
        assert FailurePolicy.WARN.value == "warn"
        assert FailurePolicy.RAISE.value == "raise"
        assert FailurePolicy.FALLBACK.value == "fallback"

    def test_from_dict_invalid_failure_policy_raises(self):
        """AlertConfig.from_dict with invalid failure_policy raises ValueError."""
        data = {"enabled": True, "failure_policy": "explode", "channels": []}
        with pytest.raises(ValueError, match="explode"):
            AlertConfig.from_dict(data)

    def test_env_var_substitution_replaces_existing(self, monkeypatch):
        """Environment variable substitution replaces ${VAR_NAME} when env var exists."""
        monkeypatch.setenv("TEST_WEBHOOK_URL", "https://resolved.example.com")
        data = {
            "enabled": True,
            "failure_policy": "warn",
            "channels": [
                {
                    "type": "teams",
                    "webhook_url": "${TEST_WEBHOOK_URL}",
                    "enabled": True,
                },
            ],
        }
        config = AlertConfig.from_dict(data)
        assert config.channels[0].settings["webhook_url"] == "https://resolved.example.com"

    def test_env_var_substitution_leaves_missing_unchanged(self, monkeypatch):
        """Env var substitution leaves ${VAR_NAME} unchanged when env var missing."""
        monkeypatch.delenv("NONEXISTENT_VAR_12345", raising=False)
        data = {
            "enabled": True,
            "failure_policy": "warn",
            "channels": [
                {
                    "type": "teams",
                    "webhook_url": "${NONEXISTENT_VAR_12345}",
                    "enabled": True,
                },
            ],
        }
        config = AlertConfig.from_dict(data)
        assert config.channels[0].settings["webhook_url"] == "${NONEXISTENT_VAR_12345}"

    def test_non_string_values_not_affected_by_env_substitution(self):
        """Non-string values (int, bool, list) are not affected by env var substitution."""
        data = {
            "enabled": True,
            "failure_policy": "warn",
            "channels": [
                {
                    "type": "teams",
                    "enabled": True,
                    "port": 443,
                    "tags": ["prod", "critical"],
                },
            ],
        }
        config = AlertConfig.from_dict(data)
        assert config.channels[0].settings["port"] == 443
        assert config.channels[0].settings["tags"] == ["prod", "critical"]
        assert config.channels[0].enabled is True
