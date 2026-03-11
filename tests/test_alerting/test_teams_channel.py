"""Tests for TeamsChannel and Adaptive Card template rendering."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from dq_framework.alerting.channels.factory import create_channel
from dq_framework.alerting.channels.teams import TeamsChannel
from dq_framework.alerting.config import ChannelConfig
from dq_framework.alerting.formatter import AlertFormatter

# ---------------------------------------------------------------------------
# TeamsChannel unit tests
# ---------------------------------------------------------------------------


class TestTeamsChannel:
    """Tests for TeamsChannel.send() with mocked httpx."""

    def _make_channel(self, url: str = "https://test.logic.azure.com/workflows/abc") -> TeamsChannel:
        return TeamsChannel(webhook_url=url, timeout=5.0)

    def _valid_card_json(self) -> str:
        return json.dumps({"type": "AdaptiveCard", "version": "1.3", "body": []})

    def test_send_success(self):
        """POST returning 200 -> returns True, payload has Workflows envelope."""
        channel = self._make_channel()
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("dq_framework.alerting.channels.teams.httpx.Client") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value.__enter__ = MagicMock(return_value=mock_client)
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.post.return_value = mock_response

            result = channel.send(self._valid_card_json(), "Test Subject", "medium")

        assert result is True
        # Verify payload structure
        call_kwargs = mock_client.post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert payload["type"] == "message"
        assert len(payload["attachments"]) == 1
        assert payload["attachments"][0]["contentType"] == "application/vnd.microsoft.card.adaptive"

    def test_send_accepts_202(self):
        """POST returning 202 (async accepted) -> returns True."""
        channel = self._make_channel()
        mock_response = MagicMock()
        mock_response.status_code = 202

        with patch("dq_framework.alerting.channels.teams.httpx.Client") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value.__enter__ = MagicMock(return_value=mock_client)
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.post.return_value = mock_response

            result = channel.send(self._valid_card_json(), "Test Subject", "high")

        assert result is True

    def test_send_http_error(self):
        """POST raising httpx.HTTPError -> returns False, no exception propagated."""
        import httpx

        channel = self._make_channel()

        with patch("dq_framework.alerting.channels.teams.httpx.Client") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value.__enter__ = MagicMock(return_value=mock_client)
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.post.side_effect = httpx.HTTPError("connection failed")

            result = channel.send(self._valid_card_json(), "Test Subject", "high")

        assert result is False

    def test_send_non_success_status(self):
        """POST returning 400 -> returns False."""
        channel = self._make_channel()
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"

        with patch("dq_framework.alerting.channels.teams.httpx.Client") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value.__enter__ = MagicMock(return_value=mock_client)
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.post.return_value = mock_response

            result = channel.send(self._valid_card_json(), "Test Subject", "medium")

        assert result is False

    def test_send_invalid_json_message(self):
        """Pass non-JSON string -> returns False, logs error."""
        channel = self._make_channel()

        result = channel.send("this is not json", "Test Subject", "medium")

        assert result is False


# ---------------------------------------------------------------------------
# Adaptive Card Template tests
# ---------------------------------------------------------------------------


class TestAdaptiveCardTemplate:
    """Tests for adaptive_card.json.j2 template rendering."""

    @pytest.fixture()
    def formatter(self) -> AlertFormatter:
        return AlertFormatter()

    @pytest.fixture()
    def sample_results(self) -> dict:
        return {
            "suite_name": "orders_dq",
            "batch_name": "batch_2026_03_09",
            "success": True,
            "success_rate": 95.5,
            "evaluated_checks": 20,
            "failed_checks": 1,
            "failed_expectations": [],
            "severity_stats": {},
            "threshold_failures": [],
            "timestamp": "2026-03-09T12:00:00Z",
        }

    def test_renders_valid_json(self, formatter: AlertFormatter, sample_results: dict):
        """Rendered template produces valid JSON with key fields."""
        rendered = formatter.render("adaptive_card.json.j2", sample_results)
        card = json.loads(rendered)

        assert card["type"] == "AdaptiveCard"
        assert card["version"] == "1.3"
        # Body should contain suite_name somewhere
        body_text = json.dumps(card["body"])
        assert "orders_dq" in body_text
        assert "95.5" in body_text
        assert "2026-03-09T12:00:00Z" in body_text

    def test_failed_expectations(self, formatter: AlertFormatter, sample_results: dict):
        """Rendered template includes failed expectation text."""
        sample_results["success"] = False
        sample_results["failed_expectations"] = [
            {"expectation": "expect_column_values_to_not_be_null", "column": "order_id", "severity": "high"},
            {"expectation": "expect_column_values_to_be_unique", "column": "email", "severity": "critical"},
        ]

        rendered = formatter.render("adaptive_card.json.j2", sample_results)
        card = json.loads(rendered)
        body_text = json.dumps(card["body"])

        assert "expect_column_values_to_not_be_null" in body_text
        assert "order_id" in body_text
        assert "expect_column_values_to_be_unique" in body_text

    def test_severity_stats(self, formatter: AlertFormatter, sample_results: dict):
        """Rendered template includes severity stats fact set."""
        sample_results["severity_stats"] = {
            "high": {"total": 5, "passed": 3},
            "medium": {"total": 10, "passed": 10},
        }

        rendered = formatter.render("adaptive_card.json.j2", sample_results)
        card = json.loads(rendered)
        body_text = json.dumps(card["body"])

        assert "HIGH" in body_text
        assert "3/5 passed" in body_text


# ---------------------------------------------------------------------------
# Factory tests (create_channel for teams)
# ---------------------------------------------------------------------------


class TestChannelFactory:
    """Tests for create_channel factory function."""

    def test_create_teams_channel(self):
        config = ChannelConfig(
            type="teams",
            settings={"webhook_url": "https://test.logic.azure.com/workflows/abc"},
        )
        channel = create_channel(config)
        assert isinstance(channel, TeamsChannel)

    def test_create_unknown_channel(self):
        config = ChannelConfig(type="unknown", settings={})
        with pytest.raises(ValueError, match="Unknown channel type"):
            create_channel(config)
