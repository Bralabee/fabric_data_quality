"""Tests for EmailChannel with mocked smtplib."""

from __future__ import annotations

import smtplib
from unittest.mock import MagicMock, patch

import pytest

from dq_framework.alerting.channels.email import EmailChannel
from dq_framework.alerting.channels.factory import create_channel
from dq_framework.alerting.config import ChannelConfig


class TestEmailChannel:
    """Tests for EmailChannel.send() with mocked smtplib."""

    def _make_channel(self, **kwargs) -> EmailChannel:
        defaults = {
            "smtp_host": "smtp.test.com",
            "smtp_port": 587,
            "from_addr": "alerts@test.com",
            "to_addrs": ["user@test.com"],
            "use_tls": True,
        }
        defaults.update(kwargs)
        return EmailChannel(**defaults)

    def test_send_success(self):
        """Mock SMTP -> returns True, send_message called with correct headers."""
        channel = self._make_channel()

        with patch("dq_framework.alerting.channels.email.smtplib.SMTP") as MockSMTP:
            mock_server = MagicMock()
            MockSMTP.return_value.__enter__ = MagicMock(return_value=mock_server)
            MockSMTP.return_value.__exit__ = MagicMock(return_value=False)

            result = channel.send("<h1>Alert</h1>", "DQ Alert: test", "medium")

        assert result is True
        mock_server.send_message.assert_called_once()
        sent_msg = mock_server.send_message.call_args[0][0]
        assert sent_msg["Subject"] == "DQ Alert: test"
        assert sent_msg["From"] == "alerts@test.com"
        assert "user@test.com" in sent_msg["To"]

    def test_send_html_content(self):
        """Message has HTML alternative with the message parameter content."""
        channel = self._make_channel()

        with patch("dq_framework.alerting.channels.email.smtplib.SMTP") as MockSMTP:
            mock_server = MagicMock()
            MockSMTP.return_value.__enter__ = MagicMock(return_value=mock_server)
            MockSMTP.return_value.__exit__ = MagicMock(return_value=False)

            channel.send("<h1>Test HTML</h1>", "Subject", "medium")

        sent_msg = mock_server.send_message.call_args[0][0]
        # The message should be multipart with HTML content
        html_parts = [
            part for part in sent_msg.walk()
            if part.get_content_type() == "text/html"
        ]
        assert len(html_parts) == 1
        assert "<h1>Test HTML</h1>" in html_parts[0].get_content()

    def test_send_with_starttls(self):
        """SMTP starttls called with ssl context when use_tls=True."""
        channel = self._make_channel(use_tls=True)

        with patch("dq_framework.alerting.channels.email.smtplib.SMTP") as MockSMTP:
            mock_server = MagicMock()
            MockSMTP.return_value.__enter__ = MagicMock(return_value=mock_server)
            MockSMTP.return_value.__exit__ = MagicMock(return_value=False)

            channel.send("<p>msg</p>", "Subject", "medium")

        mock_server.starttls.assert_called_once()
        # Verify ssl context was passed
        call_kwargs = mock_server.starttls.call_args
        assert call_kwargs.kwargs.get("context") is not None or (
            call_kwargs.args and call_kwargs.args[0] is not None
        )

    def test_send_with_auth(self):
        """SMTP login called with username/password when provided."""
        channel = self._make_channel(username="user@test.com", password="secret123")

        with patch("dq_framework.alerting.channels.email.smtplib.SMTP") as MockSMTP:
            mock_server = MagicMock()
            MockSMTP.return_value.__enter__ = MagicMock(return_value=mock_server)
            MockSMTP.return_value.__exit__ = MagicMock(return_value=False)

            channel.send("<p>msg</p>", "Subject", "medium")

        mock_server.login.assert_called_once_with("user@test.com", "secret123")

    def test_send_no_recipients(self):
        """Empty to_addrs -> returns False without connecting."""
        channel = self._make_channel(to_addrs=[])

        with patch("dq_framework.alerting.channels.email.smtplib.SMTP") as MockSMTP:
            result = channel.send("<p>msg</p>", "Subject", "medium")

        assert result is False
        MockSMTP.assert_not_called()

    def test_send_smtp_error(self):
        """SMTP raising SMTPException -> returns False."""
        channel = self._make_channel()

        with patch("dq_framework.alerting.channels.email.smtplib.SMTP") as MockSMTP:
            mock_server = MagicMock()
            MockSMTP.return_value.__enter__ = MagicMock(return_value=mock_server)
            MockSMTP.return_value.__exit__ = MagicMock(return_value=False)
            mock_server.send_message.side_effect = smtplib.SMTPException("fail")

            result = channel.send("<p>msg</p>", "Subject", "medium")

        assert result is False

    def test_send_no_tls(self):
        """use_tls=False -> starttls NOT called."""
        channel = self._make_channel(use_tls=False)

        with patch("dq_framework.alerting.channels.email.smtplib.SMTP") as MockSMTP:
            mock_server = MagicMock()
            MockSMTP.return_value.__enter__ = MagicMock(return_value=mock_server)
            MockSMTP.return_value.__exit__ = MagicMock(return_value=False)

            channel.send("<p>msg</p>", "Subject", "medium")

        mock_server.starttls.assert_not_called()


# ---------------------------------------------------------------------------
# Factory test for email
# ---------------------------------------------------------------------------


class TestEmailChannelFactory:
    """Factory test for email channel creation."""

    def test_create_email_channel(self):
        config = ChannelConfig(
            type="email",
            settings={
                "smtp_host": "smtp.test.com",
                "smtp_port": 587,
                "from_addr": "alerts@test.com",
                "to_addrs": ["user@test.com"],
            },
        )
        channel = create_channel(config)
        assert isinstance(channel, EmailChannel)
