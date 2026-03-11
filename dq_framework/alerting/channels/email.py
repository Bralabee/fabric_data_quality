"""SMTP email alert channel with HTML formatting.

Sends multipart HTML + plain-text email via SMTP with optional
STARTTLS and authentication.

Usage::

    from dq_framework.alerting.channels import EmailChannel

    channel = EmailChannel(
        smtp_host="smtp.office365.com",
        from_addr="alerts@hs2.org.uk",
        to_addrs=["data-team@hs2.org.uk"],
    )
    success = channel.send(html_body, "DQ Alert: orders - FAILED", "high")
"""

from __future__ import annotations

import logging
import smtplib
import ssl
from email.message import EmailMessage

from ..dispatcher import AlertChannel

logger = logging.getLogger(__name__)


class EmailChannel(AlertChannel):
    """Delivers alerts via SMTP email with HTML formatting.

    Args:
        smtp_host: SMTP server hostname.
        smtp_port: SMTP server port (typically 587 for STARTTLS).
        from_addr: Sender email address.
        to_addrs: List of recipient email addresses.
        username: SMTP authentication username (optional).
        password: SMTP authentication password (optional).
        use_tls: Whether to use STARTTLS (default True).
    """

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int = 587,
        from_addr: str = "",
        to_addrs: list[str] | None = None,
        username: str | None = None,
        password: str | None = None,
        use_tls: bool = True,
    ) -> None:
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port
        self._from_addr = from_addr
        self._to_addrs = to_addrs or []
        self._username = username
        self._password = password
        self._use_tls = use_tls

    def send(self, message: str, subject: str, severity: str) -> bool:
        """Send HTML email alert.

        Args:
            message: HTML message body.
            subject: Email subject line.
            severity: Alert severity level (unused but part of ABC contract).

        Returns:
            True if the email was sent successfully.
        """
        if not self._to_addrs:
            logger.warning("EmailChannel: no recipients configured")
            return False

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self._from_addr
        msg["To"] = ", ".join(self._to_addrs)
        msg.set_content(subject)  # plain-text fallback
        msg.add_alternative(message, subtype="html")

        try:
            with smtplib.SMTP(self._smtp_host, self._smtp_port) as server:
                if self._use_tls:
                    context = ssl.create_default_context()
                    server.starttls(context=context)
                if self._username and self._password:
                    server.login(self._username, self._password)
                server.send_message(msg)
            return True
        except smtplib.SMTPException as exc:
            logger.error("EmailChannel: SMTP error: %s", exc)
            return False
