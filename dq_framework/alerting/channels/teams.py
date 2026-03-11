"""Microsoft Teams alert channel via Power Automate Workflows webhook.

Posts Adaptive Card JSON wrapped in the Workflows envelope format
to a Power Automate callback URL using httpx.

Usage::

    from dq_framework.alerting.channels import TeamsChannel

    channel = TeamsChannel(webhook_url="https://prod.logic.azure.com/workflows/...")
    success = channel.send(adaptive_card_json, "DQ Alert: orders", "high")
"""

from __future__ import annotations

import json
import logging

import httpx

from ..dispatcher import AlertChannel

logger = logging.getLogger(__name__)


class TeamsChannel(AlertChannel):
    """Delivers alerts to Microsoft Teams via Power Automate Workflows webhook.

    The ``message`` parameter to :meth:`send` must be a JSON string representing
    an Adaptive Card body.  The method wraps it in the Workflows envelope format
    (``type: "message"`` with ``attachments`` array) before POSTing.

    Args:
        webhook_url: Power Automate Workflows callback URL.
        timeout: HTTP request timeout in seconds.
    """

    def __init__(self, webhook_url: str, timeout: float = 30.0) -> None:
        self._webhook_url = webhook_url
        self._timeout = timeout

    def send(self, message: str, subject: str, severity: str) -> bool:
        """Post Adaptive Card to Teams webhook.

        Args:
            message: JSON string of an Adaptive Card body.
            subject: Alert subject line (unused by Teams but part of ABC contract).
            severity: Alert severity level (unused by Teams but part of ABC contract).

        Returns:
            True if the webhook accepted the request (HTTP 200 or 202).
        """
        try:
            card_body = json.loads(message)
        except json.JSONDecodeError:
            logger.error("TeamsChannel: message is not valid JSON")
            return False

        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": card_body,
                }
            ],
        }

        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.post(
                    self._webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                if response.status_code in (200, 202):
                    return True
                logger.warning(
                    "TeamsChannel: webhook returned %d: %s",
                    response.status_code,
                    response.text[:200],
                )
                return False
        except httpx.HTTPError as exc:
            logger.error("TeamsChannel: HTTP error: %s", exc)
            return False
