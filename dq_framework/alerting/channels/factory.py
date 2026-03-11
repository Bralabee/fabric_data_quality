"""Channel factory -- creates AlertChannel instances from ChannelConfig.

Usage::

    from dq_framework.alerting.channels import create_channel
    from dq_framework.alerting.config import ChannelConfig

    config = ChannelConfig(type="teams", settings={"webhook_url": "https://..."})
    channel = create_channel(config)
"""

from __future__ import annotations

from ..config import ChannelConfig
from ..dispatcher import AlertChannel
from .email import EmailChannel
from .teams import TeamsChannel


def create_channel(config: ChannelConfig) -> AlertChannel:
    """Instantiate the correct AlertChannel subclass from a ChannelConfig.

    Args:
        config: Channel configuration with type and settings.

    Returns:
        Configured AlertChannel instance.

    Raises:
        ValueError: If the channel type is not recognized.
    """
    settings = config.settings

    if config.type == "teams":
        return TeamsChannel(
            webhook_url=settings["webhook_url"],
            timeout=settings.get("timeout", 30.0),
        )
    elif config.type == "email":
        return EmailChannel(
            smtp_host=settings["smtp_host"],
            smtp_port=settings.get("smtp_port", 587),
            from_addr=settings.get("from_addr", ""),
            to_addrs=settings.get("to_addrs", []),
            username=settings.get("username"),
            password=settings.get("password"),
            use_tls=settings.get("use_tls", True),
        )
    raise ValueError(f"Unknown channel type: {config.type!r}")
