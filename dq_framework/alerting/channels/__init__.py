"""Alert delivery channels subpackage.

Re-exports the concrete channel implementations and factory function::

    from dq_framework.alerting.channels import TeamsChannel, EmailChannel, create_channel
"""

from .email import EmailChannel
from .factory import create_channel
from .teams import TeamsChannel

__all__ = [
    "EmailChannel",
    "TeamsChannel",
    "create_channel",
]
