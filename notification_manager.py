"""Simple notification helper.

In the future this module will send messages to Telegram or other services.
For now it simply prints the message so that we can see what would be sent.
"""

from __future__ import annotations

from typing import Any


def notify(message: Any) -> None:
    """Send a notification.

    The current implementation is intentionally tiny.  It simply prints the
    message so that it appears in logs.  Later this function can be replaced
    with a real Telegram sender.
    """

    print(f"NOTIFY: {message}")
