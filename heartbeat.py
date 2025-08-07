"""System heartbeat utilities for uptime monitoring.

This module periodically emits heartbeat signals and checks for inactivity
within the arbitrage trading system. It is intended for lightweight uptime
monitoring and alerting.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class HeartbeatMonitor:
    """Track heartbeat signals and detect inactivity."""

    interval: float = 60.0
    last_heartbeat: float = field(default_factory=time.time)

    def send_heartbeat_signal(self) -> None:
        """Record a heartbeat and log the activity."""
        self.last_heartbeat = time.time()
        self.log_heartbeat()

    def check_for_inactivity(self, timeout: Optional[float] = None) -> bool:
        """Check if no heartbeat has been sent within the allowed timeframe.

        Args:
            timeout: Override for inactivity timeout in seconds. Defaults to
                twice the interval.

        Returns:
            True if inactivity detected, otherwise False.
        """
        limit = timeout if timeout is not None else self.interval * 2
        inactive = time.time() - self.last_heartbeat > limit
        if inactive:
            self.send_inactivity_alert()
        return inactive

    def send_inactivity_alert(self) -> None:
        """Emit an alert when the system appears inactive."""
        last = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.last_heartbeat))
        logger.error("INACTIVITY ALERT: no heartbeat since %s", last)

    def log_heartbeat(self) -> None:
        """Log the sending of a heartbeat signal."""
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.last_heartbeat))
        logger.info("Heartbeat sent at %s", ts)
