"""Resource monitoring utilities.

This module tracks CPU, memory, and disk usage to ensure the trading system
runs within resource limits. Alerts are logged when usage exceeds configured
thresholds.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict

import psutil

logger = logging.getLogger(__name__)


@dataclass
class ResourceMonitor:
    """Monitor system resource utilisation and emit alerts."""

    cpu_threshold: float = 90.0
    memory_threshold: float = 90.0
    disk_threshold: float = 90.0
    disk_path: str = "/"

    def check_resources(self) -> Dict[str, float]:
        """Return current resource usage and alert if limits are exceeded."""
        stats = {
            "cpu": self.monitor_cpu(),
            "memory": self.monitor_memory(),
            "disk": self.monitor_disk(),
        }
        if (
            stats["cpu"] > self.cpu_threshold
            or stats["memory"] > self.memory_threshold
            or stats["disk"] > self.disk_threshold
        ):
            self.send_resource_alert(stats)
        return stats

    def send_resource_alert(self, stats: Dict[str, float]) -> None:
        """Log an alert when resources exceed thresholds."""
        logger.error("RESOURCE ALERT: %s", stats)

    def monitor_cpu(self) -> float:
        """Return the current system-wide CPU utilisation percentage."""
        return psutil.cpu_percent(interval=0.0)

    def monitor_memory(self) -> float:
        """Return the current memory utilisation percentage."""
        return psutil.virtual_memory().percent

    def monitor_disk(self) -> float:
        """Return the current disk usage percentage for the configured path."""
        return psutil.disk_usage(self.disk_path).percent
