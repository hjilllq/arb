"""Инструменты мониторинга ресурсов.

Модуль отслеживает использование CPU, памяти и диска, чтобы торговая система
работала в пределах ресурсов. При превышении настроенных порогов
создаются записи об предупреждениях.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict

import psutil
from prometheus_client import Gauge

logger = logging.getLogger(__name__)

# Метрики Prometheus для загрузки ресурсов
CPU_USAGE = Gauge("cpu_usage_percent", "Использование CPU в процентах")
MEMORY_USAGE = Gauge("memory_usage_percent", "Использование памяти в процентах")


@dataclass
class ResourceMonitor:
    """Следить за использованием ресурсов системы и отправлять предупреждения."""

    cpu_threshold: float = 90.0
    memory_threshold: float = 90.0
    disk_threshold: float = 90.0
    disk_path: str = "/"

    def check_resources(self) -> Dict[str, float]:
        """Вернуть текущее использование ресурсов и предупредить при превышении."""
        stats = {
            "cpu": self.monitor_cpu(),
            "memory": self.monitor_memory(),
            "disk": self.monitor_disk(),
        }
        CPU_USAGE.set(stats["cpu"])
        MEMORY_USAGE.set(stats["memory"])
        if (
            stats["cpu"] > self.cpu_threshold
            or stats["memory"] > self.memory_threshold
            or stats["disk"] > self.disk_threshold
        ):
            self.send_resource_alert(stats)
        return stats

    def send_resource_alert(self, stats: Dict[str, float]) -> None:
        """Логировать предупреждение при превышении порогов использования."""
        logger.error("RESOURCE ALERT: %s", stats)

    def monitor_cpu(self) -> float:
        """Вернуть текущий процент загрузки CPU (центрального процессора) во всей системе."""
        return psutil.cpu_percent(interval=0.0)

    def monitor_memory(self) -> float:
        """Вернуть текущий процент использования памяти."""
        return psutil.virtual_memory().percent

    def monitor_disk(self) -> float:
        """Вернуть процент использования диска для указанного пути."""
        return psutil.disk_usage(self.disk_path).percent
