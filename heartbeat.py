"""Утилиты контроля "heartbeat" для мониторинга доступности.

Модуль периодически отправляет сигналы активности и проверяет бездействие
внутри арбитражной торговой системы. Предназначен для простого контроля
работоспособности и оповещений.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class HeartbeatMonitor:
    """Отслеживать сигналы активности и обнаруживать бездействие."""

    interval: float = 60.0
    last_heartbeat: float = field(default_factory=time.time)

    def send_heartbeat_signal(self) -> None:
        """Зафиксировать сигнал и записать активность."""
        self.last_heartbeat = time.time()
        self.log_heartbeat()

    def check_for_inactivity(self, timeout: Optional[float] = None) -> bool:
        """Проверить, не было ли сигналов в разрешённый интервал времени.

        Args:
            timeout: Переопределение тайм-аута бездействия в секундах.
                По умолчанию равно удвоенному интервалу.

        Returns:
            True, если бездействие обнаружено, иначе False.
        """
        limit = timeout if timeout is not None else self.interval * 2
        inactive = time.time() - self.last_heartbeat > limit
        if inactive:
            self.send_inactivity_alert()
        return inactive

    def send_inactivity_alert(self) -> None:
        """Отправить предупреждение, когда система выглядит неактивной."""
        last = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.last_heartbeat))
        logger.error("INACTIVITY ALERT: no heartbeat since %s", last)

    def log_heartbeat(self) -> None:
        """Логировать отправку сигнала активности."""
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.last_heartbeat))
        logger.info("Heartbeat sent at %s", ts)
