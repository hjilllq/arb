"""Модуль для автоматического перезапуска торговой системы.

`RestartManager` запускает `TradingSystem` и следит за его состоянием с
помощью `HealthChecker`. Если основной процесс падает или проверка
здоровья сообщает о деградации, система останавливается, записывает
ошибку и по истечении небольшой задержки запускается снова. Все случаи
перезапуска логируются, а при наличии `NotificationManager` отправляется
уведомление.
"""
from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import Callable, Optional

from logger import log_event, log_error
from health_checker import HealthChecker
from notification_manager import NotificationManager
from main import TradingSystem


@dataclass
class RestartManager:
    """Запуск и автоматический перезапуск `TradingSystem`."""

    system_factory: Callable[[], TradingSystem] = TradingSystem
    notifier: Optional[NotificationManager] = None
    check_interval: float = 30.0
    restart_delay: float = 5.0

    async def _monitor(self, system: TradingSystem) -> None:
        """Периодически проверять здоровье компонентов.

        Если `HealthChecker` сообщает о проблемах, генерируется исключение,
        чтобы внешний цикл мог перезапустить систему.
        """

        checker = HealthChecker(trading_bot=None, notifier=self.notifier)
        while True:
            status = await checker.check_system()
            if not all(status.values()):
                raise RuntimeError(f"Component failure: {status}")
            await asyncio.sleep(self.check_interval)

    async def run(self) -> None:
        """Запустить торговую систему и перезапускать при сбоях."""

        while True:
            system = self.system_factory()
            main_task = asyncio.create_task(system.run())
            monitor_task = asyncio.create_task(self._monitor(system))
            done, pending = await asyncio.wait(
                {main_task, monitor_task}, return_when=asyncio.FIRST_EXCEPTION
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task

            restart = False
            if monitor_task in done and monitor_task.exception():
                restart = True
                exc = monitor_task.exception()
                log_error("Health monitor detected failure", exc)  # type: ignore[arg-type]
                if self.notifier:
                    self.notifier.send_telegram_notification(
                        f"Health monitor detected failure: {exc}"
                    )
                system.stop_trading()
            if main_task in done and main_task.exception():
                restart = True
                exc = main_task.exception()
                log_error("Trading system crashed", exc)
                if self.notifier:
                    self.notifier.send_telegram_notification(
                        f"Trading system crashed: {exc}"
                    )

            if not restart:
                log_event("Trading system exited normally, stop restart manager")
                break

            log_event("Restarting trading system")
            await asyncio.sleep(self.restart_delay)


async def run_with_restart(notifier: Optional[NotificationManager] = None) -> None:
    """Удобная обёртка для запуска менеджера перезапусков."""

    manager = RestartManager(notifier=notifier)
    await manager.run()


if __name__ == "__main__":  # pragma: no cover - ручной запуск
    asyncio.run(run_with_restart())
