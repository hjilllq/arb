"""Точка входа для запуска арбитражной торговой системы.

Модуль связывает загрузку конфигурации, инициализацию системы и управление
торговым циклом. Он предоставляет простой класс "TradingSystem" с методами для
запуска и остановки торговли. Дизайн намеренно лёгкий, чтобы служить основой
для более сложной оркестрации."""
from __future__ import annotations

import asyncio
from typing import Optional

from config import Config, load_config, validate_config
from exchange_manager import ExchangeManager
from heartbeat import HeartbeatMonitor
from notification_manager import NotificationManager
from error_handler import install_global_handler, handle_error
import os

from logger import log_event, configure_logging


class TradingSystem:
    """Главный контроллер, отвечающий за работу торгового бота."""

    def __init__(self) -> None:
        self.config: Optional[Config] = None
        self.exchange: Optional[ExchangeManager] = None
        self.heartbeat = HeartbeatMonitor()
        self.notifier: Optional[NotificationManager] = None
        self._running = False

    def check_environment(self) -> Config:
        """Загрузить и проверить конфигурацию окружения."""
        cfg = load_config()
        configure_logging(cfg.log_level, key=os.getenv("FERNET_KEY"))
        validate_config(cfg)
        log_event("Environment validated")
        return cfg

    async def initialize_system(self) -> None:
        """Инициализировать основные компоненты, необходимые для торговли."""
        self.config = self.check_environment()
        self.notifier = NotificationManager(
            telegram_token=self.config.telegram_token or None,
            telegram_chat_id=self.config.telegram_chat_id or None,
            email_sender=self.config.email_sender or None,
            email_host=self.config.email_host,
            email_port=self.config.email_port,
            slack_webhook_url=self.config.slack_webhook_url or None,
            sms_api_url=self.config.sms_api_url or None,
        )
        install_global_handler(self.notifier)
        self.exchange = ExchangeManager(self.config, notifier=self.notifier)
        log_event("System initialization complete")

    async def start_trading(self) -> None:
        """Начать торговый цикл.

        Текущая реализация просто отправляет сигналы активности через
        равные промежутки времени. Реальную логику торговли следует
        добавить в отмеченных местах.
        """
        if not self.exchange:
            raise RuntimeError("System not initialized")
        self._running = True
        log_event("Trading started")
        try:
            while self._running:
                self.heartbeat.send_heartbeat_signal()
                await asyncio.sleep(self.heartbeat.interval)
        finally:
            log_event("Trading loop exited")

    def stop_trading(self) -> None:
        """Остановить торговый цикл."""
        self._running = False
        log_event("Trading stopped")

    async def run(self) -> None:
        """Запустить торговую систему до остановки."""
        try:
            await self.initialize_system()
            await self.start_trading()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - защитное программирование
            handle_error("Unhandled exception in run", exc, self.notifier)
        finally:
            self.stop_trading()
            if self.exchange:
                await self.exchange.close()


if __name__ == "__main__":
    asyncio.run(TradingSystem().run())
