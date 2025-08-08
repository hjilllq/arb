"""Простые утилиты для мониторинга состояния системы.

Модуль выполняет проверки, чтобы убедиться, что основные компоненты
арбитражной системы работают. Он отслеживает доступность API Bybit, базы
данных сделок, соединения по WebSocket и активность торгового процесса.
Результаты проверок экспортируются в Prometheus и при сбоях отправляются
уведомления."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Optional

from prometheus_client import Gauge, start_http_server

from bybit_api import BybitAPI
from database import TradeDatabase
from exchange_manager import ExchangeManager
from monitor import ResourceMonitor
from notification_manager import NotificationManager
from risk_manager import RiskManager
from trading_bot import TradingBot


logger = logging.getLogger(__name__)

# Метрики Prometheus: 1 — компонент в порядке, 0 — недоступен
API_UP = Gauge("bybit_api_up", "Доступность REST API Bybit")
DB_UP = Gauge("trade_database_up", "Состояние базы данных сделок")
WS_UP = Gauge("bybit_ws_up", "Доступность WebSocket соединения")
TRADING_UP = Gauge("trading_process_up", "Активность торгового процесса")
BALANCE_OK = Gauge("balance_ok", "Достаточность баланса для торговли")


@dataclass
class HealthChecker:
    """Выполнять проверки состояния внешних зависимостей."""

    db_path: str = "trades.db"
    ws_url: str = "wss://stream.bybit.com/v5/public/quote"
    notifier: Optional[NotificationManager] = None
    trading_bot: Optional[TradingBot] = None
    exchange_manager: Optional[ExchangeManager] = None
    risk_manager: Optional[RiskManager] = None
    resource_monitor: ResourceMonitor = field(default_factory=ResourceMonitor)
    min_balance: float = 0.0

    # ------------------------------------------------------------------
    def start_metrics_server(self, port: int = 8000) -> None:
        """Запустить HTTP-сервер для экспорта метрик Prometheus."""
        start_http_server(port)

    # ------------------------------------------------------------------
    async def check_system(self) -> Dict[str, bool]:
        """Проверить состояние всех компонентов и обновить метрики."""
        api_ok = await self.check_api_health()
        db_ok = await self.check_database_health()
        ws_ok = await self.check_websocket_health()
        trading_ok = self.check_trading_process()
        balance_ok = await self.check_balance()
        resources = self.resource_monitor.check_resources()
        status = {
            "api": api_ok,
            "database": db_ok,
            "websocket": ws_ok,
            "trading": trading_ok,
            "balance": balance_ok,
        }

        API_UP.set(1 if api_ok else 0)
        DB_UP.set(1 if db_ok else 0)
        WS_UP.set(1 if ws_ok else 0)
        TRADING_UP.set(1 if trading_ok else 0)
        BALANCE_OK.set(1 if balance_ok else 0)

        self.log_health_status(status, resources)
        all_ok = all(status.values())
        if not all_ok:
            self.send_health_alert(f"System health degraded: {status}")
            if self.risk_manager:
                self.risk_manager.set_safety_factor(0.5, "health degraded")
        else:
            if self.risk_manager:
                self.risk_manager.reset_safety_factor()
        return status

    # ------------------------------------------------------------------
    def send_health_alert(self, message: str) -> None:
        """Отправить предупреждение при сбое проверки состояния системы."""
        logger.error("HEALTH ALERT: %s", message)
        if self.notifier:
            # уведомление отправляется только через Telegram для простоты
            self.notifier.send_telegram_notification(message)

    # ------------------------------------------------------------------
    async def check_api_health(self) -> bool:
        """Убедиться, что REST API Bybit доступно, запросив данные спота."""
        try:
            async with BybitAPI() as api:
                await api.get_spot_data("BTCUSDT")
            return True
        except Exception as exc:  # pragma: no cover - сеть
            logger.warning("API health check failed: %s", exc)
            return False

    async def check_database_health(self) -> bool:
        """Убедиться, что база данных доступна и выполняет запросы."""
        try:
            async with TradeDatabase(self.db_path) as db:
                await db.get_trade_statistics()
            return True
        except Exception as exc:  # pragma: no cover - непредвиденная ошибка базы данных
            logger.warning("Database health check failed: %s", exc)
            return False

    async def check_websocket_health(self) -> bool:
        """Проверить возможность подключения к WebSocket."""
        try:
            import websockets  # type: ignore
        except Exception:  # pragma: no cover - библиотека отсутствует
            logger.warning("websockets library not installed")
            return False
        try:
            async with websockets.connect(self.ws_url):
                return True
        except Exception as exc:  # pragma: no cover - сеть
            logger.warning("WebSocket health check failed: %s", exc)
            return False

    async def check_balance(self) -> bool:
        """Проверить достаточность баланса для торговли."""

        if not self.exchange_manager:
            return True
        try:
            data = await self.exchange_manager.check_balance()
            balance = float(data.get("USDT", 0))
            if balance < self.min_balance:
                self.send_health_alert(
                    f"Balance {balance:.2f} below {self.min_balance}"
                )
                return False
            return True
        except Exception as exc:  # pragma: no cover - непредвиденная ошибка сети
            logger.warning("Balance check failed: %s", exc)
            self.send_health_alert("Balance check failed")
            return False

    def check_trading_process(self) -> bool:
        """Проверить, активен ли торговый процесс (бот)."""
        return bool(self.trading_bot and self.trading_bot.active)

    def log_health_status(self, status: Dict[str, bool], resources: Dict[str, float]) -> None:
        """Логировать текущее состояние и загрузку ресурсов."""
        logger.info(
            "Health status - API: %s, Database: %s, WebSocket: %s, Trading: %s",
            status["api"],
            status["database"],
            status["websocket"],
            status["trading"],
        )
        logger.info(
            "Resource usage - CPU: %.1f%%, Memory: %.1f%%",
            resources["cpu"],
            resources["memory"],
        )
