"""Наблюдение и автоматический контроль рисков в реальном времени.

Модуль содержит класс :class:`RiskMonitor`, который периодически получает
данные о балансе, ценах и открытых позициях. Полученная информация
передается в :class:`RiskManager` для расчета волатильности и проверки
лимитов. Если превышены пороги риска, менеджер автоматически снижает
объём позиции или приостанавливает торговлю и отправляет уведомления.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from error_handler import handle_error
from position_manager import PositionManager
from risk_manager import RiskManager


@dataclass
class RiskMonitor:
    """Фоновый монитор рисков, работающий без вмешательства пользователя."""

    exchange: Any
    risk_manager: RiskManager
    pair: str
    position_manager: PositionManager | None = None
    interval: float = 60.0
    exchange_name: str = "bybit"
    running: bool = False

    async def check_once(self) -> None:
        """Выполнить один цикл проверки рисков.

        Запрашивает баланс и цену, обновляет метрики риска и проверяет лимит
        по количеству открытых позиций. Если выявлены нарушения, соответствующие
        методы :class:`RiskManager` самостоятельно применят защитные меры и
        отправят уведомления.
        """

        balance = await self.exchange.check_balance(self.exchange_name)
        price_info = await self.exchange.get_spot_data(self.pair, self.exchange_name)
        price = float(price_info.get("price", 0.0))
        open_positions = (
            len(self.position_manager.positions)
            if self.position_manager
            else 0
        )
        self.risk_manager.update_balance(balance.get("USDT", 0.0))
        self.risk_manager.monitor_volatility(price)
        self.risk_manager.check_risk_limits(0.0, open_positions=open_positions)

    async def run(self) -> None:
        """Запустить непрерывное наблюдение за рисками."""

        self.running = True
        while self.running:
            try:
                await self.check_once()
            except Exception as exc:  # pragma: no cover - непредвиденный сбой
                handle_error("Risk monitoring failed", exc)
            await asyncio.sleep(self.interval)

    def stop(self) -> None:
        """Остановить наблюдение."""

        self.running = False
