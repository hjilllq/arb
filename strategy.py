"""Основные инструменты арбитражной стратегии."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Dict, Any
import asyncio

from logger import log_event


@dataclass
class ArbitrageStrategy:
    """Простая стратегия арбитража между спотом и фьючерсом.

    Parameters
    ----------
    basis_threshold:
        Минимальная разница в цене между фьючерсом и спотом для запуска сделки.
    """

    basis_threshold: float = 0.0

    def calculate_basis(self, spot_price: float, futures_price: float) -> float:
        """Вернуть базис — разницу между ценами фьючерса и спота."""
        return futures_price - spot_price

    def generate_trade_signal(self, basis: float) -> int:
        """Вернуть ``1`` для покупки спота/продажи фьючерса, ``-1`` наоборот."""
        if basis > self.basis_threshold:
            return -1
        if basis < -self.basis_threshold:
            return 1
        return 0

    async def apply_strategy(
        self,
        exchange: Any,
        spot_pair: str,
        futures_pair: str,
    ) -> Dict[str, Any]:
        """Получить рыночные данные параллельно и выдать торговое решение."""

        spot_data, futures_data = await asyncio.gather(
            exchange.get_spot_data(spot_pair),
            exchange.get_futures_data(futures_pair),
        )
        spot_price = spot_data.get("price", 0.0)
        futures_price = futures_data.get("price", 0.0)
        basis = self.calculate_basis(spot_price, futures_price)
        signal = self.generate_trade_signal(basis)
        trade = {
            "spot_pair": spot_pair,
            "futures_pair": futures_pair,
            "spot_price": spot_price,
            "futures_price": futures_price,
            "basis": basis,
            "signal": signal,
        }
        if signal:
            self.log_strategy_performance({"action": "trade_signal", **trade})
        return trade

    def evaluate_strategy(self, pnls: Sequence[float]) -> Dict[str, float]:
        """Вернуть базовые показатели эффективности по последовательности PnL (прибыль/убыток)."""
        total = sum(pnls)
        trades = len(pnls)
        wins = sum(1 for p in pnls if p > 0)
        win_rate = wins / trades if trades else 0.0
        summary = {"trades": trades, "total_pnl": total, "win_rate": win_rate}
        self.log_strategy_performance(summary)
        return summary

    def log_strategy_performance(self, info: Dict[str, Any]) -> None:
        """Логировать информацию о стратегии для наблюдаемости."""
        log_event(f"STRATEGY: {info}")
