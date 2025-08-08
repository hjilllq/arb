"""Менеджер стратегий для динамического переключения и тестирования.

Позволяет регистрировать несколько торговых стратегий, автоматически
переключаться между ними в зависимости от волатильности рынка и проверять
новые подходы на исторических данных перед использованием в реальной торговле.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Sequence
import statistics

from logger import log_event
from backtester import Backtester
from strategy import ArbitrageStrategy
from notification_manager import NotificationManager


@dataclass
class StrategyManager:
    """Управляет набором стратегий и выбирает активную.

    Attributes
    ----------
    strategies:
        Зарегистрированные стратегии по их названию.
    active_name:
        Имя текущей активной стратегии.
    vol_threshold:
        Порог стандартного отклонения цен, при превышении которого
        выбирается более консервативная стратегия.
    backtester:
        Экземпляр :class:`Backtester` для проверки новых стратегий на
        исторических данных.
    """

    strategies: Dict[str, ArbitrageStrategy] = field(default_factory=dict)
    active_name: str | None = None
    vol_threshold: float = 1.0
    backtester: Backtester | None = None
    _prices: list[float] = field(default_factory=list)

    def add_strategy(self, name: str, strategy: ArbitrageStrategy) -> None:
        """Добавить стратегию в менеджер и выбрать её, если активная отсутствует.

        Parameters
        ----------
        name:
            Уникальное имя стратегии.
        strategy:
            Экземпляр стратегии, реализующей метод ``apply_strategy``.
        """

        self.strategies[name] = strategy
        if self.active_name is None:
            self.active_name = name
        log_event(f"STRATEGY REGISTERED: {name}")

    def get_active_strategy(self) -> ArbitrageStrategy:
        """Вернуть текущую активную стратегию."""

        if self.active_name is None:
            raise RuntimeError("Не выбрана активная стратегия")
        return self.strategies[self.active_name]

    def switch_strategy(self, name: str) -> None:
        """Переключить активную стратегию и записать событие."""

        if name not in self.strategies:
            raise KeyError(f"Стратегия {name} не зарегистрирована")
        self.active_name = name
        log_event(f"STRATEGY SWITCHED TO: {name}")

    async def evaluate_market(self, exchange: Any, spot_pair: str) -> None:
        """Оценить рынок и при необходимости поменять стратегию.

        Метод запрашивает цену у биржи, добавляет её в историю и при
        накоплении достаточного количества точек вычисляет стандартное
        отклонение. Если волатильность превышает ``vol_threshold`` и в
        менеджере зарегистрировано несколько стратегий, активной становится
        последняя добавленная стратегия. Иначе — первая.
        """

        data = await exchange.get_spot_data(spot_pair)
        price = float(data.get("price") or data.get("result", {}).get("lastPrice", 0.0))
        self._prices.append(price)
        if len(self._prices) > 20:
            self._prices.pop(0)
        if len(self._prices) < 2 or len(self.strategies) < 2:
            return
        vol = statistics.pstdev(self._prices)
        names = list(self.strategies.keys())
        new_name = names[-1] if vol > self.vol_threshold else names[0]
        if new_name != self.active_name:
            self.switch_strategy(new_name)

    def test_strategy(self, name: str, historical_data: Iterable[Dict[str, float]]) -> Dict[str, Any]:
        """Протестировать стратегию на исторических данных.

        Parameters
        ----------
        name:
            Имя стратегии для тестирования.
        historical_data:
            Итерация снимков цен с ключами ``"spot"`` и ``"future"``.

        Returns
        -------
        dict[str, Any]
            Результаты, возвращённые :class:`Backtester`.
        """

        if name not in self.strategies:
            raise KeyError(f"Стратегия {name} не зарегистрирована")
        strategy = self.strategies[name]
        backtester = self.backtester or Backtester()

        def _fn(snapshot: Dict[str, float]) -> str:
            basis = strategy.calculate_basis(snapshot["spot"], snapshot["future"])
            signal = strategy.generate_trade_signal(basis)
            return "buy" if signal == 1 else "sell" if signal == -1 else "hold"

        return backtester.backtest(historical_data, _fn)

    def update_active_parameters(
        self,
        pnls: Sequence[float],
        notifier: NotificationManager | None = None,
    ) -> Dict[str, float]:
        """Проанализировать результаты и скорректировать параметры стратегии.

        Параметры
        ---------
        pnls:
            Последовательность результатов сделок (прибыль/убыток).
        notifier:
            Опциональный менеджер уведомлений. Если передан, отправляет
            сообщение о внесённых изменениях.

        Возвращает
        ---------
        Dict[str, float]
            Сводка эффективности и старое/новое значение порога ``basis_threshold``.

        Метод оценивает активную стратегию по предоставленным данным и
        автоматически регулирует её чувствительность: при низком проценте
        успешных сделок порог увеличивается (торговля становится более
        консервативной), при высокой доходности — снижается. После изменения
        параметров вызывается уведомление и запись в лог.
        """

        strategy = self.get_active_strategy()
        stats = strategy.evaluate_strategy(pnls)
        old = strategy.basis_threshold

        if stats["win_rate"] < 0.5:
            strategy.basis_threshold = round(old + 0.1, 4)
        elif stats["win_rate"] > 0.7 and old > 0.1:
            strategy.basis_threshold = round(max(0.0, old - 0.1), 4)

        message = (
            f"STRATEGY UPDATED: {self.active_name} basis {old} -> {strategy.basis_threshold}"
        )
        log_event(message)
        if notifier:
            notifier.send_telegram_notification(message)

        return {"old_basis": old, "new_basis": strategy.basis_threshold, **stats}
