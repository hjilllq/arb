"""Модуль бэктестинга для арбитражных стратегий между спотом и фьючерсом на Bybit.

Модуль содержит инструменты для моделирования сделок на исторических данных и
формирования простого отчёта об эффективности. Реализация минимальна и служит
отправной точкой для более сложного функционала бэктестинга."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Any, Sequence
from concurrent.futures import ThreadPoolExecutor
import logging


logger = logging.getLogger(__name__)


@dataclass
class Backtester:
    """Запускать базовые бэктесты для стратегий спот-фьючерсного арбитража.

    Attributes:
        initial_capital: Стартовый капитал, используемый в тесте.
    """

    initial_capital: float = 0.0

    def backtest(
        self,
        historical_data: Iterable[Dict[str, float]],
        strategy: Callable[[Dict[str, float]], str],
    ) -> Dict[str, Any]:
        """Запустить стратегию на переданных исторических данных.

        Args:
            historical_data: Последовательность снимков цен. Каждый снимок —
                словарь, который как минимум содержит цены ``"spot"`` и ``"future"``.
            strategy: Функция, решающая, открывать ли позицию по снимку цен.
                Должна возвращать ``"buy"`` (покупка), ``"sell"`` (продажа) или
                ``"hold"`` (ничего не делать).

        Returns:
            Словарь с агрегированными метриками, полученными
            из :meth:`generate_report`.
        """
        trades: List[Dict[str, float]] = []
        for snapshot in historical_data:
            trade = self.simulate_trade(
                snapshot,
                strategy,
                snapshot.get("slippage", 0.0),
            )
            if trade:
                trades.append(trade)
        return self.generate_report(trades)

    def simulate_trade(
        self,
        snapshot: Dict[str, float],
        strategy: Callable[[Dict[str, float]], str],
        slippage: float = 0.0,
    ) -> Dict[str, float] | None:
        """Смоделировать одну сделку на основе решения стратегии.

        Parameters
        ----------
        snapshot: dict[str, float]
            Информация о ценах за один период.
        strategy: Callable
            Функция, возвращающая решение по сделке.
        slippage: float, optional
            Разница между ожидаемой и исполненной ценой.

        Returns
        -------
        dict[str, float] | None
            Краткое описание сделки (решение и PnL) либо ``None`` при отсутствии
            действия.
        """
        decision = strategy(snapshot)
        pnl = 0.0
        if decision == "buy":
            pnl = snapshot["future"] - snapshot["spot"]
            expected_price = snapshot["spot"]
        elif decision == "sell":
            pnl = snapshot["spot"] - snapshot["future"]
            expected_price = snapshot["future"]
        else:
            return None
        executed_price = expected_price + slippage
        pnl -= self.calculate_slippage(expected_price, executed_price)
        return {"decision": decision, "pnl": pnl}

    def generate_report(self, trades: List[Dict[str, float]]) -> Dict[str, float]:
        """Сформировать простой отчёт с агрегированными результатами сделок.

        Args:
            trades: Список словарей сделок, полученных из
                :meth:`simulate_trade`.

        Returns:
            Словарь с количеством совершённых сделок и общим результатом
            (прибыль или убыток).
        """
        total_pnl = sum(t["pnl"] for t in trades)
        result = {"trades": len(trades), "total_pnl": total_pnl}
        self.log_results(result)
        return result

    def calculate_slippage(self, expected_price: float, executed_price: float) -> float:
        """Вернуть модуль разницы между ожидаемой и фактической ценой.

        Parameters
        ----------
        expected_price:
            Цена, на которую рассчитывала стратегия.
        executed_price:
            Цена, по которой сделка действительно исполнилась.

        Returns
        -------
        float
            Значение проскальзывания. Всегда неотрицательное.
        """

        return abs(executed_price - expected_price)

    def run_in_parallel(
        self,
        datasets: Sequence[Iterable[Dict[str, float]]],
        strategy: Callable[[Dict[str, float]], str],
        max_workers: int = 4,
    ) -> List[Dict[str, Any]]:
        """Запустить набор бэктестов параллельно в потоках.

        Parameters
        ----------
        datasets:
            Последовательность исторических наборов данных.
        strategy:
            Функция, определяющая поведение стратегии для каждого снимка.
        max_workers:
            Максимальное количество потоков в пуле. По умолчанию ``4``.

        Returns
        -------
        list[dict]
            Результаты, возвращённые :meth:`backtest` для каждого набора данных.
        """

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.backtest, data, strategy) for data in datasets]
            return [f.result() for f in futures]

    def log_results(self, result: Dict[str, Any]) -> None:
        """Записать сводную информацию о результатах в журнал.

        Parameters
        ----------
        result:
            Структура, содержащая количество сделок и суммарный PnL.
        """

        logger.info(
            "Executed %s trades with total PnL %.4f",
            result.get("trades", 0),
            result.get("total_pnl", 0.0),
        )
