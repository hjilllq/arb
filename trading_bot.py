"""Основные инструменты оркестрации торгового бота.

Модуль определяет минимальный торговый бот, который связывает арбитражную
стратегию с реализацией биржи. Он предоставляет функции для запуска и
остановки торговли, исполнения сделок на основе сигналов и записи
операционных метрик."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple, Awaitable, Optional, cast
import asyncio
from concurrent.futures import ThreadPoolExecutor

from logger import log_event, log_trade_data, log_system_health
from config import load_config
from error_handler import handle_error
from strategy import ArbitrageStrategy
from strategy_manager import StrategyManager
from risk_manager import RiskManager
from position_manager import PositionManager
from data_analyzer import detect_anomalies


@dataclass
class TradingBot:
    """Простой высокоуровневый контроллер торгового бота."""

    exchange: Any
    strategy: ArbitrageStrategy | None = None
    strategy_manager: StrategyManager | None = None
    risk_manager: RiskManager | None = None
    position_manager: PositionManager | None = None
    notifier: Any | None = None
    active: bool = field(default=False)
    slippage: float = 0.001
    fee_rate: float = 0.001
    max_retries: int = 3
    price_history: List[float] = field(default_factory=list)
    trading_pairs: List[Tuple[str, str, str]] | None = None
    executor: ThreadPoolExecutor = field(
        default_factory=lambda: ThreadPoolExecutor(max_workers=3)
    )

    def start_trading(self) -> None:
        """Отметить бота как активного и записать событие."""

        self.active = True
        log_event("TRADING BOT STARTED")

    def stop_trading(self) -> None:
        """Отметить бота как неактивного и записать событие."""

        self.active = False
        log_event("TRADING BOT STOPPED")
        self.executor.shutdown(wait=False)

    async def _detect_anomalies_async(self) -> List[int]:
        """Запустить поиск аномалий в отдельном потоке.

        Возвращает список индексов, где были обнаружены выбросы. Использование
        ``ThreadPoolExecutor`` позволяет не блокировать основной цикл ``asyncio``
        во время вычислений индикаторов.
        """

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, detect_anomalies, self.price_history)

    async def _cancel_open_orders(self, pair: str, exchange: str = "bybit") -> None:
        """Отменить уже существующие ордера по указанной паре."""

        try:
            open_orders = await self.exchange.get_open_orders(pair, exchange)
        except Exception as exc:  # pragma: no cover - сетевые сбои
            handle_error("Fetch open orders failed", exc)
            return
        for order in open_orders or []:
            order_id = (
                order.get("id")
                or order.get("orderId")
                or order.get("order_id")
            )
            if order_id is None:
                continue
            try:
                await self.exchange.cancel_order(order_id, pair, exchange)
            except Exception as exc:  # pragma: no cover - сбой отмены
                handle_error(f"Cancel order {order_id} failed", exc)

    async def _place_with_retry(
        self, pair: str, price: float, qty: float, side: str, exchange: str
    ) -> Dict[str, Any]:
        """Разместить ордер, учитывая проскальзывание, комиссию и повторы."""
        adj_price = price * (1 + self.slippage if side == "Buy" else 1 - self.slippage)
        fee = abs(adj_price * qty) * self.fee_rate
        for attempt in range(self.max_retries):
            try:
                order = await self.exchange.place_order(
                    pair, adj_price, qty, side, exchange=exchange
                )
                order_id = (
                    order.get("id")
                    or order.get("order_id")
                    or order.get("result", {}).get("orderId")
                )
                if order_id is None:
                    raise RuntimeError("No order id returned")
                status = await self.exchange.get_order_status(
                    order_id, pair, exchange
                )
                state = status.get("status") or status.get("result", {}).get("orderStatus")
                if state and state.lower() in {"filled", "closed"}:
                    order.update(
                        {
                            "pair": pair,
                            "price": adj_price,
                            "qty": qty,
                            "side": side,
                            "fee": fee,
                        }
                    )
                    return order
            except Exception as exc:
                handle_error(f"Order {pair} failed", exc)
            await asyncio.sleep(0.5)
        raise RuntimeError(f"Order {pair} failed after retries")

    async def execute_trade(
    self, spot_pair: str, futures_pair: str, qty: float, exchange: str = "bybit"
) -> Dict[str, Any]:
    """Исполнить сделку на основе текущего сигнала стратегии."""
    orders: Dict[str, Any] = {}
    try:
        # 1) Получаем стратегию
        active_strategy: Optional[ArbitrageStrategy]
        if self.strategy_manager:
            await self.strategy_manager.evaluate_market(
                self.exchange, spot_pair, exchange
            )
            active_strategy = self.strategy_manager.get_active_strategy()
        else:
            active_strategy = self.strategy

        if active_strategy is None:
            raise RuntimeError("Strategy is not configured")

        # 2) Параллельно: решение стратегии и баланс
        decision, balance = await asyncio.gather(
            active_strategy.apply_strategy(self.exchange, spot_pair, futures_pair),
            self.exchange.check_balance(exchange),
        )

        price = decision["spot_price"]
        self.price_history.append(price)
        if len(self.price_history) > 100:
            self.price_history.pop(0)

        # 3) Риск-менеджмент
        if self.risk_manager:
            vol = self.risk_manager.monitor_volatility(price)
            if self.risk_manager.trading_paused:
                return {}
            strategy_name = getattr(active_strategy, "name", None)
            qty = self.risk_manager.adjust_position_size(qty, strategy_name, vol)

            open_pos = (
                len(self.position_manager.positions)
                if self.position_manager
                else 0
            )
            if not self.risk_manager.check_risk_limits(qty, open_positions=open_pos):
                return {}

            self.risk_manager.update_balance(balance.get("USDT", 0.0))
            if self.risk_manager.trading_paused:
                return {}

        # 4) Проверка средств
        cost = decision["spot_price"] * qty
        if balance.get("USDT", 0.0) < cost:
            handle_error("Insufficient balance", ValueError("low balance"))
            return {}

        # 5) Асинхронные задачи: корректные типы
        awaitables: List[Awaitable[Any]] = []
        anomalies_task: Optional[Awaitable[List[int]]] = None

        if len(self.price_history) >= 5:
            anomalies_task = self._detect_anomalies_async()
            awaitables.append(anomalies_task)

        awaitables.append(self._cancel_open_orders(spot_pair, exchange))
        awaitables.append(self._cancel_open_orders(futures_pair, exchange))

        results = await asyncio.gather(*awaitables)
        anomalies: List[int] = []
        if anomalies_task is not None:
            anomalies = cast(List[int], results[0])

        if anomalies and anomalies[-1] == len(self.price_history) - 1:
            log_event("PRICE ANOMALY DETECTED, SKIP TRADE")
            if self.risk_manager:
                self.risk_manager.pause_trading_if_risk("price anomaly")
            return {}

        # 6) Ордеры
        signal = decision["signal"]
        if signal == 1:
            spot_coro = self._place_with_retry(
                spot_pair, decision["spot_price"], qty, "Buy", exchange
            )
            fut_coro = self._place_with_retry(
                futures_pair, decision["futures_price"], qty, "Sell", exchange
            )
            orders["spot"], orders["futures"] = await asyncio.gather(spot_coro, fut_coro)

        elif signal == -1:
            spot_coro = self._place_with_retry(
                spot_pair, decision["spot_price"], qty, "Sell", exchange
            )
            fut_coro = self._place_with_retry(
                futures_pair, decision["futures_price"], qty, "Buy", exchange
            )
            orders["spot"], orders["futures"] = await asyncio.gather(spot_coro, fut_coro)

        # 7) Учёт позиций + лог
        if orders and self.position_manager:
            for order in orders.values():
                side = "long" if order["side"].lower() == "buy" else "short"
                pair = order["pair"]
                existing = self.position_manager.positions.get(pair)
                if existing and existing.side != side:
                    self.position_manager.close_position(pair, order["price"])
                self.position_manager.open_position(
                    pair, order["qty"], order["price"], side
                )
        if orders:
            self.log_trading_activity({"decision": decision, "orders": orders})
    except Exception as exc:
        handle_error("Trade execution failed", exc)
    return orders
    
    async def monitor_bot_health(self, exchange: str = "bybit") -> Dict[str, Any]:
        """Получить и залогировать информацию о состоянии, например баланс."""
        try:
            balance = await self.exchange.check_balance(exchange)
        except Exception as exc:
            handle_error("Balance check failed", exc)
            return {}
        health = {"balance": balance}
        log_system_health(health)
        return health

    def log_trading_activity(self, info: Dict[str, Any]) -> None:
        """Записать торговую активность с помощью централизованного логгера."""

        log_trade_data(info)

    async def trade_multiple_pairs(self, qty: float) -> List[Dict[str, Any]]:
        """Параллельно исполнить сделки для всех пар в ``trading_pairs``.

        Parameters
        ----------
        qty:
            Объём для каждой сделки.

        Returns
        -------
        list[dict]
            Результаты ``execute_trade`` для каждой пары.
        """

        if not self.trading_pairs:
            return []
        tasks = []
        for item in self.trading_pairs:
            if len(item) == 3:
                exch, spot, fut = item
            else:  # поддержка старого формата
                spot, fut = item
                exch = "bybit"
            tasks.append(self.execute_trade(spot, fut, qty, exch))
        return await asyncio.gather(*tasks)

    # ------------------------------------------------------------------
    def update_trading_pairs(self, pairs: List[Tuple[str, str, str]]) -> None:
        """Обновить список торговых пар и уведомить об изменениях."""

        old = set(self.trading_pairs or [])
        new = set(pairs)
        added = new - old
        removed = old - new
        self.trading_pairs = list(pairs)
        if not self.notifier:
            return
        for exch, spot, fut in added:
            self.notifier.log_notification(
                "pairs", f"added {exch}:{spot}:{fut}"
            )
        for exch, spot, fut in removed:
            self.notifier.log_notification(
                "pairs", f"removed {exch}:{spot}:{fut}"
            )

    def reload_trading_pairs(self, path: str = ".env") -> None:
        """Перечитать конфигурацию и обновить торговые пары."""

        cfg = load_config(path)
        self.update_trading_pairs(cfg.trading_pairs)
