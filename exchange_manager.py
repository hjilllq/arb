"""Высокоуровневые помощники для управления сделками на Bybit.

Модуль оборачивает :class:`~bybit_api.BybitAPI`, предоставляя удобные
методы для получения рыночных данных, работы с ордерами и запросов
информации об аккаунте. Интерфейс намеренно упрощён и служит основой
для более сложной логики управления торговлей.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, List, TYPE_CHECKING
import asyncio
import json
import logging
import time
from contextlib import suppress
from pathlib import Path

import httpx
from prometheus_client import Histogram

from bybit_api import BybitAPI
from config import Config
from error_handler import handle_error

if TYPE_CHECKING:  # pragma: no cover
    from notification_manager import NotificationManager

logger = logging.getLogger(__name__)

# Метрика времени исполнения ордеров
ORDER_EXECUTION_LATENCY = Histogram(
    "order_execution_latency_seconds",
    "Время выполнения отправки ордеров",
)


class ExchangeManager:
    """Обёртка над :class:`BybitAPI` с дополнительными удобными методами."""

    def __init__(
        self,
        cfg: Optional[Config] = None,
        api: Optional[BybitAPI] = None,
        apis: Optional[List[BybitAPI]] = None,
        clients: Optional[Dict[str, List[BybitAPI]]] = None,
        notifier: Optional["NotificationManager"] = None,
    ) -> None:
        self.notifier = notifier

        if clients is not None:
            self.clients: Dict[str, List[BybitAPI]] = clients
        else:
            if apis is not None:
                bybit_list = apis
            elif cfg is not None:
                bybit_list = [
                    BybitAPI(api_key=a.api_key, api_secret=a.api_secret, notifier=notifier)
                    for a in cfg.accounts
                ]
            elif api is not None:
                bybit_list = [api]
            else:
                bybit_list = [BybitAPI(notifier=notifier)]
            self.clients = {"bybit": bybit_list}

        # по умолчанию используем первую Bybit API
        self.apis: List[BybitAPI] = self.clients.get("bybit", [])
        self.api: Optional[BybitAPI] = self.apis[0] if self.apis else None

        self._market_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._watcher_task: Optional[asyncio.Task[None]] = None
        self.trading_params: Dict[str, Any] = {}

    # ---------------------------- helpers ----------------------------

    def _get_apis(self, exchange: str) -> List[BybitAPI]:
        apis = self.clients.get(exchange)
        if not apis:
            raise KeyError(f"Unknown exchange {exchange}")
        return apis

    def _get_api(self, exchange: str) -> BybitAPI:
        apis = self._get_apis(exchange)
        return apis[0]

    @staticmethod
    def _extract_available_balance(data: Dict[str, Any]) -> float:
        """Извлечь доступный баланс USDT из ответа API."""
        try:
            return float(
                data.get("result", {})
                .get("list", [{}])[0]
                .get("availableBalance", data.get("USDT", 0))
            )
        except (KeyError, TypeError, ValueError):
            return 0.0

    def sync_trading_parameters(self, params: Dict[str, Any]) -> None:
        """Сохранить параметры торговли и распространить их между аккаунтами."""
        self.trading_params.update(params)
        logger.debug("Trading parameters synchronized: %s", params)

    # ----------------------- market data / orders --------------------

    async def get_spot_data(self, pair: str, exchange: str = "bybit") -> Dict[str, Any]:
        """Вернуть информацию о тике для спотовой торговой пары."""
        api = self._get_api(exchange)
        return await api.get_spot_data(pair)

    async def get_futures_data(self, pair: str, exchange: str = "bybit") -> Dict[str, Any]:
        """Вернуть информацию о тике для фьючерсной торговой пары."""
        api = self._get_api(exchange)
        return await api.get_futures_data(pair)

    async def place_order(
        self,
        pair: str,
        price: float,
        qty: float,
        side: str = "Buy",
        order_type: str = "Limit",
        exchange: str = "bybit",
    ) -> Any:
        """Отправить ордер, распределяя объём между аккаунтами."""
        start = time.perf_counter()

        if exchange != "bybit":
            api = self._get_api(exchange)
            result = await api.place_order(pair, price, qty, side, order_type)
            ORDER_EXECUTION_LATENCY.observe(time.perf_counter() - start)
            return result

        # Bybit: если единственный аккаунт — отправляем одним запросом
        if len(self.apis) == 1:
            assert self.api is not None, "Exchange client is not initialized"
            result = await self.api.place_order(pair, price, qty, side, order_type)
            ORDER_EXECUTION_LATENCY.observe(time.perf_counter() - start)
            return result

        # Несколько аккаунтов: делим объём пропорционально доступному балансу
        balances = await asyncio.gather(*(api.check_balance() for api in self.apis))
        amounts = [self._extract_available_balance(b) for b in balances]
        total = sum(amounts) or 1.0

        tasks: List[asyncio.Future[Any]] = []
        for api, bal in zip(self.apis, amounts):
            share = qty * bal / total
            if share <= 0:
                continue
            tasks.append(asyncio.create_task(api.place_order(pair, price, share, side, order_type)))

        result = await asyncio.gather(*tasks) if tasks else []
        ORDER_EXECUTION_LATENCY.observe(time.perf_counter() - start)
        return result

    async def check_balance(self, exchange: str = "bybit") -> Dict[str, Any]:
        """Суммарный баланс всех аккаунтов."""
        if exchange != "bybit":
            api = self._get_api(exchange)
            data = await api.check_balance()
            return {"USDT": self._extract_available_balance(data)}

        if len(self.apis) == 1:
            assert self.api is not None, "Exchange client is not initialized"
            data = await self.api.check_balance()
            return {"USDT": self._extract_available_balance(data)}

        balances = await asyncio.gather(*(api.check_balance() for api in self.apis))
        amounts = [self._extract_available_balance(b) for b in balances]
        return {"USDT": float(sum(amounts)), "accounts": amounts}

    async def get_open_orders(
        self, pair: Optional[str] = None, exchange: str = "bybit"
    ) -> Dict[str, Any]:
        """Получить текущие открытые ордера."""
        api = self._get_api(exchange)
        return await api.get_open_orders(pair)

    async def get_order_status(
        self, order_id: str, pair: Optional[str] = None, exchange: str = "bybit"
    ) -> Dict[str, Any]:
        """Вернуть текущее состояние ордера."""
        api = self._get_api(exchange)
        return await api.get_order_status(order_id, pair)

    async def cancel_order(
        self, order_id: str, pair: Optional[str] = None, exchange: str = "bybit"
    ) -> Dict[str, Any]:
        """Отменить существующий ордер."""
        api = self._get_api(exchange)
        return await api.cancel_order(order_id, pair)

    async def get_market_status(
        self, pair: str, category: str = "linear"
    ) -> Dict[str, Any]:
        """Вернуть информацию о статусе торговли для пары *pair*.

        Args:
            pair: Торговая пара для запроса.
            category: Категория рынка, например ``"spot"`` или ``"linear"``.
        """
        assert self.api is not None, "Exchange client is not initialized"

        for attempt in range(3):
            try:
                params: Dict[str, Any] = {"category": category, "symbol": pair}
                resp = await self.api._client.get("/v5/market/instruments-info", params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.api.handle_api_error(exc, attempt)
        # если все попытки исчерпаны
        raise RuntimeError("get_market_status: exhausted retries")

    # --------------------------- caching -----------------------------

    async def update_market_data(
        self, spot_pair: str, futures_pair: str
    ) -> Dict[str, Dict[str, Any]]:
        """Асинхронно получить и сохранить котировки спота и фьючерса."""
        try:
            spot_task = self.get_spot_data(spot_pair)
            fut_task = self.get_futures_data(futures_pair)
            spot, futures = await asyncio.gather(spot_task, fut_task)
        except Exception as exc:  # pragma: no cover - непредвиденная ошибка
            handle_error("Failed to update market data", exc, self.notifier)
            raise

        now = time.time()
        self._market_cache["spot"] = (now, spot)
        self._market_cache["futures"] = (now, futures)
        return {"spot": spot, "futures": futures}

    async def get_market_data(
        self, spot_pair: str, futures_pair: str, max_age: float = 2.0
    ) -> Dict[str, Dict[str, Any]]:
        """Вернуть свежие данные рынка, обновляя кэш при необходимости."""
        now = time.time()
        spot_ts, spot = self._market_cache.get("spot", (0.0, {}))
        fut_ts, futures = self._market_cache.get("futures", (0.0, {}))

        if (now - spot_ts > max_age) or (now - fut_ts > max_age) or not spot or not futures:
            return await self.update_market_data(spot_pair, futures_pair)
        return {"spot": spot, "futures": futures}

    # ------------------------- background tasks ----------------------

    async def start_market_watcher(
        self, spot_pair: str, futures_pair: str, interval: float = 1.0
    ) -> None:
        """Запустить фоновое обновление данных о рынке."""
        if self._watcher_task:
            return

        async def _watch() -> None:
            while True:
                try:
                    await self.update_market_data(spot_pair, futures_pair)
                except Exception as exc:  # pragma: no cover - логирование ошибок
                    handle_error("Market watcher failed", exc)
                await asyncio.sleep(interval)

        self._watcher_task = asyncio.create_task(_watch())

    async def stop_market_watcher(self) -> None:
        """Остановить фоновое обновление данных о рынке."""
        if self._watcher_task:
            self._watcher_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._watcher_task
            self._watcher_task = None

    # ----------------------------- misc ------------------------------

    async def backup_open_orders(self, backup_path: str, pair: Optional[str] = None) -> str:
        """Сохранить текущие открытые ордера в файл ``backup_path``."""
        orders = await self.get_open_orders(pair)
        await asyncio.to_thread(
            Path(backup_path).write_text, json.dumps(orders, ensure_ascii=False)
        )
        return backup_path

    async def close(self) -> None:
        """Закрыть внутренние HTTP-соединения."""
        for api in self.apis:
            await api.close()
