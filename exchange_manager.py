"""Высокоуровневые помощники для управления сделками на Bybit."""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, List
from typing import TYPE_CHECKING
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

ORDER_EXECUTION_LATENCY = Histogram(
    "order_execution_latency_seconds",
    "Время выполнения отправки ордеров",
)


class ExchangeManager:
    """Обёртка над BybitAPI с поддержкой мульти-аккаунтов и кэша рынка."""

    def __init__(
        self,
        cfg: Optional[Config] = None,
        api: Optional[BybitAPI] = None,
        apis: Optional[List[BybitAPI]] = None,
        clients: Optional[Dict[str, List[Any]]] = None,
        notifier: Optional["NotificationManager"] = None,
    ) -> None:
        self.notifier = notifier
        if clients is not None:
            self.clients = clients
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
        self.apis: List[BybitAPI] = self.clients.get("bybit", [])
        self.api: Optional[BybitAPI] = self.apis[0] if self.apis else None
        self._market_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._watcher_task: Optional[asyncio.Task] = None
        self.trading_params: Dict[str, Any] = {}

    def _ensure_api(self, exchange: str) -> BybitAPI:
        apis = self.clients.get(exchange)
        if not apis:
            raise KeyError(f"Unknown exchange {exchange}")
        api = apis[0]
        if api is None:
            raise RuntimeError(f"No API client for exchange {exchange}")
        return api

    def _extract_available_balance(self, data: Dict[str, Any]) -> float:
        try:
            return float(
                data.get("result", {})
                .get("list", [{}])[0]
                .get("availableBalance", data.get("USDT", 0))
            )
        except (KeyError, TypeError, ValueError):
            return 0.0

    def sync_trading_parameters(self, params: Dict[str, Any]) -> None:
        self.trading_params.update(params)
        logger.debug("Trading parameters synchronized: %s", params)

    async def get_spot_data(self, pair: str, exchange: str = "bybit") -> Dict[str, Any]:
        api = self._ensure_api(exchange)
        return await api.get_spot_data(pair)

    async def get_futures_data(self, pair: str, exchange: str = "bybit") -> Dict[str, Any]:
        api = self._ensure_api(exchange)
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
        start = time.perf_counter()
        api = self._ensure_api(exchange)
        if exchange != "bybit" or len(self.apis) <= 1:
            result = await api.place_order(pair, price, qty, side, order_type)
            ORDER_EXECUTION_LATENCY.observe(time.perf_counter() - start)
            return result

        balances = await asyncio.gather(*(a.check_balance() for a in self.apis))
        amounts = [self._extract_available_balance(b) for b in balances]
        total = sum(amounts) or 1.0
        tasks = []
        for a, bal in zip(self.apis, amounts):
            share = qty * bal / total
            if share > 0:
                tasks.append(a.place_order(pair, price, share, side, order_type))
        result = await asyncio.gather(*tasks)
        ORDER_EXECUTION_LATENCY.observe(time.perf_counter() - start)
        return result

    async def check_balance(self, exchange: str = "bybit") -> Dict[str, Any]:
        api = self._ensure_api(exchange)
        if exchange != "bybit" or len(self.apis) <= 1:
            data = await api.check_balance()
            return {"USDT": self._extract_available_balance(data)}
        balances = await asyncio.gather(*(a.check_balance() for a in self.apis))
        amounts = [self._extract_available_balance(b) for b in balances]
        return {"USDT": sum(amounts), "accounts": amounts}

    async def get_open_orders(self, pair: Optional[str] = None, exchange: str = "bybit") -> Dict[str, Any]:
        api = self._ensure_api(exchange)
        return await api.get_open_orders(pair)

    async def get_order_status(self, order_id: str, pair: Optional[str] = None, exchange: str = "bybit") -> Dict[str, Any]:
        api = self._ensure_api(exchange)
        return await api.get_order_status(order_id, pair)

    async def cancel_order(self, order_id: str, pair: Optional[str] = None, exchange: str = "bybit") -> Dict[str, Any]:
        api = self._ensure_api(exchange)
        return await api.cancel_order(order_id, pair)

    async def get_market_status(self, pair: str, category: str = "linear") -> Dict[str, Any]:
        """Запрос информации об инструменте через публичный REST v5."""
        if self.api is None:
            raise RuntimeError("Bybit API is not initialized")
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {"category": category, "symbol": pair}
                resp = await self.api._client.get("/v5/market/instruments-info", params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.api.handle_api_error(exc, attempt)
        raise RuntimeError("get_market_status: request failed after retries")

    async def update_market_data(self, spot_pair: str, futures_pair: str) -> Dict[str, Dict[str, Any]]:
        try:
            spot_task = self.get_spot_data(spot_pair)
            fut_task = self.get_futures_data(futures_pair)
            spot, futures = await asyncio.gather(spot_task, fut_task)
        except Exception as exc:  # pragma: no cover
            handle_error("Failed to update market data", exc, self.notifier)
            raise
        now = time.time()
        self._market_cache["spot"] = (now, spot)
        self._market_cache["futures"] = (now, futures)
        return {"spot": spot, "futures": futures}

    async def get_market_data(self, spot_pair: str, futures_pair: str, max_age: float = 2.0) -> Dict[str, Dict[str, Any]]:
        now = time.time()
        spot_ts, spot = self._market_cache.get("spot", (0.0, {}))
        fut_ts, futures = self._market_cache.get("futures", (0.0, {}))
        if (now - spot_ts > max_age) or (now - fut_ts > max_age) or not spot or not futures:
            return await self.update_market_data(spot_pair, futures_pair)
        return {"spot": spot, "futures": futures}

    async def start_market_watcher(self, spot_pair: str, futures_pair: str, interval: float = 1.0) -> None:
        if self._watcher_task:
            return

        async def _watch() -> None:
            while True:
                try:
                    await self.update_market_data(spot_pair, futures_pair)
                except Exception as exc:  # pragma: no cover
                    handle_error("Market watcher failed", exc)
                await asyncio.sleep(interval)

        self._watcher_task = asyncio.create_task(_watch())

    async def stop_market_watcher(self) -> None:
        if self._watcher_task:
            self._watcher_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._watcher_task
            self._watcher_task = None

    async def backup_open_orders(self, backup_path: str, pair: Optional[str] = None) -> str:
        orders = await self.get_open_orders(pair)
        await asyncio.to_thread(Path(backup_path).write_text, json.dumps(orders, ensure_ascii=False))
        return backup_path

    async def close(self) -> None:
        for api in self.apis:
            await api.close()
