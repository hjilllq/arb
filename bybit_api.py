"""Инструменты для взаимодействия с REST API биржи Bybit.

REST API — это веб-интерфейс, к которому обращаются через HTTP-запросы.
Модуль содержит небольшой асинхронный клиент, который получает рыночные
данные и управляет простыми ордерами на Bybit. Учётные данные передаются
при создании клиента, что позволяет полностью контролировать источник
ключей и секретов.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING
import asyncio
import hashlib
import hmac
import logging
import time

import httpx
from prometheus_client import Histogram

from error_handler import handle_error

if TYPE_CHECKING:  # pragma: no cover - только для подсказок типов
    from notification_manager import NotificationManager

logger = logging.getLogger(__name__)

# Гистограмма для времени отклика запросов к API Bybit
REQUEST_LATENCY = Histogram(
    "bybit_request_latency_seconds",
    "Время отклика запросов к Bybit",
    ["endpoint"],
)


class BybitAPI:
    """Минимальная асинхронная обёртка над HTTP-API Bybit."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: str = "https://api.bybit.com",
        cache_ttl: float = 1.0,
        notifier: Optional["NotificationManager"] = None,
    ) -> None:
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=10.0,
            http2=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=20),
        )
        # Простой кэш с ограниченным временем жизни
        self._cache: Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]] = {}
        self.cache_ttl = cache_ttl  # секунды
        self.notifier = notifier

    async def __aenter__(self) -> "BybitAPI":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    # ------------------------------ utils ----------------------------

    def cleanup_cache(self) -> None:
        """Удалить элементы кэша, старше ``cache_ttl`` секунд."""
        now = time.time()
        for key, (ts, _) in list(self._cache.items()):
            if now - ts > self.cache_ttl:
                del self._cache[key]

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Подписать приватные запросы к API."""
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API key and secret are required for this operation")
        params["api_key"] = self.api_key
        params["timestamp"] = int(time.time() * 1000)
        query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        signature = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        params["sign"] = signature
        return params

    # ----------------------------- public ----------------------------

    async def get_spot_data(self, pair: str) -> Dict[str, Any]:
        """Статистика 24h по спотовой паре (Bybit v5)."""
        self.cleanup_cache()
        cache_key = ("spot", pair)
        cached = self._cache.get(cache_key)
        now = time.time()
        if cached and now - cached[0] < self.cache_ttl:
            return cached[1]

        for attempt in range(3):
            try:
                with REQUEST_LATENCY.labels("get_spot_data").time():
                    # v5 tickers (category=spot)
                    resp = await self._client.get(
                        "/v5/market/tickers", params={"category": "spot", "symbol": pair}
                    )
                resp.raise_for_status()
                data = resp.json()
                self._cache[cache_key] = (time.time(), data)
                return data
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)
        raise RuntimeError("get_spot_data: exhausted retries")

    async def get_futures_data(self, pair: str) -> Dict[str, Any]:
        """Тикер фьючерсной пары (Bybit v5, USDT-перп = category=linear)."""
        self.cleanup_cache()
        cache_key = ("futures", pair)
        cached = self._cache.get(cache_key)
        now = time.time()
        if cached and now - cached[0] < self.cache_ttl:
            return cached[1]

        for attempt in range(3):
            try:
                with REQUEST_LATENCY.labels("get_futures_data").time():
                    resp = await self._client.get(
                        "/v5/market/tickers", params={"category": "linear", "symbol": pair}
                    )
                resp.raise_for_status()
                data = resp.json()
                self._cache[cache_key] = (time.time(), data)
                return data
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)
        raise RuntimeError("get_futures_data: exhausted retries")

    # ----------------------------- private ---------------------------

    async def place_order(
        self,
        pair: str,
        price: float,
        qty: float,
        side: str = "Buy",
        order_type: str = "Limit",
    ) -> Dict[str, Any]:
        """Создать ордер на покупку/продажу (Bybit v5)."""
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {
                    "symbol": pair,
                    "side": side,
                    "orderType": order_type,
                    "price": price,
                    "qty": qty,
                    # при необходимости: "category": "linear"/"spot"
                }
                params = self._sign(params)
                with REQUEST_LATENCY.labels("place_order").time():
                    resp = await self._client.post("/v5/order/create", json=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)
        raise RuntimeError("place_order: exhausted retries")

    async def cancel_order(self, order_id: str, pair: Optional[str] = None) -> Dict[str, Any]:
        """Отменить ранее созданный ордер."""
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {"orderId": order_id}
                if pair is not None:
                    params["symbol"] = pair
                params = self._sign(params)
                with REQUEST_LATENCY.labels("cancel_order").time():
                    resp = await self._client.post("/v5/order/cancel", json=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)
        raise RuntimeError("cancel_order: exhausted retries")

    async def check_balance(self) -> Dict[str, Any]:
        """Получить сведения о балансе аккаунта."""
        for attempt in range(3):
            try:
                params = self._sign({})
                with REQUEST_LATENCY.labels("check_balance").time():
                    resp = await self._client.get("/v5/account/wallet-balance", params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)
        raise RuntimeError("check_balance: exhausted retries")

    async def get_order_status(self, order_id: str, pair: Optional[str] = None) -> Dict[str, Any]:
        """Текущее состояние ордера."""
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {"orderId": order_id}
                if pair is not None:
                    params["symbol"] = pair
                params = self._sign(params)
                with REQUEST_LATENCY.labels("get_order_status").time():
                    resp = await self._client.get("/v5/order/realtime", params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)
        raise RuntimeError("get_order_status: exhausted retries")

    async def get_open_orders(self, pair: Optional[str] = None) -> Dict[str, Any]:
        """Список текущих открытых ордеров."""
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {}
                if pair is not None:
                    params["symbol"] = pair
                params = self._sign(params)
                with REQUEST_LATENCY.labels("get_open_orders").time():
                    resp = await self._client.get("/v5/order/realtime", params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)
        raise RuntimeError("get_open_orders: exhausted retries")

    # --------------------------- recovery ----------------------------

    async def _reset_client(self) -> None:
        """Пересоздать HTTP-клиент после критической ошибки."""
        try:
            await self._client.aclose()
        except Exception:  # pragma: no cover
            pass
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=10.0,
            http2=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=20),
        )

    async def handle_api_error(self, exc: httpx.HTTPError, attempt: int, retries: int = 3) -> None:
        """Обработать HTTP-ошибку с экспоненциальным бэкоффом и, при необходимости, ротацией клиента."""
        wait_time = 2 ** attempt
        if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
            if exc.response.status_code == 429:  # превышен лимит запросов
                retry_after = exc.response.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        wait_time = float(retry_after)
                    except ValueError:  # pragma: no cover
                        pass

        logger.warning("Bybit API error on attempt %d/%d: %s", attempt + 1, retries, exc)

        if attempt + 1 >= retries:
            await self._reset_client()
            handle_error("Bybit API request failed", exc, self.notifier)
            raise exc

        await asyncio.sleep(wait_time)

    async def close(self) -> None:
        """Корректно закрыть HTTP-клиент."""
        await self._client.aclose()
