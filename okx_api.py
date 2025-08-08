"""Минимальный асинхронный клиент для работы с HTTP‑API OKX."""
from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from error_handler import handle_error


class OkxAPI:
    """Простейшая обёртка над публичными и приватными методами OKX.

    Реализация ограничена лишь функциями, необходимыми для тестов. При
    необходимости класс можно расширять полноценной поддержкой всех
    эндпоинтов биржи.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: str = "https://www.okx.com",
        notifier: Optional[Any] = None,
    ) -> None:
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self._client = httpx.AsyncClient(base_url=base_url, timeout=10.0)
        self.notifier = notifier

    async def close(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------
    async def get_spot_data(self, pair: str) -> Dict[str, Any]:
        """Получить тикер для спотовой пары ``pair``."""
        try:
            resp = await self._client.get(
                "/api/v5/market/ticker", params={"instId": pair}
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as exc:  # pragma: no cover - сеть нестабильна
            await self.handle_api_error(exc)
            raise

    async def get_futures_data(self, pair: str) -> Dict[str, Any]:
        """Получить тикер для фьючерсной или своп-пары ``pair``."""
        try:
            resp = await self._client.get(
                "/api/v5/market/ticker", params={"instId": pair}
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as exc:  # pragma: no cover
            await self.handle_api_error(exc)
            raise

    async def place_order(
        self, pair: str, price: float, qty: float, side: str = "buy"
    ) -> Dict[str, Any]:
        """Разместить ордер. Реализация заглушена и возвращает словарь."""
        return {
            "pair": pair,
            "price": price,
            "qty": qty,
            "side": side,
            "id": "okx-order-1",
        }

    async def cancel_order(self, order_id: str, pair: Optional[str] = None) -> Dict[str, Any]:
        """Отменить ордер. Возвращает упрощённый ответ."""
        return {"id": order_id, "status": "cancelled", "pair": pair}

    async def check_balance(self) -> Dict[str, Any]:
        """Получить баланс кошелька. Реализация упрощённая."""
        return {"USDT": 1000.0}

    async def get_order_status(self, order_id: str, pair: Optional[str] = None) -> Dict[str, Any]:
        """Получить статус ордера. Возвращает фиктивные данные."""
        return {"id": order_id, "status": "filled", "pair": pair}

    async def get_open_orders(self, pair: Optional[str] = None) -> Dict[str, Any]:
        """Получить список открытых ордеров. Возвращает пустой список."""
        return []

    async def handle_api_error(self, exc: Exception) -> None:
        """Передать ошибку в общий обработчик."""
        handle_error("OKX API request failed", exc, self.notifier)
