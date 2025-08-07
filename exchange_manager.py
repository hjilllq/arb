"""High-level helpers to manage trades on Bybit.

This module wraps :class:`~bybit_api.BybitAPI` to expose convenient methods
for fetching market data, managing orders, and retrieving account
information.  It keeps the interface intentionally small to serve as a
starting point for more advanced trade management logic.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import httpx
import logging

from bybit_api import BybitAPI

logger = logging.getLogger(__name__)


class ExchangeManager:
    """Wrapper around :class:`BybitAPI` with additional helpers."""

    def __init__(self, api: Optional[BybitAPI] = None) -> None:
        self.api = api or BybitAPI()

    async def get_spot_data(self, pair: str) -> Dict[str, Any]:
        """Return ticker information for a spot trading pair."""
        return await self.api.get_spot_data(pair)

    async def get_futures_data(self, pair: str) -> Dict[str, Any]:
        """Return ticker information for a futures trading pair."""
        return await self.api.get_futures_data(pair)

    async def place_order(
        self,
        pair: str,
        price: float,
        qty: float,
        side: str = "Buy",
        order_type: str = "Limit",
    ) -> Dict[str, Any]:
        """Submit an order to Bybit."""
        return await self.api.place_order(pair, price, qty, side, order_type)

    async def check_balance(self) -> Dict[str, Any]:
        """Fetch wallet balance for the account."""
        return await self.api.check_balance()

    async def get_open_orders(self, pair: Optional[str] = None) -> Dict[str, Any]:
        """Retrieve currently open orders.

        Args:
            pair: Optional trading pair to filter orders.
        """
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {}
                if pair:
                    params["symbol"] = pair
                params = self.api._sign(params)
                resp = await self.api._client.get("/v5/order/realtime", params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.api.handle_api_error(exc, attempt)

    async def cancel_order(self, order_id: str, pair: Optional[str] = None) -> Dict[str, Any]:
        """Cancel an existing order."""
        return await self.api.cancel_order(order_id, pair)

    async def get_market_status(
        self, pair: str, category: str = "linear"
    ) -> Dict[str, Any]:
        """Return trading status information for *pair*.

        Args:
            pair: Trading pair to query.
            category: Market category, e.g. ``"spot"``, ``"linear"``.
        """
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {"category": category, "symbol": pair}
                resp = await self.api._client.get(
                    "/v5/market/instruments-info", params=params
                )
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.api.handle_api_error(exc, attempt)

    async def close(self) -> None:
        """Close underlying HTTP connections."""
        await self.api.close()
