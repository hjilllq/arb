"""Utilities for interacting with the Bybit REST API.

This module provides a small asynchronous client that can fetch market data
and manage simple orders on Bybit. Credentials are read from the environment
variables ``BYBIT_API_KEY`` and ``BYBIT_API_SECRET``.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import time
from typing import Any, Dict, Optional

import httpx
import logging


logger = logging.getLogger(__name__)


class BybitAPI:
    """Minimal asynchronous wrapper around Bybit's HTTP API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: str = "https://api.bybit.com",
    ) -> None:
        self.api_key = api_key or os.getenv("BYBIT_API_KEY", "")
        self.api_secret = api_secret or os.getenv("BYBIT_API_SECRET", "")
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)

    async def __aenter__(self) -> "BybitAPI":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Attach authentication information to *params* for private requests."""
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API key and secret are required for this operation")
        params["api_key"] = self.api_key
        params["timestamp"] = int(time.time() * 1000)
        query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        signature = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        params["sign"] = signature
        return params

    async def get_spot_data(self, pair: str) -> Dict[str, Any]:
        """Retrieve 24h ticker information for a spot trading pair."""
        for attempt in range(3):
            try:
                resp = await self._client.get("/spot/v3/public/quote/ticker/24hr", params={"symbol": pair})
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def get_futures_data(self, pair: str) -> Dict[str, Any]:
        """Retrieve ticker information for a futures trading pair."""
        for attempt in range(3):
            try:
                resp = await self._client.get("/derivatives/v3/public/tickers", params={"symbol": pair})
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def place_order(
        self,
        pair: str,
        price: float,
        qty: float,
        side: str = "Buy",
        order_type: str = "Limit",
    ) -> Dict[str, Any]:
        """Place an order on Bybit."""
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {
                    "symbol": pair,
                    "side": side,
                    "orderType": order_type,
                    "price": price,
                    "qty": qty,
                }
                params = self._sign(params)
                resp = await self._client.post("/v5/order/create", json=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def cancel_order(self, order_id: str, pair: Optional[str] = None) -> Dict[str, Any]:
        """Cancel a previously placed order."""
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {"orderId": order_id}
                if pair is not None:
                    params["symbol"] = pair
                params = self._sign(params)
                resp = await self._client.post("/v5/order/cancel", json=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def check_balance(self) -> Dict[str, Any]:
        """Fetch wallet balance for the account."""
        for attempt in range(3):
            try:
                params = self._sign({})
                resp = await self._client.get("/v5/account/wallet-balance", params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def get_order_status(self, order_id: str, pair: Optional[str] = None) -> Dict[str, Any]:
        """Retrieve the current status for a specific order."""
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {"orderId": order_id}
                if pair is not None:
                    params["symbol"] = pair
                params = self._sign(params)
                resp = await self._client.get("/v5/order/realtime", params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def handle_api_error(self, exc: httpx.HTTPError, attempt: int, retries: int = 3) -> None:
        """Simple exponential-backoff retry handler for API errors."""
        logger.warning(
            "Bybit API error on attempt %d/%d: %s", attempt + 1, retries, exc
        )
        if attempt + 1 >= retries:
            raise exc
        await asyncio.sleep(2 ** attempt)

    async def close(self) -> None:
        """Close the underlying HTTP client session."""
        await self._client.aclose()
