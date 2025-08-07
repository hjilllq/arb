"""Bybit API helper for the arbitrage bot.

This module acts like a telephone connecting our bot to Bybit.  It dials
for prices, funding rates and orders while speaking gently so the Apple
Silicon M4 Max can stay cool.  Each function explains itself with simple
metaphors and examples.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
import ccxt.async_support as ccxt

import config
import database
import logger

# Directory where cached responses live.  Think of it as a small fridge where
# we store leftovers for later reuse.
_CACHE_DIR = Path("cache")
_CACHE_DIR.mkdir(exist_ok=True)

# Global exchange client created in :func:`connect_api`.
_client: Optional[ccxt.bybit] = None


# ---------------------------------------------------------------------------
# 1. connect_api
# ---------------------------------------------------------------------------
async def connect_api(api_key: str, api_secret: str, use_testnet: bool = False) -> None:
    """Connect to Bybit's REST API asynchronously.

    The connection follows Bybit's official documentation and supports both
    the real market and the testnet environment.  Set ``use_testnet`` to
    ``True`` to run the strategy against Bybit's paper trading platform before
    moving to production.

    Parameters
    ----------
    api_key, api_secret:
        Credentials for Bybit.  They can be plain strings or encrypted values
        that were previously decrypted by :mod:`config`.
    use_testnet:
        If ``True`` the client connects to Bybit's testnet; otherwise the
        production endpoints are used.

    Examples
    --------
    >>> await connect_api('key', 'secret', use_testnet=True)  # doctest: +SKIP
    """
    global _client
    _client = ccxt.bybit({
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
    })
    _client.set_sandbox_mode(use_testnet)
    try:
        await _client.load_markets()
        env = "testnet" if use_testnet else "mainnet"
        await logger.log_info(f"Connected to Bybit API ({env})")
    except Exception as exc:
        await handle_api_error(exc)


# ---------------------------------------------------------------------------
# 2. get_historical_data
# ---------------------------------------------------------------------------
async def get_historical_data(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
    contract_type: str = "spot",
) -> List[Dict[str, Any]]:
    """Fetch OHLCV candles for spot or futures.

    Parameters
    ----------
    symbol:
        Market symbol like ``"BTC/USDT"`` for spot or ``"BTCUSDT"`` for
        futures.
    timeframe:
        Candle timeframe such as ``"1h"``.
    start_date, end_date:
        Date range in ``YYYY-MM-DD``.
    contract_type:
        ``"spot"`` or ``"future"``.  Futures use Bybit's linear contracts.

    Returns
    -------
    list of dict
        Candles with ``timestamp`` and ``open``/``high``/``low``/``close``
        values.  Empty list if data is missing.

    Examples
    --------
    >>> rows = await get_historical_data('BTC/USDT','1h','2024-01-01','2024-01-02')  # doctest: +SKIP
    >>> rows[0]['open']  # doctest: +SKIP
    42000.0
    """
    if _client is None:
        await logger.log_error("Bybit client not connected", RuntimeError("no client"))
        return []

    since = int(datetime.fromisoformat(start_date).timestamp() * 1000)
    until = int(datetime.fromisoformat(end_date).timestamp() * 1000)

    category = "spot" if contract_type == "spot" else "linear"
    results: List[Dict[str, Any]] = []
    for attempt in range(1, 4):
        try:
            ohlcv = await _client.fetch_ohlcv(
                symbol,
                timeframe,
                since=since,
                params={"until": until, "category": category},
            )
            for row in ohlcv:
                results.append(
                    {
                        "timestamp": datetime.utcfromtimestamp(row[0] / 1000).isoformat(),
                        "open": row[1],
                        "high": row[2],
                        "low": row[3],
                        "close": row[4],
                        "volume": row[5],
                    }
                )
            if not results:
                await logger.log_warning("No historical data for %s", symbol)
            return results
        except Exception as exc:
            if attempt == 3:
                await handle_api_error(exc)
                return []
            await asyncio.sleep(5)
    return results


# ---------------------------------------------------------------------------
# 3. get_spot_futures_data
# ---------------------------------------------------------------------------
async def get_spot_futures_data(spot_symbol: str, futures_symbol: str) -> Dict[str, Any]:
    """Fetch current bid/ask for both spot and futures symbols.

    The function is like asking two friends for their latest prices at the same
    time.

    Returns
    -------
    dict
        ``{"spot": {"bid": 1.0, "ask": 1.1}, "futures": {"bid": ...}}``
        Empty values are returned if the API fails.
    """
    if _client is None:
        await logger.log_error("Bybit client not connected", RuntimeError("no client"))
        return {}

    mapping = config.get_pair_mapping()
    if mapping.get(spot_symbol) != futures_symbol:
        await logger.log_warning("Pair mapping mismatch for %s", spot_symbol)

    for attempt in range(1, 4):
        try:
            spot_ticker, fut_ticker = await asyncio.gather(
                _client.fetch_ticker(spot_symbol, params={"category": "spot"}),
                _client.fetch_ticker(futures_symbol, params={"category": "linear"}),
            )
            data = {
                "spot": {"bid": spot_ticker.get("bid"), "ask": spot_ticker.get("ask")},
                "futures": {
                    "bid": fut_ticker.get("bid"),
                    "ask": fut_ticker.get("ask"),
                },
            }
            # Save a slim snapshot to the database so history can be analysed
            # later.  Only the required fields are stored.
            await database.save_data(
                [
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "spot_symbol": spot_symbol,
                        "futures_symbol": futures_symbol,
                        "spot_bid": spot_ticker.get("bid"),
                        "futures_ask": fut_ticker.get("ask"),
                    }
                ]
            )
            return data
        except Exception as exc:
            if attempt == 3:
                await handle_api_error(exc)
                return {}
            await asyncio.sleep(5)
    return {}


# ---------------------------------------------------------------------------
# 4. get_funding_rate_history
# ---------------------------------------------------------------------------
async def get_funding_rate_history(symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Return funding rates for a futures symbol.

    Parameters
    ----------
    symbol:
        Futures symbol like ``"BTCUSDT"``.
    start_date, end_date:
        Range of interest in ``YYYY-MM-DD``.

    Returns
    -------
    list of dict
        ``[{"timestamp": "2025-08-05", "funding_rate": 0.0001}]``
    """
    if _client is None:
        await logger.log_error("Bybit client not connected", RuntimeError("no client"))
        return []

    since = int(datetime.fromisoformat(start_date).timestamp() * 1000)
    until = int(datetime.fromisoformat(end_date).timestamp() * 1000)
    for attempt in range(1, 4):
        try:
            fr = await _client.fetch_funding_rate_history(
                symbol, since=since, params={"until": until, "category": "linear"}
            )
            return [
                {
                    "timestamp": datetime.utcfromtimestamp(r["timestamp"] / 1000).isoformat(),
                    "funding_rate": r["fundingRate"],
                }
                for r in fr
            ]
        except Exception as exc:
            if attempt == 3:
                await handle_api_error(exc)
                return []
            await asyncio.sleep(5)
    return []


# ---------------------------------------------------------------------------
# 5. subscribe_to_websocket
# ---------------------------------------------------------------------------
async def subscribe_to_websocket(symbol: str, contract_type: str = "spot") -> None:
    """Listen to live prices via Bybit's public WebSocket.

    This function opens a WebSocket, subscribes to ticker updates, prints a few
    messages and then closes gracefully.  It's like holding a walkie-talkie for
    a short chat.
    """
    url = (
        "wss://stream.bybit.com/v5/public/spot"
        if contract_type == "spot"
        else "wss://stream.bybit.com/v5/public/linear"
    )
    topic = f"tickers.{symbol}"
    subscribe_msg = json.dumps({"op": "subscribe", "args": [topic]})
    async with aiohttp.ClientSession() as session:
        try:
            async with session.ws_connect(url, heartbeat=20) as ws:
                await ws.send_str(subscribe_msg)
                for _ in range(3):  # read a few messages then stop
                    msg = await ws.receive(timeout=20)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await logger.log_info(f"WS message: {msg.data[:60]}")
                    await ws.send_str(json.dumps({"op": "ping"}))
                    await asyncio.sleep(1)
        except Exception as exc:
            await handle_api_error(exc)


# ---------------------------------------------------------------------------
# 6. place_order
# ---------------------------------------------------------------------------
async def place_order(
    symbol: str,
    side: str,
    amount: float,
    price: float,
    order_type: str = "limit",
    contract_type: str = "spot",
) -> Dict[str, Any]:
    """Place an order on Bybit.

    Parameters
    ----------
    symbol, side, amount, price, order_type, contract_type
        Standard order fields.  ``contract_type`` chooses spot or futures.

    Returns
    -------
    dict
        Minimal order info like ``{"order_id": "123"}`` or empty dict on error.
    """
    if _client is None:
        await logger.log_error("Bybit client not connected", RuntimeError("no client"))
        return {}

    category = "spot" if contract_type == "spot" else "linear"
    for attempt in range(1, 4):
        try:
            order = await _client.create_order(
                symbol,
                order_type,
                side,
                amount,
                price,
                params={"category": category},
            )
            await logger.log_info(f"Placed order {order.get('id')}")
            return {"order_id": order.get("id")}
        except Exception as exc:
            if attempt == 3:
                await handle_api_error(exc)
                return {}
            await asyncio.sleep(5)
    return {}


# ---------------------------------------------------------------------------
# 7. cancel_order
# ---------------------------------------------------------------------------
async def cancel_order(order_id: str, contract_type: str = "spot") -> bool:
    """Cancel an existing order.

    Parameters
    ----------
    order_id:
        Identifier returned by :func:`place_order`.

    Returns
    -------
    bool
        ``True`` if cancellation succeeded.
    """
    if _client is None:
        await logger.log_error("Bybit client not connected", RuntimeError("no client"))
        return False

    category = "spot" if contract_type == "spot" else "linear"
    for attempt in range(1, 4):
        try:
            await _client.cancel_order(order_id, params={"category": category})
            await logger.log_info(f"Cancelled order {order_id}")
            return True
        except Exception as exc:
            if attempt == 3:
                await handle_api_error(exc)
                return False
            await asyncio.sleep(5)
    return False


# ---------------------------------------------------------------------------
# 8. get_balance
# ---------------------------------------------------------------------------
async def get_balance() -> Dict[str, Any]:
    """Return account balance.

    Examples
    --------
    >>> bal = await get_balance()  # doctest: +SKIP
    >>> bal.get('USDT')  # doctest: +SKIP
    1000
    """
    if _client is None:
        await logger.log_error("Bybit client not connected", RuntimeError("no client"))
        return {}

    for attempt in range(1, 4):
        try:
            balance = await _client.fetch_balance()
            return balance.get("total", {})
        except Exception as exc:
            if attempt == 3:
                await handle_api_error(exc)
                return {}
            await asyncio.sleep(5)
    return {}


# ---------------------------------------------------------------------------
# 9. cache_data
# ---------------------------------------------------------------------------
async def cache_data(data: List[Dict[str, Any]]) -> None:
    """Cache data to ``cache/`` as JSON.

    This behaves like putting leftovers into a labelled container so we can
    snack later without cooking again.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = _CACHE_DIR / f"data_{timestamp}.json"
    try:
        content = json.dumps(data)
        await asyncio.to_thread(file_path.write_text, content)
        await logger.log_info(f"Cached data to {file_path}")
    except Exception as exc:
        await handle_api_error(exc)


# ---------------------------------------------------------------------------
# 10. handle_api_error
# ---------------------------------------------------------------------------
async def handle_api_error(error: Exception) -> None:
    """Log API errors and notify the operator.

    This is the safety net.  It whispers the problem to :mod:`logger` and then
    taps the notification manager on the shoulder.
    """
    await logger.log_error("API error", error)
    try:
        from notification_manager import notify  # type: ignore

        await notify("Bybit API error", str(error))
    except Exception:
        # If notify fails we still want to keep running; the logger already
        # captured the error.
        pass
