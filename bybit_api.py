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
from typing import Any, Dict, List, Optional, Tuple
import time

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

# In-memory cache for recent tickers to avoid hammering the API.
_TICKER_CACHE: Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]] = {}

# Simple counters to monitor cache effectiveness.  They help us observe
# whether we are mostly hitting cached values or still calling the exchange
# too often.  The numbers are exposed via :func:`get_cache_stats` and can be
# scraped by external metrics systems.
_CACHE_STATS = {
    "disk_hits": 0,
    "disk_misses": 0,
    "ticker_hits": 0,
    "ticker_misses": 0,
}


def get_cache_stats() -> Dict[str, int]:
    """Return current cache hit/miss counters."""

    return dict(_CACHE_STATS)


async def monitor_cache_usage() -> None:
    """Log how much space the cache directory consumes.

    When the cache grows beyond 90% of its configured limit a warning is
    emitted so operators can investigate or increase the allowance.
    """

    limit = config.get_cache_max_bytes()
    if limit <= 0:
        return

    def _size() -> int:
        return sum(p.stat().st_size for p in _CACHE_DIR.glob("*"))

    try:
        size = await asyncio.to_thread(_size)
    except Exception as exc:  # pragma: no cover - unexpected FS failure
        await handle_api_error(exc, context="cache_size")
        return

    usage = size / limit
    if usage >= 0.9:
        await logger.log_warning(
            f"Cache usage {size} bytes ({usage:.0%}) exceeds 90% of limit {limit}"
        )
    else:
        await logger.log_info(
            f"Cache usage {size} bytes ({usage:.0%}) within limit {limit}"
        )


async def _enforce_cache_limit() -> None:
    """Ensure the cache directory stays within the configured size limit."""
    limit = config.get_cache_max_bytes()
    if limit <= 0:
        return

    def _prune() -> List[str]:
        files = sorted(_CACHE_DIR.glob("*"), key=lambda p: p.stat().st_mtime)
        size = sum(p.stat().st_size for p in files)
        removed: List[str] = []
        while size > limit and files:
            victim = files.pop(0)
            try:
                victim_size = victim.stat().st_size
                victim.unlink()
                removed.append(victim.name)
                size -= victim_size
            except OSError:
                break
        return removed

    try:
        removed = await asyncio.to_thread(_prune)
        for name in removed:
            await logger.log_info(f"Cache trimmed: {name}")
    except Exception as exc:
        await handle_api_error(exc, context="cache_prune")
    await monitor_cache_usage()


def _normalize_futures_symbol(symbol: str) -> str:
    """Return a Bybit-friendly futures symbol.

    Bybit expects futures tickers without hyphenated suffixes, while config or
    databases may store variants like ``"BTC-USDT"`` or ``"BTC-PERP"``.  This
    helper trims those dashes so API calls work with both styles.
    """
    return symbol.replace("-", "")


# ---------------------------------------------------------------------------
# 1. connect_api
# ---------------------------------------------------------------------------
async def connect_api(
    api_key: str,
    api_secret: str,
    *,
    timeout_ms: int = 10_000,
    proxy: Optional[str] = None,
) -> None:
    """Connect to Bybit's REST API asynchronously.

    Parameters
    ----------
    api_key, api_secret:
        Credentials for Bybit. They can be plain strings or decrypted values
        from :mod:`config`.
    timeout_ms:
        Maximum time in milliseconds to wait for each HTTP request.
    proxy:
        Optional proxy URL applied to both HTTP and HTTPS.

    Examples
    --------
    >>> await connect_api('key', 'secret', timeout_ms=5_000)  # doctest: +SKIP
    """
    global _client
    cfg: Dict[str, Any] = {
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
        "timeout": timeout_ms,
        "options": {"defaultType": "spot"},
    }
    if proxy:
        cfg["proxies"] = {"http": proxy, "https": proxy}
    _client = ccxt.bybit(cfg)
    try:
        await _client.load_markets()
        # Warm up configuration helpers so they are cached in memory and
        # readily available for later calls without extra disk I/O.
        config.get_spot_pairs()
        config.get_futures_pairs()
        mapping = config.get_pair_mapping()
        await logger.log_info("Connected to Bybit API")
        await logger.log_info(f"Pair mapping warmed with {len(mapping)} items")
    except Exception as exc:
        # Close the client on failure to avoid dangling connections.
        try:
            await _client.close()
        finally:
            _client = None
        await handle_api_error(exc, context="connect_api")


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
    api_symbol = symbol if contract_type == "spot" else _normalize_futures_symbol(symbol)

    # Check cache before requesting the API to reduce load.
    cache_name = f"ohlcv_{api_symbol.replace('/', '')}_{timeframe}_{start_date}_{end_date}.json"
    cache_file = _CACHE_DIR / cache_name
    ttl = config.get_cache_ttl()
    if cache_file.exists():
        try:
            if ttl > 0 and time.time() - cache_file.stat().st_mtime <= ttl:
                text = await asyncio.to_thread(cache_file.read_text)
                cached = json.loads(text)
                if isinstance(cached, list):
                    _CACHE_STATS["disk_hits"] += 1
                    return cached
                await logger.log_warning(f"Cache format invalid: {cache_file}")
            else:
                await asyncio.to_thread(cache_file.unlink)
                await logger.log_info(f"Cache expired: {cache_file}")
        except json.JSONDecodeError as exc:
            await logger.log_warning(f"Cache corrupted {cache_file}: {exc}")
            await asyncio.to_thread(cache_file.unlink)
            await logger.log_info(f"Corrupt cache removed: {cache_file}")
        except Exception as exc:
            await handle_api_error(exc, context="cache_read ohlcv")

    results: List[Dict[str, Any]] = []
    while since < until:
        for attempt in range(1, 4):
            try:
                ohlcv = await _client.fetch_ohlcv(
                    api_symbol,
                    timeframe,
                    since=since,
                    limit=200,
                    params={"category": category},
                )
                break
            except Exception as exc:
                await handle_api_error(
                    exc, attempt, context=f"fetch_ohlcv {api_symbol} {timeframe}"
                )
                if attempt == 3:
                    return results
        if not ohlcv:
            break
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
        since = ohlcv[-1][0] + 1
        if ohlcv[-1][0] >= until:
            break
    if not results:
        await logger.log_warning(f"No historical data for {symbol}")
    else:
        await logger.log_info(f"Fetched {len(results)} candles for {symbol}")
        try:
            await asyncio.to_thread(cache_file.write_text, json.dumps(results))
            await logger.log_info(f"Cached OHLCV to {cache_file}")
            await _enforce_cache_limit()
        except Exception as exc:
            await handle_api_error(exc, context=f"cache_write ohlcv {cache_file}")
        finally:
            _CACHE_STATS["disk_misses"] += 1
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
        await logger.log_warning(f"Pair mapping mismatch for {spot_symbol}")

    api_fut = _normalize_futures_symbol(futures_symbol)

    key = (spot_symbol, futures_symbol)
    now = time.monotonic()
    cached = _TICKER_CACHE.get(key)
    ttl = config.get_ticker_cache_ttl()
    if cached and now - cached[0] < ttl:
        _CACHE_STATS["ticker_hits"] += 1
        return cached[1]

    _CACHE_STATS["ticker_misses"] += 1
    for attempt in range(1, 4):
        try:
            spot_ticker, fut_ticker = await asyncio.gather(
                _client.fetch_ticker(spot_symbol, params={"category": "spot"}),
                _client.fetch_ticker(api_fut, params={"category": "linear"}),
            )
            data = {
                "spot": {"bid": spot_ticker.get("bid"), "ask": spot_ticker.get("ask")},
                "futures": {
                    "bid": fut_ticker.get("bid"),
                    "ask": fut_ticker.get("ask"),
                },
            }
            _TICKER_CACHE[key] = (now, data)
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
            await logger.log_info(
                f"Fetched tickers for {spot_symbol}/{futures_symbol}"
            )
            return data
        except Exception as exc:
            await handle_api_error(
                exc, attempt, context=f"ticker {spot_symbol}/{futures_symbol}"
            )
            if attempt == 3:
                return {}
    return {}


# ---------------------------------------------------------------------------
# 3b. get_multiple_tickers
# ---------------------------------------------------------------------------
async def get_multiple_tickers(
    symbols: List[str], contract_type: str = "spot"
) -> Dict[str, Dict[str, Optional[float]]]:
    """Fetch tickers for many symbols in parallel."""
    if _client is None:
        await logger.log_error("Bybit client not connected", RuntimeError("no client"))
        return {}

    category = "spot" if contract_type == "spot" else "linear"
    tasks = []
    api_symbols = []
    for sym in symbols:
        api_sym = sym if contract_type == "spot" else _normalize_futures_symbol(sym)
        api_symbols.append(sym)
        tasks.append(_client.fetch_ticker(api_sym, params={"category": category}))

    tickers = await asyncio.gather(*tasks, return_exceptions=True)
    results: Dict[str, Dict[str, Optional[float]]] = {}
    for sym, ticker in zip(api_symbols, tickers):
        if isinstance(ticker, Exception):
            await handle_api_error(ticker, context=f"bulk_ticker {sym}")
        else:
            results[sym] = {"bid": ticker.get("bid"), "ask": ticker.get("ask")}
    await logger.log_info(f"Fetched {len(results)} tickers")
    return results


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
    api_symbol = _normalize_futures_symbol(symbol)

    cache_name = f"funding_{api_symbol}_{start_date}_{end_date}.json"
    cache_file = _CACHE_DIR / cache_name
    ttl = config.get_cache_ttl()
    if cache_file.exists():
        try:
            if ttl > 0 and time.time() - cache_file.stat().st_mtime <= ttl:
                text = await asyncio.to_thread(cache_file.read_text)
                cached = json.loads(text)
                if isinstance(cached, list):
                    _CACHE_STATS["disk_hits"] += 1
                    return cached
                await logger.log_warning(f"Cache format invalid: {cache_file}")
            else:
                await asyncio.to_thread(cache_file.unlink)
                await logger.log_info(f"Cache expired: {cache_file}")
        except json.JSONDecodeError as exc:
            await logger.log_warning(f"Cache corrupted {cache_file}: {exc}")
            await asyncio.to_thread(cache_file.unlink)
            await logger.log_info(f"Corrupt cache removed: {cache_file}")
        except Exception as exc:
            await handle_api_error(exc, context="cache_read funding")

    for attempt in range(1, 4):
        try:
            fr = await _client.fetch_funding_rate_history(
                api_symbol, since=since, params={"until": until, "category": "linear"}
            )
            data = [
                {
                    "timestamp": datetime.utcfromtimestamp(r["timestamp"] / 1000).isoformat(),
                    "funding_rate": r["fundingRate"],
                }
                for r in fr
            ]
            await logger.log_info(f"Fetched {len(data)} funding rates for {symbol}")
            try:
                await asyncio.to_thread(cache_file.write_text, json.dumps(data))
                await logger.log_info(f"Cached funding to {cache_file}")
                await _enforce_cache_limit()
            except Exception as exc:
                await handle_api_error(exc, context=f"cache_write funding {cache_file}")
            finally:
                _CACHE_STATS["disk_misses"] += 1
            return data
        except Exception as exc:
            await handle_api_error(exc, attempt, context=f"funding {symbol}")
            if attempt == 3:
                return []
    return []


# ---------------------------------------------------------------------------
# 5. subscribe_to_websocket
# ---------------------------------------------------------------------------
async def subscribe_to_websocket(
    symbol: str,
    contract_type: str = "spot",
    max_messages: Optional[int] = 3,
) -> None:
    """Listen to live prices via Bybit's public WebSocket.

    The connection retries on failure and uses ping/pong messages to stay
    healthy. ``max_messages`` limits how many updates are processed before the
    function returns; use ``None`` for continuous streaming.

    Notes
    -----
    Private channels (balances, order events) live at
    ``wss://stream.bybit.com/v5/private`` and require an authenticated
    handshake. This helper focuses on public price feeds but can be extended to
    cover the private stream later.
    """
    url = (
        "wss://stream.bybit.com/v5/public/spot"
        if contract_type == "spot"
        else "wss://stream.bybit.com/v5/public/linear"
    )
    api_symbol = symbol if contract_type == "spot" else _normalize_futures_symbol(symbol)
    topic = f"tickers.{api_symbol}"
    subscribe_msg = json.dumps({"op": "subscribe", "args": [topic]})

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=20) as ws:
                    await ws.send_str(subscribe_msg)
                    received = 0
                    while max_messages is None or received < max_messages:
                        msg = await ws.receive(timeout=20)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await logger.log_info(f"WS message: {msg.data[:60]}")
                        await ws.send_str(json.dumps({"op": "ping"}))
                        await asyncio.sleep(1)
                        received += 1
                    return
        except Exception as exc:
            await handle_api_error(exc, context=f"websocket {symbol}")
            continue


# ---------------------------------------------------------------------------
# 6. place_order
# ---------------------------------------------------------------------------
async def place_order(
    symbol: str,
    side: str,
    amount: float,
    price: Optional[float],
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

    side_l = side.lower()
    if side_l not in {"buy", "sell"}:
        await logger.log_error("Invalid order side", ValueError(side))
        return {}

    type_l = order_type.lower()
    if type_l not in {"limit", "market"}:
        await logger.log_error("Invalid order type", ValueError(order_type))
        return {}

    category = "spot" if contract_type == "spot" else "linear"
    api_symbol = symbol if contract_type == "spot" else _normalize_futures_symbol(symbol)
    if type_l == "market":
        price = None
    for attempt in range(1, 4):
        try:
            order = await _client.create_order(
                api_symbol,
                order_type,
                side,
                amount,
                price,
                params={"category": category},
            )
            await logger.log_info(
                f"Placed {side_l} {amount} {symbol} at {price} ({order.get('id')})"
            )
            return {"order_id": order.get("id")}
        except Exception as exc:
            await handle_api_error(
                exc, attempt, context=f"place_order {symbol} {side_l} amt={amount} price={price}"
            )
            if attempt == 3:
                return {}
    return {}


# ---------------------------------------------------------------------------
# 7. cancel_order
# ---------------------------------------------------------------------------
async def cancel_order(symbol: str, order_id: str, contract_type: str = "spot") -> bool:
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
    api_symbol = symbol if contract_type == "spot" else _normalize_futures_symbol(symbol)
    for attempt in range(1, 4):
        try:
            await _client.cancel_order(order_id, api_symbol, params={"category": category})
            await logger.log_info(f"Cancelled order {order_id} on {symbol}")
            return True
        except Exception as exc:
            await handle_api_error(
                exc, attempt, context=f"cancel_order {order_id} {symbol}"
            )
            if attempt == 3:
                return False
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
            await handle_api_error(exc, attempt, context="get_balance")
            if attempt == 3:
                return {}
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
        await _enforce_cache_limit()
    except Exception as exc:
        await handle_api_error(exc, context=f"cache_data {file_path}")


# ---------------------------------------------------------------------------
# 10. handle_api_error
# ---------------------------------------------------------------------------
async def handle_api_error(
    error: Exception, attempt: int = 1, *, context: str = ""
) -> None:
    """Log API errors, notify the operator and wait before retrying.

    Parameters
    ----------
    error:
        The exception raised by the API call.
    attempt:
        Current retry attempt starting from ``1``.  The delay grows
        exponentially to calm down when the API is angry.
    context:
        Short description of the failed operation included in logs and
        notifications.
    """

    message = context or "API error"
    await logger.log_error(message, error)
    delay = min(5 * 2 ** (attempt - 1), 60)
    if isinstance(error, ccxt.RateLimitExceeded):
        delay *= 2
    elif isinstance(error, (ccxt.NetworkError, ccxt.RequestTimeout)):
        delay += 5
    try:
        from notification_manager import notify  # type: ignore

        detail = f"{context} {type(error).__name__}: {error}".strip()
        await notify("Bybit API error", detail)
    except Exception:
        pass
    await asyncio.sleep(delay)


# ---------------------------------------------------------------------------
# 11. close_api
# ---------------------------------------------------------------------------
async def close_api() -> None:
    """Close the Bybit client gracefully."""
    global _client
    if _client is not None:
        try:
            await _client.close()
        finally:
            _client = None
