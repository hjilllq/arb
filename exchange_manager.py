"""Exchange manager for spot/futures arbitrage.

This module acts like a calm conductor of an orchestra: it keeps track of
which exchange is playing (Bybit or Binance), checks if everyone is awake, and
switches players when one dozes off.  The functions are short and friendly so a
child could follow the tune.  Everything is written with asyncio to keep the
Apple Silicon M4 Max cool and efficient.
"""
from __future__ import annotations

import asyncio
import time
from typing import Dict, List

import ccxt.async_support as ccxt

from config import CONFIG, get_pair_mapping
from logger import get_logger

try:  # pragma: no cover - notifier provided in real project
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover - fallback for early development
    async def notify(message: str, detail: str = "") -> None:
        get_logger(__name__).warning("Notification: %s - %s", message, detail)

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Internal state containers.
# ---------------------------------------------------------------------------
_EXCHANGES: Dict[str, ccxt.Exchange] = {}
_active_exchange: str | None = None
_last_health: Dict[str, float] = {}

# Helper mapping from logical names to config keys for API credentials.
_KEY_MAP = {
    "bybit": ("API_KEY", "API_SECRET"),
    "binance": ("BACKUP_API_KEY", "BACKUP_API_SECRET"),
}


# ---------------------------------------------------------------------------
# 1. add_exchange
# ---------------------------------------------------------------------------
async def add_exchange(exchange_name: str, api_key: str, api_secret: str) -> None:
    """Create and store a ccxt client for the given exchange.

    Think of this as adding a new player to our orchestra.  If the keys are
    blank we try to pull them from :mod:`config`.

    Parameters
    ----------
    exchange_name:
        Name understood by :mod:`ccxt`, e.g. ``"bybit"`` or ``"binance"``.
    api_key, api_secret:
        Credentials for private requests.  If empty, values from the config are
        used.

    Examples
    --------
    >>> asyncio.run(add_exchange('binance', 'key123', 'secret789'))  # doctest: +SKIP
    """

    name = exchange_name.lower()
    if not api_key or not api_secret:
        key_field, secret_field = _KEY_MAP.get(name, ("", ""))
        api_key = api_key or CONFIG.get(key_field, "")
        api_secret = api_secret or CONFIG.get(secret_field, "")
    try:
        exchange_cls = getattr(ccxt, name)
        client = exchange_cls(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
            }
        )
        _EXCHANGES[name] = client
        _last_health.setdefault(name, 0.0)
        global _active_exchange
        if _active_exchange is None:
            _active_exchange = name
        logger.info("Exchange %s added", name)
    except Exception as exc:  # pragma: no cover - depends on ccxt internals
        logger.error("Failed to add exchange %s: %s", name, exc)
        await notify("Exchange add failed", f"{name}: {exc}")
        raise


# ---------------------------------------------------------------------------
# 2. switch_to_backup_exchange
# ---------------------------------------------------------------------------
async def switch_to_backup_exchange() -> bool:
    """Switch active exchange to the configured backup (usually Binance).

    Returns ``True`` on success.  The backup is created on the fly if needed.
    """

    backup = CONFIG.get("BACKUP_EXCHANGE", "binance").lower()
    global _active_exchange
    if _active_exchange == backup:
        return True
    try:
        if backup not in _EXCHANGES:
            key_field, secret_field = _KEY_MAP.get(backup, ("", ""))
            await add_exchange(backup, CONFIG.get(key_field, ""), CONFIG.get(secret_field, ""))
        old = _active_exchange
        _active_exchange = backup
        logger.warning("Switching from %s to backup %s", old, backup)
        return True
    except Exception as exc:
        logger.error("Failed to switch to backup %s: %s", backup, exc)
        await notify("Backup switch failed", str(exc))
        return False


# ---------------------------------------------------------------------------
# 3. get_active_exchange
# ---------------------------------------------------------------------------
def get_active_exchange() -> str:
    """Return the name of the exchange currently used for trading."""

    return _active_exchange or ""


# ---------------------------------------------------------------------------
# 4. sync_trading_pairs
# ---------------------------------------------------------------------------
async def sync_trading_pairs(exchanges: List[str]) -> Dict[str, str]:
    """Ensure all exchanges support the required spot/futures pairs.

    We cross-check the pair list from :func:`config.get_pair_mapping` with the
    markets reported by each exchange.  Pairs missing on any exchange are
    dropped from the result.

    Parameters
    ----------
    exchanges:
        List of exchange names to verify, e.g. ``['bybit', 'binance']``.

    Returns
    -------
    dict
        Mapping like ``{'BTC/USDT': 'BTCUSDT'}`` for pairs available on every
        given exchange.
    """

    mapping = get_pair_mapping()
    if not mapping:
        logger.error("No pair mapping defined in configuration")
        await notify("Pair sync failed", "mapping missing")
        return {}

    # Load markets concurrently to minimise waiting time.
    tasks = []
    for name in exchanges:
        name = name.lower()
        client = _EXCHANGES.get(name)
        if client is None:
            key_field, secret_field = _KEY_MAP.get(name, ("", ""))
            await add_exchange(name, CONFIG.get(key_field, ""), CONFIG.get(secret_field, ""))
            client = _EXCHANGES[name]
        tasks.append(client.load_markets())
    try:
        await asyncio.gather(*tasks)
    except Exception as exc:
        logger.error("Market load failed: %s", exc)
        await notify("Market sync failed", str(exc))
        return {}

    valid_mapping: Dict[str, str] = {}
    for spot_pair, fut_pair in mapping.items():
        missing = []
        for name in exchanges:
            client = _EXCHANGES[name.lower()]
            markets = client.markets or {}
            if spot_pair not in markets or fut_pair not in markets:
                missing.append(name)
        if missing:
            logger.warning("Pair %s/%s missing on %s", spot_pair, fut_pair, ", ".join(missing))
        else:
            valid_mapping[spot_pair] = fut_pair
    logger.info("Pair sync result: %s", valid_mapping)
    return valid_mapping


# ---------------------------------------------------------------------------
# 5. check_exchange_health
# ---------------------------------------------------------------------------
async def check_exchange_health(exchange_name: str) -> bool:
    """Ping an exchange to see if it is alive.

    A simple ``fetch_time`` is used because it is lightweight and counts toward
    neither balances nor trading.  On failure the function logs the issue and
    may trigger a switch to the backup exchange if Bybit is sleepy for more
    than five minutes.
    """

    name = exchange_name.lower()
    client = _EXCHANGES.get(name)
    if client is None:
        logger.error("Exchange %s not registered", name)
        return False
    try:
        await asyncio.wait_for(client.fetch_time(), timeout=10)
        _last_health[name] = time.time()
        logger.debug("Exchange %s healthy", name)
        return True
    except Exception as exc:
        logger.warning("Health check failed for %s: %s", name, exc)
        await notify("Exchange health failed", f"{name}: {exc}")
        last = _last_health.get(name, 0)
        if name == "bybit" and time.time() - last > 300:
            await switch_to_backup_exchange()
        return False


# ---------------------------------------------------------------------------
# Optional cleanup utility.
# ---------------------------------------------------------------------------
async def close_all_exchanges() -> None:
    """Close all ccxt clients to free network resources."""

    for client in _EXCHANGES.values():
        try:
            await client.close()
        except Exception:
            pass
    _EXCHANGES.clear()

