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
# Small cache to avoid reloading markets too often: name -> (timestamp, markets)
_MARKET_CACHE: Dict[str, tuple[float, Dict]] = {}
# Small cache for pair synchronization: key (tuple of exchanges) -> (timestamp, mapping)
_PAIR_CACHE: Dict[tuple[str, ...], tuple[float, Dict[str, str]]] = {}
# Adaptive TTL so the cache refreshes faster when mappings change often and
# slows down during quiet periods.  Values in seconds.
_PAIR_TTL_MIN = 60
_PAIR_TTL_MAX = 900
_PAIR_TTL_STEP = 60
_PAIR_TTLS: Dict[tuple[str, ...], int] = {}
# Track long outages so we don't spam alerts every check
_OUTAGE_THRESHOLD = 1800  # seconds (30 minutes)
# Remember when outages were last reported to allow periodic reminders
_OUTAGE_REPORTED: set[str] = set()
_OUTAGE_ALERT_INTERVAL = 3600  # seconds (1 hour between outage alerts)
_LAST_OUTAGE_ALERT: Dict[str, float] = {}
# Count consecutive health check failures per exchange
_FAIL_COUNTS: Dict[str, int] = {}
_MAX_FAILURES = 5  # consecutive failures before extra notification

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
# 4. get_markets
# ---------------------------------------------------------------------------
async def get_markets(exchange_name: str, ttl: int = 300) -> Dict:
    """Return market metadata for an exchange with caching.

    The first call for a given exchange loads markets via ccxt and stores them
    with a timestamp.  Subsequent calls within ``ttl`` seconds reuse the cached
    copy which keeps the network calm and our ARM cores sleepy.

    Parameters
    ----------
    exchange_name:
        Name such as ``"bybit"``.  The exchange must be registered via
        :func:`add_exchange`.
    ttl:
        Time-to-live for the cache in seconds.  Defaults to five minutes.

    Returns
    -------
    dict
        The markets dictionary as returned by ``ccxt``.
    """

    name = exchange_name.lower()
    now = time.time()
    cached = _MARKET_CACHE.get(name)
    if cached and now - cached[0] < ttl:
        return cached[1]

    client = _EXCHANGES.get(name)
    if client is None:
        key_field, secret_field = _KEY_MAP.get(name, ("", ""))
        await add_exchange(name, CONFIG.get(key_field, ""), CONFIG.get(secret_field, ""))
        client = _EXCHANGES[name]

    try:
        markets = await client.load_markets()
    except ccxt.RateLimitExceeded as exc:
        logger.warning("Market load rate limited for %s: %s", name, exc)
        await notify("Rate limit", f"{name}: load_markets")
        raise
    except ccxt.NetworkError as exc:
        logger.warning("Network problem loading markets for %s: %s", name, exc)
        await notify("Network error", f"{name}: {exc}")
        raise
    except ccxt.ExchangeError as exc:
        logger.error("API error loading markets for %s: %s", name, exc)
        await notify("Market load failed", f"{name}: {exc}")
        raise
    _MARKET_CACHE[name] = (now, markets)
    return markets


# ---------------------------------------------------------------------------
# 5. sync_trading_pairs
# ---------------------------------------------------------------------------
async def sync_trading_pairs(exchanges: List[str]) -> Dict[str, str]:
    """Ensure all exchanges support the required spot/futures pairs.

    Results are cached with an adaptive time-to-live: when mappings remain
    stable the cache window widens, and if anything changes it shrinks to
    refresh quickly.  We cross-check the pair list from
    :func:`config.get_pair_mapping` with the markets reported by each exchange.
    Pairs missing on any exchange are dropped from the result.

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

    key = tuple(sorted(name.lower() for name in exchanges))
    now = time.time()
    ttl = _PAIR_TTLS.get(key, _PAIR_TTL_MIN)
    cached = _PAIR_CACHE.get(key)
    if cached and now - cached[0] < ttl:
        return cached[1]

    # Load markets concurrently, reusing cached results where possible.
    tasks = {name: asyncio.create_task(get_markets(name.lower())) for name in exchanges}
    markets_by_exchange: Dict[str, Dict] = {}
    try:
        results = await asyncio.gather(*tasks.values())
        for exch_name, markets in zip(tasks.keys(), results):
            markets_by_exchange[exch_name.lower()] = markets
    except ccxt.NetworkError as exc:
        logger.error("Network error during market sync: %s", exc)
        await notify("Market sync network error", str(exc))
        return {}
    except ccxt.ExchangeError as exc:
        logger.error("API error during market sync: %s", exc)
        await notify("Market sync API error", str(exc))
        return {}
    except Exception as exc:
        logger.error("Market load failed: %s", exc)
        await notify("Market sync failed", str(exc))
        return {}

    valid_mapping: Dict[str, str] = {}
    for spot_pair, fut_pair in mapping.items():
        missing = []
        for name in exchanges:
            markets = markets_by_exchange.get(name.lower(), {})
            if spot_pair not in markets or fut_pair not in markets:
                missing.append(name)
        if missing:
            logger.warning("Pair %s/%s missing on %s", spot_pair, fut_pair, ", ".join(missing))
        else:
            valid_mapping[spot_pair] = fut_pair
    previous = cached[1] if cached else None
    _PAIR_CACHE[key] = (now, valid_mapping)
    if previous and previous != valid_mapping:
        ttl = _PAIR_TTL_MIN
    else:
        ttl = min(ttl + _PAIR_TTL_STEP, _PAIR_TTL_MAX)
    _PAIR_TTLS[key] = ttl
    logger.info("Pair sync result: %s (ttl=%d s)", valid_mapping, ttl)
    return valid_mapping


# ---------------------------------------------------------------------------
# 5. check_exchange_health
# ---------------------------------------------------------------------------
async def check_exchange_health(exchange_name: str) -> bool:
    """Ping an exchange to see if it is alive.

    A simple ``fetch_time`` is used because it is lightweight and counts toward
    neither balances nor trading.  On failure the function increments a
    per-exchange counter, notifies operators after several consecutive errors,
    and may trigger a switch to the backup exchange if Bybit is sleepy for more
    than five minutes.  Exchanges offline for a long time keep sending periodic
    outage alerts so administrators do not miss a lingering problem.
    """

    name = exchange_name.lower()
    now = time.time()
    client = _EXCHANGES.get(name)
    if client is None:
        # Even if removed, keep reminding operators about persistent outages
        last = _last_health.get(name, 0)
        downtime = now - last
        if downtime > _OUTAGE_THRESHOLD:
            last_alert = _LAST_OUTAGE_ALERT.get(name, 0)
            if now - last_alert > _OUTAGE_ALERT_INTERVAL:
                _LAST_OUTAGE_ALERT[name] = now
                minutes = int(downtime // 60)
                logger.critical("Exchange %s still offline (%d min)", name, minutes)
                await notify("Exchange outage", f"{name} down {minutes}m")
        logger.error("Exchange %s not registered", name)
        return False
    try:
        await asyncio.wait_for(client.fetch_time(), timeout=10)
    except ccxt.RateLimitExceeded as exc:
        err_kind = "rate limit"
    except (ccxt.NetworkError, asyncio.TimeoutError) as exc:
        err_kind = "network"
    except ccxt.ExchangeError as exc:
        err_kind = "api"
    except Exception as exc:  # pragma: no cover - unexpected
        err_kind = "unknown"
    else:
        _last_health[name] = now
        _FAIL_COUNTS[name] = 0
        if name in _OUTAGE_REPORTED:
            _OUTAGE_REPORTED.discard(name)
        logger.debug("Exchange %s healthy", name)
        return True

    logger.warning("Health check %s error for %s: %s", err_kind, name, exc)
    await notify("Exchange health failed", f"{name} ({err_kind}): {exc}")
    failures = _FAIL_COUNTS.get(name, 0) + 1
    _FAIL_COUNTS[name] = failures
    if failures >= _MAX_FAILURES:
        logger.error("%s failed %d health checks", name, failures)
        await notify("Repeated health failures", f"{name} x{failures}")
        _FAIL_COUNTS[name] = 0
    last = _last_health.get(name, 0)
    if name == "bybit" and now - last > 300:
        await switch_to_backup_exchange()
    downtime = now - last
    if downtime > _OUTAGE_THRESHOLD:
        minutes = int(downtime // 60)
        last_alert = _LAST_OUTAGE_ALERT.get(name, 0)
        if now - last_alert > _OUTAGE_ALERT_INTERVAL:
            _LAST_OUTAGE_ALERT[name] = now
            if name not in _OUTAGE_REPORTED:
                _OUTAGE_REPORTED.add(name)
                logger.critical("Exchange %s outage detected (%d min)", name, minutes)
            else:
                logger.critical("Exchange %s outage persists (%d min)", name, minutes)
            await notify("Exchange outage", f"{name} down {minutes}m")
        logger.error("Removing %s after %d min offline", name, minutes)
        await remove_exchange(name, downtime)
    return False


# ---------------------------------------------------------------------------
# 7. remove_exchange
# ---------------------------------------------------------------------------
async def remove_exchange(exchange_name: str, downtime: float | None = None) -> None:
    """Remove an exchange client and forget its cached data.

    Parameters
    ----------
    exchange_name:
        Name of the exchange to drop.
    downtime:
        Seconds the venue has been unreachable. Used for logging to report how
        long the outage lasted.
    """

    name = exchange_name.lower()
    client = _EXCHANGES.pop(name, None)
    if client:
        try:
            await client.close()
        except Exception:
            pass
    _MARKET_CACHE.pop(name, None)
    # keep _last_health so later checks know how long we've been offline
    global _active_exchange
    if _active_exchange == name:
        _active_exchange = None
    if downtime is not None:
        logger.warning("Exchange %s removed after %.1f min offline", name, downtime / 60)
    else:
        logger.warning("Exchange %s removed due to outage", name)


# ---------------------------------------------------------------------------
# 8. close_all_exchanges
# ---------------------------------------------------------------------------
async def close_all_exchanges() -> None:
    """Close all ccxt clients to free network resources."""

    for client in _EXCHANGES.values():
        try:
            await client.close()
        except Exception:
            pass
    _EXCHANGES.clear()

