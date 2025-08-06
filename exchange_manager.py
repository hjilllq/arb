"""Exchange manager for spot/futures arbitrage.

This module acts like a calm conductor of an orchestra: it keeps track of
which exchange is playing (Bybit or Binance), checks if everyone is awake, and
switches players when one dozes off.  The functions are short and friendly so a
child could follow the tune.  Everything is written with asyncio to keep the
Apple Silicon M4 Max cool and efficient.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Protocol
from collections import defaultdict
from contextlib import asynccontextmanager

import ccxt.async_support as ccxt

# Graceful fallbacks if the lightweight test stub lacks these classes
RateLimitExceeded = getattr(ccxt, "RateLimitExceeded", type("RateLimitExceeded", (Exception,), {}))
NetworkError = getattr(ccxt, "NetworkError", type("NetworkError", (Exception,), {}))
ExchangeError = getattr(ccxt, "ExchangeError", type("ExchangeError", (Exception,), {}))

from config import (
    CONFIG,
    get_exchange_request_limit,
    get_pair_mapping,
    get_request_retries,
    get_request_retry_delay,
    get_switch_backup_timeout,
    get_manual_outage_threshold,
)
from logger import get_logger

# Optional Prometheus metrics support.  The module is entirely optional so the
# manager can run in very lightweight environments.  When installed a tiny HTTP
# server can be started to expose runtime metrics for systems such as Grafana or
# Prometheus.
class MetricsRecorder(Protocol):
    """Small interface for reporting optional runtime metrics."""

    def record_health(self, name: str, timestamp: float) -> None:  # pragma: no cover - interface only
        ...

    def record_latency(self, name: str, latency: float) -> None:  # pragma: no cover - interface only
        ...

    def record_error(self, name: str, kind: str) -> None:  # pragma: no cover - interface only
        ...


try:  # pragma: no cover - metrics optional in tests
    from prometheus_client import Counter, Gauge, start_http_server

    class _PromMetrics:
        """Prometheus-backed metrics recorder."""

        def __init__(self) -> None:
            self._health = Gauge(
                "exchange_last_health_seconds",
                "Unix timestamp of the last successful health check",
                ["exchange"],
            )
            self._latency = Gauge(
                "exchange_latency_seconds",
                "Latency of the last health check",
                ["exchange"],
            )
            self._errors = Counter(
                "exchange_errors_total",
                "Number of API/health errors grouped by exchange and type",
                ["exchange", "type"],
            )

        def record_health(self, name: str, timestamp: float) -> None:
            self._health.labels(name).set(timestamp)

        def record_latency(self, name: str, latency: float) -> None:
            self._latency.labels(name).set(latency)

        def record_error(self, name: str, kind: str) -> None:
            self._errors.labels(name, kind).inc()

    _METRICS: MetricsRecorder = _PromMetrics()

    def start_metrics_server(port: int = 8000) -> None:
        """Start an HTTP server exposing Prometheus metrics."""

        start_http_server(port)

except Exception:  # pragma: no cover - library missing

    class _NullMetrics:
        """Fallback recorder when no metrics backend is available."""

        def record_health(self, name: str, timestamp: float) -> None:  # pragma: no cover - simple
            pass

        def record_latency(self, name: str, latency: float) -> None:  # pragma: no cover - simple
            pass

        def record_error(self, name: str, kind: str) -> None:  # pragma: no cover - simple
            pass

    _METRICS: MetricsRecorder = _NullMetrics()

    def start_metrics_server(port: int = 8000) -> None:
        """Fallback when :mod:`prometheus_client` is not available."""

        logger.info("Prometheus client not installed; metrics disabled")


def set_metrics_recorder(recorder: MetricsRecorder) -> None:
    """Inject a custom metrics recorder (e.g. StatsD, OpenTelemetry)."""

    global _METRICS
    _METRICS = recorder

try:  # pragma: no cover - notifier provided in real project
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover - fallback for early development
    async def notify(message: str, detail: str = "") -> None:
        get_logger(__name__).warning("Notification: %s - %s", message, detail)

logger = get_logger(__name__)

@dataclass(slots=True)
class ExchangeState:
    """Small container for per-exchange data.

    Using ``__slots__`` keeps per-exchange memory usage tiny even when dozens of
    venues are tracked simultaneously.
    """

    client: ccxt.Exchange | None
    last_health: float = 0.0
    fail_count: int = 0
    rate_limiter: asyncio.Semaphore | None = None
    ping: float = 0.0


# ---------------------------------------------------------------------------
# Internal state containers.
# ---------------------------------------------------------------------------
_STATES: Dict[str, ExchangeState] = {}
_active_exchange: str | None = None
# Small cache to avoid reloading markets too often: name -> (timestamp, markets)
_MARKET_CACHE: Dict[str, tuple[float, Dict]] = {}
# Locks to ensure only one coroutine refreshes markets for a given exchange.
_MARKET_LOCKS: Dict[str, asyncio.Lock] = {}
# Background tasks refreshing market data to reduce lock contention.
_MARKET_REFRESH: Dict[str, asyncio.Task] = {}
# Small cache for pair synchronization: key (tuple of exchanges) -> (timestamp, mapping)
_PAIR_CACHE: Dict[tuple[str, ...], tuple[float, Dict[str, str]]] = {}
# Prevent concurrent pair refreshes to avoid blocking other tasks.
_PAIR_LOCKS: Dict[tuple[str, ...], asyncio.Lock] = {}
# Background tasks refreshing pair mappings.
_PAIR_REFRESH: Dict[tuple[str, ...], asyncio.Task] = {}
# Adaptive TTL so the cache refreshes faster when mappings change often and
# slows down during quiet periods.  Values in seconds and configurable via
# ``config.py`` using ``PAIR_TTL_MIN``, ``PAIR_TTL_MAX`` and ``PAIR_TTL_STEP``.
_PAIR_TTL_MIN = int(CONFIG.get("PAIR_TTL_MIN", 60))
_PAIR_TTL_MAX = int(CONFIG.get("PAIR_TTL_MAX", 900))
_PAIR_TTL_STEP = int(CONFIG.get("PAIR_TTL_STEP", 60))
_PAIR_TTLS: Dict[tuple[str, ...], int] = {}
# Track long outages so we don't spam alerts every check.  All thresholds are
# configurable via the environment so operators can tune alerting.
_OUTAGE_THRESHOLD = int(CONFIG.get("OUTAGE_THRESHOLD", 1800))
# Remember when outages were last reported to allow periodic reminders
_OUTAGE_REPORTED: set[str] = set()
_OUTAGE_ALERT_INTERVAL = int(CONFIG.get("OUTAGE_ALERT_INTERVAL", 3600))
_LAST_OUTAGE_ALERT: Dict[str, float] = {}
_MAX_FAILURES = int(CONFIG.get("HEALTH_FAILURE_THRESHOLD", 5))
# Switch after a few consecutive failures even if total downtime is short
_FAIL_SWITCH = int(CONFIG.get("HEALTH_SWITCH_FAILURES", 3))
_SWITCH_TIMEOUT = get_switch_backup_timeout()
# Escalate after very long outages to require manual intervention.
_MANUAL_OUTAGE_THRESHOLD = get_manual_outage_threshold()
_MANUAL_OUTAGES: set[str] = set()

# Retry and rate-limit settings for graceful recovery under load.
_DEFAULT_RETRIES = get_request_retries()
_RETRY_DELAY = get_request_retry_delay()
_REQUEST_LIMIT = get_exchange_request_limit()

# Error counters grouped by exchange and error type for production monitoring.
# ``defaultdict`` keeps memory usage small even with many exchanges and allows
# quick reset when metrics are scraped.
_ERROR_STATS: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

# Optional listeners notified whenever a health check completes.  Each callback
# receives ``(exchange_name, healthy, info)`` where ``info`` is a short string
# describing the error type on failure.
_HEALTH_LISTENERS: List[Callable[[str, bool, str], Awaitable[None]]] = []

# Helper mapping from logical names to config keys for API credentials.
_KEY_MAP = {
    "bybit": ("API_KEY", "API_SECRET"),
    "binance": ("BACKUP_API_KEY", "BACKUP_API_SECRET"),
}

# Primary exchange to which the bot prefers to connect.  Backup exchanges will be
# used only when the primary is considered unhealthy.
_PRIMARY = str(CONFIG.get("PRIMARY_EXCHANGE", "bybit")).lower()

# Parameters governing how often we attempt to reconnect to the primary exchange
# once a failover occurred.  ``_RECONNECT_DELAY`` starts small and grows by a
# configurable factor after each failed attempt.  ``_RECONNECT_DECAY`` allows the
# factor to scale with the time since the last healthy probe so operators can
# reduce or increase pressure dynamically.
_RECONNECT_DELAY = int(CONFIG.get("RECONNECT_DELAY", 300))
_RECONNECT_MAX_DELAY = int(CONFIG.get("RECONNECT_MAX_DELAY", 3600))
_RECONNECT_FACTOR = float(CONFIG.get("RECONNECT_FACTOR", 2.0))
_RECONNECT_DECAY = int(CONFIG.get("RECONNECT_DECAY", 3600))
_NEXT_RECONNECT: float = 0.0


def _get_backup_candidates() -> List[str]:
    """Return a prioritized list of backup exchanges.

    ``BACKUP_EXCHANGES`` may be provided in :mod:`config` as a JSON list or a
    comma separated string.  If absent, ``BACKUP_EXCHANGE`` or ``"binance"`` is
    used.  Results are normalized to lower case.
    """

    raw = CONFIG.get("BACKUP_EXCHANGES")
    if raw:
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(x).lower() for x in parsed if x]
            except Exception:
                return [s.strip().lower() for s in raw.split(",") if s.strip()]
        if isinstance(raw, (list, tuple)):
            return [str(x).lower() for x in raw if x]
    return [str(CONFIG.get("BACKUP_EXCHANGE", "binance")).lower()]


def _record_error(name: str, kind: str) -> None:
    """Aggregate error ``kind`` for ``name`` and update metrics."""

    _ERROR_STATS[name][kind] += 1
    _METRICS.record_error(name, kind)


def _get_semaphore(name: str) -> asyncio.Semaphore:
    """Return (and lazily create) the semaphore for ``name``."""

    state = _STATES.get(name)
    if state is None:
        sem = asyncio.Semaphore(_REQUEST_LIMIT)
        _STATES[name] = ExchangeState(client=None, rate_limiter=sem)
        return sem
    sem = state.rate_limiter
    if sem is None:
        sem = asyncio.Semaphore(_REQUEST_LIMIT)
        state.rate_limiter = sem
    return sem


@asynccontextmanager
async def _rate_limiter(name: str):
    """Limit concurrent requests for ``name`` failing fast when busy."""

    sem = _get_semaphore(name)
    try:
        await asyncio.wait_for(sem.acquire(), timeout=0.1)
    except asyncio.TimeoutError:
        _record_error(name, "rate_limit")
        raise RateLimitExceeded(f"{name} rate limit reached")
    try:
        yield
    finally:
        sem.release()


# Track specific operation failures such as withdrawals so the bot can react
# when an action repeatedly fails (e.g. an exchange refusing all withdrawals).
_OPERATION_ERROR_LIMITS = {
    "withdraw": int(CONFIG.get("WITHDRAW_FAILURE_LIMIT", 3)),
}
_OPERATION_FAILURES: Dict[str, int] = defaultdict(int)


def _record_operation_failure(action: str) -> bool:
    """Increment failure count for ``action`` and report if threshold hit."""

    limit = _OPERATION_ERROR_LIMITS.get(action)
    if not limit:
        return False
    _OPERATION_FAILURES[action] += 1
    if _OPERATION_FAILURES[action] >= limit:
        _OPERATION_FAILURES[action] = 0
        return True
    return False


def _reset_operation_failure(action: str) -> None:
    """Reset failure counter for ``action`` after a successful call."""

    if action in _OPERATION_FAILURES:
        _OPERATION_FAILURES[action] = 0


def get_error_stats(reset: bool = False) -> Dict[str, Dict[str, int]]:
    """Return a copy of error counters and optionally clear them."""

    stats = {ex: dict(types) for ex, types in _ERROR_STATS.items()}
    if reset:
        _ERROR_STATS.clear()
    return stats


def register_health_listener(
    callback: Callable[[str, bool, str], Awaitable[None]]
) -> None:
    """Register ``callback`` to receive health events.

    The callback is awaited with ``(exchange_name, healthy, info)`` where
    ``info`` provides a short error category such as ``"network"``.
    """

    _HEALTH_LISTENERS.append(callback)


async def _fire_health_event(name: str, healthy: bool, info: str = "") -> None:
    """Invoke all registered health listeners safely."""

    if not _HEALTH_LISTENERS:
        return
    for cb in list(_HEALTH_LISTENERS):
        try:
            await cb(name, healthy, info)
        except Exception as exc:  # pragma: no cover - diagnostic only
            logger.error("Health listener failed: %s", exc)


async def monitor_rate_limits(threshold: float | None = None) -> Dict[str, int]:
    """Report remaining request slots and warn on heavy usage.

    Parameters
    ----------
    threshold:
        Fraction of the limit in use before a warning is emitted.  If ``None``,
        ``RATE_ALERT_THRESHOLD`` from :mod:`config` is used.

    Returns
    -------
    dict
        Mapping of exchange names to remaining slots.
    """

    if threshold is None:
        threshold = float(CONFIG.get("RATE_ALERT_THRESHOLD", 0.8))

    status: Dict[str, int] = {}
    for name, state in _STATES.items():
        sem = state.rate_limiter
        if sem is None:
            continue
        remaining = sem._value
        status[name] = remaining
        used = _REQUEST_LIMIT - remaining
        if _REQUEST_LIMIT and used / _REQUEST_LIMIT >= threshold:
            logger.warning("%s rate limit high: %d/%d", name, used, _REQUEST_LIMIT)
            await notify("Rate limit high", f"{name}: {used}/{_REQUEST_LIMIT}")
    return status


async def _call_with_retries(
    func: Callable[[], Awaitable],
    name: str,
    action: str,
    *,
    detail: str = "",
    retries: int | None = None,
    delay: float | None = None,
):
    """Run ``func`` with retries for network and rate limit errors.

    ``detail`` can supply contextual information such as the affected symbol, so
    production logs and monitoring systems receive richer diagnostics.

    Parameters
    ----------
    retries, delay:
        Override the global retry policy.  ``delay`` grows exponentially after
        each attempt to reduce load during outages.
    """

    retries = retries or _DEFAULT_RETRIES
    delay = delay or _RETRY_DELAY

    for attempt in range(1, retries + 1):
        start = time.perf_counter()
        try:
            result = await func()
            _reset_operation_failure(action)
            return result
        except (RateLimitExceeded, NetworkError, ExchangeError) as exc:
            elapsed = time.perf_counter() - start
            err_type = type(exc).__name__
            _record_error(name, err_type)
            extra = f" {detail}" if detail else ""
            if attempt == retries:
                logger.error(
                    "%s failed for %s after %d attempts (%s, %.3fs)%s: %s",
                    action,
                    name,
                    attempt,
                    err_type,
                    elapsed,
                    extra,
                    exc,
                )
                if _record_operation_failure(action):
                    await notify(
                        f"Repeated {action} failures",
                        f"{name}: {err_type} x{_OPERATION_ERROR_LIMITS.get(action)}",
                    )
                else:
                    await notify(f"{action} failed", f"{name}: {exc}")
                raise
            logger.warning(
                "%s error for %s (attempt %d/%d, %s, %.3fs)%s: %s",
                action,
                name,
                attempt,
                retries,
                err_type,
                elapsed,
                extra,
                exc,
            )
            await asyncio.sleep(delay * 2 ** (attempt - 1))
        except Exception as exc:  # pragma: no cover - unexpected
            elapsed = time.perf_counter() - start
            err_type = type(exc).__name__
            _record_error(name, err_type)
            extra = f" {detail}" if detail else ""
            if attempt == retries:
                logger.error(
                    "%s unexpected failure for %s after %d attempts (%s, %.3fs)%s: %s",
                    action,
                    name,
                    attempt,
                    err_type,
                    elapsed,
                    extra,
                    exc,
                )
                if _record_operation_failure(action):
                    await notify(
                        f"Repeated {action} failures",
                        f"{name}: {err_type} x{_OPERATION_ERROR_LIMITS.get(action)}",
                    )
                else:
                    await notify(f"{action} failed", f"{name}: {exc}")
                raise
            logger.warning(
                "%s unexpected error for %s (attempt %d/%d, %s, %.3fs)%s: %s",
                action,
                name,
                attempt,
                retries,
                err_type,
                elapsed,
                extra,
                exc,
            )
            await asyncio.sleep(delay * 2 ** (attempt - 1))


async def _get_client_for_exchange(name: str) -> ccxt.Exchange:
    """Return a ccxt client for ``name`` creating it on demand."""

    state = _STATES.get(name)
    client = state.client if state else None
    if client is None:
        key_field, secret_field = _KEY_MAP.get(name, ("", ""))
        await add_exchange(name, CONFIG.get(key_field, ""), CONFIG.get(secret_field, ""))
        state = _STATES[name]
        client = state.client  # type: ignore[assignment]
    return client


async def _probe_exchange(name: str) -> float | None:
    """Measure ``fetch_time`` latency for ``name``.

    A ``None`` result indicates that the exchange is unreachable.  Latencies are
    stored in the per-exchange state and, if Prometheus metrics are enabled,
    updated in the corresponding gauges.
    """

    try:
        client = await _get_client_for_exchange(name)
    except Exception as exc:  # pragma: no cover - construction failure
        logger.error("Failed to get client for %s: %s", name, exc)
        return None

    start = time.perf_counter()
    try:
        async with _rate_limiter(name):
            await _call_with_retries(
                client.fetch_time, name, "fetch_time", detail="latency probe"
            )
    except Exception as exc:
        logger.error("Latency probe failed for %s: %s", name, exc)
        return None

    latency = time.perf_counter() - start
    state = _STATES.get(name)
    if state:
        state.ping = latency
    _METRICS.record_latency(name, latency)
    return latency


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
    _MANUAL_OUTAGES.discard(name)
    if not api_key or not api_secret:
        key_field, secret_field = _KEY_MAP.get(name, ("", ""))
        api_key = api_key or CONFIG.get(key_field, "")
        api_secret = api_secret or CONFIG.get(secret_field, "")
    logger.debug("Creating client for %s", name)
    try:
        exchange_cls = getattr(ccxt, name)
        client = exchange_cls(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                # Use spot markets by default; futures can still be accessed
                # by specifying contracts explicitly.
                "options": {"defaultType": "spot"},
            }
        )
        state = ExchangeState(
            client=client,
            rate_limiter=asyncio.Semaphore(_REQUEST_LIMIT),
        )
        _STATES[name] = state
        global _active_exchange
        if _active_exchange is None:
            _active_exchange = name
        logger.info("Exchange %s added (limit=%d)", name, _REQUEST_LIMIT)
    except Exception as exc:  # pragma: no cover - depends on ccxt internals
        logger.error("Failed to add exchange %s: %s", name, exc)
        await notify("Exchange add failed", f"{name}: {exc}")
        raise


# ---------------------------------------------------------------------------
# 2. switch_to_backup_exchange
# ---------------------------------------------------------------------------
async def switch_to_backup_exchange() -> bool:
    """Switch active exchange to the healthiest available backup.

    All candidates are probed concurrently and the one with the lowest latency
    is selected.  If none respond the active exchange remains unchanged.
    """

    global _active_exchange, _NEXT_RECONNECT
    candidates = [c for c in _get_backup_candidates() if c != _active_exchange]
    if not candidates:
        logger.error("No backup exchanges available for failover")
        return False

    logger.info("Probing backup exchanges: %s", candidates)
    probes = {name: asyncio.create_task(_probe_exchange(name)) for name in candidates}
    results = await asyncio.gather(*probes.values())
    latencies = {name: lat for name, lat in zip(probes.keys(), results) if lat is not None}
    if not latencies:
        logger.error("All backup exchanges unreachable: %s", candidates)
        return False

    logger.info("Backup probe latencies: %s", latencies)
    best = min(latencies, key=latencies.get)
    old = _active_exchange
    _active_exchange = best
    _NEXT_RECONNECT = time.time() + _RECONNECT_DELAY
    logger.warning(
        "Switching from %s to backup %s (latency %.3fs)",
        old,
        best,
        latencies[best],
    )
    return True


async def _maybe_reconnect_primary() -> bool:
    """Attempt to reconnect to the primary exchange if the backoff period passed.

    Returns ``True`` if the primary became active.  The delay between attempts
    grows by ``RECONNECT_FACTOR`` after each failure and scales with the time
    since the last healthy probe using ``RECONNECT_DECAY``.
    """

    global _active_exchange, _RECONNECT_DELAY, _NEXT_RECONNECT
    if _active_exchange == _PRIMARY:
        return True

    now = time.time()
    if now < _NEXT_RECONNECT:
        return False

    logger.info("Attempting to reconnect to primary %s", _PRIMARY)
    latency = await _probe_exchange(_PRIMARY)
    if latency is not None:
        old = _active_exchange
        _active_exchange = _PRIMARY
        _RECONNECT_DELAY = int(CONFIG.get("RECONNECT_DELAY", 300))
        state = _STATES.get(_PRIMARY)
        if state:
            state.last_health = now
        logger.warning(
            "Reconnected to primary %s from %s (latency %.3fs)",
            _PRIMARY,
            old,
            latency,
        )
        return True

    state = _STATES.get(_PRIMARY)
    last = state.last_health if state else 0
    downtime = now - last
    growth = _RECONNECT_FACTOR
    if _RECONNECT_DECAY > 0:
        growth += downtime / _RECONNECT_DECAY
    _RECONNECT_DELAY = min(int(_RECONNECT_DELAY * growth), _RECONNECT_MAX_DELAY)
    _NEXT_RECONNECT = now + _RECONNECT_DELAY
    logger.error(
        "Primary %s still unreachable; next retry in %ds",
        _PRIMARY,
        _RECONNECT_DELAY,
    )
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
        Mapping of symbol strings to market metadata as returned by
        ``ccxt.load_markets``.  The structure matches ccxt's format so fields
        like ``"precision"`` and ``"limits"`` are available.

    Examples
    --------
    >>> asyncio.run(get_markets('bybit'))  # doctest: +SKIP
    {'BTC/USDT': {'precision': {...}, 'limits': {...}}, ...}
    """

    name = exchange_name.lower()
    if name in _MANUAL_OUTAGES:
        raise ExchangeError("manual intervention required")
    now = time.time()
    cached = _MARKET_CACHE.get(name)
    if cached and now - cached[0] < ttl:
        return cached[1]
    task = _MARKET_REFRESH.get(name)
    if task and not task.done():
        if cached:
            return cached[1]
        await task
        cached = _MARKET_CACHE.get(name)
        if cached:
            return cached[1]
    lock = _MARKET_LOCKS.setdefault(name, asyncio.Lock())
    if lock.locked() and cached:
        return cached[1]
    async with lock:
        now = time.time()
        cached = _MARKET_CACHE.get(name)
        if cached and now - cached[0] < ttl:
            return cached[1]

        state = _STATES.get(name)
        client = state.client if state else None
        if client is None:
            key_field, secret_field = _KEY_MAP.get(name, ("", ""))
            await add_exchange(name, CONFIG.get(key_field, ""), CONFIG.get(secret_field, ""))
            state = _STATES[name]
            client = state.client  # type: ignore[assignment]

        async def _refresh() -> None:
            async with _rate_limiter(name):
                markets = await _call_with_retries(
                    client.load_markets, name, "load_markets", detail="sync pairs"
                )

            if not isinstance(markets, dict) or not markets:
                logger.error("%s returned invalid markets payload: %r", name, markets)
                await notify("Invalid market data", f"{name} returned {type(markets).__name__}")
                raise ExchangeError("invalid markets payload")

            _MARKET_CACHE[name] = (time.time(), markets)

        task = asyncio.create_task(_refresh())
        _MARKET_REFRESH[name] = task
    try:
        await task
    finally:
        _MARKET_REFRESH.pop(name, None)
    return _MARKET_CACHE.get(name, (0, {}))[1]


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
        given exchange.  The cache lifetime adapts between
        ``PAIR_TTL_MIN`` and ``PAIR_TTL_MAX`` from :mod:`config` with steps of
        ``PAIR_TTL_STEP``.

    Examples
    --------
    >>> asyncio.run(sync_trading_pairs(['bybit', 'binance']))  # doctest: +SKIP
    {'BTC/USDT': 'BTCUSDT'}
    """

    logger.info("Syncing trading pairs across %s", exchanges)
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
    task = _PAIR_REFRESH.get(key)
    if task and not task.done():
        if cached:
            return cached[1]
        await task
        cached = _PAIR_CACHE.get(key)
        ttl = _PAIR_TTLS.get(key, _PAIR_TTL_MIN)
        if cached and now - cached[0] < ttl:
            return cached[1]

    lock = _PAIR_LOCKS.setdefault(key, asyncio.Lock())
    if lock.locked() and cached:
        return cached[1]

    async with lock:
        now = time.time()
        cached = _PAIR_CACHE.get(key)
        ttl = _PAIR_TTLS.get(key, _PAIR_TTL_MIN)
        if cached and now - cached[0] < ttl:
            return cached[1]

        async def _refresh() -> None:
            # Load markets concurrently, reusing cached results where possible.
            tasks = {name: asyncio.create_task(get_markets(name.lower())) for name in exchanges}
            markets_by_exchange: Dict[str, Dict] = {}
            try:
                results = await asyncio.gather(*tasks.values())
                for exch_name, markets in zip(tasks.keys(), results):
                    markets_by_exchange[exch_name.lower()] = markets
            except NetworkError as exc:
                logger.error("Network error during market sync: %s", exc)
                await notify("Market sync network error", str(exc))
                return
            except ExchangeError as exc:
                logger.error("API error during market sync: %s", exc)
                await notify("Market sync API error", str(exc))
                return
            except Exception as exc:
                logger.error("Market load failed: %s", exc)
                await notify("Market sync failed", str(exc))
                return

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
            _PAIR_CACHE[key] = (time.time(), valid_mapping)
            if previous == valid_mapping:
                _PAIR_TTLS[key] = min(_PAIR_TTLS.get(key, _PAIR_TTL_MIN) + _PAIR_TTL_STEP, _PAIR_TTL_MAX)
            else:
                _PAIR_TTLS[key] = _PAIR_TTL_MIN

        task = asyncio.create_task(_refresh())
        _PAIR_REFRESH[key] = task
    try:
        await task
    finally:
        _PAIR_REFRESH.pop(key, None)
    return _PAIR_CACHE.get(key, (0, {}))[1]


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
    state = _STATES.get(name)
    client = state.client if state else None
    if client is None:
        # Even if removed, keep reminding operators about persistent outages
        last = state.last_health if state else 0
        downtime = now - last
        if downtime > _OUTAGE_THRESHOLD:
            last_alert = _LAST_OUTAGE_ALERT.get(name, 0)
            if now - last_alert > _OUTAGE_ALERT_INTERVAL:
                _LAST_OUTAGE_ALERT[name] = now
                minutes = int(downtime // 60)
                logger.critical("Exchange %s still offline (%d min)", name, minutes)
                await notify("Exchange outage", f"{name} down {minutes}m")
        if downtime > _MANUAL_OUTAGE_THRESHOLD and name not in _MANUAL_OUTAGES:
            _MANUAL_OUTAGES.add(name)
            hours = int(downtime // 3600)
            logger.critical("Exchange %s down %dh - manual intervention required", name, hours)
            await notify("Manual intervention required", f"{name} offline {hours}h")
        logger.error("Exchange %s not registered", name)
        return False
    err = None
    try:
        start = time.perf_counter()
        async with _rate_limiter(name):
            await asyncio.wait_for(
                _call_with_retries(
                    client.fetch_time, name, "fetch_time", detail="health check"
                ),
                timeout=10,
            )
        latency = time.perf_counter() - start
        if state:
            state.ping = latency
        _METRICS.record_latency(name, latency)
    except RateLimitExceeded as exc:
        err_kind = "rate limit"
        err = exc
    except (NetworkError, asyncio.TimeoutError) as exc:
        err_kind = "network"
        err = exc
    except ExchangeError as exc:
        err_kind = "api"
        err = exc
    except Exception as exc:  # pragma: no cover - unexpected
        err_kind = "unknown"
        err = exc
    else:
        if state:
            state.last_health = now
            state.fail_count = 0
        if name in _OUTAGE_REPORTED:
            _OUTAGE_REPORTED.discard(name)
        _METRICS.record_health(name, now)
        logger.info("Exchange %s healthy (%.3fs)", name, latency)
        await _fire_health_event(name, True)
        if name == _active_exchange and _active_exchange != _PRIMARY:
            await _maybe_reconnect_primary()
        return True

    failures = (state.fail_count if state else 0) + 1
    if state:
        state.fail_count = failures
    _record_error(name, err_kind)
    last = state.last_health if state else 0
    downtime = now - last
    logger.warning(
        "Health check %s error for %s (failure %d, downtime %.1fs): %s",
        err_kind,
        name,
        failures,
        downtime,
        err,
    )
    await notify("Exchange health failed", f"{name} ({err_kind}): {err}")
    if failures >= _MAX_FAILURES:
        logger.error("%s failed %d health checks", name, failures)
        await notify("Repeated health failures", f"{name} x{failures}")
        if state:
            state.fail_count = 0
    if name == "bybit" and _active_exchange == "bybit":
        if now - last > _SWITCH_TIMEOUT or failures >= _FAIL_SWITCH:
            if state:
                state.fail_count = 0
            await switch_to_backup_exchange()
    downtime = now - last
    if downtime > _MANUAL_OUTAGE_THRESHOLD and name not in _MANUAL_OUTAGES:
        _MANUAL_OUTAGES.add(name)
        hours = int(downtime // 3600)
        logger.critical("Exchange %s down %dh - manual intervention required", name, hours)
        await notify("Manual intervention required", f"{name} offline {hours}h")
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
    await _fire_health_event(name, False, err_kind)
    if name == _active_exchange and _active_exchange != _PRIMARY:
        await _maybe_reconnect_primary()
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
    state = _STATES.pop(name, None)
    client = state.client if state else None
    if client:
        try:
            await client.close()
        except Exception:
            pass
    _MARKET_CACHE.pop(name, None)
    # keep state.last_health so later checks know how long we've been offline
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

    for state in _STATES.values():
        client = state.client
        if client:
            try:
                await client.close()
            except Exception:
                pass
    _STATES.clear()


def collect_metrics() -> Dict[str, Dict[str, float | int | str]]:
    """Return internal counters for external monitoring systems.

    The returned dictionary can be easily exported to JSON or plugged into a
    metrics collector.  It contains the currently active exchange, last health
    timestamps, failure counts, and measured latencies.
    """

    return {
        "active_exchange": _active_exchange or "",
        "last_health": {n: s.last_health for n, s in _STATES.items()},
        "fail_counts": {n: s.fail_count for n, s in _STATES.items()},
        "latency": {n: s.ping for n, s in _STATES.items()},
        "error_counts": {ex: dict(kinds) for ex, kinds in _ERROR_STATS.items()},
    }
