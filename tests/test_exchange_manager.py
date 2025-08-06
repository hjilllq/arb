import asyncio
import asyncio
import time

import pytest

import exchange_manager as em
from exchange_manager import ccxt


class DummyClient:
    """Simple stub client for ccxt exchanges used in tests."""

    def __init__(self, markets=None, fail_fetch=False):
        self._markets = markets or {}
        self._fail_fetch = fail_fetch

    async def load_markets(self):
        return self._markets

    async def fetch_time(self):
        if self._fail_fetch:
            raise ccxt.NetworkError("offline")
        return int(time.time() * 1000)

    async def close(self):
        pass


@pytest.fixture(autouse=True)
def reset_state():
    original_config = dict(em.CONFIG)
    em._EXCHANGES.clear()
    em._active_exchange = None
    em._last_health.clear()
    em._MARKET_CACHE.clear()
    em._PAIR_CACHE.clear()
    em._PAIR_TTLS.clear()
    em._RATE_LIMITERS.clear()
    em._HEALTH_LISTENERS.clear()
    em._FAIL_COUNTS.clear()
    em._ERROR_STATS.clear()
    em._RETRY_DELAY = 0
    em._DEFAULT_RETRIES = 1
    yield
    em._EXCHANGES.clear()
    em.CONFIG.clear()
    em.CONFIG.update(original_config)


@pytest.mark.asyncio
async def test_add_and_switch(monkeypatch):
    monkeypatch.setattr(ccxt, "bybit", lambda config=None: DummyClient())
    monkeypatch.setattr(ccxt, "binance", lambda config=None: DummyClient())

    await em.add_exchange("bybit", "key", "secret")
    assert em.get_active_exchange() == "bybit"

    await em.add_exchange("binance", "k", "s")
    await em.switch_to_backup_exchange()
    assert em.get_active_exchange() == "binance"


@pytest.mark.asyncio
async def test_sync_trading_pairs(monkeypatch):
    async def fake_get_markets(name):
        return {"BTC/USDT": {}, "BTCUSDT": {}, "ETH/USDT": {}, "ETHUSDT": {}}

    monkeypatch.setattr(em, "get_pair_mapping", lambda: {"BTC/USDT": "BTCUSDT", "ETH/USDT": "ETHUSDT"})
    monkeypatch.setattr(em, "get_markets", fake_get_markets)

    mapping = await em.sync_trading_pairs(["bybit", "binance"])
    assert mapping == {"BTC/USDT": "BTCUSDT", "ETH/USDT": "ETHUSDT"}


@pytest.mark.asyncio
async def test_check_exchange_health_switches(monkeypatch):
    monkeypatch.setattr(ccxt, "bybit", lambda config=None: DummyClient(fail_fetch=True))
    monkeypatch.setattr(ccxt, "binance", lambda config=None: DummyClient())

    await em.add_exchange("bybit", "k", "s")
    await em.add_exchange("binance", "k", "s")
    em._last_health["bybit"] = time.time() - 400

    ok = await em.check_exchange_health("bybit")
    assert not ok
    assert em.get_active_exchange() == "binance"


@pytest.mark.asyncio
async def test_failure_count_triggers_switch(monkeypatch):
    monkeypatch.setattr(ccxt, "bybit", lambda config=None: DummyClient(fail_fetch=True))
    monkeypatch.setattr(ccxt, "binance", lambda config=None: DummyClient())

    await em.add_exchange("bybit", "k", "s")
    await em.add_exchange("binance", "k", "s")
    em._last_health["bybit"] = time.time()

    for _ in range(em._FAIL_SWITCH):
        await em.check_exchange_health("bybit")

    assert em.get_active_exchange() == "binance"


@pytest.mark.asyncio
async def test_switch_tries_multiple_backups(monkeypatch):
    """Switching falls back to the next candidate when one fails."""

    # bybit is active, backups are 'bad' then 'good'
    monkeypatch.setattr(ccxt, "bybit", lambda config=None: DummyClient())

    class Bad:
        def __init__(self, config=None):
            raise RuntimeError("boom")

    monkeypatch.setattr(ccxt, "bad", Bad, raising=False)
    monkeypatch.setattr(ccxt, "good", lambda config=None: DummyClient(), raising=False)

    await em.add_exchange("bybit", "k", "s")
    em.CONFIG["BACKUP_EXCHANGES"] = '["bad","good"]'

    ok = await em.switch_to_backup_exchange()
    assert ok
    assert em.get_active_exchange() == "good"


@pytest.mark.asyncio
async def test_get_markets_retries(monkeypatch):
    """get_markets retries on transient errors."""

    calls = {"n": 0}

    async def flaky_load():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ccxt.NetworkError("temp")
        return {"BTC/USDT": {}}

    client = DummyClient()
    client.load_markets = flaky_load
    em._EXCHANGES["bybit"] = client
    monkeypatch.setattr(em, "_RETRY_DELAY", 0)
    monkeypatch.setattr(em, "_DEFAULT_RETRIES", 3)

    markets = await em.get_markets("bybit", ttl=0)
    assert markets == {"BTC/USDT": {}}
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_monitor_rate_limits_warns(monkeypatch):
    """monitor_rate_limits reports usage and triggers notification."""

    notified = {}

    async def fake_notify(msg, detail=""):
        notified["msg"] = msg

    monkeypatch.setattr(em, "notify", fake_notify)
    em._REQUEST_LIMIT = 2
    sem = em._rate_limiter("bybit")

    async with sem:
        status = await em.monitor_rate_limits(threshold=0.4)

    assert status["bybit"] == 1
    assert notified.get("msg") == "Rate limit high"


@pytest.mark.asyncio
async def test_monitor_rate_limits_uses_config(monkeypatch):
    """Default threshold comes from configuration when not supplied."""

    notified = {}

    async def fake_notify(msg, detail=""):
        notified["msg"] = msg

    monkeypatch.setattr(em, "notify", fake_notify)
    em.CONFIG["RATE_ALERT_THRESHOLD"] = 0.1
    em._REQUEST_LIMIT = 2
    sem = em._rate_limiter("bybit")

    async with sem:
        await em.monitor_rate_limits()

    assert notified.get("msg") == "Rate limit high"


@pytest.mark.asyncio
async def test_health_listener_called(monkeypatch):
    events = []

    async def listener(name, healthy, info):
        events.append((name, healthy, info))

    em.register_health_listener(listener)

    monkeypatch.setattr(ccxt, "bybit", lambda config=None: DummyClient())
    await em.add_exchange("bybit", "k", "s")

    em._last_health["bybit"] = time.time()
    await em.check_exchange_health("bybit")

    assert events and events[0] == ("bybit", True, "")


@pytest.mark.asyncio
async def test_switch_selects_fastest_backup(monkeypatch):
    """When multiple backups respond the lowest-latency one is chosen."""

    class Slow(DummyClient):
        async def fetch_time(self):  # type: ignore[override]
            await asyncio.sleep(0.05)
            return int(time.time() * 1000)

    class Fast(DummyClient):
        async def fetch_time(self):  # type: ignore[override]
            return int(time.time() * 1000)

    monkeypatch.setattr(ccxt, "slow", lambda config=None: Slow(), raising=False)
    monkeypatch.setattr(ccxt, "fast", lambda config=None: Fast(), raising=False)
    monkeypatch.setattr(ccxt, "bybit", lambda config=None: DummyClient())

    await em.add_exchange("bybit", "k", "s")
    em.CONFIG["BACKUP_EXCHANGES"] = '["slow","fast"]'

    ok = await em.switch_to_backup_exchange()
    assert ok
    assert em.get_active_exchange() == "fast"


@pytest.mark.asyncio
async def test_collect_metrics(monkeypatch):
    monkeypatch.setattr(ccxt, "bybit", lambda config=None: DummyClient())
    await em.add_exchange("bybit", "k", "s")
    em._last_health["bybit"] = 1.0
    em._FAIL_COUNTS["bybit"] = 2
    em._PING_TIMES["bybit"] = 0.1

    metrics = em.collect_metrics()
    assert metrics["active_exchange"] == "bybit"
    assert metrics["fail_counts"]["bybit"] == 2
    assert metrics["latency"]["bybit"] == 0.1


@pytest.mark.asyncio
async def test_error_stats(monkeypatch):
    monkeypatch.setattr(ccxt, "bybit", lambda config=None: DummyClient(fail_fetch=True))
    monkeypatch.setattr(ccxt, "binance", lambda config=None: DummyClient())

    await em.add_exchange("bybit", "k", "s")
    await em.add_exchange("binance", "k", "s")
    em._last_health["bybit"] = time.time()
    await em.check_exchange_health("bybit")

    metrics = em.collect_metrics()
    assert metrics["error_counts"]["bybit"]["network"] == 1


def test_metrics_recorder_and_error_stats():
    class Dummy:
        def __init__(self):
            self.errors: list[tuple[str, str]] = []

        def record_health(self, name: str, ts: float) -> None:
            pass

        def record_latency(self, name: str, latency: float) -> None:
            pass

        def record_error(self, name: str, kind: str) -> None:
            self.errors.append((name, kind))

    rec = Dummy()
    em.set_metrics_recorder(rec)
    em._record_error("bybit", "network")
    assert rec.errors == [("bybit", "network")]
    stats = em.get_error_stats(reset=True)
    assert stats["bybit"]["network"] == 1
    assert em.get_error_stats() == {}


@pytest.mark.asyncio
async def test_reconnect_backoff(monkeypatch):
    monkeypatch.setattr(ccxt, "bybit", lambda config=None: DummyClient(fail_fetch=True))
    monkeypatch.setattr(ccxt, "binance", lambda config=None: DummyClient())

    await em.add_exchange("binance", "k", "s")
    em._active_exchange = "binance"
    em._RECONNECT_DELAY = 1
    em._RECONNECT_MAX_DELAY = 4
    em._NEXT_RECONNECT = 0

    ok = await em._maybe_reconnect_primary()
    assert not ok
    first_delay = em._RECONNECT_DELAY
    assert first_delay == 2
    # simulate another immediate attempt to ensure delay grows again
    em._NEXT_RECONNECT = 0
    await em._maybe_reconnect_primary()
    assert em._RECONNECT_DELAY == 4


@pytest.mark.asyncio
async def test_get_markets_invalid_payload(monkeypatch):
    client = DummyClient(markets=None)
    em._EXCHANGES["bybit"] = client

    with pytest.raises(em.ExchangeError):
        await em.get_markets("bybit", ttl=0)


@pytest.mark.asyncio
async def test_health_failures_reset_after_success(monkeypatch):
    """Consecutive failure counts drop back to zero on recovery."""

    class Flaky(DummyClient):
        def __init__(self):
            super().__init__()
            self.calls = 0

        async def fetch_time(self):  # type: ignore[override]
            self.calls += 1
            if self.calls <= 2:
                raise ccxt.NetworkError("offline")
            return int(time.time() * 1000)

    monkeypatch.setattr(ccxt, "bybit", lambda config=None: Flaky())

    await em.add_exchange("bybit", "k", "s")
    em._last_health["bybit"] = time.time()

    await em.check_exchange_health("bybit")  # failure 1
    await em.check_exchange_health("bybit")  # failure 2
    assert em._FAIL_COUNTS["bybit"] == 2

    await em.check_exchange_health("bybit")  # success
    assert em._FAIL_COUNTS.get("bybit", 0) == 0


@pytest.mark.asyncio
async def test_rate_limiter_enforces_limit(monkeypatch):
    """Semaphore ensures requests run sequentially when limit is 1."""

    em._REQUEST_LIMIT = 1
    sem = em._rate_limiter("bybit")

    timestamps: list[float] = []

    async def worker():
        async with sem:
            timestamps.append(time.perf_counter())
            await asyncio.sleep(0.05)

    await asyncio.gather(worker(), worker())
    assert timestamps[1] - timestamps[0] >= 0.05


@pytest.mark.asyncio
async def test_parallel_health_checks_switch_once(monkeypatch):
    """Concurrent health probes still trigger a single failover."""

    # bybit fails, binance healthy
    monkeypatch.setattr(ccxt, "bybit", lambda config=None: DummyClient(fail_fetch=True))
    monkeypatch.setattr(ccxt, "binance", lambda config=None: DummyClient())

    await em.add_exchange("bybit", "k", "s")
    await em.add_exchange("binance", "k", "s")

    em._last_health["bybit"] = time.time()
    em._FAIL_SWITCH = 1
    em._REQUEST_LIMIT = 10

    # Launch several health checks simultaneously
    await asyncio.gather(*(em.check_exchange_health("bybit") for _ in range(3)))

    # Failover only happens once and failure count resets
    assert em.get_active_exchange() == "binance"
    assert em._FAIL_COUNTS.get("bybit", 0) == 0


@pytest.mark.asyncio
async def test_rate_limiter_isolation_across_exchanges():
    """Each exchange maintains its own semaphore allowing parallelism."""

    em._REQUEST_LIMIT = 1
    sem_a = em._rate_limiter("bybit")
    sem_b = em._rate_limiter("binance")

    times: list[float] = []

    async def worker(sem: asyncio.Semaphore):
        async with sem:
            times.append(time.perf_counter())
            await asyncio.sleep(0.05)

    await asyncio.gather(worker(sem_a), worker(sem_b))

    # Because semaphores are per exchange, start times should be near-identical
    assert abs(times[1] - times[0]) < 0.05
