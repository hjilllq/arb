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
    em._EXCHANGES.clear()
    em._active_exchange = None
    em._last_health.clear()
    em._MARKET_CACHE.clear()
    em._PAIR_CACHE.clear()
    em._PAIR_TTLS.clear()
    em._RATE_LIMITERS.clear()
    em._RETRY_DELAY = 0
    em._DEFAULT_RETRIES = 1
    yield
    em._EXCHANGES.clear()


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
