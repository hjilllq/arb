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
