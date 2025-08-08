import asyncio
import time

from bybit_api import BybitAPI
from data_analyzer import calculate_macd, calculate_rsi
from database import TradeDatabase


def test_indicator_performance():
    """RSI и MACD должны быстро обрабатываться на больших массивах."""
    data = list(range(10000))
    start = time.perf_counter()
    calculate_rsi(data)
    calculate_macd(data)
    duration = time.perf_counter() - start
    assert duration < 5


def test_database_bulk_insert_performance(tmp_path):
    """Сохранение большого числа записей не должно быть медленным."""
    db_file = tmp_path / "perf.db"

    async def work():
        async with TradeDatabase(db_path=str(db_file)) as db:
            for i in range(1000):
                await db.save_trade_data("BTCUSDT", 100.0 + i, 1.0, "buy")

    start = time.perf_counter()
    asyncio.run(work())
    duration = time.perf_counter() - start
    assert duration < 10


def test_api_cache_reduces_calls(monkeypatch):
    """Повторные запросы в течение TTL должны обращаться к сети один раз."""
    api = BybitAPI(api_key="k", api_secret="s")
    calls = {"n": 0}

    async def fake_get(*args, **kwargs):
        calls["n"] += 1
        class Resp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"price": 1}
        return Resp()

    monkeypatch.setattr(api._client, "get", fake_get)

    async def run():
        await api.get_spot_data("BTCUSDT")
        await api.get_spot_data("BTCUSDT")

    start = time.perf_counter()
    asyncio.run(run())
    duration = time.perf_counter() - start
    asyncio.run(api.close())
    assert calls["n"] == 1
    assert duration < 5


def test_cache_cleanup(monkeypatch):
    """Устаревшие элементы должны удаляться из кэша после TTL."""
    api = BybitAPI(api_key="k", api_secret="s", cache_ttl=0.1)
    calls = {"n": 0}

    async def fake_get(*args, **kwargs):
        calls["n"] += 1
        class Resp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"price": calls["n"]}

        return Resp()

    monkeypatch.setattr(api._client, "get", fake_get)

    async def run() -> int:
        await api.get_spot_data("BTCUSDT")
        await asyncio.sleep(0.15)
        api.cleanup_cache()
        size_after_expire = len(api._cache)
        await api.get_spot_data("BTCUSDT")
        api.cleanup_cache()
        final_size = len(api._cache)
        await api.close()
        assert size_after_expire == 0
        return final_size

    size = asyncio.run(run())
    assert calls["n"] == 2
    assert size == 1
