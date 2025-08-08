import asyncio
import json

from exchange_manager import ExchangeManager


class DummyAPI:
    def __init__(self):
        self.spot_calls = 0
        self.fut_calls = 0

    async def get_spot_data(self, pair):
        self.spot_calls += 1
        return {"symbol": pair, "price": self.spot_calls}

    async def get_futures_data(self, pair):
        self.fut_calls += 1
        return {"symbol": pair, "price": self.fut_calls}

    async def get_open_orders(self, pair=None):
        return {"list": [{"id": 123}]}

    async def close(self):
        pass


def test_market_data_refresh():
    api = DummyAPI()
    manager = ExchangeManager(api=api)

    async def runner():
        data = await manager.get_market_data("BTCUSDT", "BTCUSDT")
        assert data["spot"]["price"] == 1
        await manager.get_market_data("BTCUSDT", "BTCUSDT")
        assert api.spot_calls == 1
        await manager.get_market_data("BTCUSDT", "BTCUSDT", max_age=0)
        assert api.spot_calls == 2
        await manager.close()

    asyncio.run(runner())


def test_backup_open_orders(tmp_path):
    api = DummyAPI()
    manager = ExchangeManager(api=api)

    async def runner():
        backup_file = tmp_path / "orders.json"
        await manager.backup_open_orders(str(backup_file))
        data = json.loads(backup_file.read_text())
        assert data["list"][0]["id"] == 123
        await manager.close()

    asyncio.run(runner())


def test_market_watcher_updates():
    api = DummyAPI()
    manager = ExchangeManager(api=api)

    async def runner():
        await manager.start_market_watcher("BTCUSDT", "BTCUSDT", interval=0.01)
        await asyncio.sleep(0.05)
        await manager.stop_market_watcher()
        assert api.spot_calls > 0 and api.fut_calls > 0
        await manager.close()

    asyncio.run(runner())
