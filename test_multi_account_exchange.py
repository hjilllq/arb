import asyncio
import pytest

from exchange_manager import ExchangeManager


class DummyAPI:
    def __init__(self, balance):
        self.balance = balance
        self.orders = []

    async def check_balance(self):
        return {"result": {"list": [{"availableBalance": self.balance}]}}

    async def place_order(self, pair, price, qty, side="Buy", order_type="Limit"):
        self.orders.append({"pair": pair, "price": price, "qty": qty, "side": side})
        return {"qty": qty}

    async def get_open_orders(self, pair=None):
        return []

    async def close(self):
        pass


def test_balanced_order_distribution():
    api1, api2 = DummyAPI(100), DummyAPI(50)
    manager = ExchangeManager(apis=[api1, api2])

    async def runner():
        balance = await manager.check_balance()
        assert balance["USDT"] == 150
        await manager.place_order("BTCUSDT", 100.0, 15.0)
        await manager.close()

    asyncio.run(runner())
    assert pytest.approx(api1.orders[0]["qty"]) == 10
    assert pytest.approx(api2.orders[0]["qty"]) == 5


def test_sync_trading_parameters():
    manager = ExchangeManager(apis=[DummyAPI(10)])
    manager.sync_trading_parameters({"threshold": 0.01})
    assert manager.trading_params["threshold"] == 0.01
    asyncio.run(manager.close())
