import asyncio

from strategy import ArbitrageStrategy
from strategy_manager import StrategyManager


class DummyExchange:
    def __init__(self, prices):
        self.prices = prices

    async def get_spot_data(self, pair):  # pragma: no cover - простая заглушка
        return {"price": self.prices.pop(0)}


def test_auto_switching():
    exchange = DummyExchange([100.0, 100.1, 100.2, 110.0])
    manager = StrategyManager(vol_threshold=2.0)
    manager.add_strategy("base", ArbitrageStrategy(basis_threshold=0.1))
    manager.add_strategy("volatile", ArbitrageStrategy(basis_threshold=0.5))

    async def run():
        for _ in range(4):
            await manager.evaluate_market(exchange, "BTCUSDT")
        return manager.active_name

    active = asyncio.run(run())
    assert active == "volatile"


def test_backtest_strategy():
    manager = StrategyManager()
    manager.add_strategy("simple", ArbitrageStrategy(basis_threshold=0.0))
    history = [
        {"spot": 100.0, "future": 101.0},
        {"spot": 102.0, "future": 100.0},
    ]
    result = manager.test_strategy("simple", history)
    assert result["trades"] >= 1
