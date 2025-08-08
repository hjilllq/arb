import strategy
import asyncio
from strategy import ArbitrageStrategy


class DummyExchange:
    def __init__(self, spot: float = 100.0, futures: float = 105.0):
        self.spot_price = spot
        self.futures_price = futures

    async def get_spot_data(self, pair: str):
        return {"price": self.spot_price}

    async def get_futures_data(self, pair: str):
        return {"price": self.futures_price}


def test_basis_and_signal():
    strat = ArbitrageStrategy(basis_threshold=1)
    basis = strat.calculate_basis(100, 105)
    assert basis == 5
    assert strat.generate_trade_signal(basis) == -1
    assert strat.generate_trade_signal(-basis) == 1


def test_apply_and_evaluate(monkeypatch):
    messages = []

    def fake_log(msg):
        messages.append(msg)

    monkeypatch.setattr(strategy, "log_event", fake_log)
    exch = DummyExchange()
    strat = ArbitrageStrategy(basis_threshold=1)
    trade = asyncio.run(strat.apply_strategy(exch, "BTCUSDT", "BTCUSDT"))
    assert trade["signal"] == -1
    summary = strat.evaluate_strategy([1.0, -0.5])
    assert summary["trades"] == 2
    assert abs(summary["total_pnl"] - 0.5) < 1e-9
    assert any("STRATEGY" in m for m in messages)
