import asyncio
from risk_monitor import RiskMonitor
from risk_manager import RiskManager


class DummyExchange:
    def __init__(self, balance: float, price: float) -> None:
        self.balance = balance
        self.price = price

    async def check_balance(self, exchange: str = "bybit") -> dict[str, float]:
        return {"USDT": self.balance}

    async def get_spot_data(self, pair: str, exchange: str = "bybit") -> dict[str, float]:
        return {"price": self.price}


class DummyNotifier:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def notify_critical(self, message: str) -> None:  # pragma: no cover - заглушка
        self.messages.append(message)


def test_monitor_stops_on_hard_loss() -> None:
    notifier = DummyNotifier()
    rm = RiskManager(daily_loss_pct=10, notifier=notifier)
    rm.update_balance(1000)
    monitor = RiskMonitor(DummyExchange(800, 100), rm, pair="BTCUSDT")
    asyncio.run(monitor.check_once())
    assert rm.trading_paused
    assert any("daily loss" in m.lower() for m in notifier.messages)


def test_monitor_reduces_volume_on_soft_loss() -> None:
    notifier = DummyNotifier()
    rm = RiskManager(daily_loss_soft_pct=10, daily_loss_pct=20, notifier=notifier)
    rm.update_balance(1000)
    monitor = RiskMonitor(DummyExchange(850, 100), rm, pair="BTCUSDT")
    asyncio.run(monitor.check_once())
    assert rm.safety_factor == 0.5
    assert not rm.trading_paused
    assert notifier.messages
