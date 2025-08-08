import risk_manager
from risk_manager import RiskManager
import pytest


class DummyNotifier:
    def __init__(self) -> None:
        self.messages: list[str] = []
        self.email_sender = "test@example.com"

    def send_telegram_notification(self, message: str) -> bool:  # pragma: no cover - простая заглушка
        self.messages.append(message)
        return True

    def send_email_notification(self, subject: str, body: str, recipients):  # pragma: no cover - простая заглушка
        self.messages.append(body)
        return True


def test_calculate_position_size(monkeypatch):
    messages = []

    def fake_log(msg):
        messages.append(msg)

    monkeypatch.setattr(risk_manager, "log_event", fake_log)
    rm = RiskManager(max_position_size=3)
    size = rm.calculate_position_size(1000, 0.01, 5)
    assert size == 2.0
    assert any("position_size" in m for m in messages)


def test_monitor_and_limits():
    rm = RiskManager(
        max_position_size=1,
        volatility_threshold=1,
        volatility_window=2,
        max_open_positions=1,
    )
    rm.monitor_volatility(100)
    rm.monitor_volatility(110)
    assert rm.trading_paused
    rm.trading_paused = False
    assert not rm.check_risk_limits(0.5, open_positions=2)
    assert rm.trading_paused


def test_total_loss_triggers_pause_and_notification():
    notifier = DummyNotifier()
    rm = RiskManager(max_total_loss=5, notifier=notifier)
    rm.record_trade(-3)
    assert not rm.trading_paused
    rm.record_trade(-3)
    assert rm.trading_paused
    assert notifier.messages


def test_pnl_threshold_notifications():
    notifier = DummyNotifier()
    rm = RiskManager(profit_alert=5, loss_alert=5, notifier=notifier)
    rm.record_trade(3)
    assert not notifier.messages
    rm.record_trade(3)
    assert "Profit target" in notifier.messages[-1]
    rm.record_trade(-12)
    assert any("Loss limit" in m for m in notifier.messages)


def test_adaptive_position_size():
    rm = RiskManager(
        max_position_size=5,
        max_total_loss=1000,
        volatility_threshold=1,
        volatility_window=2,
    )
    rm.set_strategy_multiplier("aggressive", 0.8)
    rm.record_trade(-5)
    rm.record_trade(-5)
    # высокая волатильность уменьшает размер ещё вдвое
    adjusted = rm.adjust_position_size(1.0, "aggressive", volatility=2)
    assert adjusted == pytest.approx(0.5 * 0.5 * 0.8)


def test_safety_factor_adjustment():
    rm = RiskManager(max_position_size=10)
    rm.set_safety_factor(0.5, "health degraded")
    assert rm.calculate_position_size(1000, 0.01, 10) == 0.5
    rm.reset_safety_factor()
    assert rm.calculate_position_size(1000, 0.01, 10) == 1.0


def test_daily_loss_monitoring_triggers_actions():
    notifier = DummyNotifier()
    rm = RiskManager(
        daily_loss_soft_pct=2,
        daily_loss_pct=5,
        notifier=notifier,
    )
    rm.update_balance(1000)
    rm.update_balance(980)
    assert rm.safety_factor == 0.5
    assert notifier.messages
    rm.update_balance(940)
    assert rm.trading_paused
    assert any("daily loss" in m for m in notifier.messages)
