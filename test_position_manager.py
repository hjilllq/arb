import position_manager
from position_manager import PositionManager


def test_open_and_check_positions(monkeypatch):
    logged = {}

    def fake_log(data):
        logged.update(data)

    monkeypatch.setattr(position_manager, "log_trade_data", fake_log)
    pm = PositionManager()
    pm.open_position("BTCUSDT", 1.0, 10000.0, "long")
    positions = pm.check_open_positions()
    assert "BTCUSDT" in positions
    assert positions["BTCUSDT"].qty == 1.0
    assert logged["action"] == "open"


def test_update_and_close_position():
    pm = PositionManager()
    pm.open_position("ETHUSDT", 2.0, 100.0, "short")
    pm.update_position("ETHUSDT", qty=3.0, price=95.0)
    pos = pm.check_open_positions()["ETHUSDT"]
    assert pos.qty == 3.0
    assert pos.entry_price == 95.0
    pnl = pm.close_position("ETHUSDT", 90.0)
    assert pnl == 15.0  # (95-90)*3 для короткой позиции
    assert "ETHUSDT" not in pm.check_open_positions()
