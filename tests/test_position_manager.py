import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import position_manager as pm


class DummyAPI:
    def __init__(self):
        self.orders = []
        self.ticker = {
            "spot": {"bid": 105.0, "ask": 106.0},
            "futures": {"bid": 108.0, "ask": 109.0},
        }

    async def place_order(self, symbol, side, amount, price, order_type="limit", contract_type="spot"):
        oid = f"{symbol}-{side}-{len(self.orders)}"
        self.orders.append((symbol, side, amount, price, order_type, contract_type))
        return {"order_id": oid}

    async def cancel_order(self, symbol, order_id):
        return True

    async def get_spot_futures_data(self, spot_symbol, futures_symbol):
        return self.ticker


async def _noop(*args, **kwargs):
    return None


@pytest.fixture(autouse=True)
def reset(monkeypatch):
    pm._OPEN_POSITIONS.clear()
    pm._CLOSED_POSITIONS.clear()
    api = DummyAPI()
    monkeypatch.setattr(pm, "bybit_api", api)
    monkeypatch.setattr(pm, "database", type("D", (), {"save_data": _noop}))
    monkeypatch.setattr(
        pm,
        "logger",
        type(
            "L",
            (),
            {
                "log_error": _noop,
                "log_warning": _noop,
                "log_trade": _noop,
            },
        ),
    )
    yield api


@pytest.mark.asyncio
async def test_open_and_get_positions(reset):
    res = await pm.open_position("BTC/USDT", "BTCUSDT", "buy", 1, 100.0, 110.0)
    assert "spot_order_id" in res and "futures_order_id" in res
    positions = await pm.get_open_positions()
    assert len(positions) == 1


@pytest.mark.asyncio
async def test_close_and_pnl(reset):
    res = await pm.open_position("BTC/USDT", "BTCUSDT", "buy", 1, 100.0, 110.0)
    ok = await pm.close_position(res["spot_order_id"], res["futures_order_id"])
    assert ok
    pnl = pm.calculate_pnl(res["spot_order_id"], res["futures_order_id"])
    expected = (105.0 - 100.0) - (109.0 - 110.0)
    fees = (100 + 110 + 105 + 109) * 0.00075
    assert pytest.approx(expected - fees, rel=1e-6) == pnl
