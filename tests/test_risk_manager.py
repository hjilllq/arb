import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import risk_manager as rm


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    messages = {"warning": [], "error": [], "notify": []}

    async def warn(msg):
        messages["warning"].append(msg)

    async def error(msg, err):
        messages["error"].append((msg, err))

    async def notify(msg, detail=""):
        messages["notify"].append((msg, detail))

    monkeypatch.setattr(rm, "logger", type("L", (), {"log_warning": warn, "log_error": error}))
    monkeypatch.setattr(rm, "notify", notify)
    monkeypatch.setattr(rm.config, "CONFIG", {})
    rm._TRADING_PAUSED = False
    rm._LAST_PNL_CHECK = None
    rm._LAST_PNL_VALUE = 0.0
    return messages


def test_calculate_position_size():
    assert rm.calculate_position_size(10000, 0.1) == 1000
    assert rm.calculate_position_size(-1, 0.1) == 0


@pytest.mark.asyncio
async def test_check_volatility():
    df = pd.DataFrame({"spot_price": [100, 100], "futures_price": [110, 130]})
    vol = await rm.check_volatility(df)
    assert pytest.approx(0.1, rel=1e-6) == vol


@pytest.mark.asyncio
async def test_pause_trading_on_high_vol(monkeypatch, setup):
    rm.config.CONFIG["MAX_VOLATILITY_PAUSE"] = "0.3"
    ok = await rm.pause_trading_on_high_volatility(0.4)
    assert ok and rm.trading_paused()
    assert setup["warning"]


@pytest.mark.asyncio
async def test_enforce_risk_limits(monkeypatch, setup):
    rm.config.CONFIG["MAX_DAILY_LOSS"] = "0.05"
    ok = await rm.enforce_risk_limits(-600, 10000)
    assert not ok
    assert setup["error"]
    # within limit
    rm._LAST_PNL_CHECK = None
    ok2 = await rm.enforce_risk_limits(-100, 10000)
    assert ok2
