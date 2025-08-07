import sys
from pathlib import Path
import asyncio

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import data_analyzer as da
import config


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    messages = {"warning": [], "error": [], "notify": []}

    async def warn(msg):
        messages["warning"].append(msg)

    async def err(msg, exc):
        messages["error"].append((msg, exc))

    async def info(msg):
        pass

    async def notify(msg, detail=""):
        messages["notify"].append((msg, detail))

    monkeypatch.setattr(da, "log_warning", warn)
    monkeypatch.setattr(da, "log_error", err)
    monkeypatch.setattr(da, "log_info", info)
    monkeypatch.setattr(da, "notify", notify)
    monkeypatch.setattr(config, "get_pair_mapping", lambda: {"BTC/USDT": "BTCUSDT"})
    return messages


def test_preprocess_data_removes_anomalies(setup):
    data = [
        {"spot_symbol": "BTC/USDT", "futures_symbol": "BTCUSDT", "spot_price": 100, "futures_price": 101},
        {"spot_symbol": "BTC/USDT", "futures_symbol": "BTCUSDT", "spot_price": 100, "futures_price": 150},
    ]
    df = da.preprocess_data(data)
    asyncio.run(asyncio.sleep(0))
    assert len(df) == 1
    assert setup["warning"]


def test_calculate_indicators_adds_columns():
    df = pd.DataFrame({
        "spot_price": range(1, 60),
        "futures_price": range(2, 61),
    })
    res = da.calculate_indicators(df.copy())
    for col in ["spot_rsi", "spot_macd", "futures_rsi", "futures_macd"]:
        assert col in res.columns
        assert pd.notna(res[col].iloc[-1])


def test_calculate_volatility():
    df = pd.DataFrame({"spot_price": [100, 100], "futures_price": [110, 130]})
    vol = da.calculate_volatility(df)
    assert pytest.approx(0.1, rel=1e-6) == vol
