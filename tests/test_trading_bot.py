import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import trading_bot as tb


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    messages = {"info": [], "error": [], "warning": [], "notify": []}

    async def info(msg):
        messages["info"].append(msg)

    async def error(msg, err):
        messages["error"].append((msg, err))

    async def warn(msg):
        messages["warning"].append(msg)

    async def notify(msg, detail=""):
        messages["notify"].append((msg, detail))

    monkeypatch.setattr(tb, "logger", type("L", (), {
        "log_info": info,
        "log_error": error,
        "log_warning": warn,
    }))
    monkeypatch.setattr(tb, "notify", notify)
    return messages


@pytest.mark.asyncio
async def test_initialize_bot(monkeypatch, setup):
    called = {}

    async def fake_connect(key, secret):
        called["args"] = (key, secret)

    monkeypatch.setattr(tb.bybit_api, "connect_api", fake_connect)
    monkeypatch.setattr(tb.config, "load_config", lambda: {"API_KEY": "k", "API_SECRET": "s"})

    await tb.initialize_bot()
    assert called["args"] == ("k", "s")
    assert setup["info"]


@pytest.mark.asyncio
async def test_trading_iteration_executes_trade(monkeypatch, setup):
    monkeypatch.setattr(tb.config, "get_pair_mapping", lambda: {"BTC/USDT": "BTCUSDT"})

    async def fake_data(spot, fut):
        return {"spot": {"bid": 100}, "futures": {"ask": 101}}

    monkeypatch.setattr(tb.bybit_api, "get_spot_futures_data", fake_data)
    monkeypatch.setattr(tb.strategy_manager, "calculate_basis", lambda *a, **k: 0.01)
    monkeypatch.setattr(tb.strategy_manager, "generate_basis_signal", lambda *a, **k: "buy")
    monkeypatch.setattr(tb.risk_manager, "trading_paused", lambda: False)
    monkeypatch.setattr(tb.bybit_api, "get_balance", lambda: {"USDT": 1000})
    monkeypatch.setattr(tb.risk_manager, "calculate_position_size", lambda b, r: 1)

    async def enforce(pnl, bal):
        return True

    monkeypatch.setattr(tb.risk_manager, "enforce_risk_limits", enforce)

    calls = {}

    async def fake_open(spot, fut, side, amount, sp, fp):
        calls["args"] = (spot, fut, side, amount, sp, fp)
        return {"spot_order_id": "1", "futures_order_id": "2"}

    monkeypatch.setattr(tb.position_manager, "open_position", fake_open)

    res = await tb._trading_iteration(paper=False)
    assert res is True
    assert calls["args"][2] == "buy"


@pytest.mark.asyncio
async def test_trading_iteration_paper(monkeypatch, setup):
    monkeypatch.setattr(tb.config, "get_pair_mapping", lambda: {"BTC/USDT": "BTCUSDT"})

    async def fake_data(spot, fut):
        return {"spot": {"bid": 100}, "futures": {"ask": 101}}

    monkeypatch.setattr(tb.bybit_api, "get_spot_futures_data", fake_data)
    monkeypatch.setattr(tb.strategy_manager, "calculate_basis", lambda *a, **k: 0.01)
    monkeypatch.setattr(tb.strategy_manager, "generate_basis_signal", lambda *a, **k: "buy")
    monkeypatch.setattr(tb.risk_manager, "trading_paused", lambda: False)
    monkeypatch.setattr(tb.bybit_api, "get_balance", lambda: {"USDT": 1000})
    monkeypatch.setattr(tb.risk_manager, "calculate_position_size", lambda b, r: 1)

    async def enforce(pnl, bal):
        return True

    monkeypatch.setattr(tb.risk_manager, "enforce_risk_limits", enforce)

    called = {}

    async def fake_open(*args, **kwargs):
        called["called"] = True
        return {}

    monkeypatch.setattr(tb.position_manager, "open_position", fake_open)

    res = await tb._trading_iteration(paper=True)
    assert res is True
    assert "called" not in called
    assert any("Paper" in msg for msg in setup["info"])


@pytest.mark.asyncio
async def test_run_paper_trading_calls_iteration(monkeypatch):
    called = {}

    async def fake_iter(paper):
        called["paper"] = paper
        return True

    monkeypatch.setattr(tb, "_trading_iteration", fake_iter)
    await tb.run_paper_trading(iterations=1)
    assert called["paper"] is True
