import asyncio
from types import SimpleNamespace
import pytest
import sys, pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
import bybit_api

class DummyClient:
    def __init__(self):
        self.load_markets_called = False
        self.tickers = {
            "BTC/USDT": {"bid": 1, "ask": 2},
            "BTCUSDT": {"bid": 3, "ask": 4},
        }
        self.created = {}
        self.cancel_symbol = None

    async def load_markets(self):
        self.load_markets_called = True

    async def fetch_ticker(self, symbol, params=None):
        return self.tickers[symbol]

    async def fetch_balance(self):
        return {"total": {"USDT": 1000}}

    async def create_order(self, symbol, order_type, side, amount, price, params=None):
        self.created = {"symbol": symbol, "price": price}
        return {"id": "1"}

    async def cancel_order(self, order_id, symbol, params=None):
        self.cancel_symbol = symbol
        return {"id": order_id}

@pytest.mark.asyncio
async def test_connect_api(monkeypatch):
    dummy = DummyClient()
    captured = {}

    def fake_bybit(cfg):
        captured.update(cfg)
        return dummy

    monkeypatch.setattr(bybit_api.ccxt, "bybit", fake_bybit)
    calls = {"spot": False, "fut": False, "map": False}
    monkeypatch.setattr(bybit_api.config, "get_spot_pairs", lambda: calls.__setitem__("spot", True) or [])
    monkeypatch.setattr(bybit_api.config, "get_futures_pairs", lambda: calls.__setitem__("fut", True) or [])
    monkeypatch.setattr(bybit_api.config, "get_pair_mapping", lambda: calls.__setitem__("map", True) or {})
    logs = []
    async def fake_log(msg):
        logs.append(msg)
    monkeypatch.setattr(bybit_api.logger, "log_info", fake_log)
    await bybit_api.connect_api("k", "s", timeout_ms=1234, proxy="http://p")
    assert dummy.load_markets_called
    assert all(calls.values())
    assert "Connected" in logs[0]
    assert captured["timeout"] == 1234
    assert captured["proxies"]["http"] == "http://p"

@pytest.mark.asyncio
async def test_get_spot_futures_data(monkeypatch):
    bybit_api._client = DummyClient()
    captured = {}
    async def fake_save(data):
        captured["rows"] = data
    monkeypatch.setattr(bybit_api.database, "save_data", fake_save)
    res = await bybit_api.get_spot_futures_data("BTC/USDT", "BTC-USDT")
    assert res["spot"]["bid"] == 1
    assert captured["rows"][0]["spot_symbol"] == "BTC/USDT"

@pytest.mark.asyncio
async def test_subscribe_to_websocket(monkeypatch):
    messages = []
    async def fake_log(msg):
        messages.append(msg)
    monkeypatch.setattr(bybit_api.logger, "log_info", fake_log)

    class DummyWS:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        async def send_str(self, msg):
            pass
        async def receive(self, timeout=None):
            return SimpleNamespace(type=bybit_api.aiohttp.WSMsgType.TEXT, data="hello")

    class DummySession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def ws_connect(self, url, heartbeat=20):
            return DummyWS()

    monkeypatch.setattr(bybit_api.aiohttp, "ClientSession", lambda: DummySession())
    async def fast_sleep(_):
        pass
    monkeypatch.setattr(bybit_api.asyncio, "sleep", fast_sleep)
    await bybit_api.subscribe_to_websocket("BTCUSDT", "spot", max_messages=1)
    assert any("WS message" in m for m in messages)


@pytest.mark.asyncio
async def test_retry_logic(monkeypatch):
    """Ensure retries happen and handle_api_error is invoked."""
    class DummyClient:
        def __init__(self):
            self.calls = 0
            self.tickers = {
                "BTC/USDT": {"bid": 1, "ask": 2},
                "BTCUSDT": {"bid": 3, "ask": 4},
            }

        async def fetch_ticker(self, symbol, params=None):
            self.calls += 1
            if self.calls == 1:
                raise bybit_api.ccxt.RequestTimeout("boom")
            return self.tickers[symbol]

    bybit_api._client = DummyClient()
    monkeypatch.setattr(bybit_api.database, "save_data", lambda *a, **k: asyncio.sleep(0))
    attempts: list[int] = []

    async def fake_handle(exc, attempt=1):
        attempts.append(attempt)

    monkeypatch.setattr(bybit_api, "handle_api_error", fake_handle)
    res = await bybit_api.get_spot_futures_data("BTC/USDT", "BTCUSDT")
    assert res["spot"]["bid"] == 1
    assert attempts == [1]


@pytest.mark.asyncio
async def test_retry_failure(monkeypatch):
    """If all retries fail an empty dict is returned and handler called thrice."""
    class DummyClient:
        async def fetch_ticker(self, symbol, params=None):
            raise bybit_api.ccxt.NetworkError("down")

    bybit_api._client = DummyClient()
    attempts: list[int] = []

    async def fake_handle(exc, attempt=1):
        attempts.append(attempt)

    monkeypatch.setattr(bybit_api, "handle_api_error", fake_handle)
    res = await bybit_api.get_spot_futures_data("BTC/USDT", "BTCUSDT")
    assert res == {}
    assert attempts == [1, 2, 3]


@pytest.mark.asyncio
async def test_place_order_market_sets_price_none(monkeypatch):
    client = DummyClient()
    bybit_api._client = client
    monkeypatch.setattr(bybit_api.logger, "log_info", lambda *a, **k: asyncio.sleep(0))
    res = await bybit_api.place_order("BTCUSDT", "buy", 1, 100.0, order_type="market")
    assert res["order_id"] == "1"
    assert client.created["price"] is None


@pytest.mark.asyncio
async def test_cancel_order_includes_symbol(monkeypatch):
    client = DummyClient()
    bybit_api._client = client
    monkeypatch.setattr(bybit_api.logger, "log_info", lambda *a, **k: asyncio.sleep(0))
    ok = await bybit_api.cancel_order("BTC-USDT", "42", "future")
    assert ok
    assert client.cancel_symbol == "BTCUSDT"


@pytest.mark.asyncio
async def test_place_order_validates_params(monkeypatch):
    client = DummyClient()
    bybit_api._client = client
    errors = []

    async def fake_err(msg, exc):
        errors.append(msg)

    monkeypatch.setattr(bybit_api.logger, "log_error", fake_err)
    res = await bybit_api.place_order("BTCUSDT", "hold", 1, 1)
    assert res == {}
    assert any("Invalid order side" in m for m in errors)

    errors.clear()
    res = await bybit_api.place_order("BTCUSDT", "buy", 1, 1, order_type="stop")
    assert res == {}
    assert any("Invalid order type" in m for m in errors)
