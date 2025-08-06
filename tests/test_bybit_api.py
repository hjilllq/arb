import asyncio
from types import SimpleNamespace
import pytest
import sys, pathlib, os, time, json

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
    bybit_api._TICKER_CACHE.clear()
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
    bybit_api._TICKER_CACHE.clear()
    monkeypatch.setattr(bybit_api.database, "save_data", lambda *a, **k: asyncio.sleep(0))
    attempts: list[int] = []

    async def fake_handle(exc, attempt=1, context=""):
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
    bybit_api._TICKER_CACHE.clear()
    attempts: list[int] = []

    async def fake_handle(exc, attempt=1, context=""):
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


@pytest.mark.asyncio
async def test_historical_data_cache(monkeypatch, tmp_path):
    """Repeated historical queries should hit the API once due to caching."""

    tmp_path.mkdir(exist_ok=True)
    monkeypatch.setattr(bybit_api, "_CACHE_DIR", tmp_path)

    class DummyClient:
        def __init__(self):
            self.calls = 0

        async def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None, params=None):
            self.calls += 1
            if self.calls == 1:
                return [[since, 1, 1, 1, 1, 1]]
            return []

    bybit_api._client = DummyClient()
    data1 = await bybit_api.get_historical_data(
        "BTC/USDT", "1m", "2024-01-01", "2024-01-02"
    )
    first_calls = bybit_api._client.calls
    data2 = await bybit_api.get_historical_data(
        "BTC/USDT", "1m", "2024-01-01", "2024-01-02"
    )
    assert bybit_api._client.calls == first_calls
    assert data1 == data2


@pytest.mark.asyncio
async def test_historical_cache_expiry(monkeypatch, tmp_path):
    monkeypatch.setattr(bybit_api, "_CACHE_DIR", tmp_path)
    monkeypatch.setattr(bybit_api.config, "get_cache_ttl", lambda: 1)

    cache_file = tmp_path / "ohlcv_BTCUSDT_1m_2024-01-01_2024-01-02.json"
    cache_file.write_text("[]")
    os.utime(cache_file, (time.time() - 10, time.time() - 10))

    class DummyClient:
        def __init__(self):
            self.calls = 0

        async def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None, params=None):
            self.calls += 1
            return [[since, 1, 1, 1, 1, 1]] if self.calls == 1 else []

    bybit_api._client = DummyClient()
    await bybit_api.get_historical_data("BTC/USDT", "1m", "2024-01-01", "2024-01-02")
    first = bybit_api._client.calls
    # second call should hit fresh cache without increasing calls
    await bybit_api.get_historical_data("BTC/USDT", "1m", "2024-01-01", "2024-01-02")
    assert bybit_api._client.calls == first


@pytest.mark.asyncio
async def test_historical_cache_corrupted(monkeypatch, tmp_path):
    monkeypatch.setattr(bybit_api, "_CACHE_DIR", tmp_path)
    monkeypatch.setattr(bybit_api.config, "get_cache_ttl", lambda: 3600)
    monkeypatch.setattr(bybit_api.logger, "log_warning", lambda *a, **k: asyncio.sleep(0))

    cache_file = tmp_path / "ohlcv_BTCUSDT_1m_2024-01-01_2024-01-02.json"
    cache_file.write_text("{bad json")

    class DummyClient:
        def __init__(self):
            self.calls = 0

        async def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None, params=None):
            self.calls += 1
            return [[since, 1, 1, 1, 1, 1]] if self.calls == 1 else []

    bybit_api._client = DummyClient()
    await bybit_api.get_historical_data("BTC/USDT", "1m", "2024-01-01", "2024-01-02")
    first = bybit_api._client.calls
    await bybit_api.get_historical_data("BTC/USDT", "1m", "2024-01-01", "2024-01-02")
    assert bybit_api._client.calls == first
    # cache file replaced with valid JSON
    parsed = json.loads(cache_file.read_text())
    assert parsed[0]["open"] == 1


@pytest.mark.asyncio
async def test_bulk_tickers(monkeypatch):
    class DummyClient:
        def __init__(self):
            self.called = []

        async def fetch_ticker(self, symbol, params=None):
            self.called.append(symbol)
            return {"bid": 1, "ask": 2}

    bybit_api._client = DummyClient()
    logs: list[str] = []

    async def fake_log(msg):
        logs.append(msg)

    monkeypatch.setattr(bybit_api.logger, "log_info", fake_log)
    res = await bybit_api.get_multiple_tickers(["BTC/USDT", "ETH/USDT"])
    assert set(bybit_api._client.called) == {"BTC/USDT", "ETH/USDT"}
    assert res["ETH/USDT"]["ask"] == 2
    assert any("Fetched 2 tickers" in m for m in logs)


@pytest.mark.asyncio
async def test_enforce_cache_limit(monkeypatch, tmp_path):
    monkeypatch.setattr(bybit_api, "_CACHE_DIR", tmp_path)
    f1 = tmp_path / "a.json"
    f1.write_text("x" * 40)
    time.sleep(0.01)
    f2 = tmp_path / "b.json"
    f2.write_text("x" * 40)
    monkeypatch.setattr(bybit_api.config, "get_cache_max_bytes", lambda: 50)
    logs: list[str] = []

    async def fake_log(msg):
        logs.append(msg)

    monkeypatch.setattr(bybit_api.logger, "log_info", fake_log)
    await bybit_api._enforce_cache_limit()
    assert len(list(tmp_path.iterdir())) == 1
    assert any("Cache trimmed" in m for m in logs)


@pytest.mark.asyncio
async def test_ticker_cache_respects_ttl(monkeypatch):
    """Ticker cache should reuse data until the TTL expires."""

    class DummyClient:
        def __init__(self):
            self.calls = 0

        async def fetch_ticker(self, symbol, params=None):
            self.calls += 1
            # Return distinct values per call so cache hits are evident
            return {"bid": self.calls, "ask": self.calls}

    bybit_api._client = DummyClient()
    bybit_api._TICKER_CACHE.clear()
    monkeypatch.setattr(bybit_api.database, "save_data", lambda *a, **k: asyncio.sleep(0))
    monkeypatch.setattr(bybit_api.config, "get_ticker_cache_ttl", lambda: 1)

    t = 0.0

    def fake_monotonic():
        return t

    monkeypatch.setattr(bybit_api.time, "monotonic", fake_monotonic)

    await bybit_api.get_spot_futures_data("BTC/USDT", "BTCUSDT")
    assert bybit_api._client.calls == 2

    t = 0.5  # within TTL, should hit cache
    await bybit_api.get_spot_futures_data("BTC/USDT", "BTCUSDT")
    assert bybit_api._client.calls == 2

    t = 1.5  # TTL expired, should refetch
    await bybit_api.get_spot_futures_data("BTC/USDT", "BTCUSDT")
    assert bybit_api._client.calls == 4


@pytest.mark.asyncio
async def test_ticker_cache_disabled_with_zero_ttl(monkeypatch):
    """A zero TTL disables ticker caching entirely."""

    class DummyClient:
        def __init__(self):
            self.calls = 0

        async def fetch_ticker(self, symbol, params=None):
            self.calls += 1
            return {"bid": self.calls, "ask": self.calls}

    bybit_api._client = DummyClient()
    bybit_api._TICKER_CACHE.clear()
    monkeypatch.setattr(bybit_api.database, "save_data", lambda *a, **k: asyncio.sleep(0))
    monkeypatch.setattr(bybit_api.config, "get_ticker_cache_ttl", lambda: 0)
    monkeypatch.setattr(bybit_api.time, "monotonic", lambda: 0)

    await bybit_api.get_spot_futures_data("BTC/USDT", "BTCUSDT")
    await bybit_api.get_spot_futures_data("BTC/USDT", "BTCUSDT")
    # Each call fetches both spot and futures tickers (2 calls per request)
    assert bybit_api._client.calls == 4
