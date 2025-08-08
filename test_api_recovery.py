import asyncio
import pytest
import httpx

from bybit_api import BybitAPI


class DummyNotifier:
    def __init__(self) -> None:
        self.messages = []

    def send_telegram_notification(self, message: str) -> None:  # pragma: no cover - simple sink
        self.messages.append(message)


def test_client_reset_and_notification(monkeypatch):
    notifier = DummyNotifier()
    api = BybitAPI(api_key="k", api_secret="s", notifier=notifier)
    old_client = api._client

    async def failing_get(url, params=None):  # pragma: no cover - helper
        raise httpx.ConnectError("boom", request=httpx.Request("GET", "https://test"))

    monkeypatch.setattr(api._client, "get", failing_get)

    slept = []

    async def fake_sleep(seconds):  # pragma: no cover - helper
        slept.append(seconds)

    monkeypatch.setattr("bybit_api.asyncio.sleep", fake_sleep)

    async def runner():
        with pytest.raises(httpx.ConnectError):
            await api.get_spot_data("BTCUSDT")

    asyncio.run(runner())

    assert notifier.messages  # уведомление отправлено
    assert api._client is not old_client  # клиент пересоздан
    assert slept == [1, 2]
    asyncio.run(api.close())


def test_rate_limit_retry(monkeypatch):
    api = BybitAPI()

    calls = 0

    async def rate_limited_get(url, params=None):  # pragma: no cover - helper
        nonlocal calls
        calls += 1
        request = httpx.Request("GET", "https://test")
        if calls == 1:
            response = httpx.Response(429, headers={"Retry-After": "5"}, request=request)
            raise httpx.HTTPStatusError("rate", request=request, response=response)
        return httpx.Response(200, json={"result": "ok"}, request=request)

    monkeypatch.setattr(api._client, "get", rate_limited_get)

    slept = []

    async def fake_sleep(seconds):  # pragma: no cover - helper
        slept.append(seconds)

    monkeypatch.setattr("bybit_api.asyncio.sleep", fake_sleep)

    async def runner():
        return await api.get_spot_data("BTCUSDT")

    data = asyncio.run(runner())

    assert calls == 2  # запрос был повторён
    assert slept == [5]  # ожидание из заголовка Retry-After
    assert data == {"result": "ok"}
    asyncio.run(api.close())
