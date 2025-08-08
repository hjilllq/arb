import asyncio
from typing import Dict

import httpx
import pytest

from data_analyzer import (
    calculate_macd,
    calculate_rsi,
    detect_anomalies,
    filter_data,
    normalize_data,
)
from strategy import ArbitrageStrategy
from bybit_api import BybitAPI
from notification_manager import NotificationManager


# ---------------------------------------------------------------------------
# Общие тесты методов
# ---------------------------------------------------------------------------

def test_methods():
    """Простые проверки функций индикаторов."""
    data = [1, 1, 1, 10]
    assert detect_anomalies(data, z_thresh=2.0) == [3]

    prices = list(range(1, 16))
    rsi = calculate_rsi(prices)
    assert len(rsi) == len(prices)
    macd_line, signal_line, hist = calculate_macd(prices)
    assert len(macd_line) == len(signal_line) == len(hist)

    cleaned = filter_data([10, 10, 1000, 10], z_thresh=2.0, max_change=0.5)
    assert cleaned == [10, 10, 10]

    norm = normalize_data([1, 2, 3])
    assert round(sum(norm), 6) == 0.0


def test_edge_cases():
    """Убедиться, что функции индикаторов обрабатывают пустые или маленькие входные данные."""
    assert detect_anomalies([]) == []
    assert calculate_rsi([1]) == [0.0]
    macd_line, signal_line, hist = calculate_macd([])
    assert macd_line == signal_line == hist == []


# ---------------------------------------------------------------------------
# Взаимодействие с API (программным интерфейсом) и обработка ошибок
# ---------------------------------------------------------------------------

async def _dummy_response(data: Dict[str, int]):
    class Resp:
        def __init__(self, payload: Dict[str, int]):
            self.payload = payload

        def raise_for_status(self) -> None:
            pass

        def json(self) -> Dict[str, int]:
            return self.payload

    return Resp(data)


def test_api_interaction(monkeypatch):
    """Проверить, что BybitAPI может разобрать ответ HTTP-клиента (клиента, работающего по протоколу HTTP)."""
    api = BybitAPI(api_key="k", api_secret="s")

    async def fake_get(*args, **kwargs):
        return await _dummy_response({"price": 1})

    monkeypatch.setattr(api._client, "get", fake_get)
    result = asyncio.run(api.get_spot_data("BTCUSDT"))
    assert result["price"] == 1
    asyncio.run(api.close())


def test_error_handling(monkeypatch):
    """Убедиться, что ошибки API всплывают после исчерпания повторов."""
    api = BybitAPI(api_key="k", api_secret="s")

    async def fake_get(*args, **kwargs):
        raise httpx.HTTPError("boom")

    async def fast_handle_api_error(self, exc, attempt, retries=3):
        if attempt + 1 >= retries:
            raise exc

    monkeypatch.setattr(api._client, "get", fake_get)
    monkeypatch.setattr(BybitAPI, "handle_api_error", fast_handle_api_error)

    with pytest.raises(httpx.HTTPError):
        asyncio.run(api.get_spot_data("BTCUSDT"))
    asyncio.run(api.close())


def test_api_error_notification(monkeypatch):
    """Проверить, что при ошибке API отправляется уведомление."""
    notifier = NotificationManager(telegram_token="t", telegram_chat_id="c")
    messages = {}

    def fake_send(msg: str) -> bool:
        messages["msg"] = msg
        return True

    notifier.send_telegram_notification = fake_send  # type: ignore
    api = BybitAPI(api_key="k", api_secret="s", notifier=notifier)

    async def fake_get(*args, **kwargs):
        raise httpx.HTTPError("boom")

    monkeypatch.setattr(api._client, "get", fake_get)

    with pytest.raises(httpx.HTTPError):
        asyncio.run(api.get_spot_data("BTCUSDT"))
    assert "boom" in messages["msg"]
    asyncio.run(api.close())


# ---------------------------------------------------------------------------
# Тесты стратегии
# ---------------------------------------------------------------------------


def test_strategy():
    """Проверить, что стратегия генерирует сигналы на основе базиса."""
    strat = ArbitrageStrategy(basis_threshold=0.5)

    class DummyExchange:
        async def get_spot_data(self, pair):
            return {"price": 100.0}

        async def get_futures_data(self, pair):
            return {"price": 101.0}

    trade = asyncio.run(strat.apply_strategy(DummyExchange(), "BTCUSDT", "BTCUSDT"))
    assert trade["basis"] == 1.0
    assert trade["signal"] == -1
