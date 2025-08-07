import pytest
from unittest.mock import AsyncMock

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bybit_api


class DummyClient:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        self.sandbox = None
        self.load_markets = AsyncMock()

    def set_sandbox_mode(self, mode: bool) -> None:
        self.sandbox = mode


@pytest.mark.asyncio
async def test_connect_api_testnet(monkeypatch):
    dummy = DummyClient()
    monkeypatch.setattr(bybit_api.ccxt, "bybit", lambda config: dummy)
    monkeypatch.setattr(bybit_api.logger, "log_info", AsyncMock())
    await bybit_api.connect_api("k", "s", use_testnet=True)
    assert bybit_api._client is dummy
    assert dummy.sandbox is True
    dummy.load_markets.assert_awaited()


@pytest.mark.asyncio
async def test_connect_api_mainnet(monkeypatch):
    dummy = DummyClient()
    monkeypatch.setattr(bybit_api.ccxt, "bybit", lambda config: dummy)
    monkeypatch.setattr(bybit_api.logger, "log_info", AsyncMock())
    await bybit_api.connect_api("k", "s", use_testnet=False)
    assert dummy.sandbox is False
