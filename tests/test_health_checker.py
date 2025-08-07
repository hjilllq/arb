import asyncio
from types import SimpleNamespace
from pathlib import Path

import aiohttp
import pytest

import health_checker


@pytest.mark.asyncio
async def test_check_api_health(monkeypatch):
    class DummyResp:
        status = 200
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
    class DummySession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        def get(self, url):
            return DummyResp()
    monkeypatch.setattr(health_checker.aiohttp, "ClientSession", lambda *a, **k: DummySession())
    assert await health_checker.check_api_health() is True


@pytest.mark.asyncio
async def test_check_websocket_health(monkeypatch):
    class DummyWS:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        async def ping(self):
            return
        async def receive(self, timeout=None):
            class Msg:
                type = aiohttp.WSMsgType.PONG
            return Msg()
    class DummySession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        def ws_connect(self, url, **kwargs):
            return DummyWS()
    monkeypatch.setattr(health_checker.aiohttp, "ClientSession", lambda *a, **k: DummySession())
    assert await health_checker.check_websocket_health() is True


def test_check_gui_health(tmp_path, monkeypatch):
    sentinel = tmp_path / "gui.lock"
    monkeypatch.setattr(health_checker, "_GUI_SENTINEL", sentinel)
    assert health_checker.check_gui_health() is False
    sentinel.write_text("ok")
    assert health_checker.check_gui_health() is True


def test_check_model_health(monkeypatch, tmp_path):
    dummy = tmp_path / "model.keras"
    ns = SimpleNamespace(_MODEL_PATH=dummy)
    monkeypatch.setattr(health_checker, "ml_predictor", ns)
    assert health_checker.check_model_health() is False
    dummy.write_text("x")
    assert health_checker.check_model_health() is True


def test_check_db_health(monkeypatch):
    async def fake_connect():
        return None
    async def fake_close():
        return None
    monkeypatch.setattr(health_checker.database, "connect_db", fake_connect)
    monkeypatch.setattr(health_checker.database, "close_db", fake_close)
    assert health_checker.check_db_health() is True


@pytest.mark.asyncio
async def test_generate_health_report(monkeypatch):
    async def ok_async():
        return True
    async def bad_async():
        return False
    monkeypatch.setattr(health_checker, "check_api_health", ok_async)
    monkeypatch.setattr(health_checker, "check_internet_health", ok_async)
    monkeypatch.setattr(health_checker, "check_websocket_health", bad_async)
    monkeypatch.setattr(health_checker, "check_db_health", lambda: False)
    monkeypatch.setattr(health_checker, "check_gui_health", lambda: True)
    monkeypatch.setattr(health_checker, "check_model_health", lambda: True)
    report = await health_checker.generate_health_report()
    assert report == {
        "api": True,
        "internet": True,
        "websocket": False,
        "db": False,
        "gui": True,
        "model": True,
    }
