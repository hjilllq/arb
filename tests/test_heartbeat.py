import asyncio
import time

import pytest

import heartbeat


@pytest.mark.asyncio
async def test_send_and_check(monkeypatch):
    # avoid touching real logger
    async def noop(msg):
        pass
    monkeypatch.setattr(heartbeat, "log_info", noop)
    await heartbeat.send_heartbeat()
    assert heartbeat.check_heartbeat() is True


def test_check_heartbeat_false(monkeypatch):
    heartbeat._LAST_HEARTBEAT = time.time() - 130
    assert heartbeat.check_heartbeat() is False


@pytest.mark.asyncio
async def test_alert_no_heartbeat(monkeypatch):
    calls = {}

    async def fake_notify(message: str, detail: str = ""):
        calls["msg"] = message

    async def fake_error(message: str, error: Exception):
        calls["err"] = message

    monkeypatch.setattr(heartbeat, "check_heartbeat", lambda: False)
    monkeypatch.setattr(heartbeat, "notify", fake_notify)
    monkeypatch.setattr(heartbeat, "log_error", fake_error)

    await heartbeat.alert_no_heartbeat()
    assert calls.get("msg") == "Heartbeat missing"
    assert calls.get("err") == "Heartbeat missing"
