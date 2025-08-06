import asyncio
from types import SimpleNamespace

import pytest

import monitor


@pytest.mark.asyncio
async def test_check_cpu_usage(monkeypatch):
    monkeypatch.setattr(monitor.psutil, "cpu_percent", lambda interval=None: 42.0)
    assert await monitor.check_cpu_usage() == 42.0


@pytest.mark.asyncio
async def test_run_checks_alerts(monkeypatch):
    calls = []

    async def fake_alert(metric, value):
        calls.append((metric, value))

    monkeypatch.setattr(monitor, "alert_high_usage", fake_alert)
    monkeypatch.setattr(monitor.psutil, "cpu_percent", lambda interval=None: 85.0)
    monkeypatch.setattr(
        monitor.psutil, "virtual_memory", lambda: SimpleNamespace(percent=85.0)
    )
    monkeypatch.setattr(
        monitor.psutil, "disk_usage", lambda path="./": SimpleNamespace(percent=85.0)
    )
    monkeypatch.setattr(monitor, "_ALERT_THRESHOLD", 80.0)
    await monitor._run_checks()
    assert {c[0] for c in calls} == {"cpu", "memory", "disk"}


@pytest.mark.asyncio
async def test_check_cpu_usage_handles_error(monkeypatch):
    def raiser(_interval=None):
        raise RuntimeError("no cpu")

    monkeypatch.setattr(monitor.psutil, "cpu_percent", raiser)
    val = await monitor.check_cpu_usage()
    assert val == -1.0
