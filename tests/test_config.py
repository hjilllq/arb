import asyncio
import sys
from pathlib import Path

import pytest

# Ensure the repository root is on the import path so ``config`` can be
# imported without installation.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config


def test_notify_sync_without_loop(monkeypatch):
    calls = []

    async def fake_notify(msg, detail=""):
        calls.append((msg, detail))

    monkeypatch.setattr(config, "notify", fake_notify)

    config.notify_sync("hello", "world")

    assert calls == [("hello", "world")]


@pytest.mark.asyncio
async def test_notify_sync_with_running_loop(monkeypatch):
    calls = []

    async def fake_notify(msg, detail=""):
        calls.append((msg, detail))

    monkeypatch.setattr(config, "notify", fake_notify)

    config.notify_sync("hi", "there")

    # Let the event loop process the scheduled task.
    await asyncio.sleep(0)

    assert calls == [("hi", "there")]


def test_is_testnet_false(monkeypatch):
    monkeypatch.setattr(config, "CONFIG", {})
    assert config.is_testnet() is False


def test_is_testnet_true(monkeypatch):
    monkeypatch.setattr(config, "CONFIG", {"BYBIT_TESTNET": "true"})
    assert config.is_testnet() is True

