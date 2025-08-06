import asyncio
import importlib
import sys
import types

import pytest

# Dummy implementations to capture network interactions
class DummyResp:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self):
        return "ok"


class DummySession:
    def __init__(self):
        self.posts = []

    def post(self, url, json=None):
        self.posts.append((url, json))
        return DummyResp()

    async def close(self):
        return

_sent_emails = []

class DummySMTP:
    def __init__(self, host, port, timeout=None):
        self.sent = []
        _sent_emails.append(self)

    def login(self, user, password):
        return

    def send_message(self, msg):
        self.sent.append(msg)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _import_manager(monkeypatch):
    cfg = {
        "TELEGRAM_TOKEN": "token",
        "TELEGRAM_CHAT_ID": "chat",
        "EMAIL_HOST": "smtp",
        "EMAIL_PORT": "25",
        "EMAIL_USER": "user",
        "EMAIL_PASS": "pass",
        "EMAIL_FROM": "a@b",
        "EMAIL_TO": "c@d",
    }
    config = types.SimpleNamespace(load_config=lambda: cfg)

    class FakeLogger:
        def info(self, *a, **k):
            pass
        def error(self, *a, **k):
            pass
        def warning(self, *a, **k):
            pass

    logger = types.SimpleNamespace(get_logger=lambda name: FakeLogger())

    monkeypatch.setitem(sys.modules, "config", config)
    monkeypatch.setitem(sys.modules, "logger", logger)
    monkeypatch.setattr("aiohttp.ClientSession", lambda timeout=None: DummySession())
    monkeypatch.setattr("smtplib.SMTP", DummySMTP)
    sys.modules.pop("notification_manager", None)
    return importlib.import_module("notification_manager")


@pytest.mark.asyncio
async def test_send_trade_alert(monkeypatch):
    nm = _import_manager(monkeypatch)
    await nm.setup_notifications()
    await nm.send_trade_alert({"spot_symbol": "BTC/USDT", "futures_symbol": "BTCUSDT", "pnl": 10})
    assert nm._session.posts  # Telegram called
    assert _sent_emails and _sent_emails[0].sent  # Email sent


@pytest.mark.asyncio
async def test_send_error_alert(monkeypatch):
    nm = _import_manager(monkeypatch)
    await nm.setup_notifications()
    await nm.send_error_alert(RuntimeError("boom"))
    assert "Error" in nm._session.posts[0][1]["text"]
