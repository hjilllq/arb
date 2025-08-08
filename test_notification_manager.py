import notification_manager
from notification_manager import NotificationManager
import httpx
import logging


class DummyResponse:
    def __init__(self, status_code: int = 200):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("error")


def test_slack_notification(monkeypatch):
    sent = {}

    def fake_post(url, json=None, timeout=10):
        sent["url"] = url
        sent["json"] = json
        return DummyResponse()

    monkeypatch.setattr(notification_manager.httpx, "post", fake_post)
    nm = NotificationManager(slack_webhook_url="http://example.com")
    assert nm.send_slack_notification("hi")
    assert sent["json"] == {"text": "hi"}


def test_retry(monkeypatch):
    calls = {"count": 0}

    def flaky_post(url, json=None, timeout=10):
        calls["count"] += 1
        if calls["count"] == 1:
            raise httpx.HTTPError("boom")
        return DummyResponse()

    monkeypatch.setattr(notification_manager.httpx, "post", flaky_post)
    monkeypatch.setattr(notification_manager.time, "sleep", lambda x: None)
    nm = NotificationManager(slack_webhook_url="http://example.com")
    assert nm.send_slack_notification("retries")
    assert calls["count"] == 2


def test_email_notification(monkeypatch):
    messages = {}

    class DummySMTP:
        def __init__(self, host, port):
            self.host = host
            self.port = port

        def send_message(self, msg):
            messages["msg"] = msg

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(notification_manager.smtplib, "SMTP", DummySMTP)
    nm = NotificationManager(email_sender="from@example.com")
    assert nm.send_email_notification("subj", "body", ["to@example.com"])
    assert messages["msg"]["Subject"] == "subj"


def test_log_notification(monkeypatch, caplog):
    """Отправка должна фиксироваться в логах."""
    def fake_post(url, json=None, timeout=10):
        return DummyResponse()

    monkeypatch.setattr(notification_manager.httpx, "post", fake_post)
    nm = NotificationManager(slack_webhook_url="http://example.com")
    with caplog.at_level(logging.INFO, logger="arb"):
        nm.send_slack_notification("msg")
    assert "NOTIFY[slack]: msg" in caplog.text
