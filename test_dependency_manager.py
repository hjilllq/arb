import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx

from dependency_manager import DependencyManager


class DummyNotifier:
    def __init__(self):
        self.messages = []

    def send_telegram_notification(self, msg: str) -> None:  # pragma: no cover - простой контейнер
        self.messages.append(msg)


def test_check_for_updates(monkeypatch, tmp_path):
    req = tmp_path / "req.txt"
    req.write_text("demo==1.0\n")

    def fake_get(url, timeout):
        return SimpleNamespace(json=lambda: {"info": {"version": "1.1"}}, raise_for_status=lambda: None)

    monkeypatch.setattr(httpx, "get", fake_get)
    dm = DependencyManager(req, tmp_path / "state.json")
    updates = dm.check_for_updates()
    assert updates == {"demo": ("1.0", "1.1")}


def test_update_and_rollback(monkeypatch, tmp_path):
    req = tmp_path / "req.txt"
    req.write_text("demo==1.0\n")
    state = tmp_path / "state.json"
    notifier = DummyNotifier()
    dm = DependencyManager(req, state, notifier)
    updates = {"demo": ("1.0", "1.1")}

    class Result:
        returncode = 0

    monkeypatch.setattr("subprocess.run", lambda *a, **k: Result())
    assert dm.update_dependencies(updates)
    assert dm._state["previous_versions"] == {"demo": "1.0"}
    assert any("Обновлён demo" in m for m in notifier.messages)

    dm.rollback()
    assert dm._state["previous_versions"] == {}
    assert any("Откат зависимостей" in m for m in notifier.messages)


def test_daily_check(monkeypatch, tmp_path):
    req = tmp_path / "req.txt"
    req.write_text("demo==1.0\n")
    state = tmp_path / "state.json"
    state.write_text(json.dumps({
        "last_check": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
        "previous_versions": {}
    }))
    dm = DependencyManager(req, state)

    called = {}
    def fake_check():
        called["ran"] = True
        return {}

    monkeypatch.setattr(dm, "check_for_updates", fake_check)
    assert dm.daily_check() == {}
    assert "ran" not in called

    # modify last_check to force run
    state.write_text(json.dumps({
        "last_check": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
        "previous_versions": {}
    }))
    dm = DependencyManager(req, state)
    monkeypatch.setattr(dm, "check_for_updates", fake_check)
    dm.daily_check()
    assert "ran" in called
