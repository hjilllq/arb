import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Dict

import httpx
import platform

from dependency_manager import DependencyManager


class DummyNotifier:
    def __init__(self) -> None:
        self.messages = []

    def send_telegram_notification(self, msg: str) -> None:  # pragma: no cover - контейнер
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


def test_check_api_keys(tmp_path):
    notifier = DummyNotifier()
    dm = DependencyManager(
        tmp_path / "req.txt",
        tmp_path / "state.json",
        notifier,
        api_keys={"ok": "123", "bad": ""},
    )
    res = dm.check_api_keys()
    assert res == {"ok": True, "bad": False}
    assert any("Некорректный API" in m for m in notifier.messages)


def test_check_os_version(monkeypatch, tmp_path):
    notifier = DummyNotifier()
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"os_version": "old"}))
    dm = DependencyManager(tmp_path / "req.txt", state, notifier)
    monkeypatch.setattr(platform, "platform", lambda: "new")
    dm.check_os_version()
    assert json.loads(state.read_text())["os_version"] == "new"
    assert any("Версия ОС" in m for m in notifier.messages)


def test_daily_check(monkeypatch, tmp_path):
    req = tmp_path / "req.txt"
    req.write_text("demo==1.0\n")
    state = tmp_path / "state.json"
    state.write_text(
        json.dumps(
            {
                "last_check": (
                    datetime.now(timezone.utc) - timedelta(hours=1)
                ).isoformat(),
                "previous_versions": {},
            }
        )
    )
    dm = DependencyManager(req, state)

    called: Dict[str, bool] = {}

    def fake_updates():
        called["updates"] = True
        return {}

    def fake_keys():
        called["keys"] = True
        return {}

    def fake_os():
        called["os"] = True
        return "v"

    monkeypatch.setattr(dm, "check_for_updates", fake_updates)
    monkeypatch.setattr(dm, "check_api_keys", fake_keys)
    monkeypatch.setattr(dm, "check_os_version", fake_os)
    assert dm.daily_check() == {}
    assert not called

    state.write_text(
        json.dumps(
            {
                "last_check": (
                    datetime.now(timezone.utc) - timedelta(days=2)
                ).isoformat(),
                "previous_versions": {},
            }
        )
    )
    dm = DependencyManager(req, state)
    monkeypatch.setattr(dm, "check_for_updates", fake_updates)
    monkeypatch.setattr(dm, "check_api_keys", fake_keys)
    monkeypatch.setattr(dm, "check_os_version", fake_os)
    dm.daily_check()
    assert called == {"updates": True, "keys": True, "os": True}

