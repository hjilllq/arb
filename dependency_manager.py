"""Мониторинг и обновление зависимостей проекта."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple

import httpx
from packaging import version

from logger import log_event, log_error


@dataclass
class DependencyManager:
    """Проверяет версии зависимостей и обновляет их при необходимости.

    Parameters
    ----------
    requirements_path:
        Путь к файлу ``requirements.txt``.
    state_path:
        Файл, где хранится дата последней проверки и версии для отката.
    notifier:
        Менеджер уведомлений, поддерживающий метод ``send_telegram_notification``.
    """

    requirements_path: str | Path = "requirements.txt"
    state_path: str | Path = "dependency_state.json"
    notifier: object | None = None
    _state: Dict[str, object] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.requirements_path = Path(self.requirements_path)
        self.state_path = Path(self.state_path)
        if self.state_path.exists():
            try:
                self._state = json.loads(self.state_path.read_text())
            except json.JSONDecodeError:
                log_error("Failed to parse dependency state file")
                self._state = {}
        self._state.setdefault("last_check", None)
        self._state.setdefault("previous_versions", {})

    # ------------------------------------------------------------------
    def _save_state(self) -> None:
        """Сохранить внутреннее состояние менеджера."""
        self.state_path.write_text(json.dumps(self._state))

    # ------------------------------------------------------------------
    def _fetch_latest_version(self, package: str) -> str:
        """Получить последнюю доступную версию пакета с PyPI."""
        url = f"https://pypi.org/pypi/{package}/json"
        resp = httpx.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data["info"]["version"]

    # ------------------------------------------------------------------
    def check_for_updates(self) -> Dict[str, Tuple[str, str]]:
        """Проверить доступные обновления.

        Returns
        -------
        dict
            Словарь ``{пакет: (текущая, последняя)}``, только для пакетов,
            где обнаружена новая версия.
        """
        updates: Dict[str, Tuple[str, str]] = {}
        if not self.requirements_path.exists():
            log_error("requirements.txt not found")
            return updates

        for line in self.requirements_path.read_text().splitlines():
            if "==" not in line:
                continue
            pkg, cur = line.strip().split("==")
            try:
                latest = self._fetch_latest_version(pkg)
            except httpx.HTTPError as exc:  # pragma: no cover - сеть нестабильна
                log_error(f"Failed to fetch version for {pkg}", exc)
                continue
            if version.parse(latest) > version.parse(cur):
                updates[pkg] = (cur, latest)

        self._state["last_check"] = datetime.now(timezone.utc).isoformat()
        self._save_state()
        if updates and self.notifier:
            self.notifier.send_telegram_notification(
                f"Доступны обновления: {updates}"
            )
        return updates

    # ------------------------------------------------------------------
    def update_dependencies(self, updates: Dict[str, Tuple[str, str]]) -> bool:
        """Установить новые версии пакетов.

        При успешном обновлении сохраняет предыдущие версии для возможного отката.
        """
        success = True
        for pkg, (cur, new) in updates.items():
            cmd = [
                "python",
                "-m",
                "pip",
                "install",
                f"{pkg}=={new}",
            ]
            result = subprocess.run(cmd, capture_output=True)  # noqa: S603,S607
            if result.returncode == 0:
                self._state["previous_versions"][pkg] = cur
                log_event(f"Updated {pkg} to {new}")
                if self.notifier:
                    self.notifier.send_telegram_notification(
                        f"Обновлён {pkg} до версии {new}"
                    )
            else:
                success = False
                log_error(f"Failed to update {pkg}")
        self._save_state()
        return success

    # ------------------------------------------------------------------
    def rollback(self) -> None:
        """Откатить пакеты к сохранённым версиям."""
        backups: Dict[str, str] = self._state.get("previous_versions", {})
        for pkg, ver in backups.items():
            cmd = [
                "python",
                "-m",
                "pip",
                "install",
                f"{pkg}=={ver}",
            ]
            subprocess.run(cmd, capture_output=True)  # noqa: S603,S607
            log_event(f"Rolled back {pkg} to {ver}")
        if backups and self.notifier:
            self.notifier.send_telegram_notification(
                f"Откат зависимостей: {backups}"
            )
        self._state["previous_versions"] = {}
        self._save_state()

    # ------------------------------------------------------------------
    def daily_check(self) -> Dict[str, Tuple[str, str]]:
        """Выполнить проверку, если с момента последнего запуска прошли сутки."""
        last = self._state.get("last_check")
        if last:
            last_dt = datetime.fromisoformat(last)
            if datetime.now(timezone.utc) - last_dt < timedelta(days=1):
                return {}
        return self.check_for_updates()
