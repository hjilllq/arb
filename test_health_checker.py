"""Тесты для модуля health_checker."""

from __future__ import annotations

import asyncio
import types

from health_checker import (
    API_UP,
    BALANCE_OK,
    DB_UP,
    TRADING_UP,
    WS_UP,
    HealthChecker,
)
from risk_manager import RiskManager
from monitor import CPU_USAGE, MEMORY_USAGE


class DummyNotifier:
    """Заглушка для NotificationManager, записывающая сообщения."""

    def __init__(self) -> None:
        self.messages: list[str] = []

    def send_telegram_notification(self, message: str) -> bool:  # pragma: no cover - простая заглушка
        self.messages.append(message)
        return True


class DummyBot:
    """Простая модель торгового бота с флагом активности."""

    def __init__(self, active: bool) -> None:
        self.active = active


def test_check_system_updates_metrics_and_notifies() -> None:
    """Проверить обновление метрик и отправку предупреждений."""

    notifier = DummyNotifier()
    bot = DummyBot(active=False)
    rm = RiskManager()
    checker = HealthChecker(notifier=notifier, trading_bot=bot, risk_manager=rm)

    async def fake_api(self) -> bool:  # noqa: ANN001 - заглушка
        return True

    async def fake_db(self) -> bool:  # noqa: ANN001 - заглушка
        return True

    async def fake_ws(self) -> bool:  # noqa: ANN001 - заглушка
        return False

    checker.check_api_health = types.MethodType(fake_api, checker)
    checker.check_database_health = types.MethodType(fake_db, checker)
    checker.check_websocket_health = types.MethodType(fake_ws, checker)

    status = asyncio.run(checker.check_system())

    assert status == {
        "api": True,
        "database": True,
        "websocket": False,
        "trading": False,
        "balance": True,
    }
    assert API_UP._value.get() == 1
    assert DB_UP._value.get() == 1
    assert WS_UP._value.get() == 0
    assert TRADING_UP._value.get() == 0
    assert BALANCE_OK._value.get() == 1
    assert notifier.messages  # уведомление отправлено
    assert CPU_USAGE._value.get() >= 0
    assert MEMORY_USAGE._value.get() >= 0
    assert rm.safety_factor == 0.5


def test_risk_factor_resets_after_recovery() -> None:
    """Риск уменьшается при сбое и возвращается после восстановления."""

    notifier = DummyNotifier()
    bot = DummyBot(active=True)
    rm = RiskManager()
    checker = HealthChecker(notifier=notifier, trading_bot=bot, risk_manager=rm)

    async def bad_api(self) -> bool:  # noqa: ANN001 - заглушка
        return False

    async def good_api(self) -> bool:  # noqa: ANN001 - заглушка
        return True

    async def good_ws(self) -> bool:  # noqa: ANN001 - заглушка
        return True

    checker.check_websocket_health = types.MethodType(good_ws, checker)

    checker.check_api_health = types.MethodType(bad_api, checker)
    status = asyncio.run(checker.check_system())
    assert not status["api"]
    assert rm.safety_factor == 0.5

    checker.check_api_health = types.MethodType(good_api, checker)
    status = asyncio.run(checker.check_system())
    assert status["api"]
    assert rm.safety_factor == 1.0

