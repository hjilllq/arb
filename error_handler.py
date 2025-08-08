"""Централизованная обработка ошибок с уведомлениями.

Помимо функции :func:`handle_error`, модуль предоставляет утилиты
для глобального перехвата необработанных исключений и автоматического
повторного выполнения операций. Это позволяет минимизировать простои
бота: критичные ошибки попадают в журнал, пользователю отправляется
уведомление, а временные сбои пытаются исправиться повтором.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Awaitable, Callable, Optional, TypeVar

from logger import log_error

try:  # Факультативный импорт, чтобы избежать циклических зависимостей
    from notification_manager import NotificationManager
except Exception:  # pragma: no cover - импорт может провалиться при ранней загрузке
    NotificationManager = None  # type: ignore


T = TypeVar("T")


async def retry_async(
    func: Callable[..., Awaitable[T]],
    *args,
    retries: int = 3,
    notifier: Optional["NotificationManager"] = None,
    **kwargs,
) -> T:
    """Повторно выполнить асинхронную операцию при временной ошибке.

    Попытки выполняются с экспоненциальной задержкой 1, 2, 4 секунды.
    После исчерпания ``retries`` исключение будет записано и проброшено.
    """

    for attempt in range(retries):
        try:
            return await func(*args, **kwargs)
        except Exception as exc:
            if attempt + 1 >= retries:
                handle_error("Operation failed", exc, notifier)
                raise
            await asyncio.sleep(2 ** attempt)


def install_global_handler(
    notifier: Optional["NotificationManager"] = None,
) -> None:
    """Установить глобальные обработчики исключений.

    Перехватывает необработанные ошибки как в обычном коде, так и в задачах
    ``asyncio``. Все исключения записываются в лог и, при наличии
    ``notifier``, отправляются в Telegram.
    """

    def _excepthook(exc_type, exc, tb):  # type: ignore[override]
        handle_error("Uncaught exception", exc, notifier)

    sys.excepthook = _excepthook

    def _asyncio_handler(loop, context):
        exc = context.get("exception")
        message = context.get("message", "Asyncio error")
        if exc:
            handle_error(message, exc, notifier)
        else:  # pragma: no cover - маловероятное отсутствие исключения
            log_error(message)

    try:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(_asyncio_handler)
    except RuntimeError:  # pragma: no cover - цикл ещё не создан
        pass


def handle_error(
    message: str,
    exc: Exception,
    notifier: Optional["NotificationManager"] = None,
) -> None:
    """Записать ошибку и уведомить пользователя.

    Parameters
    ----------
    message:
        Человекочитаемое описание проблемы.
    exc:
        Возникшее исключение.
    notifier:
        Необязательный менеджер уведомлений. Если указан, будет
        отправлено оповещение с текстом ошибки.
    """

    log_error(message, exc)
    if notifier:
        try:
            if hasattr(notifier, "notify_critical"):
                notifier.notify_critical(f"{message}: {exc}")
            else:
                notifier.send_telegram_notification(f"{message}: {exc}")
        except Exception as notify_exc:  # pragma: no cover - оповещение может провалиться
            log_error("Failed to send error notification", notify_exc)
