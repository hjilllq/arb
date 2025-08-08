"""Централизованная обработка ошибок с уведомлениями.

Этот модуль предоставляет функцию :func:`handle_error`, которая
логирует исключение и при наличии менеджера уведомлений сообщает
о проблеме через настроенные каналы. Использование общей точки
обработки упрощает отслеживание сбоев и причин их возникновения.
"""

from __future__ import annotations

from typing import Optional

from logger import log_error

try:  # Факультативный импорт, чтобы избежать циклических зависимостей
    from notification_manager import NotificationManager
except Exception:  # pragma: no cover - импорт может провалиться при ранней загрузке
    NotificationManager = None  # type: ignore


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
            notifier.send_telegram_notification(f"{message}: {exc}")
        except Exception as notify_exc:  # pragma: no cover - оповещение может провалиться
            log_error("Failed to send error notification", notify_exc)
