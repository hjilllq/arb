"""Ежемесячная очистка устаревших записей из базы данных.

Класс :class:`DataRetentionManager` запускает периодическую задачу,
которая удаляет старые или некорректные записи из таблицы сделок
и уведомляет разработчика о выполненной очистке.
"""

from __future__ import annotations

from dataclasses import dataclass
from contextlib import suppress
from typing import Optional

import asyncio

from database import TradeDatabase
from notification_manager import NotificationManager


@dataclass
class DataRetentionManager:
    """Периодическая проверка и очистка устаревших данных.

    Parameters
    ----------
    db:
        Экземпляр :class:`TradeDatabase`, содержащий информацию о сделках.
    notifier:
        Менеджер уведомлений. Если ``None``, уведомления не отправляются.
    interval_days:
        Периодичность проверки, по умолчанию раз в 30 дней.
    """

    db: TradeDatabase
    notifier: Optional[NotificationManager] = None
    interval_days: int = 30
    _task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Запустить фоновой процесс очистки."""
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Остановить фоновой процесс."""
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        while True:  # pragma: no cover - цикл работает в продакшене
            await self.cleanup_once()
            await asyncio.sleep(self.interval_days * 24 * 3600)

    async def cleanup_once(self) -> tuple[int, int]:
        """Выполнить очистку и вернуть количество удалённых записей.

        Returns
        -------
        tuple[int, int]
            Количество удалённых устаревших и некорректных записей.
        """

        removed_old = await self.db.clear_old_data(self.db.retention_days)
        removed_bad = await self._remove_incomplete_records()
        if self.notifier and (removed_old or removed_bad):
            msg = (
                f"Удалено устаревших записей: {removed_old}; "
                f"некорректных: {removed_bad}"
            )
            self.notifier.send_telegram_notification(msg)
        return removed_old, removed_bad

    async def _remove_incomplete_records(self) -> int:
        """Удалить записи с некорректными или пустыми данными."""

        await self.db.flush_cache()
        if self.db.conn is None:
            return 0
        cursor = await self.db.conn.execute(
            "DELETE FROM trades WHERE qty <= 0 OR price <= 0 "
            "OR side NOT IN ('buy','sell')"
        )
        await self.db.conn.commit()
        return cursor.rowcount
