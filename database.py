"""Асинхронное хранилище сделок на базе SQLite.

Использует библиотеку :mod:`aiosqlite` для неблокирующих операций
записи и чтения. Такой подход позволяет выполнять работу с базой
параллельно с запросами к API, не блокируя главный цикл событий.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiosqlite
import shutil

from error_handler import handle_error
from backup_manager import BackupManager


@dataclass
class TradeDatabase:
    """Простой интерфейс для сохранения и запроса информации о сделках.

    Параметры
    ----------
    db_path:
        Путь к файлу базы данных SQLite. По умолчанию ``"trades.db"``.
    """

    db_path: str = "trades.db"
    retention_days: int = 30
    flush_limit: int = 100
    cleanup_interval_hours: int = 1
    conn: Optional[aiosqlite.Connection] = None
    backup_manager: Optional[BackupManager] = None
    _cache: List[tuple] = field(default_factory=list)
    _last_cleanup: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    async def __aenter__(self) -> "TradeDatabase":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - гарантированное закрытие
        await self.close()

    async def connect(self) -> None:
        """Открыть соединение и подготовить таблицы."""
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self._create_tables()

    async def _create_tables(self) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                price REAL NOT NULL,
                qty REAL NOT NULL,
                side TEXT NOT NULL,
                pnl REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await self.conn.commit()

    async def save_trade_data(
        self,
        pair: str,
        price: float,
        qty: float,
        side: str,
        pnl: float = 0.0,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Добавить информацию о сделке в очередь на запись.

        Данные сохраняются в оперативной памяти и периодически сбрасываются
        на диск, что уменьшает количество операций записи на SSD.
        """

        ts = timestamp or datetime.now(timezone.utc)
        self._cache.append((pair, price, qty, side, pnl, ts.isoformat()))
        if len(self._cache) >= self.flush_limit:
            await self.flush_cache()
        await self._maybe_cleanup()
        await self._maybe_backup()

    async def get_trade_history(self, pair: str) -> List[Dict[str, Any]]:
        """Получить список сделок для указанной пары."""

        await self.flush_cache()
        assert self.conn is not None
        cursor = await self.conn.execute(
            "SELECT pair, price, qty, side, pnl, timestamp FROM trades WHERE pair = ? ORDER BY timestamp",
            (pair,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_trade_statistics(self, pair: Optional[str] = None) -> Dict[str, float]:
        """Подсчитать агрегированные метрики по сделкам."""

        await self.flush_cache()
        assert self.conn is not None
        query = "SELECT COUNT(*) AS count, SUM(pnl) AS total_pnl, SUM(qty) AS total_qty FROM trades"
        params: tuple[Any, ...] = ()
        if pair is not None:
            query += " WHERE pair = ?"
            params = (pair,)
        cursor = await self.conn.execute(query, params)
        row = await cursor.fetchone()
        return {
            "count": row["count"] or 0,
            "total_pnl": row["total_pnl"] or 0.0,
            "total_qty": row["total_qty"] or 0.0,
        }

    async def clear_old_data(self, days: int) -> int:
        """Удалить записи, которые старше указанного количества дней."""

        await self.flush_cache()
        assert self.conn is not None
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        cursor = await self.conn.execute(
            "DELETE FROM trades WHERE timestamp < ?",
            (cutoff.isoformat(),),
        )
        await self.conn.commit()
        return cursor.rowcount

    async def backup_database(self, backup_path: str) -> str:
        """Сохранить резервную копию файла базы данных."""

        await self.flush_cache()
        assert self.conn is not None
        await self.conn.commit()
        shutil.copy2(self.db_path, backup_path)
        return backup_path

    async def verify_and_restore(self) -> None:
        """Проверить целостность базы и восстановить её при повреждении."""

        if self.backup_manager is None:
            return
        await self.backup_manager.verify_and_restore()

    async def close(self) -> None:
        """Закрыть соединение и освободить ресурсы."""

        if self.conn is not None:
            await self.flush_cache()
            await self.conn.close()
            self.conn = None

    async def flush_cache(self) -> None:
        """Сохранить накопленные записи на диск."""

        if not self._cache or self.conn is None:
            return
        try:
            await self.conn.executemany(
                "INSERT INTO trades (pair, price, qty, side, pnl, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                self._cache,
            )
            await self.conn.commit()
        except Exception as exc:  # pragma: no cover - непредвиденная ошибка базы данных
            handle_error("Failed to flush trades", exc)
            raise
        finally:
            self._cache.clear()

    async def _maybe_cleanup(self) -> None:
        """При необходимости удалить устаревшие записи."""

        now = datetime.now(timezone.utc)
        if now - self._last_cleanup >= timedelta(hours=self.cleanup_interval_hours):
            await self.clear_old_data(self.retention_days)
            self._last_cleanup = now

    async def _maybe_backup(self) -> None:
        """Создать резервную копию базы данных при наступлении интервала."""

        if self.backup_manager is None:
            return
        await self.backup_manager.maybe_backup()

