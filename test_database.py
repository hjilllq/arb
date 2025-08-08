import asyncio
from pathlib import Path
import pytest

from datetime import datetime, timedelta, timezone

from database import TradeDatabase


def test_database_operations(tmp_path):
    """Проверить сохранение, выборку и резервное копирование."""
    db_file = tmp_path / "trades.db"
    backup_file = tmp_path / "backup.db"

    async def work():
        old_ts = datetime.now(timezone.utc) - timedelta(days=2)
        async with TradeDatabase(
            db_path=str(db_file), flush_limit=1, retention_days=1, cleanup_interval_hours=0
        ) as db:
            await db.save_trade_data("BTCUSDT", 100.0, 1.0, "buy", pnl=5.0, timestamp=old_ts)
            await db.save_trade_data("BTCUSDT", 101.0, 1.0, "sell", pnl=-2.0)
            history = await db.get_trade_history("BTCUSDT")
            stats = await db.get_trade_statistics("BTCUSDT")
            backup = await db.backup_database(str(backup_file))
            return history, stats, backup

    history, stats, backup_path = asyncio.run(work())
    assert len(history) == 1
    assert stats["count"] == 1
    assert Path(backup_path).exists()


def test_database_error(monkeypatch, tmp_path):
    """Обработчик ошибок должен пробрасывать исключение."""
    db_file = tmp_path / "err.db"

    async def work():
        async with TradeDatabase(db_path=str(db_file), flush_limit=1) as db:
            async def failing_executemany(*args, **kwargs):  # pragma: no cover - искусственная ошибка
                raise RuntimeError("db error")

            monkeypatch.setattr(db.conn, "executemany", failing_executemany)
            with pytest.raises(RuntimeError):
                await db.save_trade_data("BTCUSDT", 1, 1, "buy")

    asyncio.run(work())
