from datetime import datetime, timedelta, timezone

from data_retention import DataRetentionManager
from database import TradeDatabase
from notification_manager import NotificationManager
import asyncio


class DummyNotifier(NotificationManager):
    """Тестовый менеджер уведомлений, сохраняющий сообщения."""

    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def send_telegram_notification(self, message: str) -> bool:  # type: ignore[override]
        self.messages.append(message)
        return True


def test_cleanup_removes_old_and_bad_records(tmp_path):
    async def scenario() -> None:
        db_file = tmp_path / "trades.db"
        notifier = DummyNotifier()
        db = TradeDatabase(db_path=str(db_file), retention_days=30)
        async with db:
            # Валидная актуальная запись
            await db.save_trade_data(
                "BTCUSDT", 100.0, 1.0, "buy", timestamp=datetime.now(timezone.utc)
            )
            # Устаревшая запись
            old_ts = datetime.now(timezone.utc) - timedelta(days=31)
            await db.save_trade_data(
                "ETHUSDT", 200.0, 1.0, "sell", timestamp=old_ts
            )
            # Некорректная запись
            await db.save_trade_data("LTCUSDT", 50.0, 0.0, "buy")
            await db.flush_cache()

            manager = DataRetentionManager(db, notifier, interval_days=1)
            removed_old, removed_bad = await manager.cleanup_once()
            assert removed_old == 1
            assert removed_bad == 1
            assert notifier.messages
            # В базе должна остаться только актуальная запись
            history = await db.get_trade_history("BTCUSDT")
            assert len(history) == 1

    asyncio.run(scenario())
