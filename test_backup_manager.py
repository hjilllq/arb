import asyncio
from pathlib import Path

from cryptography.fernet import Fernet

from backup_manager import BackupManager
from database import TradeDatabase


def test_backup_and_restore(tmp_path):
    file = tmp_path / "data.db"
    file.write_text("hello")
    key = Fernet.generate_key()
    manager = BackupManager(
        files=[str(file)],
        backup_dir=str(tmp_path / "b"),
        interval_hours=0,
        encryption_key=key,
    )
    backups = asyncio.run(manager.maybe_backup())
    backup_path = Path(backups[0])
    assert backup_path.suffix == ".enc"
    # Повреждаем файл
    file.write_text("corrupt")
    asyncio.run(manager.verify_and_restore())
    assert file.read_text() == "hello"


def test_restore_missing_file(tmp_path):
    file = tmp_path / "config.env"
    file.write_text("TOKEN=1")
    key = Fernet.generate_key()
    manager = BackupManager(
        files=[str(file)],
        backup_dir=str(tmp_path / "backup"),
        interval_hours=0,
        encryption_key=key,
    )
    asyncio.run(manager.maybe_backup())
    file.unlink()  # удаляем оригинал
    asyncio.run(manager.verify_and_restore())
    assert file.read_text() == "TOKEN=1"


def test_database_restore_on_reconnect(tmp_path):
    db_path = tmp_path / "trades.db"
    key = Fernet.generate_key()
    manager = BackupManager(
        files=[str(db_path)],
        backup_dir=str(tmp_path / "backup"),
        interval_hours=0,
        encryption_key=key,
    )
    db = TradeDatabase(db_path=str(db_path), backup_manager=manager)

    async def _initial():
        async with db:
            await db.save_trade_data("BTCUSDT", 1.0, 1.0, "buy")

    asyncio.run(_initial())
    db_path.write_text("corrupt")

    async def _reconnect():
        async with db:
            rows = await db.get_trade_history("BTCUSDT")
        return rows

    rows = asyncio.run(_reconnect())
    assert rows[0]["price"] == 1.0
