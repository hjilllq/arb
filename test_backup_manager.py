import asyncio

from backup_manager import BackupManager


def test_backup_and_restore(tmp_path):
    file = tmp_path / "data.db"
    file.write_text("hello")
    manager = BackupManager(files=[str(file)], backup_dir=str(tmp_path / "b"), interval_hours=0)
    asyncio.run(manager.maybe_backup())
    # Повреждаем файл
    file.write_text("corrupt")
    asyncio.run(manager.verify_and_restore())
    assert file.read_text() == "hello"


def test_restore_missing_file(tmp_path):
    file = tmp_path / "config.env"
    file.write_text("TOKEN=1")
    manager = BackupManager(files=[str(file)], backup_dir=str(tmp_path / "backup"), interval_hours=0)
    asyncio.run(manager.maybe_backup())
    file.unlink()  # удаляем оригинал
    asyncio.run(manager.verify_and_restore())
    assert file.read_text() == "TOKEN=1"
