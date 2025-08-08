"""Утилита для резервного копирования критичных файлов.

Класс :class:`BackupManager` автоматически создаёт резервные копии
указанных файлов, проверяет их целостность по контрольной сумме SHA256
и восстанавливает оригиналы при повреждении или удалении.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Dict, Iterable, List
import asyncio
import shutil


@dataclass
class BackupManager:
    """Простое управление резервными копиями.

    Параметры
    ----------
    files:
        Список файлов, которые необходимо сохранять.
    backup_dir:
        Каталог, куда помещаются резервные копии.
    interval_hours:
        Периодичность создания резервных копий.
    """

    files: Iterable[str]
    backup_dir: str = "backups"
    interval_hours: int = 24
    _checksums: Dict[str, str] = field(default_factory=dict)
    _last_backup: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    async def maybe_backup(self) -> List[str]:
        """Создать резервные копии, если прошёл заданный интервал."""

        now = datetime.now(timezone.utc)
        if now - self._last_backup < timedelta(hours=self.interval_hours):
            return []
        Path(self.backup_dir).mkdir(parents=True, exist_ok=True)
        timestamp = now.strftime("%Y%m%d%H%M%S")
        backups: List[str] = []
        for file in self.files:
            src = Path(file)
            if not src.exists():
                continue
            dst = Path(self.backup_dir) / f"{src.name}.{timestamp}.bak"
            await asyncio.to_thread(shutil.copy2, src, dst)
            self._checksums[src.name] = await asyncio.to_thread(
                self._checksum, dst
            )
            backups.append(str(dst))
        self._last_backup = now
        return backups

    async def verify_and_restore(self) -> None:
        """Проверить целостность и восстановить файлы при необходимости."""

        for file in self.files:
            src = Path(file)
            backup = self._latest_backup(src.name)
            if backup is None:
                continue
            expected = self._checksums.get(src.name)
            if not src.exists():
                await asyncio.to_thread(shutil.copy2, backup, src)
                continue
            current = await asyncio.to_thread(self._checksum, src)
            if expected and current != expected:
                await asyncio.to_thread(shutil.copy2, backup, src)

    def _latest_backup(self, name: str) -> str | None:
        """Вернуть путь к последней резервной копии файла."""

        directory = Path(self.backup_dir)
        pattern = f"{name}.*.bak"
        backups = sorted(directory.glob(pattern))
        return str(backups[-1]) if backups else None

    @staticmethod
    def _checksum(path: Path) -> str:
        """Подсчитать контрольную сумму файла."""

        h = sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
