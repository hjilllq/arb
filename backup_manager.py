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
from typing import Dict, Iterable, List, Optional
import asyncio
import shutil

from cryptography.fernet import Fernet


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
    encryption_key: Optional[bytes | str] = None
    _checksums: Dict[str, str] = field(default_factory=dict)
    _last_backup: datetime = field(
        default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc)
    )
    _fernet: Optional[Fernet] = field(init=False, default=None)

    def __post_init__(self) -> None:
        if self.encryption_key is not None:
            key = (
                self.encryption_key.encode()
                if isinstance(self.encryption_key, str)
                else self.encryption_key
            )
            self._fernet = Fernet(key)

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
            data = await asyncio.to_thread(src.read_bytes)
            checksum = sha256(data).hexdigest()
            if self._fernet is not None:
                encrypted = self._fernet.encrypt(data)
                dst = Path(self.backup_dir) / f"{src.name}.{timestamp}.bak.enc"
                await asyncio.to_thread(dst.write_bytes, encrypted)
            else:
                dst = Path(self.backup_dir) / f"{src.name}.{timestamp}.bak"
                await asyncio.to_thread(shutil.copy2, src, dst)
            self._checksums[src.name] = checksum
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
            if expected is None:
                expected = await asyncio.to_thread(
                    self._checksum_from_backup, Path(backup)
                )
            if not src.exists():
                data = await asyncio.to_thread(self._read_backup, Path(backup))
                await asyncio.to_thread(src.write_bytes, data)
                continue
            current = await asyncio.to_thread(self._checksum, src)
            if current != expected:
                data = await asyncio.to_thread(self._read_backup, Path(backup))
                await asyncio.to_thread(src.write_bytes, data)

    def _latest_backup(self, name: str) -> str | None:
        """Вернуть путь к последней резервной копии файла."""

        directory = Path(self.backup_dir)
        pattern = f"{name}*.bak*"
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

    def _read_backup(self, path: Path) -> bytes:
        """Прочитать и при необходимости расшифровать резервную копию."""

        data = path.read_bytes()
        if self._fernet is not None:
            data = self._fernet.decrypt(data)
        return data

    def _checksum_from_backup(self, path: Path) -> str:
        """Получить контрольную сумму из зашифрованной копии."""

        return sha256(self._read_backup(path)).hexdigest()

    async def run(self) -> None:
        """Периодически выполнять резервное копирование."""

        while True:
            await self.maybe_backup()
            await asyncio.sleep(self.interval_hours * 3600)
