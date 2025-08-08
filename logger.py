"""Централизованные инструменты логирования для проекта арбитража.

Модуль использует пакет :mod:`logging` и добавляет поддержку нескольких
уровней сообщений, запись в отдельный файл с ротацией и опциональное
шифрование строк логов. Это позволяет безопасно хранить информацию о
торговых операциях и событиях системы."""
from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any

from cryptography.fernet import Fernet

SENSITIVE_KEYS = {"api_key", "api_secret", "token", "password"}


class EncryptedRotatingFileHandler(RotatingFileHandler):
    """Обработчик, шифрующий каждую строку перед записью в файл."""

    def __init__(
        self,
        filename: str,
        key: str,
        max_bytes: int = 1_000_000,
        backup_count: int = 5,
    ) -> None:
        os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
        self.fernet = Fernet(key.encode() if isinstance(key, str) else key)
        super().__init__(
            filename,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - тонкая работа с IO
        msg = self.format(record)
        encrypted = self.fernet.encrypt(msg.encode("utf-8")).decode()
        self.acquire()
        try:
            if self.shouldRollover(record):
                self.doRollover()
            stream = self.stream or self._open()
            stream.write(encrypted + self.terminator)
            stream.flush()
        finally:
            self.release()


def _sanitize(data: Dict[str, Any]) -> Dict[str, Any]:
    """Скрыть конфиденциальные значения перед логированием."""
    return {k: ("***" if k.lower() in SENSITIVE_KEYS else v) for k, v in data.items()}


logger = logging.getLogger("arb")


def configure_logging(
    level: str = "INFO",
    logfile: str = "logs/arb.log",
    key: str | None = None,
) -> None:
    """Настроить общий логгер проекта.

    Parameters
    ----------
    level:
        Минимальный уровень сообщений (DEBUG, INFO, WARNING, ERROR).
    logfile:
        Путь к файлу, куда будут записываться логи.
    key:
        Ключ шифрования. Если указан, строки в файле будут зашифрованы
        с использованием алгоритма Fernet.
    """

    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    if logfile:
        if key:
            file_handler = EncryptedRotatingFileHandler(logfile, key)
        else:
            os.makedirs(os.path.dirname(logfile) or ".", exist_ok=True)
            file_handler = RotatingFileHandler(
                logfile, maxBytes=1_000_000, backupCount=5, encoding="utf-8"
            )
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))


def log_event(message: str) -> None:
    """Записать общее информационное событие.

    Parameters
    ----------
    message:
        Описание события.
    """
    logger.info(message)


def log_error(message: str, error: BaseException | None = None, **ctx: Any) -> None:
    """Записать ошибку.

    Parameters
    ----------
    message:
        Понятное человеку описание ошибки.
    exc:
        Необязательное исключение, детали которого нужно залогировать.
    """
    if exc:
        logger.exception("%s: %s", message, exc)
    else:
        logger.error(message)


def log_warning(message: str) -> None:
    """Записать предупреждение."""
    logger.warning(message)


def log_debug(message: str) -> None:
    """Записать отладочное сообщение."""
    logger.debug(message)


def log_trade_data(trade: Dict[str, Any]) -> None:
    """Логировать структурированные данные сделки.

    Parameters
    ----------
    trade:
        Словарь с характеристиками сделки, например пара, цена и объём.
    """
    logger.info("TRADE DATA: %s", _sanitize(trade))


def log_system_health(status: Dict[str, Any]) -> None:
    """Логировать текущие метрики состояния системы.

    Parameters
    ----------
    status:
        Словарь, где ключ — имя проверки, а значение — её статус (число или логический тип).
    """
    logger.info("SYSTEM HEALTH: %s", _sanitize(status))
