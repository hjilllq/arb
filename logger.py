"""Централизованное JSON-логирование проекта."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional
import json
import logging
import os
import sys
from datetime import datetime

# Ключи, которые нужно маскировать в логах
SENSITIVE_KEYS = {
    "api_key", "api_secret", "bybit_api_key", "bybit_api_secret",
    "token", "password", "secret", "authorization", "fernet_key",
}


def _sanitize(data: Mapping[str, Any]) -> Dict[str, Any]:
    """Скрыть конфиденциальные значения перед логированием."""
    out: Dict[str, Any] = {}
    for k, v in data.items():
        try:
            out[k] = "***" if k.lower() in SENSITIVE_KEYS else v
        except Exception:
            out[k] = v
    return out


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "level": record.levelname,
            "module": record.module,
            "msg": record.getMessage(),
        }
        # logging.LoggerAdapter может передавать "extra" в record.__dict__
        for key in ("trace_id", "order_id"):
            if key in record.__dict__:
                payload[key] = record.__dict__[key]
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str = "INFO", key: Optional[str] = None) -> None:
    """Инициализация JSON-логов."""
    lvl = getattr(logging, level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(lvl)
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root.addHandler(handler)

    # отключим шумные логгеры сторонних пакетов, если нужно
    for noisy in ("httpx", "asyncio", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def log_event(message: str, **fields: Any) -> None:
    logging.getLogger(__name__).info(message, extra=_sanitize(fields))


def log_error(message: str, exc: Optional[Exception] = None, **fields: Any) -> None:
    """Лог ошибки. Если exc задан — попадёт стек-трейс."""
    logger = logging.getLogger(__name__)
    if exc:
        logger.exception("%s: %s", message, exc, extra=_sanitize(fields))
    else:
        logger.error(message, extra=_sanitize(fields))


def log_trade_data(trade: Mapping[str, Any]) -> None:
    logging.getLogger(__name__).info("trade", extra=_sanitize(dict(trade)))


def log_system_health(status: Mapping[str, Any]) -> None:
    logging.getLogger(__name__).info("health", extra=_sanitize(dict(status)))
