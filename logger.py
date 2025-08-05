"""Logging utility for the arbitrage bot.

This module acts like a diary that never sleeps.  It writes everything our bot
thinks or does into a log file, keeps a copy in the console, and even hides
sensitive secrets by wrapping them in encryption.  The code avoids heavy disk
usage so the Apple Silicon M4 Max can stay cool and efficient.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from cryptography.fernet import Fernet, InvalidToken

# ---------------------------------------------------------------------------
# Helper notifier: real notification_manager will replace this later.
# ---------------------------------------------------------------------------
try:  # pragma: no cover - tiny helper, tested indirectly
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover - fallback for early development
    async def notify(message: str, detail: str = "") -> None:
        logging.warning("Notification: %s - %s", message, detail)


# ---------------------------------------------------------------------------
# Global paths and cryptography setup
# ---------------------------------------------------------------------------
_LOG_FILE = Path("bot.log")
_ARCHIVE_DIR = Path("logs_archive")
_ARCHIVE_DIR.mkdir(exist_ok=True)

_FERNET_KEY_PATH = Path(os.getenv("FERNET_KEY_PATH", Path.home() / ".fernet.key"))
if _FERNET_KEY_PATH.exists():
    _FERNET_KEY = _FERNET_KEY_PATH.read_bytes().strip()
    try:
        if (_FERNET_KEY_PATH.stat().st_mode & 0o777) != 0o600:
            logging.getLogger(__name__).warning(
                "Fernet key permissions are %o, expected 600",
                _FERNET_KEY_PATH.stat().st_mode & 0o777,
            )
    except OSError:  # pragma: no cover - OS-specific
        pass
else:  # create a key once and protect permissions
    _FERNET_KEY = Fernet.generate_key()
    _FERNET_KEY_PATH.write_bytes(_FERNET_KEY)
    try:
        os.chmod(_FERNET_KEY_PATH, 0o600)
    except OSError:  # pragma: no cover - depends on OS
        pass
    logging.getLogger(__name__).info(
        "Fernet key generated and saved to %s", _FERNET_KEY_PATH
    )
_CIPHER = Fernet(_FERNET_KEY)

# Executor for blocking file operations so we do not stall the event loop.
_EXECUTOR = ThreadPoolExecutor(max_workers=2)

# Internal logger instance configured via setup_logger().
_logger = logging.getLogger("arb.bot")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _has_disk_space(required_mb: int = 1) -> bool:
    """Check that at least ``required_mb`` of free space is available.

    Warns and sends a notification if disk space is low.  This avoids failed
    writes when the filesystem is full.
    """
    try:
        free_mb = shutil.disk_usage(_ARCHIVE_DIR).free / (1024 * 1024)
    except OSError:
        return True  # assume enough space if we cannot determine
    if free_mb < required_mb:
        _logger.error("Low disk space: %.1f MB left", free_mb)
        try:
            asyncio.get_running_loop().create_task(
                notify("Low disk space", f"{free_mb:.1f} MB free")
            )
        except RuntimeError:  # no running loop
            pass
        return False
    return True


def _cleanup_old_archives(limit: int = 10) -> None:
    """Remove old log archives keeping only the newest ``limit`` files."""
    backups = sorted(
        _ARCHIVE_DIR.glob("bot.log.*.bak"), key=lambda p: p.stat().st_mtime
    )
    if len(backups) <= limit:
        return
    for old in backups[:-limit]:
        try:
            old.unlink()
            _logger.info("Old backup removed: %s", old)
        except OSError as exc:  # pragma: no cover - ignore deletion errors
            _logger.warning("Failed to remove old backup %s: %s", old, exc)

def _maybe_encrypt(message: str) -> str:
    """Encrypt message if it contains secret words.

    Words like ``key``, ``secret`` or ``token`` hint that the text might be
    sensitive.  In that case we return the encrypted form so it is safe in the
    log file.  Otherwise the original message is returned untouched.
    """
    lowered = message.lower()
    if any(word in lowered for word in ("key", "secret", "token")):
        return encrypt_log(message).decode()
    return message


def get_logger(name: str) -> logging.Logger:
    """Return a child logger with ``name``.

    Examples
    --------
    >>> log = get_logger("test")
    >>> isinstance(log, logging.Logger)
    True
    """
    return _logger.getChild(name)


# ---------------------------------------------------------------------------
# 1. setup_logger
# ---------------------------------------------------------------------------
def setup_logger(log_file: str = "bot.log") -> None:
    """Configure logging to file and console.

    Parameters
    ----------
    log_file:
        Where to store log messages.  The default is ``bot.log`` in the project
        root.
    """
    global _LOG_FILE
    _LOG_FILE = Path(log_file)
    _LOG_FILE.touch(exist_ok=True)

    _logger.setLevel(logging.DEBUG)

    # Clear existing handlers to avoid duplicate logs when reconfiguring.
    _logger.handlers.clear()

    file_handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    console_handler = logging.StreamHandler()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(fmt)
    console_handler.setFormatter(fmt)

    _logger.addHandler(file_handler)
    _logger.addHandler(console_handler)
    _logger.propagate = False


# Configure logger immediately on import.
setup_logger()


# ---------------------------------------------------------------------------
# 2. async log_info
# ---------------------------------------------------------------------------
async def log_info(message: str) -> None:
    """Record an informational message asynchronously.

    Examples
    --------
    >>> asyncio.run(log_info("Bot started"))  # doctest: +SKIP
    """
    msg = _maybe_encrypt(message)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_EXECUTOR, _logger.info, msg)


# ---------------------------------------------------------------------------
# 3. async log_warning
# ---------------------------------------------------------------------------
async def log_warning(message: str) -> None:
    """Record a warning message asynchronously."""
    msg = _maybe_encrypt(message)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_EXECUTOR, _logger.warning, msg)


# ---------------------------------------------------------------------------
# 4. async log_error
# ---------------------------------------------------------------------------
async def log_error(message: str, error: Exception) -> None:
    """Record an error message and notify external systems.

    Parameters
    ----------
    message:
        Human readable description of what went wrong.
    error:
        The exception instance that triggered this log.
    """
    msg = _maybe_encrypt(message)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_EXECUTOR, _logger.error, "%s | %s", msg, error)
    try:
        await notify("Error", f"{msg}: {error}")
    except Exception:  # pragma: no cover - notification failure
        pass


# ---------------------------------------------------------------------------
# 5. async log_trade
# ---------------------------------------------------------------------------
async def log_trade(trade: Dict[str, Any]) -> None:
    """Record a trade dictionary, e.g. spot/futures arbitrage result.

    Examples
    --------
    >>> trade = {"spot_symbol": "BTC/USDT", "futures_symbol": "BTCUSDT", "pnl": 5}
    >>> asyncio.run(log_trade(trade))  # doctest: +SKIP
    """
    msg = _maybe_encrypt(json.dumps(trade, ensure_ascii=False))
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_EXECUTOR, _logger.info, msg)


# ---------------------------------------------------------------------------
# 6. encrypt_log
# ---------------------------------------------------------------------------
def encrypt_log(message: str) -> bytes:
    """Encrypt a log message and return raw bytes.

    Examples
    --------
    >>> data = encrypt_log("secret")
    >>> isinstance(data, bytes)
    True
    """
    return _CIPHER.encrypt(message.encode())


# ---------------------------------------------------------------------------
# 7. decrypt_log
# ---------------------------------------------------------------------------
def decrypt_log(token: bytes) -> str:
    """Decrypt bytes previously produced by :func:`encrypt_log`.

    Examples
    --------
    >>> decrypt_log(encrypt_log("secret"))
    'secret'
    """
    try:
        return _CIPHER.decrypt(token).decode()
    except (InvalidToken, ValueError) as exc:
        _logger.error("Decryption failed for token: %s", exc)
        try:
            asyncio.get_running_loop().create_task(
                notify("Decryption error", str(exc))
            )
        except RuntimeError:
            pass
        return ""


# ---------------------------------------------------------------------------
# 8. async archive_logs
# ---------------------------------------------------------------------------
async def archive_logs() -> None:
    """Archive the current log file into ``logs_archive`` with a timestamp."""
    if not _LOG_FILE.exists():
        return
    if not _has_disk_space():
        await notify("Archive skipped", "Low disk space")
        return
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    archive_file = _ARCHIVE_DIR / f"bot.log.{timestamp}.bak"
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(_EXECUTOR, shutil.copy2, _LOG_FILE, archive_file)
    except Exception as exc:  # pragma: no cover - disk errors
        await notify("Archive failed", str(exc))
    else:
        await log_info(f"Log file successfully archived to {archive_file}")
        await loop.run_in_executor(_EXECUTOR, _cleanup_old_archives)


# ---------------------------------------------------------------------------
# 9. async clear_logs
# ---------------------------------------------------------------------------
async def clear_logs(max_size_mb: int = 100) -> None:
    """Clear the log file if it grows beyond ``max_size_mb``.

    Parameters
    ----------
    max_size_mb:
        Maximum file size in megabytes before truncation.  Defaults to 100.
    """
    if not _LOG_FILE.exists():
        return
    size_mb = _LOG_FILE.stat().st_size / (1024 * 1024)
    if size_mb <= max_size_mb:
        return
    await archive_logs()
    if not _has_disk_space():
        await notify("Clear logs skipped", "Low disk space")
        return
    loop = asyncio.get_running_loop()
    try:
        def _truncate() -> None:
            with _LOG_FILE.open("w", encoding="utf-8"):
                pass
        await loop.run_in_executor(_EXECUTOR, _truncate)
        await log_info("Log file cleared after exceeding size limit")
    except OSError as exc:  # pragma: no cover - disk errors
        await notify("Clear logs failed", str(exc))


# ---------------------------------------------------------------------------
# 10. expose public API
# ---------------------------------------------------------------------------
__all__ = [
    "setup_logger",
    "get_logger",
    "log_info",
    "log_warning",
    "log_error",
    "log_trade",
    "encrypt_log",
    "decrypt_log",
    "archive_logs",
    "clear_logs",
]
