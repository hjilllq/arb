"""Heartbeat utility for the arbitrage bot.

This module acts like a small alarm clock.  It periodically records a
"heartbeat" timestamp and lets operators know if the main program stops
sending those beats.  The checks are asynchronous and light so the Apple
Silicon M4 Max can spend most of its time idle.
"""
from __future__ import annotations

import asyncio
import time
import schedule

# ---------------------------------------------------------------------------
# Optional imports: logger and notification manager
# ---------------------------------------------------------------------------
try:  # pragma: no cover - runtime import
    from logger import get_logger, log_info, log_error  # type: ignore
except Exception:  # pragma: no cover - fallback for very early boot
    import logging

    logging.basicConfig(level=logging.INFO)

    def get_logger(name: str):  # type: ignore
        return logging.getLogger(name)

    async def log_info(message: str) -> None:  # type: ignore
        get_logger(__name__).info(message)

    async def log_error(message: str, error: Exception) -> None:  # type: ignore
        get_logger(__name__).error("%s | %s", message, error)

try:  # pragma: no cover - tiny helper
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover - fallback if manager missing
    async def notify(message: str, detail: str = "") -> None:
        get_logger(__name__).warning("Notification: %s - %s", message, detail)

logger = get_logger(__name__)

# Timestamp of the last heartbeat in seconds since epoch.
_LAST_HEARTBEAT: float | None = None

# Scheduler used to send heartbeats periodically.
_scheduler = schedule.Scheduler()


async def send_heartbeat() -> None:
    """Record the current time as an activity signal.

    The timestamp is stored in memory and an informational log message is
    written.  Called every 60 seconds by the module's scheduler.
    """
    global _LAST_HEARTBEAT
    _LAST_HEARTBEAT = time.time()
    await log_info("Heartbeat sent")


def check_heartbeat() -> bool:
    """Return ``True`` if a heartbeat was seen in the last 120 seconds."""
    if _LAST_HEARTBEAT is None:
        return False
    return (time.time() - _LAST_HEARTBEAT) <= 120


async def alert_no_heartbeat() -> None:
    """Send an alert when no heartbeat was recorded recently."""
    if check_heartbeat():
        return
    err = RuntimeError("No heartbeat within 120 seconds")
    await log_error("Heartbeat missing", err)
    try:
        await notify("Heartbeat missing", "No activity detected")
    except Exception:  # pragma: no cover - notification failure
        pass


async def _run_scheduler() -> None:
    """Run the heartbeat scheduler indefinitely.

    This helper schedules :func:`send_heartbeat` every 60 seconds and checks for
    missing heartbeats once per second.
    """
    _scheduler.every(60).seconds.do(lambda: asyncio.create_task(send_heartbeat()))
    while True:
        _scheduler.run_pending()
        if not check_heartbeat():
            await alert_no_heartbeat()
        await asyncio.sleep(1)


__all__ = ["send_heartbeat", "check_heartbeat", "alert_no_heartbeat", "_run_scheduler"]
