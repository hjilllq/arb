"""System resource monitor for the arbitrage bot.

This module acts like a sleepy night guard. Every few minutes it peeks at the
CPU, memory and disk usage, and if something looks scary it rings the bell via
:mod:`notification_manager`.  The checks are asynchronous and lightweight so our
Apple Silicon M4 Max can nap most of the time.
"""
from __future__ import annotations

import asyncio
from typing import Optional

import psutil
import schedule

try:  # pragma: no cover - tiny helper, tested indirectly
    from logger import get_logger, log_warning, log_error  # type: ignore
except Exception:  # pragma: no cover - fallback for very early boot
    import logging

    logging.basicConfig(level=logging.INFO)

    def get_logger(name: str):  # type: ignore
        return logging.getLogger(name)

    async def log_warning(message: str) -> None:  # type: ignore
        get_logger(__name__).warning(message)

    async def log_error(message: str, error: Exception) -> None:  # type: ignore
        get_logger(__name__).error("%s | %s", message, error)

try:  # pragma: no cover - tiny helper
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover - fallback
    async def notify(message: str, detail: str = "") -> None:
        get_logger(__name__).warning("Notification: %s - %s", message, detail)

try:  # pragma: no cover - used lazily
    from config import load_config  # type: ignore
except Exception:  # pragma: no cover - config not ready
    load_config = lambda: {}

logger = get_logger(__name__)
_CONFIG = load_config()
_ALERT_THRESHOLD = float(_CONFIG.get("RESOURCE_ALERT_THRESHOLD", 80.0))


async def check_cpu_usage() -> float:
    """Return the current CPU load in percent.

    If metrics are unavailable the function logs an error and returns ``-1``.
    """
    try:
        return await asyncio.to_thread(psutil.cpu_percent, None)
    except Exception as exc:  # pragma: no cover - depends on psutil availability
        await log_error("CPU check failed", exc)
        return -1.0


async def check_memory_usage() -> float:
    """Return the memory usage percentage or ``-1`` on failure."""
    try:
        mem = await asyncio.to_thread(psutil.virtual_memory)
        return float(mem.percent)
    except Exception as exc:  # pragma: no cover
        await log_error("Memory check failed", exc)
        return -1.0


async def check_disk_space(path: str = "./") -> float:
    """Return disk usage percentage for ``path`` or ``-1`` on failure."""
    try:
        disk = await asyncio.to_thread(psutil.disk_usage, path)
        return float(disk.percent)
    except Exception as exc:  # pragma: no cover
        await log_error("Disk check failed", exc)
        return -1.0


async def alert_high_usage(metric: str, value: float) -> None:
    """Notify operators that ``metric`` crossed the alert threshold."""
    await log_warning(f"High {metric} usage: {value:.1f}%")
    try:
        await notify("High resource usage", f"{metric} at {value:.1f}%")
    except Exception:  # pragma: no cover - notification failures
        pass


async def _run_checks() -> None:
    """Gather metrics and alert if any exceeds the threshold."""
    cpu, mem, disk = await asyncio.gather(
        check_cpu_usage(),
        check_memory_usage(),
        check_disk_space(),
    )
    metrics = {"cpu": cpu, "memory": mem, "disk": disk}
    for name, value in metrics.items():
        if value >= _ALERT_THRESHOLD > 0:
            await alert_high_usage(name, value)


async def monitor_system_health(run_once: bool = False) -> None:
    """Continuously watch system health or run a single check.

    When ``run_once`` is ``True`` the function performs one immediate check.
    Otherwise it schedules :func:`_run_checks` every five minutes using the
    :mod:`schedule` library and runs indefinitely.
    """
    schedule.clear()
    schedule.every(5).minutes.do(lambda: asyncio.create_task(_run_checks()))
    if run_once:
        schedule.run_all(delay_seconds=0)
        return
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)


__all__ = [
    "check_cpu_usage",
    "check_memory_usage",
    "check_disk_space",
    "alert_high_usage",
    "monitor_system_health",
]
