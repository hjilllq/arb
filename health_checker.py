"""System health checker for the arbitrage bot.

This module acts like a tiny doctor that visits every part of the bot and
makes sure each organ is still beating.  Checks are lightweight and use
asynchronous networking so our Apple Silicon M4 Max can stay cool while
running 24/7.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict

import aiohttp
import schedule

from logger import log_error, log_info, log_warning
from notification_manager import notify
import database
import ml_predictor

# ---------------------------------------------------------------------------
# URLs and paths used for the health checks
# ---------------------------------------------------------------------------
_API_URL = "https://api.bybit.com/v5/public/time"
_INTERNET_URL = "https://www.google.com"
_WS_URL = "wss://stream.bybit.com/v5/public/quote"
_GUI_SENTINEL = Path("gui.lock")  # GUI writes this file while active


async def check_api_health() -> bool:
    """Return ``True`` if the public Bybit API responds."""
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(_API_URL) as resp:
                if resp.status == 200:
                    return True
                await log_warning(f"API health returned status {resp.status}")
                await notify("API health warning", f"status {resp.status}")
                return False
    except aiohttp.ClientError as exc:
        await log_error("API health check failed", exc)
        await notify("API health check failed", str(exc))
        return False


def check_db_health() -> bool:
    """Ensure the SQLite database is reachable."""
    try:
        asyncio.run(database.connect_db())
        asyncio.run(database.close_db())
        return True
    except Exception as exc:  # pragma: no cover - rare failure
        try:
            asyncio.run(log_error("Database health check failed", exc))
            asyncio.run(notify("Database health check failed", str(exc)))
        except Exception:
            pass
        return False


async def check_internet_health() -> bool:
    """Return ``True`` if the bot can reach the broader internet."""
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(_INTERNET_URL) as resp:
                return resp.status == 200
    except aiohttp.ClientError as exc:
        await log_error("Internet check failed", exc)
        await notify("Internet check failed", str(exc))
        return False


async def check_websocket_health() -> bool:
    """Perform a ping/pong against Bybit's public WebSocket."""
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(_WS_URL, heartbeat=5) as ws:
                await ws.ping()
                msg = await ws.receive(timeout=5)
                return msg.type == aiohttp.WSMsgType.PONG
    except Exception as exc:
        await log_error("WebSocket health check failed", exc)
        await notify("WebSocket health check failed", str(exc))
        return False


def check_gui_health() -> bool:
    """Check whether a simple GUI sentinel file exists."""
    ok = _GUI_SENTINEL.exists()
    if not ok:
        try:
            asyncio.run(log_warning("GUI inactive"))
            asyncio.run(notify("GUI inactive"))
        except Exception:
            pass
    return ok


def check_model_health() -> bool:
    """Verify that the ML model file exists."""
    try:
        model_path = getattr(ml_predictor, "_MODEL_PATH", None)
        ok = bool(model_path and Path(model_path).exists())
        if not ok:
            asyncio.run(log_warning("ML model missing"))
            asyncio.run(notify("ML model missing"))
        return ok
    except Exception as exc:  # pragma: no cover - import failures
        try:
            asyncio.run(log_error("Model health check failed", exc))
            asyncio.run(notify("Model health check failed", str(exc)))
        except Exception:
            pass
        return False


async def generate_health_report() -> Dict[str, bool]:
    """Run all checks and return a dictionary with their results."""
    api_task = asyncio.create_task(check_api_health())
    net_task = asyncio.create_task(check_internet_health())
    ws_task = asyncio.create_task(check_websocket_health())
    db_task = asyncio.to_thread(check_db_health)
    gui_ok = check_gui_health()
    model_ok = check_model_health()
    results = {
        "api": await api_task,
        "internet": await net_task,
        "websocket": await ws_task,
        "db": await db_task,
        "gui": gui_ok,
        "model": model_ok,
    }
    await log_info(f"Health report: {results}")
    return results


async def schedule_health_checks() -> None:
    """Schedule periodic health checks every five minutes."""
    schedule.every(5).minutes.do(lambda: asyncio.create_task(generate_health_report()))
    while True:  # pragma: no cover - infinite loop for runtime use
        schedule.run_pending()
        await asyncio.sleep(1)


__all__ = [
    "check_api_health",
    "check_db_health",
    "check_internet_health",
    "check_websocket_health",
    "check_gui_health",
    "check_model_health",
    "generate_health_report",
    "schedule_health_checks",
]
