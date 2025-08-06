"""Notification utilities for the arbitrage bot.

This module acts like a small phone operator.  It delivers short messages
about trades, errors and daily performance to operators via Telegram and
email.  Functions are lightweight and aim to keep the Apple Silicon M4 Max cool
by using asynchronous I/O wherever possible.
"""
from __future__ import annotations

import asyncio
import json
import smtplib
from email.message import EmailMessage
from typing import Any, Dict, Optional

import aiohttp

# ---------------------------------------------------------------------------
# Optional helpers: logger and config are imported lazily to avoid cycles.
# ---------------------------------------------------------------------------
try:  # pragma: no cover - used only when logger module exists
    from logger import get_logger  # type: ignore
except Exception:  # pragma: no cover - fallback for very early boot
    import logging

    logging.basicConfig(level=logging.INFO)

    def get_logger(name: str):  # type: ignore
        """Return a basic logger if the real one is missing."""
        return logging.getLogger(name)

logger = get_logger(__name__)

# Global state filled by setup_notifications()
_session: Optional[aiohttp.ClientSession] = None
_tg_token: str | None = None
_tg_chat: str | None = None
_email_cfg: Dict[str, str] = {}
_is_setup = False


async def setup_notifications() -> None:
    """Load configuration and prepare network clients.

    Reads tokens and addresses from :mod:`config` and stores them in global
    variables.  The function can be called multiple times; subsequent calls
    simply return early.
    """
    global _is_setup, _session, _tg_token, _tg_chat, _email_cfg
    if _is_setup:
        return

    try:
        from config import load_config  # imported lazily to avoid circular deps
    except Exception as exc:  # pragma: no cover - config missing
        logger.error("Config import failed: %s", exc)
        return

    cfg = load_config()
    _tg_token = cfg.get("TELEGRAM_TOKEN")
    _tg_chat = cfg.get("TELEGRAM_CHAT_ID")

    _email_cfg = {
        "host": cfg.get("EMAIL_HOST", ""),
        "port": cfg.get("EMAIL_PORT", ""),
        "user": cfg.get("EMAIL_USER", ""),
        "password": cfg.get("EMAIL_PASS", ""),
        "sender": cfg.get("EMAIL_FROM", ""),
        "recipient": cfg.get("EMAIL_TO", ""),
    }

    _session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    _is_setup = True
    logger.info("Notification manager setup complete")


async def _send_telegram(text: str) -> None:
    """Send ``text`` to Telegram if credentials exist."""
    if not _tg_token or not _tg_chat:
        return
    if _session is None:
        await setup_notifications()
        if _session is None:  # still none if setup failed
            return
    url = f"https://api.telegram.org/bot{_tg_token}/sendMessage"
    payload = {"chat_id": _tg_chat, "text": text}
    try:
        async with _session.post(url, json=payload) as resp:  # pragma: no cover
            await resp.text()
    except aiohttp.ClientError as exc:
        logger.error("Telegram send failed: %s", exc)


async def _send_email(subject: str, body: str) -> None:
    """Send ``body`` as an email with ``subject`` if SMTP settings exist."""
    if not _email_cfg.get("host") or not _email_cfg.get("recipient") or not _email_cfg.get("sender"):
        return

    def _send() -> None:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = _email_cfg["sender"]
        msg["To"] = _email_cfg["recipient"]
        msg.set_content(body)
        with smtplib.SMTP(_email_cfg["host"], int(_email_cfg.get("port", 25)), timeout=10) as smtp:
            user = _email_cfg.get("user")
            password = _email_cfg.get("password")
            if user and password:
                smtp.login(user, password)
            smtp.send_message(msg)

    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _send)
    except Exception as exc:  # pragma: no cover - network/SMTP errors
        logger.error("Email send failed: %s", exc)


async def notify(message: str, detail: str = "") -> None:
    """Send a short notification via all available channels."""
    if not _is_setup:
        await setup_notifications()
    text = f"{message}: {detail}" if detail else message
    await asyncio.gather(
        _send_telegram(text),
        _send_email(message, text),
        return_exceptions=True,
    )


async def send_trade_alert(trade: Dict[str, Any]) -> None:
    """Notify about a completed trade."""
    text = (
        f"Trade {trade.get('spot_symbol')} / {trade.get('futures_symbol')} "
        f"PnL {trade.get('pnl')}"
    )
    await notify("Trade alert", text)


async def send_error_alert(error: Exception) -> None:
    """Send an error notification."""
    await notify("Error", str(error))


async def send_performance_report(report: Dict[str, Any]) -> None:
    """Send a performance summary such as daily profit."""
    await notify("Performance", json.dumps(report))


__all__ = [
    "setup_notifications",
    "send_trade_alert",
    "send_error_alert",
    "send_performance_report",
    "notify",
]
