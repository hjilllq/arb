"""Risk management helpers for the arbitrage bot.

This module watches over our trading like a careful security guard.  It keeps
track of losses and sudden price swings so the rest of the bot can trade with
confidence.  The code favours clear comments, tiny helper functions and light
asynchronous operations so the Apple Silicon M4 Max can stay cool while the bot
runs all day.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import List

import numpy as np
import pandas as pd

import config
import logger

# These modules are imported for side effects and future extension.  Tests may
# monkeypatch them so we keep the imports at module level.
import bybit_api  # noqa: F401  # pragma: no cover - used by callers
import database   # noqa: F401  # pragma: no cover - used by callers

try:  # pragma: no cover - optional notifier
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover - simple fallback
    async def notify(message: str, detail: str = "") -> None:
        await logger.log_warning(f"Notification: {message} - {detail}")


# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------
_TRADING_PAUSED = False
_LAST_PNL_CHECK: datetime | None = None
_LAST_PNL_VALUE: float = 0.0


# ---------------------------------------------------------------------------
# 1. calculate_position_size
# ---------------------------------------------------------------------------
def calculate_position_size(balance: float, risk: float) -> float:
    """Return the amount to allocate for a single trade.

    Parameters
    ----------
    balance:
        Current account balance.
    risk:
        Fraction of the balance to risk (e.g. ``0.01`` for 1 %).
    """
    if balance <= 0 or risk <= 0:
        return 0.0
    return balance * risk


# ---------------------------------------------------------------------------
# 2. async check_volatility
# ---------------------------------------------------------------------------
async def check_volatility(data: pd.DataFrame) -> float:
    """Calculate volatility of the basis between spot and futures prices.

    The basis is ``(futures - spot) / spot``.  We compute its standard
    deviation, which is returned as a fraction (``0.25`` means 25 %).
    """

    def _calc(df: pd.DataFrame) -> float:
        if df.empty or "spot_price" not in df or "futures_price" not in df:
            return 0.0
        spread = (df["futures_price"] - df["spot_price"]) / df["spot_price"]
        return float(np.std(spread.values, ddof=0))

    return await asyncio.to_thread(_calc, data)


# ---------------------------------------------------------------------------
# 3. async pause_trading_on_high_volatility
# ---------------------------------------------------------------------------
async def pause_trading_on_high_volatility(volatility: float) -> bool:
    """Pause trading when volatility exceeds the configured limit."""
    global _TRADING_PAUSED
    limit = float(config.CONFIG.get("MAX_VOLATILITY_PAUSE", 0.3))
    if volatility > limit:
        _TRADING_PAUSED = True
        await logger.log_warning(
            f"Volatility {volatility:.2%} above limit {limit:.2%}; trading paused"
        )
        try:
            await notify("Trading paused", f"Volatility {volatility:.2%}")
        except Exception:  # pragma: no cover - notification failure
            pass
        return True
    return False


# ---------------------------------------------------------------------------
# 4. async enforce_risk_limits
# ---------------------------------------------------------------------------
async def enforce_risk_limits(pnl: float, balance: float) -> bool:
    """Ensure losses stay within daily limits.

    Parameters
    ----------
    pnl:
        Realised profit and loss for the period under review.
    balance:
        Current account balance.
    """
    global _LAST_PNL_CHECK, _LAST_PNL_VALUE

    if balance <= 0:
        return False
    loss_ratio = -pnl / balance if pnl < 0 else 0.0
    max_loss = float(config.CONFIG.get("MAX_DAILY_LOSS", 0.05))

    now = datetime.utcnow()
    if _LAST_PNL_CHECK and now - _LAST_PNL_CHECK < timedelta(hours=1):
        hourly_loss = _LAST_PNL_VALUE + pnl
    else:
        hourly_loss = pnl
        _LAST_PNL_VALUE = 0.0
    _LAST_PNL_CHECK = now
    _LAST_PNL_VALUE = hourly_loss

    if hourly_loss < -0.05 * balance:
        await logger.log_warning(
            f"Hourly loss {hourly_loss/balance:.2%} exceeds 5% anomaly threshold"
        )
        try:
            await notify("Anomalous loss", f"{hourly_loss/balance:.2%}")
        except Exception:
            pass

    if loss_ratio >= max_loss:
        await logger.log_error(
            "Daily loss limit exceeded", RuntimeError(f"{loss_ratio:.2%} loss")
        )
        try:
            await notify("Daily loss limit exceeded", f"{loss_ratio:.2%}")
        except Exception:
            pass
        return False

    return True


# Expose trading state for tests or external inspection.
def trading_paused() -> bool:
    """Return ``True`` if trading is currently paused."""
    return _TRADING_PAUSED
