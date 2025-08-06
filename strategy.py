"""Trading strategy utilities for spot/futures basis arbitrage.

This module decides *when* to trade.  It keeps the math simple and the
comments friendly so even a new team member can follow the logic.  The
functions below run happily on Apple Silicon and avoid blocking the event
loop by pushing heavier calculations into a thread pool.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD

from config import get_pair_thresholds
from logger import log_error, log_info

try:  # pragma: no cover - optional notifier
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover
    async def notify(message: str, detail: str = "") -> None:
        await log_info(f"Notification: {message} - {detail}")

# Constants for trading costs
_TAKER_FEE = 0.00075  # 0.075%
_SLIPPAGE = 0.001     # 0.1%
# Both legs incur cost
_TOTAL_COST = 2 * (_TAKER_FEE + _SLIPPAGE)

# Thread pool for indicator calculations
_EXECUTOR = ThreadPoolExecutor(max_workers=2)


def calculate_basis(
    spot_symbol: str,
    futures_symbol: str,
    spot_price: float,
    futures_price: float,
) -> float:
    """Return the net basis between futures and spot prices.

    Basis is normalised by spot price and reduced by estimated trading
    costs (taker fee + slippage on both legs).

    Examples
    --------
    >>> calculate_basis('BTC/USDT', 'BTCUSDT', 59990, 60000)  # doctest: +ELLIPSIS
    -0.0033...
    """
    if spot_price <= 0 or futures_price <= 0:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(log_error("Non-positive price", ValueError()))
            loop.create_task(notify("Invalid price", f"{spot_symbol}/{futures_symbol}"))
        except RuntimeError:
            pass
        raise ValueError("Prices must be positive")
    raw_basis = (futures_price - spot_price) / spot_price
    basis = raw_basis - _TOTAL_COST
    try:
        asyncio.get_running_loop().create_task(
            log_info(
                f"Basis for {spot_symbol}/{futures_symbol}: {basis:.6f}"
            )
        )
    except RuntimeError:
        pass
    return basis


def generate_basis_signal(
    spot_symbol: str,
    futures_symbol: str,
    basis: float,
) -> str:
    """Generate ``buy``, ``sell`` or ``hold`` based on basis thresholds.

    Thresholds are fetched from :mod:`config` using the spot symbol.
    """
    thresholds = get_pair_thresholds(spot_symbol)
    open_th = thresholds.get("open", 0.0)
    close_th = thresholds.get("close", 0.0)
    if basis > open_th:
        signal = "buy"
    elif basis < -abs(close_th):
        signal = "sell"
    else:
        signal = "hold"
    try:
        asyncio.get_running_loop().create_task(
            log_info(
                f"Signal for {spot_symbol}/{futures_symbol} with basis {basis:.6f}: {signal}"
            )
        )
    except RuntimeError:
        pass
    return signal


def apply_indicators(data: pd.DataFrame) -> Dict[str, float]:
    """Calculate RSI and MACD on ``data`` while checking for price anomalies.

    Parameters
    ----------
    data:
        DataFrame with ``spot_price`` and ``futures_price`` columns.

    Returns
    -------
    dict
        ``{"rsi": 70.0, "macd": 0.5}`` for example.

    Raises
    ------
    ValueError
        If a price jump greater than 10% is detected between consecutive rows.
    """
    if data.empty:
        raise ValueError("DataFrame is empty")

    # Detect anomalous jumps
    spot_jumps = data["spot_price"].pct_change().abs().dropna()
    fut_jumps = data["futures_price"].pct_change().abs().dropna()
    if spot_jumps.gt(0.10).any() or fut_jumps.gt(0.10).any():
        msg = "Price jump >10% detected"
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(log_error(msg, ValueError()))
            loop.create_task(notify(msg))
        except RuntimeError:
            pass
        raise ValueError(msg)

    # Compute indicators concurrently to utilise multiple cores
    with _EXECUTOR as executor:
        rsi_future = executor.submit(
            lambda s=data["spot_price"]: RSIIndicator(s).rsi().iloc[-1]
        )
        macd_future = executor.submit(
            lambda s=data["spot_price"]: MACD(s).macd().iloc[-1]
        )
        rsi = rsi_future.result()
        macd_value = macd_future.result()
    return {"rsi": float(rsi), "macd": float(macd_value)}


__all__ = [
    "calculate_basis",
    "generate_basis_signal",
    "apply_indicators",
]
