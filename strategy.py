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
from ta.trend import MACD, SMAIndicator
from ta.volatility import BollingerBands

from config import (
    get_pair_thresholds,
    get_spot_slippage,
    get_futures_slippage,
    get_max_basis_risk,
)
from logger import log_error, log_info

try:  # pragma: no cover - optional notifier
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover
    async def notify(message: str, detail: str = "") -> None:
        await log_info(f"Notification: {message} - {detail}")

# Constants for trading costs
_TAKER_FEE = 0.00075  # 0.075%

# Thread pool for indicator calculations
_EXECUTOR = ThreadPoolExecutor(max_workers=4)


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
    spot_slip = get_spot_slippage()
    fut_slip = get_futures_slippage()
    total_cost = (_TAKER_FEE + spot_slip) + (_TAKER_FEE + fut_slip)
    raw_basis = (futures_price - spot_price) / spot_price
    basis = raw_basis - total_cost
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
    """Calculate RSI, MACD, moving average and Bollinger band on ``data``.

    The function also checks for price anomalies (>10% jump) and logs them with
    helpful context.

    Parameters
    ----------
    data:
        DataFrame with ``spot_price`` and ``futures_price`` columns.

    Returns
    -------
    dict
        ``{"rsi": 70.0, "macd": 0.5, "ma": 101.0, "bollinger": 110.0}``
        for example.

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
        idx = spot_jumps[spot_jumps.gt(0.10)].index.union(
            fut_jumps[fut_jumps.gt(0.10)].index
        )[0]
        prev = data.iloc[idx - 1]
        curr = data.iloc[idx]
        price_diff = max(
            abs(curr["spot_price"] - prev["spot_price"]) / prev["spot_price"],
            abs(curr["futures_price"] - prev["futures_price"]) / prev["futures_price"],
        )
        msg = (
            f"Price anomaly {price_diff:.2%}: spot {prev['spot_price']}->"
            f"{curr['spot_price']}, futures {prev['futures_price']}->{curr['futures_price']}"
        )
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(log_error(msg, ValueError()))
            loop.create_task(notify("Price anomaly", msg))
        except RuntimeError:
            pass
        raise ValueError("Price jump >10% detected")

    # Compute indicators concurrently to utilise multiple cores
    rsi_future = _EXECUTOR.submit(
        lambda s=data["spot_price"]: RSIIndicator(s).rsi().iloc[-1]
    )
    macd_future = _EXECUTOR.submit(
        lambda s=data["spot_price"]: MACD(s).macd().iloc[-1]
    )
    ma_future = _EXECUTOR.submit(
        lambda s=data["spot_price"]: SMAIndicator(s, window=5).sma_indicator().iloc[-1]
    )
    boll_future = _EXECUTOR.submit(
        lambda s=data["spot_price"]: BollingerBands(s, window=5).bollinger_hband().iloc[-1]
    )
    rsi = rsi_future.result()
    macd_value = macd_future.result()
    ma_value = ma_future.result()
    boll_value = boll_future.result()
    return {
        "rsi": float(rsi),
        "macd": float(macd_value),
        "ma": float(ma_value),
        "bollinger": float(boll_value),
    }


def manage_risk(spot_symbol: str, futures_symbol: str, basis: float) -> str:
    """Return trading signal while respecting a maximum basis risk."""
    max_risk = get_max_basis_risk()
    if abs(basis) > max_risk:
        try:
            asyncio.get_running_loop().create_task(
                log_info(
                    f"Basis {basis:.6f} exceeds risk {max_risk:.6f} for {spot_symbol}/{futures_symbol}"
                )
            )
        except RuntimeError:
            pass
        return "hold"
    return generate_basis_signal(spot_symbol, futures_symbol, basis)


__all__ = [
    "calculate_basis",
    "generate_basis_signal",
    "apply_indicators",
    "manage_risk",
]
