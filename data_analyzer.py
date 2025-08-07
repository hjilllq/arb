"""Utilities for basic market data analysis.

This module implements a few helper functions to compute common
technical indicators that can be used in arbitrage and trading
strategies.
"""
from __future__ import annotations

import logging
from typing import Iterable, List, Sequence, Tuple


logger = logging.getLogger(__name__)


def detect_anomalies(data: Sequence[float], z_thresh: float = 3.0) -> List[int]:
    """Return indices of data points whose z-score exceeds ``z_thresh``.

    Parameters
    ----------
    data:
        Sequence of numeric observations.
    z_thresh:
        Z-score threshold for flagging anomalies. Default is ``3.0``.

    Returns
    -------
    list[int]
        Indices of elements considered anomalies.
    """

    if not data:
        return []
    mean = sum(data) / len(data)
    variance = sum((x - mean) ** 2 for x in data) / len(data)
    std = variance ** 0.5
    if std == 0:
        return []
    return [i for i, x in enumerate(data) if abs((x - mean) / std) > z_thresh]


def calculate_rsi(prices: Sequence[float], period: int = 14) -> List[float]:
    """Compute the Relative Strength Index (RSI).

    Parameters
    ----------
    prices:
        Historical closing prices.
    period:
        Number of periods to use for the RSI calculation. Default ``14``.

    Returns
    -------
    list[float]
        RSI values; the first ``period`` values will be ``0`` as there is
        insufficient data to compute the indicator.
    """

    if len(prices) < 2:
        return [0.0] * len(prices)

    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains = [max(delta, 0.0) for delta in deltas]
    losses = [max(-delta, 0.0) for delta in deltas]

    avg_gain = sum(gains[: period]) / period
    avg_loss = sum(losses[: period]) / period

    rsi: List[float] = [0.0] * len(prices)
    for i in range(period, len(prices)):
        if i > period:
            avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period
        if avg_loss == 0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100 - (100 / (1 + rs))
    return rsi


def calculate_macd(
    prices: Sequence[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> Tuple[List[float], List[float], List[float]]:
    """Compute the Moving Average Convergence Divergence (MACD).

    Parameters
    ----------
    prices:
        Historical closing prices.
    fast_period:
        Number of periods for the fast EMA. Default ``12``.
    slow_period:
        Number of periods for the slow EMA. Default ``26``.
    signal_period:
        Number of periods for the signal line EMA. Default ``9``.

    Returns
    -------
    tuple[list[float], list[float], list[float]]
        MACD line, signal line and histogram values.
    """

    def ema(values: Iterable[float], period: int) -> List[float]:
        values = list(values)
        if not values:
            return []
        k = 2 / (period + 1)
        ema_vals: List[float] = [values[0]]
        for price in values[1:]:
            ema_vals.append(price * k + ema_vals[-1] * (1 - k))
        return ema_vals

    fast_ema = ema(prices, fast_period)
    slow_ema = ema(prices, slow_period)
    length = min(len(fast_ema), len(slow_ema))
    macd_line = [fast_ema[i] - slow_ema[i] for i in range(length)]
    signal_line = ema(macd_line, signal_period)
    hist_length = min(len(macd_line), len(signal_line))
    histogram = [macd_line[i] - signal_line[i] for i in range(hist_length)]
    return macd_line, signal_line, histogram


def filter_data(prices: Sequence[float], z_thresh: float = 3.0) -> List[float]:
    """Remove anomalous values from ``prices`` based on ``z_thresh``."""

    anomalies = set(detect_anomalies(prices, z_thresh))
    return [p for i, p in enumerate(prices) if i not in anomalies]


def generate_signal(prices: Sequence[float], rsi_period: int = 14) -> List[int]:
    """Generate trade signals using RSI and MACD indicators.

    A signal of ``1`` indicates a potential long entry, ``-1`` a short
    entry and ``0`` no action.
    """

    rsi = calculate_rsi(prices, rsi_period)
    macd_line, signal_line, _ = calculate_macd(prices)
    signals: List[int] = []
    for i in range(len(prices)):
        sig = 0
        if i < len(macd_line) and i < len(signal_line):
            if macd_line[i] > signal_line[i] and rsi[i] < 30:
                sig = 1
            elif macd_line[i] < signal_line[i] and rsi[i] > 70:
                sig = -1
        signals.append(sig)
    return signals


def log_data_analysis(results: dict) -> None:
    """Log key metrics from data analysis."""

    for key, value in results.items():
        logger.info("%s: %s", key, value)


def backtest_analysis(prices: Sequence[float]) -> dict:
    """Run indicator calculations and signal generation for backtesting."""

    filtered = filter_data(prices)
    rsi = calculate_rsi(filtered)
    macd_line, signal_line, histogram = calculate_macd(filtered)
    signals = generate_signal(filtered)
    summary = {
        "anomalies_removed": len(prices) - len(filtered),
        "signals_generated": sum(1 for s in signals if s),
    }
    log_data_analysis(summary)
    return {
        "filtered_prices": filtered,
        "rsi": rsi,
        "macd_line": macd_line,
        "signal_line": signal_line,
        "histogram": histogram,
        "signals": signals,
    }

