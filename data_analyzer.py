"""Data analysis helpers for the arbitrage bot.

This module behaves like a curious scientist who looks at price rows from the
ledger, cleans them and computes simple indicators. The functions are kept
small and friendly so the Apple Silicon M4 Max can work day and night without
breaking a sweat. Heavy calculations run in a tiny thread pool using
``concurrent.futures`` so the event loop stays responsive.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import ta

import config
import database  # noqa: F401  # pragma: no cover - used by callers
from logger import log_error, log_info, log_warning

try:  # pragma: no cover - optional notifier
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover - simple fallback
    async def notify(message: str, detail: str = "") -> None:
        await log_warning(f"Notification: {message} - {detail}")

# Thread pool for parallel indicator calculations.  Two workers are plenty for
# RSI and MACD computations and keep power usage low.
_EXECUTOR = ThreadPoolExecutor(max_workers=2)


# ---------------------------------------------------------------------------
# 1. preprocess_data
# ---------------------------------------------------------------------------
def preprocess_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Clean raw rows and drop anomalies.

    Parameters
    ----------
    data:
        List of dictionaries as returned by :mod:`database` queries.

    Returns
    -------
    pandas.DataFrame
        Cleaned frame without missing values or extreme price jumps.
    """
    df = pd.DataFrame(data)
    if df.empty:
        return df

    required = {"spot_symbol", "futures_symbol", "spot_price", "futures_price"}
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing columns: {', '.join(sorted(missing))}"
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(log_error(msg, ValueError()))
            loop.create_task(notify("Preprocess error", msg))
        except RuntimeError:  # no running loop
            asyncio.run(log_error(msg, ValueError()))
            try:
                asyncio.run(notify("Preprocess error", msg))
            except RuntimeError:
                pass
        return pd.DataFrame()

    df = df.dropna(subset=list(required))
    df = df[(df["spot_price"] > 0) & (df["futures_price"] > 0)]

    # Ensure the spot/futures pair matches the configuration mapping.
    pair_map = config.get_pair_mapping()
    df = df[df.apply(lambda r: pair_map.get(r["spot_symbol"]) == r["futures_symbol"], axis=1)]

    if df.empty:
        return df

    spread = (df["futures_price"] - df["spot_price"]) / df["spot_price"]
    anomalies = df[spread.abs() > 0.10]
    if not anomalies.empty:
        msg = f"Anomalous price jumps removed: {len(anomalies)} rows"
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(log_warning(msg))
            loop.create_task(notify("Price anomaly", f"{len(anomalies)} rows removed"))
        except RuntimeError:
            asyncio.run(log_warning(msg))
            try:
                asyncio.run(notify("Price anomaly", f"{len(anomalies)} rows removed"))
            except RuntimeError:
                pass
        df = df.drop(anomalies.index)

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 2. calculate_indicators
# ---------------------------------------------------------------------------
def _spot_indicators(df: pd.DataFrame) -> Dict[str, pd.Series]:
    """Return RSI and MACD for spot prices."""
    return {
        "spot_rsi": ta.momentum.rsi(df["spot_price"], window=14),
        "spot_macd": ta.trend.macd_diff(df["spot_price"]),
    }


def _futures_indicators(df: pd.DataFrame) -> Dict[str, pd.Series]:
    """Return RSI and MACD for futures prices."""
    return {
        "futures_rsi": ta.momentum.rsi(df["futures_price"], window=14),
        "futures_macd": ta.trend.macd_diff(df["futures_price"]),
    }


def calculate_indicators(data: pd.DataFrame) -> pd.DataFrame:
    """Append RSI and MACD columns for both spot and futures prices.

    Heavy calculations run in parallel via :mod:`concurrent.futures`.
    """
    if data.empty:
        return data
    if not {"spot_price", "futures_price"}.issubset(data.columns):
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(log_error("Indicator columns missing", ValueError()))
        except RuntimeError:
            asyncio.run(log_error("Indicator columns missing", ValueError()))
        return data

    jobs = [_EXECUTOR.submit(fn, data.copy()) for fn in (_spot_indicators, _futures_indicators)]
    for job in as_completed(jobs):
        try:
            res = job.result()
        except Exception as exc:  # pragma: no cover - rare
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(log_error("Indicator calc failed", exc))
                loop.create_task(notify("Indicator failure", str(exc)))
            except RuntimeError:
                asyncio.run(log_error("Indicator calc failed", exc))
                try:
                    asyncio.run(notify("Indicator failure", str(exc)))
                except RuntimeError:
                    pass
            continue
        for col, series in res.items():
            data[col] = series
    return data


# ---------------------------------------------------------------------------
# 3. calculate_volatility
# ---------------------------------------------------------------------------
def calculate_volatility(data: pd.DataFrame) -> float:
    """Return standard deviation of the basis as a fraction.

    The basis is ``(futures - spot) / spot``.  A return value of ``0.25`` means
    25 % volatility.
    """
    if data.empty or not {"spot_price", "futures_price"}.issubset(data.columns):
        return 0.0
    spread = (data["futures_price"] - data["spot_price"]) / data["spot_price"]
    return float(np.std(spread.values, ddof=0))
