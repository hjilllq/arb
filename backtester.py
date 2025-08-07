"""Simple backtesting utilities for the basis arbitrage strategy.

The functions here replay historical spot and futures prices pulled from
:mod:`database` and feed them through the :mod:`strategy` module.  Results are
summarised as profit, trade count and Sharpe ratio.  Calculations are kept light
and offloaded to a tiny thread pool so the Apple Silicon M4 Max can run 24/7
without burning extra energy.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import numpy as np
import pandas as pd

from logger import log_error, log_info, log_warning
import strategy
from config import get_spot_slippage, get_futures_slippage

try:  # pragma: no cover - optional notifier
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover
    async def notify(message: str, detail: str = "") -> None:
        await log_warning(f"Notification: {message} - {detail}")

# Trading costs per leg
_TAKER_FEE = 0.00075
_EXECUTOR = ThreadPoolExecutor(max_workers=4)


# ---------------------------------------------------------------------------
# 1. simulate_slippage
# ---------------------------------------------------------------------------
def simulate_slippage(data: pd.DataFrame) -> pd.DataFrame:
    """Return ``data`` with execution price columns including costs.

    Prices are adjusted for slippage and taker fees.  Spot trades pay the
    combined cost, futures trades receive slightly less when selling and pay
    extra when buying.
    """
    if data.empty:
        asyncio.run(log_error("No data for slippage simulation", ValueError()))
        asyncio.run(notify("Backtest error", "empty data"))
        raise ValueError("DataFrame is empty")

    required = {"spot_price", "futures_price"}
    if missing := required - set(data.columns):
        msg = f"Missing columns: {', '.join(sorted(missing))}"
        asyncio.run(log_error(msg, ValueError()))
        asyncio.run(notify("Backtest error", msg))
        raise ValueError(msg)

    df = data.copy()
    spot_cost = _TAKER_FEE + get_spot_slippage()
    fut_cost = _TAKER_FEE + get_futures_slippage()

    df["spot_exec"] = df["spot_price"] * (1 + spot_cost)
    df["futures_exec"] = df["futures_price"] * (1 - fut_cost)

    # Detect >10% jumps which may skew results
    pct = df["spot_price"].pct_change().abs().fillna(0)
    anomalies = df[pct > 0.10]
    if not anomalies.empty:
        msg = f"Removed {len(anomalies)} anomalous rows"
        asyncio.run(log_warning(msg))
        df = df.drop(anomalies.index)
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 2. run_backtest
# ---------------------------------------------------------------------------
def run_backtest(data: pd.DataFrame, spot_symbol: str, futures_symbol: str) -> Dict[str, float]:
    """Replay ``data`` and return summary metrics.

    The function enters the market when :func:`strategy.generate_basis_signal`
    returns ``buy`` and exits when it returns ``sell``.
    """
    if data.empty:
        asyncio.run(log_error("No data for backtest", ValueError()))
        asyncio.run(notify("Backtest error", "empty data"))
        raise ValueError("DataFrame is empty")

    df = simulate_slippage(data)
    if df.empty:
        return {"profit": 0.0, "trades": 0, "sharpe_ratio": 0.0}

    # Pre-calculate basis in a thread pool for speed
    def _basis(row: pd.Series) -> float:
        return strategy.calculate_basis(spot_symbol, futures_symbol, row.spot_price, row.futures_price)

    bases: List[float] = list(_EXECUTOR.map(_basis, [row for _, row in df.iterrows()]))

    profits: List[float] = []
    position_open = False
    entry_spot = entry_fut = 0.0

    for i, basis in enumerate(bases):
        signal = strategy.generate_basis_signal(spot_symbol, futures_symbol, basis)
        row = df.iloc[i]
        if signal == "buy" and not position_open:
            entry_spot = row["spot_exec"]
            entry_fut = row["futures_exec"]
            position_open = True
        elif signal == "sell" and position_open:
            exit_spot = row["spot_exec"]  # selling spot: execution cost already applied
            exit_fut = row["futures_exec"]  # buying back futures
            profit = (exit_spot - entry_spot) + (entry_fut - exit_fut)
            profits.append(profit)
            position_open = False

    total_profit = float(np.sum(profits))
    trades = len(profits)
    if profits:
        avg = float(np.mean(profits))
        std = float(np.std(profits, ddof=1)) if trades > 1 else 0.0
        sharpe = (avg / std) * np.sqrt(trades) if std else 0.0
    else:
        sharpe = 0.0

    try:
        asyncio.get_running_loop().create_task(
            log_info(
                f"Backtest {spot_symbol}/{futures_symbol}: profit={total_profit:.4f}, trades={trades}, sharpe={sharpe:.2f}"
            )
        )
    except RuntimeError:
        pass

    return {"profit": total_profit, "trades": trades, "sharpe_ratio": sharpe}
