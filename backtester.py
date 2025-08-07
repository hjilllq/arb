"""Backtesting module for Bybit spot-futures arbitrage strategies.

This module provides utilities to simulate trades on historical data and
produce a simple performance report. It is intentionally minimal and
serves as a starting point for developing more sophisticated backtesting
functionality.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Any, Sequence
from concurrent.futures import ThreadPoolExecutor
import logging


logger = logging.getLogger(__name__)


@dataclass
class Backtester:
    """Run basic backtests for spot-futures arbitrage strategies.

    Attributes:
        initial_capital: Starting capital used for the backtest.
    """

    initial_capital: float = 0.0

    def backtest(
        self,
        historical_data: Iterable[Dict[str, float]],
        strategy: Callable[[Dict[str, float]], str],
    ) -> Dict[str, Any]:
        """Run the strategy on the supplied historical data.

        Args:
            historical_data: Sequence of price snapshots. Each snapshot is a
                dictionary that at minimum contains ``"spot"`` and ``"future"``
                prices.
            strategy: Callable that decides whether to open a position given a
                price snapshot. It should return one of ``"buy"``, ``"sell"`` or
                ``"hold"``.

        Returns:
            Dictionary with aggregated metrics produced by
            :meth:`generate_report`.
        """
        trades: List[Dict[str, float]] = []
        for snapshot in historical_data:
            trade = self.simulate_trade(
                snapshot,
                strategy,
                snapshot.get("slippage", 0.0),
            )
            if trade:
                trades.append(trade)
        return self.generate_report(trades)

    def simulate_trade(
        self,
        snapshot: Dict[str, float],
        strategy: Callable[[Dict[str, float]], str],
        slippage: float = 0.0,
    ) -> Dict[str, float] | None:
        """Simulate a single trade based on the strategy decision.

        Args:
            snapshot: Price information for a single time period.
            strategy: Callable that returns the trade decision.

        Returns:
            Summary of the simulated trade containing the decision and the
            resulting PnL, or ``None`` if no trade was executed.
        """
        decision = strategy(snapshot)
        pnl = 0.0
        if decision == "buy":
            pnl = snapshot["future"] - snapshot["spot"]
        elif decision == "sell":
            pnl = snapshot["spot"] - snapshot["future"]
        else:
            return None
        pnl -= self.calculate_slippage(0.0, slippage)
        return {"decision": decision, "pnl": pnl}

    def generate_report(self, trades: List[Dict[str, float]]) -> Dict[str, float]:
        """Generate a simple report with aggregated trade results.

        Args:
            trades: List of trade dictionaries as produced by
                :meth:`simulate_trade`.

        Returns:
            Dictionary containing the number of trades executed and the total
            profit or loss.
        """
        total_pnl = sum(t["pnl"] for t in trades)
        result = {"trades": len(trades), "total_pnl": total_pnl}
        self.log_results(result)
        return result

    def calculate_slippage(self, expected_price: float, executed_price: float) -> float:
        """Return the absolute slippage between expected and executed price."""
        return executed_price - expected_price

    def run_in_parallel(
        self,
        datasets: Sequence[Iterable[Dict[str, float]]],
        strategy: Callable[[Dict[str, float]], str],
        max_workers: int = 4,
    ) -> List[Dict[str, Any]]:
        """Run multiple backtests concurrently for faster evaluation."""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.backtest, data, strategy) for data in datasets]
            return [f.result() for f in futures]

    def log_results(self, result: Dict[str, Any]) -> None:
        """Log the summary of a backtest run."""
        logger.info(
            "Executed %s trades with total PnL %.4f",
            result.get("trades", 0),
            result.get("total_pnl", 0.0),
        )
