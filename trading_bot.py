"""High level trading loop orchestrator.

This module is the *captain* of the arbitrage bot.  It glues together the
strategy (our map), the position manager (our cashier) and the risk manager (our
security guard).  The functions are small and friendly so even a new crew member
can steer the ship.  Everything runs asynchronously to keep the Apple Silicon
M4 Max cool while the bot watches the markets all day and night.
"""
from __future__ import annotations

import asyncio
from typing import Dict

import schedule

import config
import logger

# ``strategy.py`` acts as the strategy manager.  We alias it here to make the
# connection explicit and to match the wording in the project description.
import strategy as strategy_manager
import position_manager
import risk_manager
import bybit_api

try:  # pragma: no cover - optional notifier
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover - simple fallback for early tests
    async def notify(message: str, detail: str = "") -> None:
        await logger.log_warning(f"Notification: {message} - {detail}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
async def _trade_pair(
    spot_symbol: str,
    futures_symbol: str,
    *,
    paper: bool,
) -> bool:
    """Process a single spot/futures pair and optionally place a trade.

    Parameters
    ----------
    spot_symbol, futures_symbol:
        Names of the spot and futures instruments.
    paper:
        When ``True`` the function only logs signals without sending orders.

    Returns
    -------
    bool
        ``True`` if a trade was executed (or would be executed in paper mode).
    """

    try:
        data = await bybit_api.get_spot_futures_data(spot_symbol, futures_symbol)
    except Exception as exc:  # pragma: no cover - network failure
        await logger.log_error("API data fetch failed", exc)
        try:
            await notify("API failure", f"{spot_symbol}/{futures_symbol}")
        except Exception:  # pragma: no cover - notification failure
            pass
        return False

    spot_price = data.get("spot", {}).get("bid", 0)
    futures_price = data.get("futures", {}).get("ask", 0)
    try:
        basis = strategy_manager.calculate_basis(
            spot_symbol, futures_symbol, spot_price, futures_price
        )
    except Exception as exc:
        await logger.log_error("Basis calculation error", exc)
        return False

    signal = strategy_manager.generate_basis_signal(
        spot_symbol, futures_symbol, basis
    )
    if signal == "hold":
        return False

    if risk_manager.trading_paused():
        await logger.log_warning("Trading is paused; signal ignored")
        return False

    try:
        balances = await bybit_api.get_balance()
        balance = float(balances.get("USDT", 0.0))
    except Exception:  # pragma: no cover - balance fetch failed
        balance = 0.0

    size = risk_manager.calculate_position_size(balance, 0.01)
    if size <= 0:
        return False

    ok = await risk_manager.enforce_risk_limits(0.0, balance)
    if not ok:
        return False

    if paper:
        await logger.log_info(
            f"Paper {signal} {spot_symbol}/{futures_symbol} amount {size}"
        )
        return True

    await position_manager.open_position(
        spot_symbol, futures_symbol, signal, size, spot_price, futures_price
    )
    return True


async def _trading_iteration(paper: bool = False) -> bool:
    """Run one full pass over all configured trading pairs.

    Returns ``True`` if at least one trade (real or paper) was triggered.
    """
    mapping: Dict[str, str] = config.get_pair_mapping()
    if not mapping:
        await logger.log_warning("No pair mappings configured")
        return False
    tasks = [
        asyncio.create_task(_trade_pair(s, f, paper=paper)) for s, f in mapping.items()
    ]
    results = await asyncio.gather(*tasks)
    if not any(results):
        await logger.log_info("No trading signals")
        return False
    return True


async def _run_loop(*, paper: bool, iterations: int | None) -> None:
    """Helper that schedules :func:`_trading_iteration` every minute.

    ``iterations`` limits how many scheduler ticks are processed.  ``None``
    means run forever.  The first iteration runs immediately so tests can
    execute quickly.
    """

    async def job() -> None:
        await _trading_iteration(paper)

    # Run once before scheduling so that unit tests do not have to wait.
    await job()
    if iterations is not None:
        iterations -= 1
    if iterations is not None and iterations < 0:
        return

    sched = schedule.Scheduler()
    sched.every(1).minutes.do(lambda: asyncio.create_task(job()))

    count = 0
    while iterations is None or count < iterations:
        sched.run_pending()
        await asyncio.sleep(1)
        count += 1


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
async def initialize_bot() -> None:
    """Load configuration and connect to Bybit's API."""
    cfg = config.load_config()
    api_key = cfg.get("API_KEY", "")
    api_secret = cfg.get("API_SECRET", "")
    await bybit_api.connect_api(api_key, api_secret)
    await logger.log_info("Trading bot initialised")


async def run_trading_loop(*, iterations: int | None = None) -> None:
    """Run the real trading loop indefinitely or for ``iterations`` cycles."""
    await _run_loop(paper=False, iterations=iterations)


async def run_paper_trading(*, iterations: int | None = None) -> None:
    """Run the trading loop in paper mode for testing purposes."""
    await _run_loop(paper=True, iterations=iterations)


__all__ = [
    "initialize_bot",
    "run_trading_loop",
    "run_paper_trading",
    "_trading_iteration",
]
