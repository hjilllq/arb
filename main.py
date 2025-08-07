"""Entry point for running the arbitrage trading system.

This module wires together configuration loading, system initialization and
trade loop management.  It provides a thin "TradingSystem" class with helper
methods to start and stop trading.  The design is intentionally lightweight to
serve as a starting point for more sophisticated orchestration.
"""
from __future__ import annotations

import asyncio
from typing import Optional

from config import Config, load_config, validate_config
from exchange_manager import ExchangeManager
from heartbeat import HeartbeatMonitor
from logger import log_event, log_error


class TradingSystem:
    """Main controller responsible for running the trading bot."""

    def __init__(self) -> None:
        self.config: Optional[Config] = None
        self.exchange: Optional[ExchangeManager] = None
        self.heartbeat = HeartbeatMonitor()
        self._running = False

    def check_environment(self) -> Config:
        """Load and validate environment configuration."""
        cfg = load_config()
        validate_config(cfg)
        log_event("Environment validated")
        return cfg

    async def initialize_system(self) -> None:
        """Initialize core components required for trading."""
        self.config = self.check_environment()
        self.exchange = ExchangeManager()
        log_event("System initialization complete")

    async def start_trading(self) -> None:
        """Begin the trading loop.

        The current implementation simply emits heartbeat signals on a fixed
        interval.  Real trading logic should be implemented where indicated.
        """
        if not self.exchange:
            raise RuntimeError("System not initialized")
        self._running = True
        log_event("Trading started")
        try:
            while self._running:
                self.heartbeat.send_heartbeat_signal()
                await asyncio.sleep(self.heartbeat.interval)
        finally:
            log_event("Trading loop exited")

    def stop_trading(self) -> None:
        """Stop the trading loop."""
        self._running = False
        log_event("Trading stopped")

    async def run(self) -> None:
        """Run the trading system until stopped."""
        try:
            await self.initialize_system()
            await self.start_trading()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive programming
            log_error("Unhandled exception in run", exc)
        finally:
            self.stop_trading()
            if self.exchange:
                await self.exchange.close()


if __name__ == "__main__":
    asyncio.run(TradingSystem().run())
