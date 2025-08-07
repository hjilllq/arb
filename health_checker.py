"""Simple system health monitoring utilities.

This module provides lightweight checks to verify that core components of the
arbitrage system are operational. It focuses on verifying connectivity to the
Bybit API and the underlying trade database while offering basic alerting and
logging helpers.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict

from bybit_api import BybitAPI
from database import TradeDatabase


logger = logging.getLogger(__name__)


@dataclass
class HealthChecker:
    """Run health checks for external dependencies."""

    db_path: str = "trades.db"

    async def check_system(self) -> Dict[str, bool]:
        """Check both API and database components and return their status."""
        api_ok = await self.check_api_health()
        db_ok = await asyncio.to_thread(self.check_database_health)
        status = {"api": api_ok, "database": db_ok}
        self.log_health_status(status)
        if not all(status.values()):
            self.send_health_alert(f"System health degraded: {status}")
        return status

    def send_health_alert(self, message: str) -> None:
        """Send an alert when the system health check fails."""
        logger.error("HEALTH ALERT: %s", message)

    async def check_api_health(self) -> bool:
        """Ensure the Bybit API is reachable by fetching spot data."""
        try:
            async with BybitAPI() as api:
                await api.get_spot_data("BTCUSDT")
            return True
        except Exception as exc:  # pragma: no cover - network
            logger.warning("API health check failed: %s", exc)
            return False

    def check_database_health(self) -> bool:
        """Ensure the trade database can be opened and queried."""
        try:
            db = TradeDatabase(self.db_path)
            db.get_trade_statistics()
            db.close()
            return True
        except Exception as exc:  # pragma: no cover
            logger.warning("Database health check failed: %s", exc)
            return False

    def log_health_status(self, status: Dict[str, bool]) -> None:
        """Log the current health status for observability."""
        logger.info("Health status - API: %s, Database: %s", status["api"], status["database"])
