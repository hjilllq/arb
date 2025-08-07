"""Centralized logging utilities for the arbitrage project.

This module wraps Python's :mod:`logging` package to provide convenience
helpers for recording various events, error conditions and system metrics.
The helpers exposed here can be used by other modules to ensure
consistent log formatting across the codebase.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

# Configure a project-wide logger. This configuration is minimal and writes
# logs to standard output. Applications embedding this module may adjust the
# configuration (e.g. add file handlers) before importing it.
logger = logging.getLogger("arb")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def log_event(message: str) -> None:
    """Record a general informational event.

    Parameters
    ----------
    message:
        Description of the event.
    """
    logger.info(message)


def log_error(message: str, exc: Exception | None = None) -> None:
    """Record an error condition.

    Parameters
    ----------
    message:
        Human-readable error description.
    exc:
        Optional exception instance whose details should be logged.
    """
    if exc:
        logger.exception("%s: %s", message, exc)
    else:
        logger.error(message)


def log_trade_data(trade: Dict[str, Any]) -> None:
    """Log structured trade information.

    Parameters
    ----------
    trade:
        Mapping containing trade attributes such as pair, price and volume.
    """
    logger.info("TRADE DATA: %s", trade)


def log_system_health(status: Dict[str, Any]) -> None:
    """Log current system health metrics.

    Parameters
    ----------
    status:
        Mapping of health check names to their boolean or numeric status.
    """
    logger.info("SYSTEM HEALTH: %s", status)
