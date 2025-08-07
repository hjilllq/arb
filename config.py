"""Project configuration utilities.

This module loads configuration values from a ``.env`` file and validates
that the required parameters for running the arbitrage bot are present.
"""

from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv


@dataclass
class Config:
    """Container for runtime configuration values."""

    bybit_api_key: str
    bybit_api_secret: str
    arbitrage_symbol: str
    arbitrage_threshold: float
    max_position_size: float
    notification_email: str
    slack_webhook_url: str
    prometheus_endpoint: str
    grafana_url: str
    log_level: str


def load_config(path: str = ".env") -> Config:
    """Load configuration values from ``path`` and return a :class:`Config`.

    Parameters
    ----------
    path:
        Location of the ``.env`` file to read.
    """

    load_dotenv(dotenv_path=path)
    return Config(
        bybit_api_key=os.getenv("BYBIT_API_KEY", ""),
        bybit_api_secret=os.getenv("BYBIT_API_SECRET", ""),
        arbitrage_symbol=os.getenv("ARBITRAGE_SYMBOL", ""),
        arbitrage_threshold=float(os.getenv("ARBITRAGE_THRESHOLD", "0")),
        max_position_size=float(os.getenv("MAX_POSITION_SIZE", "0")),
        notification_email=os.getenv("NOTIFICATION_EMAIL", ""),
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
        prometheus_endpoint=os.getenv("PROMETHEUS_ENDPOINT", ""),
        grafana_url=os.getenv("GRAFANA_URL", ""),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )


def validate_config(cfg: Config) -> Config:
    """Validate essential configuration values.

    Raises
    ------
    ValueError
        If a required configuration value is missing or invalid.
    """

    missing = []
    if not cfg.bybit_api_key:
        missing.append("BYBIT_API_KEY")
    if not cfg.bybit_api_secret:
        missing.append("BYBIT_API_SECRET")
    if not cfg.arbitrage_symbol:
        missing.append("ARBITRAGE_SYMBOL")

    if cfg.arbitrage_threshold <= 0:
        raise ValueError("ARBITRAGE_THRESHOLD must be greater than 0")
    if cfg.max_position_size <= 0:
        raise ValueError("MAX_POSITION_SIZE must be greater than 0")

    if missing:
        raise ValueError(
            "Missing configuration values: " + ", ".join(missing)
        )
    return cfg
