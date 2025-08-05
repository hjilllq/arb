"""Configuration management for arbitrage trading between Bybit and OKX.

This module loads settings from a `.env` file, handles trading pairs,
API keys, basis thresholds and creates asynchronous backups. It supports
spot and futures trading on both Bybit and OKX exchanges.
"""

from __future__ import annotations

import asyncio
import ast
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
import re

from dotenv import load_dotenv
from cryptography.fernet import Fernet

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BACKUP_DIR = Path(os.getenv("BACKUP_DIR", "backups"))
FERNET_KEY_PATH = Path(
    os.getenv("FERNET_KEY_PATH", str(Path.home() / ".config" / "arb" / "fernet.key"))
)
CONFIG_LOCK = asyncio.Lock()

CONFIG: Dict[str, Dict[str, Any]] = {
    "bybit": {"spot_pairs": [], "futures_pairs": [], "api_key": "", "api_secret": ""},
    "okx": {"spot_pairs": [], "futures_pairs": [], "api_key": "", "api_secret": ""},
}
BASIS_THRESHOLDS: Dict[str, Tuple[float, float]] = {}


def _parse_list(value: str | None) -> List[str]:
    if not value:
        return []
    try:
        parsed = ast.literal_eval(value)
        if isinstance(parsed, list):
            unique_pairs = list(dict.fromkeys(str(p) for p in parsed))
            return unique_pairs
    except (SyntaxError, ValueError) as exc:  # pragma: no cover
        logger.error("Failed to parse list '%s': %s", value, exc)
    return []


def load_config() -> None:
    """Load configuration from the `.env` file."""
    if not load_dotenv():
        logger.warning(".env file not found; proceeding with defaults")
    CONFIG["bybit"]["api_key"] = os.getenv("BYBIT_API_KEY", "")
    CONFIG["bybit"]["api_secret"] = os.getenv("BYBIT_API_SECRET", "")
    CONFIG["okx"]["api_key"] = os.getenv("OKX_API_KEY", "")
    CONFIG["okx"]["api_secret"] = os.getenv("OKX_API_SECRET", "")

    CONFIG["bybit"]["spot_pairs"] = _parse_list(os.getenv("SPOT_PAIRS"))
    CONFIG["bybit"]["futures_pairs"] = _parse_list(os.getenv("FUTURES_PAIRS"))
    CONFIG["okx"]["spot_pairs"] = _parse_list(os.getenv("OKX_SPOT_PAIRS"))
    CONFIG["okx"]["futures_pairs"] = _parse_list(os.getenv("OKX_FUTURES_PAIRS"))

    for pair in set(CONFIG["bybit"]["spot_pairs"] + CONFIG["okx"]["spot_pairs"]):
        env_key = pair.replace("/", "_").replace("-", "_").upper()
        try:
            open_th = float(os.getenv(f"{env_key}_BASIS_THRESHOLD_OPEN", "0"))
            close_th = float(os.getenv(f"{env_key}_BASIS_THRESHOLD_CLOSE", "0"))
            BASIS_THRESHOLDS[pair] = (open_th, close_th)
        except ValueError as exc:  # pragma: no cover
            logger.error("Invalid basis thresholds for %s: %s", pair, exc)


def get_spot_pairs(exchange: str) -> List[str]:
    exchange = exchange.lower()
    if exchange not in CONFIG:
        raise ValueError("Unknown exchange")
    return list(CONFIG[exchange]["spot_pairs"])


def get_futures_pairs(exchange: str) -> List[str]:
    exchange = exchange.lower()
    if exchange not in CONFIG:
        raise ValueError("Unknown exchange")
    return list(CONFIG[exchange]["futures_pairs"])


def get_pair_mapping(exchange: str) -> Dict[str, str]:
    exchange = exchange.lower()
    if exchange not in CONFIG:
        raise ValueError("Unknown exchange")
    spots = CONFIG[exchange]["spot_pairs"]
    futures = CONFIG[exchange]["futures_pairs"]
    if len(spots) != len(futures):
        raise ValueError("Spot and futures pair counts do not match")
    mapping: Dict[str, str] = {}
    for s, f in zip(spots, futures):
        if exchange == "okx" and "-" not in f:
            raise ValueError(f"OKX futures pair must contain '-': {f}")
        mapping[str(s)] = str(f)
    return mapping


def get_basis_thresholds(pair: str, exchange: str) -> Tuple[float, float]:
    """Get basis thresholds for a trading pair.

    Args:
        pair: Trading pair (e.g., 'BTC/USDT').
        exchange: Exchange name (included for API compatibility, not used).

    Returns:
        Tuple of (open_threshold, close_threshold).
    """
    del exchange
    thresholds = BASIS_THRESHOLDS.get(pair)
    if thresholds is None:
        logger.warning(
            "No thresholds found for pair %s, using default (0.0, 0.0)", pair
        )
        return (0.0, 0.0)
    return thresholds


def encrypt_api_keys(keys: Dict[str, str]) -> bytes:
    data = json.dumps(keys).encode()
    key = Fernet.generate_key()
    fernet = Fernet(key)
    encrypted = fernet.encrypt(data)
    FERNET_KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
    FERNET_KEY_PATH.write_bytes(key)
    return encrypted


def decrypt_api_keys(encrypted: bytes) -> Dict[str, str]:
    if not FERNET_KEY_PATH.exists():
        raise FileNotFoundError(f"fernet.key not found at {FERNET_KEY_PATH}")
    key = FERNET_KEY_PATH.read_bytes()
    fernet = Fernet(key)
    decrypted = fernet.decrypt(encrypted)
    return json.loads(decrypted.decode())


async def save_config_backup() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        logger.error(".env file not found for backup")
        return
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_path = BACKUP_DIR / f".env.backup.{ts}"
    data = await asyncio.to_thread(env_path.read_bytes)
    await asyncio.to_thread(backup_path.write_bytes, data)


def validate_config() -> bool:
    required = [
        "BYBIT_API_KEY",
        "BYBIT_API_SECRET",
        "OKX_API_KEY",
        "OKX_API_SECRET",
        "SPOT_PAIRS",
        "FUTURES_PAIRS",
        "OKX_SPOT_PAIRS",
        "OKX_FUTURES_PAIRS",
    ]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        logger.error("Missing config keys: %s", ", ".join(missing))
        return False
    if len(CONFIG["bybit"]["spot_pairs"]) != len(CONFIG["bybit"]["futures_pairs"]):
        logger.error("Bybit spot/futures pair count mismatch")
        return False
    if len(CONFIG["okx"]["spot_pairs"]) != len(CONFIG["okx"]["futures_pairs"]):
        logger.error("OKX spot/futures pair count mismatch")
        return False
    pattern = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")
    for pair in CONFIG["okx"]["futures_pairs"]:
        if not pattern.match(pair):
            logger.error("Invalid OKX futures pair format: %s", pair)
            return False
    return True


async def update_config(new_settings: Dict[str, str]) -> None:
    env_path = Path(".env")
    async with CONFIG_LOCK:
        if env_path.exists():
            content = await asyncio.to_thread(env_path.read_text)
            lines = [line for line in content.splitlines() if "=" in line]
            current = dict(line.split("=", 1) for line in lines)
        else:
            current = {}
        current.update(new_settings)
        text = "\n".join(f"{k}={v}" for k, v in current.items())
        await asyncio.to_thread(env_path.write_text, text)


if __name__ == "__main__":  # pragma: no cover
    load_config()
    print("Bybit spot pairs:", get_spot_pairs("bybit"))
