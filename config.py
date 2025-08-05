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

from dotenv import load_dotenv
from cryptography.fernet import Fernet

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
            return [str(p) for p in parsed]
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
    del exchange  # exchange included for API compatibility
    return BASIS_THRESHOLDS.get(pair, (0.0, 0.0))


def encrypt_api_keys(keys: Dict[str, str]) -> bytes:
    data = json.dumps(keys).encode()
    key = Fernet.generate_key()
    fernet = Fernet(key)
    encrypted = fernet.encrypt(data)
    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)
    (backup_dir / "fernet.key").write_bytes(key)
    return encrypted


def decrypt_api_keys(encrypted: bytes) -> Dict[str, str]:
    key_path = Path("backups") / "fernet.key"
    if not key_path.exists():
        raise FileNotFoundError("fernet.key not found in backups/")
    key = key_path.read_bytes()
    fernet = Fernet(key)
    decrypted = fernet.decrypt(encrypted)
    return json.loads(decrypted.decode())


async def save_config_backup() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        logger.error(".env file not found for backup")
        return
    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_path = backup_dir / f".env.backup.{ts}"
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
    for pair in CONFIG["okx"]["futures_pairs"]:
        if "-" not in pair:
            logger.error("OKX futures pair missing '-': %s", pair)
            return False
    return True


async def update_config(new_settings: Dict[str, str]) -> None:
    env_path = Path(".env")
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
