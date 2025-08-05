"""Configuration helper for the arbitrage bot.

This module is like a treasure chest where we neatly keep our secret keys,
trading pairs, and safety rules.  Every function tries to explain itself as if
talking to a young engineer: short sentences, playful metaphors and plenty of
examples.  The code prefers light I/O and asynchronous tricks so the Apple
Silicon M4 Max can nap while we work.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import aiofiles
from dotenv import dotenv_values
from cryptography.fernet import Fernet

# ---------------------------------------------------------------------------
# Helper setup: logger and notifier.  Real modules will be provided later.     
# ---------------------------------------------------------------------------
try:  # pragma: no cover - tiny helper, tested indirectly
    from logger import get_logger  # type: ignore
except Exception:  # pragma: no cover - fallback for early development
    import logging

    logging.basicConfig(level=logging.INFO)

    def get_logger(name: str) -> "logging.Logger":
        """Return a basic logger if the real one is missing."""
        return logging.getLogger(name)

logger = get_logger(__name__)

try:  # pragma: no cover - tiny helper
    from notification_manager import notify  # type: ignore
except Exception:  # pragma: no cover - fallback
    async def notify(message: str, detail: str = "") -> None:
        """Fallback async notifier that simply logs the message."""
        logger.warning("Notification: %s - %s", message, detail)


# ---------------------------------------------------------------------------
# Constants shared across the module.                                         
# ---------------------------------------------------------------------------
_ENV_PATH = Path(".env")
_BACKUP_DIR = Path("backups")
_BACKUP_DIR.mkdir(exist_ok=True)

# Load or generate the Fernet key from a protected location.  The path can be
# overridden with an environment variable and should not be committed to VCS.
_FERNET_KEY_PATH = Path(os.getenv("FERNET_KEY_PATH", Path.home() / ".fernet.key"))
if _FERNET_KEY_PATH.exists():
    _FERNET_KEY = _FERNET_KEY_PATH.read_bytes().strip()
else:
    _FERNET_KEY = Fernet.generate_key()
    _FERNET_KEY_PATH.write_bytes(_FERNET_KEY)
    try:
        os.chmod(_FERNET_KEY_PATH, 0o600)  # owner read/write
    except OSError:  # pragma: no cover - depends on OS
        pass
    logger.info("Fernet key generated and saved to %s", _FERNET_KEY_PATH)
_CIPHER = Fernet(_FERNET_KEY)

# This global cache will hold the latest configuration after load_config runs.
CONFIG: Dict[str, Any] = {}


# ---------------------------------------------------------------------------
# 1. load_config                                                             
# ---------------------------------------------------------------------------
def load_config(file_path: str = ".env") -> Dict[str, str]:
    """Read a ``.env`` file and return a dictionary of settings.

    It is like opening a lunchbox to see every snack inside.  Reading is done
    with :func:`dotenv_values` to minimise disk chatter.  If the ``.env`` file
    is missing we peek at environment variables instead of giving up.

    Parameters
    ----------
    file_path:
        Where the ``.env`` file lives.  Defaults to ``".env"`` next to this
        module.

    Returns
    -------
    dict
        Mapping of names to values, e.g. ``{"API_KEY": "abc"}``.  Environment
        variables are used as a fallback when the file is missing.

    Examples
    --------
    >>> config = load_config()
    >>> config.get("API_KEY")  # doctest: +SKIP
    'FSE8dMTTSC6qgLbfOs'
    """
    env_path = Path(file_path)
    if not env_path.exists():
        logger.warning("Config file %s is missing. Using default values.", file_path)
        try:
            asyncio.run(notify("Config file missing", file_path))
        except RuntimeError:  # event loop already running
            pass
        config = dotenv_values()
        if config:
            logger.info("Config loaded from environment variables")
        return config
    config = dotenv_values(env_path)
    logger.info("Config loaded from %s", file_path)
    return config


# ---------------------------------------------------------------------------
# 2. validate_config                                                         
# ---------------------------------------------------------------------------
def validate_config(config: Dict[str, Any]) -> bool:
    """Check that configuration values make sense.

    Think of it as making sure puzzle pieces fit before building the castle.

    Validation rules
    ----------------
    * Pair lists must exist, match in length, and contain no duplicates.
    * Pairs must follow ``AAA/BBB`` or ``AAABBB`` formats with no extra spaces.
    * Every threshold must be positive.

    Parameters
    ----------
    config:
        Dictionary produced by :func:`load_config`.

    Returns
    -------
    bool
        ``True`` if everything looks good, ``False`` otherwise.
    """
    try:
        spot_pairs = json.loads(config.get("SPOT_PAIRS", "[]").replace("'", '"'))
        futures_pairs = json.loads(config.get("FUTURES_PAIRS", "[]").replace("'", '"'))
        spot_pairs = [s.strip() for s in spot_pairs]
        futures_pairs = [s.strip() for s in futures_pairs]
        if not spot_pairs or not futures_pairs:
            raise ValueError("pair lists cannot be empty")
        if len(spot_pairs) != len(futures_pairs):
            raise ValueError("spot and futures pairs mismatch")
        if len(set(spot_pairs)) != len(spot_pairs):
            raise ValueError("duplicate spot pairs detected")
        if len(set(futures_pairs)) != len(futures_pairs):
            raise ValueError("duplicate futures pairs detected")

        spot_re = re.compile(r"^[A-Z0-9]+/[A-Z0-9]+$")
        fut_re = re.compile(r"^[A-Z0-9]+[-]?[A-Z0-9]+$")

        for s_pair, f_pair in zip(spot_pairs, futures_pairs):
            if not spot_re.match(s_pair):
                raise ValueError(f"invalid spot pair format: {s_pair}")
            if not fut_re.match(f_pair):
                raise ValueError(f"invalid futures pair format: {f_pair}")

            base = s_pair.replace("/", "_")
            open_key = f"{base}_BASIS_THRESHOLD_OPEN"
            close_key = f"{base}_BASIS_THRESHOLD_CLOSE"
            open_val = float(config.get(open_key, 0))
            close_val = float(config.get(close_key, 0))
            if open_val <= 0 or close_val <= 0:
                raise ValueError(f"thresholds for {s_pair} must be > 0")
    except Exception as exc:  # broad to keep example child friendly
        logger.error("Validation failed: %s", exc)
        try:
            asyncio.run(notify("Config validation failed", str(exc)))
        except RuntimeError:
            pass
        return False
    logger.info("Config validated successfully.")
    return True


# ---------------------------------------------------------------------------
# 3. encrypt_config                                                          
# ---------------------------------------------------------------------------
def encrypt_config(data: Dict[str, Any]) -> bytes:
    """Encrypt sensitive dictionary values using ``Fernet``.

    Imagine locking treasure into a magic box: the result is safe, unreadable
    bytes.

    Parameters
    ----------
    data:
        Dictionary with secret values, e.g. API keys.

    Returns
    -------
    bytes
        Encrypted blob ready for storage.
    """
    try:
        plaintext = json.dumps(data).encode()
        return _CIPHER.encrypt(plaintext)
    except Exception as exc:  # pragma: no cover - encryption rarely fails
        logger.error("Encryption error: %s", exc)
        asyncio.run(notify("Encryption error", str(exc)))
        raise


# ---------------------------------------------------------------------------
# 4. decrypt_config                                                          
# ---------------------------------------------------------------------------
def decrypt_config(encrypted_data: bytes) -> Dict[str, Any]:
    """Decrypt previously encrypted bytes back into a dictionary.

    It is like opening the magic box to take the treasure out.

    Parameters
    ----------
    encrypted_data:
        Bytes produced by :func:`encrypt_config`.

    Returns
    -------
    dict
        Original dictionary with secrets.
    """
    try:
        decrypted = _CIPHER.decrypt(encrypted_data)
        return json.loads(decrypted.decode())
    except Exception as exc:  # pragma: no cover - depends on input
        logger.error("Decryption error: %s", exc)
        asyncio.run(notify("Decryption error", str(exc)))
        raise ValueError("invalid encrypted data") from exc


# ---------------------------------------------------------------------------
# 5. get_spot_pairs                                                          
# ---------------------------------------------------------------------------
def get_spot_pairs() -> List[str]:
    """Return the list of spot pairs from the loaded configuration.

    Example
    -------
    >>> CONFIG["SPOT_PAIRS"] = "['BTC/USDT']"
    >>> get_spot_pairs()
    ['BTC/USDT']
    """
    pairs = CONFIG.get("SPOT_PAIRS", "[]")
    if isinstance(pairs, str):
        pairs = pairs.replace("'", '"')
        pairs = json.loads(pairs)
    return pairs


# ---------------------------------------------------------------------------
# 6. get_futures_pairs                                                       
# ---------------------------------------------------------------------------
def get_futures_pairs() -> List[str]:
    """Return the list of futures pairs from the configuration.

    Example
    -------
    >>> CONFIG["FUTURES_PAIRS"] = "['BTCUSDT']"
    >>> get_futures_pairs()
    ['BTCUSDT']
    """
    pairs = CONFIG.get("FUTURES_PAIRS", "[]")
    if isinstance(pairs, str):
        pairs = pairs.replace("'", '"')
        pairs = json.loads(pairs)
    return pairs


# ---------------------------------------------------------------------------
# 7. get_pair_mapping                                                        
# ---------------------------------------------------------------------------
def get_pair_mapping() -> Dict[str, str]:
    """Create a mapping from each spot pair to its futures pair.

    This is the dictionary the trading engine uses to know which futures
    contract hedges which spot asset.

    Example
    -------
    >>> CONFIG["SPOT_PAIRS"] = "['BTC/USDT']"
    >>> CONFIG["FUTURES_PAIRS"] = "['BTCUSDT']"
    >>> get_pair_mapping()
    {'BTC/USDT': 'BTCUSDT'}
    """
    return dict(zip(get_spot_pairs(), get_futures_pairs()))


# ---------------------------------------------------------------------------
# 8. get_pair_thresholds                                                     
# ---------------------------------------------------------------------------
def get_pair_thresholds(symbol: str) -> Dict[str, float]:
    """Fetch open/close basis thresholds for a given spot pair.

    Parameters
    ----------
    symbol:
        Spot symbol like ``"BTC/USDT"``.

    Returns
    -------
    dict
        ``{"open": 0.005, "close": 0.001}`` for example.

    Examples
    --------
    >>> CONFIG.update({'BTC_USDT_BASIS_THRESHOLD_OPEN': '0.005',
    ...               'BTC_USDT_BASIS_THRESHOLD_CLOSE': '0.001'})
    >>> get_pair_thresholds('BTC/USDT')
    {'open': 0.005, 'close': 0.001}
    """
    base = symbol.replace("/", "_")
    open_key = f"{base}_BASIS_THRESHOLD_OPEN"
    close_key = f"{base}_BASIS_THRESHOLD_CLOSE"
    try:
        open_val = float(CONFIG[open_key])
        close_val = float(CONFIG[close_key])
    except KeyError as exc:
        logger.error("Missing threshold for %s: %s", symbol, exc)
        try:
            asyncio.run(notify("Missing threshold", f"{symbol}: {exc}"))
        except RuntimeError:
            pass
        raise
    return {"open": open_val, "close": close_val}


# ---------------------------------------------------------------------------
# 9. update_config                                                           
# ---------------------------------------------------------------------------
async def update_config(key: str, value: Any) -> None:
    """Asynchronously update a single configuration value.

    Think of it as adding a new rule to our notebook and writing it down
    carefully so we do not forget.

    Parameters
    ----------
    key:
        Name of the setting to change.
    value:
        New value for the setting.  It will be written to the ``.env`` file.

    Examples
    --------
    >>> asyncio.run(update_config('MAX_DAILY_LOSS', 0.1))  # doctest: +SKIP
    """
    CONFIG[key] = value

    lines: List[str] = []
    if _ENV_PATH.exists():
        async with aiofiles.open(_ENV_PATH, mode="r", encoding="utf-8") as f:
            lines = await f.readlines()
    line_written = False
    async with aiofiles.open(_ENV_PATH, mode="w", encoding="utf-8") as f:
        for line in lines:
            if line.startswith(f"{key}="):
                await f.write(f"{key}={value}\n")
                line_written = True
            else:
                await f.write(line)
        if not line_written:
            await f.write(f"{key}={value}\n")
    logger.info("Updated %s in config", key)


# ---------------------------------------------------------------------------
# 10. backup_config                                                          
# ---------------------------------------------------------------------------
async def backup_config() -> None:
    """Create a timestamped copy of the ``.env`` file asynchronously.

    The backup sits in ``backups/`` like a photograph of our settings, so we
    can always look back if something goes wrong.

    Examples
    --------
    >>> asyncio.run(backup_config())  # doctest: +SKIP
    """
    if not _ENV_PATH.exists():
        logger.error("Cannot backup: %s does not exist", _ENV_PATH)
        await notify("Backup failed", ".env missing")
        return
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_file = _BACKUP_DIR / f".env.{timestamp}.bak"
    async with aiofiles.open(_ENV_PATH, mode="rb") as fsrc:
        data = await fsrc.read()
    async with aiofiles.open(backup_file, mode="wb") as fdst:
        await fdst.write(data)
    logger.info("Backup created: %s", backup_file)

    # Clean up backups older than 30 days to save disk space.
    cutoff = datetime.utcnow().timestamp() - 30 * 24 * 3600
    for backup in _BACKUP_DIR.glob("*.bak"):
        try:
            if backup.stat().st_mtime < cutoff:
                backup.unlink()
                logger.info("Old backup removed: %s", backup)
        except OSError:  # pragma: no cover - rare file system issue
            logger.warning("Failed to inspect/delete backup: %s", backup)


# ---------------------------------------------------------------------------
# Initial load when the module is imported.                                  
# ---------------------------------------------------------------------------
try:  # pragma: no cover - executed at import
    CONFIG = load_config()
    if CONFIG:
        validate_config(CONFIG)
    else:
        logger.warning("Running with empty configuration; please check .env")
except Exception as exc:  # pragma: no cover - prevents crash on import
    logger.error("Initial configuration load failed: %s", exc)
    CONFIG = {}
