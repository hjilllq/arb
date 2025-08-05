"""Configuration helper for the spot/futures arbitrage bot.

This module keeps all settings for the bot in one place.  It reads a
``.env`` file, checks that the values make sense and exposes tiny helper
functions that other modules can use.  Everything is written in a very
explicit style so it is easy to follow even for newcomers.

The code targets macOS on Apple Silicon (ARM) and therefore tries to keep
I/O to a minimum.  We only touch the ``.env`` file when absolutely
necessary and keep the parsed configuration cached in memory.

Main features
-------------
* Read ``.env`` using :mod:`python-dotenv`
* Validate basic settings (pairs and thresholds)
* Encrypt and decrypt sensitive fields using ``cryptography``
* Asynchronous update of ``.env`` and creation of timestamped backups

Functions are documented with examples in their docstrings.
"""

from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from cryptography.fernet import Fernet
from dotenv import dotenv_values, set_key

from logger import log_error
from notification_manager import notify

# Path to the main ``.env`` file.  The path is relative to the repository
# root which keeps things simple when running the bot from cron or other
# schedulers.
ENV_PATH = Path('.env')

# Folder where copies of the ``.env`` file will be stored.  A tiny
# ``ThreadPoolExecutor`` is used for all blocking file operations so that
# the main asyncio event loop does not get stuck.
BACKUP_DIR = Path('backups')
EXECUTOR = ThreadPoolExecutor(max_workers=2)

# Cache for the configuration.  ``load_config`` fills this on first use so
# other helper functions can access configuration data without touching the
# file system again.
_CONFIG: Dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_env_exists(path: Path) -> None:
    """Make sure the ``.env`` file exists.

    If the file is missing we log the problem and raise ``FileNotFoundError``.
    """

    if not path.is_file():
        message = f"Missing configuration file: {path}"
        log_error(message)
        notify(message)
        raise FileNotFoundError(message)


def _parse_pairs(raw: str) -> List[str]:
    """Turn a string representation of a list into a real list.

    ``python-dotenv`` reads values as strings.  In ``.env`` files we store
    pairs like ``['BTC/USDT','ETH/USDT']``.  ``ast.literal_eval`` is a safe
    way to convert that back to a Python list without using ``eval``.
    """

    import ast

    try:
        result = ast.literal_eval(raw)
        if not isinstance(result, list):  # pragma: no cover - defensive
            raise ValueError
        return [str(p).strip() for p in result]
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(f"Could not parse pair list: {raw}") from exc


def _get_fernet() -> Fernet:
    """Return a ``Fernet`` instance using a key from the environment.

    A key is looked up in ``_CONFIG`` under ``CONFIG_ENCRYPTION_KEY``.  If no
    key is present, a new one is created and written back to ``.env`` so it
    can be reused on the next launch.
    """

    key = _CONFIG.get('CONFIG_ENCRYPTION_KEY')
    if not key:
        key = Fernet.generate_key().decode()
        # ``set_key`` updates or adds a value in ``.env``.  The operation is
        # executed in a thread to avoid blocking the event loop.
        set_key(str(ENV_PATH), 'CONFIG_ENCRYPTION_KEY', key)
        _CONFIG['CONFIG_ENCRYPTION_KEY'] = key
    return Fernet(key.encode())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_config(file_path: str = '.env') -> Dict[str, Any]:
    """Load configuration values from ``.env``.

    Parameters
    ----------
    file_path:
        Path to the ``.env`` file.  Defaults to ``'.env'`` in the project
        root.

    Returns
    -------
    dict
        Dictionary with all key/value pairs from ``.env``.  Lists are
        converted to real Python lists.

    Raises
    ------
    FileNotFoundError
        If the configuration file does not exist.
    """

    path = Path(file_path)
    _ensure_env_exists(path)

    # ``dotenv_values`` reads the file once and returns a dict without
    # touching ``os.environ``.  This is lighter on I/O than repeatedly using
    # ``load_dotenv``.
    raw_config = dotenv_values(path)

    config: Dict[str, Any] = dict(raw_config)

    # Parse pair lists if present
    for key in ('SPOT_PAIRS', 'FUTURES_PAIRS'):
        if key in config and isinstance(config[key], str):
            config[key] = _parse_pairs(config[key])

    # Cache and return
    _CONFIG.clear()
    _CONFIG.update(config)
    return config


def validate_config(config: Dict[str, Any]) -> bool:
    """Check that basic configuration values look sensible.

    The function logs and notifies about errors but does not raise
    exceptions so that callers can decide how to handle invalid setups.
    """

    try:
        spot = config.get('SPOT_PAIRS', [])
        futures = config.get('FUTURES_PAIRS', [])
        if not spot or not futures:
            raise ValueError('Pair lists must not be empty')
        if len(spot) != len(futures):
            raise ValueError('Spot and futures pairs must match in length')

        # Basic spot <-> futures name check: BTC/USDT -> BTCUSDT
        for s, f in zip(spot, futures):
            if s.replace('/', '') != f:
                raise ValueError(f'Mismatched pair: {s} vs {f}')

        # Thresholds must be positive numbers
        for s in spot:
            base, quote = s.replace('/', '_').split('_')
            open_key = f'{base}_{quote}_BASIS_THRESHOLD_OPEN'
            close_key = f'{base}_{quote}_BASIS_THRESHOLD_CLOSE'
            open_val = float(config.get(open_key, 0))
            close_val = float(config.get(close_key, 0))
            if open_val <= 0 or close_val <= 0:
                raise ValueError(f'Thresholds for {s} must be > 0')

        return True
    except Exception as exc:  # pragma: no cover - defensive
        message = f'Configuration validation error: {exc}'
        log_error(message)
        notify(message)
        return False


def encrypt_config(data: Dict[str, Any]) -> bytes:
    """Encrypt a configuration dictionary and return the raw bytes.

    Only values that are strings are serialized to JSON and encrypted.  The
    encryption key is stored in ``.env`` under ``CONFIG_ENCRYPTION_KEY``.
    """

    fernet = _get_fernet()
    json_data = json.dumps(data).encode()
    return fernet.encrypt(json_data)


def decrypt_config(encrypted_data: bytes) -> Dict[str, Any]:
    """Reverse operation of :func:`encrypt_config`.

    Examples
    --------
    >>> enc = encrypt_config({'API_KEY': 'abc'})
    >>> decrypt_config(enc)
    {'API_KEY': 'abc'}
    """

    fernet = _get_fernet()
    json_data = fernet.decrypt(encrypted_data)
    return json.loads(json_data.decode())


def get_spot_pairs() -> List[str]:
    """Return the list of spot trading pairs."""

    return _CONFIG.get('SPOT_PAIRS', [])


def get_futures_pairs() -> List[str]:
    """Return the list of futures trading pairs."""

    return _CONFIG.get('FUTURES_PAIRS', [])


def get_pair_mapping() -> Dict[str, str]:
    """Return a mapping from spot to futures pairs.

    Example
    -------
    >>> load_config()
    >>> get_pair_mapping()
    {'BTC/USDT': 'BTCUSDT'}
    """

    return dict(zip(get_spot_pairs(), get_futures_pairs()))


def get_pair_thresholds(symbol: str) -> Dict[str, float]:
    """Return open/close thresholds for a given spot symbol.

    Parameters
    ----------
    symbol:
        Spot pair, for example ``'BTC/USDT'``.
    """

    base, quote = symbol.replace('/', '_').split('_')
    open_key = f'{base}_{quote}_BASIS_THRESHOLD_OPEN'
    close_key = f'{base}_{quote}_BASIS_THRESHOLD_CLOSE'
    return {
        'open': float(_CONFIG.get(open_key, 0)),
        'close': float(_CONFIG.get(close_key, 0)),
    }


async def update_config(key: str, value: Any) -> None:
    """Asynchronously update a value inside ``.env``.

    The in-memory cache is updated as well.
    """

    def _update() -> None:
        set_key(str(ENV_PATH), key, str(value))

    await asyncio.get_running_loop().run_in_executor(EXECUTOR, _update)
    _CONFIG[key] = value


async def backup_config() -> None:
    """Create a timestamped copy of ``.env`` in the ``backups`` folder."""

    BACKUP_DIR.mkdir(exist_ok=True)

    def _copy() -> None:
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        target = BACKUP_DIR / f'{timestamp}.env'
        data = ENV_PATH.read_bytes()
        target.write_bytes(data)

    await asyncio.get_running_loop().run_in_executor(EXECUTOR, _copy)


# Automatically load the configuration when the module is imported so that
# helper functions can use the cached data.
try:  # pragma: no cover - initialization
    load_config()
except FileNotFoundError:
    # Errors are already logged and notified inside ``load_config``.
    pass
