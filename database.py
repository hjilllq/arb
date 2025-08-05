"""Asynchronous SQLite helper for storing arbitrage data.

This module is like an album where we glue tiny stickers for every price and
trade.  Each function speaks in simple sentences so even a curious kid can read
it.  Operations are asynchronous to keep our Apple Silicon M4 Max cool and
energy‑frugal.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import gzip
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import aiosqlite
import pandas as pd

# ---------------------------------------------------------------------------
# Helper setup: logger and notifier. Real modules will be plugged in later.
# ---------------------------------------------------------------------------
try:  # pragma: no cover - tiny helper
    from logger import get_logger  # type: ignore
except Exception:  # pragma: no cover - fallback
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
        """Fallback notifier that simply logs a warning."""
        logger.warning("Notification: %s - %s", message, detail)


# ---------------------------------------------------------------------------
# Global paths and connection holder
# ---------------------------------------------------------------------------
_DB_PATH = Path("data/trades.db")
_DB_PATH.parent.mkdir(exist_ok=True)
_BACKUP_DIR = Path("backups")
_BACKUP_DIR.mkdir(exist_ok=True)
_EXECUTOR = ThreadPoolExecutor(max_workers=2)

# Connection stored globally after :func:`connect_db` runs.
_DB_CONN: Optional[aiosqlite.Connection] = None


async def _get_conn() -> aiosqlite.Connection:
    """Return an active connection, reconnecting if necessary."""
    global _DB_CONN
    if _DB_CONN is None:
        await connect_db()
    else:
        try:
            await _DB_CONN.execute("SELECT 1")
        except Exception:
            try:
                await _DB_CONN.close()
            except Exception:
                pass
            _DB_CONN = None
            await connect_db()
    assert _DB_CONN is not None
    return _DB_CONN

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
async def _ensure_schema(conn: aiosqlite.Connection) -> None:
    """Create the trades table if it does not exist."""
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            spot_symbol TEXT NOT NULL,
            futures_symbol TEXT NOT NULL,
            spot_bid REAL NOT NULL,
            futures_ask REAL NOT NULL,
            trade_qty REAL DEFAULT 0,
            funding_rate REAL DEFAULT 0
        )
        """
    )
    await conn.commit()


# ---------------------------------------------------------------------------
# 1. async connect_db
# ---------------------------------------------------------------------------
async def connect_db(db_path: str = str(_DB_PATH), retries: int = 3) -> None:
    """Connect to the SQLite database asynchronously.

    Like a patient child placing a sticker, we try again with a little delay if
    the first attempt fails.  The connection is stored globally so other
    functions can reuse it.
    """
    global _DB_CONN
    delay = 0.1
    for attempt in range(1, retries + 1):
        try:
            _DB_CONN = await aiosqlite.connect(db_path)
            await _ensure_schema(_DB_CONN)
            logger.info("Connected to database at %s", db_path)
            return
        except Exception as exc:
            logger.error("Database connection failed (attempt %d/%d): %s", attempt, retries, exc)
            if attempt == retries:
                try:
                    await notify("Database connection failed", str(exc))
                except Exception:
                    pass
                raise
            await asyncio.sleep(delay + random.random() * 0.1)
            delay *= 2


# ---------------------------------------------------------------------------
# 2. async save_data
# ---------------------------------------------------------------------------
async def save_data(data: List[Dict[str, Any]]) -> None:
    """Store a list of price or trade entries into the database.

    Each dictionary is checked with :func:`validate_data` before insertion.
    Example item::

        {'timestamp': '2025-08-05T00:00:00', 'spot_symbol': 'BTC/USDT',
         'futures_symbol': 'BTCUSDT', 'spot_bid': 59990, 'futures_ask': 60000}
    """
    if not validate_data(data):
        return
    delay = 0.1
    for attempt in range(1, 4):
        conn = await _get_conn()
        try:
            await conn.executemany(
                """
                INSERT INTO trades (
                    timestamp, spot_symbol, futures_symbol,
                    spot_bid, futures_ask, trade_qty, funding_rate
                ) VALUES (:timestamp, :spot_symbol, :futures_symbol,
                          :spot_bid, :futures_ask,
                          :trade_qty, :funding_rate)
                """,
                [
                    {
                        "timestamp": item["timestamp"],
                        "spot_symbol": item["spot_symbol"],
                        "futures_symbol": item["futures_symbol"],
                        "spot_bid": item["spot_bid"],
                        "futures_ask": item["futures_ask"],
                        "trade_qty": item.get("trade_qty", 0),
                        "funding_rate": item.get("funding_rate", 0),
                    }
                    for item in data
                ],
            )
            await conn.commit()
            logger.info("Saved %d rows", len(data))
            return
        except Exception as exc:
            logger.error("Failed to save data (attempt %d/3): %s", attempt, exc)
            if attempt == 3:
                try:
                    await notify("Database write failed", str(exc))
                except Exception:
                    pass
                return
            await asyncio.sleep(delay + random.random() * 0.1)
            delay *= 2


# ---------------------------------------------------------------------------
# 3. async query_data
# ---------------------------------------------------------------------------
async def query_data(
    spot_symbol: str,
    futures_symbol: str,
    start_date: str,
    end_date: str,
    min_qty: Optional[float] = None,
    max_qty: Optional[float] = None,
    min_funding: Optional[float] = None,
    max_funding: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Return records for a pair between two dates with optional filters.

    Extra parameters allow deeper analysis, letting us peek only at trades that
    match certain quantity or funding rate ranges.
    """
    conn = await _get_conn()
    try:
        query = (
            "SELECT timestamp, spot_symbol, futures_symbol, "
            "spot_bid, futures_ask, trade_qty, funding_rate "
            "FROM trades WHERE spot_symbol=? AND futures_symbol=? "
            "AND timestamp BETWEEN ? AND ?"
        )
        params: List[Any] = [spot_symbol, futures_symbol, start_date, end_date]
        if min_qty is not None:
            query += " AND trade_qty >= ?"
            params.append(min_qty)
        if max_qty is not None:
            query += " AND trade_qty <= ?"
            params.append(max_qty)
        if min_funding is not None:
            query += " AND funding_rate >= ?"
            params.append(min_funding)
        if max_funding is not None:
            query += " AND funding_rate <= ?"
            params.append(max_funding)
        query += " ORDER BY timestamp"
        cursor = await conn.execute(query, params)
        rows = await cursor.fetchall()
        cols = [c[0] for c in cursor.description]
        df = pd.DataFrame(rows, columns=cols)
        return df.to_dict("records")
    except Exception as exc:
        logger.error("Query failed: %s", exc)
        try:
            await notify("Database query failed", str(exc))
        except Exception:
            pass
        return []


# ---------------------------------------------------------------------------
# 4. async backup_database
# ---------------------------------------------------------------------------
async def backup_database(compress: bool = True) -> None:
    """Create a timestamped backup copy of the database file.

    The file is optionally compressed with gzip to save space, and we log its
    size before copying to avoid surprises.
    """
    if not _DB_PATH.exists():
        logger.error("Cannot backup: database missing at %s", _DB_PATH)
        await notify("Database backup failed", "missing db file")
        return
    size_mb = _DB_PATH.stat().st_size / 1_048_576
    logger.info("Database size %.2f MB", size_mb)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_file = _BACKUP_DIR / f"trades.db.{timestamp}.bak"
    loop = asyncio.get_running_loop()

    def _copy(src: Path, dst: Path) -> None:
        if compress:
            with open(src, "rb") as fsrc, gzip.open(dst, "wb") as fdst:
                shutil.copyfileobj(fsrc, fdst)
        else:
            shutil.copy2(src, dst)

    try:
        if compress:
            backup_file = backup_file.with_suffix(backup_file.suffix + ".gz")
        await loop.run_in_executor(_EXECUTOR, _copy, _DB_PATH, backup_file)
        logger.info("Database backup created at %s", backup_file)
    except Exception as exc:
        logger.error("Database backup failed: %s", exc)
        await notify("Database backup failed", str(exc))


# ---------------------------------------------------------------------------
# 5. async clear_old_data
# ---------------------------------------------------------------------------
async def clear_old_data(days: int = 90) -> None:
    """Remove records older than ``days`` days."""
    conn = await _get_conn()
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    try:
        await conn.execute("DELETE FROM trades WHERE timestamp < ?", (cutoff,))
        await conn.commit()
        logger.info("Old records cleared up to %s", cutoff)
    except Exception as exc:
        logger.error("Failed to clear old data: %s", exc)
        await notify("Database cleanup failed", str(exc))


# ---------------------------------------------------------------------------
# 6. validate_data
# ---------------------------------------------------------------------------
def validate_data(data: Iterable[Dict[str, Any]]) -> bool:
    """Check that incoming rows have the correct shape and values.

    Prices must be positive numbers and required fields must exist.  If
    anything looks fishy we log and notify.
    """
    try:
        for item in data:
            required = {
                "timestamp",
                "spot_symbol",
                "futures_symbol",
                "spot_bid",
                "futures_ask",
            }
            if not required.issubset(item):
                raise ValueError(f"missing keys: {required - set(item)}")
            if float(item["spot_bid"]) <= 0 or float(item["futures_ask"]) <= 0:
                raise ValueError("prices must be positive")
    except Exception as exc:
        logger.error("Invalid data: %s", exc)
        try:
            asyncio.get_running_loop().create_task(
                notify("Invalid database data", str(exc))
            )
        except RuntimeError:
            pass
        return False
    return True


# ---------------------------------------------------------------------------
# 7. async fetch_latest_trade
# ---------------------------------------------------------------------------
async def fetch_latest_trade(
    spot_symbol: str, futures_symbol: str
) -> Optional[Dict[str, Any]]:
    """Return the most recent entry for a given pair."""
    conn = await _get_conn()
    try:
        cursor = await conn.execute(
            """
            SELECT timestamp, spot_symbol, futures_symbol, spot_bid,
                   futures_ask, trade_qty, funding_rate
            FROM trades WHERE spot_symbol=? AND futures_symbol=?
            ORDER BY timestamp DESC LIMIT 1
            """,
            (spot_symbol, futures_symbol),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        cols = [c[0] for c in cursor.description]
        return dict(zip(cols, row))
    except Exception as exc:
        logger.error("fetch_latest_trade failed: %s", exc)
        await notify("Database fetch failed", str(exc))
        return None


# ---------------------------------------------------------------------------
# 8. async count_rows
# ---------------------------------------------------------------------------
async def count_rows() -> int:
    """Return the total number of rows in the trades table."""
    conn = await _get_conn()
    try:
        cursor = await conn.execute("SELECT COUNT(*) FROM trades")
        (count,) = await cursor.fetchone()
        return int(count)
    except Exception as exc:
        logger.error("count_rows failed: %s", exc)
        await notify("Database count failed", str(exc))
        return 0


# ---------------------------------------------------------------------------
# 9. async close_db
# ---------------------------------------------------------------------------
async def close_db() -> None:
    """Close the database connection if it exists."""
    global _DB_CONN
    if _DB_CONN is not None:
        await _DB_CONN.close()
        _DB_CONN = None
        logger.info("Database connection closed")


# ---------------------------------------------------------------------------
# 10. get_db_path
# ---------------------------------------------------------------------------
def get_db_path() -> Path:
    """Expose the database path for other modules."""
    return _DB_PATH
