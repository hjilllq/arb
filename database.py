"""Asynchronous SQLite helper for storing arbitrage data.

This module is like an album where we glue tiny stickers for every price and
trade.  Each function speaks in simple sentences so even a curious kid can read
it.  Operations are asynchronous to keep our Apple Silicon M4 Max cool and
energy‑frugal.

Note that the maximum number of concurrent queries is determined when the
module is imported.  Changing ``DB_QUERY_CONCURRENCY`` in ``.env`` during
runtime requires a restart or module reload to take effect.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import gzip
import random
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

import aiosqlite
import pandas as pd

try:  # pragma: no cover - config helper may not be present in tests
    from config import load_config  # type: ignore
except Exception:  # pragma: no cover - fallback to empty config
    def load_config(*_args, **_kwargs):  # type: ignore
        """Return empty config if real loader is missing."""
        return {}

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
_BACKUP_DIR = Path("backups")

# Connection stored globally after :func:`connect_db` runs.
_DB_CONN: Optional[aiosqlite.Connection] = None

# Limit concurrent query connections so SQLite isn't overwhelmed.  The default
# value of ``5`` can be overridden via the ``DB_QUERY_CONCURRENCY`` setting in
# ``.env`` so operators can tune performance without touching the code.  The
# semaphore is created at import time; adjusting the value at runtime requires a
# process restart or module reload.
_CONFIG = load_config()
_QUERY_SEMAPHORE = asyncio.Semaphore(
    int(_CONFIG.get("DB_QUERY_CONCURRENCY", 5))
)

# Regular expressions to verify symbol formats.
_SPOT_RE = re.compile(r"^[A-Z0-9]+/[A-Z0-9]+$")
# Futures tickers sometimes carry suffixes like ``-USDT`` or ``PERP``; the
# pattern below allows an optional hyphenated part.
_FUTURES_RE = re.compile(r"^[A-Z0-9]+(-?[A-Z0-9]+)?$")


async def _get_conn() -> aiosqlite.Connection:
    """Return an active connection, reconnecting if needed.

    The function acts like a caretaker that checks if the door to our sticker
    album (the database) is still open. If the door jams or someone slams it
    shut, we quietly open a new one so other functions keep working without
    errors.
    """
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
            funding_rate REAL DEFAULT 0,
            UNIQUE(timestamp, spot_symbol, futures_symbol)
        ) STRICT
        """
    )
    # ``IF NOT EXISTS`` won't add missing columns if the table already exists
    # from an older schema.  We introspect and patch missing fields so queries
    # referencing ``trade_qty`` or ``funding_rate`` don't crash with
    # ``OperationalError: no such column``.
    cur = await conn.execute("PRAGMA table_info(trades)")
    info = await cur.fetchall()
    cols = {row[1] for row in info}
    if "trade_qty" not in cols:
        await conn.execute("ALTER TABLE trades ADD COLUMN trade_qty REAL DEFAULT 0")
    if "funding_rate" not in cols:
        await conn.execute("ALTER TABLE trades ADD COLUMN funding_rate REAL DEFAULT 0")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pair_time ON trades(spot_symbol, futures_symbol, timestamp)"
    )
    await conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_pair_time ON trades(timestamp, spot_symbol, futures_symbol)"
    )
    await conn.commit()


def _cleanup_old_backups(limit: int = 20, max_age_days: int = 30, max_total_mb: int = 500) -> None:
    """Remove aging or excess backup files.

    Backups older than ``max_age_days`` or beyond ``limit`` most recent copies
    are deleted.  If the combined size of backups exceeds ``max_total_mb`` we
    prune the oldest ones until the total drops below the limit.  The work is
    synchronous and intended to be run in a thread.
    """
    backups = sorted(
        _BACKUP_DIR.glob("trades.db.*.bak*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    cutoff = datetime.utcnow() - timedelta(days=max_age_days)
    total_size = 0
    kept: List[Path] = []
    removed = 0
    for p in backups:
        mtime = datetime.utcfromtimestamp(p.stat().st_mtime)
        if mtime < cutoff:
            p.unlink(missing_ok=True)
            removed += 1
            continue
        if len(kept) >= limit:
            p.unlink(missing_ok=True)
            removed += 1
            continue
        kept.append(p)
    for p in kept:
        total_size += p.stat().st_size
    kept_sorted = sorted(kept, key=lambda p: p.stat().st_mtime, reverse=True)
    while total_size / 1_048_576 > max_total_mb and kept_sorted:
        victim = kept_sorted.pop()
        total_size -= victim.stat().st_size
        victim.unlink(missing_ok=True)
        removed += 1
    logger.info(
        "Backup cleanup: kept %d, removed %d, total size %.2f MB",
        len(kept_sorted),
        removed,
        total_size / 1_048_576,
    )


async def checkpoint_wal() -> None:
    """Truncate the Write-Ahead Log to keep it from growing forever."""
    conn = await _get_conn()
    try:
        await conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception as exc:  # pragma: no cover - rare failure
        logger.error("WAL checkpoint failed: %s", exc)
        await notify("Database WAL checkpoint failed", str(exc))


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
    Path(db_path).parent.mkdir(exist_ok=True)
    delay = 0.1
    for attempt in range(1, retries + 1):
        try:
            _DB_CONN = await aiosqlite.connect(db_path)
            await _DB_CONN.execute("PRAGMA journal_mode=WAL")
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
    To keep every batch either fully written or fully discarded we explicitly
    start a transaction (``BEGIN IMMEDIATE``).  If anything goes wrong we roll
    back so no half-complete rows linger.
    """
    if not validate_data(data):
        return
    delay = 0.1
    for attempt in range(1, 4):
        conn = await _get_conn()
        try:
            await conn.execute("BEGIN IMMEDIATE")
            await conn.executemany(
                """
                INSERT INTO trades (
                    timestamp, spot_symbol, futures_symbol,
                    spot_bid, futures_ask, trade_qty, funding_rate
                ) VALUES (:timestamp, :spot_symbol, :futures_symbol,
                          :spot_bid, :futures_ask,
                          :trade_qty, :funding_rate)
                ON CONFLICT(timestamp, spot_symbol, futures_symbol) DO UPDATE SET
                    spot_bid=excluded.spot_bid,
                    futures_ask=excluded.futures_ask,
                    trade_qty=excluded.trade_qty,
                    funding_rate=excluded.funding_rate
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
            try:
                await conn.rollback()
            except Exception:
                pass
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
    parallel: bool = False,
    chunk_days: int = 30,
    max_concurrency: Optional[int] = None,
    as_dataframe: bool = False,
) -> Union[List[Dict[str, Any]], pd.DataFrame]:
    """Return records for a pair between two dates.

    Optional filters narrow results by quantity and funding rate.  Setting
    ``parallel=True`` splits the date range into ``chunk_days`` segments and
    queries them concurrently using separate connections.  ``max_concurrency``
    limits how many of those chunks run at once so SQLite isn't overwhelmed.
    This speeds up large reads when the table grows big while keeping resource
    usage in check.
    """

    base_query = (
        "SELECT timestamp, spot_symbol, futures_symbol, "
        "spot_bid, futures_ask, trade_qty, funding_rate "
        "FROM trades WHERE spot_symbol=? AND futures_symbol=? "
        "AND timestamp BETWEEN ? AND ?"
    )

    def _build_params(start: str, end: str) -> List[Any]:
        params: List[Any] = [spot_symbol, futures_symbol, start, end]
        if min_qty is not None:
            params.append(min_qty)
        if max_qty is not None:
            params.append(max_qty)
        if min_funding is not None:
            params.append(min_funding)
        if max_funding is not None:
            params.append(max_funding)
        return params

    filter_clause = ""
    if min_qty is not None:
        filter_clause += " AND trade_qty >= ?"
    if max_qty is not None:
        filter_clause += " AND trade_qty <= ?"
    if min_funding is not None:
        filter_clause += " AND funding_rate >= ?"
    if max_funding is not None:
        filter_clause += " AND funding_rate <= ?"

    sem = _QUERY_SEMAPHORE if max_concurrency is None else asyncio.Semaphore(max_concurrency)

    async def _run_chunk(start: str, end: str) -> List[Dict[str, Any]]:
        async with sem:
            try:
                async with aiosqlite.connect(str(_DB_PATH)) as conn:
                    query = base_query + filter_clause + " ORDER BY timestamp"
                    cursor = await conn.execute(query, _build_params(start, end))
                    rows = await cursor.fetchall()
                    cols = [c[0] for c in cursor.description]
                    df = pd.DataFrame(rows, columns=cols)
                    return df if as_dataframe else df.to_dict("records")
            except Exception as exc:
                logger.error("Parallel query chunk failed: %s", exc)
                try:
                    await notify("Database query failed", str(exc))
                except Exception:
                    pass
                return []

    if not parallel:
        conn = await _get_conn()
        try:
            query = base_query + filter_clause + " ORDER BY timestamp"
            cursor = await conn.execute(query, _build_params(start_date, end_date))
            rows = await cursor.fetchall()
            cols = [c[0] for c in cursor.description]
            df = pd.DataFrame(rows, columns=cols)
            return df if as_dataframe else df.to_dict("records")
        except Exception as exc:
            logger.error("Query failed: %s", exc)
            try:
                await notify("Database query failed", str(exc))
            except Exception:
                pass
            return []

    # Parallel branch -------------------------------------------------------
    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)
    ranges: List[Sequence[str]] = []
    cur = start_dt
    delta = timedelta(days=chunk_days)
    while cur < end_dt:
        chunk_end = min(cur + delta, end_dt)
        ranges.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + timedelta(microseconds=1)

    tasks = [asyncio.create_task(_run_chunk(s, e)) for s, e in ranges]
    results: List[Dict[str, Any]] = []
    for part in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(part, list):
            results.extend(part)
        else:
            logger.error("Parallel query task error: %s", part)
            try:
                await notify("Database parallel query failed", str(part))
            except Exception:
                pass
    results.sort(key=lambda x: x["timestamp"])
    if as_dataframe:
        return pd.DataFrame(results)
    return results


# ---------------------------------------------------------------------------
# 4. async backup_database
# ---------------------------------------------------------------------------
async def backup_database(compress: bool = True) -> None:
    """Create a timestamped backup copy of the database file.

    The file is optionally compressed with gzip to save space, and we log its
    size before copying to avoid surprises.  The destination directory is
    created lazily to avoid unnecessary I/O during imports.
    """
    _BACKUP_DIR.mkdir(exist_ok=True)
    if not _DB_PATH.exists():
        logger.error("Cannot backup: database missing at %s", _DB_PATH)
        await notify("Database backup failed", "missing db file")
        return
    size_mb = _DB_PATH.stat().st_size / 1_048_576
    logger.info("Database size %.2f MB", size_mb)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_file = _BACKUP_DIR / f"trades.db.{timestamp}.bak"

    def _copy(src: Path, dst: Path, do_compress: bool) -> None:
        """Run the expensive file copy in a background thread.

        For very large databases (>200 MB) we stream the file in small chunks
        and log progress roughly every 50 MB so operators can see that the
        backup is still moving along."""
        size = src.stat().st_size
        if do_compress:
            with open(src, "rb") as fsrc, gzip.open(dst, "wb") as fdst:
                if size > 200 * 1_048_576:
                    copied = 0
                    step = 50 * 1_048_576
                    while True:
                        chunk = fsrc.read(1_048_576)
                        if not chunk:
                            break
                        fdst.write(chunk)
                        copied += len(chunk)
                        if copied // step > (copied - len(chunk)) // step:
                            logger.info(
                                "Backup progress: %.0f%%", copied / size * 100
                            )
                else:
                    shutil.copyfileobj(fsrc, fdst)
        else:
            shutil.copy2(src, dst)

    try:
        if compress:
            backup_file = backup_file.with_suffix(backup_file.suffix + ".gz")
        await asyncio.to_thread(_copy, _DB_PATH, backup_file, compress)
        logger.info("Database backup created at %s", backup_file)
        await asyncio.to_thread(_cleanup_old_backups)
        await checkpoint_wal()
    except Exception as exc:
        logger.error("Database backup failed: %s", exc)
        await notify("Database backup failed", str(exc))


# ---------------------------------------------------------------------------
# 4b. async vacuum_db
# ---------------------------------------------------------------------------
async def vacuum_db() -> None:
    """Compact the database file by running ``VACUUM``.

    While WAL mode already keeps the database stable, an occasional VACUUM
    reclaims disk space after massive deletes.  It is safe to run while no
    other transactions are active and typically only needed during
    maintenance windows.
    """
    conn = await _get_conn()
    try:
        await conn.execute("VACUUM")
        await conn.commit()
        logger.info("Database vacuum completed")
    except Exception as exc:  # pragma: no cover - rare
        logger.error("Database vacuum failed: %s", exc)
        await notify("Database vacuum failed", str(exc))


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
            if not (_SPOT_RE.match(str(item["spot_symbol"])) and _FUTURES_RE.match(str(item["futures_symbol"]))):
                raise ValueError("bad symbol format")
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
