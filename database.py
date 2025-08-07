"""SQLite-backed storage for trade data.

This module provides a small wrapper around :mod:`sqlite3` for persisting
executed trades and retrieving historical statistics. It is intended as a
lightweight persistence layer for arbitrage strategies.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import shutil
import sqlite3


@dataclass
class TradeDatabase:
    """Simple interface for storing and querying trade information.

    Parameters
    ----------
    db_path:
        Location of the SQLite database file. Defaults to ``"trades.db"``.
    """

    db_path: str = "trades.db"

    def __post_init__(self) -> None:
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair TEXT NOT NULL,
                    price REAL NOT NULL,
                    qty REAL NOT NULL,
                    side TEXT NOT NULL,
                    pnl REAL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def save_trade_data(
        self,
        pair: str,
        price: float,
        qty: float,
        side: str,
        pnl: float = 0.0,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Store a single trade entry in the database."""
        ts = timestamp or datetime.utcnow()
        with self.conn:
            self.conn.execute(
                "INSERT INTO trades (pair, price, qty, side, pnl, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                (pair, price, qty, side, pnl, ts.isoformat()),
            )

    def get_trade_history(self, pair: str) -> List[Dict[str, Any]]:
        """Return ordered trade records for ``pair``."""
        cursor = self.conn.execute(
            "SELECT pair, price, qty, side, pnl, timestamp FROM trades WHERE pair = ? ORDER BY timestamp",
            (pair,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_trade_statistics(self, pair: Optional[str] = None) -> Dict[str, float]:
        """Return aggregate statistics for all trades or a specific pair."""
        query = "SELECT COUNT(*) AS count, SUM(pnl) AS total_pnl, SUM(qty) AS total_qty FROM trades"
        params: tuple[Any, ...] = ()
        if pair is not None:
            query += " WHERE pair = ?"
            params = (pair,)
        row = self.conn.execute(query, params).fetchone()
        return {
            "count": row["count"] or 0,
            "total_pnl": row["total_pnl"] or 0.0,
            "total_qty": row["total_qty"] or 0.0,
        }

    def clear_old_data(self, days: int) -> int:
        """Remove trades older than ``days`` days and return number removed."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.conn:
            cursor = self.conn.execute(
                "DELETE FROM trades WHERE timestamp < ?",
                (cutoff.isoformat(),),
            )
            return cursor.rowcount

    def backup_database(self, backup_path: str) -> str:
        """Create a file copy of the database at ``backup_path``."""
        self.conn.commit()
        shutil.copy2(self.db_path, backup_path)
        return backup_path

    def close(self) -> None:
        """Close the underlying database connection."""
        self.conn.close()
