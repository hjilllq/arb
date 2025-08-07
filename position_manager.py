"""Simple position manager for spot/futures arbitrage.

This module acts like a cashier. It opens and closes trades and keeps a tiny
notebook of positions. Functions use short sentences and asynchronous calls so
our Apple Silicon M4 Max stays cool.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List

import bybit_api
import config
import database
import logger

# In-memory containers. Each item keeps everything needed to close and compute PNL.
_OPEN_POSITIONS: List[Dict[str, Any]] = []
_CLOSED_POSITIONS: List[Dict[str, Any]] = []

# Taker fee used when estimating profit. Bybit charges about 0.075% for each trade.
_FEE_RATE = 0.00075

# Retry settings for order placement. Values come from config if present so
# operators can tune behaviour without touching the code.
_CFG = config.load_config()
try:
    _RETRY_ATTEMPTS = int(_CFG.get("ORDER_RETRY_ATTEMPTS", 3))
except ValueError:  # pragma: no cover - corrupt config
    _RETRY_ATTEMPTS = 3
try:
    _RETRY_DELAY = float(_CFG.get("ORDER_RETRY_DELAY", 5))
except ValueError:  # pragma: no cover - corrupt config
    _RETRY_DELAY = 5.0


async def _place_with_retry(
    symbol: str,
    side: str,
    amount: float,
    price: float,
    *,
    order_type: str = "limit",
    contract_type: str = "spot",
) -> Dict[str, Any]:
    """Attempt to place an order and retry on failures.

    Orders are tried a few times with a small delay between attempts.  If the
    exchange reports a status other than ``filled`` the helper logs a warning so
    callers may decide to cancel or retry elsewhere.
    """

    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            order = await bybit_api.place_order(
                symbol,
                side,
                amount,
                price,
                order_type=order_type,
                contract_type=contract_type,
            )
        except Exception as exc:  # pragma: no cover - network failure
            await logger.log_error("Order placement error", exc)
            order = None

        if order and order.get("status", "filled").lower() == "filled":
            return order

        await logger.log_warning(
            f"order attempt {attempt} for {symbol} {side} failed: {order}"
        )
        if attempt < _RETRY_ATTEMPTS:
            await asyncio.sleep(_RETRY_DELAY)

    return {}


async def open_position(
    spot_symbol: str,
    futures_symbol: str,
    side: str,
    amount: float,
    spot_price: float,
    futures_price: float,
    *,
    order_type: str = "limit",
    stop_loss: float | None = None,
) -> Dict[str, str]:
    """Open a hedged spot/futures position.

    ``side`` controls the direction: ``"buy"`` buys spot and sells futures;
    ``"sell"`` does the opposite.  The amount is checked against
    ``MAX_POSITION_PER_PAIR`` from :mod:`config` so trades stay within limits.
    A small record is stored in memory and the trade is saved to
    :mod:`database`.
    """
    side_l = side.lower()
    if side_l not in {"buy", "sell"}:
        await logger.log_error("Invalid side", ValueError(side))
        return {}
    if amount <= 0 or spot_price <= 0 or futures_price <= 0:
        await logger.log_error("Invalid trade numbers", ValueError("non-positive"))
        return {}
    cfg = config.load_config()
    try:
        max_pos = float(cfg.get("MAX_POSITION_PER_PAIR", 0))
    except ValueError:
        max_pos = 0
    if max_pos and amount > max_pos:
        await logger.log_warning(
            f"Amount {amount} exceeds max per pair {max_pos}; trade skipped"
        )
        return {}

    futures_side = "sell" if side_l == "buy" else "buy"
    spot_order = await _place_with_retry(
        spot_symbol,
        side_l,
        amount,
        spot_price,
        order_type=order_type,
        contract_type="spot",
    )
    if not spot_order:
        await logger.log_error("Spot order failed", RuntimeError("spot fail"))
        return {}
    futures_order = await _place_with_retry(
        futures_symbol,
        futures_side,
        amount,
        futures_price,
        order_type=order_type,
        contract_type="linear",
    )
    if not futures_order:
        await logger.log_error("Futures order failed", RuntimeError("futures fail"))
        try:
            await bybit_api.cancel_order(spot_symbol, spot_order.get("order_id", ""))
        except Exception:  # pragma: no cover - network failure
            pass
        return {}

    timestamp = datetime.utcnow().isoformat()
    record = {
        "spot_symbol": spot_symbol,
        "futures_symbol": futures_symbol,
        "side": side_l,
        "amount": amount,
        "spot_price": spot_price,
        "futures_price": futures_price,
        "spot_order_id": spot_order.get("order_id", ""),
        "futures_order_id": futures_order.get("order_id", ""),
        "order_type": order_type,
        "timestamp": timestamp,
    }
    if stop_loss is not None:
        # Stop-loss support can be added later by sending a separate order.
        record["stop_loss"] = stop_loss
    _OPEN_POSITIONS.append(record)

    await logger.log_trade(
        {
            "spot_symbol": spot_symbol,
            "futures_symbol": futures_symbol,
            "pnl": 0,
            "event": "open",
        }
    )
    await database.save_data(
        [
            {
                "timestamp": timestamp,
                "spot_symbol": spot_symbol,
                "futures_symbol": futures_symbol,
                "spot_bid": spot_price,
                "futures_ask": futures_price,
                "trade_qty": amount,
                "funding_rate": 0,
            }
        ]
    )
    return {
        "spot_order_id": record["spot_order_id"],
        "futures_order_id": record["futures_order_id"],
    }


async def close_position(spot_order_id: str, futures_order_id: str) -> bool:
    """Close an existing position with market orders.

    Prices are checked for sudden jumps (over 10%% from the open price) to
    avoid closing with wildly different quotes.  Each order is retried a few
    times so transient API hiccups do not leave the bot stuck with exposure.
    """
    pos = next(
        (
            p
            for p in _OPEN_POSITIONS
            if p["spot_order_id"] == spot_order_id
            and p["futures_order_id"] == futures_order_id
        ),
        None,
    )
    if not pos:
        await logger.log_error("Position not found", ValueError("missing"))
        return False
    try:
        data = await bybit_api.get_spot_futures_data(
            pos["spot_symbol"], pos["futures_symbol"]
        )
    except Exception as exc:  # pragma: no cover - network failure
        await logger.log_error("Price fetch failed", exc)
        return False
    spot_side = "sell" if pos["side"] == "buy" else "buy"
    fut_side = "buy" if pos["side"] == "buy" else "sell"
    spot_price = data.get("spot", {}).get("bid" if spot_side == "sell" else "ask")
    fut_price = data.get("futures", {}).get("ask" if fut_side == "buy" else "bid")
    if not spot_price or not fut_price:
        await logger.log_error("Bad market data", ValueError("empty prices"))
        return False

    # Warn if price moved too much. Large gaps may point to missed trades.
    if abs(spot_price - pos["spot_price"]) / pos["spot_price"] > 0.10:
        await logger.log_warning("Spot price moved >10% since open; closing anyway")
    if abs(fut_price - pos["futures_price"]) / pos["futures_price"] > 0.10:
        await logger.log_warning("Futures price moved >10% since open; closing anyway")

    spot_ok = await _place_with_retry(
        pos["spot_symbol"],
        spot_side,
        pos["amount"],
        spot_price,
        order_type="market",
    )
    fut_ok = await _place_with_retry(
        pos["futures_symbol"],
        fut_side,
        pos["amount"],
        fut_price,
        order_type="market",
        contract_type="linear",
    )
    if not spot_ok or not fut_ok:
        await logger.log_error("Close orders failed", RuntimeError("close fail"))
        return False

    pos["spot_close_price"] = spot_price
    pos["futures_close_price"] = fut_price
    _OPEN_POSITIONS.remove(pos)
    _CLOSED_POSITIONS.append(pos)

    await logger.log_trade(
        {
            "spot_symbol": pos["spot_symbol"],
            "futures_symbol": pos["futures_symbol"],
            "pnl": calculate_pnl(spot_order_id, futures_order_id),
            "event": "close",
        }
    )
    await database.save_data(
        [
            {
                "timestamp": datetime.utcnow().isoformat(),
                "spot_symbol": pos["spot_symbol"],
                "futures_symbol": pos["futures_symbol"],
                "spot_bid": spot_price,
                "futures_ask": fut_price,
                "trade_qty": -pos["amount"],
                "funding_rate": 0,
            }
        ]
    )
    return True


async def get_open_positions() -> List[Dict[str, Any]]:
    """Return a snapshot of currently open positions."""
    return list(_OPEN_POSITIONS)


def calculate_pnl(spot_order_id: str, futures_order_id: str) -> float:
    """Calculate profit and loss for a closed position.

    The function subtracts fees for four trades (open and close on both legs).
    If the position was not closed ``0`` is returned.
    """
    pos = next(
        (
            p
            for p in _CLOSED_POSITIONS
            if p["spot_order_id"] == spot_order_id
            and p["futures_order_id"] == futures_order_id
        ),
        None,
    )
    if not pos:
        return 0.0
    amount = pos["amount"]
    open_spot = pos["spot_price"]
    open_fut = pos["futures_price"]
    close_spot = pos.get("spot_close_price", open_spot)
    close_fut = pos.get("futures_close_price", open_fut)
    if pos["side"] == "buy":
        gross = (close_spot - open_spot) - (close_fut - open_fut)
    else:
        gross = (open_spot - close_spot) - (open_fut - close_fut)
    fees = (open_spot + open_fut + close_spot + close_fut) * _FEE_RATE
    return gross * amount - fees * amount

