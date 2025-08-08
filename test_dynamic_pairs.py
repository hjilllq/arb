from trading_bot import TradingBot
from notification_manager import NotificationManager


class DummyExchange:
    async def get_spot_data(self, *args, **kwargs):
        return {"price": 100.0}

    async def get_futures_data(self, *args, **kwargs):
        return {"price": 101.0}

    async def place_order(self, pair, price, qty, side, **kwargs):
        return {"id": "1", "side": side, "qty": qty, "price": price, "pair": pair}

    async def get_order_status(self, *args, **kwargs):
        return {"status": "filled"}

    async def cancel_order(self, *args, **kwargs):
        return {}

    async def check_balance(self, *args, **kwargs):
        return {"USDT": 1000}

    async def get_open_orders(self, *args, **kwargs):
        return []


def test_update_trading_pairs(monkeypatch):
    notifier = NotificationManager()
    messages = []
    monkeypatch.setattr(notifier, "log_notification", lambda ch, msg: messages.append((ch, msg)))
    bot = TradingBot(DummyExchange(), notifier=notifier)
    bot.trading_pairs = [("bybit", "BTCUSDT", "BTCUSDT-FUT")]

    new_pairs = [
        ("bybit", "BTCUSDT", "BTCUSDT-FUT"),
        ("okx", "ETH-USDT", "ETH-USDT-SWAP"),
    ]
    bot.update_trading_pairs(new_pairs)
    assert ("pairs", "added okx:ETH-USDT:ETH-USDT-SWAP") in messages
    bot.update_trading_pairs([("okx", "ETH-USDT", "ETH-USDT-SWAP")])
    assert ("pairs", "removed bybit:BTCUSDT:BTCUSDT-FUT") in messages
