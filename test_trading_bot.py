import asyncio
import threading
import pytest
import trading_bot
from trading_bot import TradingBot
from strategy import ArbitrageStrategy
from risk_manager import RiskManager


class DummyExchange:
    def __init__(self):
        self.orders = []
        self.attempts = {}
        self.open_orders = []
        self.cancelled = []

    async def get_spot_data(self, pair):
        return {"price": 100.0}

    async def get_futures_data(self, pair):
        return {"price": 105.0}

    async def place_order(self, pair, price, qty, side="Buy", order_type="Limit"):
        self.attempts[pair] = self.attempts.get(pair, 0) + 1
        order = {"id": len(self.orders) + 1, "pair": pair, "price": price, "qty": qty, "side": side}
        self.orders.append(order)
        return order

    async def get_order_status(self, order_id, pair=None):
        return {"status": "Filled"}

    async def check_balance(self):
        return {"USDT": 1000.0}

    async def get_open_orders(self, pair=None):
        return [o for o in self.open_orders if pair is None or o["pair"] == pair]

    async def cancel_order(self, order_id, pair=None):
        self.cancelled.append(order_id)
        self.open_orders = [o for o in self.open_orders if o["id"] != order_id]
        return {"status": "Cancelled"}


def test_start_and_stop_trading():
    bot = TradingBot(DummyExchange(), ArbitrageStrategy(basis_threshold=1.0))
    bot.start_trading()
    assert bot.active
    bot.stop_trading()
    assert not bot.active


def test_execute_trade_and_monitor(monkeypatch):
    exchange = DummyExchange()
    bot = TradingBot(exchange, ArbitrageStrategy(basis_threshold=1.0))
    logged = {}

    def fake_log_trade_data(info):
        logged.update(info)

    def fake_log_system_health(info):
        logged["health"] = info

    monkeypatch.setattr(trading_bot, "log_trade_data", fake_log_trade_data)
    monkeypatch.setattr(trading_bot, "log_system_health", fake_log_system_health)

    orders = asyncio.run(bot.execute_trade("BTCUSDT", "BTCUSDT-FUT", 1.0))
    assert len(orders) == 2
    spot_order, fut_order = exchange.orders
    assert spot_order["side"] == "Sell"
    assert pytest.approx(spot_order["price"]) == 100.0 * (1 - bot.slippage)
    assert fut_order["side"] == "Buy"
    assert pytest.approx(fut_order["price"]) == 105.0 * (1 + bot.slippage)
    assert orders["spot"]["fee"] == pytest.approx(spot_order["price"] * bot.fee_rate)

    health = asyncio.run(bot.monitor_bot_health())
    assert health["balance"]["USDT"] == 1000.0
    assert logged["health"]["balance"]["USDT"] == 1000.0


def test_order_retry_on_failure():
    class FlakyExchange(DummyExchange):
        async def place_order(self, pair, price, qty, side="Buy", order_type="Limit"):
            count = self.attempts.get(pair, 0) + 1
            self.attempts[pair] = count
            if pair == "BTCUSDT" and count == 1:
                raise Exception("temp error")
            order = {"id": len(self.orders) + 1, "pair": pair, "price": price, "qty": qty, "side": side}
            self.orders.append(order)
            return order

    exchange = FlakyExchange()
    bot = TradingBot(exchange, ArbitrageStrategy(basis_threshold=1.0))
    orders = asyncio.run(bot.execute_trade("BTCUSDT", "BTCUSDT-FUT", 1.0))
    assert len(orders) == 2
    assert exchange.attempts["BTCUSDT"] == 2


def test_skip_trade_on_anomaly(monkeypatch):
    exchange = DummyExchange()
    bot = TradingBot(exchange, ArbitrageStrategy(basis_threshold=0.0))
    bot.price_history = [100.0] * 20

    async def fake_apply_strategy(self, exch, sp, fp):
        return {"signal": 1, "spot_price": 1000.0, "futures_price": 1005.0}

    monkeypatch.setattr(ArbitrageStrategy, "apply_strategy", fake_apply_strategy)
    orders = asyncio.run(bot.execute_trade("BTCUSDT", "BTCUSDT-FUT", 1.0))
    assert orders == {}
    assert exchange.orders == []


def test_anomaly_detection_runs_in_thread(monkeypatch):
    exchange = DummyExchange()
    bot = TradingBot(exchange, ArbitrageStrategy(basis_threshold=0.0))
    bot.price_history = [100.0] * 4

    thread_name: dict[str, str] = {}

    async def fake_apply_strategy(self, exch, sp, fp):
        return {"signal": 1, "spot_price": 100.0, "futures_price": 105.0}

    def fake_detect(data):
        thread_name["name"] = threading.current_thread().name
        return [len(data) - 1]

    monkeypatch.setattr(ArbitrageStrategy, "apply_strategy", fake_apply_strategy)
    monkeypatch.setattr(trading_bot, "detect_anomalies", fake_detect)

    asyncio.run(bot.execute_trade("BTCUSDT", "BTCUSDT-FUT", 1.0))
    assert "ThreadPoolExecutor" in thread_name.get("name", "")


def test_pause_on_high_volatility(monkeypatch):
    exchange = DummyExchange()
    risk = RiskManager(volatility_threshold=0.1, volatility_window=2)
    bot = TradingBot(exchange, ArbitrageStrategy(basis_threshold=0.0), risk_manager=risk)

    async def fake_apply_strategy(self, exch, sp, fp):
        return {"signal": 1, "spot_price": 200.0, "futures_price": 205.0}

    monkeypatch.setattr(ArbitrageStrategy, "apply_strategy", fake_apply_strategy)
    risk._prices = [100.0]
    orders = asyncio.run(bot.execute_trade("BTCUSDT", "BTCUSDT-FUT", 1.0))
    assert orders == {}
    assert risk.trading_paused


def test_trade_multiple_pairs():
    exchange = DummyExchange()
    pairs = [("BTCUSDT", "BTCUSDT-FUT"), ("ETHUSDT", "ETHUSDT-FUT")]
    bot = TradingBot(
        exchange,
        ArbitrageStrategy(basis_threshold=1.0),
        trading_pairs=pairs,
    )

    async def runner():
        await bot.trade_multiple_pairs(1.0)

    asyncio.run(runner())
    symbols = {order["pair"] for order in exchange.orders}
    assert symbols == {"BTCUSDT", "BTCUSDT-FUT", "ETHUSDT", "ETHUSDT-FUT"}


def test_cancel_open_orders_and_record_positions():
    exchange = DummyExchange()
    # существующий ордер, который должен быть отменён
    exchange.open_orders = [{"id": 1, "pair": "BTCUSDT", "price": 99.0}]
    pm = trading_bot.PositionManager()
    bot = TradingBot(exchange, ArbitrageStrategy(basis_threshold=1.0), position_manager=pm)
    asyncio.run(bot.execute_trade("BTCUSDT", "BTCUSDT-FUT", 1.0))
    # ордер отменён
    assert 1 in exchange.cancelled
    # позиции открыты
    pos_spot = pm.positions["BTCUSDT"]
    assert pos_spot.side in {"long", "short"}


def test_skip_trade_on_low_balance():
    class PoorExchange(DummyExchange):
        async def check_balance(self):
            return {"USDT": 1.0}

    exchange = PoorExchange()
    bot = TradingBot(exchange, ArbitrageStrategy(basis_threshold=1.0))
    orders = asyncio.run(bot.execute_trade("BTCUSDT", "BTCUSDT-FUT", 1.0))
    assert orders == {}
    assert exchange.orders == []
