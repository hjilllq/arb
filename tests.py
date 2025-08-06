"""Unified smoke tests for the arbitrage bot.

This module uses :mod:`unittest` so developers can quickly verify that the
most critical parts of the system behave as expected. The tests are lightweight
and rely on small mocks so they can run on the Apple Silicon M4 Max without
wasting energy.
"""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

import numpy as np
import pandas as pd

import bybit_api
import config
import database
import ml_predictor
import position_manager
import risk_manager
import strategy


class ArbitrageTests(unittest.IsolatedAsyncioTestCase):
    """Collection of high level checks for all modules."""

    def test_config_loading(self) -> None:
        """Ensure the configuration loads and validates correctly."""
        cfg = config.load_config()
        self.assertTrue(config.get_spot_pairs(), "SPOT_PAIRS should not be empty")
        self.assertTrue(config.get_futures_pairs(), "FUTURES_PAIRS should not be empty")
        mapping = config.get_pair_mapping()
        self.assertEqual(set(mapping.keys()), set(config.get_spot_pairs()))

        # Invalid configuration example: empty pair list should fail validation
        bad_cfg = dict(cfg)
        bad_cfg["SPOT_PAIRS"] = []
        self.assertFalse(config.validate_config(bad_cfg))

    async def test_api_connection(self) -> None:
        """Connect to the mocked Bybit API and exercise error handling."""
        class DummyExchange:
            async def load_markets(self):
                return {}

            async def close(self):
                pass

        def factory(cfg):
            return DummyExchange()

        with mock.patch("bybit_api.ccxt.bybit", factory):
            await bybit_api.connect_api("key", "secret")
            self.assertIsNotNone(bybit_api._client)
            await bybit_api.close_api()

        # Simulate an API failure and ensure the handler sleeps once
        async_sleep = mock.AsyncMock()
        with mock.patch("asyncio.sleep", async_sleep):
            await bybit_api.handle_api_error(Exception("boom"), context="test")
        async_sleep.assert_called()

    async def test_database_operations(self) -> None:
        """Save and query a single trade record."""
        await database.connect_db(":memory:")
        row = {
            "timestamp": "2025-08-05T00:00:00",
            "spot_symbol": "BTC/USDT",
            "futures_symbol": "BTCUSDT",
            "spot_bid": 1.0,
            "futures_ask": 2.0,
            "trade_qty": 1.0,
            "funding_rate": 0.0,
        }
        self.assertTrue(database.validate_data([row]))
        await database.save_data([row])
        res = await database.query_data("BTC/USDT", "BTCUSDT", "2025-08-04", "2025-08-06")
        self.assertEqual(len(res), 1)
        # Invalid row should fail validation
        bad = {**row, "spot_bid": -1}
        self.assertFalse(database.validate_data([bad]))

    def test_spot_futures_arbitrage(self) -> None:
        """Compute basis, signals and indicator edge cases."""
        basis = strategy.calculate_basis("BTC/USDT", "BTCUSDT", 100.0, 105.0)
        self.assertIsInstance(basis, float)
        signal = strategy.generate_basis_signal("BTC/USDT", "BTCUSDT", basis)
        self.assertIn(signal, {"buy", "sell", "hold"})
        # Empty dataframe should raise an error in indicator calculation
        with self.assertRaises(ValueError):
            strategy.apply_indicators(pd.DataFrame())

    async def test_position_management(self) -> None:
        """Open and close a dummy hedged position."""
        async def dummy_place(*args, **kwargs):
            return {"order_id": "1"}

        async def dummy_data(*args, **kwargs):
            return {"spot": {"bid": 1.0, "ask": 1.0}, "futures": {"bid": 1.0, "ask": 1.0}}

        async def async_noop(*args, **kwargs):
            return None

        with mock.patch("position_manager._place_with_retry", side_effect=dummy_place), \
             mock.patch("position_manager.bybit_api.get_spot_futures_data", side_effect=dummy_data), \
             mock.patch("position_manager.logger.log_trade", new=async_noop), \
             mock.patch("position_manager.database.save_data", new=async_noop), \
             mock.patch("position_manager.logger.log_error", new=async_noop), \
             mock.patch("position_manager.logger.log_warning", new=async_noop):
            pos = await position_manager.open_position("BTC/USDT", "BTCUSDT", "buy", 1, 1.0, 1.1)
            self.assertIn("spot_order_id", pos)
            closed = await position_manager.close_position(pos["spot_order_id"], pos["futures_order_id"])
            self.assertTrue(closed)

    def test_ml_predictions(self) -> None:
        """Evaluate the model and ensure low accuracy is handled."""
        df = pd.DataFrame(
            {
                "spot_price": [1.0, 2.0, 3.0],
                "futures_price": [1.1, 2.2, 3.3],
                "rsi": [70, 71, 72],
                "macd": [0.1, 0.2, 0.3],
            }
        )

        class DummyModel:
            def predict(self, x, verbose=0):
                return np.zeros((len(x), 1))

        with mock.patch("ml_predictor._load_model", return_value=DummyModel()), \
             mock.patch("ml_predictor.data_analyzer.preprocess_data", lambda records: df):
            accuracy = ml_predictor.evaluate_model(df)
        self.assertLess(accuracy, 0.85)

    async def test_risk_management(self) -> None:
        """Check basic risk calculations and limits."""
        size = risk_manager.calculate_position_size(1000, 0.1)
        self.assertEqual(size, 100)
        df = pd.DataFrame({"spot_price": [1, 2, 3], "futures_price": [1, 2.2, 3.3]})
        vol = await risk_manager.check_volatility(df)
        self.assertGreaterEqual(vol, 0)
        paused = await risk_manager.pause_trading_on_high_volatility(1.0)
        self.assertTrue(paused)
        allowed = await risk_manager.enforce_risk_limits(-100, 1000)
        self.assertFalse(allowed)


if __name__ == "__main__":  # pragma: no cover - manual execution
    unittest.main()
