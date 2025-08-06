import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import backtester
import config


def test_simulate_slippage_applies_costs():
    config.CONFIG.update({'SPOT_SLIPPAGE': '0.001', 'FUTURES_SLIPPAGE': '0.001'})
    df = pd.DataFrame({'spot_price': [100.0], 'futures_price': [101.0]})
    adj = backtester.simulate_slippage(df)
    cost = 0.00075 + 0.001
    assert pytest.approx(100.0 * (1 + cost), rel=1e-6) == adj.loc[0, 'spot_exec']
    assert pytest.approx(101.0 * (1 - cost), rel=1e-6) == adj.loc[0, 'futures_exec']


def test_run_backtest_generates_trades():
    config.CONFIG.update({
        'SPOT_SLIPPAGE': '0.001',
        'FUTURES_SLIPPAGE': '0.001',
        'BTC_USDT_BASIS_THRESHOLD_OPEN': '0.005',
        'BTC_USDT_BASIS_THRESHOLD_CLOSE': '0.001',
    })
    df = pd.DataFrame({
        'spot_symbol': ['BTC/USDT', 'BTC/USDT'],
        'spot_price': [100.0, 101.0],
        'futures_symbol': ['BTCUSDT', 'BTCUSDT'],
        'futures_price': [101.0, 100.0],
    })
    result = backtester.run_backtest(df, 'BTC/USDT', 'BTCUSDT')
    assert result['trades'] == 1
    assert isinstance(result['profit'], float)
    assert 'sharpe_ratio' in result
