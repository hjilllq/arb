import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import strategy
import config


def test_calculate_basis_accounts_costs():
    config.CONFIG.update({'SPOT_SLIPPAGE': '0.002', 'FUTURES_SLIPPAGE': '0.003'})
    basis = strategy.calculate_basis('BTC/USDT', 'BTCUSDT', 100.0, 105.0)
    expected = (105.0 - 100.0) / 100.0 - (
        (0.00075 + 0.002) + (0.00075 + 0.003)
    )
    assert pytest.approx(expected, rel=1e-6) == basis


def test_generate_basis_signal():
    config.CONFIG.update({
        'BTC_USDT_BASIS_THRESHOLD_OPEN': '0.01',
        'BTC_USDT_BASIS_THRESHOLD_CLOSE': '0.005',
    })
    sig_buy = strategy.generate_basis_signal('BTC/USDT', 'BTCUSDT', 0.02)
    sig_sell = strategy.generate_basis_signal('BTC/USDT', 'BTCUSDT', -0.01)
    sig_hold = strategy.generate_basis_signal('BTC/USDT', 'BTCUSDT', 0.007)
    assert sig_buy == 'buy'
    assert sig_sell == 'sell'
    assert sig_hold == 'hold'


def test_apply_indicators_and_anomaly_detection():
    data = pd.DataFrame({'spot_price': [100, 101, 102, 103, 104],
                         'futures_price': [100, 101, 102, 103, 104]})
    indicators = strategy.apply_indicators(data)
    assert {'rsi', 'macd', 'ma', 'bollinger'} <= indicators.keys()

    bad = pd.DataFrame({'spot_price': [100, 150], 'futures_price': [100, 101]})
    with pytest.raises(ValueError):
        strategy.apply_indicators(bad)


def test_manage_risk_limits_signal():
    config.CONFIG.update({
        'BTC_USDT_BASIS_THRESHOLD_OPEN': '0.01',
        'BTC_USDT_BASIS_THRESHOLD_CLOSE': '0.005',
        'MAX_BASIS_RISK': '0.02'
    })
    sig = strategy.manage_risk('BTC/USDT', 'BTCUSDT', 0.03)
    assert sig == 'hold'
    sig2 = strategy.manage_risk('BTC/USDT', 'BTCUSDT', 0.015)
    assert sig2 == 'buy'
