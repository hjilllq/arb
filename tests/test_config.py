import json
import sys
import types
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
import config


def test_load_config_missing_file(tmp_path, monkeypatch):
    missing = tmp_path / "missing.env"
    monkeypatch.setattr(config, "dotenv_values", lambda *a, **k: {})
    monkeypatch.setattr(config, "os", types.SimpleNamespace(environ={}))
    with pytest.raises(config.ConfigError):
        config.load_config(str(missing), reload=True)


def test_load_config_thresholds_json(tmp_path):
    env = tmp_path / ".env"
    env.write_text("SPOT_PAIRS=['BTC/USDT']\nFUTURES_PAIRS=['BTCUSDT']\nTHRESHOLDS_FILE=extra.json\n")
    extra = tmp_path / "extra.json"
    extra.write_text(json.dumps({
        'BTC_USDT_BASIS_THRESHOLD_OPEN': 0.005,
        'BTC_USDT_BASIS_THRESHOLD_CLOSE': 0.001
    }))
    cfg = config.load_config(str(env), reload=True)
    assert cfg['BTC_USDT_BASIS_THRESHOLD_OPEN'] == '0.005'
    assert config.validate_config(cfg)


def test_validate_config_pair_mismatch():
    bad = {
        'SPOT_PAIRS': "['BTC/USDT']",
        'FUTURES_PAIRS': "['ETHUSDT']",
        'BTC_USDT_BASIS_THRESHOLD_OPEN': '0.005',
        'BTC_USDT_BASIS_THRESHOLD_CLOSE': '0.001'
    }
    assert not config.validate_config(bad)
