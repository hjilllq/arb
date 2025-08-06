import json
import sys
import types
from pathlib import Path
from datetime import datetime
import os
import json

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


def test_validate_config_negative_threshold():
    bad = {
        'SPOT_PAIRS': "['BTC/USDT']",
        'FUTURES_PAIRS': "['BTCUSDT']",
        'BTC_USDT_BASIS_THRESHOLD_OPEN': '-0.005',
        'BTC_USDT_BASIS_THRESHOLD_CLOSE': '0.001'
    }
    assert not config.validate_config(bad)


def test_load_config_thresholds_json_invalid(tmp_path):
    env = tmp_path / ".env"
    env.write_text("SPOT_PAIRS=['BTC/USDT']\nFUTURES_PAIRS=['BTCUSDT']\nTHRESHOLDS_FILE=extra.json\n")
    extra = tmp_path / "extra.json"
    extra.write_text("{bad json")
    cfg = config.load_config(str(env), reload=True)
    assert 'BTC_USDT_BASIS_THRESHOLD_OPEN' not in cfg


def test_encrypt_decrypt_roundtrip():
    data = {'API_KEY': 'abc', 'API_SECRET': 'xyz'}
    enc = config.encrypt_config(data)
    dec = config.decrypt_config(enc)
    assert dec == data


def test_update_and_backup(tmp_path, monkeypatch):
    env = tmp_path / '.env'
    env.write_text('API_KEY=foo\n')
    backup_dir = tmp_path / 'backups'
    backup_dir.mkdir()
    monkeypatch.setattr(config, '_ENV_PATH', env)
    monkeypatch.setattr(config, '_BACKUP_DIR', backup_dir)
    monkeypatch.setattr(config, 'CONFIG', {'API_KEY': 'foo'})
    import asyncio
    asyncio.run(config.update_config('API_KEY', 'bar'))
    assert 'API_KEY=bar' in env.read_text()
    asyncio.run(config.backup_config())
    assert any(backup_dir.iterdir())


def test_backup_cleanup(tmp_path, monkeypatch):
    env = tmp_path / '.env'
    env.write_text('API_KEY=foo\n')
    backup_dir = tmp_path / 'backups'
    backup_dir.mkdir()
    monkeypatch.setattr(config, '_ENV_PATH', env)
    monkeypatch.setattr(config, '_BACKUP_DIR', backup_dir)
    # allow many backups for first run, then restrict to 1 to test cleanup
    monkeypatch.setattr(
        config, 'CONFIG', {'BACKUP_RETENTION_DAYS': 365, 'MAX_BACKUPS': 10, 'API_KEY': 'foo'}
    )
    import asyncio
    asyncio.run(config.backup_config())
    assert any(backup_dir.glob('*.bak'))
    config.CONFIG['MAX_BACKUPS'] = 1
    asyncio.run(config.backup_config())
    backups = list(backup_dir.glob('*.bak'))
    assert len(backups) <= 1


def test_cache_ttl_helpers(monkeypatch):
    monkeypatch.setattr(config, 'CONFIG', {'CACHE_TTL_SECONDS': '42', 'TICKER_CACHE_TTL_SECONDS': '2.5'})
    assert config.get_cache_ttl() == 42
    assert config.get_ticker_cache_ttl() == 2.5
