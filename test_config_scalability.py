from cryptography.fernet import Fernet
from config import load_config, validate_config, encrypt_value


def test_load_config_with_pairs(tmp_path):
    key = Fernet.generate_key().decode()
    api_key = encrypt_value("key", key)
    api_secret = encrypt_value("sec", key)
    env_path = tmp_path / ".env"
    env_path.write_text(
        f"FERNET_KEY={key}\n"
        f"BYBIT_API_KEY_ENC={api_key}\n"
        f"BYBIT_API_SECRET_ENC={api_secret}\n"
        "ARBITRAGE_SYMBOL=BTCUSDT\n"
        "ARBITRAGE_THRESHOLD=0.01\n"
        "MAX_POSITION_SIZE=1\n"
        "MAX_OPEN_POSITIONS=1\n"
        "MAX_TOTAL_LOSS=10\n"
        "PROFIT_ALERT=5\nLOSS_ALERT=3\n"
        "TRADING_PAIRS=BTCUSDT:BTCUSDT,ETHUSDT:ETHUSDT\n"
        "RSI_PERIOD=10\nRSI_OVERBOUGHT=80\nRSI_OVERSOLD=20\n"
        "MACD_FAST=10\nMACD_SLOW=20\nMACD_SIGNAL=5\n"
    )
    cfg = load_config(str(env_path))
    validate_config(cfg)
    assert cfg.trading_pairs == [
        ("bybit", "BTCUSDT", "BTCUSDT"),
        ("bybit", "ETHUSDT", "ETHUSDT"),
    ]
    assert cfg.rsi_period == 10
    assert cfg.rsi_overbought == 80
    assert cfg.rsi_oversold == 20
    assert cfg.macd_fast == 10
    assert cfg.macd_slow == 20
    assert cfg.macd_signal == 5
    assert cfg.profit_alert == 5
    assert cfg.loss_alert == 3
