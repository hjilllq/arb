import logging

from cryptography.fernet import Fernet

from logger import (
    configure_logging,
    log_debug,
    log_error,
    log_event,
    log_system_health,
    log_trade_data,
    log_warning,
)


def test_log_event(caplog):
    """Проверить запись обычного сообщения."""
    with caplog.at_level(logging.INFO, logger="arb"):
        log_event("привет")
    assert "привет" in caplog.text


def test_log_error_with_exception(caplog):
    """Убедиться, что исключение попадает в лог ошибок."""
    with caplog.at_level(logging.ERROR, logger="arb"):
        log_error("ошибка", ValueError("boom"))
    assert "ошибка" in caplog.text and "boom" in caplog.text


def test_sanitize_sensitive_data(caplog):
    """Проверить, что конфиденциальные поля маскируются."""
    with caplog.at_level(logging.INFO, logger="arb"):
        log_trade_data({"api_key": "secret", "price": 1})
    assert "***" in caplog.text and "secret" not in caplog.text


def test_log_system_health(caplog):
    """Проверить логирование метрик системы."""
    with caplog.at_level(logging.INFO, logger="arb"):
        log_system_health({"token": "abc", "status": "ok"})
    assert "SYSTEM HEALTH" in caplog.text and "***" in caplog.text


def test_log_levels(caplog):
    """Проверить работу уровней WARNING и DEBUG."""
    with caplog.at_level(logging.DEBUG, logger="arb"):
        log_warning("warn")
        log_debug("dbg")
    assert "warn" in caplog.text and "dbg" in caplog.text


def test_encrypted_file_logging(tmp_path):
    """Проверить, что строки в файле логов шифруются."""
    key = Fernet.generate_key()
    log_file = tmp_path / "enc.log"
    configure_logging("INFO", str(log_file), key.decode())
    log_trade_data({"api_key": "secret", "price": 1})
    content = log_file.read_text().strip()
    assert "TRADE DATA" not in content and "secret" not in content
    decrypted = Fernet(key).decrypt(content.encode()).decode()
    assert "TRADE DATA" in decrypted and "***" in decrypted
