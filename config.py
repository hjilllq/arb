"""Инструменты конфигурации проекта.

Этот модуль загружает значения настроек из файла ``.env`` и проверяет,
что присутствуют все обязательные параметры для работы арбитражного бота."""

from __future__ import annotations

from dataclasses import dataclass, replace, field
from typing import Any, List, Tuple
import os

from cryptography.fernet import Fernet
from dotenv import load_dotenv
from logger import log_error, log_event


@dataclass
class Config:
    """Контейнер для значений конфигурации во время работы."""
    bybit_api_key: str
    bybit_api_secret: str
    arbitrage_symbol: str
    arbitrage_threshold: float
    max_position_size: float
    max_open_positions: int
    max_total_loss: float
    daily_loss_soft_pct: float
    daily_loss_pct: float
    profit_alert: float
    loss_alert: float
    trading_pairs: List[Tuple[str, str]]
    rsi_period: int
    rsi_overbought: float
    rsi_oversold: float
    macd_fast: int
    macd_slow: int
    macd_signal: int
    slack_webhook_url: str
    telegram_token: str
    telegram_chat_id: str
    email_sender: str
    email_host: str
    email_port: int
    sms_api_url: str
    log_level: str
    accounts: List["AccountConfig"] = field(default_factory=list)


@dataclass
class AccountConfig:
    """Описание отдельного аккаунта Bybit."""

    name: str
    api_key: str
    api_secret: str


def _decrypt(value: str, fernet: Fernet) -> str:
    return fernet.decrypt(value.encode()).decode()


def encrypt_value(value: str, key: str) -> str:
    """Вернуть шифротекст для ``value`` с использованием ключа ``key``."""
    fernet = Fernet(key.encode() if isinstance(key, str) else key)
    return fernet.encrypt(value.encode()).decode()


def notify_security_issue(message: str) -> None:
    """Сообщить пользователю о проблеме безопасности через лог и уведомление."""
    log_error(message)
    try:
        from notification_manager import NotificationManager

        NotificationManager().log_notification("security", message)
    except Exception:
        # Если отправка уведомления невозможна, мы ограничиваемся записью в лог
        pass


def load_config(path: str = ".env") -> Config:
    """Загрузить значения настроек из ``path`` и вернуть объект конфигурации."""

    load_dotenv(dotenv_path=path)

    for plain in ("BYBIT_API_KEY", "BYBIT_API_SECRET"):
        if os.getenv(plain):
            notify_security_issue(
                f"Plaintext credential {plain} detected; please encrypt it"
            )
            raise ValueError("Plaintext API credentials detected in environment")

    fernet_key = os.getenv("FERNET_KEY")
    if not fernet_key:
        notify_security_issue("FERNET_KEY is missing in environment")
        raise ValueError("FERNET_KEY is required")

    fernet = Fernet(fernet_key.encode())
    try:
        api_key = _decrypt(os.getenv("BYBIT_API_KEY_ENC", ""), fernet)
        api_secret = _decrypt(os.getenv("BYBIT_API_SECRET_ENC", ""), fernet)
    except Exception as exc:
        notify_security_issue("Failed to decrypt API credentials")
        raise ValueError("Unable to decrypt API credentials") from exc
    accounts: List[AccountConfig] = []
    accounts_env = os.getenv("BYBIT_ACCOUNTS", "")
    for name in [a.strip() for a in accounts_env.split(",") if a.strip()]:
        key_var = f"BYBIT_API_KEY_{name.upper()}_ENC"
        sec_var = f"BYBIT_API_SECRET_{name.upper()}_ENC"
        enc_key = os.getenv(key_var, "")
        enc_sec = os.getenv(sec_var, "")
        if not enc_key or not enc_sec:
            notify_security_issue(f"Credentials for account {name} missing")
            continue
        try:
            dec_key = _decrypt(enc_key, fernet)
            dec_sec = _decrypt(enc_sec, fernet)
            accounts.append(AccountConfig(name=name, api_key=dec_key, api_secret=dec_sec))
        except Exception:
            notify_security_issue(f"Failed to decrypt credentials for {name}")

    cfg = Config(
        bybit_api_key=api_key,
        bybit_api_secret=api_secret,
        accounts=accounts if accounts else [AccountConfig("default", api_key, api_secret)],
        arbitrage_symbol=os.getenv("ARBITRAGE_SYMBOL", ""),
        arbitrage_threshold=float(os.getenv("ARBITRAGE_THRESHOLD", "0")),
        max_position_size=float(os.getenv("MAX_POSITION_SIZE", "0")),
        max_open_positions=int(os.getenv("MAX_OPEN_POSITIONS", "0")),
        max_total_loss=float(os.getenv("MAX_TOTAL_LOSS", "0")),
        daily_loss_soft_pct=float(os.getenv("DAILY_LOSS_SOFT_PCT", "0")),
        daily_loss_pct=float(os.getenv("DAILY_LOSS_PCT", "5")),
        profit_alert=float(os.getenv("PROFIT_ALERT", "0")),
        loss_alert=float(os.getenv("LOSS_ALERT", "0")),
        trading_pairs=[
            tuple(pair.split(":", 1)) if ":" in pair else (pair, pair)
            for pair in os.getenv("TRADING_PAIRS", "").split(",")
            if pair
        ],
        rsi_period=int(os.getenv("RSI_PERIOD", "14")),
        rsi_overbought=float(os.getenv("RSI_OVERBOUGHT", "70")),
        rsi_oversold=float(os.getenv("RSI_OVERSOLD", "30")),
        macd_fast=int(os.getenv("MACD_FAST", "12")),
        macd_slow=int(os.getenv("MACD_SLOW", "26")),
        macd_signal=int(os.getenv("MACD_SIGNAL", "9")),
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
        telegram_token=os.getenv("TELEGRAM_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        email_sender=os.getenv("EMAIL_SENDER", ""),
        email_host=os.getenv("EMAIL_HOST", "localhost"),
        email_port=int(os.getenv("EMAIL_PORT", "25")),
        sms_api_url=os.getenv("SMS_API_URL", ""),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
    log_event("Configuration loaded")
    return cfg


def validate_config(cfg: Config) -> Config:
    """Убедиться, что присутствуют обязательные параметры.

    Parameters
    ----------
    cfg:
        Объект конфигурации, который требуется проверить.

    Returns
    -------
    Config
        Тот же объект ``cfg`` после проверки.

    Raises
    ------
    ValueError
        Если отсутствует один или несколько обязательных параметров.
    """

    missing = []
    if not cfg.bybit_api_key:
        missing.append("BYBIT_API_KEY")
    if not cfg.bybit_api_secret:
        missing.append("BYBIT_API_SECRET")
    if not cfg.arbitrage_symbol:
        missing.append("ARBITRAGE_SYMBOL")
    if not cfg.trading_pairs:
        missing.append("TRADING_PAIRS")

    if missing:
        raise ValueError(
            "Missing configuration values: " + ", ".join(missing)
        )

    validate_thresholds(cfg)
    validate_security(cfg)
    return cfg


def update_config(cfg: Config, **updates: Any) -> Config:
    """Вернуть копию ``cfg`` с обновлёнными значениями.

    Parameters
    ----------
    cfg:
        Исходный объект конфигурации.
    updates:
        Пары "ключ=значение", которые нужно заменить.

    Returns
    -------
    Config
        Новый объект конфигурации с применёнными изменениями.
    """

    new_cfg = replace(cfg, **updates)
    if updates:
        log_event(f"Configuration updated: {updates}")
    return new_cfg


def get_config_value(cfg: Config, key: str) -> Any:
    """Вернуть значение конкретного параметра из конфигурации.

    Parameters
    ----------
    cfg:
        Объект конфигурации.
    key:
        Название атрибута.

    Returns
    -------
    Any
        Значение поля ``key``.

    Raises
    ------
    KeyError
        Если запрошенный атрибут отсутствует в объекте ``cfg``.
    """

    if not hasattr(cfg, key):
        raise KeyError(f"Unknown configuration field: {key}")
    return getattr(cfg, key)


def validate_thresholds(cfg: Config) -> None:
    """Проверить, что числовые пороги конфигурации имеют допустимые значения.

    Parameters
    ----------
    cfg:
        Конфигурация, в которой проверяются пороги.

    Raises
    ------
    ValueError
        Если ``arbitrage_threshold`` или ``max_position_size`` меньше либо равны нулю.
    """

    if cfg.arbitrage_threshold <= 0:
        raise ValueError("ARBITRAGE_THRESHOLD must be greater than 0")
    if cfg.max_position_size <= 0:
        raise ValueError("MAX_POSITION_SIZE must be greater than 0")
    if cfg.max_open_positions <= 0:
        raise ValueError("MAX_OPEN_POSITIONS must be greater than 0")
    if cfg.max_total_loss <= 0:
        raise ValueError("MAX_TOTAL_LOSS must be greater than 0")
    if cfg.loss_alert < 0:
        raise ValueError("LOSS_ALERT must be >= 0")
    if cfg.profit_alert < 0:
        raise ValueError("PROFIT_ALERT must be >= 0")
    if cfg.daily_loss_pct <= 0:
        raise ValueError("DAILY_LOSS_PCT must be greater than 0")
    if cfg.daily_loss_soft_pct < 0:
        raise ValueError("DAILY_LOSS_SOFT_PCT must be >= 0")
    if cfg.daily_loss_soft_pct and cfg.daily_loss_soft_pct >= cfg.daily_loss_pct:
        raise ValueError("DAILY_LOSS_SOFT_PCT must be less than DAILY_LOSS_PCT")
    if cfg.rsi_period <= 0:
        raise ValueError("RSI_PERIOD must be greater than 0")
    if cfg.rsi_overbought <= cfg.rsi_oversold:
        raise ValueError("RSI_OVERBOUGHT must exceed RSI_OVERSOLD")
    for name, val in {
        "MACD_FAST": cfg.macd_fast,
        "MACD_SLOW": cfg.macd_slow,
        "MACD_SIGNAL": cfg.macd_signal,
    }.items():
        if val <= 0:
            raise ValueError(f"{name} must be greater than 0")


def validate_security(cfg: Config) -> None:
    """Проверить, что конфиденциальные данные действительно загружены."""
    if not cfg.accounts:
        notify_security_issue("No Bybit accounts configured")
        raise ValueError("No Bybit accounts configured")
    for acc in cfg.accounts:
        if not acc.api_key or not acc.api_secret:
            notify_security_issue(f"Account {acc.name} missing credentials")
            raise ValueError("API credentials missing or invalid")
