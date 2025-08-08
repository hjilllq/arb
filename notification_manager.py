"""Вспомогательные инструменты для отправки уведомлений через разные каналы."""

from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
from typing import Iterable

import httpx
import smtplib
import time

from error_handler import handle_error
from logger import log_event, log_error


@dataclass
class NotificationManager:
    """Отправка уведомлений в различные сервисы.

    Parameters
    ----------
    telegram_token:
        Токен бота для сообщений в Telegram.
    telegram_chat_id:
        Идентификатор чата для сообщений Telegram.
    email_sender:
        Адрес, используемый в поле *From* при отправке электронной почты.
    email_host:
        Хост SMTP для исходящих писем. По умолчанию ``"localhost"``.
    email_port:
        Порт SMTP для исходящих писем. По умолчанию ``25``.
    slack_webhook_url:
        URL (адрес) входящего вебхука для сообщений Slack.
    sms_api_url:
        Адрес API (веб-интерфейса) для отправки SMS (коротких сообщений).
    """

    telegram_token: str | None = None
    telegram_chat_id: str | None = None
    email_sender: str | None = None
    email_host: str = "localhost"
    email_port: int = 25
    slack_webhook_url: str | None = None
    sms_api_url: str | None = None

    # ------------------------------------------------------------------
    # основные помощники
    def log_notification(self, channel: str, message: str) -> None:
        """Записать факт отправки уведомления."""
        log_event(f"NOTIFY[{channel}]: {message}")

    # Внутренние утилиты -------------------------------------------------
    def _post_with_retry(self, url: str, payload: dict) -> bool:
        """Отправить HTTP-запрос с тремя попытками.

        При временных сетевых сбоях выполняем повторный запрос с экспоненциальной
        задержкой: 1, 2, 4 секунды. Если все попытки исчерпаны, ошибка
        регистрируется в :mod:`error_handler` и возвращается ``False``.
        """

        for attempt in range(3):
            try:
                resp = httpx.post(url, json=payload, timeout=10)
                resp.raise_for_status()
                return True
            except httpx.HTTPError as exc:  # pragma: no cover - сеть нестабильна
                if attempt == 2:
                    handle_error("Notification HTTP request failed", exc)
                    return False
                time.sleep(2 ** attempt)

    # ------------------------------------------------------------------
    # уведомления через Telegram
    def send_telegram_notification(self, message: str) -> bool:
        """Отправить сообщение в Telegram с учётными данными бота."""
        if not self.telegram_token or not self.telegram_chat_id:
            log_error("Telegram credentials not configured")
            return False

        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {"chat_id": self.telegram_chat_id, "text": message}
        if self._post_with_retry(url, payload):
            self.log_notification("telegram", message)
            return True
        return False

    # ------------------------------------------------------------------
    # уведомления по электронной почте (email)
    def send_email_notification(
        self, subject: str, body: str, recipients: Iterable[str]
    ) -> bool:
        """Отправить уведомление по электронной почте."""
        if not self.email_sender:
            log_error("Email sender not configured")
            return False

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.email_sender
        msg["To"] = ",".join(recipients)
        msg.set_content(body)

        try:
            with smtplib.SMTP(self.email_host, self.email_port) as smtp:
                smtp.send_message(msg)
            self.log_notification("email", subject)
            return True
        except Exception as exc:  # pragma: no cover - ошибки SMTP (протокол отправки почты)
            handle_error("Email notification failed", exc)
            return False

    # ------------------------------------------------------------------
    # уведомления через SMS (короткие текстовые сообщения)
    def send_sms_notification(self, phone: str, message: str) -> bool:
        """Отправить SMS (короткое текстовое сообщение) через обычный HTTP API."""
        if not self.sms_api_url:
            log_error("SMS API URL not configured")
            return False

        payload = {"phone": phone, "message": message}
        if self._post_with_retry(self.sms_api_url, payload):
            self.log_notification("sms", message)
            return True
        return False

    # ------------------------------------------------------------------
    # уведомления через Slack
    def send_slack_notification(self, message: str) -> bool:
        """Отправить уведомление в Slack через вебхук."""
        if not self.slack_webhook_url:
            log_error("Slack webhook not configured")
            return False

        if self._post_with_retry(self.slack_webhook_url, {"text": message}):
            self.log_notification("slack", message)
            return True
        return False
