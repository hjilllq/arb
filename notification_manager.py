from __future__ import annotations

"""Utility helpers for sending notifications through various channels."""

from dataclasses import dataclass
from email.message import EmailMessage
from typing import Iterable

import requests
import smtplib

from logger import log_event, log_error


@dataclass
class NotificationManager:
    """Dispatch notifications to different services.

    Parameters
    ----------
    telegram_token:
        Bot token used for Telegram messages.
    telegram_chat_id:
        Chat identifier for Telegram messages.
    email_sender:
        Address used as the *From* field when sending email.
    email_host:
        SMTP host for outgoing email. Defaults to ``"localhost"``.
    email_port:
        SMTP port for outgoing email. Defaults to ``25``.
    slack_webhook_url:
        Incoming webhook URL for Slack messages.
    sms_api_url:
        Endpoint used to dispatch SMS messages.
    """

    telegram_token: str | None = None
    telegram_chat_id: str | None = None
    email_sender: str | None = None
    email_host: str = "localhost"
    email_port: int = 25
    slack_webhook_url: str | None = None
    sms_api_url: str | None = None

    # ------------------------------------------------------------------
    # core helpers
    def log_notification(self, channel: str, message: str) -> None:
        """Record that a notification was emitted."""
        log_event(f"NOTIFY[{channel}]: {message}")

    # ------------------------------------------------------------------
    # telegram
    def send_telegram_notification(self, message: str) -> bool:
        """Send a Telegram message using the configured bot credentials."""
        if not self.telegram_token or not self.telegram_chat_id:
            log_error("Telegram credentials not configured")
            return False

        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {"chat_id": self.telegram_chat_id, "text": message}
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            self.log_notification("telegram", message)
            return True
        except Exception as exc:  # pragma: no cover - network errors
            log_error("Telegram notification failed", exc)
            return False

    # ------------------------------------------------------------------
    # email
    def send_email_notification(
        self, subject: str, body: str, recipients: Iterable[str]
    ) -> bool:
        """Send an email notification."""
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
        except Exception as exc:  # pragma: no cover - SMTP errors
            log_error("Email notification failed", exc)
            return False

    # ------------------------------------------------------------------
    # sms
    def send_sms_notification(self, phone: str, message: str) -> bool:
        """Send an SMS message using a generic HTTP API."""
        if not self.sms_api_url:
            log_error("SMS API URL not configured")
            return False

        payload = {"phone": phone, "message": message}
        try:
            resp = requests.post(self.sms_api_url, json=payload, timeout=10)
            resp.raise_for_status()
            self.log_notification("sms", message)
            return True
        except Exception as exc:  # pragma: no cover - network errors
            log_error("SMS notification failed", exc)
            return False

    # ------------------------------------------------------------------
    # slack
    def send_slack_notification(self, message: str) -> bool:
        """Send a Slack notification through a webhook."""
        if not self.slack_webhook_url:
            log_error("Slack webhook not configured")
            return False

        try:
            resp = requests.post(self.slack_webhook_url, json={"text": message}, timeout=10)
            resp.raise_for_status()
            self.log_notification("slack", message)
            return True
        except Exception as exc:  # pragma: no cover - network errors
            log_error("Slack notification failed", exc)
            return False
