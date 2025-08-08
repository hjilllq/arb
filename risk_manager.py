"""Базовые инструменты управления рисками для системы арбитража.

Модуль содержит класс :class:`RiskManager`, который помогает рассчитывать
размер позиции, отслеживать волатильность (изменчивость цен) и
приостанавливать торговлю при превышении порогов риска. Для вывода логов
используется общая функция :func:`logger.log_event`, чтобы стиль логов был
единым во всём проекте."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, TYPE_CHECKING

from logger import log_event
from notification_manager import NotificationManager

if TYPE_CHECKING:  # pragma: no cover - используется только для подсказок типов
    from config import Config


@dataclass
class RiskManager:
    """Лёгкий инструмент управления рисками."""

    max_position_size: float = 1.0
    max_open_positions: int = 5
    max_total_loss: float = 100.0
    profit_alert: float = 0.0
    loss_alert: float = 0.0
    volatility_window: int = 10
    volatility_threshold: float = 5.0  # процент
    trading_paused: bool = False
    notifier: NotificationManager | None = None
    _prices: List[float] = field(default_factory=list)
    cumulative_pnl: float = 0.0
    _profit_notified: bool = False
    _loss_notified: bool = False
    # хранение результатов и адаптивных коэффициентов
    recent_results: List[float] = field(default_factory=list)
    performance_factor: float = 1.0
    strategy_multipliers: Dict[str, float] = field(default_factory=dict)
    # дополнительный коэффициент безопасности, применяемый при сбоях
    safety_factor: float = 1.0

    # ------------------------------------------------------------------
    # работа с конфигурацией
    @classmethod
    def from_config(cls, cfg: "Config") -> "RiskManager":
        """Создать менеджер рисков на основе объекта конфигурации."""

        notifier = NotificationManager(
            telegram_token=cfg.telegram_token,
            telegram_chat_id=cfg.telegram_chat_id,
            email_sender=cfg.email_sender,
            email_host=cfg.email_host,
            email_port=cfg.email_port,
            slack_webhook_url=cfg.slack_webhook_url,
            sms_api_url=cfg.sms_api_url,
        )
        return cls(
            max_position_size=cfg.max_position_size,
            max_open_positions=cfg.max_open_positions,
            max_total_loss=cfg.max_total_loss,
            profit_alert=cfg.profit_alert,
            loss_alert=cfg.loss_alert,
            notifier=notifier,
        )

    def apply_config(self, cfg: "Config") -> None:
        """Обновить параметры риска в соответствии с конфигурацией."""

        self.max_position_size = cfg.max_position_size
        self.max_open_positions = cfg.max_open_positions
        self.max_total_loss = cfg.max_total_loss
        self.profit_alert = cfg.profit_alert
        self.loss_alert = cfg.loss_alert

    def calculate_position_size(
        self,
        account_balance: float,
        risk_per_trade: float,
        stop_loss_distance: float,
    ) -> float:
        """Рассчитать рекомендуемый объём позиции.

        Parameters
        ----------
        account_balance: float
            Текущий баланс счёта.
        risk_per_trade: float
            Доля капитала, которой трейдер готов рискнуть в сделке.
        stop_loss_distance: float
            Разница между ценой входа и уровнем стоп‑лосса.

        Returns
        -------
        float
            Предлагаемый размер позиции, ограниченный ``max_position_size``.

        Raises
        ------
        ValueError
            Если ``stop_loss_distance`` неположительный.
        """
        if stop_loss_distance <= 0:
            raise ValueError("stop_loss_distance must be positive")

        risk_capital = account_balance * risk_per_trade
        size = risk_capital / stop_loss_distance
        size = min(size, self.max_position_size) * self.safety_factor
        self.log_risk_management("position_size", {"size": size})
        return size

    def monitor_volatility(self, price: float) -> float:
        """Обновить список цен и вычислить текущую волатильность.

        Parameters
        ----------
        price: float
            Последняя наблюдаемая цена.

        Returns
        -------
        float
            Волатильность в процентах. Если данных недостаточно, возвращает ``0``.

        Notes
        -----
        При превышении ``volatility_threshold`` автоматически вызывает
        :meth:`pause_trading_if_risk`.
        """
        self._prices.append(price)
        if len(self._prices) > self.volatility_window:
            self._prices.pop(0)
        if len(self._prices) < 2:
            return 0.0
        mean_price = sum(self._prices) / len(self._prices)
        variance = sum((p - mean_price) ** 2 for p in self._prices) / len(self._prices)
        vol = (variance ** 0.5) / mean_price * 100
        if vol > self.volatility_threshold:
            self.pause_trading_if_risk(
                f"volatility {vol:.2f}% exceeds {self.volatility_threshold}%"
            )
        return vol

    def check_risk_limits(self, position_size: float, open_positions: int = 0) -> bool:
        """Проверить соответствие параметров установленным ограничениям.

        Parameters
        ----------
        position_size: float
            Размер проверяемой позиции.
        open_positions: int, optional
            Количество текущих открытых позиций.

        Returns
        -------
        bool
            ``True``, если ограничения не нарушены и торговля не приостановлена.
        """

        within = (
            position_size <= self.max_position_size
            and open_positions <= self.max_open_positions
            and not self.trading_paused
        )
        if not within:
            reason = "risk limits breached"
            if open_positions > self.max_open_positions:
                reason = "too many open positions"
            elif position_size > self.max_position_size:
                reason = "position size limit"
            self.pause_trading_if_risk(reason)
        return within

    # ------------------------------------------------------------------
    # учёт результатов сделок
    def record_trade(self, pnl: float) -> None:
        """Зарегистрировать результат сделки и проверить суммарный убыток."""

        self.cumulative_pnl += pnl
        self.recent_results.append(pnl)
        if len(self.recent_results) > self.volatility_window:
            self.recent_results.pop(0)
        self._update_performance_factor()
        self.log_risk_management(
            "trade",
            {"pnl": pnl, "cumulative_pnl": self.cumulative_pnl},
        )
        if self.cumulative_pnl <= -self.max_total_loss:
            self.pause_trading_if_risk("max total loss exceeded")
        self._check_pnl_alerts()

    # ------------------------------------------------------------------
    # адаптация правил
    def _update_performance_factor(self) -> None:
        """Обновить коэффициент в зависимости от последних результатов."""

        if not self.recent_results:
            self.performance_factor = 1.0
            return
        avg = sum(self.recent_results) / len(self.recent_results)
        # при отрицательном среднем уменьшаем размер позиции
        self.performance_factor = 0.5 if avg < 0 else 1.0

    def set_strategy_multiplier(self, name: str, multiplier: float) -> None:
        """Задать индивидуальный множитель размера позиции для стратегии."""

        self.strategy_multipliers[name] = multiplier

    # ------------------------------------------------------------------
    # управление дополнительным коэффициентом безопасности
    def set_safety_factor(self, factor: float, reason: str) -> None:
        """Установить понижающий коэффициент при сбоях или повышенном риске."""

        self.safety_factor = factor
        self.log_risk_management(
            "safety_factor", {"factor": factor, "reason": reason}
        )

    def reset_safety_factor(self) -> None:
        """Вернуть коэффициент безопасности к значению по умолчанию."""

        if self.safety_factor != 1.0:
            self.safety_factor = 1.0
            self.log_risk_management("safety_factor", {"factor": 1.0, "reason": "reset"})

    def adjust_position_size(
        self,
        qty: float,
        strategy: str | None = None,
        volatility: float | None = None,
    ) -> float:
        """Скорректировать объём позиции в зависимости от условий рынка.

        Parameters
        ----------
        qty: float
            Исходный размер позиции.
        strategy: str | None
            Название стратегии, для которой рассчитывается объём.
        volatility: float | None
            Текущая оценка волатильности в процентах.

        Returns
        -------
        float
            Итоговый объём с учётом коэффициентов и ограничений.
        """

        factor = self.performance_factor * self.safety_factor
        if volatility is not None and volatility > self.volatility_threshold:
            factor *= 0.5
        if strategy and strategy in self.strategy_multipliers:
            factor *= self.strategy_multipliers[strategy]
        adjusted = min(qty * factor, self.max_position_size)
        self.log_risk_management(
            "adjust_size",
            {"original": qty, "adjusted": adjusted, "factor": factor},
        )
        return adjusted

    def _check_pnl_alerts(self) -> None:
        """Отправить уведомления при достижении порогов прибыли/убытка."""

        if (
            self.profit_alert
            and self.cumulative_pnl >= self.profit_alert
            and not self._profit_notified
        ):
            if self.notifier:
                self.notifier.send_telegram_notification(
                    f"Profit target {self.profit_alert} reached"
                )
            self._profit_notified = True
        if (
            self.loss_alert
            and self.cumulative_pnl <= -self.loss_alert
            and not self._loss_notified
        ):
            if self.notifier:
                self.notifier.send_telegram_notification(
                    f"Loss limit {self.loss_alert} reached"
                )
            self._loss_notified = True

    def pause_trading_if_risk(self, reason: str) -> None:
        """Пометить систему как приостановленную и зафиксировать причину.

        Parameters
        ----------
        reason: str
            Пояснение, почему торговля остановлена.
        """

        self.trading_paused = True
        self.log_risk_management("pause", {"reason": reason})
        if self.notifier:
            message = f"Trading paused: {reason}"
            # отправляем хотя бы в те каналы, которые настроены
            self.notifier.send_telegram_notification(message)
            if self.notifier.email_sender:
                self.notifier.send_email_notification(
                    "Risk alert", message, [self.notifier.email_sender]
                )

    def log_risk_management(self, action: str, data: Dict[str, Any] | None = None) -> None:
        """Передать сведения об управлении рисками в общий логгер.

        Parameters
        ----------
        action: str
            Тип события (например, ``"pause"`` или ``"position_size"``).
        data: dict | None
            Дополнительные данные, которые следует включить в запись.
        """

        payload: Dict[str, Any] = {"action": action}
        if data:
            payload.update(data)
        log_event(f"RISK_MANAGEMENT: {payload}")
