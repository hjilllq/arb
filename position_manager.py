"""Управление открытыми торговыми позициями для арбитражных стратегий.

Модуль описывает небольшой менеджер позиций в памяти, который отслеживает
открытые позиции и фиксирует события жизненного цикла. Он предоставляет
функции для открытия и закрытия позиций, изменения количества или цены,
просмотра текущего состояния и логирования каждой операции через общий
логгер проекта."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Dict, Optional

from logger import log_trade_data


@dataclass
class Position:
    """Представляет одну открытую позицию."""

    pair: str
    qty: float
    entry_price: float
    side: str  # "long" (длинная) или "short" (короткая)
    open_time: datetime = field(default_factory=lambda: datetime.now(UTC))


class PositionManager:
    """Отслеживание открытых торговых позиций в памяти."""

    def __init__(self) -> None:
        self.positions: Dict[str, Position] = {}

    def open_position(self, pair: str, qty: float, price: float, side: str) -> Position:
        """Создать запись о новой позиции и вернуть её.

        Parameters
        ----------
        pair: str
            Рыночный символ позиции.
        qty: float
            Количество актива.
        price: float
            Цена входа в позицию.
        side: str
            Направление позиции: ``"long"`` или ``"short"``.

        Returns
        -------
        Position
            Объект сохранённой позиции.
        """

        pos = Position(pair=pair, qty=qty, entry_price=price, side=side)
        self.positions[pair] = pos
        self.log_position("open", pos)
        return pos

    def close_position(self, pair: str, price: float) -> float:
        """Закрыть позицию и вычислить результат сделки.

        Parameters
        ----------
        pair: str
            Символ пары, позицию по которой требуется закрыть.
        price: float
            Цена закрытия.

        Returns
        -------
        float
            Реализованная прибыль или убыток.

        Raises
        ------
        KeyError
            Если позиция для указанной пары не найдена.
        """

        pos = self.positions.pop(pair, None)
        if pos is None:
            raise KeyError(f"no open position for {pair}")
        if pos.side.lower() == "long":
            pnl = (price - pos.entry_price) * pos.qty
        else:
            pnl = (pos.entry_price - price) * pos.qty
        self.log_position("close", pos, exit_price=price, pnl=pnl)
        return pnl

    def check_open_positions(self) -> Dict[str, Position]:
        """Получить словарь текущих открытых позиций.

        Returns
        -------
        dict[str, Position]
            Копия внутреннего хранилища открытых позиций.
        """

        return dict(self.positions)

    def update_position(
        self, pair: str, qty: Optional[float] = None, price: Optional[float] = None
    ) -> Position:
        """Обновить параметры существующей позиции.

        Parameters
        ----------
        pair: str
            Символ позиции.
        qty: float, optional
            Новое количество. Если не задано, остаётся прежним.
        price: float, optional
            Новая цена входа.

        Returns
        -------
        Position
            Обновлённая позиция.

        Raises
        ------
        KeyError
            Если позиция для ``pair`` не найдена.
        """

        pos = self.positions.get(pair)
        if pos is None:
            raise KeyError(f"no open position for {pair}")
        if qty is not None:
            pos.qty = qty
        if price is not None:
            pos.entry_price = price
        self.log_position("update", pos)
        return pos

    def log_position(self, action: str, position: Position, **extra: float) -> None:
        """Записать в журнал изменение состояния позиции.

        Parameters
        ----------
        action: str
            Тип события (``"open"``, ``"close"``, ``"update"``).
        position: Position
            Позиция, к которой относится событие.
        extra:
            Дополнительные поля, которые будут включены в лог.
        """

        data = {
            "pair": position.pair,
            "qty": position.qty,
            "price": position.entry_price,
            "side": position.side,
            "action": action,
            "timestamp": position.open_time.isoformat(),
        }
        data.update(extra)
        log_trade_data(data)
