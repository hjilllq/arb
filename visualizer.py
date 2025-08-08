"""Простая визуализация торговых данных."""

from __future__ import annotations

from typing import Iterable, Mapping, Any
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from logger import log_event
from error_handler import handle_error


def plot_trade_prices(
    trades: Iterable[Mapping[str, Any]],
    filename: str | Path,
) -> bool:
    """Построить график цен по временным меткам и сохранить в файл.

    Parameters
    ----------
    trades:
        Последовательность словарей, содержащих ключи ``timestamp`` и ``price``.
    filename:
        Путь к изображению, в которое будет сохранён график.

    Returns
    -------
    bool
        ``True`` при успешном построении и сохранении графика, иначе ``False``.
    """

    try:
        trades_list = list(trades)
        if not trades_list:
            raise ValueError("Нет данных для визуализации")

        times = [t["timestamp"] for t in trades_list]
        prices = [t["price"] for t in trades_list]

        plt.figure()
        plt.plot(times, prices)
        plt.xlabel("Время")
        plt.ylabel("Цена")
        plt.tight_layout()
        path = Path(filename)
        plt.savefig(path)
        plt.close()

        log_event(f"График с {len(trades_list)} точками сохранён в {path}")
        return True
    except Exception as exc:  # pragma: no cover - защищает от неожиданных ошибок
        handle_error("Visualization failed", exc)
        return False
