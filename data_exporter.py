"""Экспорт торговых данных в различные форматы."""

from __future__ import annotations

from typing import Iterable, Mapping, Any
import csv
from pathlib import Path

import numpy as np
from openpyxl import Workbook

from logger import log_event
from error_handler import handle_error


def export_trades_to_csv(
    trades: Iterable[Mapping[str, Any]],
    filename: str | Path,
) -> bool:
    """Сохранить список сделок в CSV-файл.

    Parameters
    ----------
    trades:
        Коллекция словарей с одинаковыми ключами. Каждый словарь описывает одну
        сделку (например, пару, цену, объём и время).
    filename:
        Путь к файлу, в который нужно сохранить данные.

    Returns
    -------
    bool
        ``True``, если экспорт прошёл успешно, иначе ``False``.

    Notes
    -----
    При ошибке записи информация будет зарегистрирована через
    :func:`error_handler.handle_error`.
    """

    try:
        trades_list = list(trades)
        if not trades_list:
            raise ValueError("Нет данных для экспорта")

        path = Path(filename)
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=trades_list[0].keys())
            writer.writeheader()
            writer.writerows(trades_list)

        log_event(f"Экспортировано {len(trades_list)} сделок в {path}")
        return True
    except Exception as exc:  # pragma: no cover - защищает от неожиданных ошибок
        handle_error("CSV export failed", exc)
        return False


def export_trades_to_excel(
    trades: Iterable[Mapping[str, Any]],
    filename: str | Path,
) -> bool:
    """Сохранить список сделок в Excel-файл формата ``.xlsx``.

    Parameters
    ----------
    trades:
        Коллекция словарей с данными о сделках.
    filename:
        Имя файла назначения.

    Returns
    -------
    bool
        ``True``, если файл создан, иначе ``False``.

    Notes
    -----
    Для работы требуется библиотека :mod:`openpyxl`.
    """

    try:
        trades_list = list(trades)
        if not trades_list:
            raise ValueError("Нет данных для экспорта")

        path = Path(filename)
        wb = Workbook()
        ws = wb.active
        ws.append(list(trades_list[0].keys()))
        for trade in trades_list:
            ws.append(list(trade.values()))
        wb.save(path)

        log_event(f"Экспортировано {len(trades_list)} сделок в {path}")
        return True
    except Exception as exc:  # pragma: no cover - защищает от неожиданных ошибок
        handle_error("Excel export failed", exc)
        return False


def export_trades_to_npz(
    trades: Iterable[Mapping[str, Any]],
    filename: str | Path,
) -> bool:
    """Сохранить сделки в бинарный формат ``.npz`` для быстрого чтения.

    Parameters
    ----------
    trades:
        Коллекция словарей с данными о сделках.
    filename:
        Имя файла назначения.

    Returns
    -------
    bool
        ``True``, если файл создан, иначе ``False``.
    """

    try:
        trades_list = list(trades)
        if not trades_list:
            raise ValueError("Нет данных для экспорта")

        path = Path(filename)
        arrays = {key: [t[key] for t in trades_list] for key in trades_list[0].keys()}
        np.savez(path, **arrays)
        log_event(f"Экспортировано {len(trades_list)} сделок в {path}")
        return True
    except Exception as exc:  # pragma: no cover - защищает от неожиданных ошибок
        handle_error("NPZ export failed", exc)
        return False
