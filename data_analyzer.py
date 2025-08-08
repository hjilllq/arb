"""Инструменты для простого анализа рыночных данных.

Модуль содержит несколько вспомогательных функций для расчёта популярных
технических индикаторов, используемых в арбитражных и торговых
стратегиях."""
from __future__ import annotations

import logging
from typing import Iterable, List, Optional, Sequence, Tuple

import asyncio

import numpy as np


logger = logging.getLogger(__name__)


def detect_anomalies(data: Sequence[float], z_thresh: float = 3.0) -> List[int]:
    """Найти потенциально ошибочные точки данных по z‑оценке.

    Parameters
    ----------
    data:
        Последовательность числовых значений.
    z_thresh:
        Порог стандартных отклонений, за пределами которого точка считается
        аномальной. По умолчанию ``3.0``.

    Returns
    -------
    list[int]
        Номера элементов, вышедших за порог ``z_thresh``. Пустой список,
        если данные отсутствуют или их разброс равен нулю.
    """

    if not data:
        return []
    arr = np.asarray(data, dtype=float)
    std = arr.std()
    if std == 0:
        return []
    z_scores = np.abs((arr - arr.mean()) / std)
    return [int(i) for i in np.nonzero(z_scores > z_thresh)[0]]


def calculate_rsi(prices: Sequence[float], period: int = 14) -> List[float]:
    """Вычислить индекс относительной силы (RSI).

    Parameters
    ----------
    prices:
        Исторические цены закрытия.
    period:
        Количество периодов для расчёта RSI. По умолчанию ``14``.

    Returns
    -------
    list[float]
        Значения RSI; первые ``period`` элементов равны ``0`` из-за
        нехватки данных для расчёта индикатора.
    """

    arr = np.asarray(prices, dtype=float)
    if arr.size < 2:
        return [0.0] * arr.size

    deltas = np.diff(arr)
    gains = np.clip(deltas, 0, None)
    losses = np.clip(-deltas, 0, None)

    avg_gain = np.empty_like(arr)
    avg_loss = np.empty_like(arr)
    avg_gain[:period] = gains[:period].sum() / period
    avg_loss[:period] = losses[:period].sum() / period

    for i in range(period, arr.size - 1):
        avg_gain[i] = (avg_gain[i - 1] * (period - 1) + gains[i]) / period
        avg_loss[i] = (avg_loss[i - 1] * (period - 1) + losses[i]) / period

    rsi = np.zeros_like(arr)
    for i in range(period, arr.size):
        if avg_loss[i - 1] == 0:
            rsi[i] = 100.0
        else:
            rs = avg_gain[i - 1] / avg_loss[i - 1]
            rsi[i] = 100 - (100 / (1 + rs))
    return rsi.tolist()


async def async_calculate_rsi(prices: Sequence[float], period: int = 14) -> List[float]:
    """Асинхронный вариант :func:`calculate_rsi` для запуска в отдельном потоке."""

    return await asyncio.to_thread(calculate_rsi, prices, period)


def calculate_macd(
    prices: Sequence[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> Tuple[List[float], List[float], List[float]]:
    """Вычислить индикатор MACD (схождение/расхождение скользящих средних).

    Parameters
    ----------
    prices:
        Исторические цены закрытия.
    fast_period:
        Количество периодов для быстрой EMA (экспоненциальное скользящее
        среднее). По умолчанию ``12``.
    slow_period:
        Количество периодов для медленной EMA. По умолчанию ``26``.
    signal_period:
        Количество периодов для сигнальной линии EMA. По умолчанию ``9``.

    Returns
    -------
    tuple[list[float], list[float], list[float]]
        Значения линии MACD, сигнальной линии и гистограммы.
    """

    def ema(values: Iterable[float], period: int) -> np.ndarray:
        arr = np.asarray(list(values), dtype=float)
        if arr.size == 0:
            return arr
        k = 2 / (period + 1)
        ema_vals = np.empty_like(arr)
        ema_vals[0] = arr[0]
        for i in range(1, arr.size):
            ema_vals[i] = arr[i] * k + ema_vals[i - 1] * (1 - k)
        return ema_vals

    prices_arr = np.asarray(prices, dtype=float)
    fast_ema = ema(prices_arr, fast_period)
    slow_ema = ema(prices_arr, slow_period)
    length = min(len(fast_ema), len(slow_ema))
    macd_line = fast_ema[:length] - slow_ema[:length]
    signal_line = ema(macd_line, signal_period)
    hist_length = min(len(macd_line), len(signal_line))
    histogram = macd_line[:hist_length] - signal_line[:hist_length]
    return macd_line.tolist(), signal_line.tolist(), histogram.tolist()


async def async_calculate_macd(
    prices: Sequence[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> Tuple[List[float], List[float], List[float]]:
    """Асинхронный расчёт MACD в отдельном потоке."""

    return await asyncio.to_thread(
        calculate_macd, prices, fast_period, slow_period, signal_period
    )


def filter_data(
    prices: Sequence[float],
    z_thresh: float = 3.0,
    max_change: float = 0.2,
) -> List[float]:
    """Удалить из ``prices`` значения, помеченные как аномальные.

    Parameters
    ----------
    prices:
        Список цен, который необходимо очистить.
    z_thresh:
        Порог, передаваемый в :func:`detect_anomalies`. По умолчанию ``3.0``.
    max_change:
        Максимально допустимое относительное изменение цены между соседними
        элементами. Значение ``0.2`` означает фильтрацию скачков более чем на
        20 %. Это позволяет убрать выбросы, где котировка резко изменилась из-за
        ошибки источника данных.

    Returns
    -------
    list[float]
        Новый список цен без подозрительных значений.
    """

    anomalies = set(detect_anomalies(prices, z_thresh))
    cleaned: List[float] = []
    prev: Optional[float] = None
    for idx, price in enumerate(prices):
        if idx in anomalies:
            continue
        if prev is not None and prev != 0:
            rel = abs(price - prev) / prev
            if rel > max_change:
                # Слишком резкий скачок: пропускаем значение
                continue
        cleaned.append(price)
        prev = price
    return cleaned


def normalize_data(data: Sequence[float]) -> List[float]:
    """Нормализовать последовательность данных по z‑оценке.

    Значения приводятся к среднему ``0`` и стандартному отклонению ``1``.
    Если разброс равен нулю, возвращается список нулей той же длины.

    Parameters
    ----------
    data:
        Числовая последовательность для преобразования.

    Returns
    -------
    list[float]
        Нормализованные значения.
    """

    arr = np.asarray(data, dtype=float)
    if arr.size == 0:
        return []
    std = arr.std()
    if std == 0:
        return [0.0] * arr.size
    norm = (arr - arr.mean()) / std
    return norm.tolist()


def generate_signal(
    prices: Sequence[float],
    rsi_period: int = 14,
    rsi_overbought: float = 70,
    rsi_oversold: float = 30,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
) -> List[int]:
    """Построить сигнал покупки/продажи на основе RSI и MACD.

    Parameters
    ----------
    prices:
        Исторические цены закрытия.
    rsi_period:
        Период для расчёта RSI. По умолчанию ``14``.

    Returns
    -------
    list[int]
        Список из ``-1``, ``0`` или ``1`` для каждой точки данных: ``1`` —
        потенциальный лонг, ``-1`` — шорт, ``0`` — отсутствие действия.
    """

    rsi = calculate_rsi(prices, rsi_period)
    macd_line, signal_line, _ = calculate_macd(
        prices, macd_fast, macd_slow, macd_signal
    )
    signals: List[int] = []
    for i in range(len(prices)):
        sig = 0
        if i < len(macd_line) and i < len(signal_line):
            if macd_line[i] > signal_line[i] and rsi[i] < rsi_oversold:
                sig = 1
            elif macd_line[i] < signal_line[i] and rsi[i] > rsi_overbought:
                sig = -1
        signals.append(sig)
    return signals


async def async_generate_signal(
    prices: Sequence[float],
    rsi_period: int = 14,
    rsi_overbought: float = 70,
    rsi_oversold: float = 30,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
) -> List[int]:
    """Асинхронный расчёт торговых сигналов на основе индикаторов."""

    return await asyncio.to_thread(
        generate_signal,
        prices,
        rsi_period,
        rsi_overbought,
        rsi_oversold,
        macd_fast,
        macd_slow,
        macd_signal,
    )


def log_data_analysis(results: dict) -> None:
    """Записать метрики анализа данных в журнал.

    Parameters
    ----------
    results:
        Словарь с названиями метрик и их значениями.
    """

    for key, value in results.items():
        logger.info("%s: %s", key, value)


async def backtest_analysis(
    prices: Sequence[float],
    rsi_period: int = 14,
    rsi_overbought: float = 70,
    rsi_oversold: float = 30,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
) -> dict:
    """Подготовить данные и индикаторы для офлайн‑тестирования.

    Расчёт RSI, MACD и торговых сигналов выполняется параллельно в
    отдельных потоках, что ускоряет обработку длинных исторических
    рядов на многоядерных системах.
    """

    filtered = filter_data(prices)
    normalized = normalize_data(filtered)
    rsi_task = async_calculate_rsi(normalized, rsi_period)
    macd_task = async_calculate_macd(normalized, macd_fast, macd_slow, macd_signal)
    signal_task = async_generate_signal(
        normalized,
        rsi_period,
        rsi_overbought,
        rsi_oversold,
        macd_fast,
        macd_slow,
        macd_signal,
    )
    rsi, (macd_line, signal_line, histogram), signals = await asyncio.gather(
        rsi_task, macd_task, signal_task
    )
    summary = {
        "anomalies_removed": len(prices) - len(filtered),
        "signals_generated": sum(1 for s in signals if s),
    }
    log_data_analysis(summary)
    return {
        "filtered_prices": filtered,
        "rsi": rsi,
        "macd_line": macd_line,
        "signal_line": signal_line,
        "histogram": histogram,
        "signals": signals,
        "normalized_prices": normalized,
    }

