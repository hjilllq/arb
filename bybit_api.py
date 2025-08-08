"""Инструменты для взаимодействия с REST API биржи Bybit.

REST API — это веб-интерфейс, к которому обращаются через HTTP-запросы.
Модуль содержит небольшой асинхронный клиент, который получает рыночные
данные и управляет простыми ордерами на Bybit. Учётные данные передаются
при создании клиента, что позволяет полностью контролировать источник
ключей и секретов.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import time
from typing import Any, Dict, Optional, Tuple
from typing import TYPE_CHECKING

import httpx
import logging
from prometheus_client import Histogram

from error_handler import handle_error

if TYPE_CHECKING:  # pragma: no cover - только для подсказок типов
    from notification_manager import NotificationManager


logger = logging.getLogger(__name__)

# Гистограмма для времени отклика запросов к API Bybit
REQUEST_LATENCY = Histogram(
    "bybit_request_latency_seconds",
    "Время отклика запросов к Bybit",
    ["endpoint"],
)


class BybitAPI:
    """Минимальная асинхронная обёртка над HTTP‑API Bybit.

    HTTP‑API — интерфейс, принимающий запросы по протоколу HTTP.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: str = "https://api.bybit.com",
        cache_ttl: float = 1.0,
        notifier: Optional["NotificationManager"] = None,
    ) -> None:
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.base_url = base_url.rstrip("/")
        # Включаем HTTP/2 (вторую версию протокола HTTP) и ограничиваем количество
        # соединений, чтобы потребление памяти оставалось под контролем
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=10.0,
            http2=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=20),
        )
        # Простой кэш с ограниченным временем жизни
        self._cache: Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]] = {}
        self.cache_ttl = cache_ttl  # секунды
        self.notifier = notifier

    async def __aenter__(self) -> "BybitAPI":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    def cleanup_cache(self) -> None:
        """Очистить просроченные элементы кэша.

        Элементы, которые хранятся дольше ``cache_ttl`` секунд, удаляются,
        чтобы кэш не рос бесконтрольно и не занимал лишнюю память.
        """
        now = time.time()
        for key, (ts, _) in list(self._cache.items()):
            if now - ts > self.cache_ttl:
                del self._cache[key]

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Подписывает приватные запросы к API.

        Параметры
        ----------
        params: Dict[str, Any]
            Набор параметров запроса, к которым необходимо добавить ключ,
            метку времени и HMAC‑подпись.

        Возвращает
        ----------
        Dict[str, Any]
            Те же параметры с полями ``api_key``, ``timestamp`` и ``sign``.

        Исключения
        ----------
        RuntimeError
            Вызывается, если в объекте отсутствуют ключ и секрет для API,
            поэтому подписать запрос невозможно.

        Связь
        -----
        Метод используется внутренне в :meth:`place_order`,
        :meth:`cancel_order`, :meth:`check_balance` и
        :meth:`get_order_status` при формировании приватных запросов.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError("API key and secret are required for this operation")
        params["api_key"] = self.api_key
        params["timestamp"] = int(time.time() * 1000)
        query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        signature = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        params["sign"] = signature
        return params

    async def get_spot_data(self, pair: str) -> Dict[str, Any]:
        """Запрашивает статистику по спотовой паре за последние 24 часа.

        Параметры
        ----------
        pair: str
            Символ торговой пары, например ``"BTCUSDT"``.

        Возвращает
        ----------
        Dict[str, Any]
            Ответ API в формате JSON с ценой открытия, закрытия и
            дополнительными метриками.

        Особенности
        -----------
        Метод повторяет запрос до трёх раз при сетевых ошибках. При
        исчерпании попыток исключение :class:`httpx.HTTPError` пробрасывается
        выше.

        Связь
        -----
        Данные используются в :class:`ExchangeManager`,
        :class:`Backtester` и стратегиях, зависящих от актуальной цены.
        """
        self.cleanup_cache()
        cache_key = ("spot", pair)
        cached = self._cache.get(cache_key)
        now = time.time()
        if cached and now - cached[0] < self.cache_ttl:
            return cached[1]
        for attempt in range(3):
            try:
                with REQUEST_LATENCY.labels("get_spot_data").time():
                    resp = await self._client.get(
                        "/spot/v3/public/quote/ticker/24hr", params={"symbol": pair}
                    )
                resp.raise_for_status()
                data = resp.json()
                self._cache[cache_key] = (time.time(), data)
                return data
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def get_futures_data(self, pair: str) -> Dict[str, Any]:
        """Возвращает информацию о тике фьючерсной пары.

        Параметры
        ----------
        pair: str
            Обозначение фьючерсного инструмента, например ``"BTCUSDT"``.

        Возвращает
        ----------
        Dict[str, Any]
            JSON‑ответ с текущей ценой, объёмом и другими метриками.

        Особенности
        -----------
        Выполняет до трёх попыток при возникновении ошибок сети. После
        последней попытки исключение :class:`httpx.HTTPError` будет
        передано дальше.

        Связь
        -----
        Используется для расчёта базиса в :class:`ArbitrageStrategy` и
        при тестировании в :class:`Backtester`.
        """
        self.cleanup_cache()
        cache_key = ("futures", pair)
        cached = self._cache.get(cache_key)
        now = time.time()
        if cached and now - cached[0] < self.cache_ttl:
            return cached[1]
        for attempt in range(3):
            try:
                with REQUEST_LATENCY.labels("get_futures_data").time():
                    resp = await self._client.get(
                        "/derivatives/v3/public/tickers", params={"symbol": pair}
                    )
                resp.raise_for_status()
                data = resp.json()
                self._cache[cache_key] = (time.time(), data)
                return data
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def place_order(
        self,
        pair: str,
        price: float,
        qty: float,
        side: str = "Buy",
        order_type: str = "Limit",
    ) -> Dict[str, Any]:
        """Создаёт ордер на покупку или продажу.

        Параметры
        ----------
        pair: str
            Торговая пара, по которой размещается ордер.
        price: float
            Желаемая цена исполнения.
        qty: float
            Количество покупаемого или продаваемого актива.
        side: str, optional
            Направление ордера ``"Buy"`` или ``"Sell"``.
        order_type: str, optional
            Тип ордера, например ``"Limit"`` или ``"Market"``.

        Возвращает
        ----------
        Dict[str, Any]
            JSON‑ответ Bybit с данными об ордере.

        Особенности
        -----------
        Повторяет запрос до трёх раз. Возможны ошибки
        :class:`httpx.HTTPError` при сетевых проблемах либо при отказе API.

        Связь
        -----
        Вызывается из :class:`ExchangeManager` и в конечном итоге используется
        стратегией и торговым ботом для размещения сделок.
        """
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {
                    "symbol": pair,
                    "side": side,
                    "orderType": order_type,
                    "price": price,
                    "qty": qty,
                }
                params = self._sign(params)
                with REQUEST_LATENCY.labels("place_order").time():
                    resp = await self._client.post("/v5/order/create", json=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def cancel_order(
        self, order_id: str, pair: Optional[str] = None
    ) -> Dict[str, Any]:
        """Отменяет ранее созданный ордер на бирже.

        Параметры
        ----------
        order_id: str
            Идентификатор ордера, полученный при его создании.
        pair: str, optional
            Торговая пара. Если не указана, API попытается определить её
            автоматически.

        Возвращает
        ----------
        Dict[str, Any]
            JSON‑ответ от API Bybit о статусе отмены.

        Особенности
        -----------
        Запрос повторяется до трёх раз; ошибки сети приводят к выбрасыванию
        :class:`httpx.HTTPError`.

        Связь
        -----
        Используется в :class:`ExchangeManager` и косвенно торговым ботом
        для управления активными позициями.
        """
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {"orderId": order_id}
                if pair is not None:
                    params["symbol"] = pair
                params = self._sign(params)
                with REQUEST_LATENCY.labels("cancel_order").time():
                    resp = await self._client.post("/v5/order/cancel", json=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def check_balance(self) -> Dict[str, Any]:
        """Получает сведения о балансе аккаунта.

        Возвращает
        ----------
        Dict[str, Any]
            JSON‑структура с доступными средствами и замороженными балансами.

        Особенности
        -----------
        В случае ошибок сети выполняются повторные попытки. После
        исчерпания трёх попыток ошибка :class:`httpx.HTTPError` пробрасывается
        наружу.

        Связь
        -----
        Результаты проверяются торговым ботом и менеджером риска для
        определения доступного капитала.
        """
        for attempt in range(3):
            try:
                params = self._sign({})
                with REQUEST_LATENCY.labels("check_balance").time():
                    resp = await self._client.get(
                        "/v5/account/wallet-balance", params=params
                    )
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def get_order_status(
        self, order_id: str, pair: Optional[str] = None
    ) -> Dict[str, Any]:
        """Возвращает текущее состояние ордера.

        Параметры
        ----------
        order_id: str
            Идентификатор ордера.
        pair: str, optional
            Торговая пара, к которой относится ордер.

        Возвращает
        ----------
        Dict[str, Any]
            Структура JSON со статусом и подробностями ордера.

        Особенности
        -----------
        Повторяет запросы при ошибках сети. После трёх попыток
        :class:`httpx.HTTPError` будет поднято.

        Связь
        -----
        Используется торговым ботом и менеджером позиций для отслеживания
        исполнения сделок.
        """
        for attempt in range(3):
            try:
                params: Dict[str, Any] = {"orderId": order_id}
                if pair is not None:
                    params["symbol"] = pair
                params = self._sign(params)
                with REQUEST_LATENCY.labels("get_order_status").time():
                    resp = await self._client.get(
                        "/v5/order/realtime", params=params
                    )
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def get_open_orders(self, pair: Optional[str] = None) -> Dict[str, Any]:
        """Возвращает список текущих открытых ордеров."""

        for attempt in range(3):
            try:
                params: Dict[str, Any] = {}
                if pair is not None:
                    params["symbol"] = pair
                params = self._sign(params)
                with REQUEST_LATENCY.labels("get_open_orders").time():
                    resp = await self._client.get("/v5/order/realtime", params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                await self.handle_api_error(exc, attempt)

    async def handle_api_error(
        self, exc: httpx.HTTPError, attempt: int, retries: int = 3
    ) -> None:
        """Обрабатывает ошибки HTTP с повтором запроса.

        Параметры
        ----------
        exc: httpx.HTTPError
            Возникшая ошибка HTTP.
        attempt: int
            Номер текущей попытки (начиная с нуля).
        retries: int, optional
            Максимальное количество попыток.

        Особенности
        -----------
        Использует экспоненциальное увеличение времени ожидания: 1, 2, 4 секунды.
        Если предел попыток превышен, ошибка пробрасывается дальше.

        Связь
        -----
        Метод вызывается всеми сетевыми запросами этого клиента и помогает
        обеспечить устойчивость работы бота.
        """
        logger.warning(
            "Bybit API error on attempt %d/%d: %s", attempt + 1, retries, exc
        )
        if attempt + 1 >= retries:
            handle_error("Bybit API request failed", exc, self.notifier)
            raise exc
        await asyncio.sleep(2 ** attempt)

    async def close(self) -> None:
        """Завершает работу HTTP‑клиента.

        Особенности
        -----------
        Освобождает сетевые ресурсы. Должен вызываться при завершении работы
        всех компонентов, использующих API.

        Связь
        -----
        Применяется менеджерами обмена и тестером для корректного закрытия
        соединений.
        """
        await self._client.aclose()
