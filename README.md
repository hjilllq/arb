# Bybit Spot/Futures Arbitrage Bot

Программа для арбитражного трейдинга между спотовым и фьючерсным рынком на бирже **Bybit**. Цель — получать прибыль 0,5–5 % в день, работая круглосуточно и эффективно на Apple Silicon M4 Max.

## Установка
1. Установите **Python 3.13.5**.
2. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
3. Создайте и заполните `.env` (см. пример ниже).

## Настройка `.env`
```dotenv
API_KEY=your_bybit_key
API_SECRET=your_bybit_secret
BACKUP_EXCHANGE=binance
BACKUP_API_KEY=your_binance_key
BACKUP_API_SECRET=your_binance_secret
SPOT_PAIRS=['BTC/USDT','ETH/USDT']
FUTURES_PAIRS=['BTCUSDT','ETHUSDT']
BTC_USDT_BASIS_THRESHOLD_OPEN=0.005
BTC_USDT_BASIS_THRESHOLD_CLOSE=0.001
ETH_USDT_BASIS_THRESHOLD_OPEN=0.006
ETH_USDT_BASIS_THRESHOLD_CLOSE=0.0012
MAX_POSITION_PER_PAIR=1000
MAX_DAILY_LOSS=0.05
MAX_VOLATILITY_PAUSE=0.3
TELEGRAM_TOKEN=your_telegram_token
TELEGRAM_CHAT_ID=your_chat_id
```

## Запуск
- Реальная торговля: `python main.py`
- Бумажный режим: `python trading_bot.py --paper`

## Структура проекта
- `config.py` — загрузка и проверка настроек, шифрование ключей.
- `bybit_api.py` — связь с API Bybit, получение цен и ставок, WebSocket.
- `exchange_manager.py` — управление основными и резервными биржами, синхронизация пар.
- `strategy.py` — расчёт basis и генерация торговых сигналов.
- `position_manager.py` — открытие и закрытие позиций, расчёт PnL.
- `risk_manager.py` — контроль рисков, лимиты убытков и волатильности.
- `data_analyzer.py` — предобработка данных и технические индикаторы.
- `ml_predictor.py` — LSTM‑модель для прогнозов basis.
- `backtester.py` — тестирование стратегии на исторических данных.
- `trading_bot.py` — основной торговый цикл (реальный и бумажный режимы).
- `main.py` — точка входа, проверяет окружение и запускает бота и GUI.
- `notification_manager.py` — отправка уведомлений через Telegram и email.
- `database.py` — асинхронное хранение цен и сделок в SQLite.
- `logger.py` — файл и консольный лог, архивация и шифрование.
- `monitor.py`, `heartbeat.py`, `health_checker.py` — мониторинг ресурсов и здоровья системы.
- `tests/`, `tests.py` — модульные тесты.
- `config_manager.py` — добавление новых бирж и пар спот/фьючерс через обновление конфигурации.

## Тестирование
Запустите модульные тесты:
```bash
python -m unittest tests.py
```

## Добавление новой пары или биржи
Используйте `config_manager.py` для редактирования `.env` и добавления новых спотовых или фьючерсных пар, а также для подключения дополнительной биржи.

При ошибках смотрите записи в `bot.log` (через `logger.py`) и уведомления от `notification_manager.py`.
