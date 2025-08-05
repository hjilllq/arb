from datetime import datetime, timedelta

import pytest
import pytest_asyncio

import database


@pytest_asyncio.fixture
async def temp_db(tmp_path, monkeypatch):
    db_path = tmp_path / "trades.db"
    backup_dir = tmp_path / "backups"
    monkeypatch.setattr(database, "_DB_PATH", db_path)
    monkeypatch.setattr(database, "_BACKUP_DIR", backup_dir)
    database._DB_CONN = None
    await database.connect_db(str(db_path))
    yield db_path, backup_dir
    await database.close_db()


@pytest.mark.asyncio
async def test_save_and_query(temp_db):
    db_path, _ = temp_db
    rows = [
        {
            "timestamp": datetime.utcnow().isoformat(),
            "spot_symbol": "BTC/USDT",
            "futures_symbol": "BTCUSDT",
            "spot_bid": 59990,
            "futures_ask": 60000,
        },
        {
            "timestamp": (datetime.utcnow() + timedelta(minutes=1)).isoformat(),
            "spot_symbol": "BTC/USDT",
            "futures_symbol": "BTCUSDT",
            "spot_bid": 60010,
            "futures_ask": 60020,
        },
    ]
    await database.save_data(rows)
    data = await database.query_data(
        "BTC/USDT", "BTCUSDT", rows[0]["timestamp"], rows[1]["timestamp"]
    )
    assert len(data) == 2
    assert data[0]["spot_bid"] == 59990


@pytest.mark.asyncio
async def test_backup_and_cleanup(temp_db):
    db_path, backup_dir = temp_db
    await database.save_data(
        [
            {
                "timestamp": datetime.utcnow().isoformat(),
                "spot_symbol": "ETH/USDT",
                "futures_symbol": "ETHUSDT",
                "spot_bid": 3000,
                "futures_ask": 3010,
            }
        ]
    )
    await database.backup_database(compress=False)
    backups = list(backup_dir.glob("*.bak"))
    assert backups


@pytest.mark.asyncio
async def test_clear_old_data(temp_db):
    db_path, _ = temp_db
    old_ts = (datetime.utcnow() - timedelta(days=100)).isoformat()
    new_ts = datetime.utcnow().isoformat()
    await database.save_data(
        [
            {
                "timestamp": old_ts,
                "spot_symbol": "BTC/USDT",
                "futures_symbol": "BTCUSDT",
                "spot_bid": 1,
                "futures_ask": 2,
            },
            {
                "timestamp": new_ts,
                "spot_symbol": "BTC/USDT",
                "futures_symbol": "BTCUSDT",
                "spot_bid": 3,
                "futures_ask": 4,
            },
        ]
    )
    await database.clear_old_data(days=90)
    data = await database.query_data("BTC/USDT", "BTCUSDT", old_ts, new_ts)
    assert len(data) == 1 and data[0]["timestamp"] == new_ts


def test_validate_data_rejects_negative():
    bad = [
        {
            "timestamp": datetime.utcnow().isoformat(),
            "spot_symbol": "BTC/USDT",
            "futures_symbol": "BTCUSDT",
            "spot_bid": -1,
            "futures_ask": 2,
        }
    ]
    assert database.validate_data(bad) is False
