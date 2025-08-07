from datetime import datetime, timedelta

import asyncio
import importlib
import pytest
import pytest_asyncio

import config
import database


def test_query_semaphore_from_env(monkeypatch):
    monkeypatch.setattr(config, "CONFIG", {"DB_QUERY_CONCURRENCY": "2"})
    importlib.reload(database)
    assert database._QUERY_SEMAPHORE._value == 2
    monkeypatch.setattr(config, "CONFIG", {})
    importlib.reload(database)


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
            "trade_qty": 1.5,
            "funding_rate": 0.001,
        },
        {
            "timestamp": (datetime.utcnow() + timedelta(minutes=1)).isoformat(),
            "spot_symbol": "BTC/USDT",
            "futures_symbol": "BTCUSDT",
            "spot_bid": 60010,
            "futures_ask": 60020,
            "trade_qty": 2.0,
            "funding_rate": 0.002,
        },
    ]
    await database.save_data(rows)
    data = await database.query_data(
        "BTC/USDT", "BTCUSDT", rows[0]["timestamp"], rows[1]["timestamp"]
    )
    assert len(data) == 2
    assert data[0]["spot_bid"] == 59990
    assert data[0]["trade_qty"] == 1.5
    assert data[0]["funding_rate"] == 0.001

    # DataFrame branch
    df = await database.query_data(
        "BTC/USDT", "BTCUSDT", rows[0]["timestamp"], rows[1]["timestamp"], as_dataframe=True
    )
    assert list(df["spot_bid"]) == [59990, 60010]


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
    wal = db_path.with_suffix(".db-wal")
    if wal.exists():
        assert wal.stat().st_size == 0


@pytest.mark.asyncio
async def test_upsert_unique(temp_db):
    first = {
        "timestamp": datetime.utcnow().isoformat(),
        "spot_symbol": "BTC/USDT",
        "futures_symbol": "BTCUSDT",
        "spot_bid": 1,
        "futures_ask": 2,
    }
    await database.save_data([first])
    second = dict(first)
    second["spot_bid"] = 3
    await database.save_data([second])
    data = await database.query_data(
        "BTC/USDT", "BTCUSDT", first["timestamp"], first["timestamp"]
    )
    assert len(data) == 1 and data[0]["spot_bid"] == 3


@pytest.mark.asyncio
async def test_save_atomic(temp_db, monkeypatch):
    rows = [
        {
            "timestamp": datetime.utcnow().isoformat(),
            "spot_symbol": "BTC/USDT",
            "futures_symbol": "BTCUSDT",
            "spot_bid": 1,
            "futures_ask": 2,
        },
        {
            "timestamp": (datetime.utcnow() + timedelta(seconds=1)).isoformat(),
            "spot_symbol": "BTC/USDT",
            "futures_symbol": "BTCUSDT",
            "spot_bid": 3,
            "futures_ask": 4,
        },
    ]
    conn = await database._get_conn()
    async def boom():
        raise RuntimeError("boom")
    monkeypatch.setattr(conn, "commit", boom)
    await database.save_data(rows)
    assert await database.count_rows() == 0


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


@pytest.mark.asyncio
async def test_vacuum_db(temp_db):
    await database.save_data(
        [{
            "timestamp": datetime.utcnow().isoformat(),
            "spot_symbol": "ETH/USDT",
            "futures_symbol": "ETHUSDT",
            "spot_bid": 1,
            "futures_ask": 2,
        }]
    )
    await database.vacuum_db()  # Should run without error


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


def test_validate_data_rejects_bad_symbols():
    bad = [
        {
            "timestamp": datetime.utcnow().isoformat(),
            "spot_symbol": "BTCUSDT",  # missing slash
            "futures_symbol": "BTC/USDT",  # contains slash
            "spot_bid": 1,
            "futures_ask": 2,
        }
    ]
    assert database.validate_data(bad) is False


def test_validate_data_accepts_hyphen_suffix():
    good = [
        {
            "timestamp": datetime.utcnow().isoformat(),
            "spot_symbol": "BTC/USDT",
            "futures_symbol": "BTCUSDT-PERP",
            "spot_bid": 1,
            "futures_ask": 2,
        }
    ]
    assert database.validate_data(good) is True


@pytest.mark.asyncio
async def test_parallel_query_respects_limit(temp_db, monkeypatch):
    db_path, _ = temp_db
    start = datetime.utcnow() - timedelta(days=120)
    rows = [
        {
            "timestamp": (start + timedelta(days=30 * i)).isoformat(),
            "spot_symbol": "BTC/USDT",
            "futures_symbol": "BTCUSDT",
            "spot_bid": 1 + i,
            "futures_ask": 2 + i,
        }
        for i in range(4)
    ]
    await database.save_data(rows)

    sem = asyncio.Semaphore(1)
    monkeypatch.setattr(database, "_QUERY_SEMAPHORE", sem)

    active = {"current": 0, "max": 0}

    orig_connect = database.aiosqlite.connect

    def tracked_connect(*args, **kwargs):
        cm = orig_connect(*args, **kwargs)

        class Wrapper:
            async def __aenter__(self):
                active["current"] += 1
                active["max"] = max(active["max"], active["current"])
                await asyncio.sleep(0.05)
                self.conn = await cm.__aenter__()
                return self.conn

            async def __aexit__(self, exc_type, exc, tb):
                try:
                    return await cm.__aexit__(exc_type, exc, tb)
                finally:
                    active["current"] -= 1

        return Wrapper()

    monkeypatch.setattr(database.aiosqlite, "connect", tracked_connect)

    await database.query_data(
        "BTC/USDT",
        "BTCUSDT",
        rows[0]["timestamp"],
        (start + timedelta(days=120)).isoformat(),
        parallel=True,
        chunk_days=30,
    )

    assert active["max"] <= 1
