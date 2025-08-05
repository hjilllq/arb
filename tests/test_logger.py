import asyncio
import importlib
import os
import sys


def _import_logger(tmp_path):
    sys.modules.pop("logger", None)
    os.chdir(tmp_path)
    return importlib.import_module("logger")


def test_log_info_writes_to_file(tmp_path):
    logger = _import_logger(tmp_path)
    log_file = tmp_path / "bot.log"
    logger.setup_logger(str(log_file))
    asyncio.run(logger.log_info("Test"))
    assert "Test" in log_file.read_text()


def test_archive_logs_creates_backup(tmp_path):
    logger = _import_logger(tmp_path)
    log_file = tmp_path / "bot.log"
    logger.setup_logger(str(log_file))
    asyncio.run(logger.log_info("Archive"))
    asyncio.run(logger.archive_logs())
    archives = list((tmp_path / "logs_archive").glob("bot.log.*.bak"))
    assert len(archives) == 1
