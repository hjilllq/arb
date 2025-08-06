import sys
from pathlib import Path
import types

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import main


@pytest.mark.asyncio
async def test_setup_environment_missing_env(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    messages = {"error": []}

    async def log_error(msg, err):
        messages["error"].append((msg, err))

    monkeypatch.setattr(main.logger, "log_error", log_error)
    with pytest.raises(FileNotFoundError):
        await main.setup_environment()
    assert messages["error"]


@pytest.mark.asyncio
async def test_setup_environment_installs(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("API_KEY=x")
    (tmp_path / "requirements.txt").write_text("# test")
    monkeypatch.setattr(main.config, "load_config", lambda: {})

    called = {}

    async def fake_exec(*args, **kwargs):
        called["args"] = args
        class Proc:
            returncode = 0
            async def communicate(self):
                return b"ok", b""
        return Proc()

    monkeypatch.setattr(main.asyncio, "create_subprocess_exec", fake_exec)

    messages = {"info": [], "warning": []}

    async def log_info(msg):
        messages["info"].append(msg)

    async def log_warning(msg):
        messages["warning"].append(msg)

    monkeypatch.setattr(main.logger, "log_info", log_info)
    monkeypatch.setattr(main.logger, "log_warning", log_warning)

    await main.setup_environment()
    assert "pip" in called["args"]
    assert messages["info"] == ["Dependencies installed"]


@pytest.mark.asyncio
async def test_main_runs_bot_and_gui(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("API_KEY=x")
    monkeypatch.setattr(main.config, "load_config", lambda: {})

    async def fake_setup():
        return None

    monkeypatch.setattr(main, "setup_environment", fake_setup)

    called = {"bot_init": False, "bot_loop": False, "gui": False}

    async def init():
        called["bot_init"] = True

    async def loop():
        called["bot_loop"] = True

    dummy_tb = types.SimpleNamespace(initialize_bot=init, run_trading_loop=loop)
    monkeypatch.setitem(sys.modules, "trading_bot", dummy_tb)

    async def gui_main():
        called["gui"] = True

    dummy_gui = types.SimpleNamespace(main=gui_main)
    monkeypatch.setitem(sys.modules, "gui", dummy_gui)

    async def log_warning(msg):
        pass

    async def log_error(msg, err):
        pass

    async def log_info(msg):
        pass

    monkeypatch.setattr(main.logger, "log_warning", log_warning)
    monkeypatch.setattr(main.logger, "log_error", log_error)
    monkeypatch.setattr(main.logger, "log_info", log_info)

    await main.main()
    assert called["bot_init"] and called["bot_loop"] and called["gui"]
