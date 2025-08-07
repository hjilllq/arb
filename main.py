"""Program entry point for the arbitrage system.

This module acts like a big **ON** button.  It prepares the environment,
installs dependencies and launches both the trading bot and the optional GUI
in parallel.  Functions are small and friendly so the Apple Silicon M4 Max can
run 24/7 without breaking a sweat.
"""
from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

import config
import logger
from notification_manager import notify


async def setup_environment() -> None:
    """Prepare runtime environment and install dependencies.

    Checks for a ``.env`` file, validates configuration and installs packages
    listed in ``requirements.txt``.  Errors are logged and forwarded to the
    notification system so operators are informed immediately.
    """
    env_file = Path(".env")
    if not env_file.exists():
        err = FileNotFoundError(".env not found")
        await logger.log_error("Missing .env file", err)
        try:
            await notify("Missing .env", ".env not found")
        except Exception:  # pragma: no cover - notification may fail
            pass
        raise err

    # Validate configuration early to detect misconfigurations.
    try:
        config.load_config()
    except Exception as exc:
        await logger.log_error("Config load failed", exc)
        try:
            await notify("Config error", str(exc))
        except Exception:  # pragma: no cover
            pass
        raise

    requirements = Path("requirements.txt")
    if not requirements.exists():
        await logger.log_warning("requirements.txt not found; skipping install")
        return

    cmd = [sys.executable, "-m", "pip", "install", "-r", str(requirements)]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        err = RuntimeError(stderr.decode().strip() or "pip install failed")
        await logger.log_error("Dependency installation failed", err)
        try:
            await notify("Install error", str(err))
        except Exception:  # pragma: no cover
            pass
        raise err
    await logger.log_info("Dependencies installed")


async def _start_bot() -> None:
    """Initialise and run the trading bot."""
    import trading_bot

    await trading_bot.initialize_bot()
    await trading_bot.run_trading_loop()


async def _start_gui() -> None:
    """Launch the optional GUI if present."""
    try:
        import gui  # type: ignore
    except Exception as exc:
        await logger.log_warning(f"GUI not started: {exc}")
        return

    entry = getattr(gui, "main", None) or getattr(gui, "run", None)
    if entry is None:
        await logger.log_warning("GUI module has no entry point")
        return

    try:
        result = entry()
        if asyncio.iscoroutine(result):
            await result
    except Exception as exc:
        await logger.log_error("GUI crashed", exc)
        try:
            await notify("GUI error", str(exc))
        except Exception:  # pragma: no cover
            pass


async def main() -> None:
    """Set up the environment and run the bot and GUI concurrently."""
    await setup_environment()
    await asyncio.gather(
        _start_bot(),
        _start_gui(),
    )


if __name__ == "__main__":  # pragma: no cover - manual execution
    try:
        asyncio.run(main())
    except Exception as exc:  # log any fatal errors
        try:
            asyncio.run(logger.log_error("Main crashed", exc))
        except Exception:
            pass
