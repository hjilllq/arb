import asyncio
import sys

from error_handler import install_global_handler


class DummyNotifier:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send_telegram_notification(self, message: str) -> bool:  # pragma: no cover - простой заглушки
        self.messages.append(message)
        return True


def test_sys_excepthook_triggers_notification() -> None:
    notifier = DummyNotifier()
    original = sys.excepthook
    install_global_handler(notifier)
    try:
        # Имитация необработанного исключения
        sys.excepthook(ValueError, ValueError("boom"), None)
        assert any("boom" in msg for msg in notifier.messages)
    finally:
        sys.excepthook = original


async def _raise_async_error() -> None:
    raise RuntimeError("async boom")


def test_loop_exception_handler() -> None:
    notifier = DummyNotifier()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    original = loop.get_exception_handler()
    install_global_handler(notifier)

    async def runner() -> None:
        asyncio.create_task(_raise_async_error())
        await asyncio.sleep(0)

    try:
        loop.run_until_complete(runner())
        assert any("async boom" in msg for msg in notifier.messages)
    finally:
        loop.set_exception_handler(original)
        loop.close()
        asyncio.set_event_loop(None)
