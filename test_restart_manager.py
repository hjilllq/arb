import asyncio

from restart_manager import RestartManager


class DummySystem:
    runs = 0

    def __init__(self) -> None:
        type(self).runs += 1

    async def run(self) -> None:
        if self.runs == 1:
            raise RuntimeError("boom")

    def stop_trading(self) -> None:  # pragma: no cover - ничего не делает
        pass


def test_restart_manager_restarts(monkeypatch):
    async def dummy_monitor(self, system):
        await asyncio.sleep(3600)

    monkeypatch.setattr(RestartManager, "_monitor", dummy_monitor, raising=False)

    manager = RestartManager(system_factory=DummySystem, restart_delay=0.01)
    asyncio.run(manager.run())

    assert DummySystem.runs == 2
