from monitor import ResourceMonitor


def test_check_resources_returns_metrics():
    monitor = ResourceMonitor(cpu_threshold=0.0, memory_threshold=0.0, disk_threshold=0.0)
    stats = monitor.check_resources()
    assert set(stats.keys()) == {"cpu", "memory", "disk"}
    assert all(isinstance(value, float) for value in stats.values())
