import platform
import pytest

from main import ensure_apple_silicon


def test_ensure_apple_silicon_non_apple(monkeypatch):
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    with pytest.raises(EnvironmentError):
        ensure_apple_silicon()
