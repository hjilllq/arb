"""Lightweight stand-in for ccxt.async_support used in tests."""

class RequestTimeout(Exception):
    """Raised when an exchange request times out."""

class NetworkError(Exception):
    """Raised when a network issue occurs."""

class DummyExchange:
    """Placeholder exchange class."""
    pass

# bybit constructor placeholder; tests monkeypatch this with a fake client.
def bybit(config=None):  # pragma: no cover - only used when not patched
    return DummyExchange()
