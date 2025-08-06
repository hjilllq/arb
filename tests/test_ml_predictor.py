import sys
import types
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import config


def _build_fake_tf(monkeypatch, pred_value=0.005):
    class FakeLayer:
        def __init__(self, *a, **k):
            pass

    class FakeModel:
        def __init__(self):
            pass

        def compile(self, *a, **k):
            pass

        def fit(self, *a, **k):
            pass

        def predict(self, x, verbose=0):
            return np.full((len(x), 1), pred_value)

        def save(self, path):
            Path(path).write_text("model")

        def evaluate(self, x, y, verbose=0):
            return float(np.mean((y - pred_value) ** 2))

    fake_tf = types.SimpleNamespace(
        keras=types.SimpleNamespace(
            Sequential=lambda layers: FakeModel(),
            layers=types.SimpleNamespace(LSTM=FakeLayer, Dropout=FakeLayer, Dense=FakeLayer),
            callbacks=types.SimpleNamespace(EarlyStopping=lambda **k: object()),
            models=types.SimpleNamespace(load_model=lambda p: FakeModel()),
        )
    )
    monkeypatch.setitem(sys.modules, "tensorflow", fake_tf)


def _make_df(rows=60):
    spot = np.linspace(100, 100 + rows - 1, rows)
    futures = spot * 1.005
    return pd.DataFrame({
        "spot_symbol": ["BTC/USDT"] * rows,
        "futures_symbol": ["BTCUSDT"] * rows,
        "spot_price": spot,
        "futures_price": futures,
        "rsi": np.linspace(60, 60 + rows - 1, rows),
        "macd": np.linspace(0.1, 0.1 + rows - 1, rows),
    })


def test_train_and_predict_basis(monkeypatch, tmp_path):
    _build_fake_tf(monkeypatch, pred_value=0.005)
    monkeypatch.setattr(config, "CONFIG", {"SPOT_PAIRS": "['BTC/USDT']", "FUTURES_PAIRS": "['BTCUSDT']"})
    sys.modules.pop("ml_predictor", None)
    import ml_predictor
    monkeypatch.setattr(ml_predictor, "_MODEL_DIR", tmp_path, False)
    monkeypatch.setattr(ml_predictor, "_MODEL_PATH", tmp_path / "basis.keras", False)
    df = _make_df()
    ml_predictor.train_model(df)
    pred = ml_predictor.predict_basis(df.tail(1))
    assert pred == pytest.approx(0.005, rel=1e-6)


def test_evaluate_model_notifies_on_low_accuracy(monkeypatch, tmp_path):
    _build_fake_tf(monkeypatch, pred_value=0.0)
    monkeypatch.setattr(config, "CONFIG", {"SPOT_PAIRS": "['BTC/USDT']", "FUTURES_PAIRS": "['BTCUSDT']"})
    sys.modules.pop("ml_predictor", None)
    import ml_predictor
    monkeypatch.setattr(ml_predictor, "_MODEL_DIR", tmp_path, False)
    model_path = tmp_path / "basis.keras"
    model_path.write_text("model")
    monkeypatch.setattr(ml_predictor, "_MODEL_PATH", model_path, False)

    calls = {}
    async def fake_notify(msg, detail=""):
        calls["msg"] = msg
    module = types.SimpleNamespace(notify=fake_notify)
    monkeypatch.setitem(sys.modules, "notification_manager", module)

    acc = ml_predictor.evaluate_model(_make_df())
    assert acc < 0.85
    assert calls["msg"] == "Low model accuracy"
