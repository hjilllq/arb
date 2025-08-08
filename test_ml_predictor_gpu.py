import pytest

from ml_predictor import MLPredictor

# Тест выполняется только если установлен torch
pytest.importorskip("torch")


def test_pytorch_training_and_prediction():
    X = [[0.0], [1.0], [0.2], [0.8]]
    y = [0, 1, 0, 1]
    predictor = MLPredictor(use_torch=True)
    predictor.train_model(X, y)
    # устройство должно быть определено даже если доступен только CPU
    assert predictor.device in {"cpu", "mps"}
    pred = predictor.predict_signal([0.5])
    assert pred in (0, 1)
