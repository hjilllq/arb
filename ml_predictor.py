"""Machine learning predictor for basis forecasting.

This module behaves like a fortune teller for the arbitrage bot.  It
trains and serves a tiny LSTM network that predicts the basis between
spot and futures markets.  The implementation is intentionally small and
lightweight so it can run 24/7 on an Apple Silicon M4 Max while keeping
power consumption low.  Heavy TensorFlow work is delegated to a single
thread and the code gracefully handles missing dependencies or limited
data.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

import data_analyzer
from logger import log_error, log_info, log_warning

try:  # pragma: no cover - optional dependency in tests
    import tensorflow as tf
except Exception as exc:  # pragma: no cover - handled during runtime
    tf = None  # type: ignore
    # log asynchronously so import failures do not crash at module import
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(log_error("TensorFlow unavailable", exc))
    except RuntimeError:
        try:
            asyncio.run(log_error("TensorFlow unavailable", exc))
        except Exception:
            pass
    except Exception:
        pass

# Directory where trained models are stored
_MODEL_DIR = Path("models")
_MODEL_PATH = _MODEL_DIR / "basis_lstm.keras"


def _notify(message: str, detail: str = "") -> None:
    """Best effort notification helper."""
    try:  # pragma: no cover - optional notifier
        from notification_manager import notify  # type: ignore

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(notify(message, detail))
        except RuntimeError:
            asyncio.run(notify(message, detail))
    except Exception:
        pass


def _prepare_features(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Return feature and target arrays for the model."""
    features = df[["spot_price", "futures_price", "rsi", "macd"]].values
    target = ((df["futures_price"] - df["spot_price"]) / df["spot_price"]).values
    x = features.reshape((features.shape[0], 1, features.shape[1]))
    y = target.reshape((-1, 1))
    return x, y


def train_model(data: pd.DataFrame) -> None:
    """Train an LSTM model on three months of data.

    Parameters
    ----------
    data:
        Preprocessed dataframe containing spot/futures prices and
        indicators.  At least ~100 rows are recommended for meaningful
        training.
    """
    if tf is None:
        raise ImportError("TensorFlow is required to train the model")

    df = data_analyzer.preprocess_data(data.to_dict("records"))
    if len(df) < 50:  # need enough history for training
        msg = "Not enough data for training"
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(log_error(msg, ValueError()))
        except RuntimeError:
            asyncio.run(log_error(msg, ValueError()))
        _notify("Training error", msg)
        raise ValueError(msg)

    x, y = _prepare_features(df)

    model = tf.keras.Sequential([
        tf.keras.layers.LSTM(50, return_sequences=True, input_shape=(1, x.shape[2])),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.LSTM(50),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(1),
    ])
    model.compile(optimizer="adam", loss="mse")
    early = tf.keras.callbacks.EarlyStopping(patience=3, restore_best_weights=True)

    # Train quietly to keep tests fast and energy usage low
    model.fit(x, y, epochs=10, batch_size=32, validation_split=0.2, callbacks=[early], verbose=0)

    _MODEL_DIR.mkdir(exist_ok=True)
    model.save(_MODEL_PATH)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(log_info(f"Model trained and saved to {_MODEL_PATH}"))
    except RuntimeError:
        asyncio.run(log_info(f"Model trained and saved to {_MODEL_PATH}"))


def _load_model() -> "tf.keras.Model":
    if tf is None:
        raise ImportError("TensorFlow is required to load the model")
    if not _MODEL_PATH.exists():
        raise FileNotFoundError("Trained model not found; call train_model first")
    return tf.keras.models.load_model(_MODEL_PATH)


def predict_basis(data: pd.DataFrame) -> float:
    """Return the predicted basis for the last row of *data*.

    The incoming data is validated and passed through the same
    preprocessing as during training.
    """
    model = _load_model()
    df = data_analyzer.preprocess_data(data.to_dict("records"))
    if df.empty:
        msg = "No data provided for prediction"
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(log_error(msg, ValueError()))
        except RuntimeError:
            asyncio.run(log_error(msg, ValueError()))
        _notify("Prediction error", msg)
        raise ValueError(msg)

    x, _ = _prepare_features(df)
    pred = model.predict(x, verbose=0)[-1][0]
    return float(pred)


def evaluate_model(data: pd.DataFrame) -> float:
    """Evaluate the trained model and return accuracy in the 0-1 range."""
    model = _load_model()
    df = data_analyzer.preprocess_data(data.to_dict("records"))
    if df.empty:
        msg = "No data provided for evaluation"
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(log_error(msg, ValueError()))
        except RuntimeError:
            asyncio.run(log_error(msg, ValueError()))
        _notify("Evaluation error", msg)
        raise ValueError(msg)

    x, y = _prepare_features(df)
    preds = model.predict(x, verbose=0).reshape(-1)
    # mean absolute percentage error
    eps = np.finfo(float).eps
    mape = np.mean(np.abs((y.reshape(-1) - preds) / (y.reshape(-1) + eps)))
    accuracy = 1 - mape
    if accuracy < 0.85:
        msg = f"Model accuracy low: {accuracy:.2%}"
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(log_warning(msg))
        except RuntimeError:
            asyncio.run(log_warning(msg))
        _notify("Low model accuracy", msg)
    else:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(log_info(f"Model accuracy {accuracy:.2%}"))
        except RuntimeError:
            asyncio.run(log_info(f"Model accuracy {accuracy:.2%}"))
    return float(accuracy)

