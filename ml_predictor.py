"""Simple machine learning predictor for trading signals.

This module offers a small wrapper around scikit-learn models to train
classifiers that forecast trading signals.  It is intentionally
lightweight and meant for experimentation; real-world deployments may
require more elaborate model management.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Sequence
import logging

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV

from logger import log_event

logger = logging.getLogger(__name__)


@dataclass
class MLPredictor:
    """Train and use a basic machine learning model for signal prediction."""

    model: LogisticRegression = field(
        default_factory=lambda: LogisticRegression(max_iter=1000)
    )

    def train_model(
        self, features: Sequence[Sequence[float]], labels: Sequence[int]
    ) -> None:
        """Fit the underlying model on the provided dataset."""
        self.model.fit(features, labels)
        log_event("ML model trained on %s samples" % len(labels))

    def predict_signal(self, features: Sequence[float]) -> int:
        """Predict a trading signal for a single feature vector."""
        prediction = self.model.predict([features])[0]
        return int(prediction)

    def evaluate_model(
        self, features: Sequence[Sequence[float]], labels: Sequence[int]
    ) -> float:
        """Return accuracy of the model on the given dataset."""
        preds = self.model.predict(features)
        score = accuracy_score(labels, preds)
        return float(score)

    def optimize_model(
        self,
        features: Sequence[Sequence[float]],
        labels: Sequence[int],
        param_grid: Dict[str, Iterable[Any]] | None = None,
    ) -> None:
        """Tune hyperparameters using grid search and replace the model."""
        if param_grid is None:
            param_grid = {"C": [0.1, 1.0, 10.0]}
        # Determine a safe number of folds based on the smallest class count
        try:
            from collections import Counter

            min_class = min(Counter(labels).values())
        except Exception:  # pragma: no cover - defensive
            min_class = 1
        cv = min(3, min_class)
        if cv < 2:
            raise ValueError("Need at least two samples per class for optimization")
        grid = GridSearchCV(
            LogisticRegression(max_iter=1000),
            param_grid,
            cv=cv,
            n_jobs=-1,
        )
        grid.fit(features, labels)
        self.model = grid.best_estimator_
        log_event(f"Optimized ML model with best params {grid.best_params_}")

    def log_model_results(self, metrics: Dict[str, Any]) -> None:
        """Log provided evaluation metrics."""
        for key, value in metrics.items():
            logger.info("%s: %s", key, value)
