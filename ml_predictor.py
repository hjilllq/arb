"""Простой предсказатель торговых сигналов на основе машинного обучения.

Модуль предоставляет небольшую обёртку вокруг моделей scikit-learn для
обучения классификаторов, прогнозирующих торговые сигналы. Реализация
лёгкая и предназначена для экспериментов; для реального использования
может потребоваться более сложное управление моделями."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Sequence
import logging

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV

from logger import log_event

# Попытка подключить PyTorch для ускорения через графический процессор Apple Silicon.
try:  # pragma: no cover - отсутствие torch не является ошибкой
    import torch
    from torch import nn, optim

    TORCH_AVAILABLE = True
except Exception:  # pragma: no cover - библиотека может быть не установлена
    TORCH_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class MLPredictor:
    """Обучать и использовать простую модель машинного обучения для предсказания сигналов.

    По умолчанию применяется логистическая регрессия из scikit‑learn. Если
    доступна библиотека ``torch`` и флаг ``use_torch`` активирован, то модель
    будет обучаться на графическом ускорителе Apple Silicon (устройство ``mps``)
    или на процессоре, что заметно ускоряет обработку больших массивов данных."""

    use_torch: bool = False
    model: LogisticRegression = field(
        default_factory=lambda: LogisticRegression(max_iter=1000)
    )
    torch_model: Any | None = field(default=None, init=False)
    device: str | None = field(default=None, init=False)

    def _torch_device(self) -> str:
        """Определить доступное устройство ``torch``.

        На платформах Apple Silicon возвращает ``mps`` при наличии поддержки
        Metal Performance Shaders. В противном случае используется CPU."""
        if torch.backends.mps.is_available() and torch.backends.mps.is_built():
            return "mps"
        return "cpu"

    def train_model(
        self, features: Sequence[Sequence[float]], labels: Sequence[int]
    ) -> None:
        """Обучить модель на предоставленном наборе данных.

        Если ``use_torch`` выставлен и PyTorch присутствует в окружении, то
        данные переводятся на выбранное устройство (графический процессор
        Apple или CPU), создаётся простая нейронная сеть с одним линейным
        слоем, и выполняется несколько эпох градиентного спуска. В противном
        случае используется логистическая регрессия из scikit‑learn."""

        if self.use_torch and TORCH_AVAILABLE:
            self.device = self._torch_device()
            X = torch.tensor(features, dtype=torch.float32, device=self.device)
            y = torch.tensor(labels, dtype=torch.float32, device=self.device).view(-1, 1)
            model = nn.Sequential(nn.Linear(X.shape[1], 1), nn.Sigmoid()).to(self.device)
            optimizer = optim.Adam(model.parameters(), lr=0.01)
            criterion = nn.BCELoss()
            for _ in range(100):  # короткое обучение, достаточное для тестов
                optimizer.zero_grad()
                pred = model(X)
                loss = criterion(pred, y)
                loss.backward()
                optimizer.step()
            self.torch_model = model
            log_event(f"ML model trained with PyTorch on {self.device}")
        else:
            self.model.fit(features, labels)
            log_event("ML model trained on %s samples" % len(labels))

    def predict_signal(self, features: Sequence[float]) -> int:
        """Предсказать торговый сигнал для одного вектора признаков."""
        if self.use_torch and TORCH_AVAILABLE and self.torch_model is not None:
            X = torch.tensor([features], dtype=torch.float32, device=self.device)
            with torch.no_grad():
                prob = self.torch_model(X).item()
            return int(prob >= 0.5)
        prediction = self.model.predict([features])[0]
        return int(prediction)

    def evaluate_model(
        self, features: Sequence[Sequence[float]], labels: Sequence[int]
    ) -> float:
        """Вернуть точность модели на данном наборе данных."""
        if self.use_torch and TORCH_AVAILABLE and self.torch_model is not None:
            X = torch.tensor(features, dtype=torch.float32, device=self.device)
            with torch.no_grad():
                preds = (self.torch_model(X).cpu().numpy() >= 0.5).astype(int)
            score = accuracy_score(labels, preds)
            return float(score)
        preds = self.model.predict(features)
        score = accuracy_score(labels, preds)
        return float(score)

    def optimize_model(
        self,
        features: Sequence[Sequence[float]],
        labels: Sequence[int],
        param_grid: Dict[str, Iterable[Any]] | None = None,
    ) -> None:
        """Подобрать гиперпараметры методом перебора сетки и заменить модель."""
        if self.use_torch and TORCH_AVAILABLE:
            raise NotImplementedError(
                "Hyperparameter optimization not implemented for PyTorch models"
            )
        if param_grid is None:
            param_grid = {"C": [0.1, 1.0, 10.0]}
        # Определяем безопасное количество блоков кросс-валидации
        # (folds — отдельные части выборки) по наименьшему числу элементов класса
        try:
            from collections import Counter

            min_class = min(Counter(labels).values())
        except Exception:  # pragma: no cover - защитный код
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
        """Логировать переданные метрики оценки."""
        for key, value in metrics.items():
            logger.info("%s: %s", key, value)
