"""
Regresión simple sobre series de consumo (lags).
Reutilizable desde FastAPI sin dependencias del pipeline Spark/Cassandra legacy.
"""
from __future__ import annotations

from typing import List, Tuple

import numpy as np
from sklearn.ensemble import RandomForestRegressor


def _lags(series: np.ndarray, n_lags: int) -> Tuple[np.ndarray, np.ndarray]:
    """Construye X (lags) e y (valor siguiente)."""
    if len(series) <= n_lags + 1:
        return np.empty((0, n_lags)), np.empty((0,))
    X, y = [], []
    for i in range(n_lags, len(series)):
        X.append(series[i - n_lags : i])
        y.append(series[i])
    return np.array(X), np.array(y)


def train_consumption_model(consumptions: List[float], n_lags: int = 3) -> RandomForestRegressor | None:
    arr = np.array(consumptions, dtype=float)
    if len(arr) < n_lags + 4:
        return None
    X, y = _lags(arr, n_lags)
    if len(X) < 3:
        return None
    model = RandomForestRegressor(n_estimators=30, max_depth=6, random_state=42)
    model.fit(X, y)
    # Guardar n_lags en atributo dinámico para inferencia
    model.n_lags_ = n_lags  # type: ignore[attr-defined]
    return model


def predict_consumption(
    model: RandomForestRegressor,
    history: List[float],
    horizon_hours: int,
) -> List[float]:
    """Predice los siguientes `horizon_hours` valores autoregresivamente."""
    n_lags = int(getattr(model, "n_lags_", 3))
    seq = list(map(float, history[-n_lags:]))
    if len(seq) < n_lags:
        seq = [float(np.mean(history))] * (n_lags - len(seq)) + seq
    out: List[float] = []
    for _ in range(horizon_hours):
        x = np.array(seq[-n_lags:]).reshape(1, -1)
        nxt = float(model.predict(x)[0])
        out.append(max(0.0, nxt))
        seq.append(nxt)
    return out
