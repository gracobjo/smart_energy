"""Detección de anomalías en series de consumo (IsolationForest + puntuación)."""
from __future__ import annotations

from typing import List, Tuple

import numpy as np
from sklearn.ensemble import IsolationForest


def detect_anomalies(
    consumptions: List[float],
    contamination: float = 0.1,
) -> List[Tuple[int, float, float]]:
    """
    Devuelve lista de (índice, consumo, score_anomalía).
    Score negativo en IsolationForest suele indicar anomalía.
    """
    if len(consumptions) < 4:
        return []
    X = np.array(consumptions, dtype=float).reshape(-1, 1)
    n_est = min(50, max(10, len(consumptions) * 2))
    clf = IsolationForest(
        n_estimators=n_est,
        contamination=min(0.5, max(0.05, contamination)),
        random_state=42,
    )
    clf.fit(X)
    scores = clf.decision_function(X)
    preds = clf.predict(X)  # 1 inlier, -1 outlier
    anomalies: List[Tuple[int, float, float]] = []
    for i, (c, s, p) in enumerate(zip(consumptions, scores, preds)):
        if p == -1:
            anomalies.append((i, float(c), float(-s)))
    return anomalies
