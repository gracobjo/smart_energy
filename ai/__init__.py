"""Módulo de IA para predicción de consumo y detección de anomalías (scikit-learn)."""

from .prediction import predict_consumption, train_consumption_model
from .anomalies import detect_anomalies

__all__ = [
    "predict_consumption",
    "train_consumption_model",
    "detect_anomalies",
]
