"""
Detección de riesgo de apagón eléctrico: sobretensión, frecuencia, generación, cascada.
"""

from procesamiento.deteccion_apagon.riesgo import (
    UMBRAL_RIESGO_CRITICO,
    evaluar_riesgo_apagon_desde_snapshots,
    evaluar_riesgo_apagon_metricas,
)

__all__ = [
    "UMBRAL_RIESGO_CRITICO",
    "evaluar_riesgo_apagon_desde_snapshots",
    "evaluar_riesgo_apagon_metricas",
]
