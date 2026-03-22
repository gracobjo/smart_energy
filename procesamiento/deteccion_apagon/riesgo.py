"""
Cálculo de risk_score (0–100) y alertas críticas por riesgo de apagón.

Factores modelados (industrial / operación de red):
1. Sobretensión / subtensión: desviación del voltaje nominal por subestación.
2. Inestabilidad de frecuencia: desviación respecto a 50 Hz (referencia ENTSO-E).
3. Pérdida de margen de generación: ratio entre carga agregada y capacidad disponible.
4. Desconexión en cascada: proporción de líneas en estado anómalo + nodos de articulación.

Los pesos son configurables por variables de entorno (prefijo RIESGO_APAGON_).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# Umbral por encima del cual se emite alerta crítica (centinela SOC)
UMBRAL_RIESGO_CRITICO = float(os.environ.get("RIESGO_APAGON_UMBRAL_CRITICO", "75.0"))

# Pesos por componente (deben sumar 1.0 para interpretación directa)
PESO_VOLTAJE = float(os.environ.get("RIESGO_APAGON_PESO_VOLTAJE", "0.28"))
PESO_FRECUENCIA = float(os.environ.get("RIESGO_APAGON_PESO_FRECUENCIA", "0.27"))
PESO_GENERACION = float(os.environ.get("RIESGO_APAGON_PESO_GENERACION", "0.25"))
PESO_CASCADA = float(os.environ.get("RIESGO_APAGON_PESO_CASCADA", "0.20"))

FRECUENCIA_NOMINAL_HZ = float(os.environ.get("RIESGO_APAGON_FREQ_NOMINAL", "50.0"))
# Desviación de frecuencia que satura el componente (Hz) — orden típico de inestabilidad grave
FREQ_DESV_MAX_HZ = float(os.environ.get("RIESGO_APAGON_FREQ_DESV_MAX", "0.5"))

# Voltaje: banda ±5% como referencia; desviación mayor acerca el riesgo a 1 en ese nodo
VOLTAJE_BANDA_PCT = float(os.environ.get("RIESGO_APAGON_VOLTAJE_BANDA_PCT", "5.0"))


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def componente_voltaje(
    voltajes_kv: Dict[str, float],
    nominal_kv_por_id: Optional[Dict[str, float]] = None,
    nominal_default_kv: float = 220.0,
) -> float:
    """
    Riesgo por sobretensión/subtensión: desviación respecto a voltaje nominal por subestación.
    Dentro de ±VOLTAJE_BANDA_PCT el estrés es bajo; fuera, escala hacia 1.
    """
    if not voltajes_kv:
        return 0.0
    nominal_kv_por_id = nominal_kv_por_id or {}
    scores: List[float] = []
    banda = VOLTAJE_BANDA_PCT / 100.0
    for sid, v_kv in voltajes_kv.items():
        if v_kv is None or v_kv <= 0:
            continue
        nom = nominal_kv_por_id.get(sid, nominal_default_kv)
        if nom <= 0:
            continue
        dev = abs(v_kv / nom - 1.0)
        if dev <= banda:
            scores.append(0.2 * (dev / max(banda, 1e-9)))
        else:
            scores.append(_clamp01((dev - banda) / 0.15))
    if not scores:
        return 0.0
    return sum(scores) / len(scores)


def componente_frecuencia(frecuencia_hz: Optional[float]) -> float:
    """Inestabilidad de frecuencia. Si no hay medida, 0 (no penaliza)."""
    if frecuencia_hz is None:
        return 0.0
    desv = abs(frecuencia_hz - FRECUENCIA_NOMINAL_HZ)
    return _clamp01(desv / max(FREQ_DESV_MAX_HZ, 1e-6))


def componente_perdida_generacion(
    potencia_total_mw: float,
    capacidad_total_mw: float,
) -> float:
    """
    Pérdida de margen / estrés de generación: uso elevado de la capacidad disponible
    (proxy de falta de margen de reserva ante desconexiones).
    """
    if capacidad_total_mw <= 0:
        return 0.5
    uso = potencia_total_mw / capacidad_total_mw
    return _clamp01(uso)


def componente_cascada(
    num_lineas_anomalas: int,
    num_lineas_total: int,
    num_articulaciones: int,
    num_subestaciones: int,
) -> float:
    """
    Desconexión en cascada: líneas en estado no nominal + criticidad estructural (articulaciones).
    """
    r_lineas = 0.0
    if num_lineas_total > 0:
        r_lineas = _clamp01(num_lineas_anomalas / max(num_lineas_total, 1))
    r_art = 0.0
    if num_subestaciones > 0:
        r_art = _clamp01(num_articulaciones / max(num_subestaciones, 1))
    return _clamp01(0.55 * r_lineas + 0.45 * r_art)


def evaluar_riesgo_apagon_metricas(
    voltajes_kv: Dict[str, float],
    potencia_total_mw: float,
    capacidad_total_mw: float,
    num_lineas_anomalas: int,
    num_lineas_total: int,
    num_articulaciones: int,
    num_subestaciones: int,
    frecuencia_hz: Optional[float] = None,
    nominal_kv_por_id: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """
    Calcula risk_score 0–100 y bandera de alerta crítica.
    """
    c_v = componente_voltaje(voltajes_kv, nominal_kv_por_id)
    c_f = componente_frecuencia(frecuencia_hz)
    c_g = componente_perdida_generacion(potencia_total_mw, capacidad_total_mw)
    c_c = componente_cascada(
        num_lineas_anomalas,
        num_lineas_total,
        num_articulaciones,
        num_subestaciones,
    )

    w_sum = PESO_VOLTAJE + PESO_FRECUENCIA + PESO_GENERACION + PESO_CASCADA
    if w_sum <= 0:
        w_sum = 1.0
    risk_score = (
        100.0
        * (
            PESO_VOLTAJE * c_v
            + PESO_FRECUENCIA * c_f
            + PESO_GENERACION * c_g
            + PESO_CASCADA * c_c
        )
        / w_sum
    )
    risk_score = round(min(100.0, max(0.0, risk_score)), 2)
    alerta_critica = risk_score >= UMBRAL_RIESGO_CRITICO

    return {
        "risk_score": risk_score,
        "alerta_critica": alerta_critica,
        "umbral_critico": UMBRAL_RIESGO_CRITICO,
        "componentes": {
            "sobretension_voltaje": round(c_v, 4),
            "inestabilidad_frecuencia": round(c_f, 4),
            "perdida_margen_generacion": round(c_g, 4),
            "desconexion_cascada": round(c_c, 4),
        },
        "pesos": {
            "voltaje": PESO_VOLTAJE,
            "frecuencia": PESO_FRECUENCIA,
            "generacion": PESO_GENERACION,
            "cascada": PESO_CASCADA,
        },
        "frecuencia_hz_medida": frecuencia_hz,
    }


def evaluar_riesgo_apagon_desde_snapshots(
    subestaciones: Dict[str, Dict[str, Any]],
    lineas: Dict[str, Dict[str, Any]],
    puntos_fallo: List[Dict[str, Any]],
    frecuencia_hz: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Agrega datos típicos del dashboard/Cassandra y evalúa el riesgo.

    - voltaje: subestaciones[*].voltaje_kv
    - potencia/capacidad: sumas agregadas
    - líneas anómalas: estado distinto de 'ok'
    - articulaciones: len(puntos_fallo) o filas con es_articulacion
    """
    voltajes: Dict[str, float] = {}
    potencia_total = 0.0
    capacidad_total = 0.0
    for sid, d in subestaciones.items():
        v = d.get("voltaje_kv")
        if v is not None:
            try:
                voltajes[sid] = float(v)
            except (TypeError, ValueError):
                pass
        p = d.get("potencia_mw")
        c = d.get("capacidad_mw")
        if p is not None:
            try:
                potencia_total += float(p)
            except (TypeError, ValueError):
                pass
        if c is not None:
            try:
                capacidad_total += float(c)
            except (TypeError, ValueError):
                pass

    num_lineas_total = len(lineas)
    anomalas = 0
    for _k, ld in lineas.items():
        est = (ld.get("estado") or "ok").lower()
        if est not in ("ok", "fluido", "normal"):
            anomalas += 1

    articulaciones = len(puntos_fallo) if puntos_fallo else 0
    n_sub = max(len(subestaciones), 1)

    return evaluar_riesgo_apagon_metricas(
        voltajes_kv=voltajes,
        potencia_total_mw=potencia_total,
        capacidad_total_mw=max(capacidad_total, 1e-6),
        num_lineas_anomalas=anomalas,
        num_lineas_total=max(num_lineas_total, 1),
        num_articulaciones=articulaciones,
        num_subestaciones=n_sub,
        frecuencia_hz=frecuencia_hz,
    )
