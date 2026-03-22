"""Tests unitarios del módulo de riesgo de apagón (sin Spark)."""
import pytest

from procesamiento.deteccion_apagon.riesgo import (
    componente_cascada,
    componente_frecuencia,
    componente_perdida_generacion,
    componente_voltaje,
    evaluar_riesgo_apagon_desde_snapshots,
    evaluar_riesgo_apagon_metricas,
)


def test_voltaje_alto_aumenta_componente():
    v = {"SE1": 260.0}  # nominal 220 → fuerte desviación
    c = componente_voltaje(v, nominal_default_kv=220.0)
    assert c > 0.5


def test_frecuencia_none_es_cero():
    assert componente_frecuencia(None) == 0.0


def test_frecuencia_50_es_cero():
    assert componente_frecuencia(50.0) == 0.0


def test_frecuencia_desviacion_alta():
    c = componente_frecuencia(49.0)
    assert c > 0.5


def test_generacion_al_uso_alto():
    c = componente_perdida_generacion(950.0, 1000.0)
    assert c > 0.9


def test_cascada_con_lineas_anomalas():
    c = componente_cascada(5, 10, 2, 20)
    assert c > 0


def test_evaluar_metricas_alerta_critica():
    out = evaluar_riesgo_apagon_metricas(
        voltajes_kv={"A": 300.0},
        potencia_total_mw=990.0,
        capacidad_total_mw=1000.0,
        num_lineas_anomalas=8,
        num_lineas_total=10,
        num_articulaciones=5,
        num_subestaciones=10,
        frecuencia_hz=48.5,
    )
    assert "risk_score" in out
    assert out["risk_score"] >= 0
    assert isinstance(out["alerta_critica"], bool)


def test_evaluar_desde_snapshots_vacio():
    out = evaluar_riesgo_apagon_desde_snapshots({}, {}, [])
    assert out["risk_score"] >= 0


def test_snapshots_con_datos_demo():
    sub = {
        "S1": {"voltaje_kv": 220.0, "potencia_mw": 100.0, "capacidad_mw": 200.0, "estado": "ok"},
    }
    lineas = {"a|b": {"estado": "ok", "flujo_mw": 10.0, "capacidad_mw": 100.0}}
    pf = [{"id_subestacion": "S1"}]
    out = evaluar_riesgo_apagon_desde_snapshots(sub, lineas, pf, frecuencia_hz=50.0)
    assert 0 <= out["risk_score"] <= 100
    assert "componentes" in out
