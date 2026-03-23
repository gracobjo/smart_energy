# -*- coding: utf-8 -*-
"""
Panel Streamlit: riesgo de apagón (risk_score) y alertas críticas.
"""
from __future__ import annotations

import copy
import os
from typing import Any, Dict, List

import streamlit as st

from procesamiento.deteccion_apagon import evaluar_riesgo_apagon_desde_snapshots


def _aplicar_preset_colapso() -> None:
    st.session_state["riesgo_inc_carga_pct"] = 35
    st.session_state["riesgo_perdida_cap_pct"] = 25
    st.session_state["riesgo_horizonte_min"] = 15
    st.session_state["riesgo_freq_hz"] = "49.2"
    st.session_state["riesgo_cascada_virtual_pct"] = 80


def _aplicar_preset_preventivo() -> None:
    st.session_state["riesgo_inc_carga_pct"] = 10
    st.session_state["riesgo_perdida_cap_pct"] = 5
    st.session_state["riesgo_horizonte_min"] = 30
    st.session_state["riesgo_freq_hz"] = ""
    st.session_state["riesgo_cascada_virtual_pct"] = 20


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _estado_estimado_subestacion(voltaje_kv: float, uso_pct: float) -> str:
    # Banda nominal simplificada para panel operativo (referencia 220 kV).
    v_nom = 220.0
    desv = abs(voltaje_kv - v_nom) / v_nom if v_nom else 0.0
    if uso_pct >= 95 or desv >= 0.10:
        return "sobrecarga"
    if uso_pct >= 85 or desv >= 0.05:
        return "alerta"
    return "ok"


def _simular_escenario(
    subestaciones: Dict[str, Dict[str, Any]],
    lineas: Dict[str, Dict[str, Any]],
    incremento_carga_pct: float,
    perdida_capacidad_pct: float,
    lineas_extra_anomalas: int,
    cascada_virtual_pct: float = 0.0,
) -> Dict[str, Any]:
    sub_sim = copy.deepcopy(subestaciones or {})
    lin_sim = copy.deepcopy(lineas or {})

    # 1) Estrés de carga/capacidad en subestaciones.
    for sid, s in sub_sim.items():
        cap_orig = max(_to_float(s.get("capacidad_mw"), 0.0), 1.0)
        pot_orig = _to_float(s.get("potencia_mw"), 0.0)
        uso_orig = _to_float(s.get("uso_pct"), 0.0)

        # Si no hay potencia real (modo demo o telemetría incompleta), estimamos una base
        # para que la simulación con sliders sea operativa y no quede plana en 0.
        if pot_orig <= 0.0:
            if uso_orig > 0.0:
                pot_orig = cap_orig * (uso_orig / 100.0)
            else:
                # Base dinámica en modo demo: a mayor estrés de carga, mayor ocupación inicial.
                base_demo = 0.35 + min(0.60, max(0.0, incremento_carga_pct) / 100.0)
                pot_orig = cap_orig * min(0.95, base_demo)

        pot = pot_orig * (1.0 + incremento_carga_pct / 100.0)
        cap = cap_orig * (1.0 - perdida_capacidad_pct / 100.0)
        cap = max(cap, 1.0)
        uso = (pot / cap) * 100.0

        # Ajuste de voltaje sintético suave en estrés alto para que el componente de
        # sobretensión/subtensión también responda en escenarios sin medida PMU real.
        v_base = _to_float(s.get("voltaje_kv"), 220.0)
        v_delta = 0.0
        if uso >= 85.0:
            # En estrés alto simulamos infratensión más marcada para acercar componente voltaje a zona crítica.
            v_delta = -min(35.0, (uso - 85.0) * 1.8)
        v = v_base + v_delta

        est = _estado_estimado_subestacion(v, uso)
        s["potencia_mw"] = round(pot, 2)
        s["capacidad_mw"] = round(cap, 2)
        s["uso_pct"] = round(uso, 2)
        s["voltaje_kv"] = round(v, 2)
        s["estado"] = est
        if est != "ok" and not s.get("motivo"):
            s["motivo"] = "Escenario de contingencia (carga/capacidad)."

    # 2) Escalada en líneas (anomalías adicionales).
    candidatos = []
    for k, l in lin_sim.items():
        flujo = _to_float(l.get("flujo_mw"), 0.0)
        cap = max(_to_float(l.get("capacidad_mw"), 1.0), 1.0)
        ratio = flujo / cap
        candidatos.append((ratio, k))
    candidatos.sort(reverse=True)
    extra = max(0, int(lineas_extra_anomalas))
    for _, key in candidatos[:extra]:
        l = lin_sim.get(key, {})
        if (l.get("estado") or "ok").lower() == "ok":
            l["estado"] = "alerta"
            if not l.get("motivo"):
                l["motivo"] = "Escenario de contingencia (propagación)."

    # 3) Si no hay líneas reales, inyectar líneas virtuales para simular cascada.
    if not lin_sim:
        virt_total = 20
        pct = max(0.0, min(100.0, float(cascada_virtual_pct)))
        virt_anom = int(round((pct / 100.0) * virt_total))
        for i in range(virt_total):
            estado = "alerta" if i < virt_anom else "ok"
            lin_sim[f"virtual_{i}"] = {
                "src": f"V{i}",
                "dst": f"V{i+1}",
                "flujo_mw": 100.0 if estado == "alerta" else 60.0,
                "capacidad_mw": 100.0,
                "estado": estado,
                "motivo": "Cascada virtual (simulación)" if estado != "ok" else "",
            }

    return {"subestaciones": sub_sim, "lineas": lin_sim}


def _recomendaciones_operativas(risk_score: float, criticos: List[str]) -> List[str]:
    if risk_score >= 85:
        return [
            "Activar protocolo de crisis: shedding selectivo por prioridad de carga.",
            "Aislar temporalmente nodos críticos para contener cascada.",
            "Reconfigurar flujo por corredores alternativos y bajar transferencias en líneas al límite.",
            f"Vigilar subestaciones críticas: {', '.join(criticos[:6]) if criticos else 'sin lista prioritaria'}",
        ]
    if risk_score >= 75:
        return [
            "Pre-contingencia: reducir carga no crítica y reservar margen de generación.",
            "Rebalancear despacho para bajar uso_pct en nodos calientes.",
            "Aumentar monitorización de articulaciones y líneas no nominales.",
            f"Priorizar intervención en: {', '.join(criticos[:6]) if criticos else 'sin lista prioritaria'}",
        ]
    if risk_score >= 55:
        return [
            "Modo preventivo: suavizar picos y aplazar maniobras de mantenimiento.",
            "Comprobar tensión y frecuencia cada ciclo operativo.",
            "Preparar rutas alternativas por si escala a alerta crítica.",
        ]
    return [
        "Operación estable: mantener monitorización continua.",
        "Ensayar planes de contingencia en modo simulación.",
    ]


def render_riesgo_apagon_panel(
    subestaciones: Dict[str, Dict[str, Any]],
    lineas: Dict[str, Dict[str, Any]],
    articulaciones: List[Dict[str, Any]],
) -> None:
    """Muestra risk_score, desglose y alerta crítica; permite simular frecuencia de red (Hz)."""
    st.markdown("### Riesgo de apagón eléctrico (risk_score)")
    st.caption(
        "Modelo agregado: sobretensión/subtensión, inestabilidad de frecuencia (opcional), "
        "estrés de generación (carga vs capacidad), y propagación en cascada (líneas + articulaciones). "
        "Ver documentación: **docs/APAGON_ESPANA_2025_CASO.md**."
    )

    col_f, _ = st.columns([1, 2])
    with col_f:
        default_f = os.environ.get("SIM_FREQ_HZ", "")
        f_str = st.text_input(
            "Frecuencia de red simulada (Hz) — vacío = no medir",
            value=default_f,
            placeholder="50.0",
            key="riesgo_freq_hz",
            help="En planta real se inyecta por SCADA/PMU. En demo puede dejarse vacío.",
        )
    f_hz = None
    if f_str.strip():
        try:
            f_hz = float(f_str.replace(",", "."))
        except ValueError:
            st.warning("Frecuencia no numérica; se ignora.")

    resultado = evaluar_riesgo_apagon_desde_snapshots(
        subestaciones,
        lineas,
        articulaciones,
        frecuencia_hz=f_hz,
    )

    r = resultado["risk_score"]
    crit = resultado["alerta_critica"]
    umb = resultado["umbral_critico"]

    m1, m2, m3 = st.columns(3)
    m1.metric("risk_score (0–100)", f"{r:.1f}")
    m2.metric("Umbral crítico", f"{umb:.0f}")
    m3.metric(
        "Estado",
        "CRÍTICO" if crit else "En control",
        delta="Alerta SOC" if crit else None,
        delta_color="inverse" if crit else "normal",
    )

    comp = resultado["componentes"]
    st.markdown("**Desglose de componentes (0–1)**")
    st.json(comp)

    if crit:
        st.error(
            "**Alerta crítica:** el risk_score supera el umbral de operación. "
            "Revisar tensión, frecuencia, margen de generación y topología (cascada)."
        )
    else:
        st.success("Riesgo por debajo del umbral crítico de operación.")

    # Variables operativas para anticipar caída y acciones.
    st.markdown("#### Procedimiento operativo (anticipación + respuesta)")
    st.caption(
        "Simula estrés de red para estimar si la caída puede materializarse en la siguiente ventana "
        "operativa y define acciones de reruteo/aislamiento por nodos y subnodos."
    )

    # Presets antes de instanciar widgets para evitar modificar session_state
    # de keys ya creadas en el mismo ciclo de render.
    c_p1, c_p2 = st.columns(2)
    with c_p1:
        st.button(
            "Preset: Colapso inminente",
            use_container_width=True,
            key="riesgo_preset_colapso",
            on_click=_aplicar_preset_colapso,
        )
    with c_p2:
        st.button(
            "Preset: Preventivo",
            use_container_width=True,
            key="riesgo_preset_preventivo",
            on_click=_aplicar_preset_preventivo,
        )

    c1, c2, c3 = st.columns(3)
    with c1:
        horizonte_min = st.selectbox(
            "Horizonte de evaluación",
            options=[15, 30, 45, 60],
            index=1,
            key="riesgo_horizonte_min",
        )
    with c2:
        incremento_carga_pct = st.slider(
            "Incremento de carga esperado (%)",
            min_value=0,
            max_value=40,
            value=10,
            step=1,
            key="riesgo_inc_carga_pct",
        )
    with c3:
        perdida_capacidad_pct = st.slider(
            "Pérdida de capacidad (%)",
            min_value=0,
            max_value=30,
            value=5,
            step=1,
            key="riesgo_perdida_cap_pct",
        )

    max_lineas = max(0, len(lineas))
    cascada_virtual_pct = 0.0
    if max_lineas <= 0:
        st.caption("Sin líneas reales en el snapshot; se usa cascada virtual para simular propagación.")
        lineas_extra = 0
        cascada_virtual_pct = st.slider(
            "Cascada virtual (%)",
            min_value=0,
            max_value=100,
            value=30,
            step=5,
            key="riesgo_cascada_virtual_pct",
        )
    else:
        lineas_extra = st.slider(
            "Líneas adicionales en estado anómalo (simulación de cascada)",
            min_value=0,
            max_value=max_lineas,
            value=min(2, max_lineas),
            step=1,
            key="riesgo_lineas_extra",
        )

    escenario = _simular_escenario(
        subestaciones=subestaciones,
        lineas=lineas,
        incremento_carga_pct=float(incremento_carga_pct),
        perdida_capacidad_pct=float(perdida_capacidad_pct),
        lineas_extra_anomalas=int(lineas_extra),
        cascada_virtual_pct=float(cascada_virtual_pct),
    )
    sub_sim = escenario["subestaciones"]
    lin_sim = escenario["lineas"]
    resultado_fut = evaluar_riesgo_apagon_desde_snapshots(
        sub_sim,
        lin_sim,
        articulaciones,
        frecuencia_hz=f_hz,
    )
    r_fut = float(resultado_fut["risk_score"])

    m4, m5, m6 = st.columns(3)
    m4.metric(f"risk_score estimado @{horizonte_min} min", f"{r_fut:.1f}", delta=f"{(r_fut - r):+.1f}")
    m5.metric(
        "Probabilidad operativa de caída",
        "ALTA" if r_fut >= umb else ("MEDIA" if r_fut >= max(55.0, umb - 15.0) else "BAJA"),
    )
    m6.metric(
        "Ventana de activación",
        "Inmediata" if r_fut >= umb and horizonte_min <= 30 else (f"{horizonte_min} min" if r_fut >= umb else "No inminente"),
    )

    # Tendencia temporal por horizontes para anticipar escalada.
    st.markdown("**Tendencia temporal de riesgo (simulación)**")
    horizons = [15, 30, 45, 60]
    serie_rows = []
    for hz in horizons:
        # Escala progresiva del estrés según horizonte para visualizar deriva.
        factor = hz / 30.0
        esc = _simular_escenario(
            subestaciones=subestaciones,
            lineas=lineas,
            incremento_carga_pct=float(incremento_carga_pct) * factor,
            perdida_capacidad_pct=float(perdida_capacidad_pct) * min(1.5, factor),
            lineas_extra_anomalas=int(round(lineas_extra * min(1.5, factor))),
            cascada_virtual_pct=float(cascada_virtual_pct) * min(1.5, factor),
        )
        out_h = evaluar_riesgo_apagon_desde_snapshots(
            esc["subestaciones"],
            esc["lineas"],
            articulaciones,
            frecuencia_hz=f_hz,
        )
        serie_rows.append({"horizonte_min": hz, "risk_score": float(out_h["risk_score"])})

    serie_plot = []
    for row in serie_rows:
        serie_plot.append(
            {
                "horizonte_min": row["horizonte_min"],
                "risk_score": row["risk_score"],
                "umbral_critico": float(umb),
            }
        )
    st.line_chart(serie_plot, x="horizonte_min", y=["risk_score", "umbral_critico"], use_container_width=True)
    st.caption(
        "Lectura rápida: si la curva cruza el umbral crítico en horizontes cortos, "
        "activa plan de contingencia sin esperar al siguiente ciclo."
    )

    # Nodos prioritarios por criticidad (uso + estado + articulación).
    art_ids = {str(a.get("id", "")) for a in (articulaciones or [])}
    ranking = []
    cambios_mapa = []
    for sid, s_new in sub_sim.items():
        s_old = (subestaciones or {}).get(sid, {})
        uso = _to_float(s_new.get("uso_pct"), 0.0)
        estado_new = (s_new.get("estado") or "ok").lower()
        estado_old = (s_old.get("estado") or "ok").lower()
        score = uso + (25.0 if sid in art_ids else 0.0) + (20.0 if estado_new == "sobrecarga" else (10.0 if estado_new == "alerta" else 0.0))
        ranking.append((score, sid, uso, estado_new))
        if estado_old != estado_new:
            cambios_mapa.append(
                {"subestacion": sid, "estado_antes": estado_old, "estado_estimado": estado_new, "uso_pct_estimado": round(uso, 1)}
            )
    ranking.sort(reverse=True)
    criticos = [x[1] for x in ranking[:8]]

    st.markdown("**Plan recomendado (qué hacer y cómo redirigir flujo)**")
    for idx, rec in enumerate(_recomendaciones_operativas(r_fut, criticos), start=1):
        st.markdown(f"{idx}. {rec}")

    st.markdown("**Nodos/subnodos prioritarios para intervención**")
    st.dataframe(
        [
            {
                "nodo": sid,
                "criticidad": round(sc, 1),
                "uso_pct_estimado": round(uso, 1),
                "estado_estimado": est,
                "articulacion": "sí" if sid in art_ids else "no",
            }
            for sc, sid, uso, est in ranking[:12]
        ],
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("**Cómo quedaría en el mapa (escenario)**")
    n_ok = sum(1 for _, _, _, est in ranking if est == "ok")
    n_alert = sum(1 for _, _, _, est in ranking if est == "alerta")
    n_sob = sum(1 for _, _, _, est in ranking if est == "sobrecarga")
    cm1, cm2, cm3 = st.columns(3)
    cm1.metric("Verde (OK)", n_ok)
    cm2.metric("Naranja (Alerta)", n_alert)
    cm3.metric("Rojo (Sobrecarga)", n_sob)
    if cambios_mapa:
        st.caption("Cambios estimados de estado que verías en el mapa:")
        st.dataframe(cambios_mapa[:20], use_container_width=True, hide_index=True)
    else:
        st.caption("Sin cambios de estado estimados respecto al snapshot actual.")

    aplicar_mapa = st.checkbox(
        "Aplicar este escenario al mapa (solo visualización, no escribe en Cassandra)",
        value=False,
        key="riesgo_aplicar_escenario_mapa",
    )
    if aplicar_mapa:
        st.session_state["riesgo_map_override"] = {"subestaciones": sub_sim, "lineas": lin_sim, "horizonte_min": horizonte_min}
    else:
        st.session_state.pop("riesgo_map_override", None)

    with st.expander("Parámetros (pesos y umbrales)", expanded=False):
        st.json(resultado.get("pesos", {}))
        st.caption(
            "Variables de entorno: `RIESGO_APAGON_UMBRAL_CRITICO`, `RIESGO_APAGON_PESO_*`, "
            "`RIESGO_APAGON_FREQ_NOMINAL`, `RIESGO_APAGON_FREQ_DESV_MAX`."
        )
