# -*- coding: utf-8 -*-
"""
Panel Streamlit: riesgo de apagón (risk_score) y alertas críticas.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List

import streamlit as st

from procesamiento.deteccion_apagon import evaluar_riesgo_apagon_desde_snapshots


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

    with st.expander("Parámetros (pesos y umbrales)", expanded=False):
        st.json(resultado.get("pesos", {}))
        st.caption(
            "Variables de entorno: `RIESGO_APAGON_UMBRAL_CRITICO`, `RIESGO_APAGON_PESO_*`, "
            "`RIESGO_APAGON_FREQ_NOMINAL`, `RIESGO_APAGON_FREQ_DESV_MAX`."
        )
