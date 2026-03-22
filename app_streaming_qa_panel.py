# -*- coding: utf-8 -*-
"""
Panel de presentación: pipeline PySpark streaming, tests pytest y documentación.
"""
from __future__ import annotations

from pathlib import Path

import streamlit as st

BASE = Path(__file__).resolve().parent


def render_streaming_qa_panel() -> None:
    """Renderiza la pestaña Streaming & QA para defensa / presentación del proyecto."""
    st.markdown("### Streaming Smart Grid — PySpark & calidad")
    st.caption(
        "Módulo `procesamiento/smart_grid_streaming/`: limpieza, JOIN con maestro, "
        "detección de anomalías (sobrecarga / temperatura), ventanas 15 min con watermark, "
        "preparación de upsert Cassandra. Tests con **pytest + PySpark** (`local[*]`)."
    )

    doc_path = BASE / "docs" / "STREAMING_PYSPARK_QA.md"
    if doc_path.exists():
        with st.expander("📄 Documentación técnica completa (requisitos, diseño, casos de uso)", expanded=True):
            st.markdown(doc_path.read_text(encoding="utf-8"))
    else:
        st.warning(f"No se encuentra {doc_path}")

    st.markdown("---")
    st.markdown("#### Ejecutar tests en terminal")
    st.code(
        "cd ~/smart_energy\n"
        "source venv/bin/activate\n"
        "pytest tests/ -v\n",
        language="bash",
    )
    st.info(
        "Los tests arrancan una sesión Spark local (≈1–2 min la primera vez). "
        "Incluyen: limpieza, enriquecimiento, anomalías, ventanas, upsert Cassandra, E2E simulado."
    )
    st.info(
        "**Riesgo de apagón (España 2025):** documentación en `docs/APAGON_ESPANA_2025_CASO.md` — "
        "panel bajo los KPIs del mapa; API `GET /api/v1/riesgo-apagon`."
    )

    st.markdown("#### Arquitectura (resumen)")
    st.code(
        "Kafka (energy_*) → Spark Streaming → Hive (histórico) + Cassandra (estado RT)",
        language="text",
    )
