# -*- coding: utf-8 -*-
"""
Bloque Streamlit: guía para tomar decisiones ante logs de Spark/Hadoop (perfil no técnico).
"""
from __future__ import annotations

from pathlib import Path

import streamlit as st

BASE = Path(__file__).resolve().parent
GUIA_MD = BASE / "docs" / "GUIA_DECISIONES_SPARK_YARN.md"


def render_guia_decisiones_spark() -> None:
    """Presenta criterios claros de decisión sin jerga informática."""
    with st.expander(
        "📋 ¿Muchos avisos (WARN) en Spark o Hadoop? — Guía para **decidir** (sin ser informático)",
        expanded=False,
    ):
        st.markdown(
            """
#### Tu decisión en 3 pasos

| Paso | Pregunta | Qué hacer |
|:----:|----------|-----------|
| **1** | ¿Aparece **ERROR** o **Exception** y el proceso **se detiene**? | **No uses** el resultado como fiable. Anota hora y **pide ayuda** a quien administra el sistema. |
| **2** | ¿Solo ves **WARN** y al final un **tiempo** (`Time taken`) o “finished”? | **Puedes seguir**: suele ser normal. Lo importante es que el **informe o los datos finales** sean correctos. |
| **3** | ¿Hay **Spark Web UI** o un **Application Id**? | **Buena señal** para auditoría. Guarda el ID si alguien debe revisar después. |

---

**Regla sencilla:** los WARN **no sustituyen** a comprobar el resultado final (números, tablas, mapa).  
Los ERROR **sí** impiden dar por bueno el proceso hasta que lo revise informática.
            """
        )
        c1, c2, c3 = st.columns(3)
        with c1:
            st.success("**Todo OK para decidir** si termina bien y los datos cuadran.")
        with c2:
            st.warning("**Revisar** si el tiempo es muy largo y afecta a plazos (tema de rendimiento).")
        with c3:
            st.error("**Parar y escalar** si hay ERROR o datos claramente erróneos.")

        if GUIA_MD.exists():
            with st.expander("📄 Texto completo de la guía (para compartir o imprimir)", expanded=False):
                st.markdown(GUIA_MD.read_text(encoding="utf-8"))
        else:
            st.caption(f"No se encontró {GUIA_MD}")

        st.caption(
            "Documento en el repositorio: `docs/GUIA_DECISIONES_SPARK_YARN.md` — pensado para dirección, "
            "operación y analistas."
        )
