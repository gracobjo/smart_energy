# -*- coding: utf-8 -*-
"""
Guía para usuarios no técnicos: interpretar salidas de Spark/YARN y tomar decisiones.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

import streamlit as st


def interpretar_salida_spark(log_text: str, return_code: int) -> Dict[str, Any]:
    """
    Devuelve un dict orientado a decisión: nivel, mensaje corto, qué hacer.
    Prioriza el código de salida; los WARN habituales no marcan fallo.
    """
    text = (log_text or "").lower()

    tiempo_ok = bool(re.search(r"time taken:\s*[\d.]+", text, re.IGNORECASE))
    app_yarn_ok = "application_" in text and return_code == 0

    # Fallo claro: código distinto de cero
    if return_code != 0:
        return {
            "nivel": "rojo",
            "titulo": "El proceso no ha terminado bien",
            "resumen": (
                "El sistema ha devuelto un **código de error**. El análisis **no** se ha completado "
                "de forma fiable para tomar decisiones con los datos."
            ),
            "decision": (
                "**Siguiente paso:** comprobar que **Fase 0** (servicios) está en orden, "
                "revisar la salida técnica abajo o pedir ayuda a informática. **No** use estos resultados para informes hasta resolverlo."
            ),
            "tiempo_detectado": tiempo_ok,
            "app_yarn": app_yarn_ok,
        }

    # Posible anomalía con código 0 (raro): aplicación YARN fallida explícita
    if re.search(r"final status:\s*(failed|killed)", text, re.IGNORECASE):
        return {
            "nivel": "ambar",
            "titulo": "El log indica fallo en el cluster aunque el script terminó",
            "resumen": "A veces el script acaba pero el trabajo en el cluster no. Conviene **verificar datos** antes de decidir.",
            "decision": "**Siguiente paso:** pulse **Comprobar persistencia en Cassandra**. Si los conteos son cero o dudosos, contacte con soporte técnico.",
            "tiempo_detectado": tiempo_ok,
            "app_yarn": app_yarn_ok,
        }

    # Éxito habitual (incluye muchos WARN)
    return {
        "nivel": "verde",
        "titulo": "Proceso terminado: puede confiar para el siguiente paso",
        "resumen": (
            "El trabajo de cálculo ha **finalizado sin error**. Los textos **WARN** (avisos amarillos) "
            "son **habituales**: red, Hive, YARN subiendo librerías… **No** significan que el análisis haya fallado."
        ),
        "decision": (
            "**Siguiente paso:** pulse **Recargar datos** en el mapa (o pestaña Validación) y, si lo desea, "
            "**Comprobar persistencia en Cassandra** para confirmar que hay filas nuevas antes de informar a dirección."
        ),
        "tiempo_detectado": tiempo_ok,
        "app_yarn": app_yarn_ok,
    }


def render_panel_guia_spark() -> None:
    """Panel fijo: qué mirar antes/después sin leer logs técnicos."""
    with st.expander("📋 Cómo interpretar Spark sin ser informático (decisiones)", expanded=False):
        st.markdown(
            """
            ### Las 3 preguntas que importan

            | Pregunta | ¿Qué buscar? |
            |----------|----------------|
            | **1. ¿Ha acabado?** | Tras pulsar **Arrancar procesamiento**, si aparece **«Spark finalizó correctamente»**, el trabajo **sí ha terminado**. |
            | **2. ¿Los avisos (WARN) son graves?** | **Casi nunca** impiden decidir: el sistema suele elegir una IP de red, subir librerías a YARN, etc. |
            | **3. ¿Qué hago yo?** | Si acabó bien → **Recargar datos** en el mapa y **Comprobar Cassandra**. Si falló → revisar Fase 0 (servicios) y el log con soporte. |

            ---

            **Semáforo rápido**

            - 🟢 **Verde:** mensaje de éxito en pantalla + **Comprobar persistencia** con datos en tablas.
            - 🟡 **Ámbar:** éxito pero dudas → compruebe conteos en Cassandra antes de informar a dirección.
            - 🔴 **Rojo:** mensaje de error → no use los resultados para decisiones hasta que lo revise el equipo técnico.

            ---
            *Los mensajes técnicos largos están en «Salida Spark» solo para diagnóstico.*
            """
        )


def render_resumen_tras_spark(return_code: int, log_text: str) -> None:
    """Resumen automático tras ejecutar Spark: decisión en lenguaje de negocio."""
    info = interpretar_salida_spark(log_text, return_code)
    nivel = info["nivel"]

    if nivel == "verde":
        st.success(f"**{info['titulo']}**")
    elif nivel == "ambar":
        st.warning(f"**{info['titulo']}**")
    else:
        st.error(f"**{info['titulo']}**")

    st.markdown(info["resumen"])
    st.info(info["decision"])

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Código de salida", "0 (OK)" if return_code == 0 else str(return_code))
    with c2:
        st.metric("Tiempo «Time taken» en log", "Sí" if info["tiempo_detectado"] else "No / no claro")
