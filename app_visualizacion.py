#!/usr/bin/env python3
"""
Sistema de Monitoreo de Redes de Energía Inteligentes (Smart Grid) - Dashboard
- Mapa España (Folium): subestaciones y líneas de alta tensión coloreados por estado
- Voltaje (kV) y potencia (MW) por subestación; detección de sobrecarga en tiempo real
- PageRank: nodos críticos de la red
- Botón "Paso Siguiente (15 min)": ejecuta producer + procesamiento
"""
import os
import sys
import subprocess
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

import streamlit as st
import folium
from folium import PolyLine, CircleMarker
from streamlit_folium import st_folium
from cassandra.cluster import Cluster

from config_nodos import get_aristas, get_nodos
from config import CASSANDRA_HOST, KEYSPACE

# Colores por estado de red: OK, alerta, sobrecarga
COLORES_ESTADO = {"Ok": "green", "Alerta": "orange", "Sobrecarga": "red"}
COLOR_DEFAULT = "gray"


def get_cassandra_session():
    try:
        cluster = Cluster([CASSANDRA_HOST])
        return cluster.connect(KEYSPACE)
    except Exception as e:
        st.error(f"No se pudo conectar a Cassandra: {e}")
        return None


def cargar_subestaciones_estado(session):
    """Estado actual de subestaciones (último voltaje, potencia) desde Cassandra."""
    if not session:
        return {}
    try:
        rows = session.execute(
            "SELECT id_subestacion, lat, lon, voltaje_kv, potencia_mw, capacidad_mw, uso_pct, estado, motivo, clima_actual FROM subestaciones_estado"
        )
        return {
            r.id_subestacion: {
                "lat": r.lat,
                "lon": r.lon,
                "voltaje_kv": r.voltaje_kv,
                "potencia_mw": r.potencia_mw,
                "capacidad_mw": r.capacidad_mw,
                "uso_pct": r.uso_pct,
                "estado": r.estado or "Ok",
                "motivo": r.motivo,
                "clima": r.clima_actual,
            }
            for r in rows
        }
    except Exception as e:
        st.warning(f"subestaciones_estado no disponible: {e}")
        return {}


def cargar_pagerank(session):
    if not session:
        return {}
    try:
        rows = session.execute("SELECT id_subestacion, pagerank FROM pagerank_subestaciones")
        return {r.id_subestacion: r.pagerank for r in rows}
    except Exception:
        return {}


def cargar_lineas_estado(session):
    if not session:
        return {}
    try:
        rows = session.execute("SELECT src, dst, flujo_mw, capacidad_mw, estado FROM lineas_estado")
        return {f"{r.src}|{r.dst}": {"estado": r.estado or "Ok", "flujo_mw": r.flujo_mw, "capacidad_mw": r.capacidad_mw} for r in rows}
    except Exception:
        return {}


def crear_mapa(subestaciones_estado, lineas_estado, pagerank):
    m = folium.Map(location=[40.4, -3.7], zoom_start=6, tiles="OpenStreetMap")
    nodos = get_nodos()
    aristas = get_aristas()

    # Líneas de alta tensión
    for t in aristas:
        src, dst = t[0], t[1]
        key = f"{src}|{dst}"
        key_inv = f"{dst}|{src}"
        est = (lineas_estado or {}).get(key) or (lineas_estado or {}).get(key_inv) or {}
        estado_lin = est.get("estado", "Ok")
        color = COLORES_ESTADO.get(estado_lin, COLOR_DEFAULT)
        if src in nodos and dst in nodos:
            pts = [[nodos[src]["lat"], nodos[src]["lon"]], [nodos[dst]["lat"], nodos[dst]["lon"]]]
            folium.PolyLine(
                pts,
                color=color,
                weight=3,
                opacity=0.7,
                tooltip=f"{src}-{dst} ({estado_lin})",
            ).add_to(m)

    # Subestaciones
    for nid, datos in nodos.items():
        est = (subestaciones_estado.get(nid) or {})
        estado = est.get("estado", "Ok")
        motivo = est.get("motivo", "")
        voltaje = est.get("voltaje_kv", 220)
        potencia = est.get("potencia_mw", 0)
        uso = est.get("uso_pct", 0)
        pr = pagerank.get(nid, 0)
        color = COLORES_ESTADO.get(estado, COLOR_DEFAULT)
        popup = (
            f"<b>{nid}</b><br>"
            f"Estado: {estado}<br>"
            f"Voltaje: {voltaje} kV | Potencia: {potencia} MW<br>"
            f"Uso: {uso}%<br>"
            f"Motivo: {motivo or '-'}<br>"
            f"PageRank: {round(pr, 4)}"
        )
        CircleMarker(
            location=[datos["lat"], datos["lon"]],
            radius=12 if datos["tipo"] == "principal" else 7,
            color=color,
            fill=True,
            fill_opacity=0.8,
            popup=popup,
        ).add_to(m)

    return m


def ejecutar_paso_siguiente():
    paso = st.session_state.get("paso_15min", 0)
    env = os.environ.copy()
    env["PASO_15MIN"] = str(paso)

    with st.spinner("Ejecutando ingesta (producer)..."):
        r1 = subprocess.run(
            [sys.executable, str(BASE / "producer.py")],
            env=env,
            capture_output=True,
            text=True,
            cwd=str(BASE),
        )
    if r1.returncode != 0:
        st.error(f"Producer: {r1.stderr or r1.stdout}")
        return False

    with st.spinner("Procesando grafos (Spark)..."):
        r2 = subprocess.run(
            [sys.executable, str(BASE / "procesamiento" / "procesamiento_grafos.py")],
            env=env,
            capture_output=True,
            text=True,
            cwd=str(BASE),
            timeout=180,
        )
    if r2.returncode != 0:
        st.error(f"Procesamiento: {r2.stderr or r2.stdout}")
        return False

    st.session_state["paso_15min"] = paso + 1
    st.success("Paso completado. Actualizando vista...")
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()
    return True


def main():
    st.set_page_config(page_title="Smart Grid España", layout="wide")
    st.title("Sistema de Monitoreo de Redes de Energía Inteligentes (Smart Grid)")

    if "paso_15min" not in st.session_state:
        st.session_state["paso_15min"] = 0

    session = get_cassandra_session()

    col1, col2, _ = st.columns([1, 1, 4])
    with col1:
        if st.button("Paso Siguiente (15 min)", use_container_width=True):
            ejecutar_paso_siguiente()
    with col2:
        st.metric("Paso simulación", st.session_state["paso_15min"])

    subestaciones_estado = cargar_subestaciones_estado(session)
    lineas_estado = cargar_lineas_estado(session)
    pagerank = cargar_pagerank(session)

    if not subestaciones_estado:
        nodos = get_nodos()
        subestaciones_estado = {
            n: {
                "lat": d["lat"],
                "lon": d["lon"],
                "voltaje_kv": 220,
                "potencia_mw": 0,
                "estado": "Ok",
                "motivo": None,
                "clima": "",
            }
            for n, d in nodos.items()
        }

    m = crear_mapa(subestaciones_estado, lineas_estado, pagerank)
    st_folium(m, width=None, height=500)

    st.subheader("Leyenda")
    c1, c2, c3 = st.columns(3)
    c1.markdown("- **Verde**: OK (operación normal)")
    c2.markdown("- **Naranja**: Alerta (carga elevada)")
    c3.markdown("- **Rojo**: Sobrecarga (subestación/línea por encima de capacidad)")

    st.subheader("Detección en tiempo real")
    st.markdown(
        "El sistema detecta cuando una subestación supera su capacidad basándose en los datos de la red (voltaje, potencia). "
        "Las líneas en sobrecarga se excluyen del grafo para el cálculo de nodos críticos."
    )

    st.subheader("PageRank - Subestaciones más críticas")
    if pagerank:
        sorted_pr = sorted(pagerank.items(), key=lambda x: -x[1])[:10]
        st.dataframe(
            [{"Subestación": n, "PageRank": round(v, 4)} for n, v in sorted_pr],
            use_container_width=True,
        )
    else:
        st.info("Ejecuta al menos un paso (Producer + Procesamiento) para ver PageRank desde Cassandra.")


if __name__ == "__main__":
    main()
