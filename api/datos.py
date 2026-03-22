"""
Carga de datos desde Cassandra para la API Smart Grid.
Comparte la lógica con app_visualizacion pero sin dependencia de Streamlit.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from config import CASSANDRA_HOST, KEYSPACE

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
except ImportError:
    Cluster = None  # type: ignore


def _normalizar_estado(estado: Optional[str]) -> str:
    if not estado:
        return "ok"
    e = str(estado).strip().lower()
    if e in ("ok", "fluido", "normal"):
        return "ok"
    if e in ("alerta", "congestionado", "congestionada"):
        return "alerta"
    if e in ("sobrecarga", "bloqueado", "bloqueada"):
        return "sobrecarga"
    return "ok"


_session: Optional[Any] = None


def _get_session():
    """Sesión Cassandra lazy, reutilizable."""
    global _session
    if Cluster is None:
        return None
    if _session is not None:
        return _session
    try:
        host = os.environ.get("CASSANDRA_HOST", CASSANDRA_HOST)
        c = Cluster([host.strip() or "127.0.0.1"])
        _session = c.connect(KEYSPACE)
        return _session
    except Exception:
        return None


def cargar_subestaciones() -> Dict[str, Dict[str, Any]]:
    """Estado de subestaciones desde Cassandra (o vacío si no hay conexión)."""
    session = _get_session()
    if not session:
        return {}
    try:
        rows = session.execute(
            """
            SELECT id_subestacion, lat, lon, voltaje_kv, potencia_mw, capacidad_mw,
                   uso_pct, estado, motivo, clima_actual, temperatura, humedad,
                   ultima_actualizacion
            FROM subestaciones_estado
            """
        )
        return {
            r.id_subestacion: {
                "lat": r.lat,
                "lon": r.lon,
                "voltaje_kv": r.voltaje_kv,
                "potencia_mw": r.potencia_mw,
                "capacidad_mw": r.capacidad_mw,
                "uso_pct": r.uso_pct,
                "estado": _normalizar_estado(r.estado),
                "motivo": r.motivo or "",
                "clima": r.clima_actual or "",
                "temperatura": r.temperatura,
                "humedad": r.humedad,
                "ultima_actualizacion": str(r.ultima_actualizacion) if r.ultima_actualizacion else None,
            }
            for r in rows
        }
    except Exception:
        return {}


def cargar_lineas() -> Dict[str, Dict[str, Any]]:
    """Estado de líneas desde Cassandra."""
    session = _get_session()
    if not session:
        return {}
    try:
        rows = session.execute(
            "SELECT src, dst, flujo_mw, capacidad_mw, estado FROM lineas_estado"
        )
        return {
            f"{r.src}|{r.dst}": {
                "src": r.src,
                "dst": r.dst,
                "estado": _normalizar_estado(r.estado),
                "flujo_mw": r.flujo_mw,
                "capacidad_mw": r.capacidad_mw,
            }
            for r in rows
        }
    except Exception:
        return {}


def cargar_pagerank() -> Dict[str, float]:
    """PageRank por subestación."""
    session = _get_session()
    if not session:
        return {}
    try:
        rows = session.execute(
            "SELECT id_subestacion, pagerank FROM pagerank_subestaciones"
        )
        return {r.id_subestacion: float(r.pagerank or 0) for r in rows}
    except Exception:
        return {}


def cargar_puntos_fallo() -> List[Dict[str, Any]]:
    """Puntos de fallo únicos (articulaciones)."""
    session = _get_session()
    if not session:
        return []
    try:
        rows = session.execute(
            """
            SELECT id_subestacion, es_articulacion, fragmentos_al_fallar, nodos_afectados_json
            FROM puntos_fallo_unicos
            WHERE es_articulacion = true ALLOW FILTERING
            """
        )
        return [
            {
                "id_subestacion": r.id_subestacion,
                "es_articulacion": bool(r.es_articulacion),
                "fragmentos_al_fallar": r.fragmentos_al_fallar,
                "detalle": (r.nodos_afectados_json or "")[:500],
            }
            for r in rows
        ]
    except Exception:
        return []
