"""
API REST Smart Grid — Swagger/OpenAPI para integración con otros sistemas.
Expone subestaciones, líneas, PageRank y puntos de fallo desde Cassandra.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from api.datos import (
    cargar_lineas,
    cargar_pagerank,
    cargar_puntos_fallo,
    cargar_subestaciones,
)

# Riesgo de apagón (tras añadir BASE al path)
from procesamiento.deteccion_apagon import evaluar_riesgo_apagon_desde_snapshots

# Modelos Pydantic para documentación Swagger
class Subestacion(BaseModel):
    """Estado de una subestación."""
    model_config = ConfigDict(extra="allow")
    lat: float = Field(..., description="Latitud")
    lon: float = Field(..., description="Longitud")
    voltaje_kv: Optional[float] = Field(None, description="Voltaje en kV")
    potencia_mw: Optional[float] = Field(None, description="Potencia en MW")
    capacidad_mw: Optional[float] = Field(None, description="Capacidad en MW")
    uso_pct: Optional[float] = Field(None, description="Uso en %")
    estado: str = Field(..., description="Estado: ok, alerta, sobrecarga")
    motivo: str = Field("", description="Motivo del estado")
    clima: str = Field("", description="Clima actual")
    temperatura: Optional[float] = Field(None, description="Temperatura °C")
    humedad: Optional[float] = Field(None, description="Humedad %")
    ultima_actualizacion: Optional[str] = Field(None, description="Timestamp última actualización")


class Linea(BaseModel):
    """Estado de una línea de transmisión."""
    src: str = Field(..., description="Subestación origen")
    dst: str = Field(..., description="Subestación destino")
    estado: str = Field(..., description="Estado: ok, alerta, sobrecarga")
    flujo_mw: Optional[float] = Field(None, description="Flujo en MW")
    capacidad_mw: Optional[float] = Field(None, description="Capacidad en MW")


class PuntoFallo(BaseModel):
    """Punto de fallo único (articulación): nodo cuya caída fragmenta la red."""
    id_subestacion: str = Field(..., description="ID de la subestación")
    es_articulacion: bool = Field(..., description="Si es punto de articulación")
    fragmentos_al_fallar: Optional[int] = Field(None, description="Nº de fragmentos si falla")
    detalle: str = Field("", description="Detalle de nodos afectados (JSON)")


class SubestacionesResponse(BaseModel):
    """Respuesta: mapa de subestaciones."""
    subestaciones: Dict[str, Subestacion] = Field(..., description="id -> datos")


class LineasResponse(BaseModel):
    """Respuesta: mapa de líneas (clave src|dst)."""
    lineas: Dict[str, Linea] = Field(..., description="src|dst -> datos")


class PageRankResponse(BaseModel):
    """Respuesta: PageRank por subestación."""
    pagerank: Dict[str, float] = Field(..., description="id_subestacion -> valor PageRank")


class PuntosFalloResponse(BaseModel):
    """Respuesta: lista de puntos de fallo únicos."""
    puntos_fallo: List[PuntoFallo] = Field(..., description="Articulaciones detectadas")


class HealthResponse(BaseModel):
    """Estado de salud del servicio."""
    status: str = Field(..., description="ok o error")
    cassandra: bool = Field(..., description="Si Cassandra está accesible")
    version: str = Field("1.0", description="Versión de la API")


class RiesgoApagonResponse(BaseModel):
    """Evaluación de riesgo de apagón (0–100) y alerta crítica."""
    risk_score: float = Field(..., description="Puntuación agregada 0–100")
    alerta_critica: bool = Field(..., description="True si supera el umbral operativo")
    umbral_critico: float = Field(..., description="Umbral configurado")
    componentes: Dict[str, float] = Field(..., description="Factores sobretensión, frecuencia, generación, cascada")
    pesos: Dict[str, float] = Field(default_factory=dict)
    frecuencia_hz_medida: Optional[float] = Field(None, description="Frecuencia usada en el cálculo")


app = FastAPI(
    title="Smart Grid API",
    description="""
API REST para integrar el sistema Smart Grid con otros sistemas.

**Datos expuestos:**
- **Subestaciones**: estado (voltaje, potencia, uso), ubicación, clima
- **Líneas**: flujo MW, capacidad, estado por enlace
- **PageRank**: nodos críticos según centralidad
- **Puntos de fallo**: articulaciones cuya caída fragmenta la red
- **Riesgo de apagón**: `risk_score` y alertas (voltaje, frecuencia, generación, cascada)
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
def health():
    """
    Health check. Indica si la API y Cassandra están operativos.
    """
    try:
        _ = cargar_subestaciones()
        cass_ok = True
    except Exception:
        cass_ok = False
    return HealthResponse(
        status="ok",
        cassandra=cass_ok,
        version="1.0.0",
    )


@app.get(
    "/api/v1/subestaciones",
    response_model=SubestacionesResponse,
    summary="Estado de subestaciones",
    description="Retorna el estado actual de todas las subestaciones (voltaje, potencia, uso, ubicación).",
)
def get_subestaciones():
    """Estado de subestaciones desde Cassandra."""
    data = cargar_subestaciones()
    return SubestacionesResponse(subestaciones=data)


@app.get(
    "/api/v1/lineas",
    response_model=LineasResponse,
    summary="Estado de líneas",
    description="Retorna el estado de las líneas de transmisión (flujo MW, capacidad, estado).",
)
def get_lineas():
    """Estado de líneas desde Cassandra."""
    data = cargar_lineas()
    return LineasResponse(lineas=data)


@app.get(
    "/api/v1/pagerank",
    response_model=PageRankResponse,
    summary="PageRank por subestación",
    description="Retorna el PageRank (centralidad) de cada subestación. Valores altos = nodos más críticos.",
)
def get_pagerank():
    """PageRank desde Cassandra."""
    data = cargar_pagerank()
    return PageRankResponse(pagerank=data)


@app.get(
    "/api/v1/puntos-fallo",
    response_model=PuntosFalloResponse,
    summary="Puntos de fallo únicos",
    description="Retorna las articulaciones: nodos cuya caída fragmentaría la red.",
)
def get_puntos_fallo():
    """Puntos de fallo desde Cassandra."""
    data = cargar_puntos_fallo()
    return PuntosFalloResponse(puntos_fallo=data)


@app.get(
    "/api/v1/red",
    summary="Vista consolidada de la red",
    description="Subestaciones + líneas + PageRank + puntos de fallo en una sola llamada.",
)
def get_red_completa():
    """Vista consolidada para integradores."""
    return {
        "subestaciones": cargar_subestaciones(),
        "lineas": cargar_lineas(),
        "pagerank": cargar_pagerank(),
        "puntos_fallo": cargar_puntos_fallo(),
    }


@app.get(
    "/api/v1/riesgo-apagon",
    response_model=RiesgoApagonResponse,
    summary="Riesgo de apagón eléctrico",
    description=(
        "Calcula risk_score (0–100) a partir de telemetría en Cassandra: voltaje, "
        "carga vs capacidad, líneas anómalas, articulaciones. Frecuencia opcional (Hz). "
        "Ver docs/APAGON_ESPANA_2025_CASO.md."
    ),
)
def get_riesgo_apagon(
    frecuencia_hz: Optional[float] = Query(
        None,
        description="Frecuencia de red en Hz (p. ej. 50.0). Si se omite, el componente frecuencia es 0.",
    ),
):
    """Riesgo agregado de apagón y alerta crítica."""
    sub = cargar_subestaciones()
    lin = cargar_lineas()
    pf = cargar_puntos_fallo()
    raw = evaluar_riesgo_apagon_desde_snapshots(sub, lin, pf, frecuencia_hz=frecuencia_hz)
    return RiesgoApagonResponse(
        risk_score=raw["risk_score"],
        alerta_critica=raw["alerta_critica"],
        umbral_critico=raw["umbral_critico"],
        componentes=raw["componentes"],
        pesos=raw.get("pesos", {}),
        frecuencia_hz_medida=raw.get("frecuencia_hz_medida"),
    )


def main():
    import uvicorn
    from config import API_SMART_GRID_PORT
    uvicorn.run("api.main:app", host="0.0.0.0", port=API_SMART_GRID_PORT, reload=False)


if __name__ == "__main__":
    main()
