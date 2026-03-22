"""
Transformaciones Spark para pipeline industrial Smart Grid.

Buenas prácticas:
- Watermark en agregaciones con ventana (latitud de llegada tardía).
- Join con broadcast del maestro pequeño (dimensiones).
- Particionado lógico por id_subestacion en agregaciones por ventana.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg as spark_avg,
    col,
    lit,
    when,
    window,
)
from pyspark.sql.types import Row

# Umbral de temperatura para alerta climática / térmica (configurable en planta)
UMBRAL_TEMPERATURA_ALERTA_C = 80.0


def limpiar_lecturas(df: DataFrame) -> DataFrame:
    """
    Limpieza: elimina lecturas inválidas (nulos, capacidad <= 0, potencia negativa).

    En producción se auditan los descartes a HDFS/Dead Letter Queue.
    """
    return (
        df.filter(col("id_subestacion").isNotNull())
        .filter(col("potencia_mw").isNotNull())
        .filter(col("capacidad_mw").isNotNull())
        .filter(col("capacidad_mw") > lit(0.0))
        .filter(col("potencia_mw") >= lit(0.0))
    )


def enriquecer_con_maestro(lecturas: DataFrame, maestro: DataFrame) -> DataFrame:
    """
    Enriquecimiento: JOIN inner con dimensión maestro de subestaciones.

    Usar broadcast(maestro) si el catálogo es pequeño (típico en redes eléctricas).
    """
    from pyspark.sql.functions import broadcast

    return lecturas.join(broadcast(maestro), on="id_subestacion", how="inner")


def detectar_anomalias(df: DataFrame) -> DataFrame:
    """
    Detección de anomalías operativas:
    - potencia_mw > capacidad_mw → sobrecarga
    - temperatura > 80 °C → alerta (correlación térmica / riesgo equipos)

    Prioridad: sobrecarga eléctrica sobre alerta térmica si ambas aplican.
    """
    temp = col("temperatura")
    return df.withColumn(
        "tipo_alerta",
        when(col("potencia_mw") > col("capacidad_mw"), lit("sobrecarga"))
        .when(temp.isNotNull() & (temp > lit(UMBRAL_TEMPERATURA_ALERTA_C)), lit("alerta"))
        .otherwise(lit("ok")),
    )


def agregar_ventanas_15min(
    df: DataFrame,
    watermark_delay: str = "10 minutes",
    window_duration: str = "15 minutes",
    slide_duration: Optional[str] = None,
) -> DataFrame:
    """
    Agregación por ventanas deslizantes/tumbling con watermark.

    - watermark: tolera eventos tardíos hasta 10 min (ajustable por SLA).
    - ventana 15 min: alineado con ciclo KDD del proyecto.

    Devuelve carga media por subestación y ventana.
    """
    slide = slide_duration or window_duration
    w = window(col("event_time"), window_duration, slide)
    return (
        df.withWatermark("event_time", watermark_delay)
        .groupBy(w, col("id_subestacion"))
        .agg(
            spark_avg("potencia_mw").alias("carga_media_mw"),
            spark_avg("capacidad_mw").alias("capacidad_media_mw"),
        )
    )


def preparar_batch_upsert_cassandra(
    df_alertas: DataFrame,
    keyspace: str = "smart_grid",
    tabla: str = "alertas_streaming_batch",
) -> List[Dict[str, Any]]:
    """
    Prepara filas para upsert en Cassandra (INSERT ... IF NOT EXISTS / UPDATE).

    En tests se valida la estructura sin cluster real. En runtime el driver
    ejecuta batch preparado con consistencia LOCAL_QUORUM.

    Retorna lista de dicts con cql y valores para assert en pytest.
    """
    rows = df_alertas.collect()
    out: List[Dict[str, Any]] = []
    for r in rows:
        if isinstance(r, Row):
            d = r.asDict()
        else:
            d = dict(r)
        cql = (
            f"INSERT INTO {keyspace}.{tabla} "
            "(id_subestacion, tipo_alerta, potencia_mw, capacidad_mw, event_ts) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        out.append(
            {
                "cql": cql,
                "params": (
                    d.get("id_subestacion"),
                    d.get("tipo_alerta"),
                    d.get("potencia_mw"),
                    d.get("capacidad_mw"),
                    d.get("event_time"),
                ),
            }
        )
    return out
