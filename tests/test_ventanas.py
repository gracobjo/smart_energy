"""Tests: ventanas — media correcta con watermark."""
from datetime import datetime, timedelta

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

from procesamiento.smart_grid_streaming.transformations import agregar_ventanas_15min


def test_ventana_15min_media_potencia_correcta(spark_session):
    """
    Dos eventos en la misma ventana [10:00, 10:15) para SE_X → media = (80+100)/2 = 90.
    """
    sch = StructType(
        [
            StructField("id_subestacion", StringType(), True),
            StructField("potencia_mw", DoubleType(), True),
            StructField("capacidad_mw", DoubleType(), True),
            StructField("temperatura", DoubleType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )
    t0 = datetime(2026, 1, 15, 10, 5, 0)
    t1 = datetime(2026, 1, 15, 10, 10, 0)
    df = spark_session.createDataFrame(
        [
            Row("SE_X", 80.0, 200.0, 20.0, t0),
            Row("SE_X", 100.0, 200.0, 20.0, t1),
        ],
        sch,
    )
    agg = agregar_ventanas_15min(df)
    # Streaming aggregation returns a streaming DF — en batch sobre DF estático
    # Spark permite groupBy+window en batch si hay event_time
    collected = agg.collect()
    assert len(collected) >= 1
    # Buscar fila SE_X
    medias = [r["carga_media_mw"] for r in collected if r["id_subestacion"] == "SE_X"]
    assert medias
    assert abs(medias[0] - 90.0) < 1e-6


def test_ventanas_separadas_por_tiempo(spark_session):
    """Eventos en ventanas 15 min distintas generan al menos 2 grupos."""
    sch = StructType(
        [
            StructField("id_subestacion", StringType(), True),
            StructField("potencia_mw", DoubleType(), True),
            StructField("capacidad_mw", DoubleType(), True),
            StructField("temperatura", DoubleType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )
    t_a = datetime(2026, 1, 15, 10, 0, 0)
    t_b = t_a + timedelta(minutes=20)
    df = spark_session.createDataFrame(
        [
            Row("SE_Y", 60.0, 200.0, 20.0, t_a),
            Row("SE_Y", 80.0, 200.0, 20.0, t_b),
        ],
        sch,
    )
    agg = agregar_ventanas_15min(df).collect()
    ids_windows = {(r["id_subestacion"], r["window"].start) for r in agg}
    assert len(ids_windows) >= 2
