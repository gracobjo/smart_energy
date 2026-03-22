"""Tests: enriquecimiento — JOIN correcto con maestro."""
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

from procesamiento.smart_grid_streaming.transformations import enriquecer_con_maestro


def test_join_inner_solo_subestaciones_con_maestro(spark_session):
    """Solo aparecen lecturas cuyo id existe en el catálogo maestro."""
    sch_lect = StructType(
        [
            StructField("id_subestacion", StringType(), True),
            StructField("potencia_mw", DoubleType(), True),
            StructField("capacidad_mw", DoubleType(), True),
            StructField("temperatura", DoubleType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )
    sch_mae = StructType(
        [
            StructField("id_subestacion", StringType(), True),
            StructField("nombre", StringType(), True),
            StructField("region", StringType(), True),
            StructField("voltaje_nominal_kv", DoubleType(), True),
        ]
    )
    ts = datetime(2026, 1, 15, 10, 0, 0)
    lecturas = spark_session.createDataFrame(
        [
            Row("SE_MADRID", 120.0, 200.0, 30.0, ts),
            Row("SE_FANTASMA", 50.0, 100.0, 20.0, ts),
        ],
        sch_lect,
    )
    maestro = spark_session.createDataFrame(
        [
            Row("SE_MADRID", "Madrid Norte", "Madrid", 400.0),
        ],
        sch_mae,
    )
    out = enriquecer_con_maestro(lecturas, maestro)
    assert out.count() == 1
    r = out.collect()[0]
    assert r["nombre"] == "Madrid Norte"
    assert r["region"] == "Madrid"
