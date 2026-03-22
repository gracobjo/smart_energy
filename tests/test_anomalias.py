"""Tests: detección de anomalías — sobrecarga y alerta térmica."""
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

from procesamiento.smart_grid_streaming.transformations import detectar_anomalias


def test_sobrecarga_cuando_potencia_supera_capacidad(spark_session):
    sch = StructType(
        [
            StructField("id_subestacion", StringType(), True),
            StructField("potencia_mw", DoubleType(), True),
            StructField("capacidad_mw", DoubleType(), True),
            StructField("temperatura", DoubleType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )
    ts = datetime(2026, 1, 15, 10, 0, 0)
    df = spark_session.createDataFrame(
        [Row("SE_1", 250.0, 200.0, 40.0, ts)],
        sch,
    )
    out = detectar_anomalias(df).collect()[0]
    assert out["tipo_alerta"] == "sobrecarga"


def test_alerta_cuando_temperatura_mayor_80(spark_session):
    sch = StructType(
        [
            StructField("id_subestacion", StringType(), True),
            StructField("potencia_mw", DoubleType(), True),
            StructField("capacidad_mw", DoubleType(), True),
            StructField("temperatura", DoubleType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )
    ts = datetime(2026, 1, 15, 10, 0, 0)
    df = spark_session.createDataFrame(
        [Row("SE_2", 50.0, 200.0, 85.0, ts)],
        sch,
    )
    out = detectar_anomalias(df).collect()[0]
    assert out["tipo_alerta"] == "alerta"


def test_ok_cuando_dentro_de_limites(spark_session):
    sch = StructType(
        [
            StructField("id_subestacion", StringType(), True),
            StructField("potencia_mw", DoubleType(), True),
            StructField("capacidad_mw", DoubleType(), True),
            StructField("temperatura", DoubleType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )
    ts = datetime(2026, 1, 15, 10, 0, 0)
    df = spark_session.createDataFrame(
        [Row("SE_3", 100.0, 200.0, 40.0, ts)],
        sch,
    )
    out = detectar_anomalias(df).collect()[0]
    assert out["tipo_alerta"] == "ok"
