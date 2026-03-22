"""Tests: preparación de batch upsert hacia Cassandra (sin cluster real)."""
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

from procesamiento.smart_grid_streaming.transformations import (
    detectar_anomalias,
    preparar_batch_upsert_cassandra,
)


def test_preparar_batch_upsert_genera_cql_y_params(spark_session):
    """Cada fila produce un dict con CQL parametrizado y tupla de valores."""
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
        [Row("SE_UPSERT", 300.0, 200.0, 30.0, ts)],
        sch,
    )
    con_alerta = detectar_anomalias(df)
    batch = preparar_batch_upsert_cassandra(con_alerta, keyspace="smart_grid", tabla="alertas_streaming_batch")
    assert len(batch) == 1
    b0 = batch[0]
    assert "INSERT INTO smart_grid.alertas_streaming_batch" in b0["cql"]
    assert b0["params"][0] == "SE_UPSERT"
    assert b0["params"][1] == "sobrecarga"


def test_batch_vacio_si_dataframe_vacio(spark_session):
    sch = StructType(
        [
            StructField("id_subestacion", StringType(), True),
            StructField("potencia_mw", DoubleType(), True),
            StructField("capacidad_mw", DoubleType(), True),
            StructField("temperatura", DoubleType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )
    empty = spark_session.createDataFrame([], sch)
    assert preparar_batch_upsert_cassandra(empty) == []
