"""
Test end-to-end con datos simulados: limpieza → enriquecimiento → anomalías → batch Cassandra.
Simula un lote de telemetría industrial sin Kafka ni cluster Cassandra.
"""
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
    limpiar_lecturas,
    enriquecer_con_maestro,
    detectar_anomalias,
    preparar_batch_upsert_cassandra,
)


def test_e2e_simulacion_reducida(spark_session):
    """
    Flujo: lecturas crudas → limpieza → join maestro → reglas de anomalía → payload upsert.
    """
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
    ts = datetime(2026, 1, 15, 12, 0, 0)
    raw = spark_session.createDataFrame(
        [
            Row("SE_NORTE", 90.0, 100.0, 35.0, ts),
            Row("SE_NORTE", 150.0, 100.0, 35.0, ts),  # sobrecarga
            Row(None, 10.0, 50.0, 20.0, ts),  # descartada en limpieza
        ],
        sch_lect,
    )
    maestro = spark_session.createDataFrame(
        [
            Row("SE_NORTE", "Subestación Norte", "Galicia", 220.0),
        ],
        sch_mae,
    )

    limpio = limpiar_lecturas(raw)
    assert limpio.count() == 2

    enr = enriquecer_con_maestro(limpio, maestro)
    assert enr.count() == 2

    con_tipo = detectar_anomalias(enr)
    tipos = {r["tipo_alerta"] for r in con_tipo.collect()}
    assert "ok" in tipos
    assert "sobrecarga" in tipos

    upserts = preparar_batch_upsert_cassandra(con_tipo)
    assert len(upserts) == 2
    sobrecargas = [u for u in upserts if u["params"][1] == "sobrecarga"]
    assert len(sobrecargas) == 1
