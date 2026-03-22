"""Tests: limpieza — valores inválidos eliminados."""
from pyspark.sql import Row

from procesamiento.smart_grid_streaming.transformations import limpiar_lecturas


def test_limpieza_elimina_nulos_y_capacidad_cero(spark_session):
    """Descarta filas sin id, potencia o capacidad; capacidad <= 0; potencia negativa."""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    from datetime import datetime

    schema = StructType(
        [
            StructField("id_subestacion", StringType(), True),
            StructField("potencia_mw", DoubleType(), True),
            StructField("capacidad_mw", DoubleType(), True),
            StructField("temperatura", DoubleType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )
    ts = datetime(2026, 1, 15, 10, 0, 0)
    raw = spark_session.createDataFrame(
        [
            Row("SE_A", 100.0, 200.0, 25.0, ts),  # válida
            Row(None, 50.0, 200.0, 25.0, ts),  # sin id
            Row("SE_B", None, 200.0, 25.0, ts),  # sin potencia
            Row("SE_C", 80.0, None, 25.0, ts),  # sin capacidad
            Row("SE_D", 10.0, 0.0, 25.0, ts),  # capacidad inválida
            Row("SE_E", -1.0, 100.0, 25.0, ts),  # potencia negativa
        ],
        schema,
    )
    limpio = limpiar_lecturas(raw)
    assert limpio.count() == 1
    fila = limpio.collect()[0]
    assert fila["id_subestacion"] == "SE_A"
