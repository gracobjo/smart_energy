"""
Esquemas Spark (Structured Streaming / batch) para eventos de subestación.
Compatible con mensajes JSON en Kafka topic energy_raw (subestaciones anidadas simplificados a filas planas en tests).
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)


# Fila por subestación tras explode (streaming batch)
SCHEMA_LECTURA_SUBESTACION = StructType(
    [
        StructField("id_subestacion", StringType(), False),
        StructField("potencia_mw", DoubleType(), True),
        StructField("capacidad_mw", DoubleType(), True),
        StructField("temperatura", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

# Catálogo maestro (Hive / dimensión)
SCHEMA_MAESTRO_SUBESTACION = StructType(
    [
        StructField("id_subestacion", StringType(), False),
        StructField("nombre", StringType(), True),
        StructField("region", StringType(), True),
        StructField("voltaje_nominal_kv", DoubleType(), True),
    ]
)

# Evento agregado para Kafka (metadatos de alerta)
SCHEMA_EVENTO_KAFKA = StructType(
    [
        StructField("id_subestacion", StringType(), False),
        StructField("tipo_alerta", StringType(), True),
        StructField("valor", DoubleType(), True),
        StructField("ts", TimestampType(), True),
    ]
)
