#!/usr/bin/env python3
"""
Smart Grid - Structured Streaming: ventanas de 15 minutos.
- Lee energy_raw: agrega carga media de la red (MW) y detecta picos de demanda.
- Pico: carga media agregada > umbral o max potencia en ventana alta.
"""
import os
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg as spark_avg, max as spark_max,
    sum as spark_sum, map_entries, explode, to_timestamp, when, lit,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, MapType,
)

from config import KAFKA_BOOTSTRAP, TOPIC_RAW

# Umbral orientativo pico de demanda (MW agregados ~ suma subestaciones / escala)
UMBRAL_PICO_CARGA_MEDIA_MW = float(os.environ.get("UMBRAL_PICO_CARGA_MEDIA_MW", "12000"))


def run_streaming_15min(spark_master="local[*]", checkpoint_dir=None, topic=None):
    topic = topic or TOPIC_RAW
    checkpoint_dir = checkpoint_dir or "/tmp/streaming_smartgrid_checkpoint"
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    spark = (
        SparkSession.builder
        .appName("SmartGridStreamingCarga15min")
        .master(spark_master)
        .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    sub_struct = StructType([
        StructField("voltaje_kv", FloatType()),
        StructField("potencia_mw", FloatType()),
        StructField("capacidad_mw", FloatType()),
        StructField("uso_pct", FloatType()),
        StructField("estado", StringType()),
        StructField("motivo", StringType()),
        StructField("timestamp", StringType()),
    ])
    schema_value = StructType([
        StructField("timestamp", StringType()),
        StructField("intervalo_minutos", IntegerType()),
        StructField("subestaciones", MapType(StringType(), sub_struct)),
    ])

    parsed = df.select(
        from_json(col("value").cast("string"), schema_value).alias("data")
    ).select("data.timestamp", "data.subestaciones").filter(col("subestaciones").isNotNull())

    # Una fila por subestación por evento
    por_nodo = parsed.select(
        col("timestamp"),
        explode(map_entries(col("subestaciones"))).alias("e"),
    ).select(
        col("timestamp"),
        col("e.key").alias("id_subestacion"),
        col("e.value.potencia_mw").cast("double").alias("potencia_mw"),
        col("e.value.uso_pct").cast("double").alias("uso_pct"),
    )

    with_ts = por_nodo.withColumn(
        "event_time",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
    ).withColumn(
        "event_time",
        when(col("event_time").isNull(), to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")).otherwise(col("event_time")),
    ).na.drop(subset=["event_time", "potencia_mw"])

    ventanas = with_ts.withWatermark("event_time", "10 minutes").groupBy(
        window(col("event_time"), "15 minutes", "15 minutes")
    ).agg(
        count("*").alias("lecturas_nodo"),
        spark_avg("potencia_mw").alias("carga_media_por_medicion_mw"),
        spark_sum("potencia_mw").alias("potencia_total_red_mw"),
        spark_max("potencia_mw").alias("pico_subestacion_mw"),
        spark_avg("uso_pct").alias("uso_medio_pct_red"),
    ).withColumn(
        "alerta_pico_demanda",
        when(col("potencia_total_red_mw") > lit(UMBRAL_PICO_CARGA_MEDIA_MW * 0.8), lit("SI")).otherwise(lit("NO")),
    )

    query = (
        ventanas.select(
            col("window.start"),
            col("window.end"),
            col("lecturas_nodo"),
            col("carga_media_por_medicion_mw"),
            col("potencia_total_red_mw"),
            col("pico_subestacion_mw"),
            col("uso_medio_pct_red"),
            col("alerta_pico_demanda"),
        )
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 50)
        .start()
    )

    print("[STREAMING] Ventanas 15 min | tema:", topic)
    print("[STREAMING] Métricas: carga media por medición, potencia total red, pico, alerta pico")
    query.awaitTermination()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--master", default="local[*]")
    p.add_argument("--checkpoint", default=None)
    p.add_argument("--topic", default=None)
    args = p.parse_args()
    run_streaming_15min(
        spark_master=args.master,
        checkpoint_dir=args.checkpoint,
        topic=args.topic,
    )
