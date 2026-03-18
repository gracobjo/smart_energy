#!/usr/bin/env python3
"""
Smart Grid - Procesamiento Spark + GraphFrames
- Nodos: subestaciones. Aristas: líneas físicas (tras autosanación por sobrecarga).
- PageRank: nodos críticos. Puntos de fallo únicos: articulación (si cae el nodo, fragmentos aislados).
- Cassandra: último estado red (voltaje/potencia), puntos_fallo_unicos, pagerank.
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, BooleanType, IntegerType
from graphframes import GraphFrame

from config_nodos import get_nodos, get_aristas
sys.path.insert(0, str(Path(__file__).resolve().parent))
from grafo_puntos_fallo import analizar_puntos_fallo_unicos
from config import (
    JAR_GRAPHFRAMES,
    JAR_CASSANDRA,
    CASSANDRA_HOST,
    KEYSPACE,
    HDFS_BACKUP_PATH,
    HIVE_DB,
)

# Estados: sobrecarga se excluye del grafo; alerta penaliza peso
PESO_ALERTA = 1.5
ESTADO_SOBRECARGA = "sobrecarga"


def crear_spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    master = os.environ.get("SPARK_MASTER", "local")
    return (
        SparkSession.builder
        .appName("SmartGridMineriaGrafos")
        .master(master)
        .config("spark.driver.memory", "512m")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.timeoutMS", "30000")
        .config("spark.cassandra.connection.keepAliveMS", "30000")
        .config("spark.jars", f"{JAR_GRAPHFRAMES},{JAR_CASSANDRA}")
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
        .config("spark.eventLog.enabled", "false")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "false")
        .enableHiveSupport()
        .getOrCreate()
    )


def construir_grafo_base(spark, nodos, aristas):
    """GraphFrame: vértices = subestaciones, edges = líneas (peso = longitud_km para rutas)."""
    v_data = [
        (nid, float(datos["lat"]), float(datos["lon"]), datos["tipo"], float(datos.get("capacidad_mw", 200)))
        for nid, datos in nodos.items()
    ]
    v = spark.createDataFrame(v_data, ["id", "lat", "lon", "tipo", "capacidad_mw"])

    # aristas: (src, dst, longitud_km, capacidad_mw)
    e_data = []
    for t in aristas:
        src, dst = t[0], t[1]
        long_km = float(t[2]) if len(t) > 2 else 100.0
        e_data.append((src, dst, long_km))
    e = spark.createDataFrame(e_data, ["src", "dst", "longitud_km"])
    return GraphFrame(v, e)


def aplicar_autosanacion(g, estados_lineas, nodos):
    """
    Excluir líneas en sobrecarga del grafo; penalizar peso para líneas en alerta.
    estados_lineas: dict key "src|dst" -> {estado, flujo_mw, capacidad_mw, ...}
    """
    aristas_list = get_aristas()
    aristas_filtradas = []
    for t in aristas_list:
        src, dst = t[0], t[1]
        long_km = float(t[2]) if len(t) > 2 else 100.0
        key = f"{src}|{dst}"
        key_inv = f"{dst}|{src}"
        info = estados_lineas.get(key) or estados_lineas.get(key_inv)
        if info:
            estado = (info.get("estado") or "ok").lower()
            if estado == ESTADO_SOBRECARGA:
                continue  # Excluir línea en sobrecarga
            peso = long_km
            if estado == "alerta":
                peso *= PESO_ALERTA
            aristas_filtradas.append((src, dst, peso))
        else:
            aristas_filtradas.append((src, dst, long_km))

    if not aristas_filtradas:
        return g
    spark = SparkSession.getActiveSession()
    v = g.vertices
    e = spark.createDataFrame(aristas_filtradas, ["src", "dst", "peso_penalizado"])
    return GraphFrame(v, e)


def procesar_y_persistir(spark, subestaciones, lineas, clima_map):
    """Grafo, PageRank para nodos críticos, escritura Cassandra + Hive."""
    nodos = get_nodos()
    aristas = get_aristas()

    g0 = construir_grafo_base(spark, nodos, aristas)
    g = aplicar_autosanacion(g0, lineas, nodos)

    # Detección de nodos críticos en la red (PageRank)
    pr = g.pageRank(resetProbability=0.15, maxIter=10)
    pagerank_df = pr.vertices.select("id", col("pagerank").alias("pagerank_val"))

    now = datetime.now(timezone.utc)

    # --- Cassandra: subestaciones_estado (último voltaje por subestación) ---
    schema_sub = StructType([
        StructField("id_subestacion", StringType()),
        StructField("lat", FloatType()),
        StructField("lon", FloatType()),
        StructField("tipo", StringType()),
        StructField("voltaje_kv", FloatType()),
        StructField("potencia_mw", FloatType()),
        StructField("capacidad_mw", FloatType()),
        StructField("uso_pct", FloatType()),
        StructField("estado", StringType()),
        StructField("motivo", StringType()),
        StructField("clima_actual", StringType()),
        StructField("temperatura", FloatType()),
        StructField("humedad", FloatType()),
        StructField("ultima_actualizacion", TimestampType()),
    ])
    rows_sub = []
    for nid, datos in nodos.items():
        s = subestaciones.get(nid, {})
        clima = clima_map.get(nid, {})
        rows_sub.append((
            nid,
            float(datos["lat"]),
            float(datos["lon"]),
            datos["tipo"],
            float(s.get("voltaje_kv") or 220.0),
            float(s.get("potencia_mw") or 0.0),
            float(s.get("capacidad_mw") or datos.get("capacidad_mw", 200)),
            float(s.get("uso_pct") or 0.0),
            (s.get("estado") or "ok").capitalize(),
            s.get("motivo") or "",
            clima.get("descripcion", "N/A"),
            float(clima.get("temperatura") or 0),
            float(clima.get("humedad") or 0),
            now,
        ))
    df_sub = spark.createDataFrame(rows_sub, schema_sub)
    df_sub.write.format("org.apache.spark.sql.cassandra").options(
        table="subestaciones_estado", keyspace=KEYSPACE
    ).mode("append").save()

    # --- Cassandra: lineas_estado ---
    schema_lin = StructType([
        StructField("src", StringType()),
        StructField("dst", StringType()),
        StructField("flujo_mw", FloatType()),
        StructField("capacidad_mw", FloatType()),
        StructField("longitud_km", FloatType()),
        StructField("estado", StringType()),
        StructField("motivo", StringType()),
        StructField("ultima_actualizacion", TimestampType()),
    ])
    rows_lin = []
    for t in aristas:
        src, dst = t[0], t[1]
        long_km = float(t[2]) if len(t) > 2 else 100.0
        cap_mw = float(t[3]) if len(t) > 3 else 300.0
        key = f"{src}|{dst}"
        info = lineas.get(key, {})
        rows_lin.append((
            src,
            dst,
            float(info.get("flujo_mw") or 0),
            cap_mw,
            long_km,
            (info.get("estado") or "ok").capitalize(),
            info.get("motivo") or "",
            now,
        ))
    if rows_lin:
        df_lin = spark.createDataFrame(rows_lin, schema_lin)
        df_lin.write.format("org.apache.spark.sql.cassandra").options(
            table="lineas_estado", keyspace=KEYSPACE
        ).mode("append").save()

    # --- Cassandra: pagerank_subestaciones (nodos críticos) ---
    pr_rows = pagerank_df.collect()
    pr_data = [(r["id"], float(r["pagerank_val"]), now) for r in pr_rows]
    df_pr = spark.createDataFrame(pr_data, ["id_subestacion", "pagerank", "ultima_actualizacion"])
    df_pr.write.format("org.apache.spark.sql.cassandra").options(
        table="pagerank_subestaciones", keyspace=KEYSPACE
    ).mode("append").save()

    # Puntos de fallo únicos (articulación): ¿qué queda aislado si cae el nodo?
    try:
        edge_rows = g.edges.select("src", "dst").collect()
        aristas_undir = [(r["src"], r["dst"]) for r in edge_rows]
        pf = analizar_puntos_fallo_unicos(list(nodos.keys()), aristas_undir)
        schema_pf = StructType([
            StructField("id_subestacion", StringType()),
            StructField("es_articulacion", BooleanType()),
            StructField("fragmentos_al_fallar", IntegerType()),
            StructField("nodos_afectados_json", StringType()),
            StructField("ultima_actualizacion", TimestampType()),
        ])
        rows_pf = [
            (
                p["id_subestacion"],
                bool(p["es_articulacion"]),
                int(p["fragmentos_al_fallar"]),
                p["nodos_afectados_json"],
                now,
            )
            for p in pf
        ]
        df_pf = spark.createDataFrame(rows_pf, schema_pf)
        df_pf.write.format("org.apache.spark.sql.cassandra").options(
            table="puntos_fallo_unicos", keyspace=KEYSPACE
        ).mode("append").save()
    except Exception as ex:
        print(f"[PUNTOS_FALLO] {ex}")

    # Hive opcional: histórico
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
        spark.sql(f"USE {HIVE_DB}")
        df_sub.withColumn("fecha_proceso", current_timestamp()).write.format("hive").mode("append").saveAsTable(f"{HIVE_DB}.historico_subestaciones")
    except Exception as e:
        print(f"[HIVE] Opcional: {e}")

    return g, pagerank_df


def _cassandra_disponible():
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((CASSANDRA_HOST, 9042))
        s.close()
        return True
    except Exception:
        return False


def main():
    import subprocess
    import json

    if not _cassandra_disponible():
        print(f"[ERROR] Cassandra no disponible en {CASSANDRA_HOST}:9042")
        sys.exit(1)
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_BACKUP_PATH], capture_output=True, timeout=30)
    except Exception:
        pass

    spark = crear_spark()
    try:
        payload = None
        try:
            r = subprocess.run(
                ["hdfs", "dfs", "-ls", HDFS_BACKUP_PATH],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if r.returncode == 0 and "energy_" in (r.stdout or ""):
                df = spark.read.json(f"{HDFS_BACKUP_PATH}/*.json")
                rows = df.collect()
                if rows:
                    last = rows[-1]
                    payload = last.asDict() if hasattr(last, "asDict") else dict(last)
        except Exception:
            payload = None

        if payload is None:
            # Simulación mínima para pruebas
            import random
            nodos = get_nodos()
            aristas = get_aristas()
            subestaciones = {
                n: {
                    "voltaje_kv": 220 - random.uniform(0, 5),
                    "potencia_mw": nodos[n].get("capacidad_mw", 200) * random.uniform(0.3, 0.9),
                    "capacidad_mw": nodos[n].get("capacidad_mw", 200),
                    "uso_pct": random.uniform(30, 90),
                    "estado": random.choice(["ok", "alerta", "sobrecarga"]),
                    "motivo": "Simulación",
                }
                for n in nodos
            }
            lineas = {}
            for t in aristas:
                src, dst = t[0], t[1]
                cap = float(t[3]) if len(t) > 3 else 300
                lineas[f"{src}|{dst}"] = {
                    "flujo_mw": cap * random.uniform(0.2, 0.9),
                    "capacidad_mw": cap,
                    "estado": random.choice(["ok", "alerta"]),
                    "motivo": "",
                }
            clima_map = {}
        else:
            subestaciones = payload.get("subestaciones", payload.get("estados_nodos", {}))
            lineas = payload.get("lineas", payload.get("estados_aristas", {}))
            clima_list = payload.get("clima", [])
            clima_map = {c.get("subestacion", c.get("ciudad", "")): c for c in clima_list if c.get("subestacion") or c.get("ciudad")}

        procesar_y_persistir(spark, subestaciones, lineas, clima_map)
        print("[PROCESAMIENTO] OK - Cassandra actualizado (subestaciones, líneas, PageRank)")
    finally:
        spark.stop()
        print("[PROCESAMIENTO] Spark detenido")


if __name__ == "__main__":
    main()
