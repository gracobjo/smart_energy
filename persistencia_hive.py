"""
Capa de Persistencia Histórica - Hive (Smart Grid)
Almacena histórico de subestaciones, líneas y consumo energético diario para reportes.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType
from datetime import datetime
from typing import Dict, List, Any

HIVE_DB = "smart_grid_analytics"

TABLA_EVENTOS = "eventos_red_historico"
TABLA_SUBESTACIONES = "subestaciones_historico"
TABLA_LINEAS = "lineas_historico"
TABLA_CLIMA = "clima_historico"
TABLA_CONSUMO_DIARIO = "consumo_energetico_diario"
# Alineado con setup_hive.hql — PageRank + estado eléctrico
TABLA_METRICAS_SUB = "metricas_subestaciones_hist"


def crear_spark_hive(app_name: str = "HivePersistenceSmartGrid") -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .enableHiveSupport() \
        .getOrCreate()


def parsear_timestamp(ts: str) -> Dict:
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return {
            "anio": dt.year,
            "mes": dt.month,
            "dia": dt.day,
            "hora": dt.hour,
            "minuto": dt.minute,
        }
    except Exception:
        now = datetime.now()
        return {"anio": now.year, "mes": now.month, "dia": now.day, "hora": now.hour, "minuto": now.minute}


def inicializar_esquema_hive(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
    spark.sql(f"USE {HIVE_DB}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_EVENTOS} (
            timestamp STRING,
            anio INT,
            mes INT,
            dia INT,
            hora INT,
            tipo_entidad STRING,
            id_entidad STRING,
            estado STRING,
            motivo STRING,
            voltaje_kv FLOAT,
            potencia_mw FLOAT,
            hub_asociado STRING
        )
        PARTITIONED BY (anio_part INT, mes_part INT)
        STORED AS parquet
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_SUBESTACIONES} (
            timestamp STRING,
            anio INT,
            mes INT,
            dia INT,
            id_subestacion STRING,
            voltaje_kv FLOAT,
            potencia_mw FLOAT,
            capacidad_mw FLOAT,
            uso_pct FLOAT,
            estado STRING,
            motivo STRING,
            temperatura FLOAT,
            humedad INT
        )
        PARTITIONED BY (anio_part INT, mes_part INT)
        STORED AS parquet
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_LINEAS} (
            timestamp STRING,
            anio INT,
            mes INT,
            dia INT,
            src STRING,
            dst STRING,
            flujo_mw FLOAT,
            capacidad_mw FLOAT,
            estado STRING,
            motivo STRING
        )
        PARTITIONED BY (anio_part INT, mes_part INT)
        STORED AS parquet
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_CLIMA} (
            timestamp STRING,
            anio INT,
            mes INT,
            dia INT,
            subestacion STRING,
            temperatura FLOAT,
            humedad INT,
            descripcion STRING
        )
        PARTITIONED BY (anio_part INT, mes_part INT)
        STORED AS parquet
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_CONSUMO_DIARIO} (
            id_subestacion STRING,
            fecha STRING,
            energia_mwh DOUBLE,
            potencia_max_mw DOUBLE,
            voltaje_min_kv FLOAT,
            voltaje_max_kv FLOAT,
            num_sobrecarga INT,
            num_alerta INT
        )
        PARTITIONED BY (anio_part INT, mes_part INT)
        STORED AS parquet
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_METRICAS_SUB} (
            id_subestacion STRING,
            pagerank_score FLOAT,
            voltaje_kv FLOAT,
            potencia_mw FLOAT,
            fecha_proceso TIMESTAMP
        )
        STORED AS PARQUET
    """)


def persistir_metricas_pagerank_hive(spark, pagerank_df, df_sub) -> None:
    """Escribe PageRank + voltaje/potencia en Hive (histórico de métricas de grafo)."""
    from pyspark.sql.functions import col as Fcol, current_timestamp

    spark.sql(f"USE {HIVE_DB}")
    j = pagerank_df.alias("pr").join(
        df_sub.select(Fcol("id_subestacion").alias("sid"), "voltaje_kv", "potencia_mw"),
        Fcol("pr.id") == Fcol("sid"),
        "inner",
    )
    out = j.select(
        Fcol("pr.id").alias("id_subestacion"),
        Fcol("pagerank_val").alias("pagerank_score"),
        Fcol("voltaje_kv"),
        Fcol("potencia_mw"),
        current_timestamp().alias("fecha_proceso"),
    )
    if not out.limit(1).take(1):
        return
    out.write.mode("append").insertInto(f"{HIVE_DB}.{TABLA_METRICAS_SUB}")


def persistir_subestaciones_historico(spark: SparkSession, datos: Dict) -> None:
    ts = datos.get("timestamp", datetime.now().isoformat())
    comp = parsear_timestamp(ts)
    anio, mes = comp["anio"], comp["mes"]
    registros = []
    for sid, s in datos.get("subestaciones", {}).items():
        registros.append({
            "timestamp": ts,
            "anio": comp["anio"], "mes": comp["mes"], "dia": comp["dia"],
            "id_subestacion": sid,
            "voltaje_kv": float(s.get("voltaje_kv", 220)),
            "potencia_mw": float(s.get("potencia_mw", 0)),
            "capacidad_mw": float(s.get("capacidad_mw", 200)),
            "uso_pct": float(s.get("uso_pct", 0)),
            "estado": s.get("estado", "ok"),
            "motivo": s.get("motivo", ""),
            "temperatura": 0.0,
            "humedad": 0,
            "anio_part": anio,
            "mes_part": mes,
        })
    if registros:
        df = spark.createDataFrame(registros)
        df.write.mode("append").partitionBy("anio_part", "mes_part").insertInto(f"{HIVE_DB}.{TABLA_SUBESTACIONES}")


def persistir_lineas_historico(spark: SparkSession, datos: Dict) -> None:
    ts = datos.get("timestamp", datetime.now().isoformat())
    comp = parsear_timestamp(ts)
    anio, mes = comp["anio"], comp["mes"]
    registros = []
    for key, lin in datos.get("lineas", {}).items():
        if "|" not in key:
            continue
        src, dst = key.split("|", 1)
        registros.append({
            "timestamp": ts,
            "anio": comp["anio"], "mes": comp["mes"], "dia": comp["dia"],
            "src": src, "dst": dst,
            "flujo_mw": float(lin.get("flujo_mw", 0)),
            "capacidad_mw": float(lin.get("capacidad_mw", 300)),
            "estado": lin.get("estado", "ok"),
            "motivo": lin.get("motivo", ""),
            "anio_part": anio,
            "mes_part": mes,
        })
    if registros:
        df = spark.createDataFrame(registros)
        df.write.mode("append").partitionBy("anio_part", "mes_part").insertInto(f"{HIVE_DB}.{TABLA_LINEAS}")


def persistir_clima_historico(spark: SparkSession, datos: Dict) -> None:
    ts = datos.get("timestamp", datetime.now().isoformat())
    comp = parsear_timestamp(ts)
    anio, mes = comp["anio"], comp["mes"]
    registros = []
    for c in datos.get("clima", []):
        registros.append({
            "timestamp": ts,
            "anio": comp["anio"], "mes": comp["mes"], "dia": comp["dia"],
            "subestacion": c.get("subestacion", c.get("ciudad", "")),
            "temperatura": c.get("temperatura"),
            "humedad": c.get("humedad"),
            "descripcion": c.get("descripcion", ""),
            "anio_part": anio,
            "mes_part": mes,
        })
    if registros:
        df = spark.createDataFrame(registros)
        df.write.mode("append").partitionBy("anio_part", "mes_part").insertInto(f"{HIVE_DB}.{TABLA_CLIMA}")


def calcular_consumo_diario(spark: SparkSession) -> None:
    """Agregado diario: energía (aprox. por potencia*intervalos), max potencia, min/max voltaje, conteo sobrecarga/alerta."""
    spark.sql(f"USE {HIVE_DB}")
    spark.sql(f"""
        INSERT OVERWRITE TABLE {TABLA_CONSUMO_DIARIO} PARTITION(anio_part, mes_part)
        SELECT
            id_subestacion,
            CONCAT(CAST(anio AS STRING), '-', LPAD(CAST(mes AS STRING), 2, '0'), '-', LPAD(CAST(dia AS STRING), 2, '0')) as fecha,
            SUM(potencia_mw) * 0.25 AS energia_mwh,
            MAX(potencia_mw) AS potencia_max_mw,
            MIN(voltaje_kv) AS voltaje_min_kv,
            MAX(voltaje_kv) AS voltaje_max_kv,
            SUM(CASE WHEN estado = 'sobrecarga' THEN 1 ELSE 0 END) AS num_sobrecarga,
            SUM(CASE WHEN estado = 'alerta' THEN 1 ELSE 0 END) AS num_alerta,
            anio AS anio_part,
            mes AS mes_part
        FROM {TABLA_SUBESTACIONES}
        WHERE anio_part IS NOT NULL
        GROUP BY id_subestacion, anio, mes, dia, anio, mes
    """)


def ejecutar_persistencia_hive(
    spark: SparkSession,
    datos: Dict,
    pagerank_df=None,
    df_subestaciones=None,
) -> None:
    inicializar_esquema_hive(spark)
    persistir_subestaciones_historico(spark, datos)
    persistir_lineas_historico(spark, datos)
    persistir_clima_historico(spark, datos)
    if pagerank_df is not None and df_subestaciones is not None:
        try:
            persistir_metricas_pagerank_hive(spark, pagerank_df, df_subestaciones)
        except Exception as e:
            print(f"[HIVE] métricas PageRank: {e}")
    try:
        calcular_consumo_diario(spark)
    except Exception as e:
        print(f"Consumo diario (puede requerir datos): {e}")
    print("Persistencia Hive Smart Grid completada ✓")


if __name__ == "__main__":
    from config_nodos import get_nodos
    nodos = get_nodos()
    datos_ejemplo = {
        "timestamp": datetime.now().isoformat(),
        "subestaciones": {n: {"voltaje_kv": 220, "potencia_mw": 100, "capacidad_mw": d.get("capacidad_mw", 200), "uso_pct": 50, "estado": "ok", "motivo": ""} for n, d in list(nodos.items())[:3]},
        "lineas": {},
        "clima": [],
    }
    spark = crear_spark_hive()
    ejecutar_persistencia_hive(spark, datos_ejemplo)
    spark.stop()
