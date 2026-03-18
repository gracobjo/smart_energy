#!/usr/bin/env python3
"""
Persiste en Hive (smart_grid_analytics) los payloads de ingesta:
- sostenibilidad_carbono_hist: Electricity Maps + carga media de subestaciones (desde energy_raw).
- clima_renovables_hist: condiciones por zona solar/eólica (desde weather_raw).

Uso:
  python procesamiento/persistir_hive_ingesta.py --energy /ruta/energy.json --weather /ruta/weather.json

El producer puede dejar JSON en /tmp y ejecutar con PERSIST_HIVE_AFTER_INGEST=1.
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

HIVE_DB = "smart_grid_analytics"
TABLA_SOST = "sostenibilidad_carbono_hist"
TABLA_CLIMA_REN = "clima_renovables_hist"


def _spark():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder.appName("PersistHiveIngestaSmartGrid")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.driver.memory", "1g")
        .enableHiveSupport()
        .getOrCreate()
    )


def persistir_energy(spark, path: Path) -> bool:
    data = json.loads(path.read_text(encoding="utf-8"))
    ts = data.get("timestamp", datetime.now().isoformat())
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now()
    em = data.get("electricity_maps") or {}
    subs = data.get("subestaciones") or {}
    potencias = [float(v.get("potencia_mw") or 0) for v in subs.values()]
    carga_media = sum(potencias) / len(potencias) if potencias else 0.0
    row = {
        "fecha": dt.strftime("%Y-%m-%d"),
        "carbon_intensity_g_co2_kwh": float(em.get("carbon_intensity_g_co2_kwh") or 0),
        "renewable_pct": float(em.get("renewable_pct") or 0),
        "carga_media_subestaciones_mw": carga_media,
        "timestamp_captura": dt,
        "anio": dt.year,
        "mes": dt.month,
    }
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_DB}.{TABLA_SOST} (
            fecha STRING,
            carbon_intensity_g_co2_kwh FLOAT,
            renewable_pct FLOAT,
            carga_media_subestaciones_mw DOUBLE,
            timestamp_captura TIMESTAMP
        )
        PARTITIONED BY (anio INT, mes INT)
        STORED AS PARQUET
    """)
    df = spark.createDataFrame([row])
    df.write.mode("append").insertInto(f"{HIVE_DB}.{TABLA_SOST}")
    print(f"[Hive] Insertado 1 fila en {TABLA_SOST}")
    return True


def persistir_weather(spark, path: Path) -> bool:
    data = json.loads(path.read_text(encoding="utf-8"))
    ts_global = data.get("timestamp", datetime.now().isoformat())
    try:
        dt = datetime.fromisoformat(ts_global.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now()
    zonas = data.get("zonas_renovables") or []
    rows = []
    for z in zonas:
        rows.append({
            "ingest_ts": dt,
            "zona_id": z.get("zona_id", ""),
            "tipo_planta": z.get("tipo_planta", ""),
            "lat": float(z.get("lat") or 0),
            "lon": float(z.get("lon") or 0),
            "temperatura_c": float(z["temperatura_c"]) if z.get("temperatura_c") is not None else 0.0,
            "humedad_pct": int(z["humedad_pct"]) if z.get("humedad_pct") is not None else 0,
            "viento_ms": float(z["viento_ms"]) if z.get("viento_ms") is not None else 0.0,
            "nubes_pct": int(z["nubes_pct"]) if z.get("nubes_pct") is not None else 0,
            "descripcion_clima": (z.get("descripcion_clima") or "")[:200],
            "timestamp_evento": str(z.get("timestamp", ts_global)),
            "anio": dt.year,
            "mes": dt.month,
        })
    if not rows:
        print("[Hive] weather_raw sin zonas; omitiendo clima_renovables_hist")
        return True
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_DB}.{TABLA_CLIMA_REN} (
            ingest_ts TIMESTAMP,
            zona_id STRING,
            tipo_planta STRING,
            lat DOUBLE,
            lon DOUBLE,
            temperatura_c FLOAT,
            humedad_pct INT,
            viento_ms FLOAT,
            nubes_pct INT,
            descripcion_clima STRING,
            timestamp_evento STRING
        )
        PARTITIONED BY (anio INT, mes INT)
        STORED AS PARQUET
    """)
    df = spark.createDataFrame(rows)
    df.write.mode("append").insertInto(f"{HIVE_DB}.{TABLA_CLIMA_REN}")
    print(f"[Hive] Insertadas {len(rows)} filas en {TABLA_CLIMA_REN}")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--energy", type=Path, help="JSON energy_raw")
    ap.add_argument("--weather", type=Path, help="JSON weather_raw")
    args = ap.parse_args()
    if not args.energy and not args.weather:
        print("Indica --energy y/o --weather")
        sys.exit(1)
    spark = _spark()
    try:
        if args.energy and args.energy.exists():
            persistir_energy(spark, args.energy)
        if args.weather and args.weather.exists():
            persistir_weather(spark, args.weather)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
