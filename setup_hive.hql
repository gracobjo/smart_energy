-- Smart Grid - Hive: histórico y reportes de consumo energético diario
CREATE DATABASE IF NOT EXISTS smart_grid_analytics;
USE smart_grid_analytics;

-- Clima (contexto para correlación demanda)
CREATE TABLE IF NOT EXISTS clima_hist (
    subestacion_nombre STRING,
    temperatura FLOAT,
    descripcion STRING,
    humedad INT,
    fecha_captura TIMESTAMP
)
PARTITIONED BY (dia STRING)
STORED AS ORC;

-- Eventos de red (estado líneas: OK, ALERTA, SOBRECARGA)
CREATE TABLE IF NOT EXISTS red_electrica_hist (
    origen STRING,
    destino STRING,
    flujo_mw FLOAT,
    capacidad_mw FLOAT,
    estado STRING,
    motivo_fallo STRING,
    timestamp_evento TIMESTAMP
)
PARTITIONED BY (fecha STRING)
STORED AS PARQUET;

-- Métricas de grafo (nodos críticos)
CREATE TABLE IF NOT EXISTS metricas_subestaciones_hist (
    id_subestacion STRING,
    pagerank_score FLOAT,
    voltaje_kv FLOAT,
    potencia_mw FLOAT,
    fecha_proceso TIMESTAMP
)
STORED AS PARQUET;

-- Histórico sostenibilidad (Electricity Maps + carga media red; reportes ESG)
CREATE TABLE IF NOT EXISTS sostenibilidad_carbono_hist (
    fecha STRING,
    carbon_intensity_g_co2_kwh FLOAT,
    renewable_pct FLOAT,
    carga_media_subestaciones_mw DOUBLE,
    timestamp_captura TIMESTAMP
)
PARTITIONED BY (anio INT, mes INT)
STORED AS PARQUET;

-- Clima en zonas solares/eólicas (OpenWeather; correlación producción renovable)
CREATE TABLE IF NOT EXISTS clima_renovables_hist (
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
STORED AS PARQUET;

-- Consumo energético diario por subestación (reporting)
CREATE TABLE IF NOT EXISTS consumo_energetico_diario (
    id_subestacion STRING,
    fecha STRING,
    energia_mwh DOUBLE,
    potencia_max_mw DOUBLE,
    voltaje_min_kv FLOAT,
    voltaje_max_kv FLOAT,
    num_eventos_sobrecarga INT,
    num_eventos_alerta INT
)
PARTITIONED BY (anio INT, mes INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
