-- =============================================================================
-- Sistema de Monitoreo de Redes de Energía Inteligentes (Smart Grid)
-- Esquema Cassandra 5.0 - Estado en tiempo real
-- Ejecutar: cqlsh -f cassandra/esquema_smart_grid.cql
-- =============================================================================

CREATE KEYSPACE IF NOT EXISTS smart_grid
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;

USE smart_grid;

-- Último voltaje y potencia leídos por subestación (estado en tiempo real)
CREATE TABLE IF NOT EXISTS subestaciones_estado (
    id_subestacion TEXT PRIMARY KEY,
    lat FLOAT,
    lon FLOAT,
    tipo TEXT,
    voltaje_kv FLOAT,
    potencia_mw FLOAT,
    capacidad_mw FLOAT,
    uso_pct FLOAT,
    estado TEXT,
    motivo TEXT,
    clima_actual TEXT,
    temperatura FLOAT,
    humedad FLOAT,
    ultima_actualizacion TIMESTAMP
);

-- Estado de líneas de alta tensión (flujo, sobrecarga)
CREATE TABLE IF NOT EXISTS lineas_estado (
    src TEXT,
    dst TEXT,
    flujo_mw FLOAT,
    capacidad_mw FLOAT,
    longitud_km FLOAT,
    estado TEXT,
    motivo TEXT,
    ultima_actualizacion TIMESTAMP,
    PRIMARY KEY (src, dst)
);

-- Eventos de red (caída de tensión, sobrecarga) para análisis
CREATE TABLE IF NOT EXISTS eventos_red (
    id_evento UUID,
    tipo_entidad TEXT,
    id_entidad TEXT,
    estado_anterior TEXT,
    estado_nuevo TEXT,
    voltaje_kv FLOAT,
    potencia_mw FLOAT,
    motivo TEXT,
    timestamp_evento TIMESTAMP,
    PRIMARY KEY ((id_entidad), timestamp_evento)
) WITH CLUSTERING ORDER BY (timestamp_evento DESC)
  AND default_time_to_live = 2592000;

-- Nodos críticos de la red (PageRank / centralidad para detección)
CREATE TABLE IF NOT EXISTS pagerank_subestaciones (
    id_subestacion TEXT PRIMARY KEY,
    pagerank FLOAT,
    ultima_actualizacion TIMESTAMP
);

-- Puntos de fallo únicos (articulación): si el nodo cae, fragmentación de la red
CREATE TABLE IF NOT EXISTS puntos_fallo_unicos (
    id_subestacion TEXT PRIMARY KEY,
    es_articulacion BOOLEAN,
    fragmentos_al_fallar INT,
    nodos_afectados_json TEXT,
    ultima_actualizacion TIMESTAMP
);

-- Parámetros del modelo de predicción de necesidad de energía de respaldo (re-entrenamiento mensual)
CREATE TABLE IF NOT EXISTS modelo_respaldo (
    id TEXT PRIMARY KEY,
    umbral_carga_media_pct FLOAT,
    umbral_carbono_g_kwh FLOAT,
    prob_respaldo_umbral FLOAT,
    version TEXT,
    ultima_actualizacion TIMESTAMP
);
