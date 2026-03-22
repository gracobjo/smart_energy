# Checklist: requisitos Big Data / Smart Grid

El proyecto es **Smart Grid** (red eléctrica). Este documento enlaza con el PDF histórico *Proyecto Big Data* donde aplica.

---

## Resumen ejecutivo

| Área | Estado | Comentario |
|------|--------|------------|
| **Ingesta** | Sí | **`producer.py`** + **NiFi 2.6.0**: Electricity Maps + OpenWeather; Kafka **`energy_raw`** + **`weather_raw`** + **`gps_raw`**; HDFS. |
| **Procesamiento** | Sí | GraphFrames, puntos de fallo, PageRank; Spark batch. |
| **Persistencia** | Sí | Cassandra `smart_grid`, Hive `smart_grid_analytics` (sostenibilidad, clima renovables). |
| **Orquestación** | Sí | DAGs: arranque, parar, comprobar, KDD fases, consultas, **informes**, maestro 15 min, mensual. |
| **NiFi** | Sí | ExecuteStreamCommand (producer), InvokeHTTP, GetFile GPS, PublishKafka; UI accesible desde frontend. |
| **Informes** | Sí | `generar_informe_fases.py` + DAG; informe consolidado de todas las fases. |
| **YARN** | Opcional | Spark en `local` por defecto; ver `docs/YARN_Y_SPARK.md`. |
| **Documentación** | Sí | README, docs/, `docs/CREDENCIALES_UI.md`, `docs/INFORME_NIFI_PIPELINE_SMART_GRID.md`. |

---

## Requisitos técnicos del PDF (Stack Apache 2026)

| Requisito PDF | Versión pedida | En el proyecto | ¿Cumple? |
|---------------|----------------|----------------|----------|
| Ingesta | NiFi 2.6.0 + Kafka 3.9.1 (KRaft) | NiFi 2.6.0 (API + GPS + ExecuteStreamCommand) + producer.py | Sí |
| Procesamiento | Spark 3.5.x (SQL, Structured Streaming, GraphFrames) | Spark 3.5, GraphFrames, Spark SQL implícito; no Structured Streaming con ventanas | Parcial |
| Orquestación | Airflow 2.10.x | DAG presente (versión según instalación) | Parcial |
| Almacenamiento | HDFS 3.4.2, Cassandra 5.0, Hive | HDFS, Cassandra, Hive (versiones según instalación) | Sí |
| Gestión recursos | YARN | No; Spark en `local` | No |

---

## Fases KDD según el PDF

### Fase I: Ingesta y Selección (NiFi + Kafka)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| Fuentes externas | **NiFi** consumiendo API pública (OpenWeather, etc.) y logs GPS simulados | NiFi: InvokeHTTP OpenWeather, ExecuteStreamCommand (producer.py), GetFile (GPS) | Sí |
| Streaming | Publicar en Kafka con **dos temas**: "Datos Crudos" y "Datos Filtrados" | Un solo tema `transporte_status` (datos ya enriquecidos) | Parcial |
| Registro | Copia "raw" en HDFS para auditoría | JSON de ingesta guardado en HDFS | Sí |

**Conclusión Fase I:** NiFi integrado (API + logs GPS + ExecuteStreamCommand); topics `energy_raw`, `weather_raw`, `gps_raw`.

---

### Fase II: Preprocesamiento y Transformación (Spark)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| Limpieza | **Spark SQL** para normalizar, nulos y duplicados | Limpieza en Python (`limpiar_datos_antes_cassandra`) antes de escribir; se podría hacer con Spark SQL | Parcial |
| Enriquecimiento | Cruzar streaming Kafka con **datos maestros en Hive** | No se cruza con tablas Hive; se usa `config_nodos` y payload de ingesta | Parcial / No |
| Análisis de grafos | GraphFrames: nodos (almacenes), aristas (rutas), camino más corto o comunidades | GraphFrames con nodos/aristas, autosanación, ShortestPath, PageRank | Sí |

**Conclusión Fase II:** Grafos sí; limpieza existe pero no está formulada como “Spark SQL”; enriquecimiento con datos maestros en Hive **no** implementado.

---

### Fase III: Minería y Acción (Streaming + ML)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| Ventanas de tiempo | **Structured Streaming** con ventanas de **15 minutos** (media de retrasos) | Procesamiento por lotes (lectura desde HDFS); ciclo cada 15 min pero no ventanas de ventana en Structured Streaming | Parcial / No |
| Carga multicapa | Hive: agregados histórico; Cassandra: último estado por vehículo | Sí: Hive histórico, Cassandra estado actual (nodos, aristas, camiones, PageRank) | Sí |

**Conclusión Fase III:** Carga dual Hive/Cassandra sí; **Structured Streaming con ventanas de 15 min** no está como en el enunciado.

---

### Fase IV: Orquestación (Airflow)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| DAG | Coordinar re-entrenamiento mensual y limpieza HDFS | **dag_mensual_retrain_limpieza**: día 1 de cada mes; limpieza HDFS + re-entrenamiento grafos + modelo respaldo | Sí |
| DAGs adicionales | Orquestar servicios y fases KDD | **dag_arranque_servicios**, **dag_parar_servicios**, **dag_comprobar_servicios**; **dag_kdd_fase1/2/3**; **dag_consultas_hive_cassandra**; **dag_informes_fases** | Sí |
| Pipeline periódico | Ingesta + procesamiento cada 15 min | **dag_maestro_smart_grid** | Sí |

**Conclusión Fase IV:** Orquestación completa con Airflow: DAGs por fase, servicios, informes, maestro 15 min, mensual.

---

## Rúbrica de evaluación (resumen)

| Criterio | Excelente (10) según PDF | Estado actual |
|----------|--------------------------|----------------|
| Ingesta | NiFi y Kafka con back-pressure | NiFi 2.6.0 integrado; Kafka energy_raw, weather_raw, gps_raw |
| Procesamiento Spark | GraphFrames, SQL, Streaming | GraphFrames, PageRank, puntos de fallo; batch desde HDFS |
| Persistencia | Cassandra + Hive según caso de uso | Sí (Cassandra estado actual, Hive histórico) |
| Orquestación Airflow | DAGs con reintentos, dependencias | 10 DAGs: servicios, KDD fases, informes, maestro 15 min, mensual |
| Documentación | Cada etapa KDD, diagramas, justificación | README, docs/, AGENTS, CREDENCIALES_UI, INFORME_NIFI |

---

## Implementado (alineado al PDF)

- **Kafka**: Dos temas `transporte_raw` y `transporte_filtered`. Ingesta publica en ambos; crear con `bash sql/crear_temas_kafka.sh`.
- **Structured Streaming**: `procesamiento/streaming_ventanas_15min.py` (ventanas 15 min sobre `transporte_filtered`).
- **Enriquecimiento Hive**: `enriquecer_desde_hive()` en procesamiento; tabla `nodos_maestro`; enriquece nodos con `hub`.
- **DAG mensual**: `orquestacion/dag_mensual_retrain_limpieza.py` (día 1 de cada mes: limpieza HDFS + re-entrenamiento grafos).
- **YARN**: `SPARK_MASTER=yarn` o `spark-submit --master yarn`; ver `docs/YARN_Y_SPARK.md`.

## Qué falta para alinearse al PDF

1. ~~**NiFi**~~: Integrado. `./scripts/instalar_nifi_260.sh`; dashboard Fase 1 → NiFi. Flujos: API (InvokeHTTP), ExecuteStreamCommand (producer.py), GetFile (GPS) → PublishKafka. Ver `docs/NIFI_INTEGRACION.md`.
2. **Spark SQL** para limpieza: opcional (ya existe limpieza en Python antes de Cassandra). El resto (Kafka 2 temas, Streaming, Hive, DAG mensual, YARN) está implementado.

---

## Conclusión

- **Se cumple**: ciclo de datos (ingesta producer/NiFi → Kafka/HDFS → Spark → Cassandra + Hive), GraphFrames, persistencia dual, orquestación Airflow completa, NiFi, informes, documentación.
- **DAGs Airflow**: arranque, parar, comprobar servicios; KDD fases 1–3; consultas Hive/Cassandra; informes consolidados; maestro 15 min; mensual (limpieza HDFS + re-entrenamiento).
- **NiFi**: Ingesta vía NiFi 2.6.0 (ExecuteStreamCommand producer, InvokeHTTP OpenWeather, GetFile GPS); UI accesible desde dashboard.
- **UIs**: Enlaces Airflow y NiFi en sidebar y Monitorización; credenciales en `docs/CREDENCIALES_UI.md`.
