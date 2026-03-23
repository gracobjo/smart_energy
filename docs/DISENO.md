# Documento de Diseño — Smart Grid España

## 1. Arquitectura general

### 1.1 Vista de alto nivel

```
                    ┌─────────────────┐
                    │  APIs externas  │
                    │ Electricity Maps│
                    │  OpenWeather    │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    │
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  producer.py │     │    NiFi      │     │    Kafka     │
│  (directo o  │     │ ExecuteStream│────▶│ energy_raw   │
│  vía NiFi)   │────▶│ GetFile GPS  │     │ weather_raw  │
└──────────────┘     └──────────────┘     │ gps_raw      │
        │                    │             └──────┬───────┘
        └────────────────────┼────────────────────┘
                             ▼
                    ┌──────────────┐
                    │  HDFS backup │
                    │ energy_*.json│
                    └──────┬───────┘
                           │
                           ▼
                    ┌────────────────────────────────────┐
                    │      Spark (procesamiento_grafos)   │
                    │  GraphFrames · PageRank · Fallos    │
                    └────────┬───────────────┬────────────┘
                             │               │
              ┌──────────────┘               └──────────────┐
              ▼                                              ▼
     ┌─────────────────┐                          ┌─────────────────┐
     │   Cassandra     │                          │      Hive       │
     │  (tiempo real)  │                          │   (histórico)   │
     └────────┬────────┘                          └─────────────────┘
              │
              ▼
     ┌─────────────────────────────────────────────────────────┐
     │ app_visualizacion (Streamlit)                            │
     │ Enlaces: Airflow (8080) · NiFi (8443) · API Swagger (8000)│
     └─────────────────────────────────────────────────────────┘
                             ▲
                             │
     ┌───────────────────────┴───────────────────────┐
     │              Airflow 2.10.x / 3.x              │
     │  api-server + dag-processor + scheduler        │
     │  DAGs: arranque, parar, comprobar, KDD fases,  │
     │        consultas Hive/Cassandra, informes      │
     └───────────────────────────────────────────────┘
```

### 1.2 Capas del sistema

| Capa | Componentes | Tecnología |
|------|-------------|------------|
| Ingesta | producer.py, NiFi | Python, NiFi 2.6.0, Kafka |
| Mensajería | Topics Kafka | Apache Kafka 3.9.x (KRaft) |
| Almacenamiento crudo | HDFS | Hadoop HDFS |
| Procesamiento | procesamiento_grafos.py | Spark 3.5, GraphFrames |
| Estado tiempo real | Cassandra | Cassandra 5.0 |
| Histórico | Hive | Hive 4.x / Spark SQL |
| Visualización | app_visualizacion.py | Streamlit, Folium |
| Orquestación | DAGs Airflow | Airflow 2.10.x / 3.x (api-server, dag-processor, scheduler) |
| Informes | generar_informe_fases.py | Python |

---

## 2. Modelo de datos

### 2.1 Cassandra (tiempo real)

| Tabla | Clave primaria | Descripción |
|-------|----------------|-------------|
| subestaciones_estado | id_subestacion | Voltaje, potencia, estado, clima por subestación |
| lineas_estado | (src, dst) | Flujo MW, capacidad, estado por línea |
| pagerank_subestaciones | id_subestacion | PageRank (nodos críticos) |
| puntos_fallo_unicos | id_subestacion | Articulaciones, fragmentos al fallar |
| eventos_red | (id_entidad, timestamp) | Eventos de cambio de estado |

### 2.2 Hive (histórico)

| Tabla | Partición | Descripción |
|-------|-----------|-------------|
| consumo_energetico_diario | anio, mes | Consumo MWh, potencia max, eventos por subestación |
| sostenibilidad_carbono_hist | anio, mes | Intensidad carbono, % renovable, carga media |
| red_electrica_hist | fecha | Eventos de red (origen, destino, estado) |
| metricas_subestaciones_hist | — | PageRank, voltaje, potencia por subestación |
| clima_hist | dia | Temperatura, humedad en subestaciones |
| clima_renovables_hist | anio, mes | Clima en zonas solares/eólicas |

**Nota operativa:** El cuadro de mando consume por defecto tablas con carga continua desde `persistencia_hive.py` (`subestaciones_historico`, `lineas_historico`, `consumo_energetico_diario`, `metricas_subestaciones_hist`).  
Las tablas ESG (`sostenibilidad_carbono_hist`, `clima_renovables_hist`) se rellenan en flujo opcional de post-ingesta.

---

## 3. Diseño de componentes

### 3.1 producer.py

- **Responsabilidad:** Ingesta de datos desde APIs y simulación; publicación en Kafka; backup HDFS.
- **Entradas:** Electricity Maps, OpenWeather (o datos sintéticos); `config_nodos`, `config_plantas_renovables`.
- **Salidas:** Mensajes en `energy_raw`, `weather_raw`; ficheros JSON en HDFS.
- **Configuración:** `config.py` (KAFKA_BOOTSTRAP, TOPIC_RAW, API keys).

### 3.2 procesamiento_grafos.py

- **Responsabilidad:** Construcción del grafo, autosanación, PageRank, detección de articulaciones; persistencia Cassandra y Hive.
- **Entradas:** JSON en HDFS o simulación; topología en `config_nodos`.
- **Salidas:** Cassandra (subestaciones_estado, lineas_estado, pagerank, puntos_fallo); Hive (histórico).
- **Algoritmos:** GraphFrames, PageRank, análisis de puntos de fallo.

### 3.3 app_visualizacion.py

- **Responsabilidad:** Dashboard interactivo; mapa Folium; cuadro de mando; ciclo KDD; monitorización; enlaces a Airflow, NiFi, API Swagger.
- **Módulos:** `app_visualizacion_kdd_panel` (herramientas), `config_nodos` (topología).
- **Estados:** session_state para paso_15min, prev_cycle_snapshot, fase0_check.
- **Cuadro de mando Hive:** consultas SQL con modo rápido (`quick_check`) para no bloquear UI; parser principal (tabla con `|`) + fallback TSV sin cabecera para renderizar datos legibles cuando `spark-sql` devuelve salida tabulada.
- **Alineación frontend-Hive:** informes de sostenibilidad operativa, eventos y clima se basan en tablas históricas efectivamente pobladas (`subestaciones_historico`/`lineas_historico`) para reducir casos de “sin datos” en UI.

### 3.4 persistencia_hive.py

- **Responsabilidad:** Escritura de histórico en tablas Hive desde Spark.
- **Tablas:** subestaciones_historico, lineas_historico, eventos_red_historico, consumo_energetico_diario, metricas_subestaciones_hist.
- **Correcciones de escritura:** `insertInto()` sin `partitionBy()` (evita AnalysisException), casting explícito y reordenación de columnas para compatibilidad de tipos; agregado `consumo_energetico_diario` alineado con particiones `(anio, mes)` de `setup_hive.hql`.

### 3.5 NiFi (ingesta alternativa)

- **Responsabilidad:** Orquestar ingesta vía flujos visuales.
- **Procesadores (8):** NiFi_F1_GenerateTrigger, InvokeHTTP_OpenWeather, JoltTransformJSON_ToSchema, PublishKafka_weather_raw, PublishKafka_energy_raw, ExecuteProducer (producer.py), GetFile_GPS, PublishKafka_gps_raw. Tabla alineada en dashboard.
- **Cadencia:** Trigger principal ajustado a `15 min` para acompasar `ExecuteProducer` y evitar crecimiento de cola Trigger→ExecuteProducer.
- **Controller Service:** Kafka3ConnectionService para bootstrap Kafka.
- **Dashboard:** Crear, conectar, alinear (posiciones en canvas), arrancar/parar. Búsqueda con `includeDescendantGroups` para grupos anidados. InvokeHTTP usa Response (no Original) y auto-termina Original. ExecuteStreamCommand usa "Command Path".
- **Provenance:** API asíncrona (POST→poll→DELETE); script `nifi_flujo_comprobar.py` corregido.
- **Flow definition:** `nifi/smart_grid_flow_definition.json` importable desde UI.
- **Funcionalidades dashboard:** Crear procesadores (Fase I demo), alinear en canvas, conectar relaciones correctas (InvokeHTTP Response→Jolt; Original auto-terminada), reparar auto-terminación (ExecuteProducer, GetFile GPS), provenance API (búsqueda asíncrona). Procesadores en subgrupos vía `includeDescendantGroups=true`. Credenciales en `.env` (NIFI_USER, NIFI_PASS).

### 3.6 api/ (API REST Smart Grid)

- **Responsabilidad:** Exponer datos del Smart Grid vía REST con Swagger/OpenAPI para integración con otros sistemas.
- **Endpoints:** `/api/v1/subestaciones`, `/api/v1/lineas`, `/api/v1/pagerank`, `/api/v1/puntos-fallo`, `/api/v1/red`, `/health`.
- **Swagger UI:** `/docs` (puerto 8000 por defecto).
- **Integración:** Incluida en Fase 0 (arranque, parada, comprobación); enlaces en sidebar y Monitorización.

### 3.7 generar_informe_fases.py

- **Responsabilidad:** Generar informe consolidado de todas las fases KDD.
- **Contenido:** Estado servicios (Fase 0), HDFS/Kafka (Fase I), Cassandra (Fase II), Hive (Fase III), NiFi.
- **Salida:** `reports/informe_fases_*.md` y `informe_fases_latest.json`.

---

## 4. Flujos principales

### 4.1 Ciclo 15 minutos

1. producer.py genera datos (APIs o simulación).
2. Publica en Kafka y escribe backup HDFS.
3. procesamiento_grafos.py lee de HDFS, construye grafo, aplica autosanación.
4. Calcula PageRank y puntos de fallo.
5. Escribe en Cassandra y opcionalmente en Hive.
6. Dashboard recarga datos desde Cassandra y actualiza mapa e informe de cambios.

### 4.2 Arranque de servicios (Fase 0)

1. HDFS (start-dfs o start-all).
2. Kafka (kafka-server-start).
3. Cassandra (cassandra/bin/cassandra).
4. Creación de topics Kafka.
5. Aplicación de esquema Cassandra.
6. Aplicación de esquema Hive (setup_hive.hql).
7. Airflow (api-server en 8080 + scheduler).
8. API Swagger (uvicorn en 8000, /docs).

---

## 5. Consideraciones técnicas

### 5.1 Dependencias críticas

- **GraphFrames:** JAR para Spark 3.5; análisis de grafos.
- **spark-cassandra-connector:** Escritura/lectura Cassandra desde Spark.
- **cassandra-driver:** Conexión Python a Cassandra para dashboard.
- **six:** Requerido por cqlsh con Python 3.12+.

### 5.2 Configuración por entorno

| Variable | Uso por defecto | Descripción |
|----------|-----------------|-------------|
| KAFKA_BOOTSTRAP | localhost:9092 | Servidores Kafka |
| CASSANDRA_HOST | 127.0.0.1 | Host Cassandra |
| HDFS_DEFAULT_FS | hdfs://nodo1:9000 | FS por defecto |
| HIVE_DB | smart_grid_analytics | Base Hive |
| ELECTRICITY_MAPS_API_KEY | — | API Electricity Maps |
| API_WEATHER_KEY | — | API OpenWeather |
| NIFI_USER, NIFI_PASS | — | Credenciales NiFi (ver nifi-app.log o set-single-user-credentials) |
| AIRFLOW_USER, AIRFLOW_PASS | admin / — | Credenciales Airflow UI (ver simple_auth_manager_passwords.json.generated) |
| HIVE_UI_QUICK_HIVE_TIMEOUT_SEC | 25 | Timeout rápido (segundos) para consultas Hive desde cuadro de mando |
| HIVE_UI_QUICK_SPARK_TIMEOUT_SEC | 45 | Timeout rápido (segundos) para consultas spark-sql desde cuadro de mando |
| NIFI_USER / NIFI_PASS | — | Credenciales API NiFi (ver nifi-app.log o set-single-user-credentials) |
| AIRFLOW_USER / AIRFLOW_PASS | admin / (ver abajo) | Credenciales Airflow UI; 3.x: contraseña en `~/airflow/simple_auth_manager_passwords.json.generated` |

### 5.3 Scripts de apoyo

| Script | Función |
|--------|---------|
| scripts/iniciar_servicios.sh | Arranque HDFS, Kafka, Cassandra, Airflow (api-server + dag-processor + scheduler), calentamiento Hive (opción --only airflow) |
| scripts/parar_servicios.sh | Parada HDFS, Kafka, Cassandra, NiFi, Airflow (opción --only airflow) |
| scripts/comprobar_servicios.sh | Verificación de puertos y CLI |
| scripts/generar_informe_fases.py | Informe consolidado de todas las fases KDD |
| scripts/nifi_crear_flujo_fase1.py | Crear procesadores NiFi vía API |
| scripts/nifi_flujo_comprobar.py | Comprobar flujo NiFi, HDFS, Kafka |
| scripts/sync_dags_airflow.sh | Sincronizar DAGs a AIRFLOW_HOME |
| scripts/env_smart_grid.sh | PATH con cqlsh, hive, spark-sql |
| scripts/aplicar_esquema_cassandra.sh | Esquema Cassandra |
| scripts/aplicar_esquema_hive.sh | Esquema Hive con warehouse HDFS |
| scripts/fix_hive_metastore_derby_incompatible.sh | Corregir metastore Derby |
| scripts/instalar_hive_java21.sh | Instalación Hive 4.x |
| scripts/instalar_nifi_260.sh | Instalación NiFi 2.6.0 |

### 5.4 Configuración de Airflow (primera vez o reinstalación)

| Paso | Acción | Referencia |
|------|--------|------------|
| 1 | Sincronizar DAGs: `./scripts/sync_dags_airflow.sh` | Copia `orquestacion/dag_*.py` → `~/airflow/dags/` |
| 2 | Arrancar: `./scripts/iniciar_servicios.sh --only airflow` | Levanta api-server, dag-processor, scheduler |
| 3 | Obtener contraseña | Airflow 3.x: `~/airflow/simple_auth_manager_passwords.json.generated`; ver `docs/CREDENCIALES_UI.md` |
| 4 | Acceder a http://localhost:8080 | Usuario `admin`; contraseña del paso 3 |

**Nota:** En Airflow 3.x el **dag-processor** es obligatorio para que los DAGs aparezcan en la UI. Sin él se muestra "0 DAGs".

### 5.5 DAGs de Airflow

| DAG | Función |
|-----|---------|
| dag_arranque_servicios_smart_grid | Arranca HDFS, Kafka, Cassandra, Airflow y NiFi (espera puerto 8443) |
| dag_comprobar_servicios_smart_grid | Verifica servicios |
| dag_parar_servicios_smart_grid | Para servicios |
| dag_kdd_fase1_ingesta_smart_grid | Ejecuta producer.py |
| dag_kdd_fase2_procesamiento_smart_grid | spark-submit procesamiento_grafos.py |
| dag_kdd_fase3_validacion_smart_grid | Comprueba HDFS y NiFi |
| dag_consultas_hive_cassandra_smart_grid | Consultas ejemplo Hive y Cassandra |
| dag_informes_fases_smart_grid | Genera informe consolidado MD + JSON |
| dag_maestro_smart_grid | Pipeline cada 15 min (ingesta + procesamiento) |
| dag_mensual_retrain_limpieza_smart_grid | Limpieza HDFS + re-entrenamiento mensual |

### 5.6 Pipeline streaming PySpark (`procesamiento/smart_grid_streaming/`)

| Componente | Rol |
|--------------|-----|
| `schemas.py` | StructTypes para lecturas, maestro y eventos |
| `transformations.py` | Limpieza, broadcast JOIN, anomalías, ventanas con watermark, preparación upsert Cassandra |
| `tests/` | pytest + Spark local: limpieza, JOIN, anomalías, ventanas, Cassandra, E2E |

**Documentación unificada:** [STREAMING_PYSPARK_QA.md](STREAMING_PYSPARK_QA.md). **Frontend:** pestaña **Streaming & QA** en Streamlit.

### 5.7 Detección de riesgo de apagón (`procesamiento/deteccion_apagon/`)

| Elemento | Descripción |
|----------|-------------|
| `riesgo.py` | `risk_score`, componentes (voltaje, frecuencia, generación, cascada), umbral crítico |
| API | `GET /api/v1/riesgo-apagon` |
| UI | Panel bajo KPIs del mapa en `app_riesgo_apagon_panel.py` |
| Caso España 2025 | [APAGON_ESPANA_2025_CASO.md](APAGON_ESPANA_2025_CASO.md) |
