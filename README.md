# Smart Grid — Monitoreo de redes de energía inteligentes (España)

Sistema **Big Data** basado en el ciclo **KDD** y arquitectura **Lambda/Kappa**: ingesta con **Python** (Kafka), procesamiento **Spark 3.5** (GraphFrames, streaming 15 min), **Cassandra 5.0** (estado en tiempo real), **Hive** (histórico y sostenibilidad), **Airflow 2.10.x** (orquestación).

---

## Funcionalidades

| Módulo | Descripción |
|--------|-------------|
| **Ingesta** | `producer.py`: **Electricity Maps** (carbono / mix renovable), **OpenWeather** en zonas solares/eólicas; Kafka `energy_raw` + `weather_raw`; backup HDFS. |
| **Procesamiento** | Grafos: subestaciones y líneas; **puntos de fallo únicos** (articulación); PageRank; persistencia Cassandra. |
| **Streaming** | Ventanas **15 min**: carga media de la red, picos de demanda (`streaming_ventanas_15min.py`). |
| **Hive** | Consumo, líneas, **sostenibilidad** (`sostenibilidad_carbono_hist`), **clima renovables** (`clima_renovables_hist`). |
| **Dashboard** | Streamlit: mapa, voltaje/potencia, alertas, PageRank, cuadro de mando. |
| **NiFi** | Ingesta alternativa: APIs (OpenWeather), ExecuteStreamCommand (producer.py), logs GPS → Kafka. Ver `docs/NIFI_INTEGRACION.md`. |
| **Airflow** | DAG maestro (ingesta + batch), mensual (limpieza HDFS, grafos, modelo respaldo). |

**APIs:** ver **[docs/API_INTEGRACION.md](docs/API_INTEGRACION.md)** (Electricity Maps + OpenWeather). **API REST propia (Swagger):** [docs/API_SWAGGER.md](docs/API_SWAGGER.md) — exponer datos a otros sistemas.

---

## Flujo de datos

```
producer.py → Kafka (energy_raw, weather_raw) + HDFS
       ↓
procesamiento_grafos.py → Cassandra (subestaciones_estado, lineas_estado, puntos_fallo_unicos, …)
       ↓
app_visualizacion.py ← Cassandra
```

Opcional tras ingesta: **`PERSIST_HIVE_AFTER_INGEST=1`** → `persistir_hive_ingesta.py` escribe tablas de sostenibilidad y clima renovable en Hive.

---

## Estructura principal

| Ruta | Rol |
|------|-----|
| `config.py` | Kafka, Cassandra, Hive, claves API |
| `config_nodos.py` | Subestaciones y líneas |
| `config_plantas_renovables.py` | Zonas solares/eólicas (OpenWeather) |
| `producer.py` | Ingesta principal |
| `procesamiento/procesamiento_grafos.py` | Spark + GraphFrames |
| `procesamiento/streaming_ventanas_15min.py` | Structured Streaming |
| `procesamiento/persistir_hive_ingesta.py` | Hive desde JSON energy/weather |
| `persistencia_hive.py` | Histórico subestaciones/consumo |
| `cassandra/esquema_smart_grid.cql` | Esquema Cassandra |
| `setup_hive.hql` | Esquema Hive |
| `orquestacion/` | DAGs Airflow |
| `app_visualizacion.py` | Dashboard Streamlit |

---

## Arranque rápido

```bash
cd /ruta/smart_energy
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Cassandra
cqlsh -f cassandra/esquema_smart_grid.cql

# Kafka (instalación local en /opt/kafka)
./scripts/instalar_kafka_local.sh
# O manual: arrancar kafka-server-start.sh y crear topics energy_raw, weather_raw

# Claves (recomendado por entorno)
export ELECTRICITY_MAPS_API_KEY="..."   # opcional
export API_WEATHER_KEY="..."

python producer.py
python procesamiento/procesamiento_grafos.py
streamlit run app_visualizacion.py
```

**Hive post-ingesta (opcional):**

```bash
export PERSIST_HIVE_AFTER_INGEST=1
python producer.py
# o manual:
python procesamiento/persistir_hive_ingesta.py --energy /tmp/smart_grid_last_energy.json --weather /tmp/smart_grid_last_weather.json
```

---

## Documentación

| Documento | Contenido |
|-----------|-----------|
| [docs/ESPECIFICACION_REQUISITOS.md](docs/ESPECIFICACION_REQUISITOS.md) | Requisitos funcionales y no funcionales |
| [docs/DISENO.md](docs/DISENO.md) | Diseño del sistema, arquitectura, modelo de datos |
| [docs/CASOS_USO.md](docs/CASOS_USO.md) | Casos de uso con escenarios normales y alternativos |
| [docs/DIAGRAMAS_UML.md](docs/DIAGRAMAS_UML.md) | Diagramas UML (Mermaid): casos de uso, secuencia, componentes, etc. |
| [docs/NIFI_INTEGRACION.md](docs/NIFI_INTEGRACION.md) | NiFi 2.6.0: instalación, flujos, API + GPS |
| [docs/API_INTEGRACION.md](docs/API_INTEGRACION.md) | Electricity Maps y OpenWeather |
| [README_DESPLIEGUE_SMART_GRID.md](README_DESPLIEGUE_SMART_GRID.md) | Servicios, Cassandra, Kafka, troubleshooting |
| [docs/AIRFLOW.md](docs/AIRFLOW.md) | DAGs y carpeta `dags` |
| [docs/FLUJO_DATOS_Y_REQUISITOS.md](docs/FLUJO_DATOS_Y_REQUISITOS.md) | Flujo técnico Smart Grid |
| [AGENTS.md](AGENTS.md) | Convenciones para varios agentes |

---

## Licencia y secretos

No incluir **API keys** en el repositorio; usar variables de entorno o secretos en producción.
