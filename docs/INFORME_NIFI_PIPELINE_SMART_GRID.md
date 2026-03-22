# Informe final: Pipeline Smart Grid con NiFi API

**Proyecto:** Sistema de Monitoreo de Redes de Energía Inteligentes (Smart Grid)  
**Fecha:** Marzo 2026  
**Alcance:** Documentación de funcionalidades implementadas, verificación del pipeline de ingesta, y justificación de procesadores NiFi.

---

## 1. Resumen ejecutivo

Se ha implementado y documentado un pipeline de datos para Smart Grid que integra:

- **NiFi 2.6.0** como orquestador de ingesta (API REST)
- **Kafka** (KRaft) para streaming de energía, clima y GPS
- **HDFS** para backup raw y procesamiento batch
- **Spark** (GraphFrames) para análisis de grafos y detección de nodos críticos
- **Cassandra** para estado en tiempo real
- **Hive** para histórico y reportes

---

## 2. Funcionalidades implementadas

### 2.1 Scripts y herramientas

| Archivo | Función |
|---------|---------|
| `scripts/nifi_crear_flujo_fase1.py` | Crea procesadores NiFi Fase 1 via API: GenerateFlowFile, ExecuteStreamCommand (producer), y conexión entre ambos |
| `scripts/nifi_flujo_comprobar.py` | Comprueba flujo NiFi (procesadores, conexiones, colas), HDFS, Kafka; opción `--start` para arrancar procesadores STOPPED |
| `scripts/iniciar_servicios` / `scripts/iniciar_servicios.sh` | Arranca HDFS, Kafka (KRaft), Cassandra, y calienta catálogo Hive |
| `scripts/verificar_nifi.sh` | Verifica instalación y estado de NiFi 2.6.0 |

### 2.2 Mejoras en `nifi_flujo_comprobar.py`

- **Opción `--start` / `-s`**: arranca automáticamente los procesadores en estado STOPPED mediante `PUT /processors/{id}/run-status` con `state: RUNNING`
- **Comprobación Kafka**: usa `nc` para puerto 9092 y `kafka-topics.sh --list` para listar topics (evita bloqueo de `kafka-console-consumer`)
- **Nota cuando hay procesadores STOPPED**: sugiere ejecutar `python scripts/nifi_flujo_comprobar.py --start`

### 2.3 Flow definition JSON

- **`nifi/smart_grid_flow_definition.json`**: definición completa de flujo importable desde la UI de NiFi
- Incluye: Parameter Context, procesadores (Trigger, Producer, InvokeHTTP, PutHDFS, PublishKafka, GetFile, ExecuteSpark), conexiones, Controller Service Kafka

---

## 3. Procesadores NiFi creados vía API (Fase 1)

### 3.1 Procesadores creados por `nifi_crear_flujo_fase1.py`

| Procesador | Tipo | Configuración principal | Relaciones |
|------------|------|-------------------------|------------|
| **NiFi_F1_GenerateTrigger** | `GenerateFlowFile` | `generate-ff-custom-text`: "trigger", Batch Size: 1 | success → ExecuteProducer |
| **NiFi_F1_ExecuteProducer** | `ExecuteStreamCommand` | Command: python3, Args: producer.py, Working Dir: BASE_PATH | Recibe de GenerateTrigger |

**Conexión:** `GenerateTrigger (success)` → `ExecuteProducer`

### 3.2 Configuraciones detalladas

**GenerateFlowFile:**
- Genera un FlowFile con texto "trigger" cada ejecución
- Por defecto, scheduling depende de la configuración del procesador en NiFi (no se especifica en el script; el flow definition JSON usa `15 min`)
- Permite disparar la ingesta de forma periódica sin cron externo

**ExecuteStreamCommand:**
- Ejecuta `python3 producer.py` en el directorio del proyecto
- `producer.py` hace: Electricity Maps, OpenWeather, simulación de subestaciones, publicación a Kafka (`energy_raw`, `weather_raw`) y escritura en HDFS
- Auto-termina `output-stream` y `stderr` para no acumular FlowFiles de salida estándar

---

## 4. Procesadores del flow definition completo (importable)

El archivo `nifi/smart_grid_flow_definition.json` define un flujo más completo:

| Procesador | Función |
|------------|---------|
| **TriggerIngesta** | GenerateFlowFile cada 15 min |
| **ExecuteProducer** | producer.py (Electricity Maps + OpenWeather + simulación + Kafka + HDFS) |
| **InvokeHTTP_OpenWeather** | API OpenWeather (alternativa solo clima) |
| **PutHDFS_weather_raw** | Escribe raw weather en HDFS |
| **PublishKafka_weather_raw** | Publica en topic `weather_raw` |
| **GetFile_GPS** | Lee logs GPS de `data/gps_logs/` |
| **PublishKafka_gps_raw** | Publica en topic `gps_raw` |
| **ExecuteSpark_ProcesamientoGrafos** | spark-submit procesamiento_grafos.py → Cassandra + Hive |
| **ExecuteSpark_PersistirHive** | persistir_hive_ingesta.py (opcional, DISABLED) |

**Controller Service:** `KafkaConnService_SmartGrid` (Kafka3ConnectionService) con `bootstrap.servers=#{KAFKA_BOOTSTRAP}#`

---

## 5. Por qué estos procesadores y no otros

### 5.1 GenerateFlowFile en lugar de Cron / Timer externo

- **Motivo:** NiFi gestiona el ciclo de vida y el scheduling dentro del flujo
- **Ventaja:** No requiere cron del sistema; el disparo está integrado en el canvas
- **Alternativa descartada:** Cron + curl a un endpoint: añade complejidad y dependencias externas

### 5.2 ExecuteStreamCommand para producer.py

- **Motivo:** `producer.py` ya implementa la lógica completa (APIs, simulación, Kafka, HDFS) en Python
- **Ventaja:** Reutiliza código existente sin reescribir en NiFi
- **Alternativa descartada:** InvokeHTTP + JoltTransformJSON + PublishKafka para cada API: duplicaría lógica y no cubre Electricity Maps ni la simulación de subestaciones

### 5.3 Flujo simplificado Fase 1 (solo Trigger + Producer)

- **Motivo:** Demostración mínima que funciona con la API de NiFi sin depender de Controller Services adicionales (Kafka Connection Service)
- **Ventaja:** `producer.py` publica a Kafka y escribe en HDFS por sí mismo; no se requiere PublishKafka de NiFi para la ingesta básica
- **Ampliación:** El flow definition JSON añade InvokeHTTP, GetFile, PutHDFS, PublishKafka para flujos alternativos

### 5.4 InvokeHTTP + PutHDFS + PublishKafka (en flow definition)

- **Motivo:** Permite ingesta solo de OpenWeather sin ejecutar `producer.py`
- **Uso:** Cuando se quiere clima en Kafka/HDFS sin Electricity Maps ni simulación
- **Alternativa:** ExecuteStreamCommand con un script ligero solo de OpenWeather; InvokeHTTP es más declarativo

### 5.5 GetFile para GPS

- **Motivo:** Patrón estándar para ingesta por lotes desde directorio
- **Ventaja:** Keep Source File: true evita pérdida de datos; PublishKafka envía a `gps_raw`

### 5.6 ExecuteSpark para procesamiento

- **Motivo:** Spark (GraphFrames) requiere JVM y dependencias; NiFi no ejecuta Spark nativamente
- **Ventaja:** ExecuteStreamCommand con `spark-submit` mantiene el pipeline en NiFi de forma visual

---

## 6. Pipeline de datos (flujo completo)

```
                    ┌─────────────────────────────────────────┐
                    │  TriggerIngesta (GenerateFlowFile)      │
                    │  Cada 15 min                            │
                    └─────────────┬───────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────────────────┐
                    │  ExecuteProducer (producer.py)          │
                    │  - Electricity Maps (intensidad CO2)    │
                    │  - OpenWeather (zonas renovables)       │
                    │  - Simulación carga/voltaje             │
                    └──┬─────────────────────┬────────────────┘
                       │                     │
                       ▼                     ▼
              ┌──────────────┐      ┌──────────────────┐
              │ Kafka        │      │ HDFS             │
              │ energy_raw   │      │ energy_backup/   │
              │ weather_raw  │      │ energy_*.json    │
              └──────────────┘      └────────┬─────────┘
                                             │
                                             ▼
                    ┌─────────────────────────────────────────┐
                    │  procesamiento_grafos.py (Spark)        │
                    │  - Lee HDFS                             │
                    │  - GraphFrames (PageRank, articulación)  │
                    │  - persistencia_hive.py                 │
                    └──┬─────────────────────┬────────────────┘
                       │                     │
                       ▼                     ▼
              ┌──────────────┐      ┌──────────────────┐
              │ Cassandra    │      │ Hive             │
              │ subestaciones│      │ smart_grid_      │
              │ lineas_estado│      │ analytics        │
              │ pagerank_*   │      │ (histórico)      │
              │ puntos_fallo │      │                  │
              └──────────────┘      └──────────────────┘
```

---

## 7. Verificación del pipeline (estado actual)

### 7.1 Servicios

| Servicio | Puerto | Estado verificado |
|----------|--------|-------------------|
| HDFS | 9870, 9000 | OK |
| Kafka | 9092 | OK (topics: energy_raw, weather_raw, datos_crudos, etc.) |
| Cassandra | 9042 | OK |
| NiFi | 8443 | OK |

### 7.2 Ingesta

| Componente | Estado | Notas |
|------------|--------|-------|
| **HDFS** | OK | 10 ficheros en `/user/hadoop/energy_backup/` (energy_*.json) |
| **Kafka (producer.py)** | Requiere `kafka-python` | Sin `pip install kafka-python`, producer escribe a HDFS pero no a Kafka |
| **Topics Kafka** | Existentes | energy_raw, weather_raw, gps_raw, datos_crudos, datos_filtrados |

### 7.3 Procesamiento Spark

| Componente | Estado | Notas |
|------------|--------|-------|
| **procesamiento_grafos.py** | Requiere dependencias | `graphframes`, JARs (GraphFrames, spark-cassandra-connector); ejecutar con `spark-submit --packages` |
| **Cassandra** | Esquema requerido | `cqlsh -f cassandra/esquema_smart_grid.cql` |
| **Hive** | Depende de Hive/Spark | `persistencia_hive.py` usa `ejecutar_persistencia_hive()` desde procesamiento_grafos |

### 7.4 NiFi

| Componente | Estado | Notas |
|------------|--------|-------|
| Procesadores Fase 1 | Creados | GenerateTrigger, ExecuteProducer, conexión |
| GenerateTrigger | RUNNING | Genera flowfiles |
| ExecuteProducer | STOPPED | Se detiene si producer.py falla (ej. sin kafka-python); arrancar con `--start` |
| Cola | 13 ff | Flowfiles pendientes entre Trigger y Producer |

---

## 8. API NiFi utilizada

| Endpoint | Método | Uso |
|----------|--------|-----|
| `/access/token` | POST | Autenticación (username/password) |
| `/flow/process-groups/root` | GET | Obtener ID del grupo raíz y flujo |
| `/process-groups/{id}/processors` | POST | Crear procesador |
| `/process-groups/{id}/connections` | POST | Crear conexión |
| `/processors/{id}` | GET | Obtener detalle y revision |
| `/processors/{id}/run-status` | PUT | Cambiar estado (RUNNING/STOPPED) |
| `/flow/process-groups/{id}/status` | GET | Estado de conexiones y colas |
| `/provenance` | POST | Búsqueda de eventos (opcional) |

---

## 9. Requisitos para pipeline completo operativo

1. **Dependencias Python:** `pip install kafka-python` (para que producer.py publique a Kafka)
2. **Spark:** `pip install pyspark graphframes` o uso de `spark-submit` con `--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12`
3. **Cassandra:** aplicar esquema: `cqlsh -f cassandra/esquema_smart_grid.cql`
4. **NiFi ExecuteProducer:** arrancar con `python scripts/nifi_flujo_comprobar.py --start` tras asegurar que producer.py funciona
5. **Credenciales NiFi:** `NIFI_USER` y `NIFI_PASS` en .env o variables de entorno (ver `nifi-app.log` en instalación nueva)

---

## 10. Comandos de referencia

```bash
# Arrancar servicios base
./iniciar_servicios

# Crear procesadores NiFi Fase 1
NIFI_USER=xxx NIFI_PASS=xxx python scripts/nifi_crear_flujo_fase1.py

# Comprobar y arrancar procesadores
python scripts/nifi_flujo_comprobar.py --start

# Ver estado del flujo
python scripts/nifi_flujo_comprobar.py

# Ingesta manual (sin NiFi)
python producer.py

# Procesamiento Spark (con dependencias instaladas)
/opt/spark/bin/spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
  procesamiento/procesamiento_grafos.py
```

---

## 11. Conclusiones

- Los **procesadores NiFi Fase 1** (GenerateFlowFile + ExecuteStreamCommand) permiten orquestar la ingesta de forma visual y periódica mediante la API REST.
- La elección de **ExecuteStreamCommand** para `producer.py` evita duplicar lógica y aprovecha el script Python existente.
- El **flow definition JSON** define un flujo ampliado con OpenWeather, GPS, PutHDFS, PublishKafka y Spark, importable desde la UI.
- El pipeline **HDFS → Spark → Cassandra + Hive** está implementado; la ingesta a **Kafka** desde producer.py requiere `kafka-python`.
- La documentación y los scripts de comprobación facilitan el diagnóstico y el mantenimiento del sistema.
