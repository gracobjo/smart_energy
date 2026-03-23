# Airflow: arranque y DAGs

Documentación de cómo arrancar Apache Airflow en este proyecto y qué hace cada DAG.

---

## Cómo arrancar Airflow

### Arranque integrado (recomendado)

Airflow forma parte del entorno de ejecución. Para arrancarlo junto al resto de servicios:

```bash
cd ~/smart_energy
./scripts/iniciar_servicios.sh          # arranca HDFS, Kafka, Cassandra, Airflow
./scripts/iniciar_servicios.sh --only airflow   # solo Airflow
```

Para parar Airflow: `./scripts/parar_servicios.sh --only airflow`.

También puedes arrancar todo desde el **dashboard** (Fase 0 → Arrancar servicios) o desde el DAG `dag_arranque_servicios_smart_grid`.

### Arranque manual

En este proyecto Airflow está instalado en el **entorno virtual** del proyecto. Hay que levantar **dos procesos**: el servidor de API (interfaz web + API para los workers) y el scheduler.

### Requisitos previos

- Variable de entorno `AIRFLOW_HOME` (por defecto suele ser `~/airflow`).
- Carpeta de DAGs configurada en `AIRFLOW_HOME/dags` o enlazada al proyecto (ver más abajo).

### 1. Activar entorno y variables

```bash
cd ~/smart_energy
source venv/bin/activate   # o venv_transporte
export AIRFLOW_HOME=~/airflow
```

### 2. Arrancar el servidor de API (interfaz web + API)

En versiones recientes de Airflow el comando `webserver` fue sustituido por **`api-server`**:

```bash
airflow api-server -p 8080
```

O escuchando en todas las interfaces:

```bash
airflow api-server -H 0.0.0.0 -p 8080
```

**Importante:** Este proceso debe estar en marcha para que el **scheduler** pueda ejecutar tareas. Los workers del LocalExecutor se conectan al API en `http://localhost:8080`; si el api-server no está levantado, las tareas fallan con *Connection refused*.

### 3. Arrancar el scheduler (en otra terminal)

En una **segunda terminal**:

```bash
cd ~/smart_energy
source venv/bin/activate
export AIRFLOW_HOME=~/airflow
airflow scheduler
```

### 4. Acceder a la interfaz

- **URL:** http://localhost:8080 (o http://&lt;IP&gt;:8080 si accedes desde otra máquina)
- **Usuario:** `admin`
- **Contraseña:** `admin` (crear con `airflow users create` si no existe)
- Ver `docs/CREDENCIALES_UI.md` para más detalles

### Resumen de comandos

| Proceso        | Comando                        | Puerto |
|----------------|--------------------------------|--------|
| API/Web        | `airflow api-server -p 8080`   | 8080   |
| DAG Processor  | `airflow dag-processor`        | —      |
| Scheduler      | `airflow scheduler`            | —      |

**Airflow 3.x:** El **dag-processor** es obligatorio para que los DAGs aparezcan en la UI. Sin él verás "0 DAGs" aunque los archivos estén en `~/airflow/dags`. El script `iniciar_servicios.sh` arranca los tres procesos.

---

## DAGs en el repositorio (`orquestacion/`)

Estos DAGs están en la carpeta **`orquestacion/`** del proyecto. Para que Airflow los cargue puedes:

- Enlazar cada DAG en `~/airflow/dags/`, o  
- Configurar en `~/airflow/airflow.cfg`: `dags_folder` apuntando a `.../smart_energy/orquestacion` (o copiar DAGs a `~/airflow/dags`).

### 1. `dag_arranque_servicios_smart_grid` (`dag_arranque_servicios.py`)

- **Propósito:** Levantar los servicios necesarios antes del pipeline (HDFS, Cassandra, Kafka).
- **Ejecución:** Manual (Trigger DAG). No tiene schedule.
- **Tareas:**
  - **arrancar_hdfs:** Si el NameNode (puerto 9870) no responde, ejecuta `start-dfs.sh` (usa `HADOOP_HOME` si está definido).
  - **arrancar_cassandra:** Si el puerto 9042 no está en uso, lanza `cassandra/bin/cassandra` del proyecto en segundo plano.
  - **arrancar_kafka:** Si el puerto 9092 no está en uso, arranca el broker Kafka (usa `KAFKA_HOME`, por defecto `/opt/kafka`).
- Las tres tareas se ejecutan en paralelo. Si un servicio ya está activo, la tarea no hace nada.

### 2. `dag_maestro_smart_grid` (`dag_maestro.py`)

- **Propósito:** Cada 15 min: verificar HDFS/Kafka/Cassandra → **`producer.py`** → **`procesamiento/procesamiento_grafos.py`**.
- **Tareas:** mismas verificaciones; ingesta = productor Smart Grid (Electricity Maps + OpenWeather → Kafka).

### 3. `dag_mensual_retrain_limpieza_smart_grid` (`dag_mensual_retrain_limpieza.py`)

- **Limpieza HDFS** (`energy_*` antiguos) → **re-entrenamiento grafos** → **`modelo_respaldo_energia.py`** (umbrales respaldo + Cassandra `modelo_respaldo`).

### 4. `dag_comprobar_servicios_smart_grid` (`dag_comprobar_servicios.py`)

- Ejecuta `scripts/comprobar_servicios.sh`: verifica HDFS, Kafka, Cassandra, NiFi, Airflow.
- Manual (Trigger DAG).

### 5. `dag_parar_servicios_smart_grid` (`dag_parar_servicios.py`)

- Ejecuta `scripts/parar_servicios.sh`: para HDFS, Kafka, Cassandra, NiFi, Airflow.
- Manual (al cerrar demo).

### 6. DAGs KDD (por fase)

- **`dag_kdd_fase1_ingesta_smart_grid`**: ejecuta `producer.py` (ingesta → Kafka + HDFS).
- **`dag_kdd_fase2_procesamiento_smart_grid`**: ejecuta `spark-submit procesamiento_grafos.py` (→ Cassandra + Hive).
- **`dag_kdd_fase3_validacion_smart_grid`**: comprueba HDFS y flujo NiFi.

### 7. `dag_consultas_hive_cassandra_smart_grid` (`dag_consultas_hive_cassandra.py`)

- Ejecuta consultas de ejemplo a Hive (SHOW DATABASES) y Cassandra (DESCRIBE KEYSPACE).

### 8. `dag_informes_fases_smart_grid` (`dag_informes_fases.py`)

- Genera informe consolidado de todas las fases KDD (servicios, ingesta HDFS/Kafka, Cassandra, Hive, NiFi).
- Salida: `reports/informe_fases_YYYYMMDD_HHMMSS.md` y `reports/informe_fases_latest.json`.

---

---

## Guía paso a paso: primera vez o reinstalación

### 1. Sincronizar DAGs

```bash
cd ~/smart_energy
export AIRFLOW_HOME=~/airflow
./scripts/sync_dags_airflow.sh
```

Verifica: `ls ~/airflow/dags/dag_*.py` — deben aparecer 10 archivos.

### 2. Arrancar Airflow (tres procesos)

```bash
./scripts/parar_servicios.sh --only airflow   # por si estaba en marcha
./scripts/iniciar_servicios.sh --only airflow
```

El script arranca: **api-server** (8080), **dag-processor** (obligatorio en Airflow 3.x para que aparezcan DAGs), **scheduler**.

### 3. Obtener contraseña

Si es la primera vez, consulta `docs/CREDENCIALES_UI.md`. Resumen:

- **SimpleAuthManager (default 3.x):** `cat ~/airflow/simple_auth_manager_passwords.json.generated`
- **Opcional:** desactivar auth con `simple_auth_manager_all_admins = True` en `airflow.cfg`

### 4. Acceder a la UI

- URL: http://localhost:8080 (o http://&lt;IP&gt;:8080)
- Usuario: `admin`
- Contraseña: la obtenida en el paso 3

### 5. Comprobar que los DAGs aparecen

Tras 30–60 segundos, la UI debe mostrar los 10 DAGs. Si no: ver *Problemas típicos* abajo.

---

## DAGs en la instalación local (`~/airflow/dags`)

Si en tu máquina la carpeta de DAGs es `~/airflow/dags`, pueden existir DAGs adicionales (copias o variantes) que no están en el repositorio. El pipeline oficial del repo es **Smart Grid** (`dag_maestro_smart_grid`, etc.).

---

## Problemas típicos y solución rápida

### No aparecen DAGs (0 DAGs) o faltan algunos

**Causa habitual en Airflow 3.x:** falta el proceso **dag-processor**.

1. **Verificar que dag-processor está en marcha:**
   ```bash
   pgrep -f "airflow dag-processor"
   ```
   Si no devuelve un PID, arrancar:
   ```bash
   cd ~/smart_energy && source venv/bin/activate
   export AIRFLOW_HOME=~/airflow
   nohup airflow dag-processor >> /tmp/smart_grid_airflow_dag_processor.log 2>&1 &
   ```

2. **Sincronizar DAGs** (si faltan o están desactualizados):
   ```bash
   ./scripts/sync_dags_airflow.sh
   ```

3. **Verificar que los archivos están en `~/airflow/dags`:**
   ```bash
   ls ~/airflow/dags/dag_*.py
   ```

4. **Log del dag-processor:**
   ```bash
   tail -50 /tmp/smart_grid_airflow_dag_processor.log
   ```
   Debe mostrar "Creating ORM DAG for dag_..." para cada DAG.

5. Esperar 30–60 s y refrescar la UI (F5).

### 401 Unauthorized / contraseña incorrecta

Ver `docs/CREDENCIALES_UI.md` — en Airflow 3.x la contraseña por defecto no es `admin`; está en `simple_auth_manager_passwords.json.generated` o en el log del api-server.

### Ver logs en la terminal

Para ver el api-server en primer plano (logs en pantalla):

```bash
airflow api-server -H 0.0.0.0 -p 8080
```

Logs del proyecto: `/tmp/smart_grid_airflow_api.log`, `/tmp/smart_grid_airflow_dag_processor.log`, `/tmp/smart_grid_airflow_scheduler.log`.

---

## Arranque de Airflow con Docker (opcional)

En máquinas justas de recursos se puede levantar solo Airflow en Docker:

```bash
cd ~/smart_energy
docker compose -f docker-compose.airflow.yml up --build
```

- URL: http://localhost:8080
- Usuario/contraseña: según configuración del compose (a menudo `admin`/`admin`)

Los DAGs se leen desde `orquestacion/` montada en el contenedor.

---

## Enlaces rápidos

- Interfaz web: http://localhost:8080  
- DAGs del proyecto: `orquestacion/dag_*.py`  
- Sincronizar DAGs: `./scripts/sync_dags_airflow.sh`  
- Configuración Airflow: `$AIRFLOW_HOME/airflow.cfg`  
- Credenciales: `docs/CREDENCIALES_UI.md`  
- Logs: `$AIRFLOW_HOME/logs/`, `/tmp/smart_grid_airflow_*.log`
