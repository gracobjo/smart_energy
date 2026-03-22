# Airflow: arranque y DAGs

Documentación de cómo arrancar Apache Airflow en este proyecto y qué hace cada DAG.

---

## Cómo arrancar Airflow

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

| Proceso      | Comando                      | Puerto |
|-------------|------------------------------|--------|
| API/Web     | `airflow api-server -p 8080` | 8080   |
| Scheduler   | `airflow scheduler`          | —      |

Ambos deben estar en ejecución para que los DAGs se ejecuten correctamente.

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

- Ejecuta `scripts/parar_servicios.sh`: para HDFS, Kafka, Cassandra, NiFi.
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

## DAGs en la instalación local (`~/airflow/dags`)

Si en tu máquina la carpeta de DAGs es `~/airflow/dags`, pueden existir DAGs adicionales (copias o variantes) que no están en el repositorio. Ejemplos típicos:

DAGs adicionales en instalaciones locales (`~/airflow/dags`) pueden coexistir; el pipeline oficial del repo es **Smart Grid** (`dag_maestro_smart_grid`, etc.).

## Problemas típicos y solución rápida

- **No aparecen todos los DAGs en la UI o en `airflow dags list`**:
  1. Verifica que `AIRFLOW_HOME` apunta a `~/airflow` y que los ficheros `dag_*.py` están en `~/airflow/dags`.
  2. Evita **enlaces simbólicos recursivos** en la carpeta de DAGs.
  3. Desde el proyecto, con el entorno virtual activado, vuelve a serializar los DAGs en la base de datos:

     ```bash

### Arranque de Airflow con Docker (opcional)

En máquinas justas de recursos se puede levantar solo Airflow en Docker, manteniendo HDFS/Kafka/Cassandra en el host o en otros contenedores.

1. Construir y arrancar el servicio de Airflow:

   ```bash
   cd ~/smart_energy
   docker compose -f docker-compose.airflow.yml up --build
   ```

2. Acceder a la interfaz web/API:

   - URL: http://localhost:8080
   - Usuario: `admin`
   - Contraseña: `admin` (creada automáticamente si no existe).

3. Detener el contenedor cuando no sea necesario:

   ```bash
   docker compose -f docker-compose.airflow.yml down
   ```

Los DAGs se leen desde la carpeta `orquestacion/` del proyecto, montada dentro del contenedor en `/opt/airflow/dags`.

     cd ~/smart_energy
     source venv_transporte/bin/activate
     export AIRFLOW_HOME=~/airflow
     airflow dags reserialize
     ```

  4. Espera unos segundos, ejecuta `airflow dags list` y refresca la web de Airflow (F5).

- **Quiero ver los logs del api-server en la terminal**:
  - Lanza el servidor **sin `-D`** para que no vaya a segundo plano:

    ```bash
    airflow api-server -H 0.0.0.0 -p 8080
    ```

    Esa terminal quedará ocupada mostrando los logs hasta que pulses `Ctrl+C`.

---

## Enlaces rápidos

- Interfaz web: http://localhost:8080  
- DAGs del proyecto: `orquestacion/dag_*.py`  
- Configuración Airflow: `$AIRFLOW_HOME/airflow.cfg`  
- Logs: `$AIRFLOW_HOME/logs/`
