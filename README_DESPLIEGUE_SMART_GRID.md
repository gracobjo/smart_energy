# Despliegue — Smart Grid

Stack: **HDFS**, **Kafka (KRaft)**, **Spark 3.5** (GraphFrames), **Hive**, **Cassandra 5.0**, **Airflow 2.10.x**.

Ruta del proyecto: ajusta `cd` a tu clon (p. ej. `~/smart_energy`).

---

## Cassandra

```bash
cd ~/smart_energy
./cassandra/bin/cassandra   # o ruta de tu instalación
# Esperar 30–60 s
nc -z 127.0.0.1 9042 && echo OK
cqlsh -f cassandra/esquema_smart_grid.cql
```

### Consultas útiles (keyspace `smart_grid`)

```bash
cqlsh -e "USE smart_grid; SELECT id_subestacion, voltaje_kv, potencia_mw, estado FROM subestaciones_estado LIMIT 10;"
cqlsh -e "USE smart_grid; SELECT id_subestacion, es_articulacion, fragmentos_al_fallar FROM puntos_fallo_unicos WHERE es_articulacion = true ALLOW FILTERING;"
```

---

## Kafka

```bash
kafka-topics.sh --create --topic energy_raw --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
kafka-topics.sh --create --topic weather_raw --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
```

---

## JARs (Spark)

Variables opcionales:

```bash
export JAR_GRAPHFRAMES=/ruta/graphframes-0.8.3-spark3.5-s_2.12.jar
export JAR_CASSANDRA=/ruta/spark-cassandra-connector_2.12-3.5.0.jar
```

Ver `config.py`.

---

## Flujo manual

```bash
source venv/bin/activate   # o venv_transporte
pip install -r requirements.txt

python producer.py
python procesamiento/procesamiento_grafos.py
streamlit run app_visualizacion.py
```

---

## Airflow

Copiar `orquestacion/dag_maestro.py`, `dag_arranque_servicios.py`, `dag_mensual_retrain_limpieza.py` al directorio `dags/` de Airflow.

En `airflow.cfg`: `dags_folder` apuntando a la carpeta que contiene esos DAGs (o enlace a `orquestacion/`).

Ver **[docs/AIRFLOW.md](docs/AIRFLOW.md)** (actualizar rutas a `smart_energy` si aplica).

---

## Hive

Inicialización:

```bash
hive -f setup_hive.hql
```

Post-ingesta con Spark:

```bash
python procesamiento/persistir_hive_ingesta.py --energy /tmp/smart_grid_last_energy.json --weather /tmp/smart_grid_last_weather.json
```

---

## Troubleshooting

- **Cassandra no arranca:** revisar `cassandra/logs/` y memoria JVM.
- **Spark falla al leer HDFS:** `hdfs dfs -mkdir -p /user/hadoop/energy_backup`
- **Sin datos en dashboard:** ejecutar al menos un ciclo `producer` + `procesamiento_grafos`.

---

## APIs

Configuración de **Electricity Maps** y **OpenWeather**: **[docs/API_INTEGRACION.md](docs/API_INTEGRACION.md)**.
