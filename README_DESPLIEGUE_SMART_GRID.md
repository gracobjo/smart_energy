# Despliegue — Smart Grid

Stack: **HDFS**, **Kafka (KRaft)**, **Spark 3.5** (GraphFrames), **Hive**, **Cassandra 5.0**, **Airflow 2.10.x**.

Ruta del proyecto: ajusta `cd` a tu clon (p. ej. `~/smart_energy`).

---

## Cassandra

Arranque integrado con el resto del stack (HDFS + Kafka + Cassandra + Airflow desde la raíz del repo):

```bash
cd ~/smart_energy
./iniciar_servicios              # o: ./scripts/iniciar_servicios.sh
./scripts/iniciar_servicios.sh --only cassandra   # solo Cassandra
./scripts/iniciar_servicios.sh --only airflow     # solo Airflow (api-server + scheduler)
```

Solo Cassandra (manual):

```bash
cd ~/smart_energy
./cassandra/bin/cassandra   # o ruta de tu instalación
# Esperar 30–60 s
nc -z 127.0.0.1 9042 && echo OK
./cassandra/bin/cqlsh -f cassandra/esquema_smart_grid.cql
# Si `cqlsh: no se encontró la orden`, usa la ruta anterior o:
# ./scripts/aplicar_esquema_cassandra.sh
```

**Java 21 en el sistema (Hive/Spark) y Cassandra 4.x:** además de comentar **`UseBiasedLocking`** en `jvm-server.options` (script **`./scripts/patch_cassandra_java21_jvm.sh`**), Cassandra necesita **JDK 17 u 11** para el proceso: `cassandra/conf/cassandra-env.sh` en el repo fuerza **`JAVA_HOME`** a OpenJDK 17 y reasigna **`JAVA`** (si no, `cassandra.in.sh` usa `java` del PATH = 21 y falla con *Security Manager*). Instala en Ubuntu: `sudo apt install openjdk-17-jdk`.

**cqlsh / `No module named 'six.moves'`:** con **Python 3.12+**, el **`six` 1.12** del tarball en `cassandra/lib/six-*.zip` es incompatible (cqlsh lo mete en `sys.path` antes que el del sistema). **Solución:** ejecuta **`./scripts/fix_cqlsh_six_python312.sh`** (descarga `six` 1.16 y sustituye el zip). Si aún falla: `python3 -m pip install --user 'six>=1.16'` o **`./scripts/cqlsh_local.sh`** (solo ayuda si el fallo era falta de `six` en el Python del PATH, no el zip viejo).

### Consultas útiles (keyspace `smart_grid`)

```bash
./cassandra/bin/cqlsh -e "USE smart_grid; SELECT id_subestacion, voltaje_kv, potencia_mw, estado FROM subestaciones_estado LIMIT 10;"
./cassandra/bin/cqlsh -e "USE smart_grid; SELECT id_subestacion, es_articulacion, fragmentos_al_fallar FROM puntos_fallo_unicos WHERE es_articulacion = true ALLOW FILTERING;"
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

Airflow está integrado en el entorno de ejecución: `./scripts/iniciar_servicios.sh` arranca el api-server (puerto 8080) y el scheduler. Para pararlo: `./scripts/parar_servicios.sh --only airflow`.

Copiar DAGs de `orquestacion/` al directorio `dags/` de Airflow (o usar `./scripts/sync_dags_airflow.sh`).

Ver **[docs/AIRFLOW.md](docs/AIRFLOW.md)** y **[docs/CREDENCIALES_UI.md](docs/CREDENCIALES_UI.md)** (credenciales admin/admin).

---

## Hive

**Java 21 + Hadoop:** `hive` y `schematool` invocan `$HADOOP_HOME/bin/hadoop`, que carga **`$HADOOP_HOME/etc/hadoop/hadoop-env.sh`**. Si ahí está `JAVA_HOME` apuntando a **Java 17**, verás `UnsupportedClassVersionError` (61 vs 65) aunque en la terminal hayas exportado Java 21. **Solución:** edita `hadoop-env.sh` y pon `export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64` (o tu JDK 21). Diagnóstico: `./scripts/diagnostico_java_hadoop_hive.sh`.

**Derby / metastore embebido:** si `schematool` falla con **`ClassNotFoundException: org.apache.derby.jdbc.EmbeddedDriver`**, prueba en este orden: 1) **`./scripts/instalar_derby_en_hive.sh`** (JARs + HADOOP_CLASSPATH en hive-env.sh); 2) si sigue fallando, **`./scripts/fix_hive_schematool_derby.sh`** (copia derby*.jar a `$HADOOP_HOME/share/hadoop/common/lib/`). Asegúrate de que **`HIVE_HOME`** apunte a Hive 4.2 (p. ej. `~/apache-hive-4.2.0-bin`) si tienes varias instalaciones.

Instalación Hive 4.2: `./scripts/instalar_hive_java21.sh`.

**`hive -e` en Hive 4.x:** por defecto el comando `hive` se redirige a **Beeline**, que necesita **HiveServer2** (JDBC). Sin servidor verás *«Cannot run commands specified using -e. No current connection»*. Opciones: (1) **`./scripts/patch_hive_use_cli_driver.sh`** y luego `export USE_BEELINE_FOR_HIVE_CLI=false` para usar el CLI clásico con metastore local; (2) arrancar **HiveServer2** y `beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;"`; (3) **`spark-sql -e "SHOW DATABASES"`** si Spark comparte el mismo metastore.

**Java 21 + CLI clásico:** si aparece **`InaccessibleObjectException`** en `java.net.URI`, ejecuta **`./scripts/hive_env_java21_opens.sh`** (añade `--add-opens` a `HADOOP_CLIENT_OPTS` en `hive-env.sh`). `app_visualizacion.py` también inyecta estos flags al ejecutar `hive`/`spark-sql`.

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
- **Hive `schematool` / `UnsupportedClassVersionError` (61 vs 65):** Hadoop está usando otra JVM; revisa **`$HADOOP_HOME/etc/hadoop/hadoop-env.sh`** (`JAVA_HOME` → JDK 21). Ver `./scripts/diagnostico_java_hadoop_hive.sh`.
- **Hive `ClassNotFoundException: org.apache.derby.jdbc.EmbeddedDriver`:** `./scripts/instalar_derby_en_hive.sh`; si persiste, `./scripts/fix_hive_schematool_derby.sh` (copia Derby a Hadoop common/lib). Comprueba `HIVE_HOME` si tienes varios Hive instalados.
- **Hive `Cannot run commands specified using -e. No current connection`:** Hive 4 usa Beeline por defecto (necesita HiveServer2). Ver arriba: **`./scripts/patch_hive_use_cli_driver.sh`** + `export USE_BEELINE_FOR_HIVE_CLI=false`, o `beeline` contra el puerto 10000, o `spark-sql`.
- **Hive `InaccessibleObjectException` / `java.net`** con Java 21: **`./scripts/hive_env_java21_opens.sh`** (o reinstalar con `./scripts/instalar_hive_java21.sh` actualizado).
- **Cassandra no arranca con Java 21** (`Unrecognized VM option 'UseBiasedLocking'`): **`./scripts/patch_cassandra_java21_jvm.sh`** y vuelve a `./iniciar_servicios --only cassandra`.

---

## APIs

Configuración de **Electricity Maps** y **OpenWeather**: **[docs/API_INTEGRACION.md](docs/API_INTEGRACION.md)**.
