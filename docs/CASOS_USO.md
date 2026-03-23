# Especificación de Casos de Uso — Smart Grid España

## 1. Actores

| Actor | Descripción |
|-------|-------------|
| **Operador** | Usuario técnico que arranca servicios, ejecuta pipeline y monitoriza |
| **Directivo** | Usuario que consulta cuadro de mando e informes históricos |
| **Sistema** | Procesos automatizados (producer, Spark, Airflow) |

---

## 2. Casos de Uso

### CU-01 Ejecutar ciclo de ingesta y procesamiento

**Descripción:** El operador ejecuta un ciclo completo de 15 minutos (ingesta + procesamiento) para actualizar el estado de la red.

**Actor principal:** Operador

**Precondiciones:** Kafka, HDFS y Cassandra están levantados; topics y esquema creados.

#### Escenario normal

1. El operador abre el dashboard Streamlit.
2. El operador pulsa **"Ejecutar ciclo 15 min"** en la barra lateral.
3. El sistema ejecuta producer.py (ingesta desde APIs o simulación).
4. El sistema publica en Kafka (energy_raw, weather_raw) y escribe backup en HDFS.
5. El sistema ejecuta procesamiento_grafos.py.
6. El sistema calcula PageRank, puntos de fallo y persiste en Cassandra.
7. El sistema actualiza el dashboard y muestra el mapa con datos nuevos.
8. El sistema muestra el informe "Cambios desde el ciclo anterior" (si hay ciclo previo).

**Postcondiciones:** Cassandra actualizado; mapa y KPIs reflejan el nuevo estado.

#### Escenario alternativo 2a: producer.py falla

3a. producer.py devuelve error (API no disponible, Kafka no responde).
4a. El sistema muestra "Fallo en producer.py" y expande la salida.
5a. El caso de uso termina sin actualizar datos.

#### Escenario alternativo 2b: procesamiento_grafos.py falla

5b. procesamiento_grafos.py devuelve error (Cassandra no responde, JAR faltante).
6b. El sistema muestra "Fallo en procesamiento_grafos.py" y expande la salida.
7b. El ciclo no completa; los datos de Kafka/HDFS pueden reprocesarse manualmente.

#### Escenario alternativo 2c: servicios no levantados

3c. El operador intenta ejecutar sin tener Kafka/HDFS/Cassandra.
4c. producer.py o procesamiento_grafos.py fallan con errores de conexión.
5c. El operador debe arrancar servicios (CU-02) y reintentar.

---

### CU-02 Arrancar y comprobar servicios

**Descripción:** El operador arranca los servicios base (HDFS, Kafka, Cassandra, Airflow, API Swagger) y aplica esquemas.

**Actor principal:** Operador

**Precondiciones:** Ninguna (primera ejecución o tras parada).

#### Escenario normal

1. El operador abre la pestaña **"0 · Entorno (servicios)"**.
2. El operador pulsa **"▶ Arrancar servicios (completo)"**.
3. El sistema arranca HDFS (start-dfs o start-all).
4. El sistema arranca Kafka.
5. El sistema arranca Cassandra.
6. El sistema crea topics energy_raw y weather_raw.
7. El sistema aplica esquema Cassandra (keyspace smart_grid).
8. El sistema aplica esquema Hive (si HDFS y spark-sql/hive disponibles).
9. El sistema arranca Airflow (**api-server** en 8080 + **dag-processor** + **scheduler**).
10. El sistema arranca API Swagger (uvicorn en 8000, Swagger UI en /docs).
11. El sistema muestra el resultado de la comprobación (hdfs_ok, kafka_ok, cassandra_ok, airflow_ok, api_swagger, etc.).

**Postcondiciones:** Servicios en ejecución (incl. Airflow UI, API Swagger en http://localhost:8000/docs); esquemas aplicados.

#### Tareas previas (primera vez o DAGs faltantes)

- **Sincronizar DAGs:** `./scripts/sync_dags_airflow.sh` — copia DAGs de `orquestacion/` a `~/airflow/dags/`.
- **Obtener contraseña Airflow:** En Airflow 3.x (SimpleAuthManager), la contraseña está en `~/airflow/simple_auth_manager_passwords.json.generated` o en el log del api-server. Ver `docs/CREDENCIALES_UI.md`.

#### Escenario alternativo 3a: HDFS ya responde

3a. El sistema detecta HDFS activo (puerto 9000) y omite el arranque.
4a. Continúa con Kafka.

#### Escenario alternativo 3b: Hive omite por timeout

8b. SHOW DATABASES excede tiempo (metastore/Spark lento).
9b. El sistema muestra "Hive omitido: SHOW DATABASES excedió tiempo" y continúa.
10b. El operador puede aplicar setup_hive.hql manualmente más tarde.

#### Escenario alternativo 3c: Cassandra no arranca

5c. Cassandra falla (Java, puerto ocupado, etc.).
6c. El sistema muestra error en cassandra_start.
7c. Topics y Hive pueden aplicarse; Cassandra debe arrancarse manualmente.

#### Escenario alternativo 3d: Airflow muestra "0 DAGs"

9d. El dag-processor no está en marcha (Airflow 3.x) o faltan DAGs en `~/airflow/dags/`.
10d. El operador ejecuta `./scripts/sync_dags_airflow.sh` y reinicia Airflow (`parar_servicios --only airflow` → `iniciar_servicios --only airflow`).
11d. Tras 30–60 s, los DAGs aparecen. Ver `docs/AIRFLOW.md` (Problemas típicos).

#### Escenario alternativo 3e: 401 Unauthorized en Airflow UI

9e. Contraseña incorrecta (Airflow 3.x no usa `admin`/`admin` por defecto).
10e. El operador consulta `docs/CREDENCIALES_UI.md` y obtiene la contraseña de `simple_auth_manager_passwords.json.generated` o configura FAB / desactiva auth.

---

### CU-03 Consultar cuadro de mando (informes históricos)

**Descripción:** El directivo consulta informes históricos desde Hive para toma de decisiones.

**Actor principal:** Directivo

**Precondiciones:** Hive/Spark-SQL disponibles; catálogo con base smart_grid_analytics.

#### Escenario normal

1. El directivo abre la pestaña **"📊 Cuadro de mando"**.
2. El directivo pulsa un botón de informe (p. ej. "Consumo energético total (MWh)").
3. El sistema ejecuta la consulta Hive correspondiente en **modo rápido** (timeout corto para no bloquear la UI).
4. El sistema muestra los resultados en una tabla (parser tabular estándar o fallback TSV sin cabecera).
5. El directivo puede pulsar "Limpiar resultado" o elegir otro informe.

**Postcondiciones:** Informe mostrado en pantalla.

#### Escenario alternativo 2a: Hive no responde (timeout)

3a. spark-sql o hive exceden tiempo de espera.
4a. El sistema muestra aviso de timeout y sugiere reintentar o ajustar `HIVE_UI_QUICK_HIVE_TIMEOUT_SEC` / `HIVE_UI_QUICK_SPARK_TIMEOUT_SEC`.

#### Escenario alternativo 2b: Tabla vacía o no existe

4b. La consulta devuelve 0 filas o error de tabla inexistente.
5b. El sistema muestra "Sin filas" o el mensaje de error.
6b. El directivo debe asegurar que el pipeline ha generado datos históricos.

#### Escenario alternativo 2c: Datos existen pero formato CLI no estándar

4c. `spark-sql` devuelve filas en formato tabulado sin cabecera.
5c. El parser principal no detecta tabla con separadores `|`.
6c. El sistema aplica fallback TSV y muestra igualmente el informe en formato legible.

---

### CU-04 Visualizar mapa y estado de la red

**Descripción:** El operador o directivo visualiza el mapa de España con subestaciones, líneas y estado en tiempo real.

**Actor principal:** Operador, Directivo

**Precondiciones:** Dashboard abierto.

#### Escenario normal

1. El usuario navega al mapa (debajo de las pestañas).
2. El sistema carga subestaciones y líneas desde Cassandra.
3. El sistema muestra el mapa con Folium; colores según estado (OK=verde, Alerta=naranja, Sobrecarga=rojo).
4. El sistema muestra PageRank y puntos de fallo únicos en paneles laterales.
5. El usuario puede hacer zoom y ver popups con detalles.

**Postcondiciones:** Mapa actualizado con datos de Cassandra.

#### Escenario alternativo 2a: Cassandra no conecta (modo demo)

2a. No hay sesión al keyspace smart_grid.
3a. El sistema usa datos demo (topología sin telemetría real).
4a. El sistema muestra aviso "Mapa en modo topología (demo)".

#### Escenario alternativo 2b: Cassandra vacía

2b. subestaciones_estado está vacía.
3b. El sistema muestra "Cassandra conectada pero subestaciones_estado vacía" y usa datos demo.

---

### CU-05 Consultar informe de cambios entre ciclos

**Descripción:** El operador revisa qué ha cambiado respecto al ciclo anterior.

**Actor principal:** Operador

**Precondiciones:** Al menos un ciclo ejecutado previamente; datos cargados desde Cassandra.

#### Escenario normal

1. Tras ejecutar un ciclo (CU-01), el sistema carga los nuevos datos.
2. El sistema compara con el snapshot del ciclo anterior.
3. El sistema muestra el expander "📋 Cambios desde el ciclo anterior".
4. Si hay cambios: subestaciones que cambiaron estado, KPIs, PageRank, articulaciones.
5. Si no hay cambios: "Sin cambios detectados."

**Postcondiciones:** Informe de diff visible.

#### Escenario alternativo 1a: Primer ciclo

2a. No existe snapshot previo.
3a. No se muestra el expander de cambios.
4a. El snapshot actual se guarda para el siguiente ciclo.

---

### CU-06 Ejecutar consultas Hive manuales (Monitorización)

**Descripción:** El operador ejecuta consultas SQL en Hive para verificación KDD.

**Actor principal:** Operador

**Precondiciones:** Hive/Spark-SQL disponibles.

#### Escenario normal

1. El operador abre **"Monitorización"** (expander).
2. El operador selecciona una plantilla o escribe SQL en el text area.
3. El operador pulsa **"▶ Ejecutar SQL"**.
4. El sistema ejecuta la consulta vía spark-sql o hive.
5. El sistema muestra el resultado en tabla y salida raw.

**Postcondiciones:** Resultado de la consulta mostrado.

#### Escenario alternativo 4a: Error de sintaxis o tabla

5a. La consulta falla (rc != 0).
6a. El sistema muestra el mensaje de error en stderr.

---

### CU-07 Parar servicios

**Descripción:** El operador detiene los servicios al finalizar la demo.

**Actor principal:** Operador

#### Escenario normal

1. El operador pulsa **"■ Parar servicios base"** en Fase 0.
2. El sistema detiene HDFS, Kafka, Cassandra, NiFi, Airflow y API Swagger.
3. El sistema muestra el estado final de la comprobación.

**Postcondiciones:** Servicios detenidos.

---

### CU-08 Ejecutar DAGs de Airflow

**Descripción:** El operador ejecuta DAGs de Airflow como alternativa a los botones del dashboard.

**Actor principal:** Operador

**Precondiciones:** Airflow arrancado (api-server + dag-processor + scheduler); DAGs sincronizados con `./scripts/sync_dags_airflow.sh`. Airflow puede arrancarse desde Fase 0 del dashboard o con `./scripts/iniciar_servicios.sh --only airflow`. Ver `docs/AIRFLOW.md` y `docs/CREDENCIALES_UI.md`.

#### Escenario normal

1. El operador accede a Airflow UI (enlace en sidebar o Monitorización).
2. El operador selecciona un DAG (arrancar servicios, comprobar, parar, ingesta, procesamiento, informes, etc.).
3. El operador pulsa **Trigger DAG**.
4. El sistema ejecuta las tareas del DAG (scripts reutilizados).
5. El operador consulta el estado y los logs de cada tarea.

**Postcondiciones:** Tareas ejecutadas según el DAG seleccionado.

#### DAGs disponibles

| DAG | Acción |
|-----|--------|
| dag_arranque_servicios_smart_grid | Arranca HDFS, Kafka, Cassandra, Airflow y NiFi |
| dag_comprobar_servicios_smart_grid | Verifica puertos y servicios |
| dag_parar_servicios_smart_grid | Para HDFS, Kafka, Cassandra, NiFi, Airflow, API |
| dag_kdd_fase1_ingesta_smart_grid | Ejecuta producer.py |
| dag_kdd_fase2_procesamiento_smart_grid | spark-submit procesamiento_grafos.py |
| dag_kdd_fase3_validacion_smart_grid | Comprueba HDFS y NiFi |
| dag_consultas_hive_cassandra_smart_grid | Consultas ejemplo |
| dag_informes_fases_smart_grid | Genera informe consolidado |
| dag_maestro_smart_grid | Pipeline cada 15 min (automático) |
| dag_mensual_retrain_limpieza_smart_grid | Limpieza + re-entrenamiento (mensual) |

---

### CU-09 Generar informe consolidado de fases

**Descripción:** El operador o el sistema genera un informe con el estado de todas las fases KDD.

**Actor principal:** Operador, Sistema (DAG)

**Precondiciones:** Ninguna (el informe indica qué servicios están disponibles o no).

#### Escenario normal

1. El operador ejecuta `python scripts/generar_informe_fases.py` o el DAG `dag_informes_fases_smart_grid`.
2. El sistema recopila: estado de servicios (HDFS, Kafka, Cassandra, NiFi, Airflow, API Swagger), ficheros HDFS, topics Kafka, tablas Cassandra, catálogo Hive, flujo NiFi.
3. El sistema genera `reports/informe_fases_YYYYMMDD_HHMMSS.md` y `informe_fases_latest.json`.
4. El operador consulta el informe para verificación o auditoría.

**Postcondiciones:** Informe generado en `reports/`.

---

### CU-10 Acceder a UIs de Airflow, NiFi y API Swagger

**Descripción:** El operador accede a las interfaces web de Airflow, NiFi y API Swagger desde el frontend.

**Actor principal:** Operador

**Precondiciones:** Airflow, NiFi y/o API arrancados; credenciales conocidas (si aplica).

#### Escenario normal

1. El operador abre el dashboard Streamlit.
2. En la barra lateral y en **Monitorización**, el operador ve enlaces a **Airflow** (8080), **NiFi** (8443) y **API Swagger** (8000).
3. El operador pulsa el enlace correspondiente (se abre en nueva pestaña).
4. Para Airflow: usuario `admin`; contraseña según versión (Airflow 3.x: `simple_auth_manager_passwords.json.generated`; ver `docs/CREDENCIALES_UI.md`). Para NiFi: ver nifi-app.log. Para API Swagger: documentación interactiva sin credenciales.
5. El operador gestiona DAGs (Airflow), flujos de ingesta (NiFi) o prueba endpoints REST (API Swagger).

**Postcondiciones:** Acceso a la UI correspondiente.

---

### CU-11 Consultar documentación streaming y tests (presentación)

**Descripción:** El operador o el tribunal consultan en el dashboard el diseño del pipeline PySpark, requisitos y casos de uso, y el comando para ejecutar tests.

**Actor principal:** Operador, presentador del proyecto

**Precondiciones:** Dashboard Streamlit disponible.

#### Escenario normal

1. El usuario abre la pestaña **📡 Streaming & QA**.
2. El sistema muestra la documentación Markdown (requisitos, arquitectura Kafka → Spark → Hive/Cassandra, cuatro casos de uso industriales).
3. El usuario copia el comando `pytest tests/ -v` para validar la suite en terminal.

**Postcondiciones:** Visión integral del módulo de streaming y de la calidad automatizada.

**Detalle de casos técnicos (monitorización en tiempo real, sobrecargas, histórico, alertas):** ver **[STREAMING_PYSPARK_QA.md](STREAMING_PYSPARK_QA.md)** (CU-ST-01 … CU-ST-04).

---

### CU-12 Evaluar riesgo de apagón eléctrico (caso España 2025)

**Descripción:** El operador consulta el **risk_score** del sistema y recibe **alerta crítica** si se supera el umbral, con desglose por sobretensión, frecuencia, generación y cascada — alineado conceptualmente con el **evento real en el sistema eléctrico ibérico (2025)**.

**Actor principal:** Operador de red

**Precondiciones:** Datos de subestaciones/líneas (Cassandra o demo); opcionalmente frecuencia simulada (Hz).

#### Escenario normal

1. El operador abre el dashboard y revisa el panel **Riesgo de apagón** (debajo de los KPIs).
2. Opcionalmente introduce **frecuencia de red (Hz)** para activar el componente de inestabilidad de frecuencia.
3. El sistema calcula `risk_score` y muestra el desglose de componentes.
4. Si `risk_score ≥ umbral`, el sistema muestra **alerta crítica** y recomendaciones textuales.

**Postcondiciones:** Decisión de vigilancia reforzada o escalado operativo documentado.

**Referencia:** **[APAGON_ESPANA_2025_CASO.md](APAGON_ESPANA_2025_CASO.md)**.

---

## 3. Matriz de trazabilidad

| Caso de uso | Requisitos cubiertos |
|-------------|----------------------|
| CU-01 | RF-06.1, RF-06.2 |
| CU-02 | RF-06.3, RF-05.7, RF-07.7, RF-09.3 |
| CU-03 | RF-05.5 |
| CU-04 | RF-05.1, RF-05.2, RF-05.3, RF-05.4 |
| CU-05 | RF-05.6 |
| CU-06 | RF-04.* (verificación) |
| CU-07 | RF-05.7, RF-07.7, RF-09.3 |
| CU-08 | RF-07.1, RF-07.3, RF-07.4, RF-07.5, RF-07.7 |
| CU-09 | RF-07.6 |
| CU-10 | RF-08.3, RF-09.1, RF-09.2, RF-09.3 |
| CU-11 | RF-10.5, RF-10.6, RNF-08.1 |
| CU-12 | RF-11.1, RF-11.2, RF-11.4 |
