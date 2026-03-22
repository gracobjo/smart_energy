# Especificación de Requisitos — Smart Grid España

## 1. Introducción

Sistema de **monitoreo de redes de energía inteligentes** para España, basado en ciclo KDD y arquitectura Lambda/Kappa. Stack: Kafka, Spark 3.5, Cassandra 5.0, Hive, Airflow 2.10.x.

---

## 2. Requisitos Funcionales

### RF-01 Ingesta de datos energéticos

| ID | Descripción | Prioridad |
|----|-------------|-----------|
| RF-01.1 | El sistema debe consumir datos de intensidad de carbono y mix renovable (Electricity Maps o simulación sintética) | Alta |
| RF-01.2 | El sistema debe publicar lecturas de carga (MW), voltaje (kV) y estado por subestación en Kafka (`energy_raw`) | Alta |
| RF-01.3 | El sistema debe publicar estado de líneas (flujo MW, capacidad, estado) en Kafka (`energy_raw`) | Alta |
| RF-01.4 | El sistema debe almacenar backup de datos en HDFS para reprocesamiento | Media |
| RF-01.5 | El sistema debe admitir ejecución con o sin API key de Electricity Maps (modo sintético) | Media |

### RF-02 Ingesta de datos climáticos

| ID | Descripción | Prioridad |
|----|-------------|-----------|
| RF-02.1 | El sistema debe consumir datos de OpenWeather en zonas solares/eólicas | Alta |
| RF-02.2 | El sistema debe publicar datos climáticos (temperatura, humedad, viento, nubes) en Kafka (`weather_raw`) | Alta |
| RF-02.3 | El sistema debe correlacionar clima con demanda para producción renovable | Media |

### RF-03 Procesamiento de grafos

| ID | Descripción | Prioridad |
|----|-------------|-----------|
| RF-03.1 | El sistema debe modelar la red como grafo: nodos = subestaciones, aristas = líneas | Alta |
| RF-03.2 | El sistema debe aplicar autosanación: excluir líneas en sobrecarga del grafo | Alta |
| RF-03.3 | El sistema debe calcular PageRank para identificar nodos críticos | Alta |
| RF-03.4 | El sistema debe detectar puntos de fallo únicos (articulación) cuya caída fragmenta la red | Alta |
| RF-03.5 | El sistema debe persistir resultados en Cassandra (estado en tiempo real) | Alta |

### RF-04 Persistencia histórica (Hive)

| ID | Descripción | Prioridad |
|----|-------------|-----------|
| RF-04.1 | El sistema debe almacenar histórico de subestaciones, líneas y eventos en Hive | Alta |
| RF-04.2 | El sistema debe almacenar métricas de sostenibilidad (carbono, % renovable) en Hive | Alta |
| RF-04.3 | El sistema debe almacenar consumo energético diario por subestación | Media |
| RF-04.4 | El sistema debe almacenar clima en zonas renovables para correlación | Media |

### RF-05 Dashboard de visualización

| ID | Descripción | Prioridad |
|----|-------------|-----------|
| RF-05.1 | El sistema debe mostrar mapa de España con subestaciones y líneas | Alta |
| RF-05.2 | El sistema debe visualizar estado por color (OK, Alerta, Sobrecarga) | Alta |
| RF-05.3 | El sistema debe mostrar PageRank (nodos críticos) | Alta |
| RF-05.4 | El sistema debe mostrar puntos de fallo únicos (articulación) | Alta |
| RF-05.5 | El sistema debe proporcionar cuadro de mando directivo con reportes históricos desde Hive | Alta |
| RF-05.6 | El sistema debe mostrar informe de cambios entre ciclos de simulación | Media |
| RF-05.7 | El sistema debe permitir arrancar, comprobar y parar servicios desde la interfaz | Media |

### RF-06 Ciclo KDD y pipeline

| ID | Descripción | Prioridad |
|----|-------------|-----------|
| RF-06.1 | El sistema debe ejecutar ciclo completo: producer → procesamiento → actualización dashboard | Alta |
| RF-06.2 | El sistema debe permitir ejecución de ciclos cada 15 minutos | Alta |
| RF-06.3 | El sistema debe aplicar esquemas (Cassandra, Hive) de forma automática o manual | Media |

### RF-07 Orquestación (Airflow)

| ID | Descripción | Prioridad |
|----|-------------|-----------|
| RF-07.1 | El sistema debe orquestar ingesta y procesamiento batch mediante DAGs | Media |
| RF-07.2 | El sistema debe incluir DAG mensual de limpieza HDFS y re-entrenamiento | Baja |
| RF-07.3 | El sistema debe disponer de DAGs para arrancar, comprobar y parar servicios | Media |
| RF-07.4 | El sistema debe disponer de DAGs por fase KDD (ingesta, procesamiento, validación) | Media |
| RF-07.5 | El sistema debe disponer de DAG de consultas a Hive y Cassandra | Baja |
| RF-07.6 | El sistema debe disponer de DAG de informes consolidados de todas las fases | Media |
| RF-07.7 | El sistema debe incluir Airflow en el entorno de arranque integrado (iniciar_servicios.sh, Fase 0 del dashboard, parar_servicios.sh) | Media |

### RF-08 Ingesta alternativa (NiFi)

| ID | Descripción | Prioridad |
|----|-------------|-----------|
| RF-08.1 | El sistema debe permitir ingesta vía NiFi (ExecuteStreamCommand de producer.py) | Media |
| RF-08.2 | El sistema debe permitir flujos NiFi alternativos (InvokeHTTP OpenWeather, GetFile GPS → Kafka) | Baja |
| RF-08.3 | El sistema debe exponer UI de NiFi accesible desde el frontend | Media |

### RF-09 Interfaz de orquestación

| ID | Descripción | Prioridad |
|----|-------------|-----------|
| RF-09.1 | El sistema debe exponer enlaces a Airflow UI, NiFi UI y API Swagger desde el dashboard | Media |
| RF-09.2 | El sistema debe documentar credenciales de acceso a las UIs | Baja |
| RF-09.3 | El sistema debe incluir la API Swagger en arranque, comprobación y parada de servicios (Fase 0, scripts) | Media |

---

## 3. Requisitos No Funcionales

### RNF-01 Rendimiento

| ID | Descripción | Criterio |
|----|-------------|----------|
| RNF-01.1 | Tiempo de respuesta del dashboard | < 5 s para carga inicial |
| RNF-01.2 | Latencia de procesamiento Spark | Ciclo 15 min completado en < 10 min |
| RNF-01.3 | Throughput Kafka | Soporta ingesta de ~30 subestaciones + líneas por ciclo |

### RNF-02 Escalabilidad

| ID | Descripción | Criterio |
|----|-------------|----------|
| RNF-02.1 | Arquitectura horizontal | Kafka, Cassandra y Spark escalables por nodos |
| RNF-02.2 | Particionamiento | Topics Kafka con particiones configurables |

### RNF-03 Disponibilidad

| ID | Descripción | Criterio |
|----|-------------|----------|
| RNF-03.1 | Tolerancia a fallos | Cassandra con replicación; Kafka con réplicas |
| RNF-03.2 | Modo demo | Dashboard funcional sin Cassandra (datos sintéticos) |

### RNF-04 Seguridad

| ID | Descripción | Criterio |
|----|-------------|----------|
| RNF-04.1 | Secretos | API keys en variables de entorno, no en código |
| RNF-04.2 | Red | Servicios configurables (host, puerto) |

### RNF-05 Mantenibilidad

| ID | Descripción | Criterio |
|----|-------------|----------|
| RNF-05.1 | Configuración centralizada | `config.py`, variables de entorno |
| RNF-05.2 | Documentación | README, docs/, scripts de troubleshooting |
| RNF-05.3 | Convenciones multiagente | AGENTS.md para ramas y componentes |

### RNF-06 Usabilidad

| ID | Descripción | Criterio |
|----|-------------|----------|
| RNF-06.1 | Interfaz intuitiva | Tabs ordenados: Cuadro de mando, Entorno, Ingesta, Procesamiento, Validación |
| RNF-06.2 | Feedback visual | Spinners, mensajes de éxito/error, métricas |
| RNF-06.3 | Comprobación de servicios | Indicadores ✓/✗ por servicio |

### RNF-07 Portabilidad

| ID | Descripción | Criterio |
|----|-------------|----------|
| RNF-07.1 | Entorno reproducible | `requirements.txt`, venv, scripts de instalación |
| RNF-07.2 | Docker | Composición para Airflow y Kafka |

---

## 4. Glosario

| Término | Definición |
|---------|------------|
| KDD | Knowledge Discovery in Databases; ciclo de minería de datos |
| PageRank | Algoritmo de centralidad para identificar nodos críticos en el grafo |
| Articulación | Nodo cuya eliminación desconecta el grafo |
| Lambda/Kappa | Arquitecturas de procesamiento batch + streaming |
| Smart Grid | Red eléctrica inteligente con telemetría y control |
