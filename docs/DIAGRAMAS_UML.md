# Diagramas UML — Smart Grid España

Todos los diagramas están en sintaxis **Mermaid** y pueden visualizarse en GitHub, GitLab o editores compatibles.

---

## 1. Diagrama de casos de uso

```mermaid
flowchart TB
    subgraph Actores
        A1[Operador]
        A2[Directivo]
    end

    subgraph Sistema["Smart Grid Dashboard"]
        UC1[CU-01 Ejecutar ciclo 15 min]
        UC2[CU-02 Arrancar/comprobar servicios]
        UC3[CU-03 Consultar cuadro de mando]
        UC4[CU-04 Visualizar mapa y estado]
        UC5[CU-05 Ver informe cambios ciclos]
        UC6[CU-06 Consultas Hive manuales]
        UC7[CU-07 Parar servicios]
        UC12[CU-12 Evaluar riesgo de apagón]
    end

    A1 --> UC1
    A1 --> UC2
    A1 --> UC4
    A1 --> UC5
    A1 --> UC6
    A1 --> UC7
    A1 --> UC12
    A2 --> UC3
    A2 --> UC4

    UC1 -.->|incluye| UC4
    UC2 -.->|precondición| UC1
```

---

## 2. Diagrama de secuencia — Ciclo 15 minutos

```mermaid
sequenceDiagram
    participant O as Operador
    participant D as Dashboard
    participant P as producer.py
    participant K as Kafka
    participant H as HDFS
    participant S as procesamiento_grafos
    participant C as Cassandra
    participant M as Mapa

    O->>D: Ejecutar ciclo 15 min
    D->>P: Ejecutar producer
    P->>P: Obtener Electricity Maps / OpenWeather
    P->>K: Publicar energy_raw, weather_raw
    P->>H: Backup energy_*.json
    P-->>D: rc=0

    D->>S: Ejecutar procesamiento_grafos
    S->>H: Leer JSON
    S->>S: Construir grafo, PageRank, articulaciones
    S->>C: Escribir subestaciones_estado, lineas_estado, pagerank, puntos_fallo
    S-->>D: rc=0

    D->>D: _cluster_cassandra.clear()
    D->>D: st.rerun()
    D->>C: Cargar subestaciones, lineas, pagerank
    C-->>D: Datos
    D->>M: Construir mapa
    D->>O: Mostrar mapa + informe cambios
```

---

## 2.b Diagrama de secuencia — Riesgo de apagón (simulación operativa)

```mermaid
sequenceDiagram
    participant O as Operador
    participant D as Dashboard
    participant R as app_riesgo_apagon_panel
    participant E as riesgo.py
    participant M as Mapa

    O->>D: Abre panel Riesgo de apagón
    O->>R: Ajusta sliders o preset (colapso/preventivo)
    R->>E: evaluar_riesgo_apagon_desde_snapshots(snapshot_actual)
    E-->>R: risk_score actual + componentes
    R->>E: evaluar_riesgo_apagon_desde_snapshots(escenario_horizonte)
    E-->>R: risk_score estimado + umbral + alerta
    R->>O: Mostrar tendencia (15/30/45/60) y playbook
    O->>R: Activar "Aplicar escenario al mapa"
    R->>D: Guardar riesgo_map_override en session_state
    D->>M: Renderizar mapa con estados simulados
    M-->>O: Visualización contingencia (sin persistir en Cassandra)
```

---

## 3. Diagrama de componentes

```mermaid
flowchart TB
    subgraph Externos
        EM[Electricity Maps API]
        OW[OpenWeather API]
    end

    subgraph Ingesta
        PROD[producer.py]
    end

    subgraph Mensajeria
        KAFKA[Kafka<br/>energy_raw<br/>weather_raw]
    end

    subgraph Almacenamiento
        HDFS[HDFS<br/>backup JSON]
        CASS[(Cassandra<br/>smart_grid)]
        HIVE[(Hive<br/>smart_grid_analytics)]
    end

    subgraph Procesamiento
        SPARK[procesamiento_grafos.py<br/>Spark + GraphFrames]
    end

    subgraph Visualizacion
        APP[app_visualizacion.py<br/>Streamlit + Folium]
        RISK[app_riesgo_apagon_panel.py<br/>Simulación + playbook]
    end

    subgraph AnaliticaRiesgo
        RIESGO[procesamiento/deteccion_apagon/riesgo.py]
    end

    EM --> PROD
    OW --> PROD
    PROD --> KAFKA
    PROD --> HDFS
    KAFKA --> SPARK
    HDFS --> SPARK
    SPARK --> CASS
    SPARK --> HIVE
    CASS --> APP
    APP --> RISK
    RISK --> RIESGO
    RISK --> APP
```

---

## 4. Diagrama de clases (simplificado)

```mermaid
classDiagram
    class Config {
        +KAFKA_BOOTSTRAP
        +CASSANDRA_HOST
        +KEYSPACE
        +HIVE_DB
        +TOPIC_RAW
        +TOPIC_WEATHER_RAW
    }

    class ConfigNodos {
        +get_nodos()
        +get_aristas()
        +SUBESTACIONES_PRINCIPALES
        +SUBESTACIONES_SECUNDARIAS
    }

    class Producer {
        +obtener_electricity_maps()
        +obtener_weather()
        +publicar_kafka()
        +backup_hdfs()
    }

    class ProcesamientoGrafos {
        +crear_spark()
        +construir_grafo_base()
        +aplicar_autosanacion()
        +analizar_puntos_fallo_unicos()
    }

    class AppVisualizacion {
        +cargar_subestaciones()
        +cargar_lineas()
        +cargar_pagerank()
        +cargar_puntos_fallo()
        +construir_mapa()
        +_generar_informe_cambios_ciclo()
    }

    class AppRiesgoApagonPanel {
        +render_riesgo_apagon_panel()
        +_simular_escenario()
        +_aplicar_preset_colapso()
        +_aplicar_preset_preventivo()
    }

    class RiesgoApagon {
        +evaluar_riesgo_apagon_desde_snapshots()
        +evaluar_riesgo_apagon_metricas()
        +componente_voltaje()
        +componente_frecuencia()
        +componente_perdida_generacion()
        +componente_cascada()
    }

    class CassandraSession {
        +execute(query)
    }

    Producer --> Config : usa
    Producer --> ConfigNodos : usa
    ProcesamientoGrafos --> Config : usa
    ProcesamientoGrafos --> ConfigNodos : usa
    AppVisualizacion --> Config : usa
    AppVisualizacion --> CassandraSession : usa
    AppVisualizacion --> AppRiesgoApagonPanel : integra
    AppRiesgoApagonPanel --> RiesgoApagon : usa
```

---

## 5. Diagrama de estados — Servicios

```mermaid
stateDiagram-v2
    [*] --> Parado
    Parado --> Arrancando: Arrancar servicios
    Arrancando --> Comprobando: HDFS, Kafka, Cassandra OK
    Comprobando --> Operativo: Esquemas aplicados
    Comprobando --> Parcial: Alguno falla (Hive timeout, etc.)

    Operativo --> Ejecutando: Ejecutar ciclo 15 min
    Ejecutando --> Operativo: Ciclo OK
    Ejecutando --> Error: producer/Spark fallan

    Parcial --> Operativo: Completar manualmente
    Operativo --> Parado: Parar servicios
```

---

## 6. Diagrama de flujo de datos (DFD nivel 1)

```mermaid
flowchart LR
    subgraph Fuentes
        F1[Electricity Maps]
        F2[OpenWeather]
        F3[Simulación]
    end

    subgraph Proceso1[1.0 Ingesta]
        P1[producer.py]
    end

    subgraph Almacenes
        D1[(Kafka)]
        D2[(HDFS)]
        D3[(Cassandra)]
        D4[(Hive)]
    end

    subgraph Proceso2[2.0 Procesamiento]
        P2[procesamiento_grafos]
    end

    subgraph Destino
        U1[Dashboard]
    end

    F1 --> P1
    F2 --> P1
    F3 --> P1
    P1 --> D1
    P1 --> D2
    D1 --> P2
    D2 --> P2
    P2 --> D3
    P2 --> D4
    D3 --> U1
```

---

## 7. Diagrama de despliegue

```mermaid
flowchart TB
    subgraph Nodo1["Nodo único (desarrollo)"]
        subgraph AppLayer
            STREAM[Streamlit<br/>:8501]
            PY[Python producer]
        end

        subgraph DataLayer
            K[Kafka :9092]
            C[(Cassandra :9042)]
        end

        subgraph HadoopLayer
            HDFS[HDFS :9000]
            HIVE[Hive/Spark-SQL]
        end

        subgraph SparkLayer
            SPARK[Spark<br/>procesamiento_grafos]
        end
    end

    STREAM --> C
    PY --> K
    PY --> HDFS
    SPARK --> HDFS
    SPARK --> C
    SPARK --> HIVE
```

---

## 8. Diagrama de actividad — Arranque Fase 0

```mermaid
flowchart TD
    A[Inicio] --> B{HDFS en 9000?}
    B -->|No| C[start-dfs / start-all]
    B -->|Sí| D{Kafka en 9092?}
    C --> D
    D -->|No| E[kafka-server-start]
    D -->|Sí| F{Cassandra en 9042?}
    E --> F
    F -->|No| G[cassandra/bin/cassandra]
    F -->|Sí| H[Crear topics]
    G --> H
    H --> I{Aplicar esquema Cassandra}
    I --> J{Hive disponible?}
    J -->|Sí| K[setup_hive.hql]
    J -->|No| L[Fin]
    K --> L
```
