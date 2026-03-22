# Flow Definition Smart Grid — NiFi 2.6.0

Grupo de procesadores NiFi en formato JSON para ingesta, Kafka, HDFS, Spark, Hive y Cassandra.

## Importación

1. Arranca NiFi (`./scripts/instalar_nifi_260.sh` y `$NIFI_HOME/bin/nifi.sh start`)
2. En el canvas de NiFi: **Arrastra** el icono de **Process Group**
3. En el diálogo "Add Process Group": haz clic en el **icono de subir** (upload) junto al campo Name
4. Selecciona `smart_grid_flow_definition.json`
5. Ajusta el nombre si quieres y pulsa **Add**

## Parámetros (Parameter Context)

Tras importar, crea un **Parameter Context** y asigna estos parámetros (o usa los valores por defecto en el JSON):

| Parámetro        | Valor por defecto              | Descripción                                      |
|------------------|--------------------------------|--------------------------------------------------|
| `KAFKA_BOOTSTRAP`| `localhost:9092`               | Servidores Kafka                                 |
| `HDFS_BACKUP_PATH` | `/user/hadoop/energy_backup` | Ruta HDFS para raw                               |
| `BASE_PATH`      | `/home/hadoop/smart_energy`    | Raíz del proyecto                                |
| `API_WEATHER_KEY`| *(vacío)*                     | Clave OpenWeather API                            |
| `GPS_LOGS_DIR`   | `.../data/gps_logs`            | Directorio de logs GPS                           |
| `SPARK_MASTER`   | `local[*]`                     | Spark master (`local[*]` o `yarn`)               |
| `SPARK_HOME`     | `/opt/spark`                   | Directorio de instalación de Spark               |

Asigna el Parameter Context al grupo de procesadores importado.

## Flujo de datos

```
                    ┌──────────────────────────────────────────────────────────────────┐
                    │  TriggerIngesta (GenerateFlowFile cada 15 min)                    │
                    └─────┬─────────────┬─────────────────────┬────────────────────────┘
                          │             │                     │
         ┌────────────────┘             │                     └─────────────────┐
         ▼                              ▼                                       ▼
┌─────────────────┐          ┌──────────────────┐                    ┌─────────────────────────┐
│ ExecuteProducer │          │ InvokeHTTP       │                    │ ExecuteSpark_           │
│ (producer.py)   │          │ OpenWeather      │                    │ ProcesamientoGrafos     │
│                 │          │                  │                    │ (spark-submit)          │
│ EM + OpenWeather│          └────────┬─────────┘                    │                         │
│ + simulación    │                   │                              │ → Cassandra + Hive      │
│ + Kafka + HDFS  │                   ├─────────────────┬────────────┘                         │
└─────────────────┘                   ▼                 ▼                                     │
                          ┌──────────────────┐  ┌──────────────────┐                          │
                          │ PutHDFS_weather  │  │ PublishKafka     │                          │
                          │ _raw             │  │ weather_raw      │                          │
                          └──────────────────┘  └──────────────────┘                          │

┌─────────────────┐          ┌──────────────────┐
│ GetFile_GPS     │─────────▶│ PublishKafka     │
│ (data/gps_logs) │          │ gps_raw          │
└─────────────────┘          └──────────────────┘
```

## Procesadores

| Procesador                       | Función                                                                 |
|----------------------------------|-------------------------------------------------------------------------|
| **TriggerIngesta**               | GenerateFlowFile cada 15 min; dispara el resto del flujo                |
| **ExecuteProducer**              | `producer.py`: Electricity Maps, OpenWeather, simulación, Kafka, HDFS   |
| **InvokeHTTP_OpenWeather**       | API OpenWeather (Madrid); alternativa. El producer.py genera el schema completo para weather_raw |
| **PutHDFS_weather_raw**          | Escribe raw clima en HDFS (`/user/hadoop/energy_backup/weather/`)       |
| **PublishKafka_weather_raw**     | Publica en topic `weather_raw`                                          |
| **GetFile_GPS**                  | Lee logs GPS de `data/gps_logs/`                                        |
| **PublishKafka_gps_raw**         | Publica en topic `gps_raw`                                              |
| **ExecuteSpark_ProcesamientoGrafos** | `spark-submit procesamiento_grafos.py` → Cassandra + Hive            |
| **ExecuteSpark_PersistirHive**   | `persistir_hive_ingesta.py` (opcional; deshabilitado por defecto)       |

## Topics Kafka

- `energy_raw`: Carga y voltaje por subestación (producer.py)
- `weather_raw`: Clima zonas renovables (producer.py o InvokeHTTP)
- `gps_raw`: Logs GPS (GetFile)

## Requisitos

- **Kafka** en marcha; topics `energy_raw`, `weather_raw`, `gps_raw`
- **HDFS** configurado (para PutHDFS)
- **Spark** con `SPARK_HOME` correcto
- **Cassandra** y **Hive** para el pipeline de procesamiento

## YARN

Para usar YARN en lugar de local:

1. Parámetro `SPARK_MASTER` = `yarn`
2. Configurar `HADOOP_CONF_DIR` para que NiFi/Spark vean la configuración YARN

## Controller Service

El flujo define **KafkaConnService_SmartGrid** (Kafka3ConnectionService) con `bootstrap.servers` tomado de `#{KAFKA_BOOTSTRAP}#`. Tras importar, verifica que esté habilitado.
